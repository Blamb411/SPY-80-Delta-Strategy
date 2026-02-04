"""
Run Backtest — Historical Validation
=======================================
Score the universe on all 48 pick dates and validate against SA's
actual 97 Alpha Picks.

Usage:
    python run_backtest.py                       # Full backtest
    python run_backtest.py --start 2024-01-01    # From specific date
    python run_backtest.py --optimize            # Run weight optimization
    python run_backtest.py --report              # Save report to file
"""

import os
import sys
import argparse
import itertools
import warnings
from datetime import datetime
from typing import Dict, List, Tuple

warnings.filterwarnings("ignore", message="invalid value encountered in cast")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
import db_schema
from data.lseg_client import LSEGClient
from data.polygon_client import PolygonClient
from data.universe_builder import (
    build_universe, get_sector_map, get_pick_dates,
    load_alpha_picks_from_excel, get_alpha_picks_on_date,
)
from scoring.composite_scorer import score_universe
from analysis.rating_persistence import update_persistence
from analysis.backtest_validator import validate_against_picks, print_validation_report
from analysis.report_generator import generate_backtest_report, save_report


def run_single_date(score_date: str, symbols: List[str],
                    skip_fetch: bool = False) -> List[Dict]:
    """Score a single date (used by both backtest and live scoring)."""
    print(f"\n--- Scoring {score_date} ---")

    # Fetch data if needed
    if not skip_fetch:
        lseg = LSEGClient()
        if lseg.is_available():
            lseg.fetch_universe_batch(symbols, score_date)

        poly = PolygonClient()
        poly.fetch_momentum_batch(symbols, score_date)

    # Get sector map
    sector_map = get_sector_map(score_date)

    # Score
    results = score_universe(symbols, score_date, sector_map)

    # Update persistence
    update_persistence(score_date)

    return results


def run_backtest(start_date: str = None, end_date: str = None,
                 skip_fetch: bool = False) -> Dict:
    """Run full historical backtest across all pick dates."""
    print("\n" + "=" * 60)
    print(" HISTORICAL BACKTEST")
    print("=" * 60)

    # Initialize
    db_schema.init_db()

    # Get pick dates
    all_dates = get_pick_dates()
    if start_date:
        all_dates = [d for d in all_dates if d >= start_date]
    if end_date:
        all_dates = [d for d in all_dates if d <= end_date]

    print(f"Backtest dates: {len(all_dates)}")
    print(f"  First: {all_dates[0]}")
    print(f"  Last:  {all_dates[-1]}")

    # Load actual picks for reference
    actual_picks = load_alpha_picks_from_excel()
    print(f"Actual Alpha Picks loaded: {len(actual_picks)}")

    # Build a common universe (union of all relevant symbols)
    symbols = build_universe(all_dates[0])

    # Score each date
    for i, date in enumerate(all_dates, 1):
        print(f"\n[{i}/{len(all_dates)}] ", end="")
        sa_picks = get_alpha_picks_on_date(date)
        if sa_picks:
            print(f"(SA picked: {', '.join(sa_picks)})")

        run_single_date(date, symbols, skip_fetch=skip_fetch)

    # Validate
    print("\n\nRunning validation...")
    results = validate_against_picks()
    print_validation_report(results)

    return results


def run_weight_optimization(skip_fetch: bool = True) -> Dict:
    """
    Grid search over factor weights to maximize hit rate.

    Tests different weight combinations and reports the best one.
    """
    print("\n" + "=" * 60)
    print(" WEIGHT OPTIMIZATION")
    print("=" * 60)

    # Define weight grid (must sum to 1.0)
    # We'll vary in 5% increments
    weight_options = [0.10, 0.15, 0.20, 0.25, 0.30, 0.35]

    best_hit_rate = 0
    best_weights = None
    tested = 0

    # Generate valid weight combinations (4 factors summing to 1.0)
    for v in weight_options:
        for g in weight_options:
            for p in weight_options:
                m = round(1.0 - v - g - p, 2)
                if m < 0.05 or m > 0.40:
                    continue

                tested += 1
                # Temporarily set weights
                config.FACTOR_WEIGHTS_4F = {
                    "value": v, "growth": g,
                    "profitability": p, "momentum": m,
                }
                config.FACTOR_WEIGHTS = config.FACTOR_WEIGHTS_4F

                # Re-score all dates (using cached data)
                dates = get_pick_dates()
                symbols = build_universe(dates[0])
                for date in dates:
                    sector_map = get_sector_map(date)
                    score_universe(symbols, date, sector_map)

                # Validate
                results = validate_against_picks()
                hr = results["hit_rate"]

                if hr > best_hit_rate:
                    best_hit_rate = hr
                    best_weights = {"value": v, "growth": g, "profitability": p, "momentum": m}
                    print(f"  New best: V={v:.0%} G={g:.0%} P={p:.0%} M={m:.0%} → "
                          f"Hit Rate: {hr:.1%}")

    print(f"\nTested {tested} weight combinations")
    print(f"Best weights: {best_weights}")
    print(f"Best hit rate: {best_hit_rate:.1%}")

    # Restore best weights
    if best_weights:
        config.FACTOR_WEIGHTS_4F = best_weights
        config.FACTOR_WEIGHTS = config.FACTOR_WEIGHTS_4F

    return {"best_weights": best_weights, "best_hit_rate": best_hit_rate, "tested": tested}


def main():
    parser = argparse.ArgumentParser(description="Run historical backtest")
    parser.add_argument("--start", default=None, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", default=None, help="End date (YYYY-MM-DD)")
    parser.add_argument("--skip-fetch", action="store_true",
                        help="Skip data fetching, use cached data only")
    parser.add_argument("--optimize", action="store_true",
                        help="Run weight optimization grid search")
    parser.add_argument("--report", action="store_true",
                        help="Save report to file")
    args = parser.parse_args()

    if args.optimize:
        opt_results = run_weight_optimization(skip_fetch=True)
        return

    results = run_backtest(
        start_date=args.start,
        end_date=args.end,
        skip_fetch=args.skip_fetch,
    )

    if args.report:
        report = generate_backtest_report(results)
        path = save_report(report, f"backtest_report_{datetime.now().strftime('%Y%m%d')}.txt")
        print(f"\nReport saved to {path}")


if __name__ == "__main__":
    main()
