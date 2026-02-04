"""
Run Weekly Screen — Production Weekly Scoring
================================================
Weekly entry point for scoring the universe, updating persistence,
and generating pick predictions.

Usage:
    python run_weekly_screen.py                    # Full run
    python run_weekly_screen.py --skip-fetch       # Use cached data
    python run_weekly_screen.py --predictions 10   # Show top N predictions
    python run_weekly_screen.py --report           # Save full report
"""

import os
import sys
import argparse
import warnings
from datetime import datetime

warnings.filterwarnings("ignore", message="invalid value encountered in cast")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
import db_schema
from data.lseg_client import LSEGClient
from data.polygon_client import PolygonClient
from data.universe_builder import build_universe, get_filtered_universe, get_sector_map
from scoring.composite_scorer import score_universe, get_strong_buys
from analysis.rating_persistence import update_persistence, get_persistent_strong_buys
from analysis.pick_predictor import predict_next_picks, print_predictions
from analysis.report_generator import generate_scoring_report, save_report


def main():
    parser = argparse.ArgumentParser(description="Weekly scoring screen")
    parser.add_argument("--date", default=datetime.now().strftime("%Y-%m-%d"),
                        help="Scoring date (YYYY-MM-DD)")
    parser.add_argument("--skip-fetch", action="store_true",
                        help="Skip data fetching, use cached data")
    parser.add_argument("--predictions", type=int, default=10,
                        help="Number of pick predictions to show")
    parser.add_argument("--report", action="store_true",
                        help="Save report to file")
    args = parser.parse_args()

    score_date = args.date
    print(f"\n{'=' * 70}")
    print(f" WEEKLY SCORING SCREEN — {score_date}")
    print(f"{'=' * 70}")

    # Step 1: Init
    print("\n[1/6] Initializing...")
    db_schema.init_db()

    # Step 2: Universe
    print("\n[2/6] Building universe...")
    symbols = build_universe(score_date)

    # Step 3: Fetch data
    if not args.skip_fetch:
        print("\n[3/6] Fetching data...")

        # LSEG — fundamentals + estimates (all-in-one)
        lseg = LSEGClient()
        if lseg.is_available():
            print("  → LSEG fundamentals + estimates...")
            lseg.fetch_universe_batch(symbols, score_date)
        else:
            print("  WARNING: LSEG not available (is Workspace running?)")

        # Polygon momentum
        print("  → Polygon price/momentum...")
        poly = PolygonClient()
        poly.fetch_momentum_batch(symbols, score_date)
    else:
        print("\n[3/6] Skipping fetch (cached data)")

    # Step 4: Score
    print("\n[4/6] Scoring universe...")
    sector_map = get_sector_map(score_date)
    filtered = get_filtered_universe(score_date)
    if filtered:
        symbols = filtered

    results = score_universe(symbols, score_date, sector_map)

    # Step 5: Update persistence
    print("\n[5/6] Updating persistence streaks...")
    active_streaks = update_persistence(score_date)
    persistent = get_persistent_strong_buys(score_date)
    print(f"  Active Strong Buy streaks ≥ {config.PERSISTENCE_DAYS}d: {len(persistent)}")
    for p in persistent[:5]:
        print(f"    {p['symbol']:<7} streak: {p['streak_days']}d "
              f"(since {p['first_date']})")

    # Step 6: Predictions
    print("\n[6/6] Generating predictions...")
    candidates = predict_next_picks(score_date, n=args.predictions)
    print_predictions(candidates, f"ALPHA PICK PREDICTIONS — {score_date}")

    # Strong Buys summary
    strong_buys = [r for r in results if r["rating"] == "Strong Buy"]
    print(f"\n--- All Strong Buys ({len(strong_buys)}) ---")
    for i, sb in enumerate(strong_buys, 1):
        print(f"  {i:>3}. {sb['symbol']:<7} {sb['composite_score']:.3f}  "
              f"V:{sb['value_grade']} G:{sb['growth_grade']} "
              f"P:{sb['profitability_grade']} M:{sb['momentum_grade']}")

    # Short candidates
    shorts = [r for r in results if r["rating"] in ("Strong Sell", "Sell")]
    if shorts:
        print(f"\n--- Short Candidates ({len(shorts)}) ---")
        for r in shorts[:10]:
            print(f"  {r['symbol']:<7} {r['composite_score']:.3f}  {r['rating']}")

    # Save report
    if args.report:
        report = generate_scoring_report(score_date)
        path = save_report(report, f"weekly_screen_{score_date}.txt")
        print(f"\nReport saved to {path}")

    print(f"\nDone. Database: {config.DB_PATH}")


if __name__ == "__main__":
    main()
