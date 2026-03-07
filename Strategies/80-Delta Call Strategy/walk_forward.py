#!/usr/bin/env python3
"""
Walk-Forward Consistency Analysis for 80-Delta Call Strategy
==============================================================
Runs the full backtest once, then splits the daily snapshots into
rolling windows to verify the strategy performs consistently across
different market regimes (not overfit to a single period).

Windows (default 4-year train / 2-year test, rolling by 1 year):
  2015-2018 / 2019-2020
  2016-2019 / 2020-2021
  2017-2020 / 2021-2022
  2018-2021 / 2022-2023
  2019-2022 / 2023-2024
  2020-2023 / 2025-2025

Usage:
    python walk_forward.py
    python walk_forward.py --train-years 5 --test-years 2
"""

import os
import sys
import time
import math
import logging
import argparse

import numpy as np
import pandas as pd

_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_dir = os.path.dirname(os.path.dirname(_this_dir))
sys.path.insert(0, _project_dir)
sys.path.insert(0, _this_dir)

from backtest.thetadata_client import ThetaDataClient
from delta_capped_backtest import (
    load_all_data, run_delta_capped_simulation, compute_metrics,
    SIM_START,
)

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s  %(message)s",
    datefmt="%H:%M:%S",
)


def compute_window_metrics(snapshots, trade_log, start_date, end_date):
    """Compute metrics for a date-filtered subset of snapshots."""
    window_snaps = [s for s in snapshots
                    if start_date <= s["date"] <= end_date]
    window_trades = [t for t in trade_log
                     if start_date <= t["entry_date"] <= end_date]

    if len(window_snaps) < 20:
        return None

    df = pd.DataFrame(window_snaps)
    n_days = len(df)
    years = n_days / 252.0

    start_val = df["portfolio_value"].iloc[0]
    end_val = df["portfolio_value"].iloc[-1]
    total_return = end_val / start_val - 1
    cagr = (end_val / start_val) ** (1 / years) - 1 if years > 0 else 0

    df["daily_ret"] = df["portfolio_value"].pct_change().fillna(0)
    daily_mean = df["daily_ret"].mean()
    daily_std = df["daily_ret"].std()
    sharpe = (daily_mean / daily_std) * np.sqrt(252) if daily_std > 0 else 0

    downside = df["daily_ret"][df["daily_ret"] < 0]
    ds_std = downside.std() if len(downside) > 0 else 0
    sortino = (daily_mean / ds_std) * np.sqrt(252) if ds_std > 0 else 0

    cummax = df["portfolio_value"].cummax()
    drawdown = df["portfolio_value"] / cummax - 1
    max_dd = drawdown.min()

    n_trades = len(window_trades)
    win_rate = 0
    if n_trades > 0:
        wins = sum(1 for t in window_trades if t.get("pnl_pct", 0) > 0)
        win_rate = wins / n_trades

    return {
        "days": n_days,
        "years": years,
        "n_trades": n_trades,
        "win_rate": win_rate,
        "cagr": cagr,
        "total_return": total_return,
        "sharpe": sharpe,
        "sortino": sortino,
        "max_dd": max_dd,
    }


def generate_windows(first_year, last_year, train_years, test_years):
    """Generate rolling train/test window pairs."""
    windows = []
    for start in range(first_year, last_year - train_years - test_years + 2):
        train_start = f"{start}-01-01"
        train_end = f"{start + train_years - 1}-12-31"
        test_start = f"{start + train_years}-01-01"
        test_end_year = min(start + train_years + test_years - 1, last_year)
        test_end = f"{test_end_year}-12-31"
        windows.append((train_start, train_end, test_start, test_end))
    return windows


def main():
    parser = argparse.ArgumentParser(
        description="Walk-forward consistency analysis for 80-delta strategy")
    parser.add_argument("--train-years", type=int, default=4,
                        help="Training window size in years (default 4)")
    parser.add_argument("--test-years", type=int, default=2,
                        help="Test window size in years (default 2)")
    parser.add_argument("--no-cc", action="store_true",
                        help="Run without covered calls")
    args = parser.parse_args()

    # The 80-delta sim starts at 2015-03-01
    first_year = 2015
    last_year = 2025

    windows = generate_windows(first_year, last_year,
                               args.train_years, args.test_years)

    print("=" * 80)
    print("WALK-FORWARD ANALYSIS  --  80-Delta Call Strategy")
    print(f"  Train: {args.train_years} years,  Test: {args.test_years} years")
    print(f"  Windows: {len(windows)} rolling periods")
    print(f"  Range: {first_year}-{last_year}")
    print("=" * 80)

    # Connect and run full simulation once
    print("\nConnecting to Theta Terminal...")
    client = ThetaDataClient()
    if not client.connect():
        print("ERROR: Cannot connect to Theta Terminal.")
        return

    t0 = time.time()
    spy_by_date, trading_dates, vix_data, sma200, monthly_exps, \
        trailing_12m_returns, rolling_volatility = load_all_data(client)

    label = "no CC" if args.no_cc else "with CC"
    print(f"\nRunning full 80-delta simulation ({label})...")
    snapshots, trade_log, cc_trade_log = run_delta_capped_simulation(
        client, spy_by_date, trading_dates, vix_data, sma200, monthly_exps,
        trailing_12m_returns=trailing_12m_returns,
        rolling_volatility=rolling_volatility,
        force_exit_below_sma=True,
        sell_covered_calls=not args.no_cc,
        label=label,
    )
    client.close()

    if not snapshots:
        print("No snapshots produced.")
        return

    sim_time = time.time() - t0
    print(f"Simulation complete: {len(snapshots)} days, "
          f"{len(trade_log)} trades in {sim_time:.1f}s")

    # Split into windows
    print()
    window_results = []

    for i, (tr_start, tr_end, te_start, te_end) in enumerate(windows, 1):
        print(f"--- Window {i}/{len(windows)}: "
              f"train {tr_start[:4]}-{tr_end[:4]} / "
              f"test {te_start[:4]}-{te_end[:4]} ---")

        is_metrics = compute_window_metrics(snapshots, trade_log,
                                            tr_start, tr_end)
        oos_metrics = compute_window_metrics(snapshots, trade_log,
                                             te_start, te_end)

        if is_metrics:
            print(f"  IN-SAMPLE  ({tr_start[:4]}-{tr_end[:4]}): "
                  f"{is_metrics['n_trades']} trades, "
                  f"CAGR {is_metrics['cagr']:.1%}, "
                  f"Sharpe {is_metrics['sharpe']:.3f}, "
                  f"DD {is_metrics['max_dd']:.1%}")
        else:
            print(f"  IN-SAMPLE  ({tr_start[:4]}-{tr_end[:4]}):  "
                  f"Insufficient data")

        if oos_metrics:
            print(f"  OUT-OF-SAMPLE ({te_start[:4]}-{te_end[:4]}): "
                  f"{oos_metrics['n_trades']} trades, "
                  f"CAGR {oos_metrics['cagr']:.1%}, "
                  f"Sharpe {oos_metrics['sharpe']:.3f}, "
                  f"DD {oos_metrics['max_dd']:.1%}")
        else:
            print(f"  OUT-OF-SAMPLE ({te_start[:4]}-{te_end[:4]}):  "
                  f"Insufficient data")

        window_results.append({
            "window": f"{tr_start[:4]}-{tr_end[:4]}/{te_start[:4]}-{te_end[:4]}",
            "is": is_metrics,
            "oos": oos_metrics,
        })
        print()

    # Aggregate report
    print("=" * 80)
    print("AGGREGATE WALK-FORWARD RESULTS")
    print("=" * 80)

    header = (f"{'Window':<22} | {'IS CAGR':>8} | {'IS Sharpe':>9} | "
              f"{'OOS CAGR':>9} | {'OOS Sharpe':>10} | {'OOS DD':>8} | "
              f"{'Trades':>7}")
    print(header)
    print("-" * len(header))

    oos_sharpes = []
    is_sharpes = []
    oos_cagrs = []

    for wr in window_results:
        w = wr["window"]
        is_m = wr["is"]
        oos_m = wr["oos"]

        is_cagr = f"{is_m['cagr']:.1%}" if is_m else "N/A"
        is_sharpe = f"{is_m['sharpe']:.3f}" if is_m else "N/A"
        oos_cagr = f"{oos_m['cagr']:.1%}" if oos_m else "N/A"
        oos_sharpe = f"{oos_m['sharpe']:.3f}" if oos_m else "N/A"
        oos_dd = f"{oos_m['max_dd']:.1%}" if oos_m else "N/A"
        trades = f"{oos_m['n_trades']}" if oos_m else "0"

        print(f"  {w:<20} | {is_cagr:>8} | {is_sharpe:>9} | "
              f"{oos_cagr:>9} | {oos_sharpe:>10} | {oos_dd:>8} | "
              f"{trades:>7}")

        if oos_m and oos_m["sharpe"] is not None:
            oos_sharpes.append(oos_m["sharpe"])
            oos_cagrs.append(oos_m["cagr"])
        if is_m and is_m["sharpe"] is not None:
            is_sharpes.append(is_m["sharpe"])

    print("-" * len(header))

    if oos_sharpes:
        n = len(oos_sharpes)
        mean_oos_sharpe = sum(oos_sharpes) / n
        mean_is_sharpe = sum(is_sharpes) / len(is_sharpes) if is_sharpes else 0
        mean_oos_cagr = sum(oos_cagrs) / n

        print(f"\n  OOS windows with data: {n}")
        print(f"    Mean OOS CAGR:    {mean_oos_cagr:>8.1%}")
        print(f"    Mean OOS Sharpe:  {mean_oos_sharpe:>8.3f}")
        print(f"    Mean IS Sharpe:   {mean_is_sharpe:>8.3f}")

        if n > 1:
            std_sharpe = (sum((s - mean_oos_sharpe) ** 2
                              for s in oos_sharpes) / (n - 1)) ** 0.5
            ci_lo = mean_oos_sharpe - 1.96 * std_sharpe / n ** 0.5
            ci_hi = mean_oos_sharpe + 1.96 * std_sharpe / n ** 0.5
            print(f"    OOS Sharpe 95% CI: [{ci_lo:.3f}, {ci_hi:.3f}]")

        if mean_is_sharpe > 0:
            decay = 1 - (mean_oos_sharpe / mean_is_sharpe)
            print(f"    Sharpe Decay:     {decay:>8.0%} (IS->OOS)")
            if decay < 0.30:
                print("    -> Low decay: strategy appears robust")
            elif decay < 0.60:
                print("    -> Moderate decay: some overfitting likely")
            else:
                print("    -> High decay: significant overfitting concern")

        pos_sharpe = sum(1 for s in oos_sharpes if s > 0)
        print(f"    Windows with OOS Sharpe > 0: {pos_sharpe}/{n} "
              f"({pos_sharpe/n:.0%})")
    else:
        print("\n  No OOS results to aggregate.")

    print(f"\nWalk-forward analysis completed in {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
