#!/usr/bin/env python3
"""
Delta Offset Comparison Runner
==============================
Runs the ThetaData iron condor backtest with multiple call-delta offsets
and prints a side-by-side comparison table.

Offsets tested: 0 (symmetric), -0.03, -0.05, -0.07
Period: 2020-2025

Usage:
    python run_delta_offset_comparison.py
"""

import os
import sys
import time

_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_this_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)
if _this_dir not in sys.path:
    sys.path.insert(0, _this_dir)

from backtest.condor_thetadata import run_backtest, export_csv


OFFSETS = [0.0, -0.03, -0.05, -0.07]
START_YEAR = 2020
END_YEAR = 2025


def summarize(trades):
    """Compute summary stats from a list of trade dicts."""
    if not trades:
        return {
            "trades": 0, "win_rate": 0, "total_pnl": 0, "avg_pnl": 0,
            "put_breaches": 0, "call_breaches": 0, "stop_losses": 0,
        }
    wins = sum(1 for t in trades if t["won"])
    total_pnl = sum(t["pnl"] for t in trades)
    put_breaches = sum(1 for t in trades if t["side_breached"] == "put")
    call_breaches = sum(1 for t in trades if t["side_breached"] == "call")
    stop_losses = sum(1 for t in trades if t["exit_reason"] == "stop_loss")
    return {
        "trades": len(trades),
        "win_rate": wins / len(trades),
        "total_pnl": total_pnl,
        "avg_pnl": total_pnl / len(trades),
        "put_breaches": put_breaches,
        "call_breaches": call_breaches,
        "stop_losses": stop_losses,
    }


def main():
    results = {}
    t0 = time.time()

    for offset in OFFSETS:
        label = f"offset={offset:+.2f}"
        print(f"\n{'='*70}")
        print(f"Running backtest: {label}  ({START_YEAR}-{END_YEAR})")
        print(f"{'='*70}")

        trades = run_backtest(START_YEAR, END_YEAR, call_delta_offset=offset)
        results[offset] = summarize(trades)

        # Export each run to its own CSV
        csv_name = f"condor_{START_YEAR}_{END_YEAR}_offset_{offset:+.2f}.csv"
        csv_path = os.path.join(_this_dir, csv_name)
        export_csv(trades, csv_path)

    elapsed = time.time() - t0

    # Print comparison table
    print(f"\n\n{'='*80}")
    print(f"CALL DELTA OFFSET COMPARISON  --  SPY {START_YEAR}-{END_YEAR}")
    print(f"{'='*80}")
    header = (f"{'Offset':>8} | {'Trades':>7} | {'Win Rate':>9} | "
              f"{'Total P&L':>12} | {'Avg P&L':>10} | "
              f"{'Put Br.':>8} | {'Call Br.':>9} | {'Stop Loss':>10}")
    print(header)
    print("-" * 80)

    for offset in OFFSETS:
        s = results[offset]
        print(f"{offset:>+8.2f} | {s['trades']:>7} | {s['win_rate']:>8.1%} | "
              f"${s['total_pnl']:>+10,.2f} | ${s['avg_pnl']:>+9.2f} | "
              f"{s['put_breaches']:>8} | {s['call_breaches']:>9} | "
              f"{s['stop_losses']:>10}")

    print("-" * 80)
    print(f"\nCompleted all runs in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
