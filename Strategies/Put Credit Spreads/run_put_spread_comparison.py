#!/usr/bin/env python3
"""
Put Credit Spread Comparison Runner
=====================================
Runs put_spread_thetadata.py with different parameter combinations
and prints side-by-side comparison tables.

Section 1: SMA Filter Comparison (stop_loss=2.0x fixed)
  - SMA periods: off (0), 100, 150, 200

Section 2: Stop Loss Comparison (SMA=200 fixed)
  - Multipliers: 1.5x, 2.0x, 2.5x, 3.0x

Usage:
    python run_put_spread_comparison.py
"""

import os
import sys
import time

_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(os.path.dirname(_this_dir))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)
if _this_dir not in sys.path:
    sys.path.insert(0, _this_dir)

from backtest.put_spread_thetadata import run_backtest, export_csv

START_YEAR = 2012
END_YEAR = 2025


def summarize(trades, skipped_sma):
    """Extract summary stats from a list of trades."""
    if not trades:
        return {
            "trades": 0, "win_rate": 0, "total_pnl": 0, "avg_pnl": 0,
            "put_breaches": 0, "stop_losses": 0, "skipped_sma": skipped_sma,
        }
    n = len(trades)
    wins = sum(1 for t in trades if t["won"])
    total_pnl = sum(t["pnl"] for t in trades)
    put_breaches = sum(1 for t in trades if t["side_breached"] == "put")
    stop_losses = sum(1 for t in trades if t["exit_reason"] == "stop_loss")
    return {
        "trades": n,
        "win_rate": wins / n,
        "total_pnl": total_pnl,
        "avg_pnl": total_pnl / n,
        "put_breaches": put_breaches,
        "stop_losses": stop_losses,
        "skipped_sma": skipped_sma,
    }


def print_comparison_table(title, rows, row_labels):
    """Print a formatted comparison table."""
    print()
    print("=" * 90)
    print(title)
    print("=" * 90)
    header = (f"{'Config':<16} | {'Trades':>7} | {'Win Rate':>9} | {'Total P&L':>12} | "
              f"{'Avg P&L':>10} | {'Put Br':>7} | {'Stops':>6} | {'SMA Skip':>9}")
    print(header)
    print("-" * 90)

    for label, s in zip(row_labels, rows):
        if s["trades"] == 0:
            print(f"{label:<16} |     N/A |")
            continue
        print(f"{label:<16} | {s['trades']:>7} | {s['win_rate']:>8.1%} | "
              f"${s['total_pnl']:>+10,.2f} | ${s['avg_pnl']:>+9.2f} | "
              f"{s['put_breaches']:>7} | {s['stop_losses']:>6} | {s['skipped_sma']:>9}")

    print("=" * 90)


def main():
    t0 = time.time()

    # ------------------------------------------------------------------
    # Section 1: SMA Filter Comparison (stop_loss=2.0x fixed)
    # ------------------------------------------------------------------
    print("\n" + "#" * 90)
    print("# SECTION 1: SMA FILTER COMPARISON  (stop loss = 2.0x credit)")
    print("#" * 90)

    sma_periods = [0, 100, 150, 200]
    sma_results = []
    sma_labels = []

    for sma in sma_periods:
        label = "SMA OFF" if sma == 0 else f"SMA {sma}"
        print(f"\n--- Running: {label}, SL=2.0x, {START_YEAR}-{END_YEAR} ---")
        trades, skipped_sma, skipped_iv, skipped_oi, skipped_cw, skipped_data = run_backtest(
            START_YEAR, END_YEAR,
            stop_loss_mult=2.0,
            sma_period=sma,
        )
        csv_name = f"put_spread_sma{sma}_sl2.0.csv"
        csv_path = os.path.join(_this_dir, csv_name)
        export_csv(trades, csv_path)

        sma_results.append(summarize(trades, skipped_sma))
        sma_labels.append(label)

    print_comparison_table(
        f"SMA FILTER COMPARISON  |  SPY Put Credit Spreads  |  {START_YEAR}-{END_YEAR}  |  SL=2.0x",
        sma_results, sma_labels,
    )

    # ------------------------------------------------------------------
    # Section 2: Stop Loss Comparison (SMA=200 fixed)
    # ------------------------------------------------------------------
    print("\n" + "#" * 90)
    print("# SECTION 2: STOP LOSS COMPARISON  (SMA = 200-day)")
    print("#" * 90)

    sl_mults = [1.5, 2.0, 2.5, 3.0]
    sl_results = []
    sl_labels = []

    for sl in sl_mults:
        label = f"SL {sl:.1f}x"
        print(f"\n--- Running: SMA=200, {label}, {START_YEAR}-{END_YEAR} ---")
        trades, skipped_sma, skipped_iv, skipped_oi, skipped_cw, skipped_data = run_backtest(
            START_YEAR, END_YEAR,
            stop_loss_mult=sl,
            sma_period=200,
        )
        csv_name = f"put_spread_sma200_sl{sl:.1f}.csv"
        csv_path = os.path.join(_this_dir, csv_name)
        export_csv(trades, csv_path)

        sl_results.append(summarize(trades, skipped_sma))
        sl_labels.append(label)

    print_comparison_table(
        f"STOP LOSS COMPARISON  |  SPY Put Credit Spreads  |  {START_YEAR}-{END_YEAR}  |  SMA=200",
        sl_results, sl_labels,
    )

    elapsed = time.time() - t0
    print(f"\nAll comparisons completed in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
