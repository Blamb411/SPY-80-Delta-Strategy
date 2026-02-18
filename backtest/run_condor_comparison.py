#!/usr/bin/env python3
"""
Condor Parameter Comparison
============================
Compares different parameter combinations for put_only mode:
- Condor params (45 DTE, 6% wings, 25% TP, VIX >= 18)
- PCS-like params (30 DTE, 3% wings, 50% TP, no VIX floor)
- Various hybrid combinations

Also tests regime mode (puts above SMA, calls below SMA)
and call_only mode for comparison.

Usage:
    python run_condor_comparison.py
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

from condor_thetadata import run_backtest

START_YEAR = 2012
END_YEAR = 2025


def run_config(label, **kwargs):
    """Run a single configuration and return summary dict."""
    print(f"\n{'='*70}")
    print(f"RUNNING: {label}")
    print(f"{'='*70}")
    t0 = time.time()
    trades, sk_sma, sk_iv, sk_vix, sk_data = run_backtest(
        START_YEAR, END_YEAR, **kwargs
    )
    elapsed = time.time() - t0
    n = len(trades)
    if n == 0:
        return {"label": label, "n": 0, "elapsed": elapsed}

    wins = sum(1 for t in trades if t["won"])
    total_pnl = sum(t["pnl"] for t in trades)
    avg_pnl = total_pnl / n
    avg_credit = sum(t["credit"] for t in trades) / n
    avg_max_loss = sum(t["max_loss"] for t in trades) / n
    stops = sum(1 for t in trades if t["exit_reason"] == "stop_loss")
    tp = sum(1 for t in trades if t["exit_reason"] == "take_profit")
    time_exits = sum(1 for t in trades if t["exit_reason"] == "time_exit")
    costs = sum(t.get("transaction_costs", 0) for t in trades)

    # Mode breakdown for regime
    put_trades = [t for t in trades if t.get("mode") == "put_only"]
    call_trades = [t for t in trades if t.get("mode") == "call_only"]

    return {
        "label": label,
        "n": n,
        "wins": wins,
        "wr": wins / n,
        "pnl": total_pnl,
        "avg": avg_pnl,
        "cred": avg_credit,
        "ml": avg_max_loss,
        "stops": stops,
        "tp": tp,
        "time_exits": time_exits,
        "costs": costs,
        "put_n": len(put_trades),
        "call_n": len(call_trades),
        "put_pnl": sum(t["pnl"] for t in put_trades) if put_trades else 0,
        "call_pnl": sum(t["pnl"] for t in call_trades) if call_trades else 0,
        "elapsed": elapsed,
    }


def main():
    configs = [
        # 1. Condor params put_only (baseline - already ran)
        ("Put-Only Condor",
         dict(mode="put_only", wing_width_pct=0.06, take_profit_pct=0.25,
              vix_abs_floor=18, dte_target=45, dte_exit=21)),

        # 2. PCS-like params: 30 DTE, 3% wings, 50% TP, no VIX floor
        ("Put-Only PCS-like",
         dict(mode="put_only", wing_width_pct=0.03, take_profit_pct=0.50,
              vix_abs_floor=0, dte_target=30, dte_exit=0,
              dte_min=25, dte_max=40)),

        # 3. Hybrid: 45 DTE + 3% wings + 50% TP + VIX >= 18
        ("Put-Only 45D/3%/50%TP",
         dict(mode="put_only", wing_width_pct=0.03, take_profit_pct=0.50,
              vix_abs_floor=18, dte_target=45, dte_exit=21)),

        # 4. Hybrid: 30 DTE + 6% wings + 25% TP + VIX >= 18
        ("Put-Only 30D/6%/25%TP",
         dict(mode="put_only", wing_width_pct=0.06, take_profit_pct=0.25,
              vix_abs_floor=18, dte_target=30, dte_exit=0,
              dte_min=25, dte_max=40)),

        # 5. Hybrid: 45 DTE + 6% wings + 50% TP + VIX >= 18
        ("Put-Only 45D/6%/50%TP",
         dict(mode="put_only", wing_width_pct=0.06, take_profit_pct=0.50,
              vix_abs_floor=18, dte_target=45, dte_exit=21)),

        # 6. No VIX floor: 45 DTE + 6% wings + 25% TP + no VIX floor
        ("Put-Only NoVIX",
         dict(mode="put_only", wing_width_pct=0.06, take_profit_pct=0.25,
              vix_abs_floor=0, dte_target=45, dte_exit=21)),

        # 7. Call-only for comparison
        ("Call-Only Condor",
         dict(mode="call_only", wing_width_pct=0.06, take_profit_pct=0.25,
              vix_abs_floor=18, dte_target=45, dte_exit=21)),

        # 8. Regime mode: puts above SMA, calls below SMA
        ("Regime (P>SMA/C<SMA)",
         dict(mode="regime", wing_width_pct=0.06, take_profit_pct=0.25,
              vix_abs_floor=18, dte_target=45, dte_exit=21)),

        # 9. Original iron condor
        ("Iron Condor",
         dict(mode="condor", wing_width_pct=0.06, take_profit_pct=0.25,
              vix_abs_floor=18, dte_target=45, dte_exit=21)),
    ]

    results = []
    for label, kwargs in configs:
        r = run_config(label, **kwargs)
        results.append(r)

    # Print comparison table
    print()
    print("=" * 130)
    print(f"PARAMETER COMPARISON  |  SPY  |  {START_YEAR}-{END_YEAR}")
    print("=" * 130)
    print(f"{'Config':<24} | {'Trades':>6} | {'Win%':>6} | {'Tot P&L':>10}"
          f" | {'Avg P&L':>9} | {'AvgCred':>8} | {'AvgMaxL':>8}"
          f" | {'SL':>3} | {'TP':>3} | {'TimEx':>5} | {'Costs':>7} | {'Time':>5}")
    print("-" * 130)
    for r in results:
        if r["n"] == 0:
            print(f"{r['label']:<24} |    N/A |")
            continue
        print(f"{r['label']:<24} | {r['n']:>6} | {r['wr']*100:>5.1f}%"
              f" | ${r['pnl']:>+9,.0f} | ${r['avg']:>+8.2f}"
              f" | ${r['cred']:>7.3f} | ${r['ml']:>7.0f}"
              f" | {r['stops']:>3} | {r['tp']:>3} | {r['time_exits']:>5}"
              f" | ${r['costs']:>6,.0f} | {r['elapsed']:>4.0f}s")
    print("=" * 130)

    # Regime breakdown
    regime_results = [r for r in results if r.get("put_n", 0) + r.get("call_n", 0) > 0]
    if regime_results:
        print()
        print("=" * 80)
        print("REGIME MODE BREAKDOWN")
        print("=" * 80)
        for r in regime_results:
            if r["put_n"] > 0:
                print(f"  {r['label']} - PUT trades:  {r['put_n']:>3}  P&L: ${r['put_pnl']:>+9,.0f}")
            if r["call_n"] > 0:
                print(f"  {r['label']} - CALL trades: {r['call_n']:>3}  P&L: ${r['call_pnl']:>+9,.0f}")


if __name__ == "__main__":
    main()
