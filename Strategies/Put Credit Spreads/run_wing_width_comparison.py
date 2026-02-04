#!/usr/bin/env python3
"""
Wing Width Comparison with Risk-Adjusted Metrics
=================================================
Compares different wing widths (2%, 3%, 4%, 5%) for put credit spreads.
Reports Sharpe, Probabilistic Sharpe, and Sortino ratios.

Usage:
    python run_wing_width_comparison.py
"""

import os
import sys

_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(os.path.dirname(_this_dir))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)
if _this_dir not in sys.path:
    sys.path.insert(0, _this_dir)

from backtest.put_spread_thetadata import run_backtest, compute_risk_metrics, print_results

START_YEAR = 2012
END_YEAR = 2025
SMA_PERIOD = 200
STOP_LOSS_MULT = 3.0


def main():
    wing_widths = [0.02, 0.03, 0.04, 0.05]
    results = []

    for wwp in wing_widths:
        label = f"{wwp*100:.0f}% wing"
        print(f"\n--- Running: {label}, SMA={SMA_PERIOD}, SL={STOP_LOSS_MULT}x ---")
        trades, sk_sma, sk_iv, sk_oi, sk_cw, sk_data = run_backtest(
            START_YEAR, END_YEAR,
            stop_loss_mult=STOP_LOSS_MULT,
            sma_period=SMA_PERIOD,
            wing_width_pct=wwp,
        )
        n = len(trades)
        if n == 0:
            results.append({"label": label, "n": 0})
            continue

        wins = sum(1 for t in trades if t["won"])
        total_pnl = sum(t["pnl"] for t in trades)
        avg_pnl = total_pnl / n
        avg_credit = sum(t["credit"] for t in trades) / n
        avg_max_loss = sum(t["max_loss"] for t in trades) / n
        avg_width = sum(t["put_width"] for t in trades) / n
        stops = sum(1 for t in trades if t["exit_reason"] == "stop_loss")
        breaches = sum(1 for t in trades if t["side_breached"] == "put")
        m = compute_risk_metrics(trades)

        results.append({
            "label": label,
            "n": n,
            "wins": wins,
            "wr": wins / n,
            "pnl": total_pnl,
            "avg": avg_pnl,
            "cred": avg_credit,
            "ml": avg_max_loss,
            "width": avg_width,
            "stops": stops,
            "breaches": breaches,
            "metrics": m,
        })

    # --- Comparison Table 1: Performance ---
    print()
    print("=" * 100)
    print(f"WING WIDTH COMPARISON  |  SPY Put Spreads  |  {START_YEAR}-{END_YEAR}"
          f"  |  SMA={SMA_PERIOD}  SL={STOP_LOSS_MULT}x")
    print("=" * 100)
    print(f"{'Config':<10} | {'Trades':>6} | {'Win%':>6} | {'Tot P&L':>10}"
          f" | {'Avg P&L':>9} | {'AvgCred':>8} | {'AvgMaxL':>8}"
          f" | {'AvgWid':>6} | {'Stops':>5} | {'PutBr':>5}")
    print("-" * 100)
    for r in results:
        if r["n"] == 0:
            print(f"{r['label']:<10} |    N/A |")
            continue
        print(f"{r['label']:<10} | {r['n']:>6} | {r['wr']*100:>5.1f}%"
              f" | ${r['pnl']:>+9,.0f} | ${r['avg']:>+8.2f}"
              f" | ${r['cred']:>7.3f} | ${r['ml']:>7.0f}"
              f" | ${r['width']:>5.1f} | {r['stops']:>5} | {r['breaches']:>5}")
    print("=" * 100)

    # --- Comparison Table 2: Risk Metrics ---
    print()
    print("=" * 100)
    print("RISK-ADJUSTED METRICS")
    print("=" * 100)
    print(f"{'Config':<10} | {'Sharpe':>7} | {'Shp Ann':>8} | {'PSR':>7}"
          f" | {'Sortino':>8} | {'Srt Ann':>8}"
          f" | {'MeanRet':>8} | {'StdRet':>8} | {'Skew':>7} | {'ExKurt':>7}")
    print("-" * 100)
    for r in results:
        if r["n"] == 0 or r.get("metrics") is None:
            print(f"{r['label']:<10} |    N/A |")
            continue
        m = r["metrics"]
        print(f"{r['label']:<10} | {m['sharpe']:>7.3f} | {m['sharpe_annual']:>8.3f}"
              f" | {m['psr']*100:>6.1f}% | {m['sortino']:>8.3f}"
              f" | {m['sortino_annual']:>8.3f}"
              f" | {m['mean_return']:>8.4f} | {m['std_return']:>8.4f}"
              f" | {m['skewness']:>7.3f} | {m['kurtosis_excess']:>7.3f}")
    print("=" * 100)

    # --- Detailed output for best config ---
    print()
    print("=" * 100)
    print("DETAILED RESULTS FOR EACH WING WIDTH")
    print("=" * 100)
    for wwp in wing_widths:
        label = f"{wwp*100:.0f}% wing"
        trades, sk_sma, sk_iv, sk_oi, sk_cw, sk_data = run_backtest(
            START_YEAR, END_YEAR,
            stop_loss_mult=STOP_LOSS_MULT,
            sma_period=SMA_PERIOD,
            wing_width_pct=wwp,
        )
        print_results(trades, sk_sma, sk_iv, sk_oi, sk_data)


if __name__ == "__main__":
    main()
