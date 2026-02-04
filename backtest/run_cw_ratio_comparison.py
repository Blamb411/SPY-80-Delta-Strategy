#!/usr/bin/env python3
"""
Credit/Width Ratio Comparison
===============================
Tests the impact of enforcing a minimum credit-to-width ratio.
When enabled, the backtester dynamically narrows the wing from the
initial percentage-based width until credit/width >= target.

Section 1: C/W ratios at 5% max wing width (best prior config)
Section 2: C/W ratios at 3% max wing width (baseline)

Usage:
    python run_cw_ratio_comparison.py
"""

import os
import sys

_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_this_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)
if _this_dir not in sys.path:
    sys.path.insert(0, _this_dir)

from backtest.put_spread_thetadata import run_backtest, compute_risk_metrics, print_results

START_YEAR = 2012
END_YEAR = 2025
SMA_PERIOD = 200
STOP_LOSS_MULT = 3.0


def run_section(title, wing_pct, cw_ratios):
    """Run one section of the comparison."""
    results = []

    for cw in cw_ratios:
        label = f"C/W OFF" if cw == 0 else f"C/W>={cw:.0%}"
        print(f"\n--- Running: wing={wing_pct*100:.0f}%, {label}, SMA={SMA_PERIOD}, SL={STOP_LOSS_MULT}x ---")
        trades, sk_sma, sk_iv, sk_oi, sk_cw, sk_data = run_backtest(
            START_YEAR, END_YEAR,
            stop_loss_mult=STOP_LOSS_MULT,
            sma_period=SMA_PERIOD,
            wing_width_pct=wing_pct,
            min_credit_width_ratio=cw,
        )
        n = len(trades)
        if n == 0:
            results.append({"label": label, "n": 0, "sk_cw": sk_cw})
            continue

        wins = sum(1 for t in trades if t["won"])
        total_pnl = sum(t["pnl"] for t in trades)
        avg_pnl = total_pnl / n
        avg_credit = sum(t["credit"] for t in trades) / n
        avg_max_loss = sum(t["max_loss"] for t in trades) / n
        avg_width = sum(t["put_width"] for t in trades) / n
        avg_cw = sum(t.get("credit_width_ratio", 0) for t in trades) / n
        stops = sum(1 for t in trades if t["exit_reason"] == "stop_loss")
        breaches = sum(1 for t in trades if t["side_breached"] == "put")
        m = compute_risk_metrics(trades)

        results.append({
            "label": label, "n": n, "wins": wins, "wr": wins / n,
            "pnl": total_pnl, "avg": avg_pnl,
            "cred": avg_credit, "ml": avg_max_loss, "width": avg_width,
            "avg_cw": avg_cw,
            "stops": stops, "breaches": breaches,
            "sk_cw": sk_cw,
            "metrics": m,
        })

    # --- Performance table ---
    print()
    print("=" * 115)
    print(title)
    print("=" * 115)
    print(f"{'Config':<10} | {'Trd':>4} | {'Win%':>6} | {'Tot P&L':>10}"
          f" | {'Avg P&L':>9} | {'AvgCred':>8} | {'AvgML':>7}"
          f" | {'AvgWid':>6} | {'AvgC/W':>6} | {'SL':>3} | {'CWSkip':>6}")
    print("-" * 115)
    for r in results:
        if r["n"] == 0:
            print(f"{r['label']:<10} |  N/A | (all {r['sk_cw']} entries blocked by C/W filter)")
            continue
        print(f"{r['label']:<10} | {r['n']:>4} | {r['wr']*100:>5.1f}%"
              f" | ${r['pnl']:>+9,.0f} | ${r['avg']:>+8.2f}"
              f" | ${r['cred']:>7.3f} | ${r['ml']:>6.0f}"
              f" | ${r['width']:>5.1f} | {r['avg_cw']:>5.1%}"
              f" | {r['stops']:>3} | {r['sk_cw']:>6}")
    print("=" * 115)

    # --- Risk metrics table ---
    print()
    print(f"{'Config':<10} | {'Sharpe':>7} | {'ShpAnn':>7} | {'PSR':>7}"
          f" | {'Sortino':>8} | {'SrtAnn':>7}"
          f" | {'MeanRet':>8} | {'StdRet':>8} | {'Skew':>7} | {'ExKurt':>7}")
    print("-" * 115)
    for r in results:
        if r["n"] == 0 or r.get("metrics") is None:
            continue
        m = r["metrics"]
        print(f"{r['label']:<10} | {m['sharpe']:>7.3f} | {m['sharpe_annual']:>7.3f}"
              f" | {m['psr']*100:>6.1f}% | {m['sortino']:>8.3f}"
              f" | {m['sortino_annual']:>7.3f}"
              f" | {m['mean_return']:>8.4f} | {m['std_return']:>8.4f}"
              f" | {m['skewness']:>7.3f} | {m['kurtosis_excess']:>7.3f}")
    print("=" * 115)

    # --- Detailed output for best C/W config ---
    # Find the config with highest Sharpe (excluding n=0)
    valid = [r for r in results if r["n"] > 0 and r.get("metrics")]
    if valid:
        best = max(valid, key=lambda r: r["metrics"]["sharpe"])
        print(f"\nDetailed results for best Sharpe config: {best['label']}")
        trades, sk_sma, sk_iv, sk_oi, sk_cw, sk_data = run_backtest(
            START_YEAR, END_YEAR,
            stop_loss_mult=STOP_LOSS_MULT,
            sma_period=SMA_PERIOD,
            wing_width_pct=wing_pct,
            min_credit_width_ratio=([cw for cw, r in zip(cw_ratios, results)
                                     if r is best][0]),
        )
        print_results(trades, sk_sma, sk_iv, sk_oi, sk_cw, sk_data)


def main():
    cw_ratios = [0.0, 0.05, 0.10, 0.15, 0.20, 0.33]

    print("#" * 115)
    print("# SECTION 1: Credit/Width Ratio Comparison at 5% Max Wing Width")
    print("#" * 115)
    run_section(
        f"C/W RATIO COMPARISON  |  Wing=5% max  |  {START_YEAR}-{END_YEAR}"
        f"  |  SMA={SMA_PERIOD}  SL={STOP_LOSS_MULT}x",
        wing_pct=0.05,
        cw_ratios=cw_ratios,
    )

    print()
    print("#" * 115)
    print("# SECTION 2: Credit/Width Ratio Comparison at 3% Max Wing Width")
    print("#" * 115)
    run_section(
        f"C/W RATIO COMPARISON  |  Wing=3% max  |  {START_YEAR}-{END_YEAR}"
        f"  |  SMA={SMA_PERIOD}  SL={STOP_LOSS_MULT}x",
        wing_pct=0.03,
        cw_ratios=cw_ratios,
    )


if __name__ == "__main__":
    main()
