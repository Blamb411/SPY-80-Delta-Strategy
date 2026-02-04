#!/usr/bin/env python3
"""
IV Rank Threshold & Vol-Scaled Wing Comparison
===============================================
Tests the impact of:
  A) Lowering the IV rank threshold (more trade opportunities)
  B) Volatility-scaled wing widths instead of fixed percentage

Section 1: IV Rank threshold comparison (fixed 5% wing)
Section 2: Vol-scaled wing comparison (fixed IV rank = 30%)
Section 3: Combined A+B (lower IV rank + vol-scaled wings)

Usage:
    python run_iv_volwing_comparison.py
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


def run_config(label, **kwargs):
    """Run a single backtest config and collect summary stats."""
    defaults = dict(
        start_year=START_YEAR, end_year=END_YEAR,
        stop_loss_mult=STOP_LOSS_MULT, sma_period=SMA_PERIOD,
    )
    defaults.update(kwargs)

    print(f"\n--- Running: {label} ---")
    trades, sk_sma, sk_iv, sk_oi, sk_cw, sk_data = run_backtest(**defaults)
    n = len(trades)
    if n == 0:
        return {"label": label, "n": 0, "sk_iv": sk_iv, "sk_cw": sk_cw}

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

    return {
        "label": label, "n": n, "wins": wins, "wr": wins / n,
        "pnl": total_pnl, "avg": avg_pnl,
        "cred": avg_credit, "ml": avg_max_loss, "width": avg_width,
        "avg_cw": avg_cw,
        "stops": stops, "breaches": breaches,
        "sk_iv": sk_iv, "sk_cw": sk_cw,
        "metrics": m,
        "trades": trades,
        "skip_counts": (sk_sma, sk_iv, sk_oi, sk_cw, sk_data),
    }


def print_comparison(title, results):
    """Print comparison tables for a set of results."""
    # --- Performance table ---
    print()
    print("=" * 125)
    print(title)
    print("=" * 125)
    print(f"{'Config':<24} | {'Trd':>4} | {'Win%':>6} | {'Tot P&L':>10}"
          f" | {'Avg P&L':>9} | {'AvgCred':>8} | {'AvgML':>7}"
          f" | {'AvgWid':>6} | {'AvgC/W':>6} | {'SL':>3} | {'IVSkip':>6}")
    print("-" * 125)
    for r in results:
        if r["n"] == 0:
            print(f"{r['label']:<24} |  N/A |  (no trades — {r['sk_iv']} IV-skipped)")
            continue
        print(f"{r['label']:<24} | {r['n']:>4} | {r['wr']*100:>5.1f}%"
              f" | ${r['pnl']:>+9,.0f} | ${r['avg']:>+8.2f}"
              f" | ${r['cred']:>7.3f} | ${r['ml']:>6.0f}"
              f" | ${r['width']:>5.1f} | {r['avg_cw']:>5.1%}"
              f" | {r['stops']:>3} | {r['sk_iv']:>6}")
    print("=" * 125)

    # --- Risk metrics table ---
    print()
    print(f"{'Config':<24} | {'Sharpe':>7} | {'ShpAnn':>7} | {'PSR':>7}"
          f" | {'Sortino':>8} | {'SrtAnn':>7}"
          f" | {'MeanRet':>8} | {'StdRet':>8} | {'Skew':>7} | {'ExKurt':>7}"
          f" | {'Trd/Yr':>6}")
    print("-" * 125)
    for r in results:
        if r["n"] == 0 or r.get("metrics") is None:
            continue
        m = r["metrics"]
        print(f"{r['label']:<24} | {m['sharpe']:>7.3f} | {m['sharpe_annual']:>7.3f}"
              f" | {m['psr']*100:>6.1f}% | {m['sortino']:>8.3f}"
              f" | {m['sortino_annual']:>7.3f}"
              f" | {m['mean_return']:>8.4f} | {m['std_return']:>8.4f}"
              f" | {m['skewness']:>7.3f} | {m['kurtosis_excess']:>7.3f}"
              f" | {m['trades_per_year']:>5.1f}")
    print("=" * 125)


def main():
    all_results = {}

    # ==================================================================
    # SECTION 1: IV Rank Threshold Comparison (fixed 5% wing)
    # ==================================================================
    print("#" * 125)
    print("# SECTION 1: IV Rank Threshold Comparison (wing=5%, SL=3.0x, SMA=200)")
    print("#" * 125)

    iv_configs = [
        ("IV>=30% (baseline)", 0.30),
        ("IV>=25%", 0.25),
        ("IV>=20%", 0.20),
        ("IV>=15%", 0.15),
        ("IV>=10%", 0.10),
    ]

    s1_results = []
    for label, iv_low in iv_configs:
        r = run_config(label, wing_width_pct=0.05, iv_rank_low=iv_low)
        s1_results.append(r)
        all_results[label] = r

    print_comparison(
        f"IV RANK THRESHOLD COMPARISON  |  Wing=5%  |  {START_YEAR}-{END_YEAR}"
        f"  |  SMA={SMA_PERIOD}  SL={STOP_LOSS_MULT}x",
        s1_results,
    )

    # ==================================================================
    # SECTION 2: Vol-Scaled Wing Comparison (fixed IV rank = 30%)
    # ==================================================================
    print()
    print("#" * 125)
    print("# SECTION 2: Vol-Scaled Wing Comparison (IV>=30%, SL=3.0x, SMA=200)")
    print("#" * 125)

    wing_configs = [
        ("5% wing (baseline)", 0.05, 0.0),    # percentage-based
        ("Vol sig=0.50", 0.05, 0.50),          # vol-scaled
        ("Vol sig=0.75", 0.05, 0.75),
        ("Vol sig=1.00", 0.05, 1.00),
        ("Vol sig=1.25", 0.05, 1.25),
    ]

    s2_results = []
    for label, wing_pct, sigma in wing_configs:
        r = run_config(label, wing_width_pct=wing_pct, iv_rank_low=0.30,
                       wing_sigma=sigma)
        s2_results.append(r)
        all_results[label] = r

    print_comparison(
        f"VOL-SCALED WING COMPARISON  |  IV>=30%  |  {START_YEAR}-{END_YEAR}"
        f"  |  SMA={SMA_PERIOD}  SL={STOP_LOSS_MULT}x",
        s2_results,
    )

    # ==================================================================
    # SECTION 3: Combined A+B (lower IV rank + vol-scaled wings)
    # ==================================================================
    print()
    print("#" * 125)
    print("# SECTION 3: Combined — Lower IV Rank + Vol-Scaled Wings")
    print("#" * 125)

    combined_configs = [
        ("IV>=30% 5%wing (base)", 0.30, 0.05, 0.0),
        ("IV>=20% 5%wing", 0.20, 0.05, 0.0),
        ("IV>=20% sig=0.50", 0.20, 0.05, 0.50),
        ("IV>=20% sig=0.75", 0.20, 0.05, 0.75),
        ("IV>=20% sig=1.00", 0.20, 0.05, 1.00),
        ("IV>=15% sig=0.50", 0.15, 0.05, 0.50),
        ("IV>=15% sig=0.75", 0.15, 0.05, 0.75),
    ]

    s3_results = []
    for label, iv_low, wing_pct, sigma in combined_configs:
        r = run_config(label, wing_width_pct=wing_pct, iv_rank_low=iv_low,
                       wing_sigma=sigma)
        s3_results.append(r)
        all_results[label] = r

    print_comparison(
        f"COMBINED: IV RANK + VOL-SCALED WINGS  |  {START_YEAR}-{END_YEAR}"
        f"  |  SMA={SMA_PERIOD}  SL={STOP_LOSS_MULT}x",
        s3_results,
    )

    # ==================================================================
    # Detailed output for best overall config
    # ==================================================================
    valid = [r for r in all_results.values()
             if r["n"] > 0 and r.get("metrics")]
    if valid:
        best = max(valid, key=lambda r: r["metrics"]["sharpe"])
        print(f"\n{'#' * 125}")
        print(f"# DETAILED RESULTS: Best Sharpe config = {best['label']}")
        print(f"{'#' * 125}")
        sc = best["skip_counts"]
        print_results(best["trades"], sc[0], sc[1], sc[2], sc[3], sc[4])


if __name__ == "__main__":
    main()
