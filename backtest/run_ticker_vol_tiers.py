#!/usr/bin/env python3
"""
Ticker-Specific Vol Tier Analysis
===================================
Tests different IV rank floors and ceilings across SPY, QQQ, and IWM.

Section 1: IV rank floor comparison per ticker (no ceiling)
Section 2: IV rank ceiling comparison per ticker (floor=15%)
Section 3: Best floor+ceiling combo per ticker

Usage:
    python run_ticker_vol_tiers.py
"""

import os
import sys
from collections import defaultdict

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
WING_SIGMA = 0.75

TICKERS = ["SPY", "QQQ", "IWM"]


def run_config(ticker, iv_low, iv_high=1.0):
    """Run a single config and return summary."""
    hi_str = f"<={iv_high:.0%}" if iv_high < 1.0 else "nocap"
    label = f"{ticker} IV{iv_low:.0%}-{hi_str}"

    trades, sk_sma, sk_iv, sk_oi, sk_cw, sk_data = run_backtest(
        START_YEAR, END_YEAR,
        stop_loss_mult=STOP_LOSS_MULT,
        sma_period=SMA_PERIOD,
        iv_rank_low=iv_low,
        iv_rank_high=iv_high,
        wing_sigma=WING_SIGMA,
        root=ticker,
    )

    n = len(trades)
    if n == 0:
        return {"label": label, "ticker": ticker, "n": 0,
                "iv_low": iv_low, "iv_high": iv_high,
                "sk_iv": sk_iv, "trades": [],
                "skip_counts": (sk_sma, sk_iv, sk_oi, sk_cw, sk_data)}

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

    # Per-tier breakdown
    tier_stats = {}
    for tier in ("medium", "high", "very_high"):
        tt = [t for t in trades if t["iv_tier"] == tier]
        if tt:
            tw = sum(1 for t in tt if t["won"])
            tp = sum(t["pnl"] for t in tt)
            tier_stats[tier] = {"n": len(tt), "wr": tw/len(tt), "pnl": tp}

    return {
        "label": label, "ticker": ticker, "n": n,
        "iv_low": iv_low, "iv_high": iv_high,
        "wins": wins, "wr": wins / n,
        "pnl": total_pnl, "avg": avg_pnl,
        "cred": avg_credit, "ml": avg_max_loss, "width": avg_width,
        "avg_cw": avg_cw,
        "stops": stops, "breaches": breaches,
        "metrics": m,
        "tier_stats": tier_stats,
        "trades": trades,
        "skip_counts": (sk_sma, sk_iv, sk_oi, sk_cw, sk_data),
    }


def print_table(title, results):
    """Print comparison table."""
    print()
    print("=" * 135)
    print(title)
    print("=" * 135)
    print(f"{'Config':<22} | {'Trd':>4} | {'Win%':>6} | {'Tot P&L':>10}"
          f" | {'Avg P&L':>9} | {'AvgCred':>8} | {'AvgML':>7}"
          f" | {'AvgC/W':>6} | {'SL':>3}"
          f" | {'Sharpe':>7} | {'ShpAnn':>7} | {'SrtAnn':>7} | {'Trd/Yr':>6}")
    print("-" * 135)
    for r in results:
        if r["n"] == 0:
            print(f"{r['label']:<22} |  N/A |  (no trades — {r['sk_iv']} IV-skipped)")
            continue
        m = r.get("metrics")
        sh = m["sharpe"] if m else 0
        sha = m["sharpe_annual"] if m else 0
        srt = m["sortino_annual"] if m else 0
        tpy = m["trades_per_year"] if m else 0
        print(f"{r['label']:<22} | {r['n']:>4} | {r['wr']*100:>5.1f}%"
              f" | ${r['pnl']:>+9,.0f} | ${r['avg']:>+8.2f}"
              f" | ${r['cred']:>7.3f} | ${r['ml']:>6.0f}"
              f" | {r['avg_cw']:>5.1%} | {r['stops']:>3}"
              f" | {sh:>7.3f} | {sha:>7.3f} | {srt:>7.3f} | {tpy:>5.1f}")
    print("=" * 135)


def print_tier_breakdown(results):
    """Print per-IV-tier breakdown for a set of results."""
    print()
    print(f"{'Config':<22} | {'Medium (15-50%)':<25} | {'High (50-70%)':<25} | {'Very High (70%+)':<25}")
    print(f"{'':22} | {'Trd':>4} {'Win%':>6} {'P&L':>12} | {'Trd':>4} {'Win%':>6} {'P&L':>12} | {'Trd':>4} {'Win%':>6} {'P&L':>12}")
    print("-" * 135)
    for r in results:
        if r["n"] == 0:
            continue
        ts = r.get("tier_stats", {})
        parts = [f"{r['label']:<22}"]
        for tier in ("medium", "high", "very_high"):
            if tier in ts:
                s = ts[tier]
                parts.append(f" | {s['n']:>4} {s['wr']*100:>5.1f}% ${s['pnl']:>+10,.0f}")
            else:
                parts.append(f" |  --- {'':>6} {'':>12}")
        print("".join(parts))
    print()


def main():
    all_results = defaultdict(list)

    # ==================================================================
    # SECTION 1: IV Rank Floor per Ticker (no ceiling)
    # ==================================================================
    print("#" * 135)
    print("# SECTION 1: IV Rank Floor Comparison (no ceiling)")
    print("#" * 135)

    iv_floors = [0.10, 0.15, 0.20, 0.25, 0.30]

    for ticker in TICKERS:
        results = []
        for iv_low in iv_floors:
            print(f"\n--- {ticker} IV>={iv_low:.0%} ---")
            r = run_config(ticker, iv_low)
            results.append(r)
            all_results[ticker].append(r)

        print_table(
            f"{ticker}: IV RANK FLOOR COMPARISON  |  sigma={WING_SIGMA}"
            f"  |  SMA={SMA_PERIOD}  SL={STOP_LOSS_MULT}x  |  {START_YEAR}-{END_YEAR}",
            results,
        )
        print_tier_breakdown(results)

    # ==================================================================
    # SECTION 2: IV Rank Ceiling per Ticker (floor=15%)
    # ==================================================================
    print()
    print("#" * 135)
    print("# SECTION 2: IV Rank Ceiling Comparison (floor=15%)")
    print("#" * 135)

    iv_ceilings = [0.40, 0.50, 0.60, 0.70, 1.0]

    for ticker in TICKERS:
        results = []
        for iv_high in iv_ceilings:
            hi_label = f"<={iv_high:.0%}" if iv_high < 1.0 else "nocap"
            print(f"\n--- {ticker} IV>=15% {hi_label} ---")
            r = run_config(ticker, 0.15, iv_high)
            results.append(r)

        print_table(
            f"{ticker}: IV RANK CEILING COMPARISON (floor=15%)  |  sigma={WING_SIGMA}"
            f"  |  SMA={SMA_PERIOD}  SL={STOP_LOSS_MULT}x  |  {START_YEAR}-{END_YEAR}",
            results,
        )
        print_tier_breakdown(results)

    # ==================================================================
    # SECTION 3: Best configs per ticker
    # ==================================================================
    print()
    print("#" * 135)
    print("# SECTION 3: Promising Floor+Ceiling Combinations")
    print("#" * 135)

    combos = [
        (0.15, 1.0),   # baseline
        (0.15, 0.50),  # cap at medium tier
        (0.15, 0.70),  # cap before very_high
        (0.20, 0.50),  # tighter band
        (0.20, 0.70),  # moderate band
        (0.10, 0.50),  # wide floor, tight ceiling
    ]

    for ticker in TICKERS:
        results = []
        for iv_low, iv_high in combos:
            hi_label = f"<={iv_high:.0%}" if iv_high < 1.0 else "nocap"
            print(f"\n--- {ticker} IV>={iv_low:.0%} {hi_label} ---")
            r = run_config(ticker, iv_low, iv_high)
            results.append(r)

        print_table(
            f"{ticker}: FLOOR+CEILING COMBINATIONS  |  sigma={WING_SIGMA}"
            f"  |  SMA={SMA_PERIOD}  SL={STOP_LOSS_MULT}x  |  {START_YEAR}-{END_YEAR}",
            results,
        )

    # ==================================================================
    # SECTION 4: Detailed output for best Sharpe per ticker
    # ==================================================================
    print()
    print("#" * 135)
    print("# DETAILED OUTPUT: Best Sharpe config per ticker")
    print("#" * 135)

    # We need to re-run or use cached results for the best configs
    # Collect all results we ran in Section 3
    # (they're the last set of results for each ticker)


if __name__ == "__main__":
    main()
