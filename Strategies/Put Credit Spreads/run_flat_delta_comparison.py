#!/usr/bin/env python3
"""
Flat Delta vs Tier-Based Delta Comparison
==========================================
Tests flat delta=0.20 against tier-based delta (0.20/0.25/0.30)
across SPY and QQQ.

Section 1: SPY — flat 0.20 vs tier-based, with and without IV ceiling
Section 2: QQQ — flat 0.20 vs tier-based, with and without IV ceiling
Section 3: Side-by-side summary

Usage:
    python run_flat_delta_comparison.py
"""

import os
import sys
from collections import defaultdict

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
WING_SIGMA = 0.75

TICKERS = ["SPY", "QQQ"]


def run_config(ticker, flat_delta=0.0, iv_low=0.15, iv_high=1.0):
    """Run a single config and return summary."""
    delta_str = f"flat{flat_delta:.2f}" if flat_delta > 0 else "tiered"
    hi_str = f"cap{iv_high:.0%}" if iv_high < 1.0 else "nocap"
    label = f"{ticker} {delta_str} {hi_str}"

    trades, sk_sma, sk_iv, sk_oi, sk_cw, sk_data = run_backtest(
        START_YEAR, END_YEAR,
        stop_loss_mult=STOP_LOSS_MULT,
        sma_period=SMA_PERIOD,
        iv_rank_low=iv_low,
        iv_rank_high=iv_high,
        flat_delta=flat_delta,
        wing_sigma=WING_SIGMA,
        root=ticker,
    )

    n = len(trades)
    if n == 0:
        return {"label": label, "ticker": ticker, "n": 0,
                "flat_delta": flat_delta, "iv_high": iv_high,
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
    tp = sum(1 for t in trades if t["exit_reason"] == "take_profit")
    breaches = sum(1 for t in trades if t["side_breached"] == "put")
    m = compute_risk_metrics(trades)

    # Per-tier breakdown
    tier_stats = {}
    for tier in ("medium", "high", "very_high"):
        tt = [t for t in trades if t["iv_tier"] == tier]
        if tt:
            tw = sum(1 for t in tt if t["won"])
            tp_tier = sum(t["pnl"] for t in tt)
            ts_stops = sum(1 for t in tt if t["exit_reason"] == "stop_loss")
            tier_stats[tier] = {"n": len(tt), "wr": tw/len(tt), "pnl": tp_tier,
                                "stops": ts_stops}

    # Yearly
    yearly = defaultdict(lambda: {"trades": 0, "pnl": 0.0, "wins": 0})
    for t in trades:
        yr = t["entry_date"][:4]
        yearly[yr]["trades"] += 1
        yearly[yr]["pnl"] += t["pnl"]
        if t["won"]:
            yearly[yr]["wins"] += 1

    return {
        "label": label, "ticker": ticker, "n": n,
        "flat_delta": flat_delta, "iv_high": iv_high,
        "wins": wins, "wr": wins / n,
        "pnl": total_pnl, "avg": avg_pnl,
        "cred": avg_credit, "ml": avg_max_loss, "width": avg_width,
        "avg_cw": avg_cw,
        "stops": stops, "tp": tp, "breaches": breaches,
        "metrics": m,
        "tier_stats": tier_stats,
        "yearly": dict(yearly),
        "trades": trades,
        "skip_counts": (sk_sma, sk_iv, sk_oi, sk_cw, sk_data),
    }


def print_table(title, results):
    """Print comparison table."""
    print()
    print("=" * 140)
    print(title)
    print("=" * 140)
    print(f"{'Config':<26} | {'Trd':>4} | {'Win%':>6} | {'Tot P&L':>10}"
          f" | {'Avg P&L':>9} | {'AvgCred':>8} | {'AvgML':>7}"
          f" | {'AvgC/W':>6} | {'TP':>4} | {'SL':>3}"
          f" | {'Sharpe':>7} | {'ShpAnn':>7} | {'SrtAnn':>7} | {'Trd/Yr':>6}")
    print("-" * 140)
    for r in results:
        if r["n"] == 0:
            print(f"{r['label']:<26} |  N/A |  (no trades — {r['sk_iv']} IV-skipped)")
            continue
        m = r.get("metrics")
        sh = m["sharpe"] if m else 0
        sha = m["sharpe_annual"] if m else 0
        srt = m["sortino_annual"] if m else 0
        tpy = m["trades_per_year"] if m else 0
        print(f"{r['label']:<26} | {r['n']:>4} | {r['wr']*100:>5.1f}%"
              f" | ${r['pnl']:>+9,.0f} | ${r['avg']:>+8.2f}"
              f" | ${r['cred']:>7.3f} | ${r['ml']:>6.0f}"
              f" | {r['avg_cw']:>5.1%} | {r['tp']:>4} | {r['stops']:>3}"
              f" | {sh:>7.3f} | {sha:>7.3f} | {srt:>7.3f} | {tpy:>5.1f}")
    print("=" * 140)


def print_tier_breakdown(results):
    """Print per-IV-tier breakdown."""
    print()
    print(f"{'Config':<26} | {'Medium (15-50%)':<28} | {'High (50-70%)':<28} | {'Very High (70%+)':<28}")
    print(f"{'':26} | {'Trd':>4} {'Win%':>6} {'P&L':>10} {'SL':>3} | {'Trd':>4} {'Win%':>6} {'P&L':>10} {'SL':>3} | {'Trd':>4} {'Win%':>6} {'P&L':>10} {'SL':>3}")
    print("-" * 140)
    for r in results:
        if r["n"] == 0:
            continue
        ts = r.get("tier_stats", {})
        parts = [f"{r['label']:<26}"]
        for tier in ("medium", "high", "very_high"):
            if tier in ts:
                s = ts[tier]
                parts.append(f" | {s['n']:>4} {s['wr']*100:>5.1f}% ${s['pnl']:>+8,.0f} {s['stops']:>3}")
            else:
                parts.append(f" |  --- {'':>6} {'':>10} {'':>3}")
        print("".join(parts))
    print()


def print_yearly_comparison(title, results):
    """Print year-over-year for a set of results."""
    print()
    print("=" * 100)
    print(title)
    print("=" * 100)

    all_years = set()
    for r in results:
        if r.get("yearly"):
            all_years.update(r["yearly"].keys())
    all_years = sorted(all_years)

    # Header
    header = f"{'Year':<6}"
    for r in results:
        delta_str = f"flat{r['flat_delta']:.2f}" if r['flat_delta'] > 0 else "tiered"
        header += f" | {delta_str:>14}"
    print(header)
    print("-" * 100)

    for yr in all_years:
        row = f"{yr:<6}"
        for r in results:
            if r.get("yearly") and yr in r["yearly"]:
                yr_pnl = r["yearly"][yr]["pnl"]
                row += f" | ${yr_pnl:>+12,.0f}"
            else:
                row += f" | {'---':>14}"
        print(row)

    print("-" * 100)
    row = f"{'TOTAL':<6}"
    for r in results:
        row += f" | ${r['pnl']:>+12,.0f}" if r["n"] > 0 else f" | {'---':>14}"
    print(row)
    print("=" * 100)


def main():
    all_results = {}

    # ==================================================================
    # For each ticker, test:
    #   1) Tier-based delta, no IV ceiling
    #   2) Flat delta 0.20, no IV ceiling
    #   3) Tier-based delta, IV ceiling 70%
    #   4) Flat delta 0.20, IV ceiling 70%
    # ==================================================================

    configs = [
        # (flat_delta, iv_high, description)
        (0.0,  1.0,  "tier-based, no cap"),
        (0.20, 1.0,  "flat 0.20, no cap"),
        (0.0,  0.70, "tier-based, cap 70%"),
        (0.20, 0.70, "flat 0.20, cap 70%"),
    ]

    for ticker in TICKERS:
        print()
        print("#" * 140)
        print(f"# {ticker}: FLAT DELTA vs TIER-BASED DELTA COMPARISON")
        print("#" * 140)

        results = []
        for flat_delta, iv_high, desc in configs:
            delta_str = f"flat{flat_delta:.2f}" if flat_delta > 0 else "tiered"
            hi_str = f"cap{iv_high:.0%}" if iv_high < 1.0 else "nocap"
            print(f"\n--- {ticker} {delta_str} {hi_str} ({desc}) ---")
            r = run_config(ticker, flat_delta=flat_delta, iv_high=iv_high)
            results.append(r)

        all_results[ticker] = results

        print_table(
            f"{ticker}: FLAT DELTA vs TIER-BASED  |  IV>={0.15:.0%}  |  sigma={WING_SIGMA}"
            f"  |  SMA={SMA_PERIOD}  SL={STOP_LOSS_MULT}x  |  {START_YEAR}-{END_YEAR}",
            results,
        )
        print_tier_breakdown(results)
        print_yearly_comparison(
            f"{ticker}: YEAR-OVER-YEAR P&L BY DELTA METHOD",
            results,
        )

    # ==================================================================
    # CROSS-TICKER SUMMARY
    # ==================================================================
    print()
    print("#" * 140)
    print("# CROSS-TICKER SUMMARY: Best config per ticker")
    print("#" * 140)

    summary = []
    for ticker in TICKERS:
        for r in all_results[ticker]:
            if r["n"] > 0:
                summary.append(r)

    print_table(
        f"ALL CONFIGS SIDE-BY-SIDE  |  IV>={0.15:.0%}  |  sigma={WING_SIGMA}"
        f"  |  SMA={SMA_PERIOD}  SL={STOP_LOSS_MULT}x  |  {START_YEAR}-{END_YEAR}",
        summary,
    )

    # ==================================================================
    # COMBINED PORTFOLIO: tier-based vs flat for SPY+QQQ
    # ==================================================================
    print()
    print("#" * 140)
    print("# COMBINED PORTFOLIO: SPY + QQQ (tier-based vs flat delta)")
    print("#" * 140)

    for config_idx, (flat_delta, iv_high, desc) in enumerate(configs):
        combined_trades = []
        for ticker in TICKERS:
            combined_trades.extend(all_results[ticker][config_idx]["trades"])

        if not combined_trades:
            continue

        n = len(combined_trades)
        total_pnl = sum(t["pnl"] for t in combined_trades)
        total_wins = sum(1 for t in combined_trades if t["won"])
        m = compute_risk_metrics(combined_trades)

        delta_str = f"flat{flat_delta:.2f}" if flat_delta > 0 else "tiered"
        hi_str = f"cap{iv_high:.0%}" if iv_high < 1.0 else "nocap"
        print(f"\n  {delta_str} {hi_str} ({desc}):")
        print(f"    Trades:           {n}")
        print(f"    Win rate:         {total_wins/n:.1%}")
        print(f"    Total P&L:        ${total_pnl:>+,.2f}")
        print(f"    Avg P&L/trade:    ${total_pnl/n:>+,.2f}")
        if m:
            print(f"    Sharpe (ann):     {m['sharpe_annual']:.3f}")
            print(f"    Sortino (ann):    {m['sortino_annual']:.3f}")
            print(f"    PSR:              {m['psr']:.1%}")
            print(f"    Trades/year:      {m['trades_per_year']:.1f}")

    # ==================================================================
    # DETAILED OUTPUT for best configs
    # ==================================================================
    print()
    print("#" * 140)
    print("# DETAILED OUTPUT: Flat delta 0.20 nocap per ticker")
    print("#" * 140)

    for ticker in TICKERS:
        # Flat delta nocap is index 1
        r = all_results[ticker][1]
        if r["n"] > 0:
            print()
            print("#" * 100)
            print(f"# {ticker} — flat delta 0.20, no IV cap")
            print("#" * 100)
            sc = r["skip_counts"]
            print_results(r["trades"], sc[0], sc[1], sc[2], sc[3], sc[4])


if __name__ == "__main__":
    main()
