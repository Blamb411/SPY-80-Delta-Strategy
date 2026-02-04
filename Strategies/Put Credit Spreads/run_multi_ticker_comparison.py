#!/usr/bin/env python3
"""
Multi-Ticker Put Credit Spread Comparison
==========================================
Tests the put credit spread strategy across multiple tickers:
  SPY (S&P 500), QQQ (Nasdaq 100), IWM (Russell 2000), DIA (Dow Jones)

All use the same parameters: IV>=15%, sigma=0.75, SMA=200, SL=3.0x
VIX is used as the volatility signal for all tickers (correlated proxy).

Usage:
    python run_multi_ticker_comparison.py
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
IV_RANK_LOW = 0.15
WING_SIGMA = 0.75


def run_ticker(ticker):
    """Run the strategy for a single ticker and return summary stats."""
    print(f"\n{'='*70}")
    print(f"  Running: {ticker}")
    print(f"{'='*70}")

    trades, sk_sma, sk_iv, sk_oi, sk_cw, sk_data = run_backtest(
        START_YEAR, END_YEAR,
        stop_loss_mult=STOP_LOSS_MULT,
        sma_period=SMA_PERIOD,
        iv_rank_low=IV_RANK_LOW,
        wing_sigma=WING_SIGMA,
        root=ticker,
    )

    n = len(trades)
    if n == 0:
        return {
            "ticker": ticker, "n": 0,
            "sk_sma": sk_sma, "sk_iv": sk_iv, "sk_data": sk_data,
            "trades": [], "skip_counts": (sk_sma, sk_iv, sk_oi, sk_cw, sk_data),
        }

    wins = sum(1 for t in trades if t["won"])
    total_pnl = sum(t["pnl"] for t in trades)
    avg_pnl = total_pnl / n
    avg_credit = sum(t["credit"] for t in trades) / n
    avg_max_loss = sum(t["max_loss"] for t in trades) / n
    avg_width = sum(t["put_width"] for t in trades) / n
    avg_cw = sum(t.get("credit_width_ratio", 0) for t in trades) / n
    stops = sum(1 for t in trades if t["exit_reason"] == "stop_loss")
    breaches = sum(1 for t in trades if t["side_breached"] == "put")
    tp = sum(1 for t in trades if t["exit_reason"] == "take_profit")
    exp = sum(1 for t in trades
              if t["exit_reason"] in ("expiration", "expiration_fallback"))
    m = compute_risk_metrics(trades)

    # Year-over-year
    from collections import defaultdict
    yearly = defaultdict(lambda: {"trades": 0, "pnl": 0.0, "wins": 0})
    for t in trades:
        yr = t["entry_date"][:4]
        yearly[yr]["trades"] += 1
        yearly[yr]["pnl"] += t["pnl"]
        if t["won"]:
            yearly[yr]["wins"] += 1

    return {
        "ticker": ticker, "n": n, "wins": wins, "wr": wins / n,
        "pnl": total_pnl, "avg": avg_pnl,
        "cred": avg_credit, "ml": avg_max_loss, "width": avg_width,
        "avg_cw": avg_cw,
        "stops": stops, "breaches": breaches, "tp": tp, "exp": exp,
        "metrics": m,
        "trades": trades,
        "skip_counts": (sk_sma, sk_iv, sk_oi, sk_cw, sk_data),
        "yearly": dict(yearly),
    }


def main():
    tickers = ["SPY", "QQQ", "IWM", "DIA"]
    results = []

    for ticker in tickers:
        r = run_ticker(ticker)
        results.append(r)

    # ===================================================================
    # COMPARISON TABLE 1: Performance
    # ===================================================================
    print()
    print("#" * 130)
    print(f"# MULTI-TICKER COMPARISON  |  IV>={IV_RANK_LOW:.0%}  |  sigma={WING_SIGMA}"
          f"  |  SMA={SMA_PERIOD}  |  SL={STOP_LOSS_MULT}x  |  {START_YEAR}-{END_YEAR}")
    print("#" * 130)

    print()
    print("=" * 130)
    print("PERFORMANCE COMPARISON")
    print("=" * 130)
    print(f"{'Ticker':<8} | {'Trd':>4} | {'Win%':>6} | {'Tot P&L':>10}"
          f" | {'Avg P&L':>9} | {'AvgCred':>8} | {'AvgML':>8}"
          f" | {'AvgWid':>7} | {'AvgC/W':>6}"
          f" | {'TP':>4} | {'SL':>4} | {'Exp':>4} | {'PutBr':>5}")
    print("-" * 130)
    for r in results:
        if r["n"] == 0:
            print(f"{r['ticker']:<8} |  N/A |  (no trades — SMA:{r['sk_sma']} IV:{r['sk_iv']} data:{r['sk_data']})")
            continue
        print(f"{r['ticker']:<8} | {r['n']:>4} | {r['wr']*100:>5.1f}%"
              f" | ${r['pnl']:>+9,.0f} | ${r['avg']:>+8.2f}"
              f" | ${r['cred']:>7.3f} | ${r['ml']:>7.0f}"
              f" | ${r['width']:>6.1f} | {r['avg_cw']:>5.1%}"
              f" | {r['tp']:>4} | {r['stops']:>4} | {r['exp']:>4} | {r['breaches']:>5}")
    print("=" * 130)

    # ===================================================================
    # COMPARISON TABLE 2: Risk Metrics
    # ===================================================================
    print()
    print("=" * 130)
    print("RISK-ADJUSTED METRICS  (returns = P&L / max_loss per trade)")
    print("=" * 130)
    print(f"{'Ticker':<8} | {'Sharpe':>7} | {'ShpAnn':>7} | {'PSR':>7}"
          f" | {'Sortino':>8} | {'SrtAnn':>7}"
          f" | {'MeanRet':>8} | {'StdRet':>8}"
          f" | {'Skew':>7} | {'ExKurt':>7} | {'Trd/Yr':>6}")
    print("-" * 130)
    for r in results:
        if r["n"] == 0 or r.get("metrics") is None:
            continue
        m = r["metrics"]
        print(f"{r['ticker']:<8} | {m['sharpe']:>7.3f} | {m['sharpe_annual']:>7.3f}"
              f" | {m['psr']*100:>6.1f}% | {m['sortino']:>8.3f}"
              f" | {m['sortino_annual']:>7.3f}"
              f" | {m['mean_return']:>8.4f} | {m['std_return']:>8.4f}"
              f" | {m['skewness']:>7.3f} | {m['kurtosis_excess']:>7.3f}"
              f" | {m['trades_per_year']:>5.1f}")
    print("=" * 130)

    # ===================================================================
    # COMPARISON TABLE 3: Year-over-Year by Ticker
    # ===================================================================
    print()
    print("=" * 130)
    print("YEAR-OVER-YEAR P&L BY TICKER")
    print("=" * 130)

    # Collect all years across all tickers
    all_years = set()
    for r in results:
        if r.get("yearly"):
            all_years.update(r["yearly"].keys())
    all_years = sorted(all_years)

    # Header
    header = f"{'Year':<6}"
    for r in results:
        header += f" | {r['ticker']:>12}"
    header += f" | {'COMBINED':>12}"
    print(header)
    print("-" * 130)

    combined_total = 0
    for yr in all_years:
        row = f"{yr:<6}"
        yr_combined = 0
        for r in results:
            if r.get("yearly") and yr in r["yearly"]:
                yr_pnl = r["yearly"][yr]["pnl"]
                yr_combined += yr_pnl
                row += f" | ${yr_pnl:>+10,.0f}"
            else:
                row += f" | {'---':>12}"
        combined_total += yr_combined
        row += f" | ${yr_combined:>+10,.0f}"
        print(row)

    print("-" * 130)
    row = f"{'TOTAL':<6}"
    for r in results:
        row += f" | ${r['pnl']:>+10,.0f}" if r["n"] > 0 else f" | {'---':>12}"
    row += f" | ${combined_total:>+10,.0f}"
    print(row)
    print("=" * 130)

    # ===================================================================
    # COMBINED PORTFOLIO METRICS
    # ===================================================================
    print()
    print("=" * 130)
    print("COMBINED PORTFOLIO (all tickers traded simultaneously)")
    print("=" * 130)

    all_trades = []
    for r in results:
        all_trades.extend(r["trades"])

    if all_trades:
        n_total = len(all_trades)
        total_pnl = sum(t["pnl"] for t in all_trades)
        total_wins = sum(1 for t in all_trades if t["won"])
        combined_metrics = compute_risk_metrics(all_trades)

        print(f"  Total trades across all tickers: {n_total}")
        print(f"  Combined P&L:                    ${total_pnl:>+,.2f}")
        print(f"  Combined win rate:               {total_wins/n_total:.1%}")
        print(f"  Avg P&L/trade:                   ${total_pnl/n_total:>+,.2f}")

        if combined_metrics:
            print(f"\n  Combined Risk Metrics:")
            print(f"    Sharpe (annualized):   {combined_metrics['sharpe_annual']:.3f}")
            print(f"    Sortino (annualized):  {combined_metrics['sortino_annual']:.3f}")
            print(f"    PSR:                   {combined_metrics['psr']:.1%}")
            print(f"    Trades/year:           {combined_metrics['trades_per_year']:.1f}")

    # ===================================================================
    # DETAILED OUTPUT PER TICKER
    # ===================================================================
    for r in results:
        if r["n"] > 0:
            print()
            print("#" * 130)
            print(f"# DETAILED: {r['ticker']}")
            print("#" * 130)
            sc = r["skip_counts"]
            print_results(r["trades"], sc[0], sc[1], sc[2], sc[3], sc[4])


if __name__ == "__main__":
    main()
