#!/usr/bin/env python3
"""
Capital Efficiency Analysis
=============================
Calculates annualized returns on ACTUAL INVESTED CAPITAL rather than total
account size. Tracks daily capital at risk from overlapping positions and
computes returns on average and peak deployed capital.

This gives a fair comparison to SPY buy-and-hold, which is 100% invested.

Usage:
    python capital_efficiency.py
"""

import os
import sys
from datetime import datetime, date, timedelta
from collections import defaultdict

_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_this_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)
if _this_dir not in sys.path:
    sys.path.insert(0, _this_dir)

from backtest.put_spread_thetadata import run_backtest, compute_risk_metrics
from backtest.thetadata_client import ThetaDataClient

START_YEAR = 2012
END_YEAR = 2025
SMA_PERIOD = 200
STOP_LOSS_MULT = 3.0
IV_RANK_LOW = 0.15
WING_SIGMA = 0.75


def analyze_capital_efficiency(trades):
    """Compute returns on actually deployed capital."""
    if not trades:
        print("No trades.")
        return

    # Parse trades into date-keyed structures
    parsed = []
    for i, t in enumerate(trades):
        parsed.append({
            "id": i,
            "entry": datetime.strptime(t["entry_date"], "%Y-%m-%d").date(),
            "exit": datetime.strptime(t["exit_date"], "%Y-%m-%d").date(),
            "max_loss": t["max_loss"],
            "credit": t["credit"],
            "pnl": t["pnl"],
            "won": t["won"],
        })

    # Build set of all trading days
    all_dates = set()
    for t in parsed:
        d = t["entry"]
        while d <= t["exit"]:
            if d.weekday() < 5:
                all_dates.add(d)
            d += timedelta(days=1)

    sorted_dates = sorted(all_dates)
    first_date = sorted_dates[0]
    last_date = sorted_dates[-1]
    total_days = (last_date - first_date).days
    total_years = total_days / 365.25

    # Daily capital at risk and open position count
    daily_capital = {}
    daily_count = {}
    for d in sorted_dates:
        open_trades = [t for t in parsed if t["entry"] <= d <= t["exit"]]
        daily_capital[d] = sum(t["max_loss"] for t in open_trades)
        daily_count[d] = len(open_trades)

    # Days with zero capital deployed
    zero_days = sum(1 for d in sorted_dates if daily_capital[d] == 0)
    invested_days = len(sorted_dates) - zero_days

    # Capital statistics
    all_caps = [v for v in daily_capital.values()]
    nonzero_caps = [v for v in daily_capital.values() if v > 0]
    peak_capital = max(all_caps)
    avg_capital_all = sum(all_caps) / len(all_caps)
    avg_capital_invested = sum(nonzero_caps) / len(nonzero_caps) if nonzero_caps else 0

    # Total P&L
    total_pnl = sum(t["pnl"] for t in parsed)

    # P&L by year
    pnl_by_year = defaultdict(float)
    for t in parsed:
        yr = t["entry"].year
        pnl_by_year[yr] += t["pnl"]

    # Concurrent position distribution
    count_dist = defaultdict(int)
    for cnt in daily_count.values():
        count_dist[cnt] += 1

    # Utilization = fraction of days with at least 1 position open
    utilization = invested_days / len(sorted_dates) if sorted_dates else 0

    # ---------------------------------------------------------------------------
    # Return calculations on different capital bases
    # ---------------------------------------------------------------------------

    # Scenario A: Return on peak capital at risk (most conservative)
    ret_on_peak = total_pnl / peak_capital if peak_capital > 0 else 0
    cagr_peak = ((1 + ret_on_peak) ** (1 / total_years) - 1) if total_years > 0 else 0

    # Scenario B: Return on average capital deployed (all days including zero)
    ret_on_avg_all = total_pnl / avg_capital_all if avg_capital_all > 0 else 0
    cagr_avg_all = ((1 + ret_on_avg_all) ** (1 / total_years) - 1) if total_years > 0 else 0

    # Scenario C: Return on average capital when invested (excludes idle days)
    # This is the "what if we only count time when capital is deployed" view
    # Annualize based on invested fraction of time
    ret_on_avg_invested = total_pnl / avg_capital_invested if avg_capital_invested > 0 else 0
    invested_years = total_years * utilization
    cagr_avg_invested = ((1 + ret_on_avg_invested) ** (1 / invested_years) - 1) if invested_years > 0 else 0

    # Scenario D: True time-weighted return on deployed capital
    # Only count returns during periods when capital is actually deployed
    # Weight each day's P&L by capital at risk that day
    pnl_by_date = defaultdict(float)
    for t in parsed:
        pnl_by_date[t["exit"]] += t["pnl"]

    # Daily return = day's realized P&L / capital at risk that day
    daily_returns = []
    for d in sorted_dates:
        cap = daily_capital[d]
        if cap > 0:
            day_pnl = pnl_by_date.get(d, 0.0)
            daily_returns.append(day_pnl / cap)

    if daily_returns:
        # Compound daily returns
        compound = 1.0
        for r in daily_returns:
            compound *= (1 + r)
        total_compound_return = compound - 1
        # Annualize: we have invested_days of actual returns
        ann_factor = 252 / len(daily_returns) if daily_returns else 1
        cagr_compound = (compound ** ann_factor - 1) if ann_factor > 0 else 0
    else:
        total_compound_return = 0
        cagr_compound = 0

    # ---------------------------------------------------------------------------
    # SPY comparison
    # ---------------------------------------------------------------------------
    client = ThetaDataClient()
    client.connect()
    spy_bars = client.fetch_spy_bars(f"{START_YEAR}-01-01", f"{END_YEAR}-12-31")
    client.close()

    spy_first = spy_last = None
    for b in spy_bars:
        bd = datetime.strptime(b["bar_date"], "%Y-%m-%d").date()
        if spy_first is None and bd >= first_date:
            spy_first = b
        if bd <= last_date:
            spy_last = b

    spy_return = (spy_last["close"] - spy_first["close"]) / spy_first["close"]
    spy_years = (datetime.strptime(spy_last["bar_date"], "%Y-%m-%d").date()
                 - datetime.strptime(spy_first["bar_date"], "%Y-%m-%d").date()).days / 365.25
    spy_cagr = ((1 + spy_return) ** (1 / spy_years) - 1) if spy_years > 0 else 0

    # SPY max drawdown
    peak_spy = 0
    spy_max_dd = 0
    for b in spy_bars:
        bd = datetime.strptime(b["bar_date"], "%Y-%m-%d").date()
        if bd < first_date or bd > last_date:
            continue
        if b["close"] > peak_spy:
            peak_spy = b["close"]
        dd = (peak_spy - b["close"]) / peak_spy
        if dd > spy_max_dd:
            spy_max_dd = dd

    # Strategy max drawdown on deployed capital
    # Build equity curve on invested capital basis
    equity = 0.0
    peak_eq = 0.0
    max_dd_on_capital = 0.0
    for d in sorted_dates:
        equity += pnl_by_date.get(d, 0.0)
        if equity > peak_eq:
            peak_eq = equity
        dd = peak_eq - equity
        if avg_capital_invested > 0:
            dd_pct = dd / avg_capital_invested
            if dd_pct > max_dd_on_capital:
                max_dd_on_capital = dd_pct

    # Risk metrics
    metrics = compute_risk_metrics(trades)

    # ---------------------------------------------------------------------------
    # Print results
    # ---------------------------------------------------------------------------
    print()
    print("=" * 78)
    print("CAPITAL EFFICIENCY ANALYSIS")
    print("Returns on Actually Deployed Capital")
    print("=" * 78)

    print(f"\n  Strategy: IV>=15%, sigma=0.75, SMA=200, SL=3.0x")
    print(f"  Period:   {first_date} to {last_date}  ({total_years:.1f} years)")
    print(f"  Trades:   {len(trades)}  ({len(trades)/total_years:.1f}/year)")
    print(f"  Win rate: {sum(1 for t in trades if t['won'])/len(trades):.1%}")
    print(f"  Total P&L: ${total_pnl:>+,.2f}")

    print()
    print("-" * 78)
    print("CAPITAL DEPLOYMENT")
    print("-" * 78)
    print(f"  Peak capital at risk:        ${peak_capital:>10,.2f}")
    print(f"  Avg capital at risk (all):   ${avg_capital_all:>10,.2f}")
    print(f"  Avg capital when invested:   ${avg_capital_invested:>10,.2f}")
    print(f"  Trading days total:          {len(sorted_dates):>6}")
    print(f"  Days with position open:     {invested_days:>6}  ({utilization:.1%} utilization)")
    print(f"  Days with no position:       {zero_days:>6}  ({1-utilization:.1%} idle)")

    print(f"\n  Concurrent positions:")
    for cnt in sorted(count_dist.keys()):
        pct = count_dist[cnt] / len(sorted_dates) * 100
        print(f"    {cnt} positions: {count_dist[cnt]:>5} days  ({pct:>5.1f}%)")

    print()
    print("-" * 78)
    print("ANNUALIZED RETURNS ON DEPLOYED CAPITAL")
    print("-" * 78)

    print(f"\n  {'Scenario':<40} {'Total Ret':>10} {'CAGR':>10}")
    print(f"  {'-'*40} {'-'*10} {'-'*10}")
    print(f"  {'A: On peak capital at risk':<40} {ret_on_peak:>+9.1%} {cagr_peak:>+9.2%}")
    print(f"  {'B: On avg capital (incl. idle days)':<40} {ret_on_avg_all:>+9.1%} {cagr_avg_all:>+9.2%}")
    print(f"  {'C: On avg capital (invested days only)':<40} {ret_on_avg_invested:>+9.1%} {cagr_avg_invested:>+9.2%}")
    print(f"  {'D: Compounded daily on deployed cap':<40} {total_compound_return:>+9.1%} {cagr_compound:>+9.2%}")

    print(f"\n  Interpretation:")
    print(f"    Scenario A = most conservative: you reserve peak worst-case capital")
    print(f"    Scenario B = average capital reserved across ALL days (including idle)")
    print(f"    Scenario C = average capital reserved only on days with open positions")
    print(f"    Scenario D = compounded daily return on whatever capital was at risk each day")

    print()
    print("-" * 78)
    print("COMPARISON TO SPY BUY-AND-HOLD (APPLES TO APPLES)")
    print("-" * 78)
    print(f"\n  SPY buy-and-hold is 100% invested for {spy_years:.1f} years")
    print(f"  Strategy is {utilization:.0%} invested (has positions {utilization:.0%} of trading days)")
    print()
    print(f"  {'Metric':<35} {'Strategy':>14} {'SPY B&H':>14}")
    print(f"  {'-'*35} {'-'*14} {'-'*14}")
    print(f"  {'CAGR on total account ($10K)':<35} {0.0407:>+13.2%} {spy_cagr:>+13.2%}")
    print(f"  {'CAGR on peak deployed capital':<35} {cagr_peak:>+13.2%} {spy_cagr:>+13.2%}")
    print(f"  {'CAGR on avg deployed capital':<35} {cagr_avg_all:>+13.2%} {spy_cagr:>+13.2%}")
    print(f"  {'CAGR on invested-time capital':<35} {cagr_avg_invested:>+13.2%} {spy_cagr:>+13.2%}")
    print(f"  {'Max drawdown':<35} {max_dd_on_capital:>13.2%} {spy_max_dd:>13.2%}")

    if metrics:
        print(f"  {'Sharpe (annualized)':<35} {metrics['sharpe_annual']:>13.3f} {'~0.5-0.9':>14}")
        print(f"  {'Sortino (annualized)':<35} {metrics['sortino_annual']:>13.3f} {'~0.8-1.2':>14}")

    # The key insight
    print()
    print("-" * 78)
    print("KEY INSIGHT: OVERLAY STRATEGY")
    print("-" * 78)
    print(f"""
  This strategy only uses ${avg_capital_invested:,.0f} avg capital when positions are open,
  and is idle {1-utilization:.0%} of the time. The remaining capital can be invested
  in SPY (or bonds/T-bills) simultaneously.

  Combined portfolio (strategy + SPY buy-and-hold with idle capital):
""")

    # Compute combined: invest idle capital in SPY
    # For each year, strategy P&L + SPY return on idle capital
    combined_equity = 0.0  # excess return from strategy
    spy_bar_by_date = {}
    for b in spy_bars:
        spy_bar_by_date[b["bar_date"]] = b["close"]

    # Simple overlay: $10K in SPY + run the strategy on margin
    # Strategy P&L is pure alpha on top of SPY returns
    spy_equity_10k = 10_000 * (1 + spy_return)
    overlay_equity = spy_equity_10k + total_pnl
    overlay_return = (overlay_equity - 10_000) / 10_000
    overlay_cagr = ((overlay_equity / 10_000) ** (1 / total_years) - 1) if total_years > 0 else 0

    print(f"    $10K in SPY buy-and-hold:         ${spy_equity_10k:>12,.2f}  ({spy_cagr:>+.2%} CAGR)")
    print(f"    Strategy P&L (pure alpha):         ${total_pnl:>12,.2f}")
    print(f"    Combined (SPY + strategy overlay): ${overlay_equity:>12,.2f}  ({overlay_cagr:>+.2%} CAGR)")
    print(f"    SPY-only CAGR:                     {spy_cagr:>+12.2%}")
    print(f"    Combined CAGR:                     {overlay_cagr:>+12.2%}")
    print(f"    Alpha (excess CAGR):               {overlay_cagr - spy_cagr:>+12.2%}")

    # Year-over-year combined
    print()
    print(f"  {'Year':<6} {'Strat P&L':>11} {'SPY Ret':>9} {'$10K SPY':>12} {'Combined':>12} {'Alpha':>8}")
    print(f"  {'-'*6} {'-'*11} {'-'*9} {'-'*12} {'-'*12} {'-'*8}")

    years_list = sorted(set(t["entry_date"][:4] for t in trades))
    spy_start_price = float(spy_first["close"])
    for yr in years_list:
        yr_pnl = pnl_by_year[int(yr)]

        # SPY return for this year
        yr_spy_prices = [(b["bar_date"], b["close"]) for b in spy_bars
                         if b["bar_date"][:4] == yr]
        if len(yr_spy_prices) >= 2:
            spy_yr_start = yr_spy_prices[0][1]
            spy_yr_end = yr_spy_prices[-1][1]
            spy_yr_ret = (spy_yr_end - spy_yr_start) / spy_yr_start
        else:
            spy_yr_ret = 0

        # Running SPY value
        spy_val = 10_000 * (spy_yr_end / spy_start_price) if yr_spy_prices else 0
        combined_val = spy_val + pnl_by_year[int(yr)]  # simplified

        print(f"  {yr}   ${yr_pnl:>+9,.2f}  {spy_yr_ret:>+7.1%}  ${spy_val:>10,.2f}  ${spy_val + yr_pnl:>10,.2f}"
              f"  ${yr_pnl:>+6,.0f}")

    print()
    print("=" * 78)


def main():
    print("Running backtest...")
    trades, sk_sma, sk_iv, sk_oi, sk_cw, sk_data = run_backtest(
        START_YEAR, END_YEAR,
        stop_loss_mult=STOP_LOSS_MULT,
        sma_period=SMA_PERIOD,
        iv_rank_low=IV_RANK_LOW,
        wing_sigma=WING_SIGMA,
    )
    analyze_capital_efficiency(trades)


if __name__ == "__main__":
    main()
