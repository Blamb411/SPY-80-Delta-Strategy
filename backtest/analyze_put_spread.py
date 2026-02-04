#!/usr/bin/env python3
"""
Capital Allocation, Drawdown & Return Analysis
================================================
Analyzes put credit spread backtest results for:
- Overlapping position tracking (concurrent trades)
- Peak capital at risk
- Daily equity curve
- Maximum drawdown (peak-to-trough)
- Annualized returns (CAGR)
- Risk-adjusted metrics (return/max-drawdown)

Usage:
    python analyze_put_spread.py
"""

import os
import sys
import csv
from datetime import datetime, date, timedelta
from collections import defaultdict

_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_this_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)
if _this_dir not in sys.path:
    sys.path.insert(0, _this_dir)

from backtest.put_spread_thetadata import run_backtest


def analyze_trades(trades):
    """Full capital allocation, drawdown, and return analysis."""
    if not trades:
        print("No trades to analyze.")
        return

    # ------------------------------------------------------------------
    # 1. Build timeline of all trading days spanned by any position
    # ------------------------------------------------------------------
    all_dates = set()
    for t in trades:
        entry_dt = datetime.strptime(t["entry_date"], "%Y-%m-%d").date()
        exit_dt = datetime.strptime(t["exit_date"], "%Y-%m-%d").date()
        d = entry_dt
        while d <= exit_dt:
            if d.weekday() < 5:  # weekdays only
                all_dates.add(d)
            d += timedelta(days=1)

    sorted_dates = sorted(all_dates)
    first_date = sorted_dates[0]
    last_date = sorted_dates[-1]
    total_calendar_days = (last_date - first_date).days
    total_years = total_calendar_days / 365.25

    # ------------------------------------------------------------------
    # 2. For each date, track open positions and capital at risk
    # ------------------------------------------------------------------
    # Parse trades into date-keyed structures
    trade_entries = []
    for i, t in enumerate(trades):
        trade_entries.append({
            "id": i,
            "entry": datetime.strptime(t["entry_date"], "%Y-%m-%d").date(),
            "exit": datetime.strptime(t["exit_date"], "%Y-%m-%d").date(),
            "max_loss": t["max_loss"],  # per contract
            "credit": t["credit"],
            "pnl": t["pnl"],
            "won": t["won"],
            "exit_reason": t["exit_reason"],
        })

    # Daily tracking
    daily_open_count = {}      # date -> number of open positions
    daily_capital_at_risk = {} # date -> total max_loss of open positions
    daily_margin = {}          # date -> margin required (max_loss per position)

    for d in sorted_dates:
        open_trades = [t for t in trade_entries
                       if t["entry"] <= d <= t["exit"]]
        daily_open_count[d] = len(open_trades)
        daily_capital_at_risk[d] = sum(t["max_loss"] for t in open_trades)

    # ------------------------------------------------------------------
    # 3. Build equity curve (cumulative realized P&L)
    # ------------------------------------------------------------------
    # P&L is realized on exit date
    pnl_by_date = defaultdict(float)
    for t in trade_entries:
        pnl_by_date[t["exit"]] += t["pnl"]

    equity_curve = {}  # date -> cumulative P&L
    cumulative = 0.0
    for d in sorted_dates:
        cumulative += pnl_by_date.get(d, 0.0)
        equity_curve[d] = cumulative

    # ------------------------------------------------------------------
    # 4. Compute max drawdown from equity curve
    # ------------------------------------------------------------------
    peak = float("-inf")
    max_dd = 0.0
    max_dd_peak_date = None
    max_dd_trough_date = None
    current_dd_start = None

    for d in sorted_dates:
        eq = equity_curve[d]
        if eq > peak:
            peak = eq
            current_dd_start = d
        dd = peak - eq
        if dd > max_dd:
            max_dd = dd
            max_dd_peak_date = current_dd_start
            max_dd_trough_date = d

    # ------------------------------------------------------------------
    # 5. Capital allocation scenarios
    # ------------------------------------------------------------------
    max_concurrent = max(daily_open_count.values())
    max_capital_at_risk = max(daily_capital_at_risk.values())
    avg_capital_at_risk = sum(daily_capital_at_risk.values()) / len(daily_capital_at_risk)

    # Scenario A: 1 contract, capital = peak max_loss across all open positions
    total_pnl = sum(t["pnl"] for t in trade_entries)
    peak_single_max_loss = max(t["max_loss"] for t in trade_entries)

    # Scenario B: Fixed capital to cover worst-case concurrent positions
    # Use the peak capital at risk as the required capital
    capital_for_peak = max_capital_at_risk

    # Scenario C: Conservative — allocate for max concurrent * avg max_loss
    avg_max_loss = sum(t["max_loss"] for t in trade_entries) / len(trade_entries)
    capital_conservative = max_concurrent * avg_max_loss

    # CAGR calculations
    def cagr(total_return_pct, years):
        if years <= 0 or total_return_pct <= -100:
            return 0.0
        return ((1 + total_return_pct / 100) ** (1 / years) - 1) * 100

    # ------------------------------------------------------------------
    # 6. Print results
    # ------------------------------------------------------------------
    print()
    print("=" * 78)
    print("CAPITAL ALLOCATION & DRAWDOWN ANALYSIS")
    print("Put Credit Spread  |  SPY  |  SMA=200  |  SL=3.0x credit")
    print("=" * 78)

    print(f"\n  Period:              {first_date} to {last_date}")
    print(f"  Calendar days:       {total_calendar_days:,}")
    print(f"  Years:               {total_years:.2f}")
    print(f"  Total trades:        {len(trades)}")
    print(f"  Winners:             {sum(1 for t in trade_entries if t['won'])}"
          f"  ({sum(1 for t in trade_entries if t['won'])/len(trades):.1%})")
    print(f"  Total P&L:           ${total_pnl:>+,.2f}")

    # --- Overlapping positions ---
    print()
    print("-" * 78)
    print("OVERLAPPING POSITIONS")
    print("-" * 78)
    print(f"  Max concurrent positions:     {max_concurrent}")
    print(f"  Avg concurrent positions:     {sum(daily_open_count.values()) / len(daily_open_count):.2f}")

    # Distribution of concurrent positions
    count_dist = defaultdict(int)
    for cnt in daily_open_count.values():
        count_dist[cnt] += 1
    print(f"\n  Days by open position count:")
    for cnt in sorted(count_dist.keys()):
        pct = count_dist[cnt] / len(daily_open_count) * 100
        print(f"    {cnt} positions:  {count_dist[cnt]:>5} days  ({pct:>5.1f}%)")

    # --- Capital at risk ---
    print()
    print("-" * 78)
    print("CAPITAL AT RISK")
    print("-" * 78)
    print(f"  Peak capital at risk:         ${max_capital_at_risk:>,.2f}  (worst day)")
    print(f"  Avg capital at risk:          ${avg_capital_at_risk:>,.2f}")
    print(f"  Avg max loss per trade:       ${avg_max_loss:>,.2f}")
    print(f"  Largest single max loss:      ${peak_single_max_loss:>,.2f}")

    # --- Drawdown ---
    print()
    print("-" * 78)
    print("DRAWDOWN ANALYSIS")
    print("-" * 78)
    print(f"  Maximum drawdown:             ${max_dd:>,.2f}")
    if max_dd_peak_date and max_dd_trough_date:
        dd_days = (max_dd_trough_date - max_dd_peak_date).days
        print(f"  Drawdown peak date:           {max_dd_peak_date}")
        print(f"  Drawdown trough date:         {max_dd_trough_date}")
        print(f"  Drawdown duration:            {dd_days} calendar days")

    # Find all drawdowns > $50
    print(f"\n  Significant drawdowns (> $50):")
    print(f"    {'Start':<12} {'Trough':<12} {'Amount':>10} {'Days':>6} {'Recovery':>12}")
    print(f"    {'-'*12} {'-'*12} {'-'*10} {'-'*6} {'-'*12}")

    peak_val = float("-inf")
    peak_date = sorted_dates[0]
    in_drawdown = False
    dd_start = None
    dd_trough = None
    dd_trough_val = None
    dd_amount = 0

    drawdowns = []
    for d in sorted_dates:
        eq = equity_curve[d]
        if eq >= peak_val:
            if in_drawdown and dd_amount > 50:
                drawdowns.append({
                    "start": dd_start,
                    "trough": dd_trough,
                    "amount": dd_amount,
                    "recovery": d,
                })
            peak_val = eq
            peak_date = d
            in_drawdown = False
            dd_amount = 0
        else:
            dd = peak_val - eq
            if dd > dd_amount:
                dd_amount = dd
                dd_trough = d
                if not in_drawdown:
                    dd_start = peak_date
                    in_drawdown = True

    # Capture final drawdown if still in one
    if in_drawdown and dd_amount > 50:
        drawdowns.append({
            "start": dd_start,
            "trough": dd_trough,
            "amount": dd_amount,
            "recovery": None,
        })

    if not drawdowns:
        print(f"    (none)")
    for dd in drawdowns:
        rec = str(dd["recovery"]) if dd["recovery"] else "not recovered"
        days = (dd["trough"] - dd["start"]).days
        print(f"    {dd['start']}  {dd['trough']}  ${dd['amount']:>+8,.2f}  {days:>5}  {rec}")

    # --- Return scenarios ---
    print()
    print("-" * 78)
    print("RETURN SCENARIOS (1 contract per trade)")
    print("-" * 78)

    scenarios = [
        ("A: Peak single max loss",
         peak_single_max_loss,
         "Capital = largest max_loss of any single trade"),
        ("B: Peak concurrent risk",
         capital_for_peak,
         "Capital = worst-day total max_loss across all open"),
        ("C: Conservative",
         capital_conservative,
         f"Capital = max concurrent ({max_concurrent}) x avg max_loss"),
    ]

    for name, capital, desc in scenarios:
        if capital <= 0:
            continue
        total_return_pct = (total_pnl / capital) * 100
        annual_return = cagr(total_return_pct, total_years)
        dd_pct = (max_dd / capital) * 100 if capital > 0 else 0
        ret_dd_ratio = abs(total_return_pct / dd_pct) if dd_pct > 0 else float("inf")

        print(f"\n  {name}")
        print(f"    {desc}")
        print(f"    Allocated capital:     ${capital:>,.2f}")
        print(f"    Total return:          {total_return_pct:>+.2f}%")
        print(f"    Annualized (CAGR):     {annual_return:>+.2f}%")
        print(f"    Max drawdown:          {dd_pct:.2f}% of capital")
        print(f"    Return/MaxDD ratio:    {ret_dd_ratio:.2f}x")

    # --- Equity curve milestones ---
    print()
    print("-" * 78)
    print("EQUITY CURVE (monthly snapshots)")
    print("-" * 78)

    # Monthly snapshots
    monthly_eq = {}
    for d in sorted_dates:
        mo = d.strftime("%Y-%m")
        monthly_eq[mo] = equity_curve[d]  # last day of month wins

    print(f"  {'Month':<8} {'Equity':>10} {'Month P&L':>12} {'Bar':>35}")
    prev_eq = 0.0
    for mo in sorted(monthly_eq.keys()):
        eq = monthly_eq[mo]
        mo_pnl = eq - prev_eq
        bar_len = int(abs(mo_pnl) / 20)
        bar_char = "+" if mo_pnl >= 0 else "-"
        bar = bar_char * min(bar_len, 30)
        print(f"  {mo}  ${eq:>+9,.2f}  ${mo_pnl:>+10,.2f}  {bar}")
        prev_eq = eq

    # --- Year-over-year with capital efficiency ---
    print()
    print("-" * 78)
    print("YEAR-OVER-YEAR (using Scenario B capital)")
    print("-" * 78)
    print(f"  {'Year':<6} {'Trades':>7} {'P&L':>12} {'Return%':>10} {'MaxDD$':>10} {'Peak Cap':>12}")
    print(f"  {'-'*6} {'-'*7} {'-'*12} {'-'*10} {'-'*10} {'-'*12}")

    years = sorted(set(t["entry_date"][:4] for t in trades))
    for yr in years:
        yr_trades = [te for te in trade_entries
                     if trades[te["id"]]["entry_date"][:4] == yr]
        yr_pnl = sum(t["pnl"] for t in yr_trades)

        # Peak capital at risk this year
        yr_start = date(int(yr), 1, 1)
        yr_end = date(int(yr), 12, 31)
        yr_daily_cap = {d: v for d, v in daily_capital_at_risk.items()
                        if yr_start <= d <= yr_end}
        yr_peak_cap = max(yr_daily_cap.values()) if yr_daily_cap else 0

        # Year max drawdown
        yr_dates = [d for d in sorted_dates if yr_start <= d <= yr_end]
        yr_peak = float("-inf")
        yr_max_dd = 0
        for d in yr_dates:
            eq = equity_curve[d]
            if eq > yr_peak:
                yr_peak = eq
            dd = yr_peak - eq
            if dd > yr_max_dd:
                yr_max_dd = dd

        ret_pct = (yr_pnl / capital_for_peak * 100) if capital_for_peak > 0 else 0
        print(f"  {yr}   {len(yr_trades):>5}  ${yr_pnl:>+10,.2f}  {ret_pct:>+8.2f}%  "
              f"${yr_max_dd:>8,.2f}  ${yr_peak_cap:>10,.2f}")

    print()
    print("=" * 78)


def main():
    print("Running backtest: SMA=200, SL=3.0x, 2012-2025 ...")
    trades, skipped_sma, skipped_iv, skipped_oi, skipped_cw, skipped_data = run_backtest(
        start_year=2012, end_year=2025,
        stop_loss_mult=3.0,
        sma_period=200,
    )
    analyze_trades(trades)


if __name__ == "__main__":
    main()
