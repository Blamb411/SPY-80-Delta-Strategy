#!/usr/bin/env python3
"""
Portfolio Growth Simulation
============================
Simulates investing $10,000 in the put credit spread strategy from 2012-2025.
Tracks equity curve, drawdowns, annualized returns, and compares to SPY buy-and-hold.

Sizing: Each trade risks a fixed percentage of current equity as max loss.
Default = 1 contract per trade (no leverage scaling), so P&L is additive.

Also shows a "scaled" version where we trade N contracts = floor(equity * risk_pct / max_loss).

Usage:
    python portfolio_simulation.py
"""

import os
import sys
import math
from datetime import datetime, date, timedelta
from collections import defaultdict

_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_this_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)
if _this_dir not in sys.path:
    sys.path.insert(0, _this_dir)

from backtest.put_spread_thetadata import (
    run_backtest, compute_risk_metrics, print_results,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
STARTING_CAPITAL = 10_000
START_YEAR = 2012
END_YEAR = 2025
SMA_PERIOD = 200
STOP_LOSS_MULT = 3.0
IV_RANK_LOW = 0.15
WING_SIGMA = 0.75
RISK_PCT = 0.05  # risk 5% of equity per trade for scaled version


def analyze_portfolio(trades, starting_capital, title, scale_positions=False,
                      risk_pct=RISK_PCT):
    """
    Analyze portfolio performance given a list of trades.

    If scale_positions=False: 1 contract per trade (additive P&L).
    If scale_positions=True: N contracts = floor(equity * risk_pct / max_loss).
    """
    if not trades:
        print("No trades to analyze.")
        return

    # Sort trades by entry date
    sorted_trades = sorted(trades, key=lambda t: t["entry_date"])

    # Build equity curve
    equity = starting_capital
    equity_history = []  # list of (date, equity, trade_info)
    trade_log = []

    for t in sorted_trades:
        entry_dt = datetime.strptime(t["entry_date"], "%Y-%m-%d").date()
        exit_dt = datetime.strptime(t["exit_date"], "%Y-%m-%d").date()

        if scale_positions:
            max_loss_per = t["max_loss"]
            if max_loss_per <= 0:
                n_contracts = 1
            else:
                n_contracts = max(1, int(equity * risk_pct / max_loss_per))
            trade_pnl = t["pnl"] * n_contracts
        else:
            n_contracts = 1
            trade_pnl = t["pnl"]

        equity += trade_pnl

        trade_log.append({
            "entry": t["entry_date"],
            "exit": t["exit_date"],
            "credit": t["credit"],
            "max_loss": t["max_loss"],
            "pnl_per": t["pnl"],
            "contracts": n_contracts,
            "total_pnl": trade_pnl,
            "equity_after": equity,
            "won": t["won"],
            "exit_reason": t["exit_reason"],
            "iv_rank": t["iv_rank"],
            "vix": t["vix"],
        })

        equity_history.append((exit_dt, equity))

    # Compute key metrics
    first_date = datetime.strptime(sorted_trades[0]["entry_date"], "%Y-%m-%d").date()
    last_date = datetime.strptime(sorted_trades[-1]["exit_date"], "%Y-%m-%d").date()
    total_days = (last_date - first_date).days
    total_years = total_days / 365.25

    final_equity = equity
    total_return = (final_equity - starting_capital) / starting_capital
    cagr = ((final_equity / starting_capital) ** (1 / total_years) - 1) if total_years > 0 else 0

    # Max drawdown
    peak = starting_capital
    max_dd_dollars = 0
    max_dd_pct = 0
    dd_peak_date = first_date
    dd_trough_date = first_date
    current_dd_start = first_date

    # Track equity by exit date for drawdown
    running_equity = starting_capital
    peak_eq = starting_capital
    for t in trade_log:
        running_equity = t["equity_after"]
        exit_dt = datetime.strptime(t["exit"], "%Y-%m-%d").date()
        if running_equity > peak_eq:
            peak_eq = running_equity
            current_dd_start = exit_dt
        dd = peak_eq - running_equity
        dd_pct = dd / peak_eq if peak_eq > 0 else 0
        if dd > max_dd_dollars:
            max_dd_dollars = dd
            max_dd_pct = dd_pct
            dd_peak_date = current_dd_start
            dd_trough_date = exit_dt

    # Year-over-year
    yearly = defaultdict(lambda: {"trades": 0, "pnl": 0, "wins": 0})
    for t in trade_log:
        yr = t["entry"][:4]
        yearly[yr]["trades"] += 1
        yearly[yr]["pnl"] += t["total_pnl"]
        if t["won"]:
            yearly[yr]["wins"] += 1

    # Win/loss stats
    wins = [t for t in trade_log if t["won"]]
    losses = [t for t in trade_log if not t["won"]]
    avg_win = sum(t["total_pnl"] for t in wins) / len(wins) if wins else 0
    avg_loss = sum(t["total_pnl"] for t in losses) / len(losses) if losses else 0
    win_loss_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else float("inf")

    # Profit factor
    gross_profit = sum(t["total_pnl"] for t in wins)
    gross_loss = abs(sum(t["total_pnl"] for t in losses))
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")

    # Risk metrics from underlying trades
    metrics = compute_risk_metrics(trades)

    # ---------------------------------------------------------------------------
    # Print results
    # ---------------------------------------------------------------------------
    print()
    print("=" * 78)
    print(title)
    print("=" * 78)

    print(f"\n  Strategy Parameters:")
    print(f"    SMA Filter:       {SMA_PERIOD}-day")
    print(f"    Stop Loss:        {STOP_LOSS_MULT}x credit")
    print(f"    IV Rank Min:      {IV_RANK_LOW:.0%}")
    print(f"    Wing Mode:        Vol-scaled (sigma={WING_SIGMA})")
    sizing = "Scaled (risk {:.0%} of equity/trade)".format(risk_pct) if scale_positions else "Fixed 1 contract/trade"
    print(f"    Position Sizing:  {sizing}")

    print(f"\n  Portfolio Summary:")
    print(f"    Starting Capital:  ${starting_capital:>12,.2f}")
    print(f"    Final Equity:      ${final_equity:>12,.2f}")
    print(f"    Total Return:      {total_return:>+11.2%}")
    print(f"    CAGR:              {cagr:>+11.2%}")
    print(f"    Period:            {first_date} to {last_date}  ({total_years:.1f} years)")

    print(f"\n  Trade Statistics:")
    print(f"    Total trades:      {len(trade_log)}")
    print(f"    Winners:           {len(wins)}  ({len(wins)/len(trade_log):.1%})")
    print(f"    Losers:            {len(losses)}")
    print(f"    Avg win:           ${avg_win:>+,.2f}")
    print(f"    Avg loss:          ${avg_loss:>+,.2f}")
    print(f"    Win/Loss ratio:    {win_loss_ratio:.2f}x")
    print(f"    Profit factor:     {profit_factor:.2f}x")

    print(f"\n  Risk Metrics:")
    print(f"    Max drawdown:      ${max_dd_dollars:>,.2f}  ({max_dd_pct:.2%})")
    print(f"    DD peak date:      {dd_peak_date}")
    print(f"    DD trough date:    {dd_trough_date}")
    print(f"    Return / MaxDD:    {abs(total_return / max_dd_pct):.2f}x" if max_dd_pct > 0 else "    Return / MaxDD:    N/A")
    print(f"    CAGR / MaxDD:      {abs(cagr / max_dd_pct):.2f}x" if max_dd_pct > 0 else "    CAGR / MaxDD:      N/A")

    if metrics:
        print(f"\n  Risk-Adjusted Metrics (per-trade):")
        print(f"    Sharpe Ratio:      {metrics['sharpe']:.3f}  (annualized: {metrics['sharpe_annual']:.3f})")
        print(f"    Sortino Ratio:     {metrics['sortino']:.3f}  (annualized: {metrics['sortino_annual']:.3f})")
        print(f"    Probabilistic SR:  {metrics['psr']:.1%}")
        print(f"    Skewness:          {metrics['skewness']:.3f}")
        print(f"    Excess kurtosis:   {metrics['kurtosis_excess']:.3f}")

    # Year-over-year table
    print()
    print("-" * 78)
    print("YEAR-OVER-YEAR PERFORMANCE")
    print("-" * 78)
    print(f"  {'Year':<6} {'Trades':>7} {'Win%':>7} {'P&L':>12} {'Equity':>14} {'YTD Ret':>10}")
    print(f"  {'-'*6} {'-'*7} {'-'*7} {'-'*12} {'-'*14} {'-'*10}")

    running_eq = starting_capital
    for yr in sorted(yearly.keys()):
        y = yearly[yr]
        wr = y["wins"] / y["trades"] if y["trades"] > 0 else 0
        running_eq_before = running_eq
        running_eq += y["pnl"]
        ytd_ret = y["pnl"] / running_eq_before if running_eq_before > 0 else 0
        print(f"  {yr}   {y['trades']:>5}  {wr:>6.1%}  ${y['pnl']:>+10,.2f}"
              f"  ${running_eq:>12,.2f}  {ytd_ret:>+9.2%}")

    # Equity curve (text-based chart)
    print()
    print("-" * 78)
    print("EQUITY CURVE")
    print("-" * 78)

    # Build quarterly snapshots
    quarterly = {}
    eq = starting_capital
    for t in trade_log:
        eq = t["equity_after"]
        exit_dt = datetime.strptime(t["exit"], "%Y-%m-%d").date()
        qtr = f"{exit_dt.year}-Q{(exit_dt.month - 1) // 3 + 1}"
        quarterly[qtr] = eq

    if quarterly:
        min_eq = min(quarterly.values())
        max_eq = max(quarterly.values())
        eq_range = max_eq - min_eq if max_eq > min_eq else 1

        print(f"  {'Quarter':<9} {'Equity':>12} {'Bar':>50}")
        for qtr in sorted(quarterly.keys()):
            eq = quarterly[qtr]
            bar_len = int((eq - min_eq) / eq_range * 40) + 1
            bar = "#" * bar_len
            print(f"  {qtr}  ${eq:>11,.2f}  {bar}")

    # Trade-by-trade log (first 20 and last 10)
    print()
    print("-" * 78)
    print("TRADE LOG (first 20)")
    print("-" * 78)
    print(f"  {'#':>3} {'Entry':<12} {'Exit':<12} {'Ctrs':>4} {'P&L':>10}"
          f" {'Equity':>12} {'Exit Reason':<15} {'VIX':>5} {'IVRk':>5}")
    print(f"  {'-'*3} {'-'*12} {'-'*12} {'-'*4} {'-'*10}"
          f" {'-'*12} {'-'*15} {'-'*5} {'-'*5}")

    for i, t in enumerate(trade_log[:20]):
        print(f"  {i+1:>3} {t['entry']}  {t['exit']}  {t['contracts']:>4}"
              f" ${t['total_pnl']:>+8,.2f}"
              f"  ${t['equity_after']:>10,.2f}  {t['exit_reason']:<15}"
              f" {t['vix']:>5.1f} {t['iv_rank']:>4.0%}")

    if len(trade_log) > 30:
        print(f"\n  ... ({len(trade_log) - 30} trades omitted) ...\n")
        print(f"  {'#':>3} {'Entry':<12} {'Exit':<12} {'Ctrs':>4} {'P&L':>10}"
              f" {'Equity':>12} {'Exit Reason':<15} {'VIX':>5} {'IVRk':>5}")
        print(f"  {'-'*3} {'-'*12} {'-'*12} {'-'*4} {'-'*10}"
              f" {'-'*12} {'-'*15} {'-'*5} {'-'*5}")
        for i, t in enumerate(trade_log[-10:], len(trade_log) - 9):
            print(f"  {i:>3} {t['entry']}  {t['exit']}  {t['contracts']:>4}"
                  f" ${t['total_pnl']:>+8,.2f}"
                  f"  ${t['equity_after']:>10,.2f}  {t['exit_reason']:<15}"
                  f" {t['vix']:>5.1f} {t['iv_rank']:>4.0%}")

    print()
    print("=" * 78)

    return {
        "final_equity": final_equity,
        "total_return": total_return,
        "cagr": cagr,
        "max_dd_dollars": max_dd_dollars,
        "max_dd_pct": max_dd_pct,
        "trade_log": trade_log,
        "equity_history": equity_history,
        "yearly": dict(yearly),
        "quarterly": quarterly,
        "metrics": metrics,
    }


def compare_to_spy(result, starting_capital):
    """Compare strategy to SPY buy-and-hold."""
    from backtest.thetadata_client import ThetaDataClient

    client = ThetaDataClient()
    client.connect()
    spy_bars = client.fetch_spy_bars(f"{START_YEAR}-01-01", f"{END_YEAR}-12-31")
    client.close()

    if not spy_bars:
        print("Cannot fetch SPY data for comparison.")
        return

    # Find first and last bar matching our trade period
    first_trade_date = result["trade_log"][0]["entry"]
    last_trade_date = result["trade_log"][-1]["exit"]

    first_bar = None
    last_bar = None
    for b in spy_bars:
        if first_bar is None and b["bar_date"] >= first_trade_date:
            first_bar = b
        if b["bar_date"] <= last_trade_date:
            last_bar = b

    if first_bar is None or last_bar is None:
        return

    spy_return = (last_bar["close"] - first_bar["close"]) / first_bar["close"]
    spy_years = (datetime.strptime(last_bar["bar_date"], "%Y-%m-%d").date()
                 - datetime.strptime(first_bar["bar_date"], "%Y-%m-%d").date()).days / 365.25
    spy_cagr = ((1 + spy_return) ** (1 / spy_years) - 1) if spy_years > 0 else 0

    spy_final = starting_capital * (1 + spy_return)

    # SPY max drawdown over period
    peak = 0
    spy_max_dd = 0
    for b in spy_bars:
        if b["bar_date"] < first_trade_date or b["bar_date"] > last_trade_date:
            continue
        if b["close"] > peak:
            peak = b["close"]
        dd = (peak - b["close"]) / peak
        if dd > spy_max_dd:
            spy_max_dd = dd

    # SPY annualized Sharpe (approximate from daily returns)
    daily_returns = []
    for i in range(1, len(spy_bars)):
        if spy_bars[i]["bar_date"] < first_trade_date:
            continue
        if spy_bars[i]["bar_date"] > last_trade_date:
            break
        r = (spy_bars[i]["close"] - spy_bars[i-1]["close"]) / spy_bars[i-1]["close"]
        daily_returns.append(r)

    if daily_returns:
        spy_mean = sum(daily_returns) / len(daily_returns)
        spy_std = (sum((r - spy_mean)**2 for r in daily_returns) / len(daily_returns)) ** 0.5
        spy_sharpe_ann = (spy_mean / spy_std) * (252 ** 0.5) if spy_std > 0 else 0
    else:
        spy_sharpe_ann = 0

    print()
    print("=" * 78)
    print("COMPARISON: PUT SPREAD STRATEGY vs SPY BUY-AND-HOLD")
    print("=" * 78)
    print(f"  Period: {first_bar['bar_date']} to {last_bar['bar_date']}")
    print(f"  SPY:  ${first_bar['close']:.2f} -> ${last_bar['close']:.2f}")
    print()
    print(f"  {'Metric':<25} {'Strategy':>14} {'SPY B&H':>14}")
    print(f"  {'-'*25} {'-'*14} {'-'*14}")
    print(f"  {'Starting Capital':<25} ${starting_capital:>12,.2f} ${starting_capital:>12,.2f}")
    print(f"  {'Final Equity':<25} ${result['final_equity']:>12,.2f} ${spy_final:>12,.2f}")
    print(f"  {'Total Return':<25} {result['total_return']:>+13.2%} {spy_return:>+13.2%}")
    print(f"  {'CAGR':<25} {result['cagr']:>+13.2%} {spy_cagr:>+13.2%}")
    print(f"  {'Max Drawdown':<25} {result['max_dd_pct']:>13.2%} {spy_max_dd:>13.2%}")
    print(f"  {'Sharpe (annualized)':<25} {result['metrics']['sharpe_annual']:>13.3f} {spy_sharpe_ann:>13.3f}")

    strat_calmar = abs(result['cagr'] / result['max_dd_pct']) if result['max_dd_pct'] > 0 else 0
    spy_calmar = abs(spy_cagr / spy_max_dd) if spy_max_dd > 0 else 0
    print(f"  {'CAGR / MaxDD (Calmar)':<25} {strat_calmar:>13.2f} {spy_calmar:>13.2f}")
    print()

    # Correlation note
    print(f"  Note: The strategy has {result['metrics']['skewness']:.1f} skewness "
          f"(positive = right tail, desirable)")
    print(f"  SPY buy-and-hold experienced {spy_max_dd:.1%} max drawdown in this period")
    print(f"  Strategy max drawdown was only {result['max_dd_pct']:.1%} of portfolio")
    print()
    print("=" * 78)


def main():
    print("Running backtest: IV>=15%, sigma=0.75, SMA=200, SL=3.0x, 2012-2025")
    print()

    trades, sk_sma, sk_iv, sk_oi, sk_cw, sk_data = run_backtest(
        START_YEAR, END_YEAR,
        stop_loss_mult=STOP_LOSS_MULT,
        sma_period=SMA_PERIOD,
        iv_rank_low=IV_RANK_LOW,
        wing_sigma=WING_SIGMA,
    )

    if not trades:
        print("No trades generated.")
        return

    # Section 1: Fixed 1-contract portfolio
    result_fixed = analyze_portfolio(
        trades, STARTING_CAPITAL,
        title=f"PORTFOLIO SIMULATION: $10,000 Starting Capital (1 Contract/Trade)",
        scale_positions=False,
    )

    # Section 2: Scaled position sizing
    result_scaled = analyze_portfolio(
        trades, STARTING_CAPITAL,
        title=f"PORTFOLIO SIMULATION: $10,000 Starting Capital (Scaled Sizing, {RISK_PCT:.0%} risk/trade)",
        scale_positions=True,
        risk_pct=RISK_PCT,
    )

    # Section 3: Comparison to SPY
    compare_to_spy(result_fixed, STARTING_CAPITAL)

    # Section 4: Detailed backtest output
    print()
    print("#" * 78)
    print("# DETAILED BACKTEST OUTPUT")
    print("#" * 78)
    print_results(trades, sk_sma, sk_iv, sk_oi, sk_cw, sk_data)


if __name__ == "__main__":
    main()
