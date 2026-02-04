"""
Detailed Trade Analysis
========================
Analyzes trades from 'Data for Claude.xlsx' with comprehensive metrics.

Features:
  - $50,000 account with margin (~$16,667 per trade = 1/3 of account)
  - Daily cash invested in T-bills (2.5% annualized yield proxy)
  - Holding periods: 10, 20, 30 days with 5% stop-loss
  - Comprehensive metrics: Sharpe, Sortino, Max DD, Win/Loss, etc.
  - Outputs results to a text file

Usage:
    python trade_analysis_detailed.py
"""

import os
import sys
import numpy as np
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from collections import defaultdict

_this_dir = os.path.dirname(os.path.abspath(__file__))

INITIAL_CAPITAL = 50_000
POSITION_SIZE = INITIAL_CAPITAL / 3  # ~$16,667 per trade
STOP_LOSS_PCT = 0.05  # 5%
TBILL_ANNUAL_YIELD = 0.025  # 2.5% annual yield on idle cash
TBILL_DAILY_YIELD = (1 + TBILL_ANNUAL_YIELD) ** (1/252) - 1

HOLDING_PERIODS = [10, 20, 30]


def fetch_price_history(ticker, start_date, end_date):
    """Fetch daily close prices for a ticker from Yahoo Finance."""
    try:
        df = yf.download(ticker, start=start_date, end=end_date,
                         auto_adjust=True, progress=False)
        if df.empty:
            return None
        close = df["Close"]
        if isinstance(close, pd.DataFrame):
            close = close.iloc[:, 0]
        return close
    except Exception as e:
        return None


def simulate_trade(entry_price, prices_after_entry, holding_period, stop_loss_pct):
    """Simulate a trade with stop-loss."""
    stop_price = entry_price * (1 - stop_loss_pct)

    for day, price in enumerate(prices_after_entry.values[:holding_period], 1):
        if price <= stop_price:
            return {
                "exit_price": price,
                "exit_day": day,
                "return_pct": (price / entry_price - 1) * 100,
                "reason": "stop_loss",
            }

    actual_days = min(holding_period, len(prices_after_entry))
    if actual_days == 0:
        return None

    exit_price = prices_after_entry.values[actual_days - 1]
    return {
        "exit_price": exit_price,
        "exit_day": actual_days,
        "return_pct": (exit_price / entry_price - 1) * 100,
        "reason": "held",
    }


def analyze_trades(df, sheet_name):
    """Analyze trades from one sheet."""
    results = []

    for idx, row in df.iterrows():
        ticker = row['ticker']
        trade_date = pd.to_datetime(row['trade date'])
        target = row.get('target', np.nan)

        start = trade_date - timedelta(days=5)
        end = trade_date + timedelta(days=50)

        prices = fetch_price_history(ticker, start.strftime('%Y-%m-%d'),
                                     end.strftime('%Y-%m-%d'))

        if prices is None or len(prices) == 0:
            continue

        trade_date_str = trade_date.strftime('%Y-%m-%d')
        entry_idx = None
        for i, dt in enumerate(prices.index):
            if dt.strftime('%Y-%m-%d') >= trade_date_str:
                entry_idx = i
                break

        if entry_idx is None:
            continue

        entry_price = prices.iloc[entry_idx]
        entry_date_actual = prices.index[entry_idx]
        prices_after = prices.iloc[entry_idx + 1:]

        if len(prices_after) == 0:
            continue

        for hp in HOLDING_PERIODS:
            result = simulate_trade(entry_price, prices_after, hp, STOP_LOSS_PCT)
            if result:
                results.append({
                    "sheet": sheet_name,
                    "ticker": ticker,
                    "trade_date": entry_date_actual,
                    "entry_price": entry_price,
                    "target": target,
                    "holding_period": hp,
                    "exit_price": result["exit_price"],
                    "exit_day": result["exit_day"],
                    "return_pct": result["return_pct"],
                    "reason": result["reason"],
                })

    return results


def simulate_portfolio_with_cash(trades, holding_period):
    """
    Simulate portfolio with T-bill returns on idle cash.
    Assumes sequential execution of trades.
    """
    filtered = [r for r in trades if r["holding_period"] == holding_period]
    if not filtered:
        return None

    filtered = sorted(filtered, key=lambda x: x["trade_date"])

    # Track daily portfolio value for Sharpe/Sortino calculation
    daily_values = [INITIAL_CAPITAL]
    current_capital = INITIAL_CAPITAL

    # Trade statistics
    trade_pnls = []
    wins = 0
    losses = 0
    stop_outs = 0
    avg_win = []
    avg_loss = []

    # Calculate based on each trade
    for trade in filtered:
        ret = trade["return_pct"]
        days_held = trade["exit_day"]

        # Cash not in position earns T-bill rate
        cash_portion = INITIAL_CAPITAL - POSITION_SIZE  # ~$33,333 in cash
        cash_return = cash_portion * ((1 + TBILL_DAILY_YIELD) ** days_held - 1)

        # Position return
        position_pnl = POSITION_SIZE * (ret / 100)

        # Total P&L for this trade period
        period_pnl = position_pnl + cash_return
        trade_pnls.append(position_pnl)  # Track just the trade P&L

        current_capital += period_pnl
        daily_values.append(current_capital)

        if ret > 0:
            wins += 1
            avg_win.append(ret)
        else:
            losses += 1
            avg_loss.append(ret)
            if trade["reason"] == "stop_loss":
                stop_outs += 1

    # Calculate returns series
    daily_values = np.array(daily_values)
    returns = np.diff(daily_values) / daily_values[:-1]

    # Risk metrics
    n_trades = len(filtered)
    total_days = sum(t["exit_day"] for t in filtered)
    years = total_days / 252.0

    # CAGR
    cagr = (daily_values[-1] / daily_values[0]) ** (1 / years) - 1 if years > 0 else 0

    # Annualized volatility
    vol = np.std(returns) * np.sqrt(252 / holding_period) if len(returns) > 1 else 0

    # Sharpe ratio (assuming 2.5% risk-free rate)
    excess_returns = returns - TBILL_DAILY_YIELD * holding_period
    sharpe = (np.mean(excess_returns) / np.std(excess_returns)) * np.sqrt(252 / holding_period) if np.std(excess_returns) > 0 else 0

    # Sortino (downside deviation)
    downside = returns[returns < 0]
    downside_std = np.std(downside) if len(downside) > 0 else 0
    sortino = (np.mean(returns) / downside_std) * np.sqrt(252 / holding_period) if downside_std > 0 else 0

    # Max drawdown
    cummax = np.maximum.accumulate(daily_values)
    drawdowns = daily_values / cummax - 1
    max_dd = np.min(drawdowns)

    # Calmar ratio
    calmar = cagr / abs(max_dd) if max_dd < 0 else 0

    # Win/Loss statistics
    win_rate = wins / n_trades if n_trades > 0 else 0
    avg_win_ret = np.mean(avg_win) if avg_win else 0
    avg_loss_ret = np.mean(avg_loss) if avg_loss else 0

    # Profit factor
    gross_profits = sum(p for p in trade_pnls if p > 0)
    gross_losses = abs(sum(p for p in trade_pnls if p < 0))
    profit_factor = gross_profits / gross_losses if gross_losses > 0 else float('inf')

    # Expected value per trade
    expected_value = np.mean(trade_pnls) if trade_pnls else 0

    return {
        "holding_period": holding_period,
        "n_trades": n_trades,
        "total_days": total_days,
        "years": years,
        "start_value": INITIAL_CAPITAL,
        "end_value": daily_values[-1],
        "total_pnl": daily_values[-1] - INITIAL_CAPITAL,
        "total_return": (daily_values[-1] / INITIAL_CAPITAL - 1) * 100,
        "cagr": cagr * 100,
        "sharpe": sharpe,
        "sortino": sortino,
        "max_dd": max_dd * 100,
        "calmar": calmar,
        "vol": vol * 100,
        "win_rate": win_rate * 100,
        "wins": wins,
        "losses": losses,
        "stop_outs": stop_outs,
        "avg_win": avg_win_ret,
        "avg_loss": avg_loss_ret,
        "profit_factor": profit_factor,
        "expected_value": expected_value,
        "daily_values": daily_values,
        "returns": returns,
    }


def write_report(all_results, output_file):
    """Write comprehensive analysis report to file."""
    W = 100

    with open(output_file, 'w') as f:
        f.write("=" * W + "\n")
        f.write("DETAILED TRADE ANALYSIS REPORT\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * W + "\n\n")

        f.write("ACCOUNT PARAMETERS:\n")
        f.write(f"  Initial Capital:    ${INITIAL_CAPITAL:,}\n")
        f.write(f"  Position Size:      ${POSITION_SIZE:,.0f} (1/3 of account)\n")
        f.write(f"  Stop-Loss:          {STOP_LOSS_PCT:.0%}\n")
        f.write(f"  Cash Yield:         {TBILL_ANNUAL_YIELD:.1%} annual (T-bill proxy)\n")
        f.write(f"  Holding Periods:    {HOLDING_PERIODS}\n")
        f.write("\n")

        # Analyze by sheet and combined
        sheets = list(set(r["sheet"] for r in all_results))
        sheets.append("COMBINED")

        for sheet in sheets:
            f.write("=" * W + "\n")
            f.write(f"RESULTS: {sheet}\n")
            f.write("=" * W + "\n\n")

            if sheet == "COMBINED":
                sheet_trades = all_results
            else:
                sheet_trades = [r for r in all_results if r["sheet"] == sheet]

            if not sheet_trades:
                f.write("  No valid trades found.\n\n")
                continue

            # Summary table
            f.write("-" * W + "\n")
            f.write("PORTFOLIO PERFORMANCE SUMMARY\n")
            f.write("-" * W + "\n\n")

            f.write(f"  {'Period':<10} {'Trades':>7} {'End Value':>12} {'Total P&L':>12} "
                    f"{'CAGR':>8} {'Sharpe':>8} {'Sortino':>8} {'Max DD':>8} {'Calmar':>8}\n")
            f.write(f"  {'-' * 93}\n")

            for hp in HOLDING_PERIODS:
                perf = simulate_portfolio_with_cash(sheet_trades, hp)
                if perf:
                    f.write(f"  {hp:>7}d  {perf['n_trades']:>7} ${perf['end_value']:>10,.0f} "
                            f"${perf['total_pnl']:>+10,.0f} {perf['cagr']:>+7.1f}% "
                            f"{perf['sharpe']:>8.2f} {perf['sortino']:>8.2f} "
                            f"{perf['max_dd']:>7.1f}% {perf['calmar']:>8.2f}\n")

            f.write("\n")

            # Win/Loss statistics
            f.write("-" * W + "\n")
            f.write("WIN/LOSS STATISTICS\n")
            f.write("-" * W + "\n\n")

            f.write(f"  {'Period':<10} {'Win Rate':>10} {'Wins':>7} {'Losses':>7} "
                    f"{'Stop-outs':>10} {'Avg Win':>10} {'Avg Loss':>10} {'Profit Factor':>14} {'EV/Trade':>12}\n")
            f.write(f"  {'-' * 95}\n")

            for hp in HOLDING_PERIODS:
                perf = simulate_portfolio_with_cash(sheet_trades, hp)
                if perf:
                    pf_str = f"{perf['profit_factor']:.2f}" if perf['profit_factor'] < 100 else "Inf"
                    f.write(f"  {hp:>7}d  {perf['win_rate']:>9.0f}% {perf['wins']:>7} {perf['losses']:>7} "
                            f"{perf['stop_outs']:>10} {perf['avg_win']:>+9.1f}% {perf['avg_loss']:>+9.1f}% "
                            f"{pf_str:>14} ${perf['expected_value']:>+10,.0f}\n")

            f.write("\n")

            # Individual trades for 30-day period
            f.write("-" * W + "\n")
            f.write("INDIVIDUAL TRADES (30-day holding, 5% stop-loss)\n")
            f.write("-" * W + "\n\n")

            hp_trades = [r for r in sheet_trades if r["holding_period"] == 30]
            hp_trades = sorted(hp_trades, key=lambda x: x["trade_date"])

            f.write(f"  {'Ticker':<8} {'Trade Date':<12} {'Entry':>10} {'Exit':>10} "
                    f"{'Return':>10} {'Days':>6} {'Reason':<12} {'Target':>8}\n")
            f.write(f"  {'-' * 85}\n")

            for t in hp_trades:
                target_str = f"{t['target']:.1f}%" if not pd.isna(t.get('target')) else "N/A"
                f.write(f"  {t['ticker']:<8} {t['trade_date'].strftime('%Y-%m-%d'):<12} "
                        f"${t['entry_price']:>9.2f} ${t['exit_price']:>9.2f} "
                        f"{t['return_pct']:>+9.1f}% {t['exit_day']:>5} {t['reason']:<12} {target_str:>8}\n")

            f.write("\n")

            # Return distribution
            f.write("-" * W + "\n")
            f.write("RETURN DISTRIBUTION (30-day period)\n")
            f.write("-" * W + "\n\n")

            returns = [t["return_pct"] for t in hp_trades]
            if returns:
                f.write(f"  Mean Return:        {np.mean(returns):>+8.1f}%\n")
                f.write(f"  Median Return:      {np.median(returns):>+8.1f}%\n")
                f.write(f"  Std Dev:            {np.std(returns):>8.1f}%\n")
                f.write(f"  Min Return:         {np.min(returns):>+8.1f}%\n")
                f.write(f"  Max Return:         {np.max(returns):>+8.1f}%\n")
                f.write(f"  25th Percentile:    {np.percentile(returns, 25):>+8.1f}%\n")
                f.write(f"  75th Percentile:    {np.percentile(returns, 75):>+8.1f}%\n")

            f.write("\n")

        # Pattern analysis
        f.write("=" * W + "\n")
        f.write("PATTERN ANALYSIS\n")
        f.write("=" * W + "\n\n")

        # Unique tickers
        hp30_trades = [r for r in all_results if r["holding_period"] == 30]
        tickers = list(set(r["ticker"] for r in hp30_trades))
        f.write(f"  Total unique tickers: {len(tickers)}\n\n")

        # Repeating tickers
        ticker_counts = defaultdict(int)
        for r in hp30_trades:
            ticker_counts[r["ticker"]] += 1

        repeats = {t: c for t, c in ticker_counts.items() if c > 1}
        if repeats:
            f.write("  Tickers appearing multiple times:\n")
            for t, c in sorted(repeats.items(), key=lambda x: -x[1]):
                f.write(f"    {t}: {c} times\n")
            f.write("\n")

        # Target analysis
        targets = [r["target"] for r in hp30_trades if not pd.isna(r.get("target"))]
        if targets:
            f.write("  Target Returns (from spreadsheet):\n")
            f.write(f"    Mean:   {np.mean(targets):.1f}%\n")
            f.write(f"    Min:    {np.min(targets):.1f}%\n")
            f.write(f"    Max:    {np.max(targets):.1f}%\n\n")

        # Performance by target level
        f.write("  Actual Returns by Target Level (30-day with stop-loss):\n")
        low_target = [r for r in hp30_trades if not pd.isna(r.get("target")) and r["target"] <= 4]
        mid_target = [r for r in hp30_trades if not pd.isna(r.get("target")) and 4 < r["target"] <= 7]
        high_target = [r for r in hp30_trades if not pd.isna(r.get("target")) and r["target"] > 7]

        for name, group in [("Low (<=4%)", low_target), ("Mid (4-7%)", mid_target),
                            ("High (>7%)", high_target)]:
            if group:
                rets = [r["return_pct"] for r in group]
                wins = sum(1 for r in rets if r > 0)
                f.write(f"    {name}: n={len(group)}, avg={np.mean(rets):+.1f}%, "
                        f"win={wins/len(group):.0%}\n")

        f.write("\n")

        # Monthly performance
        f.write("  Performance by Month (30-day period):\n")
        by_month = defaultdict(list)
        for r in hp30_trades:
            month = r["trade_date"].strftime("%Y-%m")
            by_month[month].append(r["return_pct"])

        f.write(f"    {'Month':<10} {'Trades':>7} {'Avg Ret':>10} {'Win Rate':>10}\n")
        f.write(f"    {'-' * 40}\n")
        for month in sorted(by_month.keys()):
            rets = by_month[month]
            wins = sum(1 for r in rets if r > 0)
            f.write(f"    {month:<10} {len(rets):>7} {np.mean(rets):>+9.1f}% {wins/len(rets):>9.0%}\n")

        f.write("\n")

        # Summary
        f.write("=" * W + "\n")
        f.write("EXECUTIVE SUMMARY\n")
        f.write("=" * W + "\n\n")

        perf_30 = simulate_portfolio_with_cash(all_results, 30)
        perf_20 = simulate_portfolio_with_cash(all_results, 20)
        perf_10 = simulate_portfolio_with_cash(all_results, 10)

        if perf_30 and perf_20 and perf_10:
            best_hp = max([perf_10, perf_20, perf_30], key=lambda x: x["sharpe"])
            f.write(f"  Best risk-adjusted holding period: {best_hp['holding_period']} days\n")
            f.write(f"    Sharpe:  {best_hp['sharpe']:.2f}\n")
            f.write(f"    Sortino: {best_hp['sortino']:.2f}\n")
            f.write(f"    CAGR:    {best_hp['cagr']:+.1f}%\n")
            f.write(f"    Max DD:  {best_hp['max_dd']:.1f}%\n")
            f.write("\n")

            # Optimal holding period comparison
            f.write("  Holding Period Comparison:\n")
            f.write(f"    10-day: Sharpe {perf_10['sharpe']:.2f}, Win Rate {perf_10['win_rate']:.0f}%, "
                    f"Max DD {perf_10['max_dd']:.1f}%\n")
            f.write(f"    20-day: Sharpe {perf_20['sharpe']:.2f}, Win Rate {perf_20['win_rate']:.0f}%, "
                    f"Max DD {perf_20['max_dd']:.1f}%\n")
            f.write(f"    30-day: Sharpe {perf_30['sharpe']:.2f}, Win Rate {perf_30['win_rate']:.0f}%, "
                    f"Max DD {perf_30['max_dd']:.1f}%\n")

        f.write("\n" + "=" * W + "\n")
        f.write("END OF REPORT\n")
        f.write("=" * W + "\n")

    print(f"Report saved to: {output_file}")


def main():
    print("=" * 70)
    print("Detailed Trade Analysis")
    print("=" * 70)

    xlsx_path = os.path.join(_this_dir, "Data for Claude.xlsx")

    print("\nLoading trades from Excel file...")
    xl = pd.ExcelFile(xlsx_path)

    all_results = []
    for sheet_name in xl.sheet_names:
        print(f"\nAnalyzing: {sheet_name}")
        df = pd.read_excel(xl, sheet_name=sheet_name)
        results = analyze_trades(df, sheet_name)
        all_results.extend(results)
        print(f"  Found {len([r for r in results if r['holding_period'] == 30])} valid trades")

    # Write report
    output_file = os.path.join(_this_dir, "trade_analysis_report.txt")
    write_report(all_results, output_file)

    print("\nDone!")


if __name__ == "__main__":
    main()
