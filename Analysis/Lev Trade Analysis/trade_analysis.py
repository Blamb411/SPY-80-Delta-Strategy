"""
Trade Analysis
===============
Analyzes trades from 'Data for Claude.xlsx' sheets.

Simulates a $50,000 account with margin:
  - Each position is ~1/3 of initial cash (~$16,667)
  - Holding periods: 10, 20, and 30 days
  - 5% stop-loss that liquidates position if price drops 5%+ from entry

Fetches historical daily prices from Yahoo Finance.

Usage:
    python trade_analysis.py
"""

import os
import sys
import numpy as np
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta

_this_dir = os.path.dirname(os.path.abspath(__file__))

INITIAL_CAPITAL = 50_000
POSITION_SIZE = INITIAL_CAPITAL / 3  # ~$16,667 per trade
STOP_LOSS_PCT = 0.05  # 5%

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
        print(f"    Error fetching {ticker}: {e}")
        return None


def simulate_trade(entry_price, prices_after_entry, holding_period, stop_loss_pct):
    """
    Simulate a trade with stop-loss.

    Parameters:
        entry_price: float, price at entry
        prices_after_entry: Series of prices for days after entry
        holding_period: int, max days to hold (10, 20, or 30)
        stop_loss_pct: float, stop-loss threshold (0.05 = 5%)

    Returns:
        dict with exit_price, exit_day, return_pct, reason
    """
    stop_price = entry_price * (1 - stop_loss_pct)

    for day, price in enumerate(prices_after_entry.values[:holding_period], 1):
        if price <= stop_price:
            return {
                "exit_price": price,
                "exit_day": day,
                "return_pct": (price / entry_price - 1) * 100,
                "reason": "stop_loss",
            }

    # Hold to end of period
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


def analyze_sheet(df, sheet_name):
    """Analyze trades from one sheet."""
    print(f"\n{'=' * 70}")
    print(f"Analyzing: {sheet_name}")
    print(f"{'=' * 70}")

    results = []

    for idx, row in df.iterrows():
        ticker = row['ticker']
        trade_date = pd.to_datetime(row['trade date'])
        target = row.get('target', np.nan)

        # Fetch prices: from trade date to +40 days to cover 30-day hold
        start = trade_date - timedelta(days=5)
        end = trade_date + timedelta(days=50)

        print(f"\n  Processing {ticker} ({trade_date.strftime('%Y-%m-%d')})...")
        prices = fetch_price_history(ticker, start.strftime('%Y-%m-%d'),
                                     end.strftime('%Y-%m-%d'))

        if prices is None or len(prices) == 0:
            print(f"    No price data available for {ticker}")
            continue

        # Find entry price (trade date close)
        trade_date_str = trade_date.strftime('%Y-%m-%d')

        # Find the closest trading day to trade_date
        entry_idx = None
        for i, dt in enumerate(prices.index):
            if dt.strftime('%Y-%m-%d') >= trade_date_str:
                entry_idx = i
                break

        if entry_idx is None:
            print(f"    Could not find entry date for {ticker}")
            continue

        entry_price = prices.iloc[entry_idx]
        prices_after = prices.iloc[entry_idx + 1:]

        if len(prices_after) == 0:
            print(f"    No price data after entry for {ticker}")
            continue

        print(f"    Entry: ${entry_price:.2f} on {prices.index[entry_idx].strftime('%Y-%m-%d')}")

        for hp in HOLDING_PERIODS:
            result = simulate_trade(entry_price, prices_after, hp, STOP_LOSS_PCT)
            if result:
                results.append({
                    "sheet": sheet_name,
                    "ticker": ticker,
                    "trade_date": trade_date,
                    "entry_price": entry_price,
                    "target": target,
                    "holding_period": hp,
                    "exit_price": result["exit_price"],
                    "exit_day": result["exit_day"],
                    "return_pct": result["return_pct"],
                    "reason": result["reason"],
                })

    return results


def compute_portfolio_performance(results, holding_period):
    """
    Compute portfolio performance for a specific holding period.
    Assumes sequential execution with ~$16,667 per trade.
    """
    filtered = [r for r in results if r["holding_period"] == holding_period]
    if not filtered:
        return None

    # Sort by trade date
    filtered = sorted(filtered, key=lambda x: x["trade_date"])

    portfolio_value = INITIAL_CAPITAL
    trades_executed = 0
    wins = 0
    losses = 0
    stop_outs = 0
    total_return_pct = 0

    for trade in filtered:
        ret = trade["return_pct"]
        reason = trade["reason"]

        # Apply return to position size
        position_pnl = POSITION_SIZE * (ret / 100)
        portfolio_value += position_pnl
        trades_executed += 1
        total_return_pct += ret

        if ret > 0:
            wins += 1
        else:
            losses += 1
            if reason == "stop_loss":
                stop_outs += 1

    avg_return = total_return_pct / trades_executed if trades_executed > 0 else 0
    win_rate = wins / trades_executed if trades_executed > 0 else 0

    return {
        "holding_period": holding_period,
        "n_trades": trades_executed,
        "start_value": INITIAL_CAPITAL,
        "end_value": portfolio_value,
        "total_pnl": portfolio_value - INITIAL_CAPITAL,
        "total_return": (portfolio_value / INITIAL_CAPITAL - 1) * 100,
        "avg_trade_return": avg_return,
        "win_rate": win_rate,
        "wins": wins,
        "losses": losses,
        "stop_outs": stop_outs,
    }


def print_results(all_results, sheet_name):
    """Print analysis results for a sheet."""
    W = 90
    print(f"\n\n{'=' * W}")
    print(f"RESULTS: {sheet_name}")
    print(f"{'=' * W}")

    sheet_results = [r for r in all_results if r["sheet"] == sheet_name]

    if not sheet_results:
        print("  No valid trades found.")
        return

    # Summary by holding period
    print(f"\n  PORTFOLIO PERFORMANCE (${INITIAL_CAPITAL:,} account, ${POSITION_SIZE:,.0f}/trade)")
    print(f"  5% stop-loss applied")
    print(f"\n  {'Holding':>10} {'Trades':>8} {'End Value':>12} {'Total P&L':>12} "
          f"{'Return':>8} {'Win Rate':>10} {'Stop-outs':>10}")
    print(f"  {'-' * 78}")

    for hp in HOLDING_PERIODS:
        perf = compute_portfolio_performance(sheet_results, hp)
        if perf:
            print(f"  {hp:>7}d {perf['n_trades']:>8} ${perf['end_value']:>11,.0f} "
                  f"${perf['total_pnl']:>+11,.0f} {perf['total_return']:>+7.1f}% "
                  f"{perf['win_rate']:>9.0%} {perf['stop_outs']:>10}")

    # Individual trade details
    print(f"\n  INDIVIDUAL TRADES (30-day holding period with 5% stop-loss)")
    print(f"\n  {'Ticker':<8} {'Trade Date':<12} {'Entry':>8} {'Exit':>8} "
          f"{'Return':>8} {'Days':>6} {'Reason':<12}")
    print(f"  {'-' * 72}")

    hp_results = [r for r in sheet_results if r["holding_period"] == 30]
    hp_results = sorted(hp_results, key=lambda x: x["trade_date"])

    for r in hp_results:
        print(f"  {r['ticker']:<8} {r['trade_date'].strftime('%Y-%m-%d'):<12} "
              f"${r['entry_price']:>7.2f} ${r['exit_price']:>7.2f} "
              f"{r['return_pct']:>+7.1f}% {r['exit_day']:>5} {r['reason']:<12}")

    # Win/loss distribution
    print(f"\n  RETURN DISTRIBUTION (30-day period)")
    returns = [r["return_pct"] for r in hp_results]
    if returns:
        print(f"    Mean:   {np.mean(returns):>+6.1f}%")
        print(f"    Median: {np.median(returns):>+6.1f}%")
        print(f"    Min:    {np.min(returns):>+6.1f}%")
        print(f"    Max:    {np.max(returns):>+6.1f}%")
        print(f"    Std:    {np.std(returns):>6.1f}%")


def analyze_patterns(all_results):
    """Look for patterns in the trade selections."""
    W = 90
    print(f"\n\n{'=' * W}")
    print("PATTERN ANALYSIS")
    print(f"{'=' * W}")

    # Get unique tickers
    tickers = list(set(r["ticker"] for r in all_results))
    print(f"\n  Total unique tickers: {len(tickers)}")

    # Sector analysis would require additional data
    # Let's look at what we can discern from the data

    # Check for repeating tickers
    ticker_counts = {}
    for r in all_results:
        if r["holding_period"] == 30:  # Only count once per trade
            ticker = r["ticker"]
            if ticker not in ticker_counts:
                ticker_counts[ticker] = 0
            ticker_counts[ticker] += 1

    repeats = {t: c for t, c in ticker_counts.items() if c > 1}
    if repeats:
        print(f"\n  Tickers appearing multiple times:")
        for t, c in sorted(repeats.items(), key=lambda x: -x[1]):
            print(f"    {t}: {c} times")

    # Target return analysis
    print(f"\n  Target Returns (from spreadsheet):")
    targets = [r["target"] for r in all_results if r["holding_period"] == 30
               and not pd.isna(r.get("target", np.nan))]
    if targets:
        print(f"    Mean target:   {np.mean(targets):.1f}%")
        print(f"    Min target:    {np.min(targets):.1f}%")
        print(f"    Max target:    {np.max(targets):.1f}%")

    # Performance by target level
    print(f"\n  Actual Returns by Target Level (30-day with stop-loss):")
    hp_results = [r for r in all_results if r["holding_period"] == 30]

    # Group by target buckets
    low_target = [r for r in hp_results if not pd.isna(r.get("target")) and r["target"] <= 4]
    mid_target = [r for r in hp_results if not pd.isna(r.get("target")) and 4 < r["target"] <= 7]
    high_target = [r for r in hp_results if not pd.isna(r.get("target")) and r["target"] > 7]

    for name, group in [("Low (<=4%)", low_target), ("Mid (4-7%)", mid_target),
                        ("High (>7%)", high_target)]:
        if group:
            returns = [r["return_pct"] for r in group]
            wins = sum(1 for r in returns if r > 0)
            print(f"    {name}: n={len(group)}, avg={np.mean(returns):+.1f}%, "
                  f"win={wins/len(group):.0%}")

    # Temporal pattern
    print(f"\n  Performance by Month (30-day period):")
    by_month = {}
    for r in hp_results:
        month = r["trade_date"].strftime("%Y-%m")
        if month not in by_month:
            by_month[month] = []
        by_month[month].append(r["return_pct"])

    for month in sorted(by_month.keys()):
        rets = by_month[month]
        avg = np.mean(rets)
        print(f"    {month}: n={len(rets)}, avg={avg:+.1f}%")


def main():
    print("=" * 70)
    print("Trade Analysis: Data for Claude")
    print(f"  Account: ${INITIAL_CAPITAL:,}")
    print(f"  Position size: ~${POSITION_SIZE:,.0f} (1/3 of account)")
    print(f"  Stop-loss: {STOP_LOSS_PCT:.0%}")
    print(f"  Holding periods: {HOLDING_PERIODS}")
    print("=" * 70)

    xlsx_path = os.path.join(_this_dir, "Data for Claude.xlsx")
    xl = pd.ExcelFile(xlsx_path)

    all_results = []

    for sheet_name in xl.sheet_names:
        df = pd.read_excel(xl, sheet_name=sheet_name)
        results = analyze_sheet(df, sheet_name)
        all_results.extend(results)

    # Print results for each sheet
    for sheet_name in xl.sheet_names:
        print_results(all_results, sheet_name)

    # Combined analysis
    print_results(all_results, "COMBINED (both sheets)")

    # Pattern analysis
    analyze_patterns(all_results)

    print(f"\n\n{'=' * 70}")
    print("Done!")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
