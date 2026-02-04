"""
Stop-Loss Sensitivity Analysis
===============================
Analyzes Lev's trades across different stop-loss levels and holding periods.

Matrix:
  Stop-Loss:      5%, 10%, 15%, 20%
  Holding Period: 10, 20, 30 days

Usage:
    python stop_loss_sensitivity.py
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
TBILL_ANNUAL_YIELD = 0.025  # 2.5% annual yield on idle cash
TBILL_DAILY_YIELD = (1 + TBILL_ANNUAL_YIELD) ** (1/252) - 1

# Sensitivity parameters
STOP_LOSSES = [0.05, 0.10, 0.15, 0.20]  # 5%, 10%, 15%, 20%
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


def load_and_fetch_prices(xlsx_path):
    """Load trades and fetch all price data once."""
    print("Loading trades from Excel file...")
    xl = pd.ExcelFile(xlsx_path)

    trades_raw = []
    for sheet_name in xl.sheet_names:
        df = pd.read_excel(xl, sheet_name=sheet_name)
        for idx, row in df.iterrows():
            trades_raw.append({
                "sheet": sheet_name,
                "ticker": row['ticker'],
                "trade_date": pd.to_datetime(row['trade date']),
                "target": row.get('target', np.nan),
            })

    print(f"Found {len(trades_raw)} trades across {len(xl.sheet_names)} sheets")
    print("\nFetching price data...")

    trades_with_prices = []
    for i, trade in enumerate(trades_raw):
        ticker = trade["ticker"]
        trade_date = trade["trade_date"]

        start = trade_date - timedelta(days=5)
        end = trade_date + timedelta(days=50)

        prices = fetch_price_history(ticker, start.strftime('%Y-%m-%d'),
                                     end.strftime('%Y-%m-%d'))

        if prices is None or len(prices) == 0:
            continue

        trade_date_str = trade_date.strftime('%Y-%m-%d')
        entry_idx = None
        for j, dt in enumerate(prices.index):
            if dt.strftime('%Y-%m-%d') >= trade_date_str:
                entry_idx = j
                break

        if entry_idx is None:
            continue

        entry_price = prices.iloc[entry_idx]
        entry_date_actual = prices.index[entry_idx]
        prices_after = prices.iloc[entry_idx + 1:]

        if len(prices_after) == 0:
            continue

        trades_with_prices.append({
            "sheet": trade["sheet"],
            "ticker": ticker,
            "trade_date": entry_date_actual,
            "entry_price": entry_price,
            "target": trade["target"],
            "prices_after": prices_after,
        })

        if (i + 1) % 10 == 0:
            print(f"  Processed {i + 1}/{len(trades_raw)} trades...")

    print(f"\n{len(trades_with_prices)} trades with valid price data")
    return trades_with_prices


def run_simulation(trades_with_prices, stop_loss_pct, holding_period):
    """Run portfolio simulation for specific SL/HP combination."""
    results = []

    for trade in trades_with_prices:
        result = simulate_trade(
            trade["entry_price"],
            trade["prices_after"],
            holding_period,
            stop_loss_pct
        )
        if result:
            results.append({
                "sheet": trade["sheet"],
                "ticker": trade["ticker"],
                "trade_date": trade["trade_date"],
                "entry_price": trade["entry_price"],
                "exit_price": result["exit_price"],
                "exit_day": result["exit_day"],
                "return_pct": result["return_pct"],
                "reason": result["reason"],
            })

    if not results:
        return None

    # Sort by trade date
    results = sorted(results, key=lambda x: x["trade_date"])

    # Track daily portfolio value
    daily_values = [INITIAL_CAPITAL]
    current_capital = INITIAL_CAPITAL

    # Trade statistics
    trade_pnls = []
    wins = 0
    losses = 0
    stop_outs = 0
    avg_win = []
    avg_loss = []

    for trade in results:
        ret = trade["return_pct"]
        days_held = trade["exit_day"]

        # Cash not in position earns T-bill rate
        cash_portion = INITIAL_CAPITAL - POSITION_SIZE
        cash_return = cash_portion * ((1 + TBILL_DAILY_YIELD) ** days_held - 1)

        # Position return
        position_pnl = POSITION_SIZE * (ret / 100)

        # Total P&L for this trade period
        period_pnl = position_pnl + cash_return
        trade_pnls.append(position_pnl)

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

    # Calculate metrics
    daily_values = np.array(daily_values)
    returns = np.diff(daily_values) / daily_values[:-1]

    n_trades = len(results)
    total_days = sum(t["exit_day"] for t in results)
    years = total_days / 252.0

    # CAGR
    cagr = (daily_values[-1] / daily_values[0]) ** (1 / years) - 1 if years > 0 else 0

    # Annualized volatility
    vol = np.std(returns) * np.sqrt(252 / holding_period) if len(returns) > 1 else 0

    # Sharpe ratio
    excess_returns = returns - TBILL_DAILY_YIELD * holding_period
    sharpe = (np.mean(excess_returns) / np.std(excess_returns)) * np.sqrt(252 / holding_period) if np.std(excess_returns) > 0 else 0

    # Sortino
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
        "stop_loss": stop_loss_pct,
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
    }


def write_report(all_results, output_file):
    """Write sensitivity analysis report."""
    W = 110

    with open(output_file, 'w') as f:
        f.write("=" * W + "\n")
        f.write("STOP-LOSS SENSITIVITY ANALYSIS\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * W + "\n\n")

        f.write("PARAMETERS:\n")
        f.write(f"  Initial Capital:    ${INITIAL_CAPITAL:,}\n")
        f.write(f"  Position Size:      ${POSITION_SIZE:,.0f} (1/3 of account)\n")
        f.write(f"  Stop-Losses:        {[f'{sl:.0%}' for sl in STOP_LOSSES]}\n")
        f.write(f"  Holding Periods:    {HOLDING_PERIODS} days\n")
        f.write(f"  Cash Yield:         {TBILL_ANNUAL_YIELD:.1%} annual (T-bill proxy)\n")
        f.write("\n")

        # Create a matrix view
        f.write("=" * W + "\n")
        f.write("SENSITIVITY MATRIX: SHARPE RATIO\n")
        f.write("=" * W + "\n\n")

        # Header
        f.write(f"  {'Stop-Loss':<12}")
        for hp in HOLDING_PERIODS:
            f.write(f"  {hp:>6}d")
        f.write("\n")
        f.write(f"  {'-' * 40}\n")

        for sl in STOP_LOSSES:
            f.write(f"  {sl:>10.0%}  ")
            for hp in HOLDING_PERIODS:
                key = (sl, hp)
                if key in all_results:
                    f.write(f"  {all_results[key]['sharpe']:>6.2f}")
                else:
                    f.write(f"  {'N/A':>6}")
            f.write("\n")

        f.write("\n")

        # Sortino matrix
        f.write("=" * W + "\n")
        f.write("SENSITIVITY MATRIX: SORTINO RATIO\n")
        f.write("=" * W + "\n\n")

        f.write(f"  {'Stop-Loss':<12}")
        for hp in HOLDING_PERIODS:
            f.write(f"  {hp:>6}d")
        f.write("\n")
        f.write(f"  {'-' * 40}\n")

        for sl in STOP_LOSSES:
            f.write(f"  {sl:>10.0%}  ")
            for hp in HOLDING_PERIODS:
                key = (sl, hp)
                if key in all_results:
                    f.write(f"  {all_results[key]['sortino']:>6.2f}")
                else:
                    f.write(f"  {'N/A':>6}")
            f.write("\n")

        f.write("\n")

        # CAGR matrix
        f.write("=" * W + "\n")
        f.write("SENSITIVITY MATRIX: CAGR\n")
        f.write("=" * W + "\n\n")

        f.write(f"  {'Stop-Loss':<12}")
        for hp in HOLDING_PERIODS:
            f.write(f"  {hp:>6}d")
        f.write("\n")
        f.write(f"  {'-' * 40}\n")

        for sl in STOP_LOSSES:
            f.write(f"  {sl:>10.0%}  ")
            for hp in HOLDING_PERIODS:
                key = (sl, hp)
                if key in all_results:
                    f.write(f"  {all_results[key]['cagr']:>+5.0f}%")
                else:
                    f.write(f"  {'N/A':>6}")
            f.write("\n")

        f.write("\n")

        # Max Drawdown matrix
        f.write("=" * W + "\n")
        f.write("SENSITIVITY MATRIX: MAX DRAWDOWN\n")
        f.write("=" * W + "\n\n")

        f.write(f"  {'Stop-Loss':<12}")
        for hp in HOLDING_PERIODS:
            f.write(f"  {hp:>6}d")
        f.write("\n")
        f.write(f"  {'-' * 40}\n")

        for sl in STOP_LOSSES:
            f.write(f"  {sl:>10.0%}  ")
            for hp in HOLDING_PERIODS:
                key = (sl, hp)
                if key in all_results:
                    f.write(f"  {all_results[key]['max_dd']:>5.0f}%")
                else:
                    f.write(f"  {'N/A':>6}")
            f.write("\n")

        f.write("\n")

        # Win Rate matrix
        f.write("=" * W + "\n")
        f.write("SENSITIVITY MATRIX: WIN RATE\n")
        f.write("=" * W + "\n\n")

        f.write(f"  {'Stop-Loss':<12}")
        for hp in HOLDING_PERIODS:
            f.write(f"  {hp:>6}d")
        f.write("\n")
        f.write(f"  {'-' * 40}\n")

        for sl in STOP_LOSSES:
            f.write(f"  {sl:>10.0%}  ")
            for hp in HOLDING_PERIODS:
                key = (sl, hp)
                if key in all_results:
                    f.write(f"  {all_results[key]['win_rate']:>5.0f}%")
                else:
                    f.write(f"  {'N/A':>6}")
            f.write("\n")

        f.write("\n")

        # Stop-out frequency matrix
        f.write("=" * W + "\n")
        f.write("SENSITIVITY MATRIX: STOP-OUT RATE (% of trades hitting stop-loss)\n")
        f.write("=" * W + "\n\n")

        f.write(f"  {'Stop-Loss':<12}")
        for hp in HOLDING_PERIODS:
            f.write(f"  {hp:>6}d")
        f.write("\n")
        f.write(f"  {'-' * 40}\n")

        for sl in STOP_LOSSES:
            f.write(f"  {sl:>10.0%}  ")
            for hp in HOLDING_PERIODS:
                key = (sl, hp)
                if key in all_results:
                    r = all_results[key]
                    stop_rate = (r['stop_outs'] / r['n_trades'] * 100) if r['n_trades'] > 0 else 0
                    f.write(f"  {stop_rate:>5.0f}%")
                else:
                    f.write(f"  {'N/A':>6}")
            f.write("\n")

        f.write("\n")

        # End Value matrix
        f.write("=" * W + "\n")
        f.write("SENSITIVITY MATRIX: ENDING PORTFOLIO VALUE\n")
        f.write("=" * W + "\n\n")

        f.write(f"  {'Stop-Loss':<12}")
        for hp in HOLDING_PERIODS:
            f.write(f"  {hp:>10}d")
        f.write("\n")
        f.write(f"  {'-' * 50}\n")

        for sl in STOP_LOSSES:
            f.write(f"  {sl:>10.0%}  ")
            for hp in HOLDING_PERIODS:
                key = (sl, hp)
                if key in all_results:
                    f.write(f"  ${all_results[key]['end_value']:>9,.0f}")
                else:
                    f.write(f"  {'N/A':>10}")
            f.write("\n")

        f.write("\n")

        # Profit Factor matrix
        f.write("=" * W + "\n")
        f.write("SENSITIVITY MATRIX: PROFIT FACTOR\n")
        f.write("=" * W + "\n\n")

        f.write(f"  {'Stop-Loss':<12}")
        for hp in HOLDING_PERIODS:
            f.write(f"  {hp:>6}d")
        f.write("\n")
        f.write(f"  {'-' * 40}\n")

        for sl in STOP_LOSSES:
            f.write(f"  {sl:>10.0%}  ")
            for hp in HOLDING_PERIODS:
                key = (sl, hp)
                if key in all_results:
                    pf = all_results[key]['profit_factor']
                    if pf > 100:
                        f.write(f"  {'Inf':>6}")
                    else:
                        f.write(f"  {pf:>6.2f}")
                else:
                    f.write(f"  {'N/A':>6}")
            f.write("\n")

        f.write("\n")

        # Detailed results for each combination
        f.write("=" * W + "\n")
        f.write("DETAILED RESULTS BY COMBINATION\n")
        f.write("=" * W + "\n\n")

        for sl in STOP_LOSSES:
            for hp in HOLDING_PERIODS:
                key = (sl, hp)
                if key not in all_results:
                    continue

                r = all_results[key]
                f.write(f"-" * W + "\n")
                f.write(f"Stop-Loss: {sl:.0%}  |  Holding Period: {hp} days\n")
                f.write(f"-" * W + "\n")

                f.write(f"  Trades:          {r['n_trades']}\n")
                f.write(f"  End Value:       ${r['end_value']:,.0f}\n")
                f.write(f"  Total P&L:       ${r['total_pnl']:+,.0f}\n")
                f.write(f"  Total Return:    {r['total_return']:+.1f}%\n")
                f.write(f"  CAGR:            {r['cagr']:+.1f}%\n")
                f.write(f"  Sharpe:          {r['sharpe']:.2f}\n")
                f.write(f"  Sortino:         {r['sortino']:.2f}\n")
                f.write(f"  Max Drawdown:    {r['max_dd']:.1f}%\n")
                f.write(f"  Calmar:          {r['calmar']:.2f}\n")
                f.write(f"  Volatility:      {r['vol']:.1f}%\n")
                f.write(f"  Win Rate:        {r['win_rate']:.0f}% ({r['wins']}W / {r['losses']}L)\n")
                f.write(f"  Stop-outs:       {r['stop_outs']} ({r['stop_outs']/r['n_trades']*100:.0f}% of trades)\n")
                f.write(f"  Avg Win:         {r['avg_win']:+.1f}%\n")
                f.write(f"  Avg Loss:        {r['avg_loss']:+.1f}%\n")
                pf_str = f"{r['profit_factor']:.2f}" if r['profit_factor'] < 100 else "Inf"
                f.write(f"  Profit Factor:   {pf_str}\n")
                f.write(f"  EV per Trade:    ${r['expected_value']:+,.0f}\n")
                f.write("\n")

        # Summary / Best combination
        f.write("=" * W + "\n")
        f.write("OPTIMAL COMBINATIONS\n")
        f.write("=" * W + "\n\n")

        results_list = list(all_results.values())

        # Best Sharpe
        best_sharpe = max(results_list, key=lambda x: x['sharpe'])
        f.write(f"  Best Sharpe Ratio:\n")
        f.write(f"    Stop-Loss: {best_sharpe['stop_loss']:.0%}, HP: {best_sharpe['holding_period']}d\n")
        f.write(f"    Sharpe: {best_sharpe['sharpe']:.2f}, CAGR: {best_sharpe['cagr']:+.1f}%, "
                f"Max DD: {best_sharpe['max_dd']:.1f}%, Win Rate: {best_sharpe['win_rate']:.0f}%\n\n")

        # Best Sortino
        best_sortino = max(results_list, key=lambda x: x['sortino'])
        f.write(f"  Best Sortino Ratio:\n")
        f.write(f"    Stop-Loss: {best_sortino['stop_loss']:.0%}, HP: {best_sortino['holding_period']}d\n")
        f.write(f"    Sortino: {best_sortino['sortino']:.2f}, CAGR: {best_sortino['cagr']:+.1f}%, "
                f"Max DD: {best_sortino['max_dd']:.1f}%, Win Rate: {best_sortino['win_rate']:.0f}%\n\n")

        # Best CAGR
        best_cagr = max(results_list, key=lambda x: x['cagr'])
        f.write(f"  Best CAGR:\n")
        f.write(f"    Stop-Loss: {best_cagr['stop_loss']:.0%}, HP: {best_cagr['holding_period']}d\n")
        f.write(f"    CAGR: {best_cagr['cagr']:+.1f}%, Sharpe: {best_cagr['sharpe']:.2f}, "
                f"Max DD: {best_cagr['max_dd']:.1f}%, Win Rate: {best_cagr['win_rate']:.0f}%\n\n")

        # Lowest Max DD (best = closest to 0)
        best_dd = max(results_list, key=lambda x: x['max_dd'])  # max because values are negative
        f.write(f"  Lowest Max Drawdown:\n")
        f.write(f"    Stop-Loss: {best_dd['stop_loss']:.0%}, HP: {best_dd['holding_period']}d\n")
        f.write(f"    Max DD: {best_dd['max_dd']:.1f}%, CAGR: {best_dd['cagr']:+.1f}%, "
                f"Sharpe: {best_dd['sharpe']:.2f}, Win Rate: {best_dd['win_rate']:.0f}%\n\n")

        # Highest Win Rate
        best_wr = max(results_list, key=lambda x: x['win_rate'])
        f.write(f"  Highest Win Rate:\n")
        f.write(f"    Stop-Loss: {best_wr['stop_loss']:.0%}, HP: {best_wr['holding_period']}d\n")
        f.write(f"    Win Rate: {best_wr['win_rate']:.0f}%, CAGR: {best_wr['cagr']:+.1f}%, "
                f"Sharpe: {best_wr['sharpe']:.2f}, Max DD: {best_wr['max_dd']:.1f}%\n\n")

        # Key observations
        f.write("=" * W + "\n")
        f.write("KEY OBSERVATIONS\n")
        f.write("=" * W + "\n\n")

        # Calculate averages by stop-loss level
        f.write("  Average Metrics by Stop-Loss Level:\n")
        f.write(f"    {'SL':<8} {'Sharpe':>8} {'Sortino':>8} {'CAGR':>8} {'Max DD':>8} {'Win Rate':>10}\n")
        f.write(f"    {'-' * 52}\n")

        for sl in STOP_LOSSES:
            sl_results = [r for r in results_list if r['stop_loss'] == sl]
            if sl_results:
                avg_sharpe = np.mean([r['sharpe'] for r in sl_results])
                avg_sortino = np.mean([r['sortino'] for r in sl_results])
                avg_cagr = np.mean([r['cagr'] for r in sl_results])
                avg_dd = np.mean([r['max_dd'] for r in sl_results])
                avg_wr = np.mean([r['win_rate'] for r in sl_results])
                f.write(f"    {sl:>6.0%}  {avg_sharpe:>8.2f} {avg_sortino:>8.2f} "
                        f"{avg_cagr:>+7.1f}% {avg_dd:>7.1f}% {avg_wr:>9.0f}%\n")

        f.write("\n")

        # Calculate averages by holding period
        f.write("  Average Metrics by Holding Period:\n")
        f.write(f"    {'HP':<8} {'Sharpe':>8} {'Sortino':>8} {'CAGR':>8} {'Max DD':>8} {'Win Rate':>10}\n")
        f.write(f"    {'-' * 52}\n")

        for hp in HOLDING_PERIODS:
            hp_results = [r for r in results_list if r['holding_period'] == hp]
            if hp_results:
                avg_sharpe = np.mean([r['sharpe'] for r in hp_results])
                avg_sortino = np.mean([r['sortino'] for r in hp_results])
                avg_cagr = np.mean([r['cagr'] for r in hp_results])
                avg_dd = np.mean([r['max_dd'] for r in hp_results])
                avg_wr = np.mean([r['win_rate'] for r in hp_results])
                f.write(f"    {hp:>5}d   {avg_sharpe:>8.2f} {avg_sortino:>8.2f} "
                        f"{avg_cagr:>+7.1f}% {avg_dd:>7.1f}% {avg_wr:>9.0f}%\n")

        f.write("\n" + "=" * W + "\n")
        f.write("END OF REPORT\n")
        f.write("=" * W + "\n")

    print(f"Report saved to: {output_file}")


def main():
    print("=" * 70)
    print("Stop-Loss Sensitivity Analysis")
    print("=" * 70)

    xlsx_path = os.path.join(_this_dir, "Data for Claude.xlsx")

    # Load trades and fetch prices once
    trades_with_prices = load_and_fetch_prices(xlsx_path)

    if not trades_with_prices:
        print("No valid trades found!")
        return

    # Run simulations for all combinations
    print("\nRunning sensitivity analysis...")
    all_results = {}

    for sl in STOP_LOSSES:
        for hp in HOLDING_PERIODS:
            result = run_simulation(trades_with_prices, sl, hp)
            if result:
                all_results[(sl, hp)] = result
                print(f"  SL={sl:.0%}, HP={hp}d: Sharpe={result['sharpe']:.2f}, "
                      f"CAGR={result['cagr']:+.1f}%, MaxDD={result['max_dd']:.1f}%")

    # Write report
    output_file = os.path.join(_this_dir, "stop_loss_sensitivity_report.txt")
    write_report(all_results, output_file)

    print("\nDone!")


if __name__ == "__main__":
    main()
