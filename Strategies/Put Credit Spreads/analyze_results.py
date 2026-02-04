#!/usr/bin/env python3
"""
Analyze backtest results with comprehensive risk metrics.
"""

import sys
import json
import math
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# Add scipy for statistical functions
try:
    from scipy import stats
except ImportError:
    print("Installing scipy...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "scipy", "-q"])
    from scipy import stats

from backtest.ibkr_data_fetcher import load_from_cache, is_cache_valid
from backtest.put_spread_backtest import run_put_spread_backtest_multi
import logging
logging.disable(logging.INFO)


def main():
    # Load all cached symbols
    symbol_data = {}
    cache_dir = Path('backtest/cache')
    for f in cache_dir.glob('*_hist.json'):
        symbol = f.stem.replace('_hist', '')
        if is_cache_valid(symbol, max_age_days=365):
            data = load_from_cache(symbol)
            if data:
                symbol_data[symbol] = data

    print("Running backtest with optimal settings: 50% TP / 75% SL")
    print("Using realistic pricing: 5% bid/ask spread + IV skew")
    print("=" * 70)

    # Run with optimal settings
    results = run_put_spread_backtest_multi(
        symbol_data,
        entry_interval_days=5,
        use_early_exit=True,
        take_profit_pct=0.50,
        stop_loss_pct=0.75,
        use_realistic_pricing=True,
        bid_ask_spread_pct=0.05,
        use_skew=True,
    )

    # Collect all trades
    all_trades = []
    for r in results.values():
        all_trades.extend(r.trades)

    # Sort by entry date
    all_trades.sort(key=lambda t: t.entry_date)

    total_trades = len(all_trades)
    winning = sum(1 for t in all_trades if t.won)
    losing = total_trades - winning
    total_pnl = sum(t.pnl for t in all_trades)

    print(f"Total Trades: {total_trades}")
    print(f"Winners: {winning} ({winning/total_trades*100:.1f}%)")
    print(f"Losers: {losing} ({losing/total_trades*100:.1f}%)")
    print(f"Total P&L: ${total_pnl:,.2f}")
    print()

    # Track cumulative P&L and drawdown
    cumulative_pnl = []
    running_pnl = 0
    for trade in all_trades:
        running_pnl += trade.pnl
        cumulative_pnl.append(running_pnl)

    # Calculate max drawdown
    peak = 0
    max_drawdown = 0
    for pnl in cumulative_pnl:
        if pnl > peak:
            peak = pnl
        drawdown = peak - pnl
        if drawdown > max_drawdown:
            max_drawdown = drawdown

    # Track concurrent positions to find max capital at risk
    open_positions = []
    max_capital = 0
    capital_over_time = []

    for trade in all_trades:
        entry_date = trade.entry_date

        # Remove expired/closed positions (estimate based on avg 9-day hold)
        # Parse actual exit date from reason
        if 'day' in trade.reason:
            try:
                days = int(trade.reason.split('day ')[1])
            except:
                days = 30
        else:
            days = 30

        # Find exit date
        exit_date = trade.expiration_date  # default
        for sym, data in symbol_data.items():
            for i, bar in enumerate(data.price_bars):
                if bar.date == entry_date:
                    if i + days < len(data.price_bars):
                        exit_date = data.price_bars[i + days].date
                    break
            break

        # Remove positions that have closed
        open_positions = [p for p in open_positions if p['exit'] > entry_date]

        # Add new position
        open_positions.append({
            'max_loss': trade.max_loss,
            'exit': exit_date
        })

        current_capital = sum(p['max_loss'] for p in open_positions)
        capital_over_time.append((entry_date, current_capital))

        if current_capital > max_capital:
            max_capital = current_capital

    print("=" * 70)
    print("CAPITAL & RISK METRICS")
    print("=" * 70)
    print(f"Maximum Capital at Risk: ${max_capital:,.2f}")
    print(f"Maximum Drawdown: ${max_drawdown:,.2f}")
    if max_capital > 0:
        print(f"Max Drawdown as % of Max Capital: {max_drawdown/max_capital*100:.2f}%")
    print()

    # Calculate Sharpe Ratio
    # Use individual trade returns relative to capital at risk
    trade_returns = [t.pnl / t.max_loss for t in all_trades]
    avg_return = sum(trade_returns) / len(trade_returns)
    variance = sum((r - avg_return)**2 for r in trade_returns) / (len(trade_returns) - 1)
    std_return = math.sqrt(variance)

    # Sharpe ratio (assuming risk-free rate of 0)
    sharpe_ratio = avg_return / std_return if std_return > 0 else 0

    # Annualized Sharpe (assuming ~100 trades/year)
    trades_per_year = 100
    annualized_sharpe = sharpe_ratio * math.sqrt(trades_per_year)

    print("=" * 70)
    print("SHARPE RATIO ANALYSIS")
    print("=" * 70)
    print(f"Average Return per Trade: {avg_return*100:.2f}% of max risk")
    print(f"Std Dev of Returns: {std_return*100:.2f}%")
    print(f"Sharpe Ratio (per trade): {sharpe_ratio:.3f}")
    print(f"Annualized Sharpe Ratio: {annualized_sharpe:.3f}")
    print()

    # Probabilistic Sharpe Ratio
    # PSR = probability that true Sharpe > benchmark (0)
    n = len(trade_returns)
    mean_r = avg_return

    # Calculate skewness and kurtosis
    if std_return > 0:
        skewness = sum((r - mean_r)**3 for r in trade_returns) / (n * std_return**3)
        kurtosis = sum((r - mean_r)**4 for r in trade_returns) / (n * std_return**4)
    else:
        skewness = 0
        kurtosis = 3

    # Benchmark Sharpe (0 = no skill)
    sr_benchmark = 0

    # Standard error of Sharpe ratio (Bailey & Lopez de Prado formula)
    se_sharpe = math.sqrt((1 - skewness * sharpe_ratio + (kurtosis - 1)/4 * sharpe_ratio**2) / (n - 1))

    # PSR = probability that true Sharpe > 0
    if se_sharpe > 0:
        z_score = (sharpe_ratio - sr_benchmark) / se_sharpe
        psr = stats.norm.cdf(z_score)
    else:
        psr = 0.5

    print("=" * 70)
    print("PROBABILISTIC SHARPE RATIO")
    print("=" * 70)
    print(f"Sample Size (n): {n}")
    print(f"Skewness: {skewness:.3f}")
    print(f"Kurtosis: {kurtosis:.3f}")
    print(f"Standard Error of Sharpe: {se_sharpe:.4f}")
    print(f"Probabilistic Sharpe Ratio (vs SR=0): {psr*100:.1f}%")
    print(f"  -> {psr*100:.1f}% confidence that true Sharpe > 0")
    print()

    # Additional metrics
    avg_win = sum(t.pnl for t in all_trades if t.won) / winning if winning > 0 else 0
    avg_loss = sum(t.pnl for t in all_trades if not t.won) / losing if losing > 0 else 0

    total_wins = sum(t.pnl for t in all_trades if t.won)
    total_losses = abs(sum(t.pnl for t in all_trades if not t.won))
    profit_factor = total_wins / total_losses if total_losses > 0 else float('inf')

    expectancy = (winning/total_trades * avg_win) + (losing/total_trades * avg_loss)

    # Average days in trade
    exit_days = []
    for t in all_trades:
        if 'day' in t.reason:
            try:
                day = int(t.reason.split('day ')[1])
                exit_days.append(day)
            except:
                exit_days.append(30)
        else:
            exit_days.append(30)
    avg_days = sum(exit_days) / len(exit_days)

    print("=" * 70)
    print("ADDITIONAL METRICS")
    print("=" * 70)
    print(f"Average Win: ${avg_win:.2f}")
    print(f"Average Loss: ${avg_loss:.2f}")
    print(f"Win/Loss Ratio: {abs(avg_win/avg_loss):.2f}" if avg_loss != 0 else "Win/Loss Ratio: N/A")
    print(f"Profit Factor: {profit_factor:.2f}")
    print(f"Expectancy: ${expectancy:.2f} per trade")
    print(f"Average Days in Trade: {avg_days:.1f}")
    print()

    # Return on capital
    roi = total_pnl / max_capital * 100 if max_capital > 0 else 0
    print(f"Return on Max Capital: {roi:.2f}%")
    print()

    # Save results
    output = {
        'generated_at': datetime.now().isoformat(),
        'strategy': {
            'name': 'Put Credit Spread with Early Exit',
            'take_profit_pct': 0.50,
            'stop_loss_pct': 0.75,
            'entry_interval_days': 5,
            'dte': 30,
            'short_delta': -0.25,
        },
        'pricing_assumptions': {
            'bid_ask_spread_pct': 0.05,
            'use_volatility_skew': True,
            'skew_slope': 0.0015,
        },
        'summary': {
            'total_trades': total_trades,
            'winning_trades': winning,
            'losing_trades': losing,
            'win_rate': winning / total_trades,
            'total_pnl': total_pnl,
            'avg_pnl_per_trade': total_pnl / total_trades,
        },
        'risk_metrics': {
            'max_capital_at_risk': max_capital,
            'max_drawdown': max_drawdown,
            'max_drawdown_pct': max_drawdown / max_capital if max_capital > 0 else 0,
            'return_on_max_capital': roi,
        },
        'sharpe_analysis': {
            'avg_return_per_trade': avg_return,
            'std_dev_returns': std_return,
            'sharpe_ratio_per_trade': sharpe_ratio,
            'annualized_sharpe_ratio': annualized_sharpe,
            'skewness': skewness,
            'kurtosis': kurtosis,
            'probabilistic_sharpe_ratio': psr,
        },
        'trade_metrics': {
            'avg_win': avg_win,
            'avg_loss': avg_loss,
            'win_loss_ratio': abs(avg_win/avg_loss) if avg_loss != 0 else None,
            'profit_factor': profit_factor,
            'expectancy': expectancy,
            'avg_days_in_trade': avg_days,
        },
        'trades': [
            {
                'symbol': t.symbol,
                'entry_date': t.entry_date,
                'expiration_date': t.expiration_date,
                'spot_price': t.spot_price,
                'short_strike': t.short_strike,
                'long_strike': t.long_strike,
                'credit': t.credit,
                'max_loss': t.max_loss,
                'pnl': t.pnl,
                'won': t.won,
                'reason': t.reason,
                'iv_rank': t.iv_rank,
                'iv_at_entry': t.iv_at_entry,
            }
            for t in all_trades
        ]
    }

    # Save to file
    output_dir = Path('backtest/results')
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_path = output_dir / f'realistic_backtest_{timestamp}.json'

    with open(output_path, 'w') as f:
        json.dump(output, f, indent=2)

    print("=" * 70)
    print(f"Results saved to: {output_path}")


if __name__ == "__main__":
    main()
