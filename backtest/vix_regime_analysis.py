#!/usr/bin/env python3
"""
VIX Regime Analysis
===================

Analyze put credit spread and iron condor performance segmented by
VIX levels at entry. Tests whether strategies perform better in
high or low volatility environments.

VIX Buckets:
- Very Low:  VIX < 15
- Low:       15 <= VIX < 20
- Medium:    20 <= VIX < 25
- High:      25 <= VIX < 30
- Very High: VIX >= 30
"""

import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).parent.parent))

import yfinance as yf
import pandas as pd
from backtest.ibkr_data_fetcher import load_from_cache, is_cache_valid
from backtest.put_spread_backtest import run_put_spread_backtest_multi
from backtest.condor_backtest import run_condor_backtest_multi
import logging

logging.disable(logging.INFO)

# VIX bucket definitions
VIX_BUCKETS = [
    ('Very Low', 0, 15),
    ('Low', 15, 20),
    ('Medium', 20, 25),
    ('High', 25, 30),
    ('Very High', 30, 100),
]


def fetch_vix_data(start_date: str, end_date: str) -> Dict[str, float]:
    """
    Fetch historical VIX data from Yahoo Finance.

    Returns dict mapping date string (YYYY-MM-DD) to VIX close value.
    """
    print("Fetching VIX data from Yahoo Finance...")

    try:
        vix = yf.download("^VIX", start=start_date, end=end_date, progress=False)

        if vix.empty:
            print("  Warning: No VIX data returned")
            return {}

        # Handle multi-level columns if present
        if isinstance(vix.columns, pd.MultiIndex):
            vix.columns = vix.columns.get_level_values(0)

        # Convert to dict with date string keys
        vix_dict = {}
        for idx, row in vix.iterrows():
            date_str = idx.strftime("%Y-%m-%d")
            vix_dict[date_str] = float(row['Close'])

        print(f"  Retrieved {len(vix_dict)} days of VIX data")

        # Show VIX range
        vix_values = list(vix_dict.values())
        print(f"  VIX range: {min(vix_values):.1f} - {max(vix_values):.1f}")
        print(f"  VIX mean: {sum(vix_values)/len(vix_values):.1f}")

        return vix_dict

    except Exception as e:
        print(f"  Error fetching VIX: {e}")
        return {}


def get_vix_bucket(vix_value: float) -> str:
    """Return the bucket name for a given VIX value."""
    for name, low, high in VIX_BUCKETS:
        if low <= vix_value < high:
            return name
    return 'Unknown'


def analyze_by_vix(trades: List, vix_data: Dict[str, float],
                   get_date_func, get_pnl_func, get_won_func) -> Dict:
    """
    Analyze trades segmented by VIX at entry.

    Returns dict with stats by VIX bucket.
    """
    # Group trades by VIX bucket
    bucket_trades = defaultdict(list)

    for trade in trades:
        entry_date = get_date_func(trade)
        vix_value = vix_data.get(entry_date)

        if vix_value is None:
            continue

        bucket = get_vix_bucket(vix_value)
        bucket_trades[bucket].append({
            'pnl': get_pnl_func(trade),
            'won': get_won_func(trade),
            'vix': vix_value,
        })

    # Calculate stats by bucket
    results = {}
    for bucket_name, _, _ in VIX_BUCKETS:
        trades_in_bucket = bucket_trades.get(bucket_name, [])

        if not trades_in_bucket:
            results[bucket_name] = None
            continue

        n = len(trades_in_bucket)
        wins = sum(1 for t in trades_in_bucket if t['won'])
        total_pnl = sum(t['pnl'] for t in trades_in_bucket)
        avg_vix = sum(t['vix'] for t in trades_in_bucket) / n

        results[bucket_name] = {
            'trades': n,
            'wins': wins,
            'win_rate': wins / n,
            'total_pnl': total_pnl,
            'avg_pnl': total_pnl / n,
            'avg_vix': avg_vix,
        }

    return results


def print_vix_analysis(results: Dict, title: str):
    """Print formatted VIX analysis table."""
    print()
    print(f"{title}")
    print("=" * 80)
    print()
    print(f"{'VIX Bucket':<12} | {'VIX Range':^12} | {'Trades':>7} | {'Win Rate':>9} | {'Total P&L':>12} | {'Avg P&L':>10}")
    print("-" * 80)

    total_trades = 0
    total_pnl = 0
    total_wins = 0

    for bucket_name, low, high in VIX_BUCKETS:
        stats = results.get(bucket_name)

        if stats is None:
            print(f"{bucket_name:<12} | {low:>4}-{high:<5} | {'N/A':>7} |")
            continue

        range_str = f"{low}-{high}" if high < 100 else f"{low}+"
        print(
            f"{bucket_name:<12} | {range_str:^12} | "
            f"{stats['trades']:>7,} | {stats['win_rate']:>8.1%} | "
            f"${stats['total_pnl']:>10,.0f} | ${stats['avg_pnl']:>9.2f}"
        )

        total_trades += stats['trades']
        total_pnl += stats['total_pnl']
        total_wins += stats['wins']

    print("-" * 80)
    if total_trades > 0:
        print(
            f"{'TOTAL':<12} | {'':<12} | "
            f"{total_trades:>7,} | {total_wins/total_trades:>8.1%} | "
            f"${total_pnl:>10,.0f} | ${total_pnl/total_trades:>9.2f}"
        )


def main():
    # Load cached data
    symbol_data = {}
    cache_dir = Path("backtest/cache")
    for f in cache_dir.glob("*_hist.json"):
        symbol = f.stem.replace("_hist", "")
        if is_cache_valid(symbol, max_age_days=365):
            data = load_from_cache(symbol)
            if data:
                symbol_data[symbol] = data

    print(f"Loaded {len(symbol_data)} symbols")
    print()

    # Fetch VIX data for the backtest period
    vix_data = fetch_vix_data("2023-01-01", "2026-01-31")

    if not vix_data:
        print("Failed to fetch VIX data. Exiting.")
        return

    print()

    # Show VIX distribution
    print("=" * 80)
    print("VIX DISTRIBUTION IN BACKTEST PERIOD")
    print("=" * 80)
    print()

    vix_values = list(vix_data.values())
    bucket_counts = defaultdict(int)
    for v in vix_values:
        bucket_counts[get_vix_bucket(v)] += 1

    print(f"{'VIX Bucket':<12} | {'Days':>8} | {'Percentage':>10}")
    print("-" * 40)
    for bucket_name, low, high in VIX_BUCKETS:
        count = bucket_counts.get(bucket_name, 0)
        pct = count / len(vix_values) * 100 if vix_values else 0
        print(f"{bucket_name:<12} | {count:>8} | {pct:>9.1f}%")

    print()

    # ==================== PUT CREDIT SPREADS ====================

    print("=" * 80)
    print("RUNNING PUT CREDIT SPREAD BACKTEST...")
    print("=" * 80)

    put_results = run_put_spread_backtest_multi(
        symbol_data,
        entry_interval_days=5,
        use_early_exit=True,
        take_profit_pct=0.50,
        stop_loss_pct=0.75,
        use_realistic_pricing=True,
        bid_ask_spread_pct=0.01,
        use_skew=True,
    )

    # Collect all put spread trades
    put_trades = []
    for r in put_results.values():
        put_trades.extend(r.trades)

    print(f"Total put spread trades: {len(put_trades)}")

    # Analyze by VIX
    put_vix_results = analyze_by_vix(
        put_trades,
        vix_data,
        get_date_func=lambda t: t.entry_date,
        get_pnl_func=lambda t: t.pnl,
        get_won_func=lambda t: t.won,
    )

    print_vix_analysis(put_vix_results, "PUT CREDIT SPREAD PERFORMANCE BY VIX")

    # ==================== IRON CONDORS ====================

    print()
    print("=" * 80)
    print("RUNNING IRON CONDOR BACKTEST...")
    print("=" * 80)

    condor_results = run_condor_backtest_multi(
        symbol_data,
        entry_interval_days=5,
        use_early_exit=True,
        take_profit_pct=0.50,
        stop_loss_pct=0.75,
        use_realistic_pricing=True,
        bid_ask_spread_pct=0.01,
        use_skew=True,
    )

    # Collect all condor trades
    condor_trades = []
    for r in condor_results.values():
        condor_trades.extend(r.trades)

    print(f"Total condor trades: {len(condor_trades)}")

    # Analyze by VIX
    condor_vix_results = analyze_by_vix(
        condor_trades,
        vix_data,
        get_date_func=lambda t: t.entry_date,
        get_pnl_func=lambda t: t.pnl,
        get_won_func=lambda t: t.won,
    )

    print_vix_analysis(condor_vix_results, "IRON CONDOR PERFORMANCE BY VIX")

    # ==================== CONDOR WITH HYBRID STRATEGY ====================

    print()
    print("=" * 80)
    print("IRON CONDOR WITH HYBRID STRATEGY BY VIX")
    print("(Hold long call on call breaches)")
    print("=" * 80)

    # We need to recalculate condor P&L with hybrid approach
    # Import the hybrid calculation
    from backtest.condor_hybrid_strategy_test import calculate_hybrid_pnl

    hybrid_trades = []

    for symbol, result in condor_results.items():
        data = symbol_data[symbol]
        price_bars = data.price_bars
        iv_data = data.iv_data
        date_to_idx = {bar.date: idx for idx, bar in enumerate(price_bars)}

        for trade in result.trades:
            entry_idx = date_to_idx.get(trade.entry_date)
            if entry_idx is not None:
                pnl_result = calculate_hybrid_pnl(trade, price_bars, iv_data, entry_idx)
                hybrid_trades.append({
                    'entry_date': trade.entry_date,
                    'pnl': pnl_result['hybrid_pnl'],
                    'won': pnl_result['hybrid_pnl'] > 0,
                })

    # Analyze by VIX
    hybrid_vix_results = analyze_by_vix(
        hybrid_trades,
        vix_data,
        get_date_func=lambda t: t['entry_date'],
        get_pnl_func=lambda t: t['pnl'],
        get_won_func=lambda t: t['won'],
    )

    print_vix_analysis(hybrid_vix_results, "IRON CONDOR (HYBRID) PERFORMANCE BY VIX")

    # ==================== SUMMARY COMPARISON ====================

    print()
    print("=" * 80)
    print("SUMMARY: STRATEGY PERFORMANCE BY VIX REGIME")
    print("=" * 80)
    print()
    print(f"{'VIX Bucket':<12} | {'Put Spread':^20} | {'Condor Std':^20} | {'Condor Hybrid':^20}")
    print(f"{'':12} | {'Trades':>7} {'Avg P&L':>11} | {'Trades':>7} {'Avg P&L':>11} | {'Trades':>7} {'Avg P&L':>11}")
    print("-" * 85)

    for bucket_name, _, _ in VIX_BUCKETS:
        put_stats = put_vix_results.get(bucket_name)
        cond_stats = condor_vix_results.get(bucket_name)
        hyb_stats = hybrid_vix_results.get(bucket_name)

        put_str = f"{put_stats['trades']:>7} ${put_stats['avg_pnl']:>9.2f}" if put_stats else f"{'N/A':>7} {'':>11}"
        cond_str = f"{cond_stats['trades']:>7} ${cond_stats['avg_pnl']:>9.2f}" if cond_stats else f"{'N/A':>7} {'':>11}"
        hyb_str = f"{hyb_stats['trades']:>7} ${hyb_stats['avg_pnl']:>9.2f}" if hyb_stats else f"{'N/A':>7} {'':>11}"

        print(f"{bucket_name:<12} | {put_str} | {cond_str} | {hyb_str}")

    print()

    # ==================== KEY INSIGHTS ====================

    print("=" * 80)
    print("KEY INSIGHTS")
    print("=" * 80)
    print()

    # Find best/worst VIX regime for each strategy
    def find_best_worst(results):
        valid = [(k, v) for k, v in results.items() if v is not None and v['trades'] >= 50]
        if not valid:
            return None, None
        best = max(valid, key=lambda x: x[1]['avg_pnl'])
        worst = min(valid, key=lambda x: x[1]['avg_pnl'])
        return best, worst

    put_best, put_worst = find_best_worst(put_vix_results)
    cond_best, cond_worst = find_best_worst(condor_vix_results)
    hyb_best, hyb_worst = find_best_worst(hybrid_vix_results)

    print("PUT CREDIT SPREADS:")
    if put_best and put_worst:
        print(f"  Best VIX regime:  {put_best[0]:<12} (${put_best[1]['avg_pnl']:>+.2f}/trade)")
        print(f"  Worst VIX regime: {put_worst[0]:<12} (${put_worst[1]['avg_pnl']:>+.2f}/trade)")
    print()

    print("IRON CONDORS (Standard):")
    if cond_best and cond_worst:
        print(f"  Best VIX regime:  {cond_best[0]:<12} (${cond_best[1]['avg_pnl']:>+.2f}/trade)")
        print(f"  Worst VIX regime: {cond_worst[0]:<12} (${cond_worst[1]['avg_pnl']:>+.2f}/trade)")
    print()

    print("IRON CONDORS (Hybrid):")
    if hyb_best and hyb_worst:
        print(f"  Best VIX regime:  {hyb_best[0]:<12} (${hyb_best[1]['avg_pnl']:>+.2f}/trade)")
        print(f"  Worst VIX regime: {hyb_worst[0]:<12} (${hyb_worst[1]['avg_pnl']:>+.2f}/trade)")
    print()

    # Check if any VIX regime makes condors profitable
    print("=" * 80)
    print("CAN WE FIND A PROFITABLE VIX REGIME FOR CONDORS?")
    print("=" * 80)
    print()

    profitable_condor_regimes = [
        (k, v) for k, v in condor_vix_results.items()
        if v is not None and v['avg_pnl'] > 0 and v['trades'] >= 50
    ]

    profitable_hybrid_regimes = [
        (k, v) for k, v in hybrid_vix_results.items()
        if v is not None and v['avg_pnl'] > 0 and v['trades'] >= 50
    ]

    if profitable_condor_regimes:
        print("Standard Condors are PROFITABLE in these VIX regimes:")
        for name, stats in profitable_condor_regimes:
            print(f"  {name}: {stats['trades']} trades, ${stats['avg_pnl']:.2f}/trade")
    else:
        print("Standard Condors are NOT profitable in ANY VIX regime.")

    print()

    if profitable_hybrid_regimes:
        print("Hybrid Condors are PROFITABLE in these VIX regimes:")
        for name, stats in profitable_hybrid_regimes:
            print(f"  {name}: {stats['trades']} trades, ${stats['avg_pnl']:.2f}/trade")
    else:
        print("Hybrid Condors are NOT profitable in ANY VIX regime.")

    print()


if __name__ == "__main__":
    main()
