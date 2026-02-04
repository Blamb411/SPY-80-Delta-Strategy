#!/usr/bin/env python3
"""
Compare conservative (bid/ask) vs mid-point execution assumptions.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from backtest.ibkr_data_fetcher import load_from_cache, is_cache_valid
from backtest.put_spread_backtest import run_put_spread_backtest_multi
import logging

logging.disable(logging.INFO)


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

    print("=" * 80)
    print("EXECUTION ASSUMPTION COMPARISON")
    print("Put Credit Spread with Original Filters (stock > 200 SMA, RSI < 75, IV Rank > 30%)")
    print("=" * 80)
    print()

    results = []

    for spread_pct in [0.01, 0.03, 0.05]:
        spread_label = f"{int(spread_pct*100)}%"

        # Conservative execution (sell at bid, buy at ask)
        results_cons = run_put_spread_backtest_multi(
            symbol_data,
            entry_interval_days=5,
            use_early_exit=True,
            take_profit_pct=0.50,
            stop_loss_pct=0.75,
            use_realistic_pricing=True,
            bid_ask_spread_pct=spread_pct,
            use_skew=True,
        )
        cons_trades = []
        for r in results_cons.values():
            cons_trades.extend(r.trades)
        cons_pnl = sum(t.pnl for t in cons_trades)
        cons_wins = sum(1 for t in cons_trades if t.won)
        cons_total = len(cons_trades)

        # Mid-point execution (effectively 0% spread)
        results_mid = run_put_spread_backtest_multi(
            symbol_data,
            entry_interval_days=5,
            use_early_exit=True,
            take_profit_pct=0.50,
            stop_loss_pct=0.75,
            use_realistic_pricing=True,
            bid_ask_spread_pct=0.001,  # Nearly zero = mid-point fills
            use_skew=True,
        )
        mid_trades = []
        for r in results_mid.values():
            mid_trades.extend(r.trades)
        mid_pnl = sum(t.pnl for t in mid_trades)
        mid_wins = sum(1 for t in mid_trades if t.won)
        mid_total = len(mid_trades)

        diff = mid_pnl - cons_pnl

        results.append({
            "spread": spread_label,
            "cons_total": cons_total,
            "cons_win_pct": cons_wins / cons_total * 100,
            "cons_pnl": cons_pnl,
            "mid_total": mid_total,
            "mid_win_pct": mid_wins / mid_total * 100,
            "mid_pnl": mid_pnl,
            "diff": diff,
        })

    # Print results table
    print(f"{'Spread':>8} | {'--- Conservative (Bid/Ask) ---':^32} | {'--- Mid-Point Execution ---':^32} | {'Diff':>10}")
    print(f"{'':>8} | {'Trades':>8} {'Win%':>8} {'P&L':>14} | {'Trades':>8} {'Win%':>8} {'P&L':>14} | {'':>10}")
    print("-" * 100)

    for r in results:
        print(
            f"{r['spread']:>8} | "
            f"{r['cons_total']:>8} {r['cons_win_pct']:>7.1f}% ${r['cons_pnl']:>12,.0f} | "
            f"{r['mid_total']:>8} {r['mid_win_pct']:>7.1f}% ${r['mid_pnl']:>12,.0f} | "
            f"${r['diff']:>+9,.0f}"
        )

    print()
    print("=" * 80)
    print("ANALYSIS: Impact of Execution Assumptions")
    print("=" * 80)
    print()

    for r in results:
        per_trade_diff = r["diff"] / r["cons_total"]
        pct_improvement = r["diff"] / abs(r["cons_pnl"]) * 100 if r["cons_pnl"] != 0 else 0
        print(f"At {r['spread']} assumed spread:")
        print(f"  Mid-point adds: ${r['diff']:+,.0f} total (${per_trade_diff:+.2f} per trade)")
        if r["cons_pnl"] > 0:
            print(f"  Improvement: {pct_improvement:+.1f}% over conservative estimate")
        print()

    print("=" * 80)
    print("IMPORTANT CAVEATS")
    print("=" * 80)
    print()
    print("1. WE DO NOT HAVE ACTUAL BID/ASK DATA")
    print("   - These spreads (1%, 3%, 5%) are ASSUMPTIONS, not historical data")
    print("   - Real spreads vary by stock, time of day, and market conditions")
    print()
    print("2. MID-POINT FILLS ARE NOT GUARANTEED")
    print("   - On liquid options (SPY, QQQ): Often achievable with limit orders")
    print("   - On illiquid options (5%+ spread): Rarely get mid-point fills")
    print()
    print("3. REALISTIC EXPECTATIONS")
    print("   - 1% spread options: Expect fills between mid and conservative")
    print("   - 3% spread options: Expect fills closer to conservative")
    print("   - 5% spread options: Expect conservative or worse")
    print()
    print("4. TO GET REAL DATA, YOU WOULD NEED:")
    print("   - Historical options chain data (expensive: CBOE, OptionMetrics)")
    print("   - Or paper trade with real-time IBKR data for several months")
    print()


if __name__ == "__main__":
    main()
