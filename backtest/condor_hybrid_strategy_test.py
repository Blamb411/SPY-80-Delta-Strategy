#!/usr/bin/env python3
"""
Test Hybrid "Hold the Wing" Strategy for Iron Condors
=====================================================

Based on findings from condor_hold_wing_test.py:
- Put breaches: Price tends to reverse (mean reversion) -> holding long put HURTS
- Call breaches: Price tends to continue up (momentum) -> holding long call HELPS

This tests a HYBRID strategy:
- On PUT breach: Close all 4 legs immediately (standard)
- On CALL breach: Close 3 legs, hold the long call to expiration
"""

import sys
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, List, Dict

sys.path.insert(0, str(Path(__file__).parent.parent))

from backtest.ibkr_data_fetcher import load_from_cache, is_cache_valid, SymbolData, DailyBar, IVDataPoint
from backtest.condor_backtest import (
    run_condor_backtest_multi,
    CondorTrade,
    DTE,
    RISK_FREE_RATE,
)
from backtest.black_scholes import black_scholes_price
import logging

logging.disable(logging.INFO)


def calculate_hybrid_pnl(
    trade: CondorTrade,
    price_bars: List[DailyBar],
    iv_data: List[IVDataPoint],
    entry_idx: int,
    dte_days: int = DTE,
    rate: float = RISK_FREE_RATE,
) -> Dict:
    """
    Calculate P&L under both standard and hybrid strategies.

    Returns dict with:
    - standard_pnl: Close all 4 legs at stop loss
    - hybrid_pnl: For call breaches, hold long call to expiration
    - strategy_used: 'standard' or 'hold_call'
    """
    result = {
        'standard_pnl': trade.pnl,
        'hybrid_pnl': trade.pnl,
        'strategy_used': 'standard',
        'wing_pnl': 0,
        'side_breached': trade.side_breached,
    }

    # If not a stop loss breach, both strategies are the same
    if trade.side_breached is None or "SL hit" not in trade.reason:
        return result

    # If PUT breach, hybrid = standard (close all)
    if trade.side_breached == 'put':
        return result

    # CALL breach - hold the long call
    result['strategy_used'] = 'hold_call'

    # Parse the stop loss day
    try:
        sl_day = int(trade.reason.split("day ")[1])
    except (IndexError, ValueError):
        return result

    sl_idx = entry_idx + sl_day
    if sl_idx >= len(price_bars):
        return result

    days_remaining = dte_days - sl_day

    # Build IV lookup
    iv_by_date = {iv.date: iv.iv for iv in iv_data}

    # Get spot and IV at stop loss
    sl_bar = price_bars[sl_idx]
    spot_at_sl = sl_bar.close
    iv_at_sl = iv_by_date.get(sl_bar.date, trade.iv_at_entry)
    t_years_at_sl = max(days_remaining / 365.0, 1/365.0)

    # Calculate long call value at stop loss
    long_call_strike = trade.long_call_strike
    wing_value_at_sl = black_scholes_price(
        spot_at_sl, long_call_strike, t_years_at_sl, rate, iv_at_sl, 'C'
    )
    if wing_value_at_sl is None:
        return result

    # Calculate long call value at expiration
    exp_idx = entry_idx + dte_days
    if exp_idx >= len(price_bars):
        return result

    exp_bar = price_bars[exp_idx]
    exp_spot = exp_bar.close

    # Intrinsic value at expiration
    intrinsic = max(0, exp_spot - long_call_strike)

    # Wing P&L = what we get at exp - what we "paid" (opportunity cost at SL)
    wing_pnl = (intrinsic - wing_value_at_sl) * 100

    result['wing_pnl'] = wing_pnl
    result['hybrid_pnl'] = trade.pnl + wing_pnl

    return result


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

    print("=" * 80)
    print("HYBRID STRATEGY TEST - Iron Condor")
    print("=" * 80)
    print()
    print("Standard Strategy:")
    print("  - On ANY breach: Close all 4 legs at stop loss")
    print()
    print("Hybrid Strategy:")
    print("  - On PUT breach: Close all 4 legs (same as standard)")
    print("  - On CALL breach: Close 3 legs, HOLD the long call to expiration")
    print()

    # Run standard condor backtest
    print("Running iron condor backtest...")
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

    # Analyze all trades
    print("Calculating hybrid strategy outcomes...")

    all_results = []

    for symbol, result in condor_results.items():
        data = symbol_data[symbol]
        price_bars = data.price_bars
        iv_data = data.iv_data

        # Build date-to-index lookup
        date_to_idx = {bar.date: idx for idx, bar in enumerate(price_bars)}

        for trade in result.trades:
            entry_idx = date_to_idx.get(trade.entry_date)
            if entry_idx is not None:
                pnl_result = calculate_hybrid_pnl(
                    trade, price_bars, iv_data, entry_idx
                )
                all_results.append(pnl_result)

    print(f"Analyzed {len(all_results)} total trades")
    print()

    # === CATEGORIZE TRADES ===

    # Winners (no breach)
    winners = [r for r in all_results if r['side_breached'] is None]

    # Losers by type
    put_breaches = [r for r in all_results if r['side_breached'] == 'put']
    call_breaches = [r for r in all_results if r['side_breached'] == 'call']

    print("=" * 80)
    print("TRADE BREAKDOWN")
    print("=" * 80)
    print()
    print(f"  Winning trades (no breach):  {len(winners):>6}")
    print(f"  Put breaches (downside):     {len(put_breaches):>6}")
    print(f"  Call breaches (upside):      {len(call_breaches):>6}")
    print(f"  {'':->30}")
    print(f"  Total trades:                {len(all_results):>6}")
    print()

    # === STRATEGY COMPARISON ===

    print("=" * 80)
    print("STRATEGY COMPARISON")
    print("=" * 80)
    print()

    # Standard strategy totals
    std_total = sum(r['standard_pnl'] for r in all_results)
    std_winners = sum(r['standard_pnl'] for r in winners)
    std_put_breach = sum(r['standard_pnl'] for r in put_breaches)
    std_call_breach = sum(r['standard_pnl'] for r in call_breaches)

    # Hybrid strategy totals
    hyb_total = sum(r['hybrid_pnl'] for r in all_results)
    hyb_winners = sum(r['hybrid_pnl'] for r in winners)  # Same as standard
    hyb_put_breach = sum(r['hybrid_pnl'] for r in put_breaches)  # Same as standard
    hyb_call_breach = sum(r['hybrid_pnl'] for r in call_breaches)  # Different!

    # Wing P&L from call breaches
    wing_pnl_total = sum(r['wing_pnl'] for r in call_breaches)

    print(f"{'Category':<25} | {'Standard':>14} | {'Hybrid':>14} | {'Difference':>12}")
    print("-" * 75)
    print(f"{'Winning Trades':<25} | ${std_winners:>12,.0f} | ${hyb_winners:>12,.0f} | ${0:>+11,.0f}")
    print(f"{'Put Breaches':<25} | ${std_put_breach:>12,.0f} | ${hyb_put_breach:>12,.0f} | ${0:>+11,.0f}")
    print(f"{'Call Breaches':<25} | ${std_call_breach:>12,.0f} | ${hyb_call_breach:>12,.0f} | ${wing_pnl_total:>+11,.0f}")
    print("-" * 75)
    print(f"{'TOTAL':<25} | ${std_total:>12,.0f} | ${hyb_total:>12,.0f} | ${hyb_total - std_total:>+11,.0f}")
    print()

    # === PER-TRADE ANALYSIS ===

    print("=" * 80)
    print("PER-TRADE ANALYSIS")
    print("=" * 80)
    print()

    n_trades = len(all_results)
    print(f"{'Metric':<35} | {'Standard':>14} | {'Hybrid':>14}")
    print("-" * 70)
    print(f"{'Total P&L':<35} | ${std_total:>12,.0f} | ${hyb_total:>12,.0f}")
    print(f"{'Average P&L per Trade':<35} | ${std_total/n_trades:>12.2f} | ${hyb_total/n_trades:>12.2f}")
    print()

    # Win rate (considering a trade "won" if P&L > 0)
    std_wins = sum(1 for r in all_results if r['standard_pnl'] > 0)
    hyb_wins = sum(1 for r in all_results if r['hybrid_pnl'] > 0)

    print(f"{'Profitable Trades':<35} | {std_wins:>14,} | {hyb_wins:>14,}")
    print(f"{'Win Rate':<35} | {std_wins/n_trades:>13.1%} | {hyb_wins/n_trades:>13.1%}")
    print()

    # === CALL BREACH DEEP DIVE ===

    print("=" * 80)
    print("CALL BREACH DEEP DIVE (Where Hybrid Differs)")
    print("=" * 80)
    print()

    if call_breaches:
        n_call = len(call_breaches)

        # How often does holding the call improve the outcome?
        improved = sum(1 for r in call_breaches if r['wing_pnl'] > 0)
        worsened = sum(1 for r in call_breaches if r['wing_pnl'] < 0)
        unchanged = n_call - improved - worsened

        print(f"Call Breaches: {n_call}")
        print()
        print(f"  Holding long call IMPROVED outcome:  {improved:>5} ({improved/n_call:.1%})")
        print(f"  Holding long call WORSENED outcome:  {worsened:>5} ({worsened/n_call:.1%})")
        print(f"  Holding long call NO CHANGE:         {unchanged:>5} ({unchanged/n_call:.1%})")
        print()

        # Average wing P&L
        avg_wing = wing_pnl_total / n_call
        print(f"  Average wing P&L:  ${avg_wing:>+.2f}/trade")
        print()

        # Distribution of outcomes
        wing_pnls = [r['wing_pnl'] for r in call_breaches]
        wing_pnls_sorted = sorted(wing_pnls)

        print(f"  Wing P&L Distribution:")
        print(f"    Min:     ${min(wing_pnls):>+10,.0f}")
        print(f"    25th %:  ${wing_pnls_sorted[len(wing_pnls_sorted)//4]:>+10,.0f}")
        print(f"    Median:  ${wing_pnls_sorted[len(wing_pnls_sorted)//2]:>+10,.0f}")
        print(f"    75th %:  ${wing_pnls_sorted[3*len(wing_pnls_sorted)//4]:>+10,.0f}")
        print(f"    Max:     ${max(wing_pnls):>+10,.0f}")
        print()

        # Convert losing call breaches to winners?
        std_call_losers = sum(1 for r in call_breaches if r['standard_pnl'] <= 0)
        hyb_call_losers = sum(1 for r in call_breaches if r['hybrid_pnl'] <= 0)
        converted = std_call_losers - hyb_call_losers

        print(f"  Losing call breach trades (standard):  {std_call_losers}")
        print(f"  Losing call breach trades (hybrid):    {hyb_call_losers}")
        print(f"  Trades converted from loss to win:     {converted}")

    print()

    # === FINAL VERDICT ===

    print("=" * 80)
    print("FINAL VERDICT")
    print("=" * 80)
    print()

    improvement = hyb_total - std_total
    per_trade_improvement = improvement / n_trades

    if improvement > 0:
        print(f"  HYBRID STRATEGY WINS!")
        print()
        print(f"  Total improvement:      ${improvement:>+,.0f}")
        print(f"  Per-trade improvement:  ${per_trade_improvement:>+.2f}")
        print()
        print(f"  The hybrid approach adds ${per_trade_improvement:.2f} per trade by")
        print(f"  holding the long call after upside breaches.")
    else:
        print(f"  STANDARD STRATEGY WINS!")
        print()
        print(f"  Hybrid underperforms by: ${-improvement:>,.0f}")
        print(f"  Per-trade difference:    ${per_trade_improvement:>+.2f}")

    print()
    print("=" * 80)
    print("RECOMMENDATION")
    print("=" * 80)
    print()

    if improvement > 0:
        print("When trading iron condors with this system:")
        print()
        print("  1. On WINNING trades: Close all 4 legs at take-profit (no change)")
        print()
        print("  2. On PUT BREACH (downside): Close all 4 legs at stop-loss")
        print("     Reason: Price tends to reverse upward (mean reversion)")
        print()
        print("  3. On CALL BREACH (upside): Close 3 legs, HOLD the long call")
        print("     Reason: Price tends to continue upward (momentum)")
        print("     Let the long call ride to expiration for additional gains")
    else:
        print("The hybrid approach does not improve results in this backtest.")
        print("Continue using the standard approach: close all 4 legs on any stop loss.")

    print()

    # === CAVEAT ===
    print("=" * 80)
    print("IMPORTANT CAVEATS")
    print("=" * 80)
    print()
    print("1. This was tested during 2023-2025, a strongly BULLISH period")
    print("   The call-holding advantage may not persist in bear markets")
    print()
    print("2. Holding the long call adds RISK:")
    print("   - Theta decay if price reverses")
    print("   - Continued capital at risk")
    print("   - Requires monitoring until expiration")
    print()
    print("3. The iron condor strategy is STILL UNPROFITABLE overall")
    print("   This modification reduces losses but doesn't make it profitable")
    print()


if __name__ == "__main__":
    main()
