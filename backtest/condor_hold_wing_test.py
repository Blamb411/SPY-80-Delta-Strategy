#!/usr/bin/env python3
"""
Test "Hold the Wing" Strategy for Iron Condors
===============================================

When a condor hits stop loss due to a breach, instead of closing all 4 legs,
this tests what happens if we:
1. Close 3 legs (the losing spread + the winning spread)
2. Hold the profitable long option (the protective wing)

We test different holding periods:
- Hold to original expiration
- Hold for 15 days after closing 3 legs
- Hold for 30 days after closing 3 legs
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


@dataclass
class WingHoldResult:
    """Result of holding a wing after stop loss."""
    symbol: str
    entry_date: str
    sl_day: int  # Day stop loss was hit
    side_breached: str  # 'put' or 'call'
    days_remaining_at_sl: int

    # Standard close-all P&L
    standard_pnl: float

    # Wing details at stop loss
    wing_strike: float
    wing_type: str  # 'P' or 'C'
    wing_value_at_sl: float
    spot_at_sl: float

    # Outcomes at different holding periods
    wing_value_at_exp: Optional[float] = None
    wing_pnl_at_exp: Optional[float] = None

    wing_value_15d: Optional[float] = None
    wing_pnl_15d: Optional[float] = None

    wing_value_30d: Optional[float] = None
    wing_pnl_30d: Optional[float] = None


def analyze_wing_holding(
    trade: CondorTrade,
    price_bars: List[DailyBar],
    iv_data: List[IVDataPoint],
    entry_idx: int,
    dte_days: int = DTE,
    rate: float = RISK_FREE_RATE,
) -> Optional[WingHoldResult]:
    """
    Analyze what would happen if we held the winning wing after stop loss.
    """
    # Only analyze trades that hit stop loss with a side breach
    if trade.side_breached is None:
        return None

    if "SL hit" not in trade.reason:
        return None

    # Parse the stop loss day from reason like "SL hit day 15"
    try:
        sl_day = int(trade.reason.split("day ")[1])
    except (IndexError, ValueError):
        return None

    sl_idx = entry_idx + sl_day
    if sl_idx >= len(price_bars):
        return None

    days_remaining = dte_days - sl_day

    # Build IV lookup
    iv_by_date = {iv.date: iv.iv for iv in iv_data}

    # Determine which wing to hold
    if trade.side_breached == 'put':
        # Price dropped - hold the long put
        wing_strike = trade.long_put_strike
        wing_type = 'P'
    else:
        # Price rose - hold the long call
        wing_strike = trade.long_call_strike
        wing_type = 'C'

    # Get spot and IV at stop loss
    sl_bar = price_bars[sl_idx]
    spot_at_sl = sl_bar.close
    iv_at_sl = iv_by_date.get(sl_bar.date, trade.iv_at_entry)
    t_years_at_sl = max(days_remaining / 365.0, 1/365.0)

    # Calculate wing value at stop loss
    wing_value_at_sl = black_scholes_price(
        spot_at_sl, wing_strike, t_years_at_sl, rate, iv_at_sl, wing_type
    )
    if wing_value_at_sl is None:
        return None

    result = WingHoldResult(
        symbol=trade.symbol,
        entry_date=trade.entry_date,
        sl_day=sl_day,
        side_breached=trade.side_breached,
        days_remaining_at_sl=days_remaining,
        standard_pnl=trade.pnl,
        wing_strike=wing_strike,
        wing_type=wing_type,
        wing_value_at_sl=wing_value_at_sl,
        spot_at_sl=spot_at_sl,
    )

    # === Outcome at EXPIRATION ===
    exp_idx = entry_idx + dte_days
    if exp_idx < len(price_bars):
        exp_bar = price_bars[exp_idx]
        exp_spot = exp_bar.close

        # Calculate intrinsic value at expiration
        if wing_type == 'P':
            intrinsic = max(0, wing_strike - exp_spot)
        else:
            intrinsic = max(0, exp_spot - wing_strike)

        result.wing_value_at_exp = intrinsic
        # P&L = final value - value at SL (what we "paid" to keep it)
        result.wing_pnl_at_exp = (intrinsic - wing_value_at_sl) * 100

    # === Outcome at 15 DAYS after SL ===
    if days_remaining >= 15:
        check_idx = sl_idx + 15
        if check_idx < len(price_bars):
            check_bar = price_bars[check_idx]
            check_spot = check_bar.close
            check_iv = iv_by_date.get(check_bar.date, iv_at_sl)
            t_years_15d = max((days_remaining - 15) / 365.0, 1/365.0)

            wing_value_15d = black_scholes_price(
                check_spot, wing_strike, t_years_15d, rate, check_iv, wing_type
            )
            if wing_value_15d is not None:
                result.wing_value_15d = wing_value_15d
                result.wing_pnl_15d = (wing_value_15d - wing_value_at_sl) * 100

    # === Outcome at 30 DAYS after SL ===
    if days_remaining >= 30:
        check_idx = sl_idx + 30
        if check_idx < len(price_bars):
            check_bar = price_bars[check_idx]
            check_spot = check_bar.close
            check_iv = iv_by_date.get(check_bar.date, iv_at_sl)
            t_years_30d = max((days_remaining - 30) / 365.0, 1/365.0)

            wing_value_30d = black_scholes_price(
                check_spot, wing_strike, t_years_30d, rate, check_iv, wing_type
            )
            if wing_value_30d is not None:
                result.wing_value_30d = wing_value_30d
                result.wing_pnl_30d = (wing_value_30d - wing_value_at_sl) * 100

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
    print("HOLD THE WING TEST - Iron Condor Alternative Strategy")
    print("=" * 80)
    print()
    print("When condor hits stop loss, instead of closing all 4 legs:")
    print("  - Close 3 legs (losing spread + opposite spread)")
    print("  - Hold the profitable long option (the wing)")
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

    # Collect all trades with stop loss breaches
    all_wing_results: List[WingHoldResult] = []

    print("Analyzing wing holding outcomes...")
    for symbol, result in condor_results.items():
        data = symbol_data[symbol]
        price_bars = data.price_bars
        iv_data = data.iv_data

        # Build date-to-index lookup
        date_to_idx = {bar.date: idx for idx, bar in enumerate(price_bars)}

        for trade in result.trades:
            if trade.side_breached is not None and "SL hit" in trade.reason:
                entry_idx = date_to_idx.get(trade.entry_date)
                if entry_idx is not None:
                    wing_result = analyze_wing_holding(
                        trade, price_bars, iv_data, entry_idx
                    )
                    if wing_result:
                        all_wing_results.append(wing_result)

    print(f"Found {len(all_wing_results)} trades that hit stop loss with a breach")
    print()

    if not all_wing_results:
        print("No eligible trades to analyze.")
        return

    # === SUMMARY STATISTICS ===

    # Separate by breach type
    put_breaches = [r for r in all_wing_results if r.side_breached == 'put']
    call_breaches = [r for r in all_wing_results if r.side_breached == 'call']

    print("=" * 80)
    print("SUMMARY BY BREACH TYPE")
    print("=" * 80)
    print()
    print(f"{'Breach Type':<15} {'Count':>8} {'Avg SL Day':>12} {'Avg Days Left':>14}")
    print("-" * 55)
    print(f"{'Put (downside)':<15} {len(put_breaches):>8} {sum(r.sl_day for r in put_breaches)/len(put_breaches) if put_breaches else 0:>12.1f} {sum(r.days_remaining_at_sl for r in put_breaches)/len(put_breaches) if put_breaches else 0:>14.1f}")
    print(f"{'Call (upside)':<15} {len(call_breaches):>8} {sum(r.sl_day for r in call_breaches)/len(call_breaches) if call_breaches else 0:>12.1f} {sum(r.days_remaining_at_sl for r in call_breaches)/len(call_breaches) if call_breaches else 0:>14.1f}")
    print()

    # === HOLDING PERIOD ANALYSIS ===

    def analyze_holding_period(results: List[WingHoldResult], period: str) -> Dict:
        """Analyze results for a specific holding period."""
        if period == 'expiration':
            valid = [r for r in results if r.wing_pnl_at_exp is not None]
            get_pnl = lambda r: r.wing_pnl_at_exp
        elif period == '15d':
            valid = [r for r in results if r.wing_pnl_15d is not None]
            get_pnl = lambda r: r.wing_pnl_15d
        else:  # 30d
            valid = [r for r in results if r.wing_pnl_30d is not None]
            get_pnl = lambda r: r.wing_pnl_30d

        if not valid:
            return None

        wing_pnls = [get_pnl(r) for r in valid]
        standard_pnls = [r.standard_pnl for r in valid]

        # Combined P&L = standard close + wing holding result
        # But we need to adjust: when we "hold the wing", we're forgoing selling it at SL
        # So combined = standard_pnl + wing_pnl_change
        combined_pnls = [s + w for s, w in zip(standard_pnls, wing_pnls)]

        wins = sum(1 for p in wing_pnls if p > 0)

        return {
            'count': len(valid),
            'wing_total': sum(wing_pnls),
            'wing_avg': sum(wing_pnls) / len(wing_pnls),
            'wing_wins': wins,
            'wing_win_rate': wins / len(wing_pnls),
            'standard_total': sum(standard_pnls),
            'combined_total': sum(combined_pnls),
            'improvement': sum(wing_pnls),  # The delta from holding
        }

    print("=" * 80)
    print("WING HOLDING RESULTS - ALL BREACHES COMBINED")
    print("=" * 80)
    print()
    print(f"{'Holding Period':<20} | {'Trades':>7} | {'Wing P&L':>12} | {'Avg Wing':>10} | {'Wing Win%':>10} | {'Improves?':>10}")
    print("-" * 85)

    for period, label in [('expiration', 'To Expiration'), ('15d', '15 Days'), ('30d', '30 Days')]:
        stats = analyze_holding_period(all_wing_results, period)
        if stats:
            improves = "YES" if stats['wing_total'] > 0 else "NO"
            print(f"{label:<20} | {stats['count']:>7} | ${stats['wing_total']:>10,.0f} | ${stats['wing_avg']:>9.2f} | {stats['wing_win_rate']:>9.1%} | {improves:>10}")
        else:
            print(f"{label:<20} | {'N/A':>7} |")

    print()

    # === BY BREACH TYPE ===

    for breach_type, breach_results, label in [
        ('put', put_breaches, 'PUT BREACHES (Hold Long Put)'),
        ('call', call_breaches, 'CALL BREACHES (Hold Long Call)')
    ]:
        if not breach_results:
            continue

        print("=" * 80)
        print(f"{label}")
        print("=" * 80)
        print()
        print(f"{'Holding Period':<20} | {'Trades':>7} | {'Wing P&L':>12} | {'Avg Wing':>10} | {'Wing Win%':>10} | {'Improves?':>10}")
        print("-" * 85)

        for period, plabel in [('expiration', 'To Expiration'), ('15d', '15 Days'), ('30d', '30 Days')]:
            stats = analyze_holding_period(breach_results, period)
            if stats:
                improves = "YES" if stats['wing_total'] > 0 else "NO"
                print(f"{plabel:<20} | {stats['count']:>7} | ${stats['wing_total']:>10,.0f} | ${stats['wing_avg']:>9.2f} | {stats['wing_win_rate']:>9.1%} | {improves:>10}")
            else:
                print(f"{plabel:<20} | {'N/A':>7} |")

        print()

    # === COMPARISON: CLOSE ALL vs HOLD WING ===

    print("=" * 80)
    print("STRATEGY COMPARISON: Close All 4 Legs vs Hold the Wing")
    print("=" * 80)
    print()

    # Use expiration as the main comparison
    exp_valid = [r for r in all_wing_results if r.wing_pnl_at_exp is not None]

    if exp_valid:
        close_all_pnl = sum(r.standard_pnl for r in exp_valid)
        hold_wing_pnl = sum(r.standard_pnl + r.wing_pnl_at_exp for r in exp_valid)
        improvement = hold_wing_pnl - close_all_pnl

        print(f"For {len(exp_valid)} trades that hit stop loss:")
        print()
        print(f"  Strategy A (Close All 4 Legs):     ${close_all_pnl:>12,.0f}  (${close_all_pnl/len(exp_valid):>8.2f}/trade)")
        print(f"  Strategy B (Hold Winning Wing):    ${hold_wing_pnl:>12,.0f}  (${hold_wing_pnl/len(exp_valid):>8.2f}/trade)")
        print()
        print(f"  Difference:                        ${improvement:>+12,.0f}  (${improvement/len(exp_valid):>+8.2f}/trade)")
        print()

        if improvement > 0:
            print("  CONCLUSION: Holding the wing IMPROVES results")
        else:
            print("  CONCLUSION: Holding the wing HURTS results")

    print()

    # === MOMENTUM ANALYSIS ===

    print("=" * 80)
    print("POST-BREACH MOMENTUM ANALYSIS")
    print("=" * 80)
    print()
    print("Does the move that caused the breach typically continue or reverse?")
    print()

    # For put breaches: did price continue down (good for long put) or reverse up (bad)?
    put_exp_valid = [r for r in put_breaches if r.wing_pnl_at_exp is not None]
    call_exp_valid = [r for r in call_breaches if r.wing_pnl_at_exp is not None]

    if put_exp_valid:
        put_continued = sum(1 for r in put_exp_valid if r.wing_pnl_at_exp > 0)
        put_reversed = len(put_exp_valid) - put_continued
        print(f"PUT BREACHES ({len(put_exp_valid)} trades):")
        print(f"  Move continued (long put gained):  {put_continued:>5} ({put_continued/len(put_exp_valid):.1%})")
        print(f"  Move reversed (long put lost):     {put_reversed:>5} ({put_reversed/len(put_exp_valid):.1%})")
        print()

    if call_exp_valid:
        call_continued = sum(1 for r in call_exp_valid if r.wing_pnl_at_exp > 0)
        call_reversed = len(call_exp_valid) - call_continued
        print(f"CALL BREACHES ({len(call_exp_valid)} trades):")
        print(f"  Move continued (long call gained): {call_continued:>5} ({call_continued/len(call_exp_valid):.1%})")
        print(f"  Move reversed (long call lost):    {call_reversed:>5} ({call_reversed/len(call_exp_valid):.1%})")
        print()

    print("=" * 80)
    print("INTERPRETATION")
    print("=" * 80)
    print()
    print("If wing holding improves results:")
    print("  -> Post-breach moves tend to CONTINUE (momentum)")
    print("  -> The long option gains value as the move extends")
    print()
    print("If wing holding hurts results:")
    print("  -> Post-breach moves tend to REVERSE (mean reversion)")
    print("  -> The long option loses value as price recovers")
    print("  -> Time decay (theta) destroys the option value")
    print()


if __name__ == "__main__":
    main()
