#!/usr/bin/env python3
"""
Diagnostic script to examine individual trades in detail.
Shows the mechanics of spread pricing and early exit evaluation.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from backtest.ibkr_data_fetcher import load_from_cache, is_cache_valid
from backtest.black_scholes import (
    black_scholes_price,
    calculate_spread_price,
    find_strike_for_delta,
    round_strike_to_standard,
)
from backtest.put_spread_backtest import (
    simulate_entry_date,
    check_entry_filters,
    construct_put_spread,
    DTE,
    SMA_PERIOD,
    RISK_FREE_RATE,
)


def diagnose_single_trade(symbol: str, trade_number: int = 0):
    """
    Walk through a single trade step by step, showing all calculations.
    """
    print(f"\n{'='*70}")
    print(f"DIAGNOSING TRADE FOR {symbol}")
    print(f"{'='*70}\n")

    # Load cached data
    if not is_cache_valid(symbol, max_age_days=365):
        print(f"No cached data for {symbol}")
        return

    data = load_from_cache(symbol)
    if not data:
        print(f"Failed to load cache for {symbol}")
        return

    price_bars = data.price_bars
    iv_data = data.iv_data

    print(f"Loaded {len(price_bars)} price bars, {len(iv_data)} IV points")

    # Find trades
    trades_found = 0
    for date_idx in range(SMA_PERIOD, len(price_bars) - DTE - 1, 5):  # Every 5 days
        trade = simulate_entry_date(symbol, date_idx, price_bars, iv_data)

        if trade is None:
            continue

        if trades_found < trade_number:
            trades_found += 1
            continue

        # Found the trade we want to diagnose
        print(f"\n--- TRADE #{trades_found + 1} ---")
        print(f"Entry Date: {trade.entry_date}")
        print(f"Expiration: {trade.expiration_date}")
        print(f"Spot Price: ${trade.spot_price:.2f}")
        print(f"Short Strike: ${trade.short_strike:.2f}")
        print(f"Long Strike: ${trade.long_strike:.2f}")
        print(f"Credit (per share): ${trade.credit:.4f}")
        print(f"Credit (per contract): ${trade.credit * 100:.2f}")
        print(f"Max Loss: ${trade.max_loss:.2f}")
        print(f"Width: ${trade.short_strike - trade.long_strike:.2f}")
        print(f"IV at Entry: {trade.iv_at_entry:.1%}")
        print(f"IV Rank: {trade.iv_rank:.1%}")
        print(f"Theoretical POP: {trade.theoretical_pop:.1%}")

        # Now walk through each day and show the PROPER spread valuation
        print(f"\n--- DAILY PROGRESSION (using Black-Scholes repricing) ---")
        print(f"{'Day':<5} {'Date':<12} {'Spot':<10} {'DTE':<5} {'Short Put':<12} {'Long Put':<12} {'Spread':<10} {'P&L':<12} {'P&L %':<8}")
        print("-" * 100)

        max_profit = trade.credit * 100

        for day in range(0, DTE + 1):
            bar_idx = date_idx + day
            if bar_idx >= len(price_bars):
                break

            bar = price_bars[bar_idx]
            spot = bar.close
            days_remaining = DTE - day
            dte_years = max(days_remaining / 365.0, 1/365.0)  # Minimum 1 day

            # Find IV for this date
            iv = trade.iv_at_entry  # Use entry IV as approximation
            for iv_point in iv_data:
                if iv_point.date == bar.date:
                    iv = iv_point.iv
                    break

            # Price each leg using Black-Scholes
            short_put_price = black_scholes_price(
                spot, trade.short_strike, dte_years, RISK_FREE_RATE, iv, 'P'
            )
            long_put_price = black_scholes_price(
                spot, trade.long_strike, dte_years, RISK_FREE_RATE, iv, 'P'
            )

            # Spread value = what we'd pay to close
            spread_value = short_put_price - long_put_price if short_put_price and long_put_price else 0

            # P&L = credit received - cost to close
            pnl = (trade.credit - spread_value) * 100
            pnl_pct = pnl / max_profit * 100 if max_profit > 0 else 0

            marker = ""
            if pnl_pct >= 50:
                marker = " <-- 50% TP"
            elif pnl_pct >= 75:
                marker = " <-- 75% TP"

            print(f"{day:<5} {bar.date:<12} ${spot:<9.2f} {days_remaining:<5} ${short_put_price or 0:<11.4f} ${long_put_price or 0:<11.4f} ${spread_value:<9.4f} ${pnl:<11.2f} {pnl_pct:>6.1f}%{marker}")

            # Show first 10 days, then skip to last few
            if day == 10 and DTE > 15:
                print("  ... (skipping middle days) ...")

        # Compare to the simplified formula
        print(f"\n--- COMPARISON: Simplified vs Black-Scholes ---")
        print(f"Simplified formula on day 1: P&L = credit * (1 - 0.967 * 0.5) * 100")
        print(f"  = ${trade.credit:.4f} * 0.517 * 100 = ${trade.credit * 0.517 * 100:.2f}")
        print(f"  = {51.7:.1f}% of max profit (INSTANT 50% TP trigger!)")

        # Black-Scholes day 1
        bar = price_bars[date_idx + 1]
        dte_years = (DTE - 1) / 365.0
        short_put_price = black_scholes_price(bar.close, trade.short_strike, dte_years, RISK_FREE_RATE, trade.iv_at_entry, 'P')
        long_put_price = black_scholes_price(bar.close, trade.long_strike, dte_years, RISK_FREE_RATE, trade.iv_at_entry, 'P')
        spread_value = (short_put_price or 0) - (long_put_price or 0)
        pnl = (trade.credit - spread_value) * 100
        pnl_pct = pnl / max_profit * 100

        print(f"\nBlack-Scholes on day 1 (at spot ${bar.close:.2f}):")
        print(f"  Spread value = ${spread_value:.4f}")
        print(f"  P&L = ${pnl:.2f} = {pnl_pct:.1f}% of max profit")

        return trade

    print(f"Could not find trade #{trade_number} for {symbol}")


def compare_valuation_methods():
    """
    Show the difference between simplified and Black-Scholes valuation
    across different scenarios.
    """
    print("\n" + "="*70)
    print("VALUATION METHOD COMPARISON")
    print("="*70)

    # Example spread parameters
    spot = 100.0
    short_strike = 95.0  # 5% OTM
    long_strike = 90.0
    iv = 0.30  # 30% IV
    dte = 30
    rate = 0.05

    # Price at entry
    dte_years = dte / 365.0
    short_put = black_scholes_price(spot, short_strike, dte_years, rate, iv, 'P')
    long_put = black_scholes_price(spot, long_strike, dte_years, rate, iv, 'P')
    credit = short_put - long_put - 0.04  # With slippage
    max_profit = credit * 100

    print(f"\nSpread Parameters:")
    print(f"  Spot: ${spot}")
    print(f"  Short strike: ${short_strike}")
    print(f"  Long strike: ${long_strike}")
    print(f"  IV: {iv:.0%}")
    print(f"  DTE: {dte} days")
    print(f"  Credit received: ${credit:.4f} (${credit*100:.2f}/contract)")

    print(f"\n{'Day':<5} {'DTE':<5} {'Simplified':<15} {'BS (spot unch)':<15} {'BS (spot +2%)':<15} {'BS (spot -2%)':<15}")
    print("-" * 80)

    for day in [0, 1, 5, 10, 15, 20, 25, 29, 30]:
        days_remaining = dte - day
        dte_years_now = max(days_remaining / 365.0, 1/365.0)

        # Simplified formula (the buggy one)
        if days_remaining > 0:
            time_factor = days_remaining / dte
            simplified_value = credit * time_factor * 0.5
            simplified_pnl = (credit - simplified_value) * 100
            simplified_pct = simplified_pnl / max_profit * 100
        else:
            simplified_pct = 100.0

        # Black-Scholes - spot unchanged
        short_put = black_scholes_price(spot, short_strike, dte_years_now, rate, iv, 'P')
        long_put = black_scholes_price(spot, long_strike, dte_years_now, rate, iv, 'P')
        spread_val = (short_put or 0) - (long_put or 0)
        bs_unch_pnl = (credit - spread_val) * 100
        bs_unch_pct = bs_unch_pnl / max_profit * 100

        # Black-Scholes - spot +2%
        spot_up = spot * 1.02
        short_put = black_scholes_price(spot_up, short_strike, dte_years_now, rate, iv, 'P')
        long_put = black_scholes_price(spot_up, long_strike, dte_years_now, rate, iv, 'P')
        spread_val = (short_put or 0) - (long_put or 0)
        bs_up_pnl = (credit - spread_val) * 100
        bs_up_pct = bs_up_pnl / max_profit * 100

        # Black-Scholes - spot -2%
        spot_down = spot * 0.98
        short_put = black_scholes_price(spot_down, short_strike, dte_years_now, rate, iv, 'P')
        long_put = black_scholes_price(spot_down, long_strike, dte_years_now, rate, iv, 'P')
        spread_val = (short_put or 0) - (long_put or 0)
        bs_down_pnl = (credit - spread_val) * 100
        bs_down_pct = bs_down_pnl / max_profit * 100

        print(f"{day:<5} {days_remaining:<5} {simplified_pct:>6.1f}%         {bs_unch_pct:>6.1f}%         {bs_up_pct:>6.1f}%         {bs_down_pct:>6.1f}%")

    print("\nKEY INSIGHT: Simplified formula shows 51.7% profit on day 1!")
    print("             Black-Scholes shows only ~3% profit (realistic)")


def main():
    print("="*70)
    print("EARLY EXIT TRADE DIAGNOSTICS")
    print("="*70)

    # First show the valuation comparison
    compare_valuation_methods()

    # Then examine a real trade
    # Try a few symbols that likely have cached data
    for symbol in ['AAPL', 'MSFT', 'NVDA', 'GOOGL']:
        if is_cache_valid(symbol, max_age_days=365):
            diagnose_single_trade(symbol, trade_number=0)
            break


if __name__ == "__main__":
    main()
