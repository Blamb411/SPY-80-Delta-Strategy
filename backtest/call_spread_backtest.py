#!/usr/bin/env python3
"""
Call Credit Spread Backtester
=============================
Tests call credit spreads with bearish filters (opposite of put credit spread).
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from backtest.ibkr_data_fetcher import load_from_cache, is_cache_valid, SymbolData
from backtest.black_scholes import (
    find_strike_for_delta,
    round_strike_to_standard,
    calculate_spread_price_realistic,
    price_spread_to_close,
    calculate_sma,
    calculate_rsi,
    calculate_iv_rank,
)
import logging

logging.disable(logging.INFO)

DTE = 30
RATE = 0.05


def run_call_spread_backtest(
    data: SymbolData,
    spread_pct: float = 0.01,
    price_below_sma: bool = True,
    rsi_threshold: float = 50,
    iv_rank_min: float = 0.30,
):
    """
    Run call credit spread backtest with configurable filters.

    Call credit spread = bearish strategy (profits when price stays flat or falls)
    """
    trades = []
    price_bars = data.price_bars
    iv_data = data.iv_data

    if len(price_bars) < 250 or len(iv_data) < 100:
        return trades

    iv_by_date = {iv.date: iv.iv for iv in iv_data}
    last_entry_idx = -5

    for idx in range(200, len(price_bars) - DTE - 1):
        if idx - last_entry_idx < 5:
            continue

        bar = price_bars[idx]
        spot = bar.close

        # Get historical prices for indicators
        hist_prices = [b.close for b in price_bars[: idx + 1]]

        # Calculate indicators
        sma_200 = calculate_sma(hist_prices, 200)
        rsi = calculate_rsi(hist_prices, 14)

        # Get IV
        current_iv = iv_by_date.get(bar.date)
        if current_iv is None:
            continue

        iv_history = [
            iv_by_date.get(b.date) for b in price_bars[: idx + 1] if iv_by_date.get(b.date)
        ]
        iv_rank = (
            calculate_iv_rank(current_iv, iv_history[-252:])
            if len(iv_history) >= 50
            else None
        )

        if sma_200 is None or rsi is None or iv_rank is None:
            continue

        # BEARISH FILTERS for call credit spread
        if price_below_sma and spot >= sma_200:
            continue
        if rsi >= rsi_threshold:
            continue
        if iv_rank < iv_rank_min:
            continue

        # Construct call credit spread (sell lower strike, buy higher strike)
        dte_years = DTE / 365.0

        # Find 25-delta call for short strike
        short_strike_raw = find_strike_for_delta(spot, dte_years, RATE, current_iv, 0.25, "C")
        if short_strike_raw is None:
            continue
        short_strike = round_strike_to_standard(short_strike_raw, spot)

        # Long strike is higher (5% width)
        width = spot * 0.05
        long_strike = round_strike_to_standard(short_strike + width, spot)

        # Price the spread (for calls, short is lower strike, long is higher)
        pricing = calculate_spread_price_realistic(
            spot,
            short_strike,
            long_strike,
            dte_years,
            RATE,
            current_iv,
            "C",
            bid_ask_spread_pct=spread_pct,
            use_skew=True,
            skew_slope=0.0008,  # Less skew for calls
        )

        if pricing is None or pricing["open_credit"] <= 0:
            continue

        credit = pricing["open_credit"]
        max_loss = pricing["max_loss"]

        # Evaluate with early exit
        take_profit_target = credit * 0.50
        stop_loss_target = max_loss * 0.75

        pnl = None
        reason = ""
        won = False

        for day in range(1, DTE + 1):
            check_idx = idx + day
            if check_idx >= len(price_bars):
                break

            check_bar = price_bars[check_idx]
            check_spot = check_bar.close
            days_left = DTE - day
            t_years = max(days_left / 365.0, 1 / 365.0)

            check_iv = iv_by_date.get(check_bar.date, current_iv)

            close_cost = price_spread_to_close(
                check_spot,
                short_strike,
                long_strike,
                t_years,
                RATE,
                check_iv,
                "C",
                bid_ask_spread_pct=spread_pct,
                use_skew=True,
                skew_slope=0.0008,
            )

            if close_cost is None:
                continue

            current_pnl = (credit - close_cost) * 100

            # Take profit
            if close_cost <= take_profit_target:
                pnl = current_pnl
                won = True
                reason = f"TP day {day}"
                break

            # Stop loss
            if current_pnl <= -stop_loss_target:
                pnl = current_pnl
                won = False
                reason = f"SL day {day}"
                break

        # Hold to expiration if no early exit
        if pnl is None:
            exp_idx = idx + DTE
            if exp_idx < len(price_bars):
                exp_price = price_bars[exp_idx].close
                if exp_price <= short_strike:
                    pnl = credit * 100  # Full profit
                    won = True
                    reason = "Exp OTM"
                elif exp_price >= long_strike:
                    pnl = -max_loss  # Full loss
                    won = False
                    reason = "Exp ITM"
                else:
                    intrinsic = exp_price - short_strike
                    pnl = (credit - intrinsic) * 100
                    won = pnl > 0
                    reason = "Exp partial"

        if pnl is not None:
            trades.append(
                {
                    "symbol": data.symbol,
                    "date": bar.date,
                    "spot": spot,
                    "short": short_strike,
                    "long": long_strike,
                    "credit": credit,
                    "pnl": pnl,
                    "won": won,
                    "reason": reason,
                    "rsi": rsi,
                    "iv_rank": iv_rank,
                    "sma_200": sma_200,
                }
            )
            last_entry_idx = idx

    return trades


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
    print("=" * 70)
    print("CALL CREDIT SPREAD BACKTEST - BEARISH FILTERS")
    print("=" * 70)
    print()

    # Test 1: Strict bearish filters (Price < 200 SMA, RSI < 30)
    print("Test 1: STRICT bearish filters (Price < 200 SMA, RSI < 30)")
    print("-" * 50)
    for spread_pct in [0.01, 0.03, 0.05]:
        all_trades = []
        for symbol, data in symbol_data.items():
            trades = run_call_spread_backtest(
                data,
                spread_pct=spread_pct,
                price_below_sma=True,
                rsi_threshold=30,
                iv_rank_min=0.30,
            )
            all_trades.extend(trades)

        if all_trades:
            total = len(all_trades)
            wins = sum(1 for t in all_trades if t["won"])
            total_pnl = sum(t["pnl"] for t in all_trades)
            print(f"  {int(spread_pct*100)}% spread: {total} trades, {wins/total*100:.1f}% win rate, ${total_pnl:,.0f} PnL")
        else:
            print(f"  {int(spread_pct*100)}% spread: No trades (filters too restrictive)")
    print()

    # Test 2: Relaxed bearish filters (Price < 200 SMA, RSI < 50)
    print("Test 2: RELAXED bearish filters (Price < 200 SMA, RSI < 50)")
    print("-" * 50)
    for spread_pct in [0.01, 0.03, 0.05]:
        all_trades = []
        for symbol, data in symbol_data.items():
            trades = run_call_spread_backtest(
                data,
                spread_pct=spread_pct,
                price_below_sma=True,
                rsi_threshold=50,
                iv_rank_min=0.30,
            )
            all_trades.extend(trades)

        if all_trades:
            total = len(all_trades)
            wins = sum(1 for t in all_trades if t["won"])
            total_pnl = sum(t["pnl"] for t in all_trades)
            print(f"  {int(spread_pct*100)}% spread: {total} trades, {wins/total*100:.1f}% win rate, ${total_pnl:,.0f} PnL")
        else:
            print(f"  {int(spread_pct*100)}% spread: No trades")
    print()

    # Test 3: Only price below SMA filter (no RSI requirement)
    print("Test 3: Price < 200 SMA only (no RSI filter)")
    print("-" * 50)
    for spread_pct in [0.01, 0.03, 0.05]:
        all_trades = []
        for symbol, data in symbol_data.items():
            trades = run_call_spread_backtest(
                data,
                spread_pct=spread_pct,
                price_below_sma=True,
                rsi_threshold=100,  # Effectively no RSI filter
                iv_rank_min=0.30,
            )
            all_trades.extend(trades)

        if all_trades:
            total = len(all_trades)
            wins = sum(1 for t in all_trades if t["won"])
            total_pnl = sum(t["pnl"] for t in all_trades)
            print(f"  {int(spread_pct*100)}% spread: {total} trades, {wins/total*100:.1f}% win rate, ${total_pnl:,.0f} PnL")
        else:
            print(f"  {int(spread_pct*100)}% spread: No trades")
    print()

    # Show sample trades from the best scenario
    print("=" * 70)
    print("Sample Trades (1% spread, Price < 200 SMA, RSI < 50)")
    print("=" * 70)

    all_trades = []
    for symbol, data in symbol_data.items():
        trades = run_call_spread_backtest(
            data,
            spread_pct=0.01,
            price_below_sma=True,
            rsi_threshold=50,
            iv_rank_min=0.30,
        )
        all_trades.extend(trades)

    all_trades.sort(key=lambda t: t["date"])

    for t in all_trades[:20]:
        status = "WIN" if t["won"] else "LOSS"
        print(f"{t['date']} {t['symbol']:5} Spot={t['spot']:.0f} Short={t['short']:.0f} Long={t['long']:.0f} Credit=${t['credit']:.2f} PnL=${t['pnl']:.0f} {status} ({t['reason']})")


if __name__ == "__main__":
    main()
