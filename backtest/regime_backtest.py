#!/usr/bin/env python3
"""
Market Regime Backtest
======================
Tests credit spreads using SPY's 200-day SMA as a market regime filter.
- Bull regime (SPY > 200 SMA): Put credit spreads
- Bear regime (SPY < 200 SMA): Call credit spreads
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from backtest.ibkr_data_fetcher import load_from_cache, is_cache_valid
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


def build_spy_regime(spy_data):
    """Build a dictionary of date -> bull regime (True/False)."""
    regime = {}
    spy_prices = [b.close for b in spy_data.price_bars]

    for i, bar in enumerate(spy_data.price_bars):
        if i >= 199:
            sma_200 = sum(spy_prices[i - 199 : i + 1]) / 200
            regime[bar.date] = bar.close > sma_200

    return regime


def run_spread_with_regime(
    data,
    spy_regime,
    spread_pct,
    trade_in_bull,
    spread_type="put",
):
    """
    Run credit spread backtest with SPY regime filter.

    Args:
        data: SymbolData for the stock to trade
        spy_regime: dict of date -> True if bull regime
        spread_pct: bid/ask spread percentage
        trade_in_bull: True = only trade in bull regime, False = only in bear
        spread_type: 'put' or 'call'
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

        # Check SPY regime
        regime = spy_regime.get(bar.date)
        if regime is None:
            continue

        # Filter by regime
        if trade_in_bull and not regime:
            continue
        if not trade_in_bull and regime:
            continue

        # Get IV
        current_iv = iv_by_date.get(bar.date)
        if current_iv is None:
            continue

        iv_history = [
            iv_by_date.get(b.date)
            for b in price_bars[: idx + 1]
            if iv_by_date.get(b.date)
        ]
        iv_rank = (
            calculate_iv_rank(current_iv, iv_history[-252:])
            if len(iv_history) >= 50
            else None
        )

        if iv_rank is None or iv_rank < 0.30:
            continue

        # Stock-level RSI filter
        hist_prices = [b.close for b in price_bars[: idx + 1]]
        rsi = calculate_rsi(hist_prices, 14)
        if rsi is None:
            continue

        # RSI filters
        if spread_type == "put" and rsi >= 75:
            continue
        if spread_type == "call" and rsi <= 25:
            continue

        dte_years = DTE / 365.0

        if spread_type == "put":
            short_strike_raw = find_strike_for_delta(
                spot, dte_years, RATE, current_iv, -0.25, "P"
            )
            if short_strike_raw is None:
                continue
            short_strike = round_strike_to_standard(short_strike_raw, spot)
            width = spot * 0.05
            long_strike = round_strike_to_standard(short_strike - width, spot)
            opt_type = "P"
            skew = 0.0015
        else:
            short_strike_raw = find_strike_for_delta(
                spot, dte_years, RATE, current_iv, 0.25, "C"
            )
            if short_strike_raw is None:
                continue
            short_strike = round_strike_to_standard(short_strike_raw, spot)
            width = spot * 0.05
            long_strike = round_strike_to_standard(short_strike + width, spot)
            opt_type = "C"
            skew = 0.0008

        pricing = calculate_spread_price_realistic(
            spot,
            short_strike,
            long_strike,
            dte_years,
            RATE,
            current_iv,
            opt_type,
            bid_ask_spread_pct=spread_pct,
            use_skew=True,
            skew_slope=skew,
        )

        if pricing is None or pricing["open_credit"] <= 0:
            continue

        credit = pricing["open_credit"]
        max_loss = pricing["max_loss"]
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
                opt_type,
                bid_ask_spread_pct=spread_pct,
                use_skew=True,
                skew_slope=skew,
            )

            if close_cost is None:
                continue

            current_pnl = (credit - close_cost) * 100

            if close_cost <= take_profit_target:
                pnl = current_pnl
                won = True
                reason = f"TP day {day}"
                break

            if current_pnl <= -stop_loss_target:
                pnl = current_pnl
                won = False
                reason = f"SL day {day}"
                break

        if pnl is None:
            exp_idx = idx + DTE
            if exp_idx < len(price_bars):
                exp_price = price_bars[exp_idx].close
                if spread_type == "put":
                    if exp_price >= short_strike:
                        pnl = credit * 100
                        won = True
                        reason = "Exp OTM"
                    elif exp_price <= long_strike:
                        pnl = -max_loss
                        won = False
                        reason = "Exp ITM"
                    else:
                        intrinsic = short_strike - exp_price
                        pnl = (credit - intrinsic) * 100
                        won = pnl > 0
                        reason = "Exp partial"
                else:
                    if exp_price <= short_strike:
                        pnl = credit * 100
                        won = True
                        reason = "Exp OTM"
                    elif exp_price >= long_strike:
                        pnl = -max_loss
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
                    "regime": "bull" if regime else "bear",
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

    # Build SPY regime
    spy_data = symbol_data.get("SPY")
    if not spy_data:
        print("ERROR: SPY data not found")
        return

    spy_regime = build_spy_regime(spy_data)

    bull_days = sum(1 for v in spy_regime.values() if v)
    bear_days = sum(1 for v in spy_regime.values() if not v)
    print(f"SPY Regime: {bull_days} bull days, {bear_days} bear days")
    print()

    print("=" * 70)
    print("MARKET REGIME FILTER: Using SPY 200-day SMA")
    print("=" * 70)
    print()

    # Test 1: Put credit spreads in BULL regime
    print("PUT CREDIT SPREADS - Bull Regime Only (SPY > 200 SMA)")
    print("-" * 50)
    for spread_pct in [0.01, 0.03, 0.05]:
        all_trades = []
        for symbol, data in symbol_data.items():
            trades = run_spread_with_regime(
                data, spy_regime, spread_pct, trade_in_bull=True, spread_type="put"
            )
            all_trades.extend(trades)

        if all_trades:
            total = len(all_trades)
            wins = sum(1 for t in all_trades if t["won"])
            total_pnl = sum(t["pnl"] for t in all_trades)
            avg_pnl = total_pnl / total
            print(
                f"  {int(spread_pct*100)}% spread: {total} trades, "
                f"{wins/total*100:.1f}% win, ${total_pnl:,.0f} PnL (${avg_pnl:.2f}/trade)"
            )

    print()

    # Test 2: Call credit spreads in BEAR regime
    print("CALL CREDIT SPREADS - Bear Regime Only (SPY < 200 SMA)")
    print("-" * 50)
    for spread_pct in [0.01, 0.03, 0.05]:
        all_trades = []
        for symbol, data in symbol_data.items():
            trades = run_spread_with_regime(
                data, spy_regime, spread_pct, trade_in_bull=False, spread_type="call"
            )
            all_trades.extend(trades)

        if all_trades:
            total = len(all_trades)
            wins = sum(1 for t in all_trades if t["won"])
            total_pnl = sum(t["pnl"] for t in all_trades)
            avg_pnl = total_pnl / total
            print(
                f"  {int(spread_pct*100)}% spread: {total} trades, "
                f"{wins/total*100:.1f}% win, ${total_pnl:,.0f} PnL (${avg_pnl:.2f}/trade)"
            )
        else:
            print(f"  {int(spread_pct*100)}% spread: No trades")

    print()

    # Test 3: Combined strategy (puts in bull, calls in bear)
    print("COMBINED STRATEGY - Puts in Bull + Calls in Bear")
    print("-" * 50)
    for spread_pct in [0.01, 0.03, 0.05]:
        all_trades = []
        for symbol, data in symbol_data.items():
            # Put spreads in bull regime
            trades = run_spread_with_regime(
                data, spy_regime, spread_pct, trade_in_bull=True, spread_type="put"
            )
            all_trades.extend(trades)
            # Call spreads in bear regime
            trades = run_spread_with_regime(
                data, spy_regime, spread_pct, trade_in_bull=False, spread_type="call"
            )
            all_trades.extend(trades)

        if all_trades:
            total = len(all_trades)
            wins = sum(1 for t in all_trades if t["won"])
            total_pnl = sum(t["pnl"] for t in all_trades)

            # Break down by regime
            bull_trades = [t for t in all_trades if t["regime"] == "bull"]
            bear_trades = [t for t in all_trades if t["regime"] == "bear"]
            bull_pnl = sum(t["pnl"] for t in bull_trades)
            bear_pnl = sum(t["pnl"] for t in bear_trades)

            print(
                f"  {int(spread_pct*100)}% spread: {total} trades, "
                f"{wins/total*100:.1f}% win, ${total_pnl:,.0f} total"
            )
            print(f"      Bull regime (puts): {len(bull_trades)} trades, ${bull_pnl:,.0f}")
            print(f"      Bear regime (calls): {len(bear_trades)} trades, ${bear_pnl:,.0f}")

    print()
    print("=" * 70)
    print("COMPARISON SUMMARY (1% spread)")
    print("=" * 70)


if __name__ == "__main__":
    main()
