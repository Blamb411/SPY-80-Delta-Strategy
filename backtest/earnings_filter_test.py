#!/usr/bin/env python3
"""
Test impact of avoiding earnings dates on put credit spread performance.
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))

import yfinance as yf
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


def fetch_earnings_dates(symbol):
    """Fetch historical earnings dates for a symbol from Yahoo Finance."""
    try:
        ticker = yf.Ticker(symbol)
        earnings = ticker.earnings_dates
        if earnings is not None and len(earnings) > 0:
            # Convert index to list of date strings (YYYY-MM-DD)
            dates = [d.strftime("%Y-%m-%d") for d in earnings.index]
            return set(dates)
    except Exception:
        pass
    return set()


def is_near_earnings(trade_date, earnings_dates, days_before=7, days_after=3):
    """
    Check if a trade date is within the danger zone around earnings.

    Args:
        trade_date: Date string (YYYY-MM-DD)
        earnings_dates: Set of earnings date strings
        days_before: Days before earnings to avoid
        days_after: Days after earnings to avoid
    """
    try:
        trade_dt = datetime.strptime(trade_date, "%Y-%m-%d")
    except ValueError:
        return False

    for earn_date_str in earnings_dates:
        try:
            earn_dt = datetime.strptime(earn_date_str, "%Y-%m-%d")
            diff = (earn_dt - trade_dt).days

            # If earnings is within the danger window
            if -days_after <= diff <= days_before:
                return True
        except ValueError:
            continue

    return False


def run_put_spread_test(symbol_data, earnings_data, avoid_earnings=False,
                        days_before=7, days_after=3, spread_pct=0.01):
    """Run put spread backtest with optional earnings avoidance."""
    all_trades = []

    for symbol, data in symbol_data.items():
        price_bars = data.price_bars
        iv_data = data.iv_data

        if len(price_bars) < 250 or len(iv_data) < 100:
            continue

        earnings_dates = earnings_data.get(symbol, set())
        iv_by_date = {iv.date: iv.iv for iv in iv_data}
        last_entry_idx = -5

        for idx in range(200, len(price_bars) - DTE - 1):
            if idx - last_entry_idx < 5:
                continue

            bar = price_bars[idx]
            spot = bar.close
            entry_date = bar.date

            # Skip if near earnings
            if avoid_earnings and is_near_earnings(entry_date, earnings_dates, days_before, days_after):
                continue

            hist_prices = [b.close for b in price_bars[:idx+1]]

            sma_200 = calculate_sma(hist_prices, 200)
            rsi = calculate_rsi(hist_prices, 14)

            current_iv = iv_by_date.get(bar.date)
            if current_iv is None:
                continue

            iv_history = [iv_by_date.get(b.date) for b in price_bars[:idx+1] if iv_by_date.get(b.date)]
            iv_rank = calculate_iv_rank(current_iv, iv_history[-252:]) if len(iv_history) >= 50 else None

            if sma_200 is None or rsi is None or iv_rank is None:
                continue

            # Filters
            if spot <= sma_200:
                continue
            if rsi >= 75:
                continue
            if iv_rank < 0.30:
                continue

            dte_years = DTE / 365.0
            short_strike_raw = find_strike_for_delta(spot, dte_years, RATE, current_iv, -0.25, 'P')
            if short_strike_raw is None:
                continue
            short_strike = round_strike_to_standard(short_strike_raw, spot)

            width = spot * 0.05
            long_strike = round_strike_to_standard(short_strike - width, spot)

            pricing = calculate_spread_price_realistic(
                spot, short_strike, long_strike, dte_years, RATE, current_iv,
                'P', bid_ask_spread_pct=spread_pct, use_skew=True
            )

            if pricing is None or pricing['open_credit'] <= 0:
                continue

            credit = pricing['open_credit']
            max_loss = pricing['max_loss']
            take_profit_target = credit * 0.50
            stop_loss_target = max_loss * 0.75

            pnl = None
            won = False
            reason = ""

            for day in range(1, DTE + 1):
                check_idx = idx + day
                if check_idx >= len(price_bars):
                    break

                check_bar = price_bars[check_idx]
                check_spot = check_bar.close
                days_left = DTE - day
                t_years = max(days_left / 365.0, 1/365.0)
                check_iv = iv_by_date.get(check_bar.date, current_iv)

                close_cost = price_spread_to_close(
                    check_spot, short_strike, long_strike, t_years, RATE, check_iv,
                    'P', bid_ask_spread_pct=spread_pct, use_skew=True
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

            if pnl is not None:
                all_trades.append({
                    'symbol': symbol,
                    'date': entry_date,
                    'pnl': pnl,
                    'won': won,
                    'reason': reason,
                })
                last_entry_idx = idx

    return all_trades


def main():
    # Load cached price/IV data
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

    # Fetch earnings dates for all symbols
    print("Fetching earnings dates from Yahoo Finance...")
    earnings_data = {}
    symbols_with_earnings = 0

    for i, symbol in enumerate(symbol_data.keys()):
        if (i + 1) % 20 == 0:
            print(f"  Progress: {i+1}/{len(symbol_data)}")
        earnings_dates = fetch_earnings_dates(symbol)
        if earnings_dates:
            earnings_data[symbol] = earnings_dates
            symbols_with_earnings += 1

    print(f"Found earnings data for {symbols_with_earnings} symbols")
    print()

    print("=" * 70)
    print("EARNINGS AVOIDANCE TEST")
    print("Put Credit Spread, 200-day SMA, 1% spread, 50% TP / 75% SL")
    print("=" * 70)
    print()

    # Test 1: No earnings filter (baseline)
    print("Running baseline (no earnings filter)...")
    baseline_trades = run_put_spread_test(symbol_data, earnings_data, avoid_earnings=False)

    baseline_total = len(baseline_trades)
    baseline_wins = sum(1 for t in baseline_trades if t['won'])
    baseline_pnl = sum(t['pnl'] for t in baseline_trades)
    baseline_losses = [t for t in baseline_trades if not t['won']]
    baseline_avg_loss = sum(t['pnl'] for t in baseline_losses) / len(baseline_losses) if baseline_losses else 0

    print(f"Baseline: {baseline_total:,} trades, {baseline_wins/baseline_total*100:.1f}% win rate")
    print(f"          ${baseline_pnl:,.0f} total P&L (${baseline_pnl/baseline_total:.2f}/trade)")
    print(f"          Avg loss: ${baseline_avg_loss:.2f}")
    print()

    # Test 2: Avoid 7 days before, 3 days after earnings
    print("Running with earnings avoidance (7 days before, 3 days after)...")
    avoid_trades = run_put_spread_test(
        symbol_data, earnings_data,
        avoid_earnings=True, days_before=7, days_after=3
    )

    avoid_total = len(avoid_trades)
    avoid_wins = sum(1 for t in avoid_trades if t['won'])
    avoid_pnl = sum(t['pnl'] for t in avoid_trades)
    avoid_losses = [t for t in avoid_trades if not t['won']]
    avoid_avg_loss = sum(t['pnl'] for t in avoid_losses) / len(avoid_losses) if avoid_losses else 0

    print(f"Avoid Earnings: {avoid_total:,} trades, {avoid_wins/avoid_total*100:.1f}% win rate")
    print(f"                ${avoid_pnl:,.0f} total P&L (${avoid_pnl/avoid_total:.2f}/trade)")
    print(f"                Avg loss: ${avoid_avg_loss:.2f}")
    print()

    # Test 3: More aggressive avoidance (14 days before, 5 days after)
    print("Running with aggressive avoidance (14 days before, 5 days after)...")
    aggressive_trades = run_put_spread_test(
        symbol_data, earnings_data,
        avoid_earnings=True, days_before=14, days_after=5
    )

    agg_total = len(aggressive_trades)
    agg_wins = sum(1 for t in aggressive_trades if t['won'])
    agg_pnl = sum(t['pnl'] for t in aggressive_trades)
    agg_losses = [t for t in aggressive_trades if not t['won']]
    agg_avg_loss = sum(t['pnl'] for t in agg_losses) / len(agg_losses) if agg_losses else 0

    print(f"Aggressive:     {agg_total:,} trades, {agg_wins/agg_total*100:.1f}% win rate")
    print(f"                ${agg_pnl:,.0f} total P&L (${agg_pnl/agg_total:.2f}/trade)")
    print(f"                Avg loss: ${agg_avg_loss:.2f}")
    print()

    # Summary
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print()
    print(f"{'Filter':<25} {'Trades':>8} {'Win%':>8} {'Total P&L':>12} {'Per Trade':>10} {'Avg Loss':>10}")
    print("-" * 75)
    print(f"{'No Filter (Baseline)':<25} {baseline_total:>8,} {baseline_wins/baseline_total*100:>7.1f}% ${baseline_pnl:>10,.0f} ${baseline_pnl/baseline_total:>9.2f} ${baseline_avg_loss:>9.2f}")
    print(f"{'Avoid 7d before/3d after':<25} {avoid_total:>8,} {avoid_wins/avoid_total*100:>7.1f}% ${avoid_pnl:>10,.0f} ${avoid_pnl/avoid_total:>9.2f} ${avoid_avg_loss:>9.2f}")
    print(f"{'Avoid 14d before/5d after':<25} {agg_total:>8,} {agg_wins/agg_total*100:>7.1f}% ${agg_pnl:>10,.0f} ${agg_pnl/agg_total:>9.2f} ${agg_avg_loss:>9.2f}")
    print()

    # Calculate improvement
    trades_avoided = baseline_total - avoid_total
    pnl_diff = avoid_pnl - baseline_pnl
    per_trade_diff = (avoid_pnl/avoid_total) - (baseline_pnl/baseline_total)

    print(f"Earnings filter (7d/3d) avoided {trades_avoided} trades")
    print(f"P&L difference: ${pnl_diff:+,.0f}")
    print(f"Per-trade improvement: ${per_trade_diff:+.2f}")


if __name__ == "__main__":
    main()
