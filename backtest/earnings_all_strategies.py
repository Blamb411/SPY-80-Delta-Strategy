#!/usr/bin/env python3
"""
Test earnings avoidance on ALL strategies:
- Put Credit Spreads
- Iron Condors
- Call Credit Spreads
"""

import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

import yfinance as yf
from backtest.ibkr_data_fetcher import load_from_cache, is_cache_valid
from backtest.condor_backtest import run_condor_backtest_multi
from backtest.put_spread_backtest import run_put_spread_backtest_multi
from backtest.black_scholes import (
    find_strike_for_delta, round_strike_to_standard,
    calculate_spread_price_realistic, price_spread_to_close,
    calculate_rsi, calculate_iv_rank,
)
import logging

logging.disable(logging.INFO)

DTE = 30
RATE = 0.05


def fetch_all_earnings(symbols):
    """Fetch earnings dates for all symbols."""
    earnings_data = {}
    for i, symbol in enumerate(symbols):
        if (i + 1) % 50 == 0:
            print(f"  Fetched {i+1}/{len(symbols)}...")
        try:
            ticker = yf.Ticker(symbol)
            earnings = ticker.earnings_dates
            if earnings is not None and len(earnings) > 0:
                dates = [d.strftime("%Y-%m-%d") for d in earnings.index]
                earnings_data[symbol] = set(dates)
        except:
            pass
    return earnings_data


def is_near_earnings(trade_date, earnings_dates, days_before=7, days_after=3):
    """Check if trade date is near an earnings announcement."""
    try:
        trade_dt = datetime.strptime(trade_date, "%Y-%m-%d")
    except ValueError:
        return False

    for earn_date_str in earnings_dates:
        try:
            earn_dt = datetime.strptime(earn_date_str, "%Y-%m-%d")
            diff = (earn_dt - trade_dt).days
            if -days_after <= diff <= days_before:
                return True
        except ValueError:
            continue
    return False


def analyze_trades(trades, earnings_data, has_symbol_attr=True):
    """Analyze trades with and without earnings filter."""
    if not trades:
        return None

    # Get attributes correctly based on trade type
    def get_symbol(t):
        return t.symbol if has_symbol_attr else t.get("symbol", "")

    def get_date(t):
        return t.entry_date if has_symbol_attr else t.get("date", "")

    def get_won(t):
        return t.won if has_symbol_attr else t.get("won", False)

    def get_pnl(t):
        return t.pnl if has_symbol_attr else t.get("pnl", 0)

    # Baseline
    total = len(trades)
    wins = sum(1 for t in trades if get_won(t))
    pnl = sum(get_pnl(t) for t in trades)

    # Filtered
    filtered = [
        t for t in trades
        if not is_near_earnings(get_date(t), earnings_data.get(get_symbol(t), set()))
    ]
    f_total = len(filtered)
    f_wins = sum(1 for t in filtered if get_won(t))
    f_pnl = sum(get_pnl(t) for t in filtered)

    return {
        "baseline": {"trades": total, "wins": wins, "pnl": pnl},
        "filtered": {"trades": f_total, "wins": f_wins, "pnl": f_pnl},
    }


def run_call_spreads(symbol_data):
    """Run call credit spread backtest with bearish filter."""
    call_trades = []

    for symbol, data in symbol_data.items():
        price_bars = data.price_bars
        iv_data = data.iv_data
        if len(price_bars) < 250 or len(iv_data) < 100:
            continue

        iv_by_date = {iv.date: iv.iv for iv in iv_data}
        last_entry_idx = -5

        for idx in range(200, len(price_bars) - DTE - 1):
            if idx - last_entry_idx < 5:
                continue

            bar = price_bars[idx]
            spot = bar.close
            hist_prices = [b.close for b in price_bars[:idx + 1]]

            sma_200 = sum(hist_prices[-200:]) / 200
            rsi = calculate_rsi(hist_prices, 14)

            current_iv = iv_by_date.get(bar.date)
            if current_iv is None:
                continue

            iv_history = [
                iv_by_date.get(b.date)
                for b in price_bars[:idx + 1]
                if iv_by_date.get(b.date)
            ]
            iv_rank = (
                calculate_iv_rank(current_iv, iv_history[-252:])
                if len(iv_history) >= 50
                else None
            )

            if rsi is None or iv_rank is None:
                continue

            # Bearish filters
            if spot >= sma_200:
                continue
            if rsi >= 50:
                continue
            if iv_rank < 0.30:
                continue

            dte_years = DTE / 365.0
            short_strike_raw = find_strike_for_delta(
                spot, dte_years, RATE, current_iv, 0.25, "C"
            )
            if short_strike_raw is None:
                continue
            short_strike = round_strike_to_standard(short_strike_raw, spot)
            width = spot * 0.05
            long_strike = round_strike_to_standard(short_strike + width, spot)

            pricing = calculate_spread_price_realistic(
                spot, short_strike, long_strike, dte_years, RATE, current_iv,
                "C", bid_ask_spread_pct=0.01, use_skew=True, skew_slope=0.0008,
            )

            if pricing is None or pricing["open_credit"] <= 0:
                continue

            credit = pricing["open_credit"]
            max_loss = pricing["max_loss"]
            take_profit_target = credit * 0.50
            stop_loss_target = max_loss * 0.75

            pnl = None
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
                    check_spot, short_strike, long_strike, t_years, RATE, check_iv,
                    "C", bid_ask_spread_pct=0.01, use_skew=True, skew_slope=0.0008,
                )
                if close_cost is None:
                    continue

                current_pnl = (credit - close_cost) * 100
                if close_cost <= take_profit_target:
                    pnl = current_pnl
                    won = True
                    break
                if current_pnl <= -stop_loss_target:
                    pnl = current_pnl
                    won = False
                    break

            if pnl is None:
                exp_idx = idx + DTE
                if exp_idx < len(price_bars):
                    exp_price = price_bars[exp_idx].close
                    if exp_price <= short_strike:
                        pnl = credit * 100
                        won = True
                    elif exp_price >= long_strike:
                        pnl = -max_loss
                        won = False
                    else:
                        intrinsic = exp_price - short_strike
                        pnl = (credit - intrinsic) * 100
                        won = pnl > 0

            if pnl is not None:
                call_trades.append({
                    "symbol": symbol,
                    "date": bar.date,
                    "pnl": pnl,
                    "won": won,
                })
                last_entry_idx = idx

    return call_trades


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

    # Fetch earnings dates
    print("Fetching earnings dates from Yahoo Finance...")
    earnings_data = fetch_all_earnings(list(symbol_data.keys()))
    print(f"Found earnings data for {len(earnings_data)} symbols")
    print()

    print("=" * 75)
    print("EARNINGS AVOIDANCE TEST - ALL STRATEGIES")
    print("1% bid/ask spread, 50% TP / 75% SL, Avoid 7 days before / 3 days after")
    print("=" * 75)
    print()

    results = {}

    # ==================== PUT CREDIT SPREADS ====================
    print("Testing PUT CREDIT SPREADS...")
    put_results = run_put_spread_backtest_multi(
        symbol_data, entry_interval_days=5, use_early_exit=True,
        take_profit_pct=0.50, stop_loss_pct=0.75,
        use_realistic_pricing=True, bid_ask_spread_pct=0.01, use_skew=True,
    )
    put_trades = []
    for r in put_results.values():
        put_trades.extend(r.trades)

    results["Put Credit Spread"] = analyze_trades(put_trades, earnings_data, has_symbol_attr=True)

    # ==================== IRON CONDORS ====================
    print("Testing IRON CONDORS...")
    condor_results = run_condor_backtest_multi(
        symbol_data, entry_interval_days=5, use_early_exit=True,
        take_profit_pct=0.50, stop_loss_pct=0.75,
        use_realistic_pricing=True, bid_ask_spread_pct=0.01, use_skew=True,
    )
    condor_trades = []
    for r in condor_results.values():
        condor_trades.extend(r.trades)

    results["Iron Condor"] = analyze_trades(condor_trades, earnings_data, has_symbol_attr=True)

    # ==================== CALL CREDIT SPREADS ====================
    print("Testing CALL CREDIT SPREADS...")
    call_trades = run_call_spreads(symbol_data)
    results["Call Credit Spread"] = analyze_trades(call_trades, earnings_data, has_symbol_attr=False)

    # ==================== PRINT RESULTS ====================
    print()
    print("=" * 75)
    print("RESULTS SUMMARY")
    print("=" * 75)
    print()

    print(f"{'Strategy':<22} | {'--- Baseline ---':<28} | {'--- Avoid Earnings ---':<28} | {'Diff/Trade':>10}")
    print(f"{'':22} | {'Trades':>8} {'Win%':>7} {'P&L':>11} | {'Trades':>8} {'Win%':>7} {'P&L':>11} | {'':>10}")
    print("-" * 95)

    for strategy, data in results.items():
        if data is None:
            print(f"{strategy:<22} | No trades")
            continue

        b = data["baseline"]
        f = data["filtered"]

        b_win_pct = b["wins"] / b["trades"] * 100 if b["trades"] > 0 else 0
        f_win_pct = f["wins"] / f["trades"] * 100 if f["trades"] > 0 else 0

        b_per_trade = b["pnl"] / b["trades"] if b["trades"] > 0 else 0
        f_per_trade = f["pnl"] / f["trades"] if f["trades"] > 0 else 0
        diff_per_trade = f_per_trade - b_per_trade

        print(
            f"{strategy:<22} | "
            f"{b['trades']:>8,} {b_win_pct:>6.1f}% ${b['pnl']:>9,.0f} | "
            f"{f['trades']:>8,} {f_win_pct:>6.1f}% ${f['pnl']:>9,.0f} | "
            f"${diff_per_trade:>+8.2f}"
        )

    print()
    print("=" * 75)
    print("INTERPRETATION")
    print("=" * 75)
    print()
    print("Positive Diff/Trade = Earnings avoidance HELPS")
    print("Negative Diff/Trade = Earnings avoidance HURTS")
    print()
    print("For premium-selling strategies, earnings periods often have elevated IV,")
    print("which means higher premiums collected. Avoiding these periods may reduce")
    print("profitability even though it seems counterintuitive.")


if __name__ == "__main__":
    main()
