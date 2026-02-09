"""
Alpha Analysis with ThetaData
=============================
Uses actual ThetaData options pricing to determine if the strategy
generates alpha beyond leveraged SPY exposure.

This is the definitive test - using real options data, not a proxy.

Usage:
    python alpha_analysis_thetadata.py
"""

import os
import sys
import math
from datetime import datetime

import numpy as np
import pandas as pd
import yfinance as yf

_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_dir = os.path.dirname(os.path.dirname(_this_dir))
sys.path.insert(0, _project_dir)

from backtest.thetadata_client import ThetaDataClient
from backtest.black_scholes import find_strike_for_delta
from backtest.metrics import calculate_all_metrics

# Parameters matching delta_capped_backtest.py
SHARES = 3125
OPTIONS_CASH = 100_000
DELTA = 0.80
DTE_TARGET = 120
DTE_MIN = 90
DTE_MAX = 150
MH = 60
PT = 0.50
RATE = 0.04
SMA_EXIT_THRESHOLD = 0.02

DATA_START = "2014-01-01"
DATA_END = "2026-01-31"
SIM_START = "2015-03-01"


def is_monthly_opex(exp_str):
    exp_dt = datetime.strptime(exp_str, "%Y-%m-%d").date()
    if exp_dt.weekday() != 4:
        return False
    return 15 <= exp_dt.day <= 21


def find_best_expiration(entry_date_str, monthly_exps_dates):
    entry_dt = datetime.strptime(entry_date_str, "%Y-%m-%d").date()
    best_exp, best_dte, best_diff = None, 0, 9999
    for exp_str, exp_dt in monthly_exps_dates:
        dte = (exp_dt - entry_dt).days
        if DTE_MIN <= dte <= DTE_MAX:
            diff = abs(dte - DTE_TARGET)
            if diff < best_diff:
                best_diff, best_exp, best_dte = diff, exp_str, dte
    return best_exp, best_dte


def get_bid_ask(eod_row):
    if eod_row is None:
        return None, None
    bid = eod_row.get("bid", 0) or 0
    ask = eod_row.get("ask", 0) or 0
    if bid > 0 and ask > 0 and ask >= bid:
        return bid, ask
    return None, None


def calculate_delta(spot, strike, dte, iv=0.16):
    if dte <= 0:
        return 1.0 if spot > strike else 0.0
    t = dte / 365.0
    d1 = (math.log(spot / strike) + (RATE + 0.5 * iv * iv) * t) / (iv * math.sqrt(t))
    return 0.5 * (1.0 + math.erf(d1 / math.sqrt(2.0)))


def run_strategy(client, spy_by_date, trading_dates, vix_data, sma200, monthly_exps):
    """Run the actual 80-delta strategy with ThetaData."""
    options_cash = float(OPTIONS_CASH)
    pending_cash = 0.0
    positions = []

    contract_eod = {}
    strikes_cache = {}

    start_idx = next((i for i, d in enumerate(trading_dates) if d >= SIM_START), 0)

    daily_values = []
    daily_returns = []
    dates = []
    prev_value = None

    for day_idx in range(start_idx, len(trading_dates)):
        today = trading_dates[day_idx]
        bar = spy_by_date[today]
        spot = bar["close"]
        sma_val = sma200.get(today)
        above_sma = (spot > sma_val) if sma_val else False
        vix = vix_data.get(today, 20.0)
        iv_est = max(0.08, min(0.90, vix / 100.0))

        shares_value = SHARES * spot
        options_cash += pending_cash
        pending_cash = 0.0

        # Force-exit when below threshold
        pct_below = (sma_val - spot) / sma_val if sma_val and sma_val > 0 else 0
        if pct_below >= SMA_EXIT_THRESHOLD and positions:
            for pos in positions:
                ckey = (pos["expiration"], pos["strike"])
                eod = contract_eod.get(ckey, {}).get(today)
                bid, _ = get_bid_ask(eod)
                if bid is None or bid <= 0:
                    intrinsic = max(0, spot - pos["strike"])
                    bid = intrinsic * 0.998 if intrinsic > 0 else 0.001
                proceeds = bid * 100 * pos["quantity"]
                pending_cash += proceeds
            positions = []

        # Normal exits
        still_open = []
        for pos in positions:
            pos["days_held"] += 1
            ckey = (pos["expiration"], pos["strike"])
            eod = contract_eod.get(ckey, {}).get(today)
            bid, _ = get_bid_ask(eod)
            if bid is None or bid <= 0:
                intrinsic = max(0, spot - pos["strike"])
                bid = intrinsic * 0.998 if intrinsic > 0 else 0.001

            pnl_pct = bid / pos["entry_price"] - 1
            exit_reason = None
            if pnl_pct >= PT:
                exit_reason = "PT"
            elif pos["days_held"] >= MH:
                exit_reason = "MH"

            if exit_reason:
                proceeds = bid * 100 * pos["quantity"]
                pending_cash += proceeds
            else:
                still_open.append(pos)
        positions = still_open

        # Calculate delta
        current_delta = 0.0
        for pos in positions:
            dte = (datetime.strptime(pos["expiration"], "%Y-%m-%d").date() -
                   datetime.strptime(today, "%Y-%m-%d").date()).days
            current_delta += calculate_delta(spot, pos["strike"], dte, iv_est) * pos["quantity"] * 100

        delta_room = SHARES - current_delta

        # Entry
        if above_sma and sma_val and delta_room > 80:
            best_exp, dte_cal = find_best_expiration(today, monthly_exps)
            if best_exp:
                t_years = dte_cal / 365.0
                bs_strike = find_strike_for_delta(spot, t_years, RATE, iv_est, DELTA, "C")
                if bs_strike:
                    if best_exp not in strikes_cache:
                        strikes_cache[best_exp] = client.get_strikes("SPY", best_exp)
                    strikes = strikes_cache.get(best_exp, [])
                    if strikes:
                        real_strike = min(strikes, key=lambda s: abs(s - bs_strike))
                        ckey = (best_exp, real_strike)
                        if ckey not in contract_eod:
                            data = client.prefetch_option_life("SPY", best_exp, real_strike, "C", today)
                            contract_eod[ckey] = {r["bar_date"]: r for r in data}
                        eod = contract_eod[ckey].get(today)
                        _, ask = get_bid_ask(eod)
                        if ask and ask > 0:
                            option_delta = calculate_delta(spot, real_strike, dte_cal, iv_est)
                            max_by_delta = int(delta_room / (option_delta * 100))
                            contract_cost = ask * 100
                            max_by_cash = int(options_cash / contract_cost)
                            qty = min(max_by_delta, max_by_cash, 1)
                            if qty > 0:
                                total_cost = contract_cost * qty
                                options_cash -= total_cost
                                positions.append({
                                    "expiration": best_exp,
                                    "strike": real_strike,
                                    "entry_price": ask,
                                    "quantity": qty,
                                    "contract_cost": total_cost,
                                    "days_held": 0,
                                })

        # Mark to market
        positions_value = 0.0
        for pos in positions:
            ckey = (pos["expiration"], pos["strike"])
            eod = contract_eod.get(ckey, {}).get(today)
            bid, ask = get_bid_ask(eod)
            mid = (bid + ask) / 2.0 if bid and ask else max(0, spot - pos["strike"])
            positions_value += mid * 100 * pos["quantity"]

        portfolio_value = shares_value + options_cash + pending_cash + positions_value
        daily_values.append(portfolio_value)
        dates.append(today)

        if prev_value is not None:
            daily_returns.append(portfolio_value / prev_value - 1)
        prev_value = portfolio_value

    return daily_values, daily_returns, dates


def run_spy_leveraged_bh(spy_by_date, trading_dates, leverage, sim_start=SIM_START):
    """Run leveraged SPY buy-and-hold (no timing)."""
    daily_values = []
    daily_returns = []
    dates = []

    start_idx = next((i for i, d in enumerate(trading_dates) if d >= sim_start), 0)
    first_price = None
    prev_value = None
    initial_value = SHARES * spy_by_date[trading_dates[start_idx]]["close"] + OPTIONS_CASH

    for day_idx in range(start_idx, len(trading_dates)):
        today = trading_dates[day_idx]
        spot = spy_by_date[today]["close"]

        if first_price is None:
            first_price = spot

        # Leveraged return
        spy_return = (spot / first_price - 1)
        leveraged_return = leverage * spy_return
        portfolio_value = initial_value * (1 + leveraged_return)

        daily_values.append(portfolio_value)
        dates.append(today)

        if prev_value is not None:
            daily_returns.append(portfolio_value / prev_value - 1)
        prev_value = portfolio_value

    return daily_values, daily_returns, dates


def run_spy_bh(spy_by_date, trading_dates, sim_start=SIM_START):
    """Run SPY buy-and-hold (no leverage)."""
    daily_values = []
    daily_returns = []
    dates = []

    start_idx = next((i for i, d in enumerate(trading_dates) if d >= sim_start), 0)
    first_price = None
    prev_value = None
    initial_value = SHARES * spy_by_date[trading_dates[start_idx]]["close"] + OPTIONS_CASH

    for day_idx in range(start_idx, len(trading_dates)):
        today = trading_dates[day_idx]
        spot = spy_by_date[today]["close"]

        if first_price is None:
            first_price = spot

        spy_return = (spot / first_price - 1)
        portfolio_value = initial_value * (1 + spy_return)

        daily_values.append(portfolio_value)
        dates.append(today)

        if prev_value is not None:
            daily_returns.append(portfolio_value / prev_value - 1)
        prev_value = portfolio_value

    return daily_values, daily_returns, dates


def calculate_beta(strategy_returns, benchmark_returns):
    """Calculate beta of strategy vs benchmark."""
    min_len = min(len(strategy_returns), len(benchmark_returns))
    sr = np.array(strategy_returns[:min_len])
    br = np.array(benchmark_returns[:min_len])
    cov = np.cov(sr, br)[0, 1]
    var = np.var(br)
    return cov / var if var > 0 else 1.0


def calculate_alpha(strategy_returns, benchmark_returns, beta, rf_annual=0.04):
    """Calculate Jensen's alpha."""
    rf_daily = rf_annual / 252
    min_len = min(len(strategy_returns), len(benchmark_returns))
    sr = np.array(strategy_returns[:min_len])
    br = np.array(benchmark_returns[:min_len])

    strategy_mean = np.mean(sr)
    benchmark_mean = np.mean(br)

    expected = rf_daily + beta * (benchmark_mean - rf_daily)
    alpha_daily = strategy_mean - expected
    return alpha_daily * 252


def main():
    print("=" * 80)
    print("ALPHA ANALYSIS WITH THETADATA")
    print("=" * 80)
    print()
    print("Using ACTUAL options data to determine if strategy generates alpha.")
    print()

    # Connect to ThetaData
    client = ThetaDataClient()
    if not client.connect():
        print("ERROR: Cannot connect to Theta Terminal.")
        return

    print("Loading data...")
    spy_bars = client.fetch_spy_bars(DATA_START, DATA_END)
    spy_by_date = {b["bar_date"]: b for b in spy_bars}
    trading_dates = sorted(spy_by_date.keys())

    vix_data = client.fetch_vix_history(DATA_START, DATA_END)

    sma200 = {}
    for i in range(199, len(trading_dates)):
        window = [spy_by_date[trading_dates[j]]["close"] for j in range(i - 199, i + 1)]
        sma200[trading_dates[i]] = sum(window) / 200.0

    all_exps = client.get_expirations("SPY")
    monthly_exps = [(e, datetime.strptime(e, "%Y-%m-%d").date())
                    for e in all_exps if is_monthly_opex(e)]
    monthly_exps.sort(key=lambda x: x[1])

    print(f"Data: {len(trading_dates)} trading days")
    print(f"Period: {SIM_START} to {trading_dates[-1]}")
    print()

    # Run strategies
    print("Running 80-delta strategy with ThetaData...")
    strat_values, strat_returns, strat_dates = run_strategy(
        client, spy_by_date, trading_dates, vix_data, sma200, monthly_exps
    )
    strat_metrics = calculate_all_metrics(strat_values, strat_dates)

    print("Running SPY B&H...")
    spy_values, spy_returns, spy_dates = run_spy_bh(spy_by_date, trading_dates)
    spy_metrics = calculate_all_metrics(spy_values, spy_dates)

    # Calculate beta
    beta = calculate_beta(strat_returns, spy_returns)
    print(f"\nStrategy beta vs SPY: {beta:.3f}")

    # Run leveraged SPY at same beta
    print(f"Running leveraged SPY B&H at beta={beta:.2f}...")
    lev_values, lev_returns, lev_dates = run_spy_leveraged_bh(
        spy_by_date, trading_dates, leverage=beta
    )
    lev_metrics = calculate_all_metrics(lev_values, lev_dates)

    # Calculate alpha
    alpha = calculate_alpha(strat_returns, spy_returns, beta)

    client.close()

    # Print results
    print()
    print("=" * 80)
    print("RESULTS: ACTUAL THETADATA OPTIONS")
    print("=" * 80)
    print()
    print(f"{'Strategy':<45} {'CAGR':>10} {'Sharpe':>10} {'Sortino':>10} {'Max DD':>10}")
    print("-" * 85)
    print(f"{'SPY B&H (beta=1.0)':<45} {spy_metrics.cagr:>+9.1%} {spy_metrics.sharpe_ratio:>10.3f} "
          f"{spy_metrics.sortino_ratio:>10.3f} {spy_metrics.max_drawdown:>9.1%}")
    print(f"{'Leveraged SPY B&H (beta={beta:.2f}, no timing)':<45} {lev_metrics.cagr:>+9.1%} {lev_metrics.sharpe_ratio:>10.3f} "
          f"{lev_metrics.sortino_ratio:>10.3f} {lev_metrics.max_drawdown:>9.1%}")
    print(f"{'80-Delta Strategy (beta={beta:.2f}, with timing)':<45} {strat_metrics.cagr:>+9.1%} {strat_metrics.sharpe_ratio:>10.3f} "
          f"{strat_metrics.sortino_ratio:>10.3f} {strat_metrics.max_drawdown:>9.1%}")

    print()
    print("=" * 80)
    print("ALPHA ANALYSIS")
    print("=" * 80)
    print()
    print(f"Jensen's Alpha (annualized): {alpha:>+.2%}")
    print()
    print(f"Sharpe Comparison at same beta ({beta:.2f}):")
    print(f"  Leveraged SPY B&H:  {lev_metrics.sharpe_ratio:.3f}")
    print(f"  80-Delta Strategy:  {strat_metrics.sharpe_ratio:.3f}")
    sharpe_diff = strat_metrics.sharpe_ratio - lev_metrics.sharpe_ratio
    print(f"  Difference:         {sharpe_diff:+.3f}")
    print()

    if alpha > 0.005:
        print("CONCLUSION: Strategy generates POSITIVE ALPHA")
        print(f"  The SMA filter and options mechanics add {alpha:+.2%} annual return")
        print(f"  beyond what leverage alone would provide.")
    elif alpha > -0.005:
        print("CONCLUSION: Strategy generates MINIMAL ALPHA")
        print("  Returns are approximately what you'd expect from leverage.")
        print("  The SMA filter may help with drawdowns but not returns.")
    else:
        print("CONCLUSION: Strategy generates NEGATIVE ALPHA")
        print("  Leveraged SPY without timing would have done better.")
        print("  The SMA filter caused missed gains in this bull market.")

    print()
    print("-" * 80)
    print("KEY INSIGHT")
    print("-" * 80)
    print()
    if strat_metrics.sharpe_ratio > lev_metrics.sharpe_ratio:
        print("The strategy has HIGHER Sharpe than leveraged B&H at same beta.")
        print("This means the timing (SMA filter) IMPROVES risk-adjusted returns.")
        print()
        print("Benefit breakdown:")
        print(f"  - Sharpe improvement: +{sharpe_diff:.3f}")
        print(f"  - Max DD improvement: {strat_metrics.max_drawdown - lev_metrics.max_drawdown:+.1%}")
    else:
        print("The strategy has LOWER Sharpe than leveraged B&H at same beta.")
        print("In this period, simply holding leveraged SPY was better.")
        print()
        print("However, this 2015-2026 period was a strong bull market.")
        print("The SMA filter's value appears during BEAR markets (see 2008 test).")


if __name__ == "__main__":
    main()
