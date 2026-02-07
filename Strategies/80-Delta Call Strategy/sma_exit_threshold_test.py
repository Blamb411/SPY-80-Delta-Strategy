"""
SMA Exit Threshold Test
=======================
Tests different SMA exit thresholds (0%, 1%, 2%, 3%) to determine
optimal point for exiting all positions when SPY falls below SMA200.

Usage:
    python sma_exit_threshold_test.py
"""

import os
import sys
import math
import logging
from datetime import datetime
from collections import defaultdict

import numpy as np
import pandas as pd

_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_dir = os.path.dirname(os.path.dirname(_this_dir))
sys.path.insert(0, _project_dir)

from backtest.thetadata_client import ThetaDataClient
from backtest.black_scholes import find_strike_for_delta

# Parameters
SHARES = 3125
DELTA = 0.80
DTE_TARGET = 120
DTE_MIN = 90
DTE_MAX = 150
MH = 60
PT = 0.50
RATE = 0.04
OPTIONS_CASH = 100_000

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
    close = eod_row.get("close", 0) or 0
    if close > 0:
        return close * 0.998, close * 1.002
    return None, None


def calculate_delta(spot, strike, dte, iv=0.16):
    if dte <= 0:
        return 1.0 if spot > strike else 0.0
    t = dte / 365.0
    d1 = (math.log(spot / strike) + (RATE + 0.5 * iv * iv) * t) / (iv * math.sqrt(t))
    return 0.5 * (1.0 + math.erf(d1 / math.sqrt(2.0)))


def run_simulation(client, spy_by_date, trading_dates, vix_data, sma200,
                   monthly_exps, exit_threshold, contract_eod, strikes_cache):
    """Run simulation with specific SMA exit threshold."""
    options_cash = float(OPTIONS_CASH)
    pending_cash = 0.0
    positions = []
    trade_log = []
    force_exit_count = 0

    start_idx = next((i for i, d in enumerate(trading_dates) if d >= SIM_START), 0)
    daily_values = []

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
        if pct_below >= exit_threshold and positions:
            for pos in positions:
                ckey = (pos["expiration"], pos["strike"])
                eod = contract_eod.get(ckey, {}).get(today)
                bid, _ = get_bid_ask(eod)
                if bid is None or bid <= 0:
                    intrinsic = max(0, spot - pos["strike"])
                    bid = intrinsic * 0.998 if intrinsic > 0 else 0.001
                proceeds = bid * 100 * pos["quantity"]
                pending_cash += proceeds
                pnl_pct = bid / pos["entry_price"] - 1
                trade_log.append({
                    "pnl_pct": pnl_pct,
                    "pnl_dollar": proceeds - pos["contract_cost"],
                    "exit_reason": "SMA",
                })
                force_exit_count += 1
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
                trade_log.append({
                    "pnl_pct": pnl_pct,
                    "pnl_dollar": proceeds - pos["contract_cost"],
                    "exit_reason": exit_reason,
                })
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

    # Compute metrics
    df = pd.DataFrame({"value": daily_values})
    df["ret"] = df["value"].pct_change().fillna(0)

    years = len(df) / 252.0
    total_ret = df["value"].iloc[-1] / df["value"].iloc[0] - 1
    cagr = (df["value"].iloc[-1] / df["value"].iloc[0]) ** (1/years) - 1
    sharpe = (df["ret"].mean() / df["ret"].std()) * np.sqrt(252) if df["ret"].std() > 0 else 0
    max_dd = (df["value"] / df["value"].cummax() - 1).min()

    tdf = pd.DataFrame(trade_log)
    n_trades = len(tdf)
    win_rate = len(tdf[tdf["pnl_pct"] > 0]) / n_trades if n_trades > 0 else 0
    total_pnl = tdf["pnl_dollar"].sum() if n_trades > 0 else 0
    sma_exits = len(tdf[tdf["exit_reason"] == "SMA"]) if n_trades > 0 else 0

    return {
        "threshold": exit_threshold,
        "cagr": cagr,
        "sharpe": sharpe,
        "max_dd": max_dd,
        "trades": n_trades,
        "win_rate": win_rate,
        "total_pnl": total_pnl,
        "sma_exits": sma_exits,
    }


def main():
    logging.basicConfig(level=logging.WARNING)

    print("=" * 80)
    print("SMA EXIT THRESHOLD TEST")
    print("=" * 80)

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

    print(f"Data loaded: {len(trading_dates)} trading days")

    # Shared caches
    contract_eod = {}
    strikes_cache = {}

    # Test thresholds
    thresholds = [0.0, 0.01, 0.02, 0.03]
    results = []

    for thresh in thresholds:
        print(f"\nTesting threshold: {thresh:.0%}...")
        result = run_simulation(
            client, spy_by_date, trading_dates, vix_data, sma200,
            monthly_exps, thresh, contract_eod, strikes_cache
        )
        results.append(result)
        print(f"  CAGR: {result['cagr']:+.1%}, Sharpe: {result['sharpe']:.2f}, "
              f"SMA Exits: {result['sma_exits']}")

    # Print comparison table
    print("\n" + "=" * 80)
    print("SMA EXIT THRESHOLD COMPARISON")
    print("=" * 80)
    print(f"\n  {'Threshold':<12} {'CAGR':>8} {'Sharpe':>8} {'Max DD':>9} "
          f"{'Trades':>8} {'Win Rate':>10} {'SMA Exits':>10} {'Total P&L':>12}")
    print(f"  {'-' * 85}")

    for r in results:
        thresh_str = f"{r['threshold']:.0%}"
        print(f"  {thresh_str:<12} {r['cagr']:>+7.1%} {r['sharpe']:>8.2f} "
              f"{r['max_dd']:>8.1%} {r['trades']:>8} {r['win_rate']:>9.1%} "
              f"{r['sma_exits']:>10} ${r['total_pnl']:>11,.0f}")

    print("\n" + "=" * 80)
    print("CONCLUSION")
    print("=" * 80)
    best = max(results, key=lambda x: x['sharpe'])
    print(f"\nBest risk-adjusted returns: {best['threshold']:.0%} threshold")
    print(f"  - Sharpe: {best['sharpe']:.2f}")
    print(f"  - CAGR: {best['cagr']:+.1%}")
    print(f"  - SMA exits: {best['sma_exits']}")

    client.close()


if __name__ == "__main__":
    main()
