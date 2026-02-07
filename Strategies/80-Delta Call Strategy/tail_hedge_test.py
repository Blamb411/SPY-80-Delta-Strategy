"""
Tail Risk Hedge Test
====================
Tests adding rolling far OTM puts as portfolio insurance against
significant market declines (tail risk hedging).

Strategies tested:
A: Base strategy (calls only, no hedge)
B: Rolling 5-delta puts (~20-25% OTM) - always on
C: Tactical puts - only buy puts when within 5% of SMA200

The goal is to determine if the insurance cost is worth the
reduced drawdown and improved risk-adjusted returns.

Usage:
    python tail_hedge_test.py
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

# Base strategy parameters
SHARES = 3125
CALL_DELTA = 0.80
DTE_TARGET = 120
DTE_MIN = 90
DTE_MAX = 150
MH = 60
PT = 0.50
RATE = 0.04
SMA_EXIT_THRESHOLD = 0.02
OPTIONS_CASH = 100_000

# Tail hedge parameters
HEDGE_PUT_DELTA = 0.05  # 5-delta puts (~20-25% OTM)
HEDGE_DTE_TARGET = 90   # 90 DTE
HEDGE_DTE_MIN = 60
HEDGE_DTE_MAX = 120
HEDGE_ROLL_DTE = 30     # Roll when 30 DTE remaining
HEDGE_ANNUAL_BUDGET_PCT = 0.01  # 1% of portfolio annually for hedge

# Tactical hedge parameters
TACTICAL_SMA_THRESHOLD = 0.05  # Only hedge when within 5% of SMA

DATA_START = "2014-01-01"
DATA_END = "2026-01-31"
SIM_START = "2015-03-01"


def is_monthly_opex(exp_str):
    exp_dt = datetime.strptime(exp_str, "%Y-%m-%d").date()
    if exp_dt.weekday() != 4:
        return False
    return 15 <= exp_dt.day <= 21


def find_best_expiration(entry_date_str, monthly_exps_dates, dte_target=DTE_TARGET,
                         dte_min=DTE_MIN, dte_max=DTE_MAX):
    entry_dt = datetime.strptime(entry_date_str, "%Y-%m-%d").date()
    best_exp, best_dte, best_diff = None, 0, 9999
    for exp_str, exp_dt in monthly_exps_dates:
        dte = (exp_dt - entry_dt).days
        if dte_min <= dte <= dte_max:
            diff = abs(dte - dte_target)
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


def calculate_delta(spot, strike, dte, iv=0.16, right="C"):
    if dte <= 0:
        if right == "C":
            return 1.0 if spot > strike else 0.0
        else:
            return -1.0 if spot < strike else 0.0
    t = dte / 365.0
    d1 = (math.log(spot / strike) + (RATE + 0.5 * iv * iv) * t) / (iv * math.sqrt(t))
    call_delta = 0.5 * (1.0 + math.erf(d1 / math.sqrt(2.0)))
    if right == "P":
        return call_delta - 1.0
    return call_delta


def find_put_strike_for_delta(spot, t, rate, iv, target_delta):
    """Find strike for a specific put delta (e.g., -0.05 for 5-delta put)."""
    # 5-delta put is far OTM, strike well below spot
    low, high = spot * 0.5, spot * 1.0
    for _ in range(50):
        mid = (low + high) / 2
        d1 = (math.log(spot / mid) + (rate + 0.5 * iv * iv) * t) / (iv * math.sqrt(t))
        call_delta = 0.5 * (1.0 + math.erf(d1 / math.sqrt(2.0)))
        put_delta = call_delta - 1.0
        if put_delta < target_delta:
            high = mid
        else:
            low = mid
    return mid


def run_simulation(client, spy_by_date, trading_dates, vix_data, sma200,
                   monthly_exps, contract_eod, strikes_cache,
                   hedge_mode="none", label=""):
    """
    Run simulation with optional tail hedge.

    hedge_mode:
    - "none": No hedge (baseline)
    - "always": Always maintain rolling hedge puts
    - "tactical": Only hedge when within 5% of SMA200
    """
    options_cash = float(OPTIONS_CASH)
    pending_cash = 0.0
    call_positions = []
    hedge_position = None  # Single hedge put position

    call_trade_log = []
    hedge_trade_log = []
    hedge_cost_total = 0.0

    start_idx = next((i for i, d in enumerate(trading_dates) if d >= SIM_START), 0)
    daily_values = []

    # Calculate initial portfolio value for budget
    initial_spot = spy_by_date[trading_dates[start_idx]]["close"]
    initial_portfolio = SHARES * initial_spot + OPTIONS_CASH
    daily_hedge_budget = (initial_portfolio * HEDGE_ANNUAL_BUDGET_PCT) / 252

    for day_idx in range(start_idx, len(trading_dates)):
        today = trading_dates[day_idx]
        bar = spy_by_date[today]
        spot = bar["close"]
        sma_val = sma200.get(today)
        above_sma = (spot > sma_val) if sma_val else True
        vix = vix_data.get(today, 20.0)
        iv_est = max(0.08, min(0.90, vix / 100.0))

        shares_value = SHARES * spot
        options_cash += pending_cash
        pending_cash = 0.0

        pct_from_sma = (spot - sma_val) / sma_val if sma_val else 0

        # ===== CALL EXITS =====
        if pct_from_sma < -SMA_EXIT_THRESHOLD and call_positions:
            for pos in call_positions:
                ckey = (pos["expiration"], pos["strike"], "C")
                eod = contract_eod.get(ckey, {}).get(today)
                bid, _ = get_bid_ask(eod)
                if bid is None or bid <= 0:
                    intrinsic = max(0, spot - pos["strike"])
                    bid = intrinsic * 0.998 if intrinsic > 0 else 0.001
                proceeds = bid * 100 * pos["quantity"]
                pending_cash += proceeds
                pnl_pct = bid / pos["entry_price"] - 1
                call_trade_log.append({
                    "pnl_pct": pnl_pct,
                    "pnl_dollar": proceeds - pos["contract_cost"],
                    "exit_reason": "SMA",
                })
            call_positions = []

        still_open = []
        for pos in call_positions:
            pos["days_held"] += 1
            ckey = (pos["expiration"], pos["strike"], "C")
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
                call_trade_log.append({
                    "pnl_pct": pnl_pct,
                    "pnl_dollar": proceeds - pos["contract_cost"],
                    "exit_reason": exit_reason,
                })
            else:
                still_open.append(pos)
        call_positions = still_open

        # ===== HEDGE PUT MANAGEMENT =====
        if hedge_mode != "none":
            # Determine if we should have a hedge today
            should_hedge = False
            if hedge_mode == "always":
                should_hedge = True
            elif hedge_mode == "tactical":
                # Only hedge when within 5% of SMA (either direction)
                should_hedge = abs(pct_from_sma) < TACTICAL_SMA_THRESHOLD

            # Check if we need to roll or close existing hedge
            if hedge_position:
                exp_date = datetime.strptime(hedge_position["expiration"], "%Y-%m-%d").date()
                today_date = datetime.strptime(today, "%Y-%m-%d").date()
                dte_remaining = (exp_date - today_date).days

                should_close = False
                close_reason = None

                if dte_remaining <= HEDGE_ROLL_DTE:
                    should_close = True
                    close_reason = "ROLL"
                elif not should_hedge:
                    should_close = True
                    close_reason = "TACTICAL_EXIT"

                if should_close:
                    ckey = (hedge_position["expiration"], hedge_position["strike"], "P")
                    eod = contract_eod.get(ckey, {}).get(today)
                    bid, _ = get_bid_ask(eod)
                    if bid is None or bid <= 0:
                        intrinsic = max(0, hedge_position["strike"] - spot)
                        bid = intrinsic * 0.998 if intrinsic > 0 else 0.001
                    proceeds = bid * 100 * hedge_position["quantity"]
                    pending_cash += proceeds
                    pnl = proceeds - hedge_position["contract_cost"]
                    hedge_trade_log.append({
                        "entry_date": hedge_position["entry_date"],
                        "exit_date": today,
                        "entry_price": hedge_position["entry_price"],
                        "exit_price": bid,
                        "pnl_dollar": pnl,
                        "exit_reason": close_reason,
                    })
                    hedge_position = None

            # Open new hedge if needed
            if should_hedge and hedge_position is None:
                best_exp, dte_cal = find_best_expiration(
                    today, monthly_exps,
                    dte_target=HEDGE_DTE_TARGET,
                    dte_min=HEDGE_DTE_MIN,
                    dte_max=HEDGE_DTE_MAX
                )
                if best_exp:
                    t_years = dte_cal / 365.0
                    # Find 5-delta put strike
                    target_strike = find_put_strike_for_delta(spot, t_years, RATE, iv_est, -HEDGE_PUT_DELTA)

                    if best_exp not in strikes_cache:
                        strikes_cache[best_exp] = client.get_strikes("SPY", best_exp)
                    strikes = strikes_cache.get(best_exp, [])
                    if strikes:
                        real_strike = min(strikes, key=lambda s: abs(s - target_strike))
                        ckey = (best_exp, real_strike, "P")
                        if ckey not in contract_eod:
                            data = client.prefetch_option_life("SPY", best_exp, real_strike, "P", today)
                            contract_eod[ckey] = {r["bar_date"]: r for r in data}
                        eod = contract_eod[ckey].get(today)
                        _, ask = get_bid_ask(eod)

                        if ask and ask > 0:
                            # Buy 1 contract (or scale based on budget)
                            contract_cost = ask * 100
                            # Check if within budget
                            if contract_cost < options_cash * 0.1:  # Max 10% of options cash per hedge
                                options_cash -= contract_cost
                                hedge_cost_total += contract_cost
                                hedge_position = {
                                    "entry_date": today,
                                    "expiration": best_exp,
                                    "strike": real_strike,
                                    "entry_price": ask,
                                    "quantity": 1,
                                    "contract_cost": contract_cost,
                                }

        # ===== CALL ENTRY =====
        current_call_delta = sum(
            calculate_delta(spot, p["strike"],
                          (datetime.strptime(p["expiration"], "%Y-%m-%d").date() -
                           datetime.strptime(today, "%Y-%m-%d").date()).days, iv_est, "C")
            * p["quantity"] * 100
            for p in call_positions
        )
        call_delta_room = SHARES - current_call_delta

        if above_sma and sma_val and call_delta_room > 80:
            best_exp, dte_cal = find_best_expiration(today, monthly_exps)
            if best_exp:
                t_years = dte_cal / 365.0
                bs_strike = find_strike_for_delta(spot, t_years, RATE, iv_est, CALL_DELTA, "C")
                if bs_strike:
                    if best_exp not in strikes_cache:
                        strikes_cache[best_exp] = client.get_strikes("SPY", best_exp)
                    strikes = strikes_cache.get(best_exp, [])
                    if strikes:
                        real_strike = min(strikes, key=lambda s: abs(s - bs_strike))
                        ckey = (best_exp, real_strike, "C")
                        if ckey not in contract_eod:
                            data = client.prefetch_option_life("SPY", best_exp, real_strike, "C", today)
                            contract_eod[ckey] = {r["bar_date"]: r for r in data}
                        eod = contract_eod[ckey].get(today)
                        _, ask = get_bid_ask(eod)
                        if ask and ask > 0:
                            option_delta = calculate_delta(spot, real_strike, dte_cal, iv_est, "C")
                            max_by_delta = int(call_delta_room / (option_delta * 100))
                            contract_cost = ask * 100
                            max_by_cash = int(options_cash / contract_cost)
                            qty = min(max_by_delta, max_by_cash, 1)
                            if qty > 0:
                                total_cost = contract_cost * qty
                                options_cash -= total_cost
                                call_positions.append({
                                    "expiration": best_exp,
                                    "strike": real_strike,
                                    "entry_price": ask,
                                    "quantity": qty,
                                    "contract_cost": total_cost,
                                    "days_held": 0,
                                })

        # Mark to market
        call_value = 0.0
        for pos in call_positions:
            ckey = (pos["expiration"], pos["strike"], "C")
            eod = contract_eod.get(ckey, {}).get(today)
            bid, ask = get_bid_ask(eod)
            mid = (bid + ask) / 2.0 if bid and ask else max(0, spot - pos["strike"])
            call_value += mid * 100 * pos["quantity"]

        hedge_value = 0.0
        if hedge_position:
            ckey = (hedge_position["expiration"], hedge_position["strike"], "P")
            eod = contract_eod.get(ckey, {}).get(today)
            bid, ask = get_bid_ask(eod)
            mid = (bid + ask) / 2.0 if bid and ask else max(0, hedge_position["strike"] - spot)
            hedge_value = mid * 100 * hedge_position["quantity"]

        portfolio_value = shares_value + options_cash + pending_cash + call_value + hedge_value
        daily_values.append(portfolio_value)

    # Compute metrics
    df = pd.DataFrame({"value": daily_values})
    df["ret"] = df["value"].pct_change().fillna(0)

    years = len(df) / 252.0
    cagr = (df["value"].iloc[-1] / df["value"].iloc[0]) ** (1/years) - 1
    sharpe = (df["ret"].mean() / df["ret"].std()) * np.sqrt(252) if df["ret"].std() > 0 else 0

    downside = df["ret"][df["ret"] < 0]
    ds_std = downside.std() if len(downside) > 0 else 0
    sortino = (df["ret"].mean() / ds_std) * np.sqrt(252) if ds_std > 0 else 0

    max_dd = (df["value"] / df["value"].cummax() - 1).min()

    # Trade stats
    call_df = pd.DataFrame(call_trade_log) if call_trade_log else pd.DataFrame()
    hedge_df = pd.DataFrame(hedge_trade_log) if hedge_trade_log else pd.DataFrame()

    call_pnl = call_df["pnl_dollar"].sum() if len(call_df) > 0 else 0
    hedge_pnl = hedge_df["pnl_dollar"].sum() if len(hedge_df) > 0 else 0

    return {
        "label": label,
        "cagr": cagr,
        "sharpe": sharpe,
        "sortino": sortino,
        "max_dd": max_dd,
        "end_value": df["value"].iloc[-1],
        "call_trades": len(call_df),
        "call_pnl": call_pnl,
        "hedge_trades": len(hedge_df),
        "hedge_pnl": hedge_pnl,
        "hedge_cost_total": hedge_cost_total,
    }


def main():
    logging.basicConfig(level=logging.WARNING)

    print("=" * 80)
    print("TAIL RISK HEDGE TEST")
    print("=" * 80)
    print("\nTesting rolling OTM puts as portfolio insurance")
    print(f"Hedge puts: {HEDGE_PUT_DELTA*100:.0f}-delta (~{(1-HEDGE_PUT_DELTA)*100:.0f}% OTM)")
    print(f"Hedge DTE: ~{HEDGE_DTE_TARGET} days, roll at {HEDGE_ROLL_DTE} DTE")

    client = ThetaDataClient()
    if not client.connect():
        print("ERROR: Cannot connect to Theta Terminal.")
        return

    print("\nLoading data...")
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

    print(f"  Trading days: {len(trading_dates)}")

    contract_eod = {}
    strikes_cache = {}

    results = []

    # Test A: No hedge (baseline)
    print("\nTesting A: No hedge (baseline)...")
    result_a = run_simulation(
        client, spy_by_date, trading_dates, vix_data, sma200,
        monthly_exps, contract_eod, strikes_cache,
        hedge_mode="none", label="A: No Hedge"
    )
    results.append(result_a)
    print(f"  CAGR: {result_a['cagr']:+.1%}, Sharpe: {result_a['sharpe']:.2f}, Max DD: {result_a['max_dd']:.1%}")

    # Test B: Always hedge
    print("\nTesting B: Always hedge (rolling 5-delta puts)...")
    result_b = run_simulation(
        client, spy_by_date, trading_dates, vix_data, sma200,
        monthly_exps, contract_eod, strikes_cache,
        hedge_mode="always", label="B: Always Hedge"
    )
    results.append(result_b)
    print(f"  CAGR: {result_b['cagr']:+.1%}, Sharpe: {result_b['sharpe']:.2f}, Max DD: {result_b['max_dd']:.1%}")

    # Test C: Tactical hedge
    print("\nTesting C: Tactical hedge (only near SMA200)...")
    result_c = run_simulation(
        client, spy_by_date, trading_dates, vix_data, sma200,
        monthly_exps, contract_eod, strikes_cache,
        hedge_mode="tactical", label="C: Tactical Hedge"
    )
    results.append(result_c)
    print(f"  CAGR: {result_c['cagr']:+.1%}, Sharpe: {result_c['sharpe']:.2f}, Max DD: {result_c['max_dd']:.1%}")

    # Print comparison
    print("\n" + "=" * 95)
    print("TAIL HEDGE COMPARISON")
    print("=" * 95)

    print(f"\n  {'Metric':<20} {'No Hedge':>18} {'Always Hedge':>18} {'Tactical':>18}")
    print(f"  {'-' * 75}")

    metrics = [
        ("CAGR", [f"{r['cagr']:+.2%}" for r in results]),
        ("Sharpe", [f"{r['sharpe']:.3f}" for r in results]),
        ("Sortino", [f"{r['sortino']:.3f}" for r in results]),
        ("Max Drawdown", [f"{r['max_dd']:.1%}" for r in results]),
        ("End Value", [f"${r['end_value']:,.0f}" for r in results]),
        ("Call P&L", [f"${r['call_pnl']:+,.0f}" for r in results]),
        ("Hedge Trades", [f"{r['hedge_trades']}" for r in results]),
        ("Hedge P&L", [f"${r['hedge_pnl']:+,.0f}" for r in results]),
        ("Hedge Cost", [f"${r['hedge_cost_total']:,.0f}" for r in results]),
    ]

    for name, vals in metrics:
        print(f"  {name:<20} {vals[0]:>18} {vals[1]:>18} {vals[2]:>18}")

    # Analysis
    print("\n" + "=" * 95)
    print("ANALYSIS")
    print("=" * 95)

    # Compare max DD improvement vs CAGR cost
    dd_improvement_b = result_a['max_dd'] - result_b['max_dd']  # Positive = better
    cagr_cost_b = result_a['cagr'] - result_b['cagr']  # Positive = cost

    dd_improvement_c = result_a['max_dd'] - result_c['max_dd']
    cagr_cost_c = result_a['cagr'] - result_c['cagr']

    print(f"\n  Always Hedge (B):")
    print(f"    Max DD improvement: {dd_improvement_b:+.1%} (from {result_a['max_dd']:.1%} to {result_b['max_dd']:.1%})")
    print(f"    CAGR cost: {cagr_cost_b:+.2%}")
    print(f"    Sharpe change: {result_b['sharpe'] - result_a['sharpe']:+.3f}")

    print(f"\n  Tactical Hedge (C):")
    print(f"    Max DD improvement: {dd_improvement_c:+.1%} (from {result_a['max_dd']:.1%} to {result_c['max_dd']:.1%})")
    print(f"    CAGR cost: {cagr_cost_c:+.2%}")
    print(f"    Sharpe change: {result_c['sharpe'] - result_a['sharpe']:+.3f}")

    # Conclusion
    print("\n" + "-" * 95)
    print("CONCLUSION")
    print("-" * 95)

    best_sharpe = max(results, key=lambda x: x['sharpe'])
    best_dd = min(results, key=lambda x: x['max_dd'])

    print(f"\n  Best Sharpe: {best_sharpe['label']} ({best_sharpe['sharpe']:.3f})")
    print(f"  Best Max DD: {best_dd['label']} ({best_dd['max_dd']:.1%})")

    if best_sharpe['label'] == "A: No Hedge":
        print("\n  FINDING: Tail hedging is NOT worth the cost.")
        print("  The insurance premium exceeds the benefit from reduced drawdowns.")
        print("  The SMA200 exit rule already provides meaningful downside protection.")
    else:
        print(f"\n  FINDING: {best_sharpe['label']} provides the best risk-adjusted returns.")
        if "Tactical" in best_sharpe['label']:
            print("  Hedging only when near SMA200 reduces cost while maintaining protection.")

    print("\n" + "=" * 95)

    client.close()


if __name__ == "__main__":
    main()
