"""
Puts Below SMA200 Test
======================
Tests buying put options when SPY is below its 200-day SMA,
as a bearish complement to the bullish call strategy.

Hypothesis: If being above SMA200 is favorable for long calls,
being below SMA200 might be favorable for long puts.

Counter-hypothesis: Markets can stay below SMA200 while grinding
sideways or slowly recovering, making puts unprofitable.

Usage:
    python puts_below_sma_test.py
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
CALL_DELTA = 0.80
PUT_DELTA = 0.80  # 80-delta put = 20-delta from ATM on put side
DTE_TARGET = 120
DTE_MIN = 90
DTE_MAX = 150
MH = 60
PT = 0.50
RATE = 0.04
SMA_EXIT_THRESHOLD = 0.02
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
        return call_delta - 1.0  # Put delta is negative
    return call_delta


def find_put_strike_for_delta(spot, t, rate, iv, target_delta):
    """Find strike that gives target put delta (negative value like -0.80)."""
    # Put delta ranges from -1 (deep ITM) to 0 (far OTM)
    # For 80-delta put, we want delta ~ -0.80, meaning strike above spot
    low, high = spot * 0.8, spot * 1.5
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
                   buy_puts_below_sma=False, label=""):
    """
    Run simulation with optional put buying below SMA.

    When buy_puts_below_sma=True:
    - Buy calls when above SMA (normal)
    - Buy puts when below SMA (new)
    """
    options_cash = float(OPTIONS_CASH)
    pending_cash = 0.0
    call_positions = []
    put_positions = []

    call_trade_log = []
    put_trade_log = []

    start_idx = next((i for i, d in enumerate(trading_dates) if d >= SIM_START), 0)
    daily_values = []

    for day_idx in range(start_idx, len(trading_dates)):
        today = trading_dates[day_idx]
        bar = spy_by_date[today]
        spot = bar["close"]
        sma_val = sma200.get(today)
        above_sma = (spot > sma_val) if sma_val else True
        below_sma = (spot < sma_val) if sma_val else False
        vix = vix_data.get(today, 20.0)
        iv_est = max(0.08, min(0.90, vix / 100.0))

        shares_value = SHARES * spot
        options_cash += pending_cash
        pending_cash = 0.0

        # Calculate how far above/below SMA
        pct_from_sma = (spot - sma_val) / sma_val if sma_val else 0

        # ===== CALL EXITS =====
        # Force-exit calls when >2% below SMA
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
                    "type": "CALL",
                    "pnl_pct": pnl_pct,
                    "pnl_dollar": proceeds - pos["contract_cost"],
                    "exit_reason": "SMA",
                    "days_held": pos["days_held"],
                })
            call_positions = []

        # Normal call exits (PT/MH)
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
                    "type": "CALL",
                    "pnl_pct": pnl_pct,
                    "pnl_dollar": proceeds - pos["contract_cost"],
                    "exit_reason": exit_reason,
                    "days_held": pos["days_held"],
                })
            else:
                still_open.append(pos)
        call_positions = still_open

        # ===== PUT EXITS =====
        # Force-exit puts when >2% above SMA (market recovered)
        if buy_puts_below_sma and pct_from_sma > SMA_EXIT_THRESHOLD and put_positions:
            for pos in put_positions:
                ckey = (pos["expiration"], pos["strike"], "P")
                eod = contract_eod.get(ckey, {}).get(today)
                bid, _ = get_bid_ask(eod)
                if bid is None or bid <= 0:
                    intrinsic = max(0, pos["strike"] - spot)
                    bid = intrinsic * 0.998 if intrinsic > 0 else 0.001
                proceeds = bid * 100 * pos["quantity"]
                pending_cash += proceeds
                pnl_pct = bid / pos["entry_price"] - 1
                put_trade_log.append({
                    "type": "PUT",
                    "pnl_pct": pnl_pct,
                    "pnl_dollar": proceeds - pos["contract_cost"],
                    "exit_reason": "SMA",
                    "days_held": pos["days_held"],
                })
            put_positions = []

        # Normal put exits (PT/MH)
        if buy_puts_below_sma:
            still_open = []
            for pos in put_positions:
                pos["days_held"] += 1
                ckey = (pos["expiration"], pos["strike"], "P")
                eod = contract_eod.get(ckey, {}).get(today)
                bid, _ = get_bid_ask(eod)
                if bid is None or bid <= 0:
                    intrinsic = max(0, pos["strike"] - spot)
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
                    put_trade_log.append({
                        "type": "PUT",
                        "pnl_pct": pnl_pct,
                        "pnl_dollar": proceeds - pos["contract_cost"],
                        "exit_reason": exit_reason,
                        "days_held": pos["days_held"],
                    })
                else:
                    still_open.append(pos)
            put_positions = still_open

        # ===== CALL ENTRY (above SMA) =====
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

        # ===== PUT ENTRY (below SMA) =====
        if buy_puts_below_sma and below_sma and sma_val:
            current_put_delta = sum(
                abs(calculate_delta(spot, p["strike"],
                              (datetime.strptime(p["expiration"], "%Y-%m-%d").date() -
                               datetime.strptime(today, "%Y-%m-%d").date()).days, iv_est, "P"))
                * p["quantity"] * 100
                for p in put_positions
            )
            put_delta_room = SHARES - current_put_delta  # Use same delta cap for puts

            if put_delta_room > 80:
                best_exp, dte_cal = find_best_expiration(today, monthly_exps)
                if best_exp:
                    t_years = dte_cal / 365.0
                    # Find 80-delta put (ITM put, strike above spot)
                    bs_strike = find_put_strike_for_delta(spot, t_years, RATE, iv_est, -PUT_DELTA)
                    if bs_strike:
                        if best_exp not in strikes_cache:
                            strikes_cache[best_exp] = client.get_strikes("SPY", best_exp)
                        strikes = strikes_cache.get(best_exp, [])
                        if strikes:
                            real_strike = min(strikes, key=lambda s: abs(s - bs_strike))
                            ckey = (best_exp, real_strike, "P")
                            if ckey not in contract_eod:
                                data = client.prefetch_option_life("SPY", best_exp, real_strike, "P", today)
                                contract_eod[ckey] = {r["bar_date"]: r for r in data}
                            eod = contract_eod[ckey].get(today)
                            _, ask = get_bid_ask(eod)
                            if ask and ask > 0:
                                option_delta = abs(calculate_delta(spot, real_strike, dte_cal, iv_est, "P"))
                                max_by_delta = int(put_delta_room / (option_delta * 100))
                                contract_cost = ask * 100
                                max_by_cash = int(options_cash / contract_cost)
                                qty = min(max_by_delta, max_by_cash, 1)
                                if qty > 0:
                                    total_cost = contract_cost * qty
                                    options_cash -= total_cost
                                    put_positions.append({
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

        put_value = 0.0
        for pos in put_positions:
            ckey = (pos["expiration"], pos["strike"], "P")
            eod = contract_eod.get(ckey, {}).get(today)
            bid, ask = get_bid_ask(eod)
            mid = (bid + ask) / 2.0 if bid and ask else max(0, pos["strike"] - spot)
            put_value += mid * 100 * pos["quantity"]

        portfolio_value = shares_value + options_cash + pending_cash + call_value + put_value
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
    put_df = pd.DataFrame(put_trade_log) if put_trade_log else pd.DataFrame()

    call_stats = {}
    if len(call_df) > 0:
        call_stats = {
            "trades": len(call_df),
            "win_rate": len(call_df[call_df["pnl_pct"] > 0]) / len(call_df),
            "total_pnl": call_df["pnl_dollar"].sum(),
            "mean_ret": call_df["pnl_pct"].mean(),
        }

    put_stats = {}
    if len(put_df) > 0:
        put_stats = {
            "trades": len(put_df),
            "win_rate": len(put_df[put_df["pnl_pct"] > 0]) / len(put_df),
            "total_pnl": put_df["pnl_dollar"].sum(),
            "mean_ret": put_df["pnl_pct"].mean(),
        }

    return {
        "label": label,
        "cagr": cagr,
        "sharpe": sharpe,
        "sortino": sortino,
        "max_dd": max_dd,
        "end_value": df["value"].iloc[-1],
        "call_stats": call_stats,
        "put_stats": put_stats,
    }


def main():
    logging.basicConfig(level=logging.WARNING)

    print("=" * 80)
    print("PUTS BELOW SMA200 TEST")
    print("=" * 80)
    print("\nHypothesis: Buy puts when below SMA200 to profit from declines")

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

    # Shared caches
    contract_eod = {}
    strikes_cache = {}

    # Test A: Calls only (baseline)
    print("\nTesting A: Calls only (baseline)...")
    result_a = run_simulation(
        client, spy_by_date, trading_dates, vix_data, sma200,
        monthly_exps, contract_eod, strikes_cache,
        buy_puts_below_sma=False, label="A: Calls Only"
    )
    print(f"  CAGR: {result_a['cagr']:+.1%}, Sharpe: {result_a['sharpe']:.2f}")

    # Test B: Calls + Puts
    print("\nTesting B: Calls above SMA + Puts below SMA...")
    result_b = run_simulation(
        client, spy_by_date, trading_dates, vix_data, sma200,
        monthly_exps, contract_eod, strikes_cache,
        buy_puts_below_sma=True, label="B: Calls + Puts"
    )
    print(f"  CAGR: {result_b['cagr']:+.1%}, Sharpe: {result_b['sharpe']:.2f}")

    # Print comparison
    print("\n" + "=" * 90)
    print("COMPARISON: CALLS ONLY vs CALLS + PUTS")
    print("=" * 90)

    print(f"\n  {'Metric':<25} {'Calls Only':>18} {'Calls + Puts':>18} {'Difference':>18}")
    print(f"  {'-' * 75}")

    metrics = [
        ("CAGR", f"{result_a['cagr']:+.2%}", f"{result_b['cagr']:+.2%}",
         f"{result_b['cagr'] - result_a['cagr']:+.2%}"),
        ("Sharpe", f"{result_a['sharpe']:.3f}", f"{result_b['sharpe']:.3f}",
         f"{result_b['sharpe'] - result_a['sharpe']:+.3f}"),
        ("Sortino", f"{result_a['sortino']:.3f}", f"{result_b['sortino']:.3f}",
         f"{result_b['sortino'] - result_a['sortino']:+.3f}"),
        ("Max Drawdown", f"{result_a['max_dd']:.1%}", f"{result_b['max_dd']:.1%}",
         f"{result_b['max_dd'] - result_a['max_dd']:+.1%}"),
        ("End Value", f"${result_a['end_value']:,.0f}", f"${result_b['end_value']:,.0f}",
         f"${result_b['end_value'] - result_a['end_value']:+,.0f}"),
    ]

    for name, a_val, b_val, diff in metrics:
        print(f"  {name:<25} {a_val:>18} {b_val:>18} {diff:>18}")

    # Call stats
    print(f"\n  CALL TRADES:")
    ca = result_a["call_stats"]
    cb = result_b["call_stats"]
    if ca:
        print(f"    Calls Only:  {ca['trades']} trades, {ca['win_rate']:.1%} win rate, "
              f"${ca['total_pnl']:+,.0f} P&L, {ca['mean_ret']:+.1%} avg return")
    if cb:
        print(f"    Calls+Puts:  {cb['trades']} trades, {cb['win_rate']:.1%} win rate, "
              f"${cb['total_pnl']:+,.0f} P&L, {cb['mean_ret']:+.1%} avg return")

    # Put stats
    print(f"\n  PUT TRADES:")
    pb = result_b.get("put_stats", {})
    if pb:
        print(f"    Puts:        {pb['trades']} trades, {pb['win_rate']:.1%} win rate, "
              f"${pb['total_pnl']:+,.0f} P&L, {pb['mean_ret']:+.1%} avg return")
    else:
        print(f"    No put trades executed")

    # Conclusion
    print("\n" + "=" * 90)
    print("CONCLUSION")
    print("=" * 90)

    sharpe_diff = result_b['sharpe'] - result_a['sharpe']
    cagr_diff = result_b['cagr'] - result_a['cagr']

    if sharpe_diff > 0.02 and cagr_diff > 0:
        print("\n  FINDING: Adding puts below SMA200 IMPROVES the strategy.")
        print(f"  Sharpe improvement: {sharpe_diff:+.3f}")
        print(f"  CAGR improvement: {cagr_diff:+.2%}")
    elif sharpe_diff < -0.02 or cagr_diff < -0.01:
        print("\n  FINDING: Adding puts below SMA200 HURTS the strategy.")
        print(f"  Sharpe change: {sharpe_diff:+.3f}")
        print(f"  CAGR change: {cagr_diff:+.2%}")
        if pb:
            print(f"\n  Put trades lost ${-pb['total_pnl']:,.0f} with {pb['win_rate']:.1%} win rate.")
            print("  Markets below SMA200 often grind sideways or recover, making puts unprofitable.")
    else:
        print("\n  FINDING: No significant difference.")

    print("\n" + "=" * 90)

    client.close()


if __name__ == "__main__":
    main()
