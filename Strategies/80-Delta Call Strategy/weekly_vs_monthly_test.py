"""
Weekly vs Monthly Expirations Test
==================================
Tests whether using weekly expirations instead of monthly expirations
improves or degrades strategy performance.

Hypothesis: Monthly expirations are better due to:
- Higher liquidity (tighter bid-ask spreads)
- Lower transaction costs
- Sufficient frequency for the strategy

Counter-hypothesis: Weekly expirations might offer:
- More precise DTE targeting
- Different theta decay characteristics
- More frequent entry opportunities

Usage:
    python weekly_vs_monthly_test.py
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
SMA_EXIT_THRESHOLD = 0.02
OPTIONS_CASH = 100_000

DATA_START = "2014-01-01"
DATA_END = "2026-01-31"
SIM_START = "2015-03-01"


def is_monthly_opex(exp_str):
    """Check if expiration is a monthly (3rd Friday)."""
    exp_dt = datetime.strptime(exp_str, "%Y-%m-%d").date()
    if exp_dt.weekday() != 4:  # Must be Friday
        return False
    return 15 <= exp_dt.day <= 21  # 3rd Friday is between 15th and 21st


def is_weekly_opex(exp_str):
    """Check if expiration is any Friday (weekly)."""
    exp_dt = datetime.strptime(exp_str, "%Y-%m-%d").date()
    return exp_dt.weekday() == 4  # Any Friday


def find_best_expiration(entry_date_str, expirations, dte_target=DTE_TARGET,
                         dte_min=DTE_MIN, dte_max=DTE_MAX):
    """Find best expiration within DTE range."""
    entry_dt = datetime.strptime(entry_date_str, "%Y-%m-%d").date()
    best_exp, best_dte, best_diff = None, 0, 9999
    for exp_str, exp_dt in expirations:
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


def calculate_delta(spot, strike, dte, iv=0.16):
    if dte <= 0:
        return 1.0 if spot > strike else 0.0
    t = dte / 365.0
    d1 = (math.log(spot / strike) + (RATE + 0.5 * iv * iv) * t) / (iv * math.sqrt(t))
    return 0.5 * (1.0 + math.erf(d1 / math.sqrt(2.0)))


def run_simulation(client, spy_by_date, trading_dates, vix_data, sma200,
                   expirations, contract_eod, strikes_cache, label=""):
    """Run simulation with specific set of expirations."""
    options_cash = float(OPTIONS_CASH)
    pending_cash = 0.0
    positions = []
    trade_log = []
    force_exit_count = 0
    no_expiration_count = 0
    no_quote_count = 0

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
                pnl_pct = bid / pos["entry_price"] - 1
                trade_log.append({
                    "pnl_pct": pnl_pct,
                    "pnl_dollar": proceeds - pos["contract_cost"],
                    "exit_reason": "SMA",
                    "days_held": pos["days_held"],
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
                    "days_held": pos["days_held"],
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
            best_exp, dte_cal = find_best_expiration(today, expirations)
            if not best_exp:
                no_expiration_count += 1
            elif best_exp:
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
                        else:
                            no_quote_count += 1

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

    # Sortino
    downside = df["ret"][df["ret"] < 0]
    ds_std = downside.std() if len(downside) > 0 else 0
    sortino = (df["ret"].mean() / ds_std) * np.sqrt(252) if ds_std > 0 else 0

    max_dd = (df["value"] / df["value"].cummax() - 1).min()

    tdf = pd.DataFrame(trade_log)
    n_trades = len(tdf)
    win_rate = len(tdf[tdf["pnl_pct"] > 0]) / n_trades if n_trades > 0 else 0
    total_pnl = tdf["pnl_dollar"].sum() if n_trades > 0 else 0
    sma_exits = len(tdf[tdf["exit_reason"] == "SMA"]) if n_trades > 0 else 0
    pt_exits = len(tdf[tdf["exit_reason"] == "PT"]) if n_trades > 0 else 0
    avg_days = tdf["days_held"].mean() if n_trades > 0 else 0

    return {
        "label": label,
        "cagr": cagr,
        "sharpe": sharpe,
        "sortino": sortino,
        "max_dd": max_dd,
        "trades": n_trades,
        "win_rate": win_rate,
        "total_pnl": total_pnl,
        "sma_exits": sma_exits,
        "pt_exits": pt_exits,
        "avg_days": avg_days,
        "no_expiration": no_expiration_count,
        "no_quote": no_quote_count,
    }


def main():
    logging.basicConfig(level=logging.WARNING)

    print("=" * 80)
    print("WEEKLY vs MONTHLY EXPIRATIONS TEST")
    print("=" * 80)

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

    # Get all expirations
    all_exps = client.get_expirations("SPY")

    # Monthly expirations (3rd Friday only)
    monthly_exps = [(e, datetime.strptime(e, "%Y-%m-%d").date())
                    for e in all_exps if is_monthly_opex(e)]
    monthly_exps.sort(key=lambda x: x[1])

    # Weekly expirations (any Friday)
    weekly_exps = [(e, datetime.strptime(e, "%Y-%m-%d").date())
                   for e in all_exps if is_weekly_opex(e)]
    weekly_exps.sort(key=lambda x: x[1])

    print(f"  Trading days: {len(trading_dates)}")
    print(f"  Monthly expirations: {len(monthly_exps)}")
    print(f"  Weekly expirations: {len(weekly_exps)}")

    # Shared caches
    contract_eod = {}
    strikes_cache = {}

    results = []

    # Test Monthly
    print(f"\nTesting Monthly expirations...")
    result_monthly = run_simulation(
        client, spy_by_date, trading_dates, vix_data, sma200,
        monthly_exps, contract_eod, strikes_cache, "Monthly (3rd Friday)"
    )
    results.append(result_monthly)
    print(f"  CAGR: {result_monthly['cagr']:+.1%}, Sharpe: {result_monthly['sharpe']:.2f}, "
          f"Trades: {result_monthly['trades']}")

    # Test Weekly
    print(f"\nTesting Weekly expirations...")
    result_weekly = run_simulation(
        client, spy_by_date, trading_dates, vix_data, sma200,
        weekly_exps, contract_eod, strikes_cache, "Weekly (all Fridays)"
    )
    results.append(result_weekly)
    print(f"  CAGR: {result_weekly['cagr']:+.1%}, Sharpe: {result_weekly['sharpe']:.2f}, "
          f"Trades: {result_weekly['trades']}")

    # Print comparison
    print("\n" + "=" * 90)
    print("WEEKLY vs MONTHLY COMPARISON")
    print("=" * 90)

    print(f"\n  {'Metric':<25} {'Monthly':>18} {'Weekly':>18} {'Difference':>18}")
    print(f"  {'-' * 75}")

    metrics = [
        ("CAGR", f"{result_monthly['cagr']:+.2%}", f"{result_weekly['cagr']:+.2%}",
         f"{result_weekly['cagr'] - result_monthly['cagr']:+.2%}"),
        ("Sharpe Ratio", f"{result_monthly['sharpe']:.3f}", f"{result_weekly['sharpe']:.3f}",
         f"{result_weekly['sharpe'] - result_monthly['sharpe']:+.3f}"),
        ("Sortino Ratio", f"{result_monthly['sortino']:.3f}", f"{result_weekly['sortino']:.3f}",
         f"{result_weekly['sortino'] - result_monthly['sortino']:+.3f}"),
        ("Max Drawdown", f"{result_monthly['max_dd']:.1%}", f"{result_weekly['max_dd']:.1%}",
         f"{result_weekly['max_dd'] - result_monthly['max_dd']:+.1%}"),
        ("Total Trades", f"{result_monthly['trades']}", f"{result_weekly['trades']}",
         f"{result_weekly['trades'] - result_monthly['trades']:+d}"),
        ("Win Rate", f"{result_monthly['win_rate']:.1%}", f"{result_weekly['win_rate']:.1%}",
         f"{result_weekly['win_rate'] - result_monthly['win_rate']:+.1%}"),
        ("Total P&L", f"${result_monthly['total_pnl']:,.0f}", f"${result_weekly['total_pnl']:,.0f}",
         f"${result_weekly['total_pnl'] - result_monthly['total_pnl']:+,.0f}"),
        ("PT Exits", f"{result_monthly['pt_exits']}", f"{result_weekly['pt_exits']}",
         f"{result_weekly['pt_exits'] - result_monthly['pt_exits']:+d}"),
        ("SMA Exits", f"{result_monthly['sma_exits']}", f"{result_weekly['sma_exits']}",
         f"{result_weekly['sma_exits'] - result_monthly['sma_exits']:+d}"),
        ("Avg Days Held", f"{result_monthly['avg_days']:.1f}", f"{result_weekly['avg_days']:.1f}",
         f"{result_weekly['avg_days'] - result_monthly['avg_days']:+.1f}"),
        ("No Expiration Skips", f"{result_monthly['no_expiration']}", f"{result_weekly['no_expiration']}",
         f"{result_weekly['no_expiration'] - result_monthly['no_expiration']:+d}"),
        ("No Quote Skips", f"{result_monthly['no_quote']}", f"{result_weekly['no_quote']}",
         f"{result_weekly['no_quote'] - result_monthly['no_quote']:+d}"),
    ]

    for name, m_val, w_val, diff in metrics:
        print(f"  {name:<25} {m_val:>18} {w_val:>18} {diff:>18}")

    # Conclusion
    print("\n" + "=" * 90)
    print("ANALYSIS")
    print("=" * 90)

    sharpe_diff = result_weekly['sharpe'] - result_monthly['sharpe']
    cagr_diff = result_weekly['cagr'] - result_monthly['cagr']

    if abs(sharpe_diff) < 0.02 and abs(cagr_diff) < 0.005:
        print("\n  CONCLUSION: No meaningful difference between weekly and monthly expirations.")
        print("  Recommend sticking with MONTHLY for liquidity and simplicity.")
    elif sharpe_diff > 0.02:
        print(f"\n  CONCLUSION: Weekly expirations show better risk-adjusted returns.")
        print(f"  Sharpe improvement: {sharpe_diff:+.3f}")
        print("  Consider switching to weekly if liquidity is acceptable.")
    else:
        print(f"\n  CONCLUSION: Monthly expirations perform better.")
        print(f"  Sharpe advantage: {-sharpe_diff:+.3f}")
        print("  Recommend keeping MONTHLY expirations.")

    # Data quality notes
    print("\n" + "-" * 90)
    print("DATA QUALITY NOTES")
    print("-" * 90)
    print(f"\n  Monthly: {result_monthly['no_expiration']} days with no valid expiration, "
          f"{result_monthly['no_quote']} days with no quote")
    print(f"  Weekly:  {result_weekly['no_expiration']} days with no valid expiration, "
          f"{result_weekly['no_quote']} days with no quote")

    if result_weekly['no_quote'] > result_monthly['no_quote'] * 1.5:
        print("\n  WARNING: Weekly expirations have significantly more missing quotes.")
        print("  This may indicate liquidity issues with weekly options.")

    print("\n" + "=" * 90)

    client.close()


if __name__ == "__main__":
    main()
