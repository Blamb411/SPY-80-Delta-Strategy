"""
SMA Period Comparison Test
==========================
Tests the 80-delta call strategy with different SMA periods (50, 100, 150, 200)
to determine which provides the best risk-adjusted returns.

Usage:
    python sma_period_comparison.py
"""

import os
import sys
import math
import numpy as np
import pandas as pd
from datetime import datetime
from collections import defaultdict

_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_dir = os.path.dirname(os.path.dirname(_this_dir))
sys.path.insert(0, _project_dir)

from backtest.thetadata_client import ThetaDataClient
from backtest.black_scholes import black_scholes_price, find_strike_for_delta

# ======================================================================
# PARAMETERS
# ======================================================================

TICKER = "SPY"
SHARES = 3125
DELTA = 0.80
DTE_TARGET = 120
DTE_MIN = 90
DTE_MAX = 150
MH = 60
PT = 0.50
RATE = 0.04
SMA_EXIT_THRESHOLD = 0.02
OPTIONS_CASH_ALLOCATION = 100_000

DATA_START = "2014-01-01"
DATA_END = "2026-01-31"
SIM_START = "2015-03-01"

# SMA periods to test
SMA_PERIODS = [50, 100, 150, 200]


# ======================================================================
# HELPERS (copied from main backtest)
# ======================================================================

def is_monthly_opex(exp_str):
    exp_dt = datetime.strptime(exp_str, "%Y-%m-%d").date()
    if exp_dt.weekday() != 4:
        return False
    return 15 <= exp_dt.day <= 21


def find_best_expiration(entry_date_str, monthly_exps_dates, target=DTE_TARGET,
                         dte_min=DTE_MIN, dte_max=DTE_MAX):
    entry_dt = datetime.strptime(entry_date_str, "%Y-%m-%d").date()
    best_exp = None
    best_dte = 0
    best_diff = 9999
    for exp_str, exp_dt in monthly_exps_dates:
        dte = (exp_dt - entry_dt).days
        if dte < dte_min or dte > dte_max:
            continue
        diff = abs(dte - target)
        if diff < best_diff:
            best_diff = diff
            best_exp = exp_str
            best_dte = dte
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


def calculate_delta(spot, strike, dte, iv=0.16, rate=0.04, right="C"):
    if dte <= 0:
        if right == "C":
            return 1.0 if spot > strike else 0.0
        else:
            return -1.0 if spot < strike else 0.0
    t = dte / 365.0
    d1 = (math.log(spot / strike) + (rate + 0.5 * iv * iv) * t) / (iv * math.sqrt(t))
    delta = 0.5 * (1.0 + math.erf(d1 / math.sqrt(2.0)))
    if right == "P":
        delta = delta - 1.0
    return delta


def compute_sma(bars_by_date, trading_dates, period):
    """Compute SMA for a given period."""
    sma = {}
    for i in range(period - 1, len(trading_dates)):
        window = [bars_by_date[trading_dates[j]]["close"] for j in range(i - period + 1, i + 1)]
        sma[trading_dates[i]] = sum(window) / float(period)
    return sma


# ======================================================================
# SIMULATION ENGINE
# ======================================================================

def run_simulation(client, bars_by_date, trading_dates, vix_data, sma, monthly_exps,
                   sma_period, contract_eod_cache, strikes_cache):
    """Run the simulation with a specific SMA."""

    shares_held = SHARES
    options_cash = float(OPTIONS_CASH_ALLOCATION)
    pending_cash = 0.0
    positions = []

    daily_snapshots = []
    trade_log = []
    entry_skip_reasons = defaultdict(int)
    force_exit_count = 0

    # Find start index
    start_idx = next((i for i, d in enumerate(trading_dates) if d >= SIM_START), 0)

    for day_idx in range(start_idx, len(trading_dates)):
        today = trading_dates[day_idx]
        bar = bars_by_date[today]
        spot = bar["close"]
        sma_val = sma.get(today)
        above_sma = (spot > sma_val) if sma_val else False
        vix = vix_data.get(today, 20.0)
        iv_est = max(0.08, min(0.90, vix / 100.0))

        shares_value = shares_held * spot
        options_cash += pending_cash
        pending_cash = 0.0

        # Force-exit check
        pct_below_sma = (sma_val - spot) / sma_val if sma_val and sma_val > 0 else 0
        if pct_below_sma >= SMA_EXIT_THRESHOLD and positions:
            for pos in positions:
                ckey = (pos["expiration"], pos["strike"])
                eod = contract_eod_cache.get(ckey, {}).get(today)
                bid, _ = get_bid_ask(eod)
                if bid is None or bid <= 0:
                    intrinsic = max(0, spot - pos["strike"])
                    bid = intrinsic * 0.998 if intrinsic > 0 else 0.001
                proceeds = bid * 100 * pos["quantity"]
                pending_cash += proceeds
                pnl_pct = bid / pos["entry_price"] - 1
                trade_log.append({
                    "entry_date": pos["entry_date"],
                    "exit_date": today,
                    "expiration": pos["expiration"],
                    "strike": pos["strike"],
                    "quantity": pos["quantity"],
                    "entry_price": pos["entry_price"],
                    "exit_price": bid,
                    "pnl_pct": pnl_pct,
                    "pnl_dollar": proceeds - pos["contract_cost"],
                    "days_held": pos["days_held"] + 1,
                    "exit_reason": "SMA",
                })
                force_exit_count += 1
            positions = []

        # Normal exits
        still_open = []
        for pos in positions:
            pos["days_held"] += 1
            ckey = (pos["expiration"], pos["strike"])
            eod = contract_eod_cache.get(ckey, {}).get(today)
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
                    "entry_date": pos["entry_date"],
                    "exit_date": today,
                    "expiration": pos["expiration"],
                    "strike": pos["strike"],
                    "quantity": pos["quantity"],
                    "entry_price": pos["entry_price"],
                    "exit_price": bid,
                    "pnl_pct": pnl_pct,
                    "pnl_dollar": proceeds - pos["contract_cost"],
                    "days_held": pos["days_held"],
                    "exit_reason": exit_reason,
                })
            else:
                still_open.append(pos)
        positions = still_open

        # Current options delta
        current_options_delta = 0.0
        for pos in positions:
            dte = (datetime.strptime(pos["expiration"], "%Y-%m-%d").date() -
                   datetime.strptime(today, "%Y-%m-%d").date()).days
            pos_delta = calculate_delta(spot, pos["strike"], dte, iv_est) * pos["quantity"] * 100
            current_options_delta += pos_delta

        # Entry
        delta_room = SHARES - current_options_delta
        if above_sma and sma_val is not None and delta_room > 80:
            best_exp, dte_cal = find_best_expiration(today, monthly_exps)
            if not best_exp:
                entry_skip_reasons["no_expiration"] += 1
            else:
                t_years = dte_cal / 365.0
                bs_strike = find_strike_for_delta(spot, t_years, RATE, iv_est, DELTA, "C")
                if not bs_strike:
                    entry_skip_reasons["bs_fail"] += 1
                else:
                    if best_exp not in strikes_cache:
                        strikes_cache[best_exp] = client.get_strikes(TICKER, best_exp)
                    strikes = strikes_cache[best_exp]
                    if not strikes:
                        entry_skip_reasons["no_strikes"] += 1
                    else:
                        real_strike = min(strikes, key=lambda s: abs(s - bs_strike))
                        ckey = (best_exp, real_strike)
                        if ckey not in contract_eod_cache:
                            data = client.prefetch_option_life(TICKER, best_exp, real_strike, "C", today)
                            contract_eod_cache[ckey] = {r["bar_date"]: r for r in data}
                        eod = contract_eod_cache[ckey].get(today)
                        _, ask = get_bid_ask(eod)
                        if ask is None or ask <= 0:
                            entry_skip_reasons["no_ask"] += 1
                        else:
                            option_delta = calculate_delta(spot, real_strike, dte_cal, iv_est)
                            max_by_delta = int(delta_room / (option_delta * 100))
                            contract_cost = ask * 100
                            max_by_cash = int(options_cash / contract_cost)
                            qty = min(max_by_delta, max_by_cash, 1)

                            if qty <= 0:
                                if max_by_delta <= 0:
                                    entry_skip_reasons["delta_cap"] += 1
                                else:
                                    entry_skip_reasons["no_capital"] += 1
                            else:
                                total_cost = contract_cost * qty
                                options_cash -= total_cost
                                positions.append({
                                    "entry_date": today,
                                    "expiration": best_exp,
                                    "strike": real_strike,
                                    "entry_price": ask,
                                    "quantity": qty,
                                    "contract_cost": total_cost,
                                    "days_held": 0,
                                    "entry_delta": option_delta,
                                })

        # Mark to market
        positions_value = 0.0
        total_options_delta = 0.0
        for pos in positions:
            ckey = (pos["expiration"], pos["strike"])
            eod = contract_eod_cache.get(ckey, {}).get(today)
            bid, ask = get_bid_ask(eod)
            if bid and ask:
                mid = (bid + ask) / 2.0
            else:
                mid = max(0, spot - pos["strike"])
            positions_value += mid * 100 * pos["quantity"]

            dte = (datetime.strptime(pos["expiration"], "%Y-%m-%d").date() -
                   datetime.strptime(today, "%Y-%m-%d").date()).days
            pos_delta = calculate_delta(spot, pos["strike"], dte, iv_est) * pos["quantity"] * 100
            total_options_delta += pos_delta

        portfolio_value = shares_value + options_cash + pending_cash + positions_value
        total_delta = shares_held + total_options_delta

        daily_snapshots.append({
            "date": today,
            "portfolio_value": portfolio_value,
            "shares_value": shares_value,
            "options_value": positions_value,
            "options_cash": options_cash + pending_cash,
            "total_delta": total_delta,
            "above_sma": above_sma,
            "spot": spot,
            "sma": sma_val,
        })

    return daily_snapshots, trade_log, force_exit_count


def compute_metrics(snapshots, trade_log):
    """Compute portfolio metrics."""
    df = pd.DataFrame(snapshots)
    tdf = pd.DataFrame(trade_log) if trade_log else pd.DataFrame()

    n_days = len(df)
    years = n_days / 252.0

    start_val = df["portfolio_value"].iloc[0]
    end_val = df["portfolio_value"].iloc[-1]

    total_return = end_val / start_val - 1
    cagr = (end_val / start_val) ** (1 / years) - 1 if years > 0 else 0

    df["daily_ret"] = df["portfolio_value"].pct_change().fillna(0)
    daily_std = df["daily_ret"].std()
    sharpe = (df["daily_ret"].mean() / daily_std) * np.sqrt(252) if daily_std > 0 else 0

    cummax = df["portfolio_value"].cummax()
    drawdown = df["portfolio_value"] / cummax - 1
    max_dd = drawdown.min()

    # Trade stats
    n_trades = len(tdf)
    win_rate = (tdf["pnl_pct"] > 0).mean() if n_trades > 0 else 0
    mean_ret = tdf["pnl_pct"].mean() if n_trades > 0 else 0
    total_pnl = tdf["pnl_dollar"].sum() if n_trades > 0 else 0

    # Count whipsaws (SMA exits)
    sma_exits = len(tdf[tdf["exit_reason"] == "SMA"]) if n_trades > 0 else 0

    return {
        "start_val": start_val,
        "end_val": end_val,
        "total_return": total_return,
        "cagr": cagr,
        "sharpe": sharpe,
        "max_dd": max_dd,
        "n_trades": n_trades,
        "win_rate": win_rate,
        "mean_ret": mean_ret,
        "total_pnl": total_pnl,
        "sma_exits": sma_exits,
    }


# ======================================================================
# MAIN
# ======================================================================

def main():
    print("=" * 80)
    print("SMA Period Comparison Test")
    print("=" * 80)
    print(f"\nTesting SMA periods: {SMA_PERIODS}")
    print(f"Other parameters held constant:")
    print(f"  Delta: {DELTA}, DTE: {DTE_TARGET}, PT: {PT:.0%}, MH: {MH} days")
    print(f"  SMA exit threshold: {SMA_EXIT_THRESHOLD:.0%}")

    client = ThetaDataClient()
    if not client.connect():
        print("\nERROR: Cannot connect to Theta Terminal.")
        return

    print("\nConnected to Theta Terminal.")

    # Load data once
    print("\nLoading SPY data...")
    spy_bars = client.fetch_ticker_bars(TICKER, DATA_START, DATA_END)
    bars_by_date = {b["bar_date"]: b for b in spy_bars}
    trading_dates = sorted(bars_by_date.keys())

    print("Loading VIX history...")
    vix_data = client.fetch_vix_history(DATA_START, DATA_END)

    print("Loading expirations...")
    all_exps = client.get_expirations(TICKER)
    monthly_exps = [(e, datetime.strptime(e, "%Y-%m-%d").date())
                    for e in all_exps if is_monthly_opex(e)]
    monthly_exps.sort(key=lambda x: x[1])

    print(f"  SPY bars: {len(spy_bars)}")
    print(f"  Monthly expirations: {len(monthly_exps)}")

    # Shared caches for options data (since we're testing same underlying)
    contract_eod_cache = {}
    strikes_cache = {}

    # Run tests
    results = {}

    for sma_period in SMA_PERIODS:
        print(f"\n{'='*60}")
        print(f"Testing SMA{sma_period}...")
        print(f"{'='*60}")

        # Compute SMA for this period
        sma = compute_sma(bars_by_date, trading_dates, sma_period)
        first_sma_date = sorted(sma.keys())[0]
        print(f"  SMA{sma_period} available from: {first_sma_date}")

        # Run simulation
        snapshots, trades, force_exits = run_simulation(
            client, bars_by_date, trading_dates, vix_data, sma, monthly_exps,
            sma_period, contract_eod_cache, strikes_cache
        )

        # Compute metrics
        metrics = compute_metrics(snapshots, trades)
        metrics["sma_period"] = sma_period
        metrics["force_exits"] = force_exits
        results[sma_period] = metrics

        print(f"  Trades: {metrics['n_trades']}, SMA exits: {metrics['sma_exits']}")
        print(f"  CAGR: {metrics['cagr']:+.1%}, Sharpe: {metrics['sharpe']:.2f}")

    client.close()

    # Print comparison table
    print(f"\n{'='*80}")
    print("COMPARISON RESULTS")
    print(f"{'='*80}")

    print(f"\n  {'SMA':>6} {'CAGR':>10} {'Sharpe':>8} {'MaxDD':>10} {'Trades':>8} {'WinRate':>8} {'SMAExits':>10} {'TotalPnL':>12}")
    print(f"  {'-'*76}")

    for period in SMA_PERIODS:
        m = results[period]
        print(f"  {period:>6} {m['cagr']:>+9.1%} {m['sharpe']:>8.2f} {m['max_dd']:>9.1%} "
              f"{m['n_trades']:>8} {m['win_rate']:>7.1%} {m['sma_exits']:>10} ${m['total_pnl']:>11,.0f}")

    # Find best by different metrics
    print(f"\n{'='*80}")
    print("BEST BY METRIC")
    print(f"{'='*80}")

    best_sharpe = max(results.items(), key=lambda x: x[1]['sharpe'])
    best_cagr = max(results.items(), key=lambda x: x[1]['cagr'])
    best_dd = max(results.items(), key=lambda x: x[1]['max_dd'])  # least negative
    fewest_whipsaws = min(results.items(), key=lambda x: x[1]['sma_exits'])

    print(f"\n  Best Sharpe ratio: SMA{best_sharpe[0]} ({best_sharpe[1]['sharpe']:.2f})")
    print(f"  Best CAGR: SMA{best_cagr[0]} ({best_cagr[1]['cagr']:+.1%})")
    print(f"  Smallest max drawdown: SMA{best_dd[0]} ({best_dd[1]['max_dd']:.1%})")
    print(f"  Fewest SMA exits (whipsaws): SMA{fewest_whipsaws[0]} ({fewest_whipsaws[1]['sma_exits']})")

    # Analysis
    print(f"\n{'='*80}")
    print("ANALYSIS")
    print(f"{'='*80}")

    sma50 = results[50]
    sma200 = results[200]

    whipsaw_diff = sma50['sma_exits'] - sma200['sma_exits']
    sharpe_diff = sma200['sharpe'] - sma50['sharpe']

    print(f"\n  SMA50 vs SMA200:")
    print(f"    Additional SMA exits (whipsaws): {whipsaw_diff}")
    print(f"    Sharpe difference: {sharpe_diff:+.2f}")
    print(f"    CAGR difference: {sma50['cagr'] - sma200['cagr']:+.1%}")

    print(f"\n{'='*80}")


if __name__ == "__main__":
    main()
