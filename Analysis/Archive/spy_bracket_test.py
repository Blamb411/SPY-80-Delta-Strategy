"""
SPY Deployment Bracket Test
============================
Two focused tests to validate parameter sensitivity and whipsaw handling:

  Part 1 -- PT/MH bracket: 3x3 grid around 50%/60td to confirm
            we're on a plateau (good) not an edge (bad).

  Part 2 -- Whipsaw exit rules: 9 configs testing smarter force-exit
            rules that reduce drawdowns without whipsaw pain.

Total: 18 simulation runs.

Shares ThetaData SQLite cache with spy_deployment_sim.py so API calls
only happen once.

Usage:
    python spy_bracket_test.py
"""

import os
import sys
import math
import logging
from datetime import datetime, date, timedelta
from collections import defaultdict

import numpy as np
import pandas as pd

_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_dir = os.path.dirname(_this_dir)
sys.path.insert(0, _project_dir)

from backtest.thetadata_client import ThetaDataClient
from backtest.black_scholes import black_scholes_price, find_strike_for_delta

log = logging.getLogger("spy_bracket")

# ======================================================================
# FIXED PARAMETERS (same as deployment sim)
# ======================================================================

CAPITAL = 100_000
DELTA = 0.80
DTE_TARGET = 120
DTE_MIN = 90
DTE_MAX = 150
RATE = 0.04

DATA_START = "2014-01-01"
DATA_END = "2026-01-31"
SIM_START = "2015-03-01"


# ======================================================================
# HELPERS (borrowed from spy_deployment_sim.py)
# ======================================================================

def is_monthly_opex(exp_str):
    exp_dt = datetime.strptime(exp_str, "%Y-%m-%d").date()
    if exp_dt.weekday() != 4:
        return False
    return 15 <= exp_dt.day <= 21


def find_best_expiration(entry_date_str, monthly_exps_dates,
                         target=DTE_TARGET, dte_min=DTE_MIN, dte_max=DTE_MAX):
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


# ======================================================================
# DATA LOADING
# ======================================================================

def load_all_data(client):
    """Load SPY bars, VIX, SMA200, and monthly expirations."""
    print("Loading SPY bars...")
    spy_bars = client.fetch_spy_bars(DATA_START, DATA_END)
    spy_by_date = {b["bar_date"]: b for b in spy_bars}
    trading_dates = sorted(spy_by_date.keys())

    print("Loading VIX history...")
    vix_data = client.fetch_vix_history(DATA_START, DATA_END)

    # SMA200
    sma200 = {}
    for i in range(199, len(trading_dates)):
        window = [spy_by_date[trading_dates[j]]["close"]
                  for j in range(i - 199, i + 1)]
        sma200[trading_dates[i]] = sum(window) / 200.0

    print("Loading SPY expirations...")
    all_exps = client.get_expirations("SPY")
    monthly_exps = [(e, datetime.strptime(e, "%Y-%m-%d").date())
                    for e in all_exps if is_monthly_opex(e)]
    monthly_exps.sort(key=lambda x: x[1])

    print(f"  SPY bars: {len(spy_bars)} "
          f"({trading_dates[0]} to {trading_dates[-1]})")
    print(f"  VIX days: {len(vix_data)}")
    first_sma = sorted(sma200.keys())[0] if sma200 else "N/A"
    print(f"  SMA200 from: {first_sma}")
    print(f"  Monthly expirations: {len(monthly_exps)} "
          f"({monthly_exps[0][0]} to {monthly_exps[-1][0]})")

    return {
        "spy_by_date": spy_by_date,
        "trading_dates": trading_dates,
        "vix_data": vix_data,
        "sma200": sma200,
        "monthly_exps": monthly_exps,
    }


# ======================================================================
# SIMULATION ENGINE (parameterized)
# ======================================================================

def run_sim(client, market_data, config):
    """
    Streamlined simulation parameterized by config dict.

    config keys:
        label       -- display name
        pt          -- profit target fraction (e.g. 0.50 = +50%)
        mh          -- max hold in trading days
        exit_rule   -- dict with:
            mode          : "none" | "immediate" | "delay" | "threshold" | "combo"
            delay_days    : consecutive trading days below SMA200 before trigger
            threshold_pct : how far below SMA200 before trigger (fraction, e.g. 0.02)
    """
    spy_by_date = market_data["spy_by_date"]
    trading_dates = market_data["trading_dates"]
    vix_data = market_data["vix_data"]
    sma200 = market_data["sma200"]
    monthly_exps = market_data["monthly_exps"]

    pt = config["pt"]
    mh = config["mh"]
    exit_rule = config.get("exit_rule", {"mode": "none"})
    exit_mode = exit_rule.get("mode", "none")
    delay_days = exit_rule.get("delay_days", 0)
    threshold_pct = exit_rule.get("threshold_pct", 0.0)
    label = config.get("label", f"PT={pt:.0%}/MH={mh}")

    available_cash = float(CAPITAL)
    pending_cash = 0.0
    positions = []

    contract_eod = {}
    strikes_cache = {}

    daily_snapshots = []
    trade_log = []
    force_exit_count = 0
    days_below_sma = 0  # new state variable for whipsaw rules

    start_idx = next(
        (i for i, d in enumerate(trading_dates) if d >= SIM_START), 0
    )

    for day_idx in range(start_idx, len(trading_dates)):
        today = trading_dates[day_idx]
        bar = spy_by_date[today]
        spot = bar["close"]
        sma_val = sma200.get(today)
        above_sma = (spot > sma_val) if sma_val else False
        vix = vix_data.get(today, 20.0)

        # Track consecutive days below SMA200
        if not above_sma and sma_val is not None:
            days_below_sma += 1
        else:
            days_below_sma = 0

        # 1. Settle yesterday's exit proceeds
        available_cash += pending_cash
        pending_cash = 0.0

        # 2a. Force-exit logic based on exit_rule
        should_force_exit = False
        if exit_mode != "none" and not above_sma and sma_val is not None and positions:
            pct_below = (sma_val - spot) / sma_val if sma_val > 0 else 0

            if exit_mode == "immediate":
                # Exit on first day below (days_below_sma == 1 means just crossed)
                should_force_exit = (days_below_sma == 1)
            elif exit_mode == "delay":
                should_force_exit = (days_below_sma >= delay_days)
            elif exit_mode == "threshold":
                should_force_exit = (pct_below >= threshold_pct)
            elif exit_mode == "combo":
                should_force_exit = (
                    days_below_sma >= delay_days and pct_below >= threshold_pct
                )

        if should_force_exit:
            for pos in positions:
                ckey = (pos["expiration"], pos["strike"])
                eod = contract_eod.get(ckey, {}).get(today)
                bid, _ = get_bid_ask(eod)
                if bid is None or bid <= 0:
                    intrinsic = max(0, spot - pos["strike"])
                    bid = intrinsic * 0.998 if intrinsic > 0 else 0.001
                proceeds = bid * 100
                pending_cash += proceeds
                pnl_pct = bid / pos["entry_price"] - 1
                trade_log.append({
                    "entry_date": pos["entry_date"],
                    "exit_date": today,
                    "expiration": pos["expiration"],
                    "strike": pos["strike"],
                    "entry_price": pos["entry_price"],
                    "exit_price": bid,
                    "pnl_pct": pnl_pct,
                    "pnl_dollar": proceeds - pos["contract_cost"],
                    "days_held": pos["days_held"] + 1,
                    "exit_reason": "SMA",
                    "contract_cost": pos["contract_cost"],
                })
                force_exit_count += 1
            positions = []

        # 2b. Process normal exits (PT / MH)
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
            if pnl_pct >= pt:
                exit_reason = "PT"
            elif pos["days_held"] >= mh:
                exit_reason = "MH"

            if exit_reason:
                proceeds = bid * 100
                pending_cash += proceeds
                trade_log.append({
                    "entry_date": pos["entry_date"],
                    "exit_date": today,
                    "expiration": pos["expiration"],
                    "strike": pos["strike"],
                    "entry_price": pos["entry_price"],
                    "exit_price": bid,
                    "pnl_pct": pnl_pct,
                    "pnl_dollar": proceeds - pos["contract_cost"],
                    "days_held": pos["days_held"],
                    "exit_reason": exit_reason,
                    "contract_cost": pos["contract_cost"],
                })
            else:
                still_open.append(pos)
        positions = still_open

        # 3. Entry: buy 1 contract if above SMA200 and have capital
        entered = False
        if above_sma and sma_val is not None:
            best_exp, dte_cal = find_best_expiration(today, monthly_exps)
            if best_exp:
                t_years = dte_cal / 365.0
                iv_est = max(0.08, min(0.90, vix / 100.0))
                bs_strike = find_strike_for_delta(
                    spot, t_years, RATE, iv_est, DELTA, "C"
                )
                if bs_strike:
                    if best_exp not in strikes_cache:
                        strikes_cache[best_exp] = client.get_strikes(
                            "SPY", best_exp
                        )
                    strikes = strikes_cache[best_exp]
                    if strikes:
                        real_strike = min(
                            strikes, key=lambda s: abs(s - bs_strike)
                        )
                        ckey = (best_exp, real_strike)
                        if ckey not in contract_eod:
                            data = client.prefetch_option_life(
                                "SPY", best_exp, real_strike, "C", today
                            )
                            contract_eod[ckey] = {
                                r["bar_date"]: r for r in data
                            }
                        eod = contract_eod[ckey].get(today)
                        _, ask = get_bid_ask(eod)
                        if ask and ask > 0:
                            contract_cost = ask * 100
                            if available_cash >= contract_cost:
                                available_cash -= contract_cost
                                positions.append({
                                    "entry_date": today,
                                    "expiration": best_exp,
                                    "strike": real_strike,
                                    "entry_price": ask,
                                    "contract_cost": contract_cost,
                                    "days_held": 0,
                                })
                                entered = True

        # 4. Mark to market
        positions_value = 0.0
        for pos in positions:
            ckey = (pos["expiration"], pos["strike"])
            eod = contract_eod.get(ckey, {}).get(today)
            bid, ask = get_bid_ask(eod)
            if bid and ask:
                mid = (bid + ask) / 2.0
            else:
                mid = max(0, spot - pos["strike"])
            positions_value += mid * 100

        portfolio_value = available_cash + pending_cash + positions_value
        capital_deployed = sum(p["contract_cost"] for p in positions)
        notional_exposure = len(positions) * DELTA * spot * 100

        daily_snapshots.append({
            "date": today,
            "portfolio_value": portfolio_value,
            "cash": available_cash + pending_cash,
            "positions_value": positions_value,
            "n_positions": len(positions),
            "capital_deployed": capital_deployed,
            "notional_exposure": notional_exposure,
            "leverage": (notional_exposure / portfolio_value
                         if portfolio_value > 0 else 0),
            "above_sma": above_sma,
            "spy_close": spot,
            "entered": entered,
        })

    return daily_snapshots, trade_log, force_exit_count


# ======================================================================
# METRICS
# ======================================================================

def compute_metrics(snapshots, trade_log):
    """Compute portfolio and trade metrics. Returns dict."""
    df = pd.DataFrame(snapshots)
    tdf = pd.DataFrame(trade_log) if trade_log else pd.DataFrame()

    n_days = len(df)
    years = n_days / 252.0
    start_val = CAPITAL
    end_val = df["portfolio_value"].iloc[-1]
    total_return = end_val / start_val - 1
    cagr = (end_val / start_val) ** (1 / years) - 1 if years > 0 else 0

    df["daily_ret"] = df["portfolio_value"].pct_change().fillna(0)
    daily_mean = df["daily_ret"].mean()
    daily_std = df["daily_ret"].std()
    sharpe = ((daily_mean / daily_std) * np.sqrt(252)
              if daily_std > 0 else 0)

    downside = df["daily_ret"][df["daily_ret"] < 0]
    ds_std = downside.std() if len(downside) > 0 else 0
    sortino = ((daily_mean / ds_std) * np.sqrt(252)
               if ds_std > 0 else 0)

    cummax = df["portfolio_value"].cummax()
    drawdown = df["portfolio_value"] / cummax - 1
    max_dd = drawdown.min()

    # Trade stats
    n_trades = 0
    win_rate = 0
    avg_loss = 0
    sma_exits = 0
    if len(tdf) > 0:
        n_trades = len(tdf)
        wins = tdf[tdf["pnl_pct"] > 0]
        losses = tdf[tdf["pnl_pct"] <= 0]
        win_rate = len(wins) / n_trades
        avg_loss = losses["pnl_pct"].mean() if len(losses) > 0 else 0
        sma_exits = len(tdf[tdf["exit_reason"] == "SMA"])

    return {
        "cagr": cagr,
        "sharpe": sharpe,
        "sortino": sortino,
        "max_dd": max_dd,
        "total_return": total_return,
        "end_val": end_val,
        "n_trades": n_trades,
        "win_rate": win_rate,
        "avg_loss": avg_loss,
        "sma_exits": sma_exits,
    }


# ======================================================================
# PART 1: PT/MH BRACKET
# ======================================================================

def run_pt_mh_bracket(client, market_data):
    """Run 3x3 grid of PT/MH combinations (no force-exit)."""
    pt_values = [0.40, 0.50, 0.60]
    mh_values = [50, 60, 70]

    results = {}  # (pt, mh) -> metrics dict

    total = len(pt_values) * len(mh_values)
    run_num = 0
    for pt in pt_values:
        for mh in mh_values:
            run_num += 1
            label = f"PT={pt:.0%}/MH={mh}td"
            print(f"\n  [{run_num}/{total}] Running {label} ...")

            config = {
                "label": label,
                "pt": pt,
                "mh": mh,
                "exit_rule": {"mode": "none"},
            }
            snaps, trades, _ = run_sim(client, market_data, config)
            m = compute_metrics(snaps, trades)
            results[(pt, mh)] = m

            print(f"           CAGR={m['cagr']:+.1%}  "
                  f"Sharpe={m['sharpe']:.2f}  "
                  f"MaxDD={m['max_dd']:.1%}  "
                  f"Trades={m['n_trades']}")

    # Print grid tables
    W = 80
    print(f"\n{'=' * W}")
    print("PART 1: PT / MH BRACKET (no force-exit)")
    print(f"{'=' * W}")
    print(f"  Entry: buy 1 contract/day when SPY > SMA200")
    print(f"  Options: {DELTA:.0%}-delta call, ~{DTE_TARGET} DTE, monthly")
    print(f"  Exit: profit target OR max hold (no SMA force-exit)")

    # CAGR grid
    pt_mh_hdr = "PT \\ MH"
    print(f"\n  CAGR:")
    print(f"  {pt_mh_hdr:<12}", end="")
    for mh in mh_values:
        print(f"  {mh:>8}td", end="")
    print()
    print(f"  {'-' * 42}")
    for pt in pt_values:
        print(f"  {pt:>8.0%}    ", end="")
        for mh in mh_values:
            m = results[(pt, mh)]
            marker = " *" if pt == 0.50 and mh == 60 else "  "
            print(f"  {m['cagr']:>+7.1%}{marker}", end="")
        print()

    # Sharpe grid
    print(f"\n  Sharpe:")
    print(f"  {pt_mh_hdr:<12}", end="")
    for mh in mh_values:
        print(f"  {mh:>8}td", end="")
    print()
    print(f"  {'-' * 42}")
    for pt in pt_values:
        print(f"  {pt:>8.0%}    ", end="")
        for mh in mh_values:
            m = results[(pt, mh)]
            marker = " *" if pt == 0.50 and mh == 60 else "  "
            print(f"  {m['sharpe']:>8.2f}{marker}", end="")
        print()

    # Max DD grid
    print(f"\n  Max Drawdown:")
    print(f"  {pt_mh_hdr:<12}", end="")
    for mh in mh_values:
        print(f"  {mh:>8}td", end="")
    print()
    print(f"  {'-' * 42}")
    for pt in pt_values:
        print(f"  {pt:>8.0%}    ", end="")
        for mh in mh_values:
            m = results[(pt, mh)]
            marker = " *" if pt == 0.50 and mh == 60 else "  "
            print(f"  {m['max_dd']:>+7.1%}{marker}", end="")
        print()

    print(f"\n  (* = current production setting: PT=50%/MH=60td)")

    # Plateau vs edge analysis
    center = results[(0.50, 60)]
    neighbors = [results[k] for k in results if k != (0.50, 60)]
    cagr_spread = max(r["cagr"] for r in neighbors) - min(r["cagr"] for r in neighbors)
    sharpe_spread = max(r["sharpe"] for r in neighbors) - min(r["sharpe"] for r in neighbors)
    center_cagr_rank = sum(1 for r in neighbors if r["cagr"] > center["cagr"]) + 1
    center_sharpe_rank = sum(1 for r in neighbors if r["sharpe"] > center["sharpe"]) + 1

    print(f"\n  Sensitivity analysis:")
    print(f"    CAGR range across grid:   {cagr_spread:.1%}")
    print(f"    Sharpe range across grid:  {sharpe_spread:.2f}")
    print(f"    50%/60td CAGR rank:   {center_cagr_rank} of 9")
    print(f"    50%/60td Sharpe rank:  {center_sharpe_rank} of 9")

    if cagr_spread < 0.05 and sharpe_spread < 0.15:
        print(f"    --> PLATEAU: parameters are robust, small sensitivity")
    elif center_cagr_rank <= 3 and center_sharpe_rank <= 3:
        print(f"    --> NEAR-OPTIMAL: 50%/60td is in the sweet spot")
    elif center_cagr_rank >= 7 or center_sharpe_rank >= 7:
        print(f"    --> EDGE WARNING: 50%/60td may not be optimal")
    else:
        print(f"    --> MODERATE: 50%/60td is reasonable but not dominant")

    return results


# ======================================================================
# PART 2: WHIPSAW EXIT RULES
# ======================================================================

def run_whipsaw_bracket(client, market_data):
    """Run 9 whipsaw exit rule configurations (all with PT=50%, MH=60)."""

    configs = [
        {
            "label": "no_exit",
            "pt": 0.50, "mh": 60,
            "exit_rule": {"mode": "none"},
        },
        {
            "label": "immediate",
            "pt": 0.50, "mh": 60,
            "exit_rule": {"mode": "immediate"},
        },
        {
            "label": "delay_5d",
            "pt": 0.50, "mh": 60,
            "exit_rule": {"mode": "delay", "delay_days": 5},
        },
        {
            "label": "delay_10d",
            "pt": 0.50, "mh": 60,
            "exit_rule": {"mode": "delay", "delay_days": 10},
        },
        {
            "label": "delay_20d",
            "pt": 0.50, "mh": 60,
            "exit_rule": {"mode": "delay", "delay_days": 20},
        },
        {
            "label": "thresh_2pct",
            "pt": 0.50, "mh": 60,
            "exit_rule": {"mode": "threshold", "threshold_pct": 0.02},
        },
        {
            "label": "thresh_3pct",
            "pt": 0.50, "mh": 60,
            "exit_rule": {"mode": "threshold", "threshold_pct": 0.03},
        },
        {
            "label": "thresh_5pct",
            "pt": 0.50, "mh": 60,
            "exit_rule": {"mode": "threshold", "threshold_pct": 0.05},
        },
        {
            "label": "combo_5d_2pct",
            "pt": 0.50, "mh": 60,
            "exit_rule": {
                "mode": "combo",
                "delay_days": 5,
                "threshold_pct": 0.02,
            },
        },
    ]

    results = []
    total = len(configs)

    for i, cfg in enumerate(configs):
        print(f"\n  [{i+1}/{total}] Running {cfg['label']} ...")
        snaps, trades, force_exits = run_sim(client, market_data, cfg)
        m = compute_metrics(snaps, trades)
        m["label"] = cfg["label"]
        m["force_exits"] = force_exits
        results.append(m)

        print(f"           CAGR={m['cagr']:+.1%}  "
              f"Sharpe={m['sharpe']:.2f}  "
              f"MaxDD={m['max_dd']:.1%}  "
              f"SMA-exits={m['sma_exits']}  "
              f"Trades={m['n_trades']}")

    # Sort by Sharpe descending
    ranked = sorted(results, key=lambda r: r["sharpe"], reverse=True)

    W = 100
    print(f"\n{'=' * W}")
    print("PART 2: WHIPSAW EXIT RULES (PT=50%, MH=60td)")
    print(f"{'=' * W}")
    print(f"  Ranked by Sharpe ratio (best to worst):\n")

    header = (f"  {'Rule':<16} {'CAGR':>8} {'Sharpe':>8} {'Sortino':>8} "
              f"{'MaxDD':>8} {'Trades':>8} {'SMA-exits':>10} "
              f"{'AvgLoss':>8} {'WinRate':>8}")
    print(header)
    print(f"  {'-' * (len(header) - 2)}")

    for r in ranked:
        print(f"  {r['label']:<16} "
              f"{r['cagr']:>+7.1%} "
              f"{r['sharpe']:>8.2f} "
              f"{r['sortino']:>8.2f} "
              f"{r['max_dd']:>+7.1%} "
              f"{r['n_trades']:>8} "
              f"{r['sma_exits']:>10} "
              f"{r['avg_loss']:>+7.1%} "
              f"{r['win_rate']:>7.1%}")

    # Best Sharpe vs baseline
    baseline = next(r for r in results if r["label"] == "no_exit")
    best = ranked[0]

    print(f"\n  Baseline (no_exit):  CAGR={baseline['cagr']:+.1%}  "
          f"Sharpe={baseline['sharpe']:.2f}  MaxDD={baseline['max_dd']:.1%}")
    print(f"  Best Sharpe ({best['label']}):  CAGR={best['cagr']:+.1%}  "
          f"Sharpe={best['sharpe']:.2f}  MaxDD={best['max_dd']:.1%}")

    sharpe_gain = best["sharpe"] - baseline["sharpe"]
    cagr_cost = baseline["cagr"] - best["cagr"]
    dd_improvement = baseline["max_dd"] - best["max_dd"]

    print(f"\n  Sharpe improvement: {sharpe_gain:+.2f}")
    print(f"  CAGR cost:          {cagr_cost:+.1%}")
    print(f"  Drawdown reduction: {dd_improvement:+.1%}")

    if sharpe_gain > 0.05 and cagr_cost < 0.03:
        print(f"\n  --> RECOMMENDATION: {best['label']} improves risk-adjusted "
              f"returns with minimal CAGR cost")
    elif sharpe_gain > 0:
        print(f"\n  --> {best['label']} offers modest improvement; "
              f"evaluate if {cagr_cost:.1%} CAGR cost is acceptable")
    else:
        print(f"\n  --> No exit rule improves on entry-only filter")

    return results


# ======================================================================
# MAIN
# ======================================================================

def main():
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    W = 80
    print("=" * W)
    print("SPY Deployment Bracket Test")
    print("=" * W)
    print(f"  Capital:   ${CAPITAL:,}")
    print(f"  Strategy:  {DELTA:.0%}-delta call, ~{DTE_TARGET} DTE, monthly")
    print(f"  Period:    {SIM_START} to {DATA_END}")
    print(f"  Tests:     9 PT/MH configs + 9 whipsaw configs = 18 runs")

    client = ThetaDataClient()
    if not client.connect():
        print("\nERROR: Cannot connect to Theta Terminal.")
        print("Make sure Theta Terminal v3 is running.")
        return

    print("\nConnected to Theta Terminal.\n")

    market_data = load_all_data(client)

    # Part 1: PT/MH bracket
    print(f"\n{'#' * W}")
    print("PART 1: PT / MH BRACKET (9 runs)")
    print(f"{'#' * W}")
    pt_mh_results = run_pt_mh_bracket(client, market_data)

    # Part 2: Whipsaw exit rules
    print(f"\n{'#' * W}")
    print("PART 2: WHIPSAW EXIT RULES (9 runs)")
    print(f"{'#' * W}")
    whipsaw_results = run_whipsaw_bracket(client, market_data)

    # Final summary
    print(f"\n{'=' * W}")
    print("FINAL SUMMARY")
    print(f"{'=' * W}")

    # PT/MH verdict
    center = pt_mh_results[(0.50, 60)]
    all_sharpes = [pt_mh_results[k]["sharpe"] for k in pt_mh_results]
    best_ptmh_key = max(pt_mh_results, key=lambda k: pt_mh_results[k]["sharpe"])
    best_ptmh = pt_mh_results[best_ptmh_key]

    print(f"\n  PT/MH Analysis:")
    print(f"    Current (50%/60td): CAGR={center['cagr']:+.1%}  "
          f"Sharpe={center['sharpe']:.2f}  MaxDD={center['max_dd']:.1%}")
    print(f"    Best in grid ({best_ptmh_key[0]:.0%}/{best_ptmh_key[1]}td): "
          f"CAGR={best_ptmh['cagr']:+.1%}  "
          f"Sharpe={best_ptmh['sharpe']:.2f}  MaxDD={best_ptmh['max_dd']:.1%}")
    print(f"    Sharpe range: {min(all_sharpes):.2f} to {max(all_sharpes):.2f}")

    # Whipsaw verdict
    ws_ranked = sorted(whipsaw_results, key=lambda r: r["sharpe"], reverse=True)
    ws_best = ws_ranked[0]
    ws_baseline = next(r for r in whipsaw_results if r["label"] == "no_exit")

    print(f"\n  Whipsaw Analysis:")
    print(f"    Baseline (no_exit): CAGR={ws_baseline['cagr']:+.1%}  "
          f"Sharpe={ws_baseline['sharpe']:.2f}  MaxDD={ws_baseline['max_dd']:.1%}")
    print(f"    Best rule ({ws_best['label']}): CAGR={ws_best['cagr']:+.1%}  "
          f"Sharpe={ws_best['sharpe']:.2f}  MaxDD={ws_best['max_dd']:.1%}")

    print(f"\n{'=' * W}")
    print("Done. 18 configs tested.")
    print(f"{'=' * W}")

    client.close()


if __name__ == "__main__":
    main()
