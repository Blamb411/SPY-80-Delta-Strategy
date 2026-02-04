"""
QQQ Combined Strategy Analysis
===============================
Models a combined portfolio of:

  Component 1: $1M QQQ Buy & Hold (shares held throughout)
  Component 2: $1M leveraged 80-delta call strategy on QQQ
      - 50% profit target, no stop-loss, 60td max hold
      - Force-exit all when QQQ >2% below SMA200
      - Idle cash in BIL (short-term treasuries)

Compares two versions:
  A) Without covered calls
  B) With covered calls sold on QQQ shares during below-SMA200 periods
     (~30-delta OTM calls, ~35 DTE, monthly expiration)
     Sell at bid, buy back at ask (conservative execution)

Usage:
    python qqq_combined_analysis.py
"""

import os
import sys
import math
import logging
from datetime import datetime, date, timedelta
from collections import defaultdict

import numpy as np
import pandas as pd
import yfinance as yf

_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_dir = os.path.dirname(os.path.dirname(_this_dir))
sys.path.insert(0, _project_dir)
sys.path.insert(0, _this_dir)

from backtest.thetadata_client import ThetaDataClient
from backtest.black_scholes import black_scholes_price, find_strike_for_delta

log = logging.getLogger("qqq_combined")

# ======================================================================
# PARAMETERS
# ======================================================================

ALLOC = 1_000_000
OPTIONS_CAPITAL = 100_000
SCALE = ALLOC / OPTIONS_CAPITAL

DELTA = 0.80
DTE_TARGET = 120
DTE_MIN = 90
DTE_MAX = 150
MH = 60
PT = 0.50
RATE = 0.04
SMA_EXIT_THRESHOLD = 0.02   # 2% below SMA200 force-exit

CC_DELTA = 0.30
CC_DTE_TARGET = 35
CC_DTE_MIN = 20
CC_DTE_MAX = 50

DATA_START = "2014-01-01"
DATA_END = "2026-01-31"
SIM_START = "2015-03-01"


# ======================================================================
# HELPERS
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


# ======================================================================
# DATA LOADING
# ======================================================================

def fetch_etf_returns(ticker, start, end):
    print(f"  Fetching {ticker}...")
    df = yf.download(ticker, start=start, end=end, auto_adjust=True, progress=False)
    if df.empty:
        return {}
    close = df["Close"]
    if isinstance(close, pd.DataFrame):
        close = close.iloc[:, 0]
    rets = close.pct_change().fillna(0)
    result = {}
    for idx in rets.index:
        result[idx.strftime("%Y-%m-%d")] = float(rets.loc[idx])
    cum = (1 + pd.Series(list(result.values()))).prod() - 1
    n_yr = len(result) / 252.0
    cagr = (1 + cum) ** (1 / n_yr) - 1 if n_yr > 0 else 0
    print(f"    {ticker}: {len(result)} days, cumulative {cum:+.1%}, CAGR {cagr:+.1%}")
    return result


def load_all_data(client):
    print("Loading QQQ bars...")
    qqq_bars = client.fetch_ticker_bars("QQQ", DATA_START, DATA_END)
    qqq_by_date = {b["bar_date"]: b for b in qqq_bars}
    trading_dates = sorted(qqq_by_date.keys())

    print("Loading SPY bars (for B&H reference)...")
    spy_bars = client.fetch_spy_bars(DATA_START, DATA_END)
    spy_by_date = {b["bar_date"]: b for b in spy_bars}

    print("Loading VIX history...")
    vix_data = client.fetch_vix_history(DATA_START, DATA_END)

    sma200 = {}
    for i in range(199, len(trading_dates)):
        window = [qqq_by_date[trading_dates[j]]["close"] for j in range(i - 199, i + 1)]
        sma200[trading_dates[i]] = sum(window) / 200.0

    print("Loading QQQ expirations...")
    all_exps = client.get_expirations("QQQ")
    monthly_exps = [(e, datetime.strptime(e, "%Y-%m-%d").date())
                    for e in all_exps if is_monthly_opex(e)]
    monthly_exps.sort(key=lambda x: x[1])

    print(f"  QQQ bars: {len(qqq_bars)} ({trading_dates[0]} to {trading_dates[-1]})")
    print(f"  SPY bars: {len(spy_bars)} (for B&H reference)")
    print(f"  VIX days: {len(vix_data)}")
    first_sma = sorted(sma200.keys())[0] if sma200 else "N/A"
    print(f"  SMA200 from: {first_sma}")
    print(f"  Monthly exps: {len(monthly_exps)}")

    return qqq_by_date, spy_by_date, trading_dates, vix_data, sma200, monthly_exps


# ======================================================================
# OPTIONS STRATEGY SIMULATION
# ======================================================================

def run_options_sim(client, qqq_by_date, trading_dates, vix_data, sma200,
                    monthly_exps):
    available_cash = float(OPTIONS_CAPITAL)
    pending_cash = 0.0
    positions = []

    contract_eod = {}
    strikes_cache = {}

    daily_snapshots = []
    trade_log = []
    entry_skip_reasons = defaultdict(int)
    force_exit_count = 0

    start_idx = next((i for i, d in enumerate(trading_dates) if d >= SIM_START), 0)

    print(f"\n{'='*70}")
    print(f"QQQ Options Strategy: {DELTA:.0%}-delta calls, {SMA_EXIT_THRESHOLD:.0%} SMA exit")
    print(f"  Period: {trading_dates[start_idx]} to {trading_dates[-1]}")
    print(f"{'='*70}")

    for day_idx in range(start_idx, len(trading_dates)):
        today = trading_dates[day_idx]
        bar = qqq_by_date[today]
        spot = bar["close"]
        sma_val = sma200.get(today)
        above_sma = (spot > sma_val) if sma_val else False
        vix = vix_data.get(today, 20.0)

        available_cash += pending_cash
        pending_cash = 0.0

        pct_below = (sma_val - spot) / sma_val if sma_val and sma_val > 0 else 0
        if pct_below >= SMA_EXIT_THRESHOLD and positions:
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
                    "entry_date": pos["entry_date"], "exit_date": today,
                    "expiration": pos["expiration"], "strike": pos["strike"],
                    "entry_price": pos["entry_price"], "exit_price": bid,
                    "pnl_pct": pnl_pct,
                    "pnl_dollar": proceeds - pos["contract_cost"],
                    "days_held": pos["days_held"] + 1, "exit_reason": "SMA",
                    "contract_cost": pos["contract_cost"],
                })
                force_exit_count += 1
            positions = []

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
                proceeds = bid * 100
                pending_cash += proceeds
                trade_log.append({
                    "entry_date": pos["entry_date"], "exit_date": today,
                    "expiration": pos["expiration"], "strike": pos["strike"],
                    "entry_price": pos["entry_price"], "exit_price": bid,
                    "pnl_pct": pnl_pct,
                    "pnl_dollar": proceeds - pos["contract_cost"],
                    "days_held": pos["days_held"], "exit_reason": exit_reason,
                    "contract_cost": pos["contract_cost"],
                })
            else:
                still_open.append(pos)
        positions = still_open

        entered = False
        if above_sma and sma_val is not None:
            best_exp, dte_cal = find_best_expiration(today, monthly_exps)
            if not best_exp:
                entry_skip_reasons["no_expiration"] += 1
            else:
                t_years = dte_cal / 365.0
                iv_est = max(0.08, min(0.90, vix / 100.0))
                bs_strike = find_strike_for_delta(spot, t_years, RATE, iv_est, DELTA, "C")
                if not bs_strike:
                    entry_skip_reasons["bs_fail"] += 1
                else:
                    if best_exp not in strikes_cache:
                        strikes_cache[best_exp] = client.get_strikes("QQQ", best_exp)
                    strikes = strikes_cache[best_exp]
                    if not strikes:
                        entry_skip_reasons["no_strikes"] += 1
                    else:
                        real_strike = min(strikes, key=lambda s: abs(s - bs_strike))
                        ckey = (best_exp, real_strike)
                        if ckey not in contract_eod:
                            data = client.prefetch_option_life(
                                "QQQ", best_exp, real_strike, "C", today
                            )
                            contract_eod[ckey] = {r["bar_date"]: r for r in data}
                        eod = contract_eod[ckey].get(today)
                        _, ask = get_bid_ask(eod)
                        if ask is None or ask <= 0:
                            entry_skip_reasons["no_ask"] += 1
                        else:
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
                            else:
                                entry_skip_reasons["no_capital"] += 1

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

        daily_snapshots.append({
            "date": today, "portfolio_value": portfolio_value,
            "cash": available_cash + pending_cash,
            "positions_value": positions_value,
            "n_positions": len(positions),
            "above_sma": above_sma, "qqq_close": spot, "entered": entered,
        })

        real_idx = day_idx - start_idx
        total_days = len(trading_dates) - start_idx
        if (real_idx + 1) % 500 == 0 or real_idx == 0:
            print(f"  [{real_idx+1}/{total_days}] {today}  "
                  f"Portfolio=${portfolio_value:,.0f}  "
                  f"Pos={len(positions)}  Cash=${available_cash + pending_cash:,.0f}")

    print(f"\n  Trades: {len(trade_log)}  |  Force-exits: {force_exit_count}")
    print(f"  Entry skips: {dict(entry_skip_reasons)}")

    return daily_snapshots, trade_log


# ======================================================================
# COVERED CALL OVERLAY
# ======================================================================

def run_cc_overlay(client, qqq_by_date, trading_dates, vix_data, sma200,
                   monthly_exps, n_cc_contracts, sim_start):
    cc_position = None
    cc_realized = 0.0
    contract_eod = {}
    strikes_cache = {}
    cc_trade_log = []
    daily_cc_values = []

    start_idx = next((i for i, d in enumerate(trading_dates) if d >= sim_start), 0)

    print(f"\n{'='*70}")
    print(f"QQQ Covered Call Overlay: {CC_DELTA:.0%}-delta, ~{CC_DTE_TARGET}d DTE")
    print(f"  Active when QQQ < SMA200  |  {n_cc_contracts} contracts")
    print(f"{'='*70}")

    cc_sells = 0
    cc_expirations = 0
    cc_buybacks = 0

    for day_idx in range(len(trading_dates)):
        today = trading_dates[day_idx]
        if today < sim_start:
            daily_cc_values.append((0.0, 0.0))
            continue

        spot = qqq_by_date[today]["close"]
        sma_val = sma200.get(today)
        above_sma = (spot > sma_val) if sma_val else True
        vix = vix_data.get(today, 20.0)

        if cc_position:
            exp_dt = datetime.strptime(cc_position["expiration"], "%Y-%m-%d").date()
            today_dt = datetime.strptime(today, "%Y-%m-%d").date()
            if today_dt >= exp_dt:
                intrinsic = max(0, spot - cc_position["strike"])
                settlement = intrinsic * 100 * cc_position["n_contracts"]
                cc_realized -= settlement
                cc_trade_log.append({
                    "entry_date": cc_position["entry_date"], "exit_date": today,
                    "expiration": cc_position["expiration"],
                    "strike": cc_position["strike"],
                    "n_contracts": cc_position["n_contracts"],
                    "premium": cc_position["entry_price"],
                    "exit_cost": intrinsic,
                    "pnl_per_share": cc_position["entry_price"] - intrinsic,
                    "total_pnl": (cc_position["entry_price"] - intrinsic) * 100 * cc_position["n_contracts"],
                    "exit_reason": "expired_itm" if intrinsic > 0 else "expired_otm",
                })
                cc_expirations += 1
                cc_position = None

        if above_sma and cc_position is not None:
            ckey = (cc_position["expiration"], cc_position["strike"])
            eod = contract_eod.get(ckey, {}).get(today)
            _, ask = get_bid_ask(eod)
            if ask is None or ask <= 0:
                exp_dt = datetime.strptime(cc_position["expiration"], "%Y-%m-%d").date()
                today_dt = datetime.strptime(today, "%Y-%m-%d").date()
                t_left = max(1, (exp_dt - today_dt).days) / 365.0
                iv_est = max(0.08, min(0.90, vix / 100.0))
                bs_price = black_scholes_price(spot, cc_position["strike"], t_left,
                                               RATE, iv_est, "C")
                ask = (bs_price or 0) * 1.002
            buyback_cost = ask * 100 * cc_position["n_contracts"]
            cc_realized -= buyback_cost
            cc_trade_log.append({
                "entry_date": cc_position["entry_date"], "exit_date": today,
                "expiration": cc_position["expiration"],
                "strike": cc_position["strike"],
                "n_contracts": cc_position["n_contracts"],
                "premium": cc_position["entry_price"],
                "exit_cost": ask,
                "pnl_per_share": cc_position["entry_price"] - ask,
                "total_pnl": (cc_position["entry_price"] - ask) * 100 * cc_position["n_contracts"],
                "exit_reason": "buyback_sma",
            })
            cc_buybacks += 1
            cc_position = None

        if not above_sma and cc_position is None:
            cc_exp, cc_dte = find_best_expiration(
                today, monthly_exps,
                target=CC_DTE_TARGET, dte_min=CC_DTE_MIN, dte_max=CC_DTE_MAX,
            )
            if cc_exp:
                t_years = cc_dte / 365.0
                iv_est = max(0.08, min(0.90, vix / 100.0))
                bs_strike = find_strike_for_delta(spot, t_years, RATE, iv_est,
                                                  CC_DELTA, "C")
                if bs_strike:
                    if cc_exp not in strikes_cache:
                        strikes_cache[cc_exp] = client.get_strikes("QQQ", cc_exp)
                    strikes = strikes_cache.get(cc_exp, [])
                    if strikes:
                        real_strike = min(strikes, key=lambda s: abs(s - bs_strike))
                        ckey = (cc_exp, real_strike)
                        if ckey not in contract_eod:
                            data = client.prefetch_option_life(
                                "QQQ", cc_exp, real_strike, "C", today
                            )
                            contract_eod[ckey] = {r["bar_date"]: r for r in data}
                        eod = contract_eod[ckey].get(today)
                        bid, _ = get_bid_ask(eod)
                        if bid and bid > 0:
                            premium_cash = bid * 100 * n_cc_contracts
                            cc_realized += premium_cash
                            cc_position = {
                                "expiration": cc_exp, "strike": real_strike,
                                "n_contracts": n_cc_contracts,
                                "entry_price": bid, "entry_date": today,
                            }
                            cc_sells += 1

        cc_unrealized = 0.0
        if cc_position:
            ckey = (cc_position["expiration"], cc_position["strike"])
            eod = contract_eod.get(ckey, {}).get(today)
            bid, ask = get_bid_ask(eod)
            if bid and ask:
                mid = (bid + ask) / 2.0
            else:
                exp_dt = datetime.strptime(cc_position["expiration"], "%Y-%m-%d").date()
                today_dt = datetime.strptime(today, "%Y-%m-%d").date()
                t_left = max(1, (exp_dt - today_dt).days) / 365.0
                iv_est = max(0.08, min(0.90, vix / 100.0))
                mid = black_scholes_price(spot, cc_position["strike"], t_left,
                                          RATE, iv_est, "C") or 0
            cc_unrealized = -(mid * 100 * cc_position["n_contracts"])

        daily_cc_values.append((cc_realized, cc_unrealized))

    print(f"\n  CC trades: {len(cc_trade_log)}")
    print(f"  Sells: {cc_sells}  |  Expirations: {cc_expirations}  |  Buybacks: {cc_buybacks}")
    if cc_trade_log:
        wins = [t for t in cc_trade_log if t["total_pnl"] > 0]
        total_pnl = sum(t["total_pnl"] for t in cc_trade_log)
        avg_prem = np.mean([t["premium"] for t in cc_trade_log])
        print(f"  Win rate: {len(wins)/len(cc_trade_log):.0%}  |  "
              f"Total CC P&L: ${total_pnl:+,.0f}  |  Avg premium: ${avg_prem:.2f}")

    return daily_cc_values, cc_trade_log


# ======================================================================
# PORTFOLIO CONSTRUCTION
# ======================================================================

def build_portfolios(snapshots, bil_rets, qqq_by_date, spy_by_date, cc_values):
    dates = [s["date"] for s in snapshots]

    # QQQ B&H ($1M)
    start_price = qqq_by_date[dates[0]]["close"]
    n_shares = int(ALLOC // start_price)
    leftover = ALLOC - n_shares * start_price
    qqq_bh = np.array([n_shares * qqq_by_date[d]["close"] + leftover for d in dates])

    # SPY B&H reference ($1M)
    spy_start = spy_by_date.get(dates[0], {}).get("close", 1)
    spy_bh = np.array([ALLOC * spy_by_date.get(d, {}).get("close", spy_start) / spy_start
                        for d in dates])

    # Options + BIL
    opt_raw = np.array([s["portfolio_value"] for s in snapshots]) * SCALE
    cum_yield = 0.0
    opt_w_bil = []
    for s in snapshots:
        cash = s["cash"]
        r = bil_rets.get(s["date"], 0.0)
        effective = max(0, cash + cum_yield)
        cum_yield += effective * r
        opt_w_bil.append(s["portfolio_value"] + cum_yield)
    opt_w_bil = np.array(opt_w_bil) * SCALE

    # CC contribution
    n_snap = len(dates)
    cc_trimmed = cc_values[-n_snap:]
    cc_contrib = np.array([realized + unrealized for realized, unrealized in cc_trimmed])

    combined_no_cc = qqq_bh + opt_w_bil
    combined_w_cc = qqq_bh + opt_w_bil + cc_contrib

    return {
        "dates": dates, "qqq_bh": qqq_bh, "spy_bh": spy_bh,
        "opt_raw": opt_raw, "opt_w_bil": opt_w_bil,
        "combined_no_cc": combined_no_cc, "combined_w_cc": combined_w_cc,
        "cc_contrib": cc_contrib, "n_shares": n_shares,
    }


# ======================================================================
# METRICS
# ======================================================================

def compute_stats(daily_values, label=""):
    pv = np.array(daily_values, dtype=float)
    n = len(pv)
    years = n / 252.0
    rets = np.diff(pv) / pv[:-1]
    rets = rets[np.isfinite(rets)]
    cagr = (pv[-1] / pv[0]) ** (1 / years) - 1 if years > 0 and pv[0] > 0 else 0
    total_ret = pv[-1] / pv[0] - 1 if pv[0] > 0 else 0
    mean_r = rets.mean()
    std_r = rets.std()
    sharpe = (mean_r / std_r) * np.sqrt(252) if std_r > 0 else 0
    down = rets[rets < 0]
    ds_std = down.std() if len(down) > 0 else 0
    sortino = (mean_r / ds_std) * np.sqrt(252) if ds_std > 0 else 0
    cummax = np.maximum.accumulate(pv)
    dd = pv / cummax - 1
    max_dd = dd.min()
    calmar = cagr / abs(max_dd) if max_dd < 0 else 0
    vol = std_r * np.sqrt(252)
    return {
        "label": label, "start": pv[0], "end": pv[-1],
        "total_ret": total_ret, "cagr": cagr,
        "sharpe": sharpe, "sortino": sortino,
        "max_dd": max_dd, "calmar": calmar, "vol": vol,
    }


# ======================================================================
# OUTPUT
# ======================================================================

def print_results(port, cc_trade_log, opt_trade_log, snapshots, qqq_by_date):
    W = 105
    dates = port["dates"]
    years = len(dates) / 252.0

    print(f"\n\n{'=' * W}")
    print("QQQ COMBINED STRATEGY ANALYSIS")
    print(f"{'=' * W}")
    print(f"  Period:     {dates[0]} to {dates[-1]} ({years:.1f} years)")
    print(f"  Component 1: $1M QQQ B&H ({port['n_shares']:,} shares)")
    print(f"  Component 2: $1M leveraged {DELTA:.0%}-delta calls on QQQ")
    print(f"               (50% PT, {MH}td MH, {SMA_EXIT_THRESHOLD:.0%} SMA exit)")
    print(f"  Cash:        Idle cash in BIL (short-term treasuries)")
    print(f"  Covered calls: {CC_DELTA:.0%}-delta OTM on QQQ, ~{CC_DTE_TARGET}d DTE")
    print(f"                 Active when QQQ < SMA200")

    s_qqq = compute_stats(port["qqq_bh"], "QQQ B&H ($1M)")
    s_spy = compute_stats(port["spy_bh"], "SPY B&H ($1M ref)")
    s_opt = compute_stats(port["opt_w_bil"], "Options + BIL ($1M)")
    s_no_cc = compute_stats(port["combined_no_cc"], "Combined (no CC)")
    s_w_cc = compute_stats(port["combined_w_cc"], "Combined (with CC)")

    print(f"\n{'-' * W}")
    print("COMPONENT BREAKDOWN")
    print(f"{'-' * W}")

    components = [s_qqq, s_spy, s_opt, s_no_cc, s_w_cc]
    print(f"  {'Component':<30} {'Start':>12} {'End':>12} {'CAGR':>8} "
          f"{'Sharpe':>8} {'Sortino':>8} {'Max DD':>8} {'Calmar':>8}")
    print(f"  {'-' * (W - 4)}")
    for s in components:
        print(f"  {s['label']:<30} ${s['start']:>10,.0f} ${s['end']:>10,.0f} "
              f"{s['cagr']:>+7.1%} {s['sharpe']:>8.2f} {s['sortino']:>8.2f} "
              f"{s['max_dd']:>7.1%} {s['calmar']:>8.2f}")

    print(f"\n{'-' * W}")
    print("HEAD-TO-HEAD: WITH vs WITHOUT COVERED CALLS ($2M combined portfolio)")
    print(f"{'-' * W}")

    rows = [
        ("Starting value",   f"${s_no_cc['start']:>12,.0f}", f"${s_w_cc['start']:>12,.0f}"),
        ("Ending value",     f"${s_no_cc['end']:>12,.0f}", f"${s_w_cc['end']:>12,.0f}"),
        ("Total return",     f"{s_no_cc['total_ret']:>+12.1%}", f"{s_w_cc['total_ret']:>+12.1%}"),
        ("CAGR",             f"{s_no_cc['cagr']:>+12.1%}", f"{s_w_cc['cagr']:>+12.1%}"),
        ("Sharpe",           f"{s_no_cc['sharpe']:>12.2f}", f"{s_w_cc['sharpe']:>12.2f}"),
        ("Sortino",          f"{s_no_cc['sortino']:>12.2f}", f"{s_w_cc['sortino']:>12.2f}"),
        ("Max drawdown",     f"{s_no_cc['max_dd']:>12.1%}", f"{s_w_cc['max_dd']:>12.1%}"),
        ("Calmar",           f"{s_no_cc['calmar']:>12.2f}", f"{s_w_cc['calmar']:>12.2f}"),
        ("Annual vol",       f"{s_no_cc['vol']:>12.1%}", f"{s_w_cc['vol']:>12.1%}"),
    ]

    print(f"  {'Metric':<25} {'Without CC':>20} {'With CC':>20}")
    print(f"  {'-' * 80}")
    for name, va, vb in rows:
        print(f"  {name:<25} {va:>20} {vb:>20}")

    print(f"\n  Impact of Covered Calls:")
    print(f"    Sharpe:  {s_no_cc['sharpe']:.2f} -> {s_w_cc['sharpe']:.2f} ({s_w_cc['sharpe'] - s_no_cc['sharpe']:+.2f})")
    print(f"    Sortino: {s_no_cc['sortino']:.2f} -> {s_w_cc['sortino']:.2f} ({s_w_cc['sortino'] - s_no_cc['sortino']:+.2f})")
    print(f"    Max DD:  {s_no_cc['max_dd']:.1%} -> {s_w_cc['max_dd']:.1%} ({abs(s_no_cc['max_dd']) - abs(s_w_cc['max_dd']):+.1%} pts)")
    print(f"    CAGR:    {s_no_cc['cagr']:+.1%} -> {s_w_cc['cagr']:+.1%} ({s_w_cc['cagr'] - s_no_cc['cagr']:+.1%})")

    if cc_trade_log:
        print(f"\n{'-' * W}")
        print("COVERED CALL TRADE STATISTICS")
        print(f"{'-' * W}")
        tdf = pd.DataFrame(cc_trade_log)
        wins = tdf[tdf["total_pnl"] > 0]
        expired_otm = tdf[tdf["exit_reason"] == "expired_otm"]
        expired_itm = tdf[tdf["exit_reason"] == "expired_itm"]
        buybacks = tdf[tdf["exit_reason"] == "buyback_sma"]

        print(f"  Total CC trades:      {len(tdf)}")
        print(f"  Win rate:             {len(wins)/len(tdf):.0%}")
        print(f"  Total CC P&L:         ${tdf['total_pnl'].sum():+,.0f}")
        print(f"  Avg premium received: ${tdf['premium'].mean():.2f}/share")
        print(f"  Avg P&L per trade:    ${tdf['total_pnl'].mean():+,.0f}")
        print(f"")
        print(f"  Expired OTM (full premium): {len(expired_otm)}")
        print(f"  Expired ITM (called away):  {len(expired_itm)}")
        print(f"  Bought back (SMA recovery): {len(buybacks)}")
        if len(buybacks) > 0:
            print(f"    Avg buyback P&L:    ${buybacks['total_pnl'].mean():+,.0f}")

    if opt_trade_log:
        print(f"\n{'-' * W}")
        print("OPTIONS STRATEGY TRADE STATISTICS")
        print(f"{'-' * W}")
        odf = pd.DataFrame(opt_trade_log)
        owins = odf[odf["pnl_pct"] > 0]
        print(f"  Total trades:   {len(odf)}")
        print(f"  Win rate:       {len(owins)/len(odf):.0%}")
        print(f"  Total P&L:      ${odf['pnl_dollar'].sum():+,.0f} (at $100K scale)")
        print(f"  PT exits:       {len(odf[odf['exit_reason'] == 'PT'])}")
        print(f"  MH exits:       {len(odf[odf['exit_reason'] == 'MH'])}")
        print(f"  SMA exits:      {len(odf[odf['exit_reason'] == 'SMA'])}")
        print(f"  Avg days held:  {odf['days_held'].mean():.0f}")

    # Year-by-year
    print(f"\n{'-' * W}")
    print("YEAR-BY-YEAR COMPARISON")
    print(f"{'-' * W}")

    df_dates = pd.to_datetime(dates)
    years_list = sorted(set(df_dates.year))

    print(f"\n  {'Year':<6} {'--- Without CC ---':^36} {'--- With CC ---':^36} {'QQQ B&H':>8}")
    print(f"  {'':6} {'Return':>8} {'Sharpe':>8} {'Max DD':>8} {'End Val':>12}"
          f" {'Return':>8} {'Sharpe':>8} {'Max DD':>8} {'End Val':>12} {'':>8}")
    print(f"  {'-' * 100}")

    for yr in years_list:
        mask = df_dates.year == yr
        idx = np.where(mask)[0]
        if len(idx) == 0:
            continue
        nc_yr = port["combined_no_cc"][idx]
        wc_yr = port["combined_w_cc"][idx]
        qqq_yr = port["qqq_bh"][idx]

        nc_ret = nc_yr[-1] / nc_yr[0] - 1
        wc_ret = wc_yr[-1] / wc_yr[0] - 1
        qqq_ret = qqq_yr[-1] / qqq_yr[0] - 1
        nc_dd = (nc_yr / np.maximum.accumulate(nc_yr) - 1).min()
        wc_dd = (wc_yr / np.maximum.accumulate(wc_yr) - 1).min()
        nc_rets = np.diff(nc_yr) / nc_yr[:-1] if len(nc_yr) > 1 else [0]
        wc_rets = np.diff(wc_yr) / wc_yr[:-1] if len(wc_yr) > 1 else [0]
        nc_sh = (np.mean(nc_rets) / np.std(nc_rets) * np.sqrt(252)) if np.std(nc_rets) > 0 else 0
        wc_sh = (np.mean(wc_rets) / np.std(wc_rets) * np.sqrt(252)) if np.std(wc_rets) > 0 else 0

        print(f"  {yr:<6} {nc_ret:>+7.1%} {nc_sh:>8.2f} {nc_dd:>7.1%} ${nc_yr[-1]:>10,.0f}"
              f" {wc_ret:>+7.1%} {wc_sh:>8.2f} {wc_dd:>7.1%} ${wc_yr[-1]:>10,.0f}"
              f" {qqq_ret:>+7.1%}")

    # Below-SMA periods
    print(f"\n{'-' * W}")
    print("BELOW-SMA200 PERIODS & COVERED CALL ACTIVITY")
    print(f"{'-' * W}")

    in_below = False
    periods = []
    start_d = None
    for s in snapshots:
        if not s["above_sma"] and not in_below:
            in_below = True
            start_d = s["date"]
        elif s["above_sma"] and in_below:
            in_below = False
            periods.append((start_d, s["date"]))
    if in_below:
        periods.append((start_d, snapshots[-1]["date"]))

    sig_periods = [(s, e) for s, e in periods
                   if len([x for x in snapshots if s <= x["date"] <= e]) > 10]

    if sig_periods and cc_trade_log:
        tdf = pd.DataFrame(cc_trade_log)
        print(f"\n  {'Period':<30} {'Days':>6} {'CC Trades':>10} {'CC P&L':>12} {'QQQ Move':>10}")
        print(f"  {'-' * 75}")
        for s, e in sig_periods:
            days_in = len([x for x in snapshots if s <= x["date"] <= e])
            qqq_s = qqq_by_date.get(s, {}).get("close", 0)
            qqq_e = qqq_by_date.get(e, {}).get("close", qqq_s)
            qqq_move = qqq_e / qqq_s - 1 if qqq_s > 0 else 0
            cc_in = tdf[(tdf["entry_date"] >= s) & (tdf["entry_date"] <= e)]
            cc_pnl = cc_in["total_pnl"].sum() if len(cc_in) > 0 else 0
            n_cc = len(cc_in)
            print(f"  {s} to {e:<14} {days_in:>6} {n_cc:>10} ${cc_pnl:>+10,.0f} {qqq_move:>+9.1%}")

    # Summary
    print(f"\n{'=' * W}")
    print("SUMMARY")
    print(f"{'=' * W}")

    print(f"\n  Without Covered Calls ($2M combined):")
    print(f"    ${s_no_cc['start']:,.0f} -> ${s_no_cc['end']:,.0f}  |  "
          f"CAGR: {s_no_cc['cagr']:+.1%}  |  Sharpe: {s_no_cc['sharpe']:.2f}  |  "
          f"Sortino: {s_no_cc['sortino']:.2f}  |  Max DD: {s_no_cc['max_dd']:.1%}")

    print(f"\n  With Covered Calls ($2M combined):")
    print(f"    ${s_w_cc['start']:,.0f} -> ${s_w_cc['end']:,.0f}  |  "
          f"CAGR: {s_w_cc['cagr']:+.1%}  |  Sharpe: {s_w_cc['sharpe']:.2f}  |  "
          f"Sortino: {s_w_cc['sortino']:.2f}  |  Max DD: {s_w_cc['max_dd']:.1%}")

    cc_total_pnl = sum(t["total_pnl"] for t in cc_trade_log) if cc_trade_log else 0
    print(f"\n  Covered Call total P&L: ${cc_total_pnl:+,.0f}")
    print(f"  CC added {s_w_cc['cagr'] - s_no_cc['cagr']:+.1%} CAGR, "
          f"{s_w_cc['sharpe'] - s_no_cc['sharpe']:+.02f} Sharpe")

    print(f"\n  QQQ B&H reference: ${s_qqq['start']:,.0f} -> ${s_qqq['end']:,.0f}, "
          f"CAGR: {s_qqq['cagr']:+.1%}, Sharpe: {s_qqq['sharpe']:.2f}")
    print(f"  SPY B&H reference: ${s_spy['start']:,.0f} -> ${s_spy['end']:,.0f}, "
          f"CAGR: {s_spy['cagr']:+.1%}, Sharpe: {s_spy['sharpe']:.2f}")
    print(f"{'=' * W}")


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
    print("QQQ Combined Strategy: B&H + Leveraged Options + Covered Calls")
    print("=" * W)

    print("\nLoading ETF data...")
    bil_rets = fetch_etf_returns("BIL", DATA_START, DATA_END)
    if not bil_rets:
        print("ERROR: Could not fetch BIL data.")
        return

    client = ThetaDataClient()
    if not client.connect():
        print("\nERROR: Cannot connect to Theta Terminal.")
        return
    print("\nConnected to Theta Terminal.")

    qqq_by_date, spy_by_date, trading_dates, vix_data, sma200, monthly_exps = load_all_data(client)

    sim_start_idx = next((i for i, d in enumerate(trading_dates) if d >= SIM_START), 0)
    sim_start_date = trading_dates[sim_start_idx]
    start_price = qqq_by_date[sim_start_date]["close"]
    n_shares = int(ALLOC // start_price)
    n_cc_contracts = n_shares // 100
    print(f"\n  QQQ at sim start ({sim_start_date}): ${start_price:.2f}")
    print(f"  $1M -> {n_shares:,} shares -> {n_cc_contracts} CC contracts")

    snapshots, opt_trade_log = run_options_sim(
        client, qqq_by_date, trading_dates, vix_data, sma200, monthly_exps,
    )

    cc_values, cc_trade_log = run_cc_overlay(
        client, qqq_by_date, trading_dates, vix_data, sma200, monthly_exps,
        n_cc_contracts, SIM_START,
    )

    if not snapshots:
        print("\nInsufficient data.")
        client.close()
        return

    port = build_portfolios(snapshots, bil_rets, qqq_by_date, spy_by_date, cc_values)
    print_results(port, cc_trade_log, opt_trade_log, snapshots, qqq_by_date)

    client.close()


if __name__ == "__main__":
    main()
