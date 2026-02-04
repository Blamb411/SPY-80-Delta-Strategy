"""
SPY 80-Delta Call Deployment Simulator
=======================================
Models actual deployment of the systematic SPY call strategy with
real ThetaData bid/ask pricing:

  - $100K starting capital, no margin
  - Buy 1 contract per day when SPY > 200 SMA
  - 80-delta call, ~120 DTE (monthly expiration)
  - Exit: +50% profit target or 60 trading day max hold
  - No stop-loss
  - Capital recycled T+1 (next trading day)
  - Buy at ask, sell at bid (conservative execution)

Compares two SMA200 modes:
  A) Entry-only filter: stop new entries below SMA, let positions run
  B) Threshold exit: force-exit all positions when SPY drops >2% below SMA200

Requires:
  - Theta Terminal v3 running locally
  - thetadata_cache.db (auto-populated)

Usage:
    python spy_deployment_sim.py
"""

import os
import sys
import math
import logging
from datetime import datetime, date, timedelta
from collections import defaultdict, Counter

import numpy as np
import pandas as pd

_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_dir = os.path.dirname(os.path.dirname(_this_dir))
sys.path.insert(0, _project_dir)

from backtest.thetadata_client import ThetaDataClient
from backtest.black_scholes import black_scholes_price, find_strike_for_delta

log = logging.getLogger("spy_deploy")

# ======================================================================
# PARAMETERS
# ======================================================================

CAPITAL = 100_000
DELTA = 0.80
DTE_TARGET = 120       # calendar days
DTE_MIN = 90
DTE_MAX = 150
MH = 60                # max hold in trading days
PT = 0.50              # +50% profit target
RATE = 0.04            # risk-free rate for B-S
SMA_EXIT_THRESHOLD = 0.02  # force-exit when SPY >2% below SMA200

# Liquidity filters (set to None to disable)
MIN_OPEN_INTEREST = None   # e.g., 500 to require OI >= 500
MAX_SPREAD_PCT = None      # e.g., 0.02 to require spread <= 2% of mid
MAX_SPREAD_ABS = None      # e.g., 1.00 to require spread <= $1.00

# Restrict to years with solid ThetaData coverage
DATA_START = "2014-01-01"
DATA_END = "2026-01-31"
SIM_START = "2015-03-01"   # first usable date (expirations start 2015-02-20)


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


def check_liquidity(eod_row, min_oi=None, max_spread_pct=None, max_spread_abs=None):
    """
    Check if option meets liquidity requirements.

    Returns: (passes: bool, reason: str or None)
    """
    if eod_row is None:
        return False, "no_data"

    bid = eod_row.get("bid", 0) or 0
    ask = eod_row.get("ask", 0) or 0
    oi = eod_row.get("open_interest", 0) or 0

    if bid <= 0 or ask <= 0:
        return False, "no_quote"

    # Check open interest
    if min_oi is not None and oi < min_oi:
        return False, f"low_oi_{int(oi)}"

    # Check spread
    mid = (bid + ask) / 2.0
    spread = ask - bid
    spread_pct = spread / mid if mid > 0 else 1.0

    if max_spread_pct is not None and spread_pct > max_spread_pct:
        return False, f"wide_spread_{spread_pct:.1%}"

    if max_spread_abs is not None and spread > max_spread_abs:
        return False, f"wide_spread_${spread:.2f}"

    return True, None


# ======================================================================
# DATA LOADING
# ======================================================================

def load_all_data(client):
    print("Loading SPY bars...")
    spy_bars = client.fetch_spy_bars(DATA_START, DATA_END)
    spy_by_date = {b["bar_date"]: b for b in spy_bars}
    trading_dates = sorted(spy_by_date.keys())

    print("Loading VIX history...")
    vix_data = client.fetch_vix_history(DATA_START, DATA_END)

    # SMA200
    sma200 = {}
    for i in range(199, len(trading_dates)):
        window = [spy_by_date[trading_dates[j]]["close"] for j in range(i - 199, i + 1)]
        sma200[trading_dates[i]] = sum(window) / 200.0

    print("Loading SPY expirations...")
    all_exps = client.get_expirations("SPY")
    monthly_exps = [(e, datetime.strptime(e, "%Y-%m-%d").date())
                    for e in all_exps if is_monthly_opex(e)]
    monthly_exps.sort(key=lambda x: x[1])

    print(f"  SPY bars: {len(spy_bars)} ({trading_dates[0]} to {trading_dates[-1]})")
    print(f"  VIX days: {len(vix_data)}")
    first_sma = sorted(sma200.keys())[0] if sma200 else "N/A"
    print(f"  SMA200 from: {first_sma}")
    print(f"  Monthly expirations: {len(monthly_exps)} "
          f"({monthly_exps[0][0]} to {monthly_exps[-1][0]})")

    return spy_by_date, trading_dates, vix_data, sma200, monthly_exps


# ======================================================================
# SIMULATION ENGINE
# ======================================================================

def run_simulation(client, spy_by_date, trading_dates, vix_data, sma200,
                   monthly_exps, force_exit_below_sma=False, label=""):
    """
    Daily portfolio simulation.

    force_exit_below_sma: if True, force-exit all positions when SPY drops
                          more than SMA_EXIT_THRESHOLD (2%) below SMA200.
    """
    available_cash = float(CAPITAL)
    pending_cash = 0.0
    positions = []

    contract_eod = {}
    strikes_cache = {}

    daily_snapshots = []
    trade_log = []
    entry_skip_reasons = defaultdict(int)
    force_exit_count = 0

    # Start from SIM_START
    start_idx = next((i for i, d in enumerate(trading_dates) if d >= SIM_START), 0)

    mode_label = f"thresh-exit (>{SMA_EXIT_THRESHOLD:.0%} below SMA)" if force_exit_below_sma else "entry-only"
    print(f"\n{'='*70}")
    print(f"Config: {label or mode_label}")
    print(f"  SMA200 mode: {mode_label}")
    print(f"  Period: {trading_dates[start_idx]} to {trading_dates[-1]}")
    print(f"{'='*70}")

    for day_idx in range(start_idx, len(trading_dates)):
        today = trading_dates[day_idx]
        bar = spy_by_date[today]
        spot = bar["close"]
        sma_val = sma200.get(today)
        above_sma = (spot > sma_val) if sma_val else False
        vix = vix_data.get(today, 20.0)

        # 1. Settle yesterday's exit proceeds
        available_cash += pending_cash
        pending_cash = 0.0

        # 2a. Force-exit all positions when SPY >2% below SMA200
        pct_below_sma = (sma_val - spot) / sma_val if sma_val and sma_val > 0 else 0
        if force_exit_below_sma and pct_below_sma >= SMA_EXIT_THRESHOLD and positions:
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
            if pnl_pct >= PT:
                exit_reason = "PT"
            elif pos["days_held"] >= MH:
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
                        strikes_cache[best_exp] = client.get_strikes("SPY", best_exp)
                    strikes = strikes_cache[best_exp]
                    if not strikes:
                        entry_skip_reasons["no_strikes"] += 1
                    else:
                        real_strike = min(strikes, key=lambda s: abs(s - bs_strike))
                        ckey = (best_exp, real_strike)
                        if ckey not in contract_eod:
                            data = client.prefetch_option_life(
                                "SPY", best_exp, real_strike, "C", today
                            )
                            contract_eod[ckey] = {r["bar_date"]: r for r in data}
                        eod = contract_eod[ckey].get(today)

                        # Liquidity check (if filters enabled)
                        liq_ok, liq_reason = check_liquidity(
                            eod,
                            min_oi=MIN_OPEN_INTEREST,
                            max_spread_pct=MAX_SPREAD_PCT,
                            max_spread_abs=MAX_SPREAD_ABS
                        )
                        if not liq_ok:
                            entry_skip_reasons[f"liquidity_{liq_reason}"] += 1
                        else:
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
            "leverage": notional_exposure / portfolio_value if portfolio_value > 0 else 0,
            "above_sma": above_sma,
            "spy_close": spot,
            "entered": entered,
        })

        # Progress
        real_idx = day_idx - start_idx
        total_days = len(trading_dates) - start_idx
        if (real_idx + 1) % 500 == 0 or real_idx == 0:
            print(f"  [{real_idx+1}/{total_days}] {today}  "
                  f"Portfolio=${portfolio_value:,.0f}  "
                  f"Pos={len(positions)}  Cash=${available_cash + pending_cash:,.0f}")

    print(f"\n  Trades: {len(trade_log)}  |  Force-exits: {force_exit_count}")
    print(f"  Entry skips: {dict(entry_skip_reasons)}")
    print(f"  Unique contracts: {len(contract_eod)}")

    return daily_snapshots, trade_log


# ======================================================================
# ANALYSIS
# ======================================================================

def compute_metrics(snapshots, trade_log, label=""):
    """Compute portfolio metrics. Returns dict."""
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
    sharpe = (daily_mean / daily_std) * np.sqrt(252) if daily_std > 0 else 0

    downside = df["daily_ret"][df["daily_ret"] < 0]
    ds_std = downside.std() if len(downside) > 0 else 0
    sortino = (daily_mean / ds_std) * np.sqrt(252) if ds_std > 0 else 0

    cummax = df["portfolio_value"].cummax()
    drawdown = df["portfolio_value"] / cummax - 1
    max_dd = drawdown.min()

    # SPY B&H
    spy_start = df["spy_close"].iloc[0]
    spy_end = df["spy_close"].iloc[-1]
    spy_total = spy_end / spy_start - 1
    spy_cagr = (spy_end / spy_start) ** (1 / years) - 1 if years > 0 else 0
    df["spy_ret"] = df["spy_close"].pct_change().fillna(0)
    spy_sharpe = (df["spy_ret"].mean() / df["spy_ret"].std()) * np.sqrt(252) if df["spy_ret"].std() > 0 else 0
    spy_dd = (df["spy_close"] / df["spy_close"].cummax() - 1).min()

    # Position stats
    n_pos = df["n_positions"].values
    lev = df["leverage"].values
    lev_active = lev[lev > 0]

    # Trade stats
    trade_stats = {}
    if len(tdf) > 0:
        wins = tdf[tdf["pnl_pct"] > 0]
        losses = tdf[tdf["pnl_pct"] <= 0]
        trade_stats = {
            "n_trades": len(tdf),
            "win_rate": len(wins) / len(tdf),
            "mean_ret": tdf["pnl_pct"].mean(),
            "med_ret": tdf["pnl_pct"].median(),
            "avg_win": wins["pnl_pct"].mean() if len(wins) > 0 else 0,
            "avg_loss": losses["pnl_pct"].mean() if len(losses) > 0 else 0,
            "total_pnl": tdf["pnl_dollar"].sum(),
            "pt_exits": len(tdf[tdf["exit_reason"] == "PT"]),
            "mh_exits": len(tdf[tdf["exit_reason"] == "MH"]),
            "sma_exits": len(tdf[tdf["exit_reason"] == "SMA"]),
            "avg_days": tdf["days_held"].mean(),
            "avg_cost": tdf["contract_cost"].mean(),
        }

    # Yearly
    df["year"] = pd.to_datetime(df["date"]).dt.year
    yearly = {}
    for year in sorted(df["year"].unique()):
        ydf = df[df["year"] == year]
        y_start = ydf["portfolio_value"].iloc[0]
        y_end = ydf["portfolio_value"].iloc[-1]
        y_ret = y_end / y_start - 1
        spy_y_start = ydf["spy_close"].iloc[0]
        spy_y_end = ydf["spy_close"].iloc[-1]
        spy_y_ret = spy_y_end / spy_y_start - 1

        y_trades = 0
        if len(tdf) > 0:
            y_trades = len(tdf[pd.to_datetime(tdf["exit_date"]).dt.year == year])

        yearly[year] = {
            "ret": y_ret, "spy_ret": spy_y_ret, "alpha": y_ret - spy_y_ret,
            "trades": y_trades, "avg_pos": ydf["n_positions"].mean(),
            "max_pos": ydf["n_positions"].max(), "end_val": y_end,
        }

    return {
        "label": label,
        "years": years,
        "start_val": start_val, "end_val": end_val,
        "total_return": total_return, "cagr": cagr,
        "sharpe": sharpe, "sortino": sortino, "max_dd": max_dd,
        "spy_total": spy_total, "spy_cagr": spy_cagr,
        "spy_sharpe": spy_sharpe, "spy_dd": spy_dd,
        "avg_pos": n_pos.mean(), "med_pos": np.median(n_pos),
        "max_pos": n_pos.max(),
        "avg_lev": lev_active.mean() if len(lev_active) > 0 else 0,
        "max_lev": lev_active.max() if len(lev_active) > 0 else 0,
        "trades": trade_stats,
        "yearly": yearly,
        "snapshots_df": df,
        "trade_df": tdf,
    }


# ======================================================================
# OUTPUT
# ======================================================================

def print_comparison(m_a, m_b):
    """Side-by-side comparison of two simulation runs."""

    W = 95
    label_a = m_a["label"]
    label_b = m_b["label"]

    print(f"\n{'=' * W}")
    print("SPY 80-DELTA CALL DEPLOYMENT SIMULATION -- COMPARISON")
    print(f"{'=' * W}")
    print(f"  Period:    {SIM_START} to {DATA_END}")
    print(f"  Capital:   ${CAPITAL:,} (no margin)")
    print(f"  Strategy:  Buy 1 contract/day when SPY > SMA200")
    print(f"  Options:   {DELTA:.0%}-delta call, ~{DTE_TARGET} DTE, monthly expiry")
    print(f"  Rules:     PT=+{PT:.0%}, MH={MH}td, no stop-loss")
    print(f"  Execution: buy at ask, sell at bid, T+1 settlement")
    print(f"\n  A = {label_a}")
    print(f"  B = {label_b}")

    # -- Portfolio --
    print(f"\n{'-' * W}")
    print("PORTFOLIO PERFORMANCE")
    print(f"{'-' * W}")

    rows = [
        ("Ending value",    f"${m_a['end_val']:>12,.0f}", f"${m_b['end_val']:>12,.0f}"),
        ("Total return",    f"{m_a['total_return']:>+12.1%}", f"{m_b['total_return']:>+12.1%}"),
        ("CAGR",            f"{m_a['cagr']:>+12.1%}", f"{m_b['cagr']:>+12.1%}"),
        ("Sharpe",          f"{m_a['sharpe']:>12.2f}", f"{m_b['sharpe']:>12.2f}"),
        ("Sortino",         f"{m_a['sortino']:>12.2f}", f"{m_b['sortino']:>12.2f}"),
        ("Max drawdown",    f"{m_a['max_dd']:>12.1%}", f"{m_b['max_dd']:>12.1%}"),
        ("SPY CAGR (ref)",  f"{m_a['spy_cagr']:>+12.1%}", f"{m_b['spy_cagr']:>+12.1%}"),
        ("SPY Sharpe (ref)",f"{m_a['spy_sharpe']:>12.2f}", f"{m_b['spy_sharpe']:>12.2f}"),
    ]

    print(f"  {'Metric':<25} {label_a:>20} {label_b:>20}")
    print(f"  {'-' * 67}")
    for name, va, vb in rows:
        print(f"  {name:<25} {va:>20} {vb:>20}")

    # -- Trade Stats --
    print(f"\n{'-' * W}")
    print("TRADE STATISTICS")
    print(f"{'-' * W}")

    ta = m_a["trades"]
    tb = m_b["trades"]
    if ta and tb:
        trade_rows = [
            ("Total trades",    f"{ta['n_trades']}", f"{tb['n_trades']}"),
            ("Win rate",        f"{ta['win_rate']:.1%}", f"{tb['win_rate']:.1%}"),
            ("Mean return",     f"{ta['mean_ret']:+.1%}", f"{tb['mean_ret']:+.1%}"),
            ("Median return",   f"{ta['med_ret']:+.1%}", f"{tb['med_ret']:+.1%}"),
            ("Avg win",         f"{ta['avg_win']:+.1%}", f"{tb['avg_win']:+.1%}"),
            ("Avg loss",        f"{ta['avg_loss']:+.1%}", f"{tb['avg_loss']:+.1%}"),
            ("Total P&L",       f"${ta['total_pnl']:+,.0f}", f"${tb['total_pnl']:+,.0f}"),
            ("PT exits",        f"{ta['pt_exits']} ({ta['pt_exits']/ta['n_trades']:.0%})",
                                f"{tb['pt_exits']} ({tb['pt_exits']/tb['n_trades']:.0%})"),
            ("MH exits",        f"{ta['mh_exits']} ({ta['mh_exits']/ta['n_trades']:.0%})",
                                f"{tb['mh_exits']} ({tb['mh_exits']/tb['n_trades']:.0%})"),
            ("SMA exits",       f"{ta['sma_exits']}", f"{tb['sma_exits']}"),
            ("Avg days held",   f"{ta['avg_days']:.0f}", f"{tb['avg_days']:.0f}"),
            ("Avg contract $",  f"${ta['avg_cost']:,.0f}", f"${tb['avg_cost']:,.0f}"),
        ]

        print(f"  {'Metric':<25} {label_a:>20} {label_b:>20}")
        print(f"  {'-' * 67}")
        for name, va, vb in trade_rows:
            print(f"  {name:<25} {va:>20} {vb:>20}")

    # -- Position Stats --
    print(f"\n{'-' * W}")
    print("POSITION & LEVERAGE")
    print(f"{'-' * W}")
    pos_rows = [
        ("Avg positions",   f"{m_a['avg_pos']:.1f}", f"{m_b['avg_pos']:.1f}"),
        ("Max positions",   f"{m_a['max_pos']}", f"{m_b['max_pos']}"),
        ("Avg leverage",    f"{m_a['avg_lev']:.1f}x", f"{m_b['avg_lev']:.1f}x"),
        ("Max leverage",    f"{m_a['max_lev']:.1f}x", f"{m_b['max_lev']:.1f}x"),
    ]
    print(f"  {'Metric':<25} {label_a:>20} {label_b:>20}")
    print(f"  {'-' * 67}")
    for name, va, vb in pos_rows:
        print(f"  {name:<25} {va:>20} {vb:>20}")

    # -- Year-by-Year --
    print(f"\n{'-' * W}")
    print("YEAR-BY-YEAR COMPARISON")
    print(f"{'-' * W}")

    all_years = sorted(set(list(m_a["yearly"].keys()) + list(m_b["yearly"].keys())))

    hdr_a = f"--- A: Entry-only ---"
    hdr_b = f"--- B: Thresh-exit ---"
    print(f"\n  {'Year':<6}  "
          f"{hdr_a:^30}  "
          f"{hdr_b:^30}  {'SPY':>7}")
    print(f"  {'':6}  {'Return':>8} {'Alpha':>8} {'Trades':>7} {'MaxPos':>7}  "
          f"{'Return':>8} {'Alpha':>8} {'Trades':>7} {'MaxPos':>7}  {'':>7}")
    print(f"  {'-' * 88}")

    for year in all_years:
        ya = m_a["yearly"].get(year, {})
        yb = m_b["yearly"].get(year, {})

        ra = ya.get("ret", 0)
        aa = ya.get("alpha", 0)
        ta_n = ya.get("trades", 0)
        ma = ya.get("max_pos", 0)

        rb = yb.get("ret", 0)
        ab = yb.get("alpha", 0)
        tb_n = yb.get("trades", 0)
        mb = yb.get("max_pos", 0)

        spy_r = ya.get("spy_ret", yb.get("spy_ret", 0))

        print(f"  {year:<6}  {ra:>+7.1%} {aa:>+7.1%} {ta_n:>7} {ma:>7}  "
              f"{rb:>+7.1%} {ab:>+7.1%} {tb_n:>7} {mb:>7}  {spy_r:>+6.1%}")

    # -- Monthly Returns for both --
    for m, lbl in [(m_a, label_a), (m_b, label_b)]:
        df = m["snapshots_df"]
        df["month"] = pd.to_datetime(df["date"]).dt.to_period("M")
        monthly = df.groupby("month").agg(
            start_val=("portfolio_value", "first"),
            end_val=("portfolio_value", "last"),
        )
        monthly["return"] = monthly["end_val"] / monthly["start_val"] - 1
        mdf = monthly.reset_index()
        mdf["year"] = mdf["month"].dt.year
        mdf["mon"] = mdf["month"].dt.month

        month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                       "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

        print(f"\n{'-' * W}")
        print(f"MONTHLY RETURNS: {lbl}")
        print(f"{'-' * W}")
        print(f"\n  {'Year':<6}", end="")
        for mn in month_names:
            print(f" {mn:>6}", end="")
        print(f" {'Total':>8}")
        print(f"  {'-' * 85}")

        for year in sorted(mdf["year"].unique()):
            ydata = mdf[mdf["year"] == year]
            print(f"  {year:<6}", end="")
            ytot = 1.0
            for mo in range(1, 13):
                mrow = ydata[ydata["mon"] == mo]
                if len(mrow) > 0:
                    r = mrow["return"].iloc[0]
                    ytot *= (1 + r)
                    print(f" {r:>+5.1%}", end="")
                else:
                    print(f" {'--':>6}", end="")
            print(f" {ytot - 1:>+7.1%}")

    # -- Drawdown detail --
    print(f"\n{'-' * W}")
    print("DRAWDOWN DETAIL")
    print(f"{'-' * W}")

    for m, lbl in [(m_a, label_a), (m_b, label_b)]:
        df = m["snapshots_df"]
        cummax = df["portfolio_value"].cummax()
        dd = df["portfolio_value"] / cummax - 1

        # Find top 3 drawdowns
        in_dd = False
        dd_periods = []
        peak_idx = 0
        for i in range(len(dd)):
            if dd.iloc[i] < -0.05 and not in_dd:
                in_dd = True
                peak_idx = i - 1 if i > 0 else 0
            elif dd.iloc[i] >= 0 and in_dd:
                trough_idx = dd.iloc[peak_idx:i].idxmin()
                dd_periods.append({
                    "peak": df["date"].iloc[peak_idx],
                    "trough": df["date"].iloc[trough_idx],
                    "recovery": df["date"].iloc[i],
                    "depth": dd.iloc[trough_idx],
                })
                in_dd = False

        # Still in drawdown at end
        if in_dd:
            trough_idx = dd.iloc[peak_idx:].idxmin()
            dd_periods.append({
                "peak": df["date"].iloc[peak_idx],
                "trough": df["date"].iloc[trough_idx],
                "recovery": "(ongoing)",
                "depth": dd.iloc[trough_idx],
            })

        dd_periods.sort(key=lambda x: x["depth"])
        print(f"\n  {lbl} -- top drawdowns:")
        for j, ddp in enumerate(dd_periods[:5]):
            print(f"    {j+1}. {ddp['depth']:+.1%}  "
                  f"({ddp['peak']} -> {ddp['trough']} -> {ddp['recovery']})")

    # -- Summary --
    print(f"\n{'=' * W}")
    print("SUMMARY")
    print(f"{'=' * W}")

    for m in [m_a, m_b]:
        t = m["trades"]
        trades_str = ""
        if t:
            trades_str = (f"  Trades: {t['n_trades']}  |  Win: {t['win_rate']:.0%}  |  "
                          f"SMA exits: {t['sma_exits']}")
        print(f"  {m['label']}:")
        print(f"    ${CAPITAL:,} -> ${m['end_val']:,.0f}  |  "
              f"CAGR: {m['cagr']:+.1%}  |  Sharpe: {m['sharpe']:.2f}  |  "
              f"Max DD: {m['max_dd']:.1%}")
        print(f"  {trades_str}")

    print(f"\n  SPY B&H: CAGR {m_a['spy_cagr']:+.1%}  |  "
          f"Sharpe: {m_a['spy_sharpe']:.2f}  |  Max DD: {m_a['spy_dd']:.1%}")
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

    print("=" * 80)
    print("SPY 80-Delta Call -- Deployment Simulation (v3)")
    print("=" * 80)

    client = ThetaDataClient()
    if not client.connect():
        print("\nERROR: Cannot connect to Theta Terminal.")
        return

    print("Connected to Theta Terminal.\n")

    spy_by_date, trading_dates, vix_data, sma200, monthly_exps = load_all_data(client)

    # Run A: entry-only filter (current approach)
    snaps_a, trades_a = run_simulation(
        client, spy_by_date, trading_dates, vix_data, sma200, monthly_exps,
        force_exit_below_sma=False,
        label="A: Entry-only SMA filter",
    )

    # Run B: threshold exit when SPY >2% below SMA200
    snaps_b, trades_b = run_simulation(
        client, spy_by_date, trading_dates, vix_data, sma200, monthly_exps,
        force_exit_below_sma=True,
        label="B: Thresh-exit (>2% below SMA)",
    )

    if not snaps_a or not snaps_b:
        print("\nInsufficient data.")
        return

    m_a = compute_metrics(snaps_a, trades_a, "A: Entry-only SMA filter")
    m_b = compute_metrics(snaps_b, trades_b, "B: Thresh-exit (>2% below SMA)")

    print_comparison(m_a, m_b)

    client.close()


if __name__ == "__main__":
    main()
