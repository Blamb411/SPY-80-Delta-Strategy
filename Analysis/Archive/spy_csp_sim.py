"""
SPY Cash-Secured Put Simulator
================================
Models actual deployment of a systematic SPY cash-secured put (CSP)
strategy with real ThetaData bid/ask pricing:

  - $200K starting capital, no margin
  - Sell 1 put per day when SPY > 200 SMA
  - 25-delta OTM put, ~37 DTE (monthly expiration)
  - Exit: 50% profit target, 30 trading day max hold, or near-expiry
  - No stop-loss
  - Capital recycled T+1 (next trading day)
  - Sell at bid, buy to close at ask (conservative execution)

Test grid: 3 deltas (-0.20, -0.25, -0.30) x 2 SMA modes = 6 configs
  A) Entry-only filter: stop new entries below SMA, let positions run
  B) Threshold exit: force-exit all positions when SPY drops >2% below SMA200

Requires:
  - Theta Terminal v3 running locally
  - thetadata_cache.db (auto-populated)

Usage:
    python "Seeking Alpha Backtests/spy_csp_sim.py"
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
_project_dir = os.path.dirname(_this_dir)
sys.path.insert(0, _project_dir)

from backtest.thetadata_client import ThetaDataClient
from backtest.black_scholes import black_scholes_price, find_strike_for_delta

log = logging.getLogger("spy_csp")

# ======================================================================
# PARAMETERS
# ======================================================================

CAPITAL = 200_000
DELTA_TARGET = -0.25       # 25-delta OTM put (negative for puts)
DTE_TARGET = 37            # calendar days
DTE_MIN = 25
DTE_MAX = 50
MH = 30                    # max hold in trading days
PT = 0.50                  # close when 50% of max profit captured
RATE = 0.04                # risk-free rate for B-S
SMA_EXIT_THRESHOLD = 0.02  # force-exit when SPY >2% below SMA200

# Restrict to years with solid ThetaData coverage
DATA_START = "2014-01-01"
DATA_END = "2026-01-31"
SIM_START = "2015-03-01"   # first usable date (expirations start 2015-02-20)


# ======================================================================
# HELPERS (reused from spy_deployment_sim.py)
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
# DATA LOADING (reused verbatim)
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
                   monthly_exps, force_exit_below_sma=False,
                   delta_target=DELTA_TARGET, label=""):
    """
    Daily portfolio simulation for cash-secured puts.

    Sell to open at bid, buy to close at ask.
    Collateral = strike * 100 per contract.

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

    mode_label = (f"thresh-exit (>{SMA_EXIT_THRESHOLD:.0%} below SMA)"
                  if force_exit_below_sma else "entry-only")
    print(f"\n{'='*70}")
    print(f"Config: {label or mode_label}")
    print(f"  SMA200 mode: {mode_label}")
    print(f"  Delta target: {delta_target}")
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
                _, ask = get_bid_ask(eod)
                if ask is None or ask <= 0:
                    # Intrinsic fallback for puts: max(0, strike - spot)
                    intrinsic = max(0, pos["strike"] - spot)
                    ask = intrinsic * 1.002 if intrinsic > 0 else 0.001
                close_cost = ask * 100
                # Release collateral minus close cost
                net_return = pos["collateral"] - close_cost
                pending_cash += net_return
                pnl_dollar = pos["premium_received"] - close_cost
                pnl_pct = pnl_dollar / pos["collateral"] if pos["collateral"] > 0 else 0
                trade_log.append({
                    "entry_date": pos["entry_date"],
                    "exit_date": today,
                    "expiration": pos["expiration"],
                    "strike": pos["strike"],
                    "entry_bid": pos["entry_bid"],
                    "exit_ask": ask,
                    "premium_received": pos["premium_received"],
                    "close_cost": close_cost,
                    "collateral": pos["collateral"],
                    "pnl_dollar": pnl_dollar,
                    "pnl_pct": pnl_pct,
                    "days_held": pos["days_held"] + 1,
                    "exit_reason": "SMA",
                })
                force_exit_count += 1
            positions = []

        # 2b. Process normal exits (PT / MH / EXPIRY)
        still_open = []
        for pos in positions:
            pos["days_held"] += 1
            ckey = (pos["expiration"], pos["strike"])
            eod = contract_eod.get(ckey, {}).get(today)
            _, ask = get_bid_ask(eod)
            if ask is None or ask <= 0:
                intrinsic = max(0, pos["strike"] - spot)
                ask = intrinsic * 1.002 if intrinsic > 0 else 0.001

            # PT: (entry_bid - current_ask) / entry_bid >= 0.50
            profit_frac = (pos["entry_bid"] - ask) / pos["entry_bid"] if pos["entry_bid"] > 0 else 0

            # Days to expiration (calendar)
            exp_dt = datetime.strptime(pos["expiration"], "%Y-%m-%d").date()
            today_dt = datetime.strptime(today, "%Y-%m-%d").date()
            cal_dte = (exp_dt - today_dt).days

            exit_reason = None
            if profit_frac >= PT:
                exit_reason = "PT"
            elif pos["days_held"] >= MH:
                exit_reason = "MH"
            elif cal_dte <= 2:
                exit_reason = "EXPIRY"

            if exit_reason:
                close_cost = ask * 100
                net_return = pos["collateral"] - close_cost
                pending_cash += net_return
                pnl_dollar = pos["premium_received"] - close_cost
                pnl_pct = pnl_dollar / pos["collateral"] if pos["collateral"] > 0 else 0
                trade_log.append({
                    "entry_date": pos["entry_date"],
                    "exit_date": today,
                    "expiration": pos["expiration"],
                    "strike": pos["strike"],
                    "entry_bid": pos["entry_bid"],
                    "exit_ask": ask,
                    "premium_received": pos["premium_received"],
                    "close_cost": close_cost,
                    "collateral": pos["collateral"],
                    "pnl_dollar": pnl_dollar,
                    "pnl_pct": pnl_pct,
                    "days_held": pos["days_held"],
                    "exit_reason": exit_reason,
                })
            else:
                still_open.append(pos)
        positions = still_open

        # 3. Entry: sell 1 put if above SMA200 and have capital
        entered = False
        if above_sma and sma_val is not None:
            best_exp, dte_cal = find_best_expiration(today, monthly_exps)
            if not best_exp:
                entry_skip_reasons["no_expiration"] += 1
            else:
                t_years = dte_cal / 365.0
                iv_est = max(0.08, min(0.90, vix / 100.0))
                bs_strike = find_strike_for_delta(
                    spot, t_years, RATE, iv_est, delta_target, "P"
                )
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
                                "SPY", best_exp, real_strike, "P", today
                            )
                            contract_eod[ckey] = {r["bar_date"]: r for r in data}
                        eod = contract_eod[ckey].get(today)
                        bid, _ = get_bid_ask(eod)
                        if bid is None or bid <= 0:
                            entry_skip_reasons["no_bid"] += 1
                        elif bid < 0.10:
                            entry_skip_reasons["premium_too_small"] += 1
                        else:
                            collateral = real_strike * 100
                            if available_cash >= collateral:
                                # Reserve collateral, receive premium
                                available_cash -= collateral
                                premium_received = bid * 100
                                available_cash += premium_received
                                positions.append({
                                    "entry_date": today,
                                    "expiration": best_exp,
                                    "strike": real_strike,
                                    "entry_bid": bid,
                                    "premium_received": premium_received,
                                    "collateral": collateral,
                                    "days_held": 0,
                                })
                                entered = True
                            else:
                                entry_skip_reasons["no_capital"] += 1

        # 4. Mark to market
        # portfolio_value = available_cash + pending_cash + total_collateral - positions_liability
        total_collateral = sum(p["collateral"] for p in positions)
        positions_liability = 0.0
        for pos in positions:
            ckey = (pos["expiration"], pos["strike"])
            eod = contract_eod.get(ckey, {}).get(today)
            bid, ask = get_bid_ask(eod)
            if bid and ask:
                mid = (bid + ask) / 2.0
            else:
                mid = max(0, pos["strike"] - spot)
            positions_liability += mid * 100

        portfolio_value = available_cash + pending_cash + total_collateral - positions_liability

        daily_snapshots.append({
            "date": today,
            "portfolio_value": portfolio_value,
            "cash": available_cash + pending_cash,
            "total_collateral": total_collateral,
            "positions_liability": positions_liability,
            "n_positions": len(positions),
            "collateral_util": total_collateral / portfolio_value if portfolio_value > 0 else 0,
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
                  f"Pos={len(positions)}  Cash=${available_cash + pending_cash:,.0f}  "
                  f"Collateral=${total_collateral:,.0f}")

    print(f"\n  Trades: {len(trade_log)}  |  Force-exits: {force_exit_count}")
    print(f"  Entry skips: {dict(entry_skip_reasons)}")
    print(f"  Unique contracts: {len(contract_eod)}")

    return daily_snapshots, trade_log


# ======================================================================
# ANALYSIS
# ======================================================================

def compute_metrics(snapshots, trade_log, label=""):
    """Compute portfolio metrics including CSP-specific stats. Returns dict."""
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
    spy_sharpe = ((df["spy_ret"].mean() / df["spy_ret"].std()) * np.sqrt(252)
                  if df["spy_ret"].std() > 0 else 0)
    spy_dd = (df["spy_close"] / df["spy_close"].cummax() - 1).min()

    # Collateral utilization
    collateral_util = df["collateral_util"].values
    avg_collateral_util = collateral_util.mean()
    max_collateral_util = collateral_util.max()

    # Trade stats
    trade_stats = {}
    if len(tdf) > 0:
        wins = tdf[tdf["pnl_dollar"] > 0]
        losses = tdf[tdf["pnl_dollar"] <= 0]
        trade_stats = {
            "n_trades": len(tdf),
            "win_rate": len(wins) / len(tdf),
            "mean_pnl_pct": tdf["pnl_pct"].mean(),
            "med_pnl_pct": tdf["pnl_pct"].median(),
            "avg_win_pct": wins["pnl_pct"].mean() if len(wins) > 0 else 0,
            "avg_loss_pct": losses["pnl_pct"].mean() if len(losses) > 0 else 0,
            "total_pnl": tdf["pnl_dollar"].sum(),
            "pt_exits": len(tdf[tdf["exit_reason"] == "PT"]),
            "mh_exits": len(tdf[tdf["exit_reason"] == "MH"]),
            "sma_exits": len(tdf[tdf["exit_reason"] == "SMA"]),
            "expiry_exits": len(tdf[tdf["exit_reason"] == "EXPIRY"]),
            "avg_days": tdf["days_held"].mean(),
            "avg_premium": tdf["premium_received"].mean(),
            "avg_collateral": tdf["collateral"].mean(),
            "avg_return_on_collateral": tdf["pnl_pct"].mean(),
            "total_premium_collected": tdf["premium_received"].sum(),
            "total_close_cost": tdf["close_cost"].sum(),
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
        "avg_pos": df["n_positions"].mean(),
        "med_pos": np.median(df["n_positions"].values),
        "max_pos": df["n_positions"].max(),
        "avg_collateral_util": avg_collateral_util,
        "max_collateral_util": max_collateral_util,
        "trades": trade_stats,
        "yearly": yearly,
        "snapshots_df": df,
        "trade_df": tdf,
    }


# ======================================================================
# OUTPUT
# ======================================================================

def print_delta_comparison(m_a, m_b, delta_val):
    """Side-by-side comparison of entry-only vs thresh-exit for one delta."""

    W = 95
    label_a = m_a["label"]
    label_b = m_b["label"]

    print(f"\n{'=' * W}")
    print(f"SPY CSP -- DELTA {abs(delta_val):.0%} -- COMPARISON")
    print(f"{'=' * W}")
    print(f"  Period:    {SIM_START} to {DATA_END}")
    print(f"  Capital:   ${CAPITAL:,} (no margin)")
    print(f"  Strategy:  Sell 1 CSP/day when SPY > SMA200")
    print(f"  Options:   {abs(delta_val):.0%}-delta put, ~{DTE_TARGET} DTE, monthly expiry")
    print(f"  Rules:     PT={PT:.0%}, MH={MH}td, expiry<=2d, no stop-loss")
    print(f"  Execution: sell at bid, buy to close at ask, T+1 settlement")
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
            ("Mean P&L/collat", f"{ta['mean_pnl_pct']:+.2%}", f"{tb['mean_pnl_pct']:+.2%}"),
            ("Median P&L/col",  f"{ta['med_pnl_pct']:+.2%}", f"{tb['med_pnl_pct']:+.2%}"),
            ("Avg win",         f"{ta['avg_win_pct']:+.2%}", f"{tb['avg_win_pct']:+.2%}"),
            ("Avg loss",        f"{ta['avg_loss_pct']:+.2%}", f"{tb['avg_loss_pct']:+.2%}"),
            ("Total P&L",       f"${ta['total_pnl']:+,.0f}", f"${tb['total_pnl']:+,.0f}"),
            ("Avg premium",     f"${ta['avg_premium']:,.0f}", f"${tb['avg_premium']:,.0f}"),
            ("Avg collateral",  f"${ta['avg_collateral']:,.0f}", f"${tb['avg_collateral']:,.0f}"),
            ("Total premium",   f"${ta['total_premium_collected']:,.0f}",
                                f"${tb['total_premium_collected']:,.0f}"),
            ("Total close $",   f"${ta['total_close_cost']:,.0f}",
                                f"${tb['total_close_cost']:,.0f}"),
            ("PT exits",        f"{ta['pt_exits']} ({ta['pt_exits']/ta['n_trades']:.0%})",
                                f"{tb['pt_exits']} ({tb['pt_exits']/tb['n_trades']:.0%})"),
            ("MH exits",        f"{ta['mh_exits']} ({ta['mh_exits']/ta['n_trades']:.0%})",
                                f"{tb['mh_exits']} ({tb['mh_exits']/tb['n_trades']:.0%})"),
            ("Expiry exits",    f"{ta['expiry_exits']}", f"{tb['expiry_exits']}"),
            ("SMA exits",       f"{ta['sma_exits']}", f"{tb['sma_exits']}"),
            ("Avg days held",   f"{ta['avg_days']:.0f}", f"{tb['avg_days']:.0f}"),
        ]

        print(f"  {'Metric':<25} {label_a:>20} {label_b:>20}")
        print(f"  {'-' * 67}")
        for name, va, vb in trade_rows:
            print(f"  {name:<25} {va:>20} {vb:>20}")

    # -- Collateral Stats --
    print(f"\n{'-' * W}")
    print("POSITION & COLLATERAL")
    print(f"{'-' * W}")
    pos_rows = [
        ("Avg positions",       f"{m_a['avg_pos']:.1f}", f"{m_b['avg_pos']:.1f}"),
        ("Max positions",       f"{m_a['max_pos']}", f"{m_b['max_pos']}"),
        ("Avg collateral util", f"{m_a['avg_collateral_util']:.1%}",
                                f"{m_b['avg_collateral_util']:.1%}"),
        ("Max collateral util", f"{m_a['max_collateral_util']:.1%}",
                                f"{m_b['max_collateral_util']:.1%}"),
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

    hdr_a = "--- A: Entry-only ---"
    hdr_b = "--- B: Thresh-exit ---"
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

    # -- Drawdown detail --
    print(f"\n{'-' * W}")
    print("DRAWDOWN DETAIL")
    print(f"{'-' * W}")

    for m, lbl in [(m_a, label_a), (m_b, label_b)]:
        df = m["snapshots_df"]
        cummax = df["portfolio_value"].cummax()
        dd = df["portfolio_value"] / cummax - 1

        in_dd = False
        dd_periods = []
        peak_idx = 0
        for i in range(len(dd)):
            if dd.iloc[i] < -0.03 and not in_dd:
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


def print_ranked_table(all_metrics):
    """Cross-delta ranked table by Sharpe."""

    W = 110
    print(f"\n{'=' * W}")
    print("CROSS-DELTA RANKED TABLE (by Sharpe)")
    print(f"{'=' * W}")

    # Sort by Sharpe descending
    ranked = sorted(all_metrics, key=lambda m: m["sharpe"], reverse=True)

    header = (f"  {'Config':<20} {'CAGR':>7} {'Sharpe':>7} {'Sortino':>8} "
              f"{'MaxDD':>7} {'Trades':>7} {'WinRate':>8} {'AvgPrem':>9} "
              f"{'AvgCollat':>10} {'AvgRoC':>8} {'SMA-exits':>10}")
    print(header)
    print(f"  {'-' * (W - 2)}")

    for m in ranked:
        t = m["trades"]
        if t:
            print(f"  {m['label']:<20} "
                  f"{m['cagr']:>+6.1%} "
                  f"{m['sharpe']:>7.2f} "
                  f"{m['sortino']:>8.2f} "
                  f"{m['max_dd']:>7.1%} "
                  f"{t['n_trades']:>7} "
                  f"{t['win_rate']:>7.1%} "
                  f"${t['avg_premium']:>7,.0f} "
                  f"${t['avg_collateral']:>8,.0f} "
                  f"{t['avg_return_on_collateral']:>+7.2%} "
                  f"{t['sma_exits']:>10}")
        else:
            print(f"  {m['label']:<20} "
                  f"{m['cagr']:>+6.1%} "
                  f"{m['sharpe']:>7.2f} "
                  f"{m['sortino']:>8.2f} "
                  f"{m['max_dd']:>7.1%} "
                  f"{'--':>7} {'--':>8} {'--':>9} {'--':>10} {'--':>8} {'--':>10}")

    print()


def print_summary(all_metrics):
    """Print overall summary with best delta, best exit rule, and assessment."""

    W = 95
    print(f"\n{'=' * W}")
    print("SUMMARY")
    print(f"{'=' * W}")

    for m in sorted(all_metrics, key=lambda x: x["sharpe"], reverse=True):
        t = m["trades"]
        trades_str = ""
        if t:
            trades_str = (f"  Trades: {t['n_trades']}  |  Win: {t['win_rate']:.0%}  |  "
                          f"SMA exits: {t['sma_exits']}  |  "
                          f"Prem collected: ${t['total_premium_collected']:,.0f}")
        print(f"  {m['label']}:")
        print(f"    ${CAPITAL:,} -> ${m['end_val']:,.0f}  |  "
              f"CAGR: {m['cagr']:+.1%}  |  Sharpe: {m['sharpe']:.2f}  |  "
              f"Max DD: {m['max_dd']:.1%}")
        print(f"  {trades_str}")

    # Best config
    best = max(all_metrics, key=lambda x: x["sharpe"])
    print(f"\n  BEST CONFIG (by Sharpe): {best['label']}")
    print(f"    Sharpe={best['sharpe']:.2f}  CAGR={best['cagr']:+.1%}  MaxDD={best['max_dd']:.1%}")

    # SPY B&H reference
    ref = all_metrics[0]
    print(f"\n  SPY B&H: CAGR {ref['spy_cagr']:+.1%}  |  "
          f"Sharpe: {ref['spy_sharpe']:.2f}  |  Max DD: {ref['spy_dd']:.1%}")

    # Delta sensitivity
    print(f"\n  DELTA SENSITIVITY:")
    # Group by delta
    entry_only = [m for m in all_metrics if "no_exit" in m["label"]]
    thresh_exit = [m for m in all_metrics if "thresh" in m["label"]]

    for group, group_name in [(entry_only, "Entry-only"), (thresh_exit, "Thresh-exit")]:
        if group:
            group_sorted = sorted(group, key=lambda x: x["sharpe"], reverse=True)
            best_g = group_sorted[0]
            print(f"    {group_name}: best = {best_g['label']} "
                  f"(Sharpe={best_g['sharpe']:.2f}, CAGR={best_g['cagr']:+.1%})")

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
    print("SPY Cash-Secured Put Simulator (v1)")
    print("=" * 80)

    client = ThetaDataClient()
    if not client.connect():
        print("\nERROR: Cannot connect to Theta Terminal.")
        return

    print("Connected to Theta Terminal.\n")

    spy_by_date, trading_dates, vix_data, sma200, monthly_exps = load_all_data(client)

    # Test grid: 3 deltas x 2 SMA modes = 6 configs
    deltas = [-0.20, -0.25, -0.30]
    all_metrics = []

    for delta_val in deltas:
        delta_pct = int(abs(delta_val) * 100)

        # Config A: entry-only filter
        label_a = f"{delta_pct}d_no_exit"
        snaps_a, trades_a = run_simulation(
            client, spy_by_date, trading_dates, vix_data, sma200, monthly_exps,
            force_exit_below_sma=False,
            delta_target=delta_val,
            label=f"A: {delta_pct}d entry-only",
        )

        # Config B: threshold exit
        label_b = f"{delta_pct}d_thresh"
        snaps_b, trades_b = run_simulation(
            client, spy_by_date, trading_dates, vix_data, sma200, monthly_exps,
            force_exit_below_sma=True,
            delta_target=delta_val,
            label=f"B: {delta_pct}d thresh-exit",
        )

        if not snaps_a or not snaps_b:
            print(f"\nInsufficient data for delta={delta_val}.")
            continue

        m_a = compute_metrics(snaps_a, trades_a, label_a)
        m_b = compute_metrics(snaps_b, trades_b, label_b)

        # Per-delta comparison table
        print_delta_comparison(m_a, m_b, delta_val)

        all_metrics.append(m_a)
        all_metrics.append(m_b)

    if not all_metrics:
        print("\nNo results to display.")
        client.close()
        return

    # Cross-delta ranked table
    print_ranked_table(all_metrics)

    # Summary
    print_summary(all_metrics)

    client.close()


if __name__ == "__main__":
    main()
