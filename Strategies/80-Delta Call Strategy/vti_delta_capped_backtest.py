"""
Delta-Capped 80-Delta Call Strategy Backtest - VTI Version
===========================================================
Same strategy as SPY version but applied to VTI (Equal-Weight S&P 500 ETF).

VTI options may have lower liquidity, so this version includes liquidity filtering:
- Minimum open interest requirement
- Maximum bid-ask spread check

Uses VIX as IV proxy.

Usage:
    python vti_delta_capped_backtest.py              # With liquidity filters
    python vti_delta_capped_backtest.py --midpoint   # No filters, midpoint execution
    python vti_delta_capped_backtest.py --no-filter  # No filters, buy at ask/sell at bid
"""

import os
import sys
import math
import logging
import argparse
from datetime import datetime, date, timedelta
from collections import defaultdict

import numpy as np
import pandas as pd

_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_dir = os.path.dirname(os.path.dirname(_this_dir))
sys.path.insert(0, _project_dir)

from backtest.thetadata_client import ThetaDataClient
from backtest.black_scholes import black_scholes_price, find_strike_for_delta

log = logging.getLogger("vti_delta_capped")

# ======================================================================
# PARAMETERS
# ======================================================================

TICKER = "VTI"

# Share holdings (adjusted for VTI price)
SHARES = 3500              # Number of VTI shares held

# Liquidity filters (can be overridden with --midpoint flag)
MIN_OPEN_INTEREST = 100    # Minimum OI for entry
MAX_SPREAD_PCT = 0.03      # Maximum 3% bid-ask spread
USE_MIDPOINT = False       # If True, use midpoint for entry instead of ask

# Strategy parameters - LONG CALLS (above SMA)
DELTA = 0.80
DTE_TARGET = 120
DTE_MIN = 90
DTE_MAX = 150
MH = 60
PT = 0.50
RATE = 0.04
SMA_EXIT_THRESHOLD = 0.02

# Strategy parameters - COVERED CALLS (below SMA)
CC_DELTA = 0.25
CC_DTE_TARGET = 45
CC_DTE_MIN = 30
CC_DTE_MAX = 60
CC_PT = 0.50
CC_MAX_CONTRACTS = 35      # Adjusted for VTI

# Cash allocation for options
OPTIONS_CASH_ALLOCATION = 100_000

# Data range
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


def find_cc_expiration(entry_date_str, monthly_exps_dates):
    return find_best_expiration(
        entry_date_str, monthly_exps_dates,
        target=CC_DTE_TARGET, dte_min=CC_DTE_MIN, dte_max=CC_DTE_MAX
    )


def get_bid_ask(eod_row):
    if eod_row is None:
        return None, None, None
    bid = eod_row.get("bid", 0) or 0
    ask = eod_row.get("ask", 0) or 0
    oi = eod_row.get("open_interest", 0) or 0
    if bid > 0 and ask > 0 and ask >= bid:
        return bid, ask, oi
    close = eod_row.get("close", 0) or 0
    if close > 0:
        return close * 0.998, close * 1.002, oi
    return None, None, oi


def check_liquidity(bid, ask, oi, skip_liquidity=False):
    """Check if the option meets liquidity requirements."""
    if skip_liquidity:
        # Relaxed mode: only require some quote exists
        if bid is None and ask is None:
            return False, "no_quote"
        return True, "ok"

    # Strict mode: full liquidity checks
    if bid is None or ask is None:
        return False, "no_quote"
    if bid <= 0:
        return False, "zero_bid"
    if oi is not None and oi < MIN_OPEN_INTEREST:
        return False, "low_oi"
    spread_pct = (ask - bid) / bid if bid > 0 else 1.0
    if spread_pct > MAX_SPREAD_PCT:
        return False, "wide_spread"
    return True, "ok"


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


# ======================================================================
# DATA LOADING
# ======================================================================

def load_all_data(client):
    print(f"Loading {TICKER} bars...")
    ticker_bars = client.fetch_ticker_bars(TICKER, DATA_START, DATA_END)
    if not ticker_bars:
        print(f"ERROR: No data for {TICKER}. ETF may not have sufficient history.")
        return None, None, None, None, None, None, None

    bars_by_date = {b["bar_date"]: b for b in ticker_bars}
    trading_dates = sorted(bars_by_date.keys())

    print("Loading VIX history (as IV proxy)...")
    vix_data = client.fetch_vix_history(DATA_START, DATA_END)

    # SMA200
    sma200 = {}
    for i in range(199, len(trading_dates)):
        window = [bars_by_date[trading_dates[j]]["close"] for j in range(i - 199, i + 1)]
        sma200[trading_dates[i]] = sum(window) / 200.0

    # Trailing 12-month returns
    print("Calculating trailing 12-month returns...")
    trailing_12m_returns = {}
    for i in range(252, len(trading_dates)):
        today = trading_dates[i]
        prior = trading_dates[i - 252]
        price_today = bars_by_date[today]["close"]
        price_prior = bars_by_date[prior]["close"]
        trailing_12m_returns[today] = (price_today / price_prior) - 1.0

    # 20-day rolling volatility
    print("Calculating 20-day rolling volatility...")
    rolling_volatility = {}
    for i in range(20, len(trading_dates)):
        today = trading_dates[i]
        daily_returns = []
        for j in range(i - 19, i + 1):
            if j > 0:
                prev_close = bars_by_date[trading_dates[j - 1]]["close"]
                curr_close = bars_by_date[trading_dates[j]]["close"]
                daily_returns.append(curr_close / prev_close - 1.0)
        if daily_returns:
            vol = np.std(daily_returns, ddof=1)
            rolling_volatility[today] = vol * np.sqrt(252)

    print(f"Loading {TICKER} expirations...")
    all_exps = client.get_expirations(TICKER)
    monthly_exps = [(e, datetime.strptime(e, "%Y-%m-%d").date())
                    for e in all_exps if is_monthly_opex(e)]
    monthly_exps.sort(key=lambda x: x[1])

    print(f"  {TICKER} bars: {len(ticker_bars)} ({trading_dates[0]} to {trading_dates[-1]})")
    print(f"  VIX days: {len(vix_data)}")
    first_sma = sorted(sma200.keys())[0] if sma200 else "N/A"
    print(f"  SMA200 from: {first_sma}")
    print(f"  Monthly expirations: {len(monthly_exps)}")
    if monthly_exps:
        print(f"    ({monthly_exps[0][0]} to {monthly_exps[-1][0]})")
    else:
        print("    WARNING: No monthly expirations found!")

    return bars_by_date, trading_dates, vix_data, sma200, monthly_exps, trailing_12m_returns, rolling_volatility


# ======================================================================
# SIMULATION ENGINE
# ======================================================================

def run_delta_capped_simulation(client, bars_by_date, trading_dates, vix_data,
                                 sma200, monthly_exps, trailing_12m_returns=None,
                                 rolling_volatility=None, force_exit_below_sma=False,
                                 sell_covered_calls=False, skip_liquidity=False,
                                 use_midpoint=False, label=""):
    """Daily portfolio simulation with delta-capped options for VTI."""

    shares_held = SHARES
    options_cash = float(OPTIONS_CASH_ALLOCATION)
    pending_cash = 0.0
    positions = []
    cc_positions = []

    contract_eod = {}
    strikes_cache = {}

    daily_snapshots = []
    trade_log = []
    cc_trade_log = []
    entry_skip_reasons = defaultdict(int)
    cc_skip_reasons = defaultdict(int)
    force_exit_count = 0
    liquidity_skips = defaultdict(int)

    if trailing_12m_returns is None:
        trailing_12m_returns = {}
    if rolling_volatility is None:
        rolling_volatility = {}

    start_idx = next((i for i, d in enumerate(trading_dates) if d >= SIM_START), 0)

    mode_label = f"thresh-exit (>{SMA_EXIT_THRESHOLD:.0%} below SMA)" if force_exit_below_sma else "entry-only"
    cc_label = " + covered calls below SMA" if sell_covered_calls else ""
    if use_midpoint:
        exec_label = " (MIDPOINT EXECUTION)"
    elif skip_liquidity:
        exec_label = " (NO FILTER, BID/ASK)"
    else:
        exec_label = ""
    print(f"\n{'='*70}")
    print(f"Config: {label or mode_label}{exec_label}")
    print(f"  Ticker: {TICKER}")
    print(f"  Share holdings: {SHARES:,} shares")
    print(f"  Options cash: ${OPTIONS_CASH_ALLOCATION:,}")
    if use_midpoint:
        print(f"  Liquidity: DISABLED (midpoint execution)")
    elif skip_liquidity:
        print(f"  Liquidity: DISABLED (buy at ask, sell at bid)")
    else:
        print(f"  Liquidity: OI >= {MIN_OPEN_INTEREST}, spread <= {MAX_SPREAD_PCT:.0%}")
    print(f"  Delta cap: {SHARES:,}")
    print(f"  SMA200 mode: {mode_label}{cc_label}")
    print(f"  Period: {trading_dates[start_idx]} to {trading_dates[-1]}")
    print(f"{'='*70}")

    for day_idx in range(start_idx, len(trading_dates)):
        today = trading_dates[day_idx]
        bar = bars_by_date[today]
        spot = bar["close"]
        sma_val = sma200.get(today)
        above_sma = (spot > sma_val) if sma_val else False
        vix = vix_data.get(today, 20.0)
        iv_est = max(0.08, min(0.90, vix / 100.0))

        shares_value = shares_held * spot

        options_cash += pending_cash
        pending_cash = 0.0

        # Analysis fields
        pct_above_sma = (spot - sma_val) / sma_val if sma_val else 0
        trailing_ret = trailing_12m_returns.get(today, 0)
        roll_vol = rolling_volatility.get(today, 0)
        std_above_sma = 0.0
        if roll_vol > 0 and sma_val:
            daily_vol = roll_vol / np.sqrt(252)
            if daily_vol > 0:
                std_above_sma = pct_above_sma / daily_vol

        # Force-exit
        pct_below_sma = (sma_val - spot) / sma_val if sma_val and sma_val > 0 else 0
        if force_exit_below_sma and pct_below_sma >= SMA_EXIT_THRESHOLD and positions:
            for pos in positions:
                ckey = (pos["expiration"], pos["strike"])
                eod = contract_eod.get(ckey, {}).get(today)
                bid, _, _ = get_bid_ask(eod)
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
                    "contract_cost": pos["contract_cost"],
                    "entry_delta": pos["entry_delta"],
                    "entry_spot": pos.get("entry_spot", 0),
                    "entry_sma200": pos.get("entry_sma200", 0),
                    "entry_pct_above_sma": pos.get("entry_pct_above_sma", 0),
                    "entry_std_above_sma": pos.get("entry_std_above_sma", 0),
                    "entry_trailing_12m_return": pos.get("entry_trailing_12m_return", 0),
                })
                force_exit_count += 1
            positions = []

        # Normal exits
        still_open = []
        for pos in positions:
            pos["days_held"] += 1
            ckey = (pos["expiration"], pos["strike"])
            eod = contract_eod.get(ckey, {}).get(today)
            bid, _, _ = get_bid_ask(eod)
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
                    "contract_cost": pos["contract_cost"],
                    "entry_delta": pos["entry_delta"],
                    "entry_spot": pos.get("entry_spot", 0),
                    "entry_sma200": pos.get("entry_sma200", 0),
                    "entry_pct_above_sma": pos.get("entry_pct_above_sma", 0),
                    "entry_std_above_sma": pos.get("entry_std_above_sma", 0),
                    "entry_trailing_12m_return": pos.get("entry_trailing_12m_return", 0),
                })
            else:
                still_open.append(pos)
        positions = still_open

        # Covered call exits
        cc_still_open = []
        for cc in cc_positions:
            cc["days_held"] += 1
            ckey = (cc["expiration"], cc["strike"])
            eod = contract_eod.get(ckey, {}).get(today)
            _, ask, _ = get_bid_ask(eod)
            if ask is None or ask <= 0:
                intrinsic = max(0, spot - cc["strike"])
                ask = intrinsic + 0.10 if intrinsic > 0 else 0.05

            pnl_pct = (cc["entry_price"] - ask) / cc["entry_price"]
            dte = (datetime.strptime(cc["expiration"], "%Y-%m-%d").date() -
                   datetime.strptime(today, "%Y-%m-%d").date()).days

            exit_reason = None
            if pnl_pct >= CC_PT:
                exit_reason = "PT"
            elif dte <= 5:
                exit_reason = "EXP"
            elif above_sma:
                exit_reason = "SMA_UP"

            if exit_reason:
                cost_to_close = ask * 100 * cc["quantity"]
                options_cash -= cost_to_close
                net_profit = cc["premium_received"] - cost_to_close
                cc_trade_log.append({
                    "entry_date": cc["entry_date"],
                    "exit_date": today,
                    "expiration": cc["expiration"],
                    "strike": cc["strike"],
                    "quantity": cc["quantity"],
                    "entry_price": cc["entry_price"],
                    "exit_price": ask,
                    "premium_received": cc["premium_received"],
                    "cost_to_close": cost_to_close,
                    "pnl_dollar": net_profit,
                    "pnl_pct": pnl_pct,
                    "days_held": cc["days_held"],
                    "exit_reason": exit_reason,
                })
            else:
                cc_still_open.append(cc)
                cc["current_ask"] = ask
        cc_positions = cc_still_open

        # Current options delta
        current_options_delta = 0.0
        for pos in positions:
            dte = (datetime.strptime(pos["expiration"], "%Y-%m-%d").date() -
                   datetime.strptime(today, "%Y-%m-%d").date()).days
            pos_delta = calculate_delta(spot, pos["strike"], dte, iv_est) * pos["quantity"] * 100
            current_options_delta += pos_delta

        # Entry with liquidity check
        entered = False
        contracts_entered = 0
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
                        if ckey not in contract_eod:
                            data = client.prefetch_option_life(
                                TICKER, best_exp, real_strike, "C", today
                            )
                            contract_eod[ckey] = {r["bar_date"]: r for r in data}
                        eod = contract_eod[ckey].get(today)
                        bid, ask, oi = get_bid_ask(eod)

                        # Liquidity check
                        liq_ok, liq_reason = check_liquidity(bid, ask, oi, skip_liquidity or use_midpoint)
                        if not liq_ok:
                            liquidity_skips[liq_reason] += 1
                            entry_skip_reasons["liquidity"] += 1
                        else:
                            # Determine entry price: midpoint if relaxed, ask if strict
                            if use_midpoint and bid and ask and bid > 0 and ask > 0:
                                entry_price = (bid + ask) / 2.0
                            elif ask and ask > 0:
                                entry_price = ask
                            else:
                                entry_skip_reasons["no_ask"] += 1
                                entry_price = None

                            if entry_price:
                                option_delta = calculate_delta(spot, real_strike, dte_cal, iv_est)
                                max_by_delta = int(delta_room / (option_delta * 100))
                                contract_cost = entry_price * 100
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
                                        "entry_price": entry_price,
                                        "quantity": qty,
                                        "contract_cost": total_cost,
                                        "days_held": 0,
                                        "entry_delta": option_delta,
                                        "entry_spot": spot,
                                        "entry_sma200": sma_val,
                                        "entry_pct_above_sma": pct_above_sma,
                                        "entry_std_above_sma": std_above_sma,
                                        "entry_trailing_12m_return": trailing_ret,
                                        "entry_oi": oi,
                                        "entry_spread_pct": (ask - bid) / bid if bid and bid > 0 else 0,
                                    })
                                    entered = True
                                contracts_entered = qty

        # Covered calls entry (with liquidity check)
        cc_entered = False
        cc_contracts_entered = 0
        if sell_covered_calls and not above_sma and sma_val is not None:
            current_cc_contracts = sum(cc["quantity"] for cc in cc_positions)
            cc_room = CC_MAX_CONTRACTS - current_cc_contracts

            if cc_room > 0:
                cc_exp, cc_dte = find_cc_expiration(today, monthly_exps)
                if not cc_exp:
                    cc_skip_reasons["no_expiration"] += 1
                else:
                    t_years = cc_dte / 365.0
                    cc_bs_strike = find_strike_for_delta(spot, t_years, RATE, iv_est, CC_DELTA, "C")
                    if not cc_bs_strike:
                        cc_skip_reasons["bs_fail"] += 1
                    else:
                        if cc_exp not in strikes_cache:
                            strikes_cache[cc_exp] = client.get_strikes(TICKER, cc_exp)
                        strikes = strikes_cache[cc_exp]
                        if not strikes:
                            cc_skip_reasons["no_strikes"] += 1
                        else:
                            cc_real_strike = min(strikes, key=lambda s: abs(s - cc_bs_strike))
                            ckey = (cc_exp, cc_real_strike)
                            if ckey not in contract_eod:
                                data = client.prefetch_option_life(
                                    TICKER, cc_exp, cc_real_strike, "C", today
                                )
                                contract_eod[ckey] = {r["bar_date"]: r for r in data}
                            eod = contract_eod[ckey].get(today)
                            bid, ask, oi = get_bid_ask(eod)

                            liq_ok, liq_reason = check_liquidity(bid, ask, oi)
                            if not liq_ok:
                                cc_skip_reasons[f"liq_{liq_reason}"] += 1
                            elif bid is None or bid <= 0.10:
                                cc_skip_reasons["low_premium"] += 1
                            else:
                                qty = min(cc_room, 1)
                                premium = bid * 100 * qty
                                options_cash += premium
                                cc_positions.append({
                                    "entry_date": today,
                                    "expiration": cc_exp,
                                    "strike": cc_real_strike,
                                    "entry_price": bid,
                                    "quantity": qty,
                                    "premium_received": premium,
                                    "days_held": 0,
                                })
                                cc_entered = True
                                cc_contracts_entered = qty

        # Mark to market
        positions_value = 0.0
        total_options_delta = 0.0
        for pos in positions:
            ckey = (pos["expiration"], pos["strike"])
            eod = contract_eod.get(ckey, {}).get(today)
            bid, ask, _ = get_bid_ask(eod)
            if bid and ask:
                mid = (bid + ask) / 2.0
            else:
                mid = max(0, spot - pos["strike"])
            positions_value += mid * 100 * pos["quantity"]

            dte = (datetime.strptime(pos["expiration"], "%Y-%m-%d").date() -
                   datetime.strptime(today, "%Y-%m-%d").date()).days
            pos_delta = calculate_delta(spot, pos["strike"], dte, iv_est) * pos["quantity"] * 100
            total_options_delta += pos_delta

        cc_liability = 0.0
        cc_delta = 0.0
        for cc in cc_positions:
            ckey = (cc["expiration"], cc["strike"])
            eod = contract_eod.get(ckey, {}).get(today)
            _, ask, _ = get_bid_ask(eod)
            if ask is None or ask <= 0:
                intrinsic = max(0, spot - cc["strike"])
                ask = intrinsic + 0.05 if intrinsic > 0 else 0.05
            cc_liability += ask * 100 * cc["quantity"]

            dte = (datetime.strptime(cc["expiration"], "%Y-%m-%d").date() -
                   datetime.strptime(today, "%Y-%m-%d").date()).days
            cc_pos_delta = calculate_delta(spot, cc["strike"], dte, iv_est) * cc["quantity"] * 100
            cc_delta += cc_pos_delta

        portfolio_value = shares_value + options_cash + pending_cash + positions_value - cc_liability
        net_options_delta = total_options_delta - cc_delta
        total_delta = shares_held + net_options_delta
        capital_deployed = sum(p["contract_cost"] for p in positions)
        n_contracts = sum(p["quantity"] for p in positions)
        n_cc_contracts = sum(cc["quantity"] for cc in cc_positions)

        daily_snapshots.append({
            "date": today,
            "portfolio_value": portfolio_value,
            "shares_value": shares_value,
            "options_value": positions_value,
            "cc_liability": cc_liability,
            "options_cash": options_cash + pending_cash,
            "n_positions": len(positions),
            "n_contracts": n_contracts,
            "n_cc_positions": len(cc_positions),
            "n_cc_contracts": n_cc_contracts,
            "capital_deployed": capital_deployed,
            "shares_delta": shares_held,
            "long_options_delta": total_options_delta,
            "cc_delta": cc_delta,
            "net_options_delta": net_options_delta,
            "total_delta": total_delta,
            "effective_leverage": total_delta / shares_held if shares_held > 0 else 0,
            "above_sma": above_sma,
            "ticker_close": spot,
            "entered": entered,
            "contracts_entered": contracts_entered,
            "cc_entered": cc_entered,
            "cc_contracts_entered": cc_contracts_entered,
            "sma200": sma_val,
            "pct_above_sma": pct_above_sma,
            "std_above_sma": std_above_sma,
            "trailing_12m_return": trailing_ret,
            "rolling_volatility": roll_vol,
        })

        real_idx = day_idx - start_idx
        total_days = len(trading_dates) - start_idx
        if (real_idx + 1) % 500 == 0 or real_idx == 0:
            cc_str = f"  CC={n_cc_contracts}" if sell_covered_calls else ""
            print(f"  [{real_idx+1}/{total_days}] {today}  "
                  f"Portfolio=${portfolio_value:,.0f}  "
                  f"Shares=${shares_value:,.0f}  Options=${positions_value:,.0f}{cc_str}  "
                  f"Delta={total_delta:,.0f}")

    print(f"\n  Long Call Trades: {len(trade_log)}  |  Force-exits: {force_exit_count}")
    print(f"  Entry skips: {dict(entry_skip_reasons)}")
    if liquidity_skips:
        print(f"  Liquidity skips: {dict(liquidity_skips)}")
    if sell_covered_calls:
        print(f"  Covered Call Trades: {len(cc_trade_log)}")
        print(f"  CC skips: {dict(cc_skip_reasons)}")
    print(f"  Unique contracts: {len(contract_eod)}")

    return daily_snapshots, trade_log, cc_trade_log


# ======================================================================
# ANALYSIS
# ======================================================================

def compute_metrics(snapshots, trade_log, cc_trade_log=None, label=""):
    """Compute portfolio metrics."""
    df = pd.DataFrame(snapshots)
    tdf = pd.DataFrame(trade_log) if trade_log else pd.DataFrame()

    n_days = len(df)
    years = n_days / 252.0

    start_shares = df["shares_value"].iloc[0]
    start_options_cash = df["options_cash"].iloc[0]
    start_val = df["portfolio_value"].iloc[0]
    end_val = df["portfolio_value"].iloc[-1]

    total_return = end_val / start_val - 1
    cagr = (end_val / start_val) ** (1 / years) - 1 if years > 0 else 0

    df["daily_ret"] = df["portfolio_value"].pct_change().fillna(0)
    daily_mean = df["daily_ret"].mean()
    daily_std = df["daily_ret"].std()
    sharpe = (daily_mean / daily_std) * np.sqrt(252) if daily_std > 0 else 0

    cummax = df["portfolio_value"].cummax()
    drawdown = df["portfolio_value"] / cummax - 1
    max_dd = drawdown.min()

    # Ticker B&H
    ticker_start = df["ticker_close"].iloc[0]
    ticker_end = df["ticker_close"].iloc[-1]
    ticker_cagr = (ticker_end / ticker_start) ** (1 / years) - 1 if years > 0 else 0

    # Shares-only
    shares_only_start = start_shares
    shares_only_end = df["shares_value"].iloc[-1]
    shares_only_return = shares_only_end / shares_only_start - 1
    shares_only_cagr = (shares_only_end / shares_only_start) ** (1 / years) - 1 if years > 0 else 0

    # Trade stats
    trade_stats = {}
    if len(tdf) > 0:
        wins = tdf[tdf["pnl_pct"] > 0]
        trade_stats = {
            "n_trades": len(tdf),
            "win_rate": len(wins) / len(tdf),
            "mean_ret": tdf["pnl_pct"].mean(),
            "total_pnl": tdf["pnl_dollar"].sum(),
        }

    return {
        "label": label,
        "years": years,
        "start_val": start_val,
        "end_val": end_val,
        "total_return": total_return,
        "cagr": cagr,
        "sharpe": sharpe,
        "max_dd": max_dd,
        "ticker_cagr": ticker_cagr,
        "shares_only_cagr": shares_only_cagr,
        "trades": trade_stats,
        "snapshots_df": df,
        "trade_df": tdf,
    }


def print_results(metrics, use_midpoint=False, no_filter=False):
    """Print results summary."""
    m = metrics
    W = 80

    print(f"\n{'=' * W}")
    print(f"VTI DELTA-CAPPED 80-DELTA CALL STRATEGY -- BACKTEST RESULTS")
    print(f"{'=' * W}")
    print(f"  Period:         {SIM_START} to {DATA_END}")
    print(f"  Share Holdings: {SHARES:,} {TICKER} shares")
    print(f"  Options Cash:   ${OPTIONS_CASH_ALLOCATION:,}")
    if use_midpoint:
        print(f"  Execution:      MIDPOINT (no liquidity filters)")
    elif no_filter:
        print(f"  Execution:      BID/ASK (no liquidity filters)")
    else:
        print(f"  Liquidity:      OI >= {MIN_OPEN_INTEREST}, spread <= {MAX_SPREAD_PCT:.0%}")
    print(f"  Config:         {m['label']}")

    print(f"\n{'-' * W}")
    print("PORTFOLIO PERFORMANCE")
    print(f"{'-' * W}")

    print(f"  Starting Value:    ${m['start_val']:>15,.0f}")
    print(f"  Ending Value:      ${m['end_val']:>15,.0f}")
    print(f"  Total Return:      {m['total_return']:>+15.1%}")
    print(f"  CAGR:              {m['cagr']:>+15.1%}")
    print(f"  Sharpe Ratio:      {m['sharpe']:>15.2f}")
    print(f"  Max Drawdown:      {m['max_dd']:>15.1%}")

    print(f"\n  {TICKER} B&H CAGR:       {m['ticker_cagr']:>+15.1%}")
    print(f"  Shares-Only CAGR:  {m['shares_only_cagr']:>+15.1%}")
    print(f"  Alpha vs Shares:   {m['cagr'] - m['shares_only_cagr']:>+15.1%}")

    t = m["trades"]
    if t:
        print(f"\n{'-' * W}")
        print("TRADE STATISTICS")
        print(f"{'-' * W}")
        print(f"  Total Trades:      {t['n_trades']}")
        print(f"  Win Rate:          {t['win_rate']:.1%}")
        print(f"  Mean Return:       {t['mean_ret']:+.1%}")
        print(f"  Total P&L:         ${t['total_pnl']:+,.0f}")

    print(f"\n{'=' * W}")


# ======================================================================
# MAIN
# ======================================================================

def main():
    parser = argparse.ArgumentParser(description="VTI Delta-Capped 80-Delta Call Strategy Backtest")
    parser.add_argument("--midpoint", action="store_true",
                        help="Disable liquidity filters and use midpoint execution")
    parser.add_argument("--no-filter", action="store_true", dest="no_filter",
                        help="Disable liquidity filters but use bid/ask (worst case spread)")
    args = parser.parse_args()

    use_midpoint = args.midpoint
    skip_liquidity = args.no_filter or args.midpoint

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    print("=" * 80)
    print(f"Delta-Capped 80-Delta Call Strategy Backtest - {TICKER}")
    if use_midpoint:
        print("*** MIDPOINT EXECUTION MODE (no liquidity filters) ***")
    elif args.no_filter:
        print("*** NO LIQUIDITY FILTER (buy at ask, sell at bid) ***")
    print("=" * 80)
    print(f"\nModeling {SHARES:,} {TICKER} shares + options capped at {SHARES:,} delta")
    print(f"Options cash allocation: ${OPTIONS_CASH_ALLOCATION:,}")
    if use_midpoint:
        print(f"Execution: MIDPOINT (assumes fills at mid-price)")
    elif args.no_filter:
        print(f"Execution: BID/ASK (no liquidity filter, worst case spreads)")
    else:
        print(f"Liquidity filters: OI >= {MIN_OPEN_INTEREST}, spread <= {MAX_SPREAD_PCT:.0%}")
    print()

    client = ThetaDataClient()
    if not client.connect():
        print("\nERROR: Cannot connect to Theta Terminal.")
        return

    print("Connected to Theta Terminal.\n")

    result = load_all_data(client)
    if result[0] is None:
        print("\nFailed to load data. Exiting.")
        client.close()
        return

    bars_by_date, trading_dates, vix_data, sma200, monthly_exps, trailing_12m_returns, rolling_volatility = result

    if not monthly_exps:
        print("\nWARNING: No monthly expirations found for VTI.")
        print("VTI may have limited options history in ThetaData.")
        client.close()
        return

    # Run backtest
    if use_midpoint:
        label = "Midpoint execution"
    elif args.no_filter:
        label = "No filter (bid/ask)"
    else:
        label = "Thresh-exit (no CC)"
    snaps, trades, cc_trades = run_delta_capped_simulation(
        client, bars_by_date, trading_dates, vix_data, sma200, monthly_exps,
        trailing_12m_returns=trailing_12m_returns,
        rolling_volatility=rolling_volatility,
        force_exit_below_sma=True,
        sell_covered_calls=False,
        skip_liquidity=skip_liquidity,
        use_midpoint=use_midpoint,
        label=label,
    )

    if not snaps:
        print("\nInsufficient data.")
        client.close()
        return

    m = compute_metrics(snaps, trades, cc_trades, label)
    print_results(m, use_midpoint, args.no_filter)

    # Save trade log for analysis
    if trades:
        if use_midpoint:
            suffix = "_midpoint"
        elif args.no_filter:
            suffix = "_nofilter"
        else:
            suffix = ""
        output_file = os.path.join(_this_dir, f"vti_trades{suffix}.csv")
        pd.DataFrame(trades).to_csv(output_file, index=False)
        print(f"\nTrade log saved to: {output_file}")

    client.close()


if __name__ == "__main__":
    main()
