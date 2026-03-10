"""
Delta-Capped 80-Delta Call Strategy Backtest
=============================================
Models a portfolio where:
  - Hold a fixed number of SPY shares (3,125 shares as in IRA)
  - Run 80-delta call strategy with options delta CAPPED at share count
  - Max options delta = 3,125 (equivalent to share holdings)
  - SELL COVERED CALLS when SPY is below SMA200 (income overlay)

This tests the "hedged overlay" concept where options exposure never
exceeds the underlying share position.

Key differences from standard backtest:
  - Options sized by delta, not by dollar amount
  - Entry only allowed if total options delta < share delta cap
  - Combined portfolio = shares + cash + options
  - Covered calls sold during bearish periods for income

Covered Call Assumptions:
  - Shares are NEVER assigned (we buy back or roll before expiration)
  - Share count remains constant at 3,125 throughout

Usage:
    python delta_capped_backtest.py
"""

import os
import sys
import math
import logging
from datetime import datetime, date, timedelta
from collections import defaultdict

import numpy as np
import pandas as pd
from scipy.stats import norm

_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_dir = os.path.dirname(os.path.dirname(_this_dir))
sys.path.insert(0, _project_dir)

from backtest.thetadata_client import ThetaDataClient
from backtest.black_scholes import black_scholes_price, find_strike_for_delta

log = logging.getLogger("delta_capped")

# ======================================================================
# PARAMETERS
# ======================================================================

# Share holdings
SHARES = 3125              # Number of SPY shares held (IRA amount)

# Strategy parameters - LONG CALLS (above SMA)
DELTA = 0.80               # Target delta for long calls
DTE_TARGET = 120           # Calendar days to expiration
DTE_MIN = 90
DTE_MAX = 150
MH = 60                    # Max hold in trading days
PT = 0.50                  # +50% profit target
RATE = 0.04                # Risk-free rate for B-S
SMA_EXIT_THRESHOLD = 0.02  # Force-exit threshold

# Strategy parameters - COVERED CALLS (below SMA)
CC_DELTA = 0.25            # Target delta for covered calls (OTM)
CC_DTE_TARGET = 45         # Shorter duration for covered calls
CC_DTE_MIN = 30
CC_DTE_MAX = 60
CC_PT = 0.50               # Buy back at 50% profit (collected 50% of premium)
CC_MAX_CONTRACTS = 31      # Max covered calls (3125 shares / 100)

# Cash allocation for options (separate from share holdings)
# Set to None to use "excess cash" model, or a fixed amount
OPTIONS_CASH_ALLOCATION = 100_000  # Starting cash for options

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
    """Find best expiration for covered calls (shorter duration)."""
    return find_best_expiration(
        entry_date_str, monthly_exps_dates,
        target=CC_DTE_TARGET, dte_min=CC_DTE_MIN, dte_max=CC_DTE_MAX
    )


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
    """Calculate option delta using Black-Scholes with exact normal CDF."""
    if dte <= 0:
        if spot == strike:
            return 0.5 if right == "C" else -0.5
        if right == "C":
            return 1.0 if spot > strike else 0.0
        else:
            return -1.0 if spot < strike else 0.0

    t = dte / 365.0
    d1 = (math.log(spot / strike) + (rate + 0.5 * iv * iv) * t) / (iv * math.sqrt(t))
    # Use scipy.stats.norm.cdf for exact Black-Scholes delta (not erf approximation)
    delta = norm.cdf(d1)

    if right == "P":
        delta = delta - 1.0

    return delta


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

    # Pre-calculate trailing 12-month returns (252 trading days)
    print("Calculating trailing 12-month returns...")
    trailing_12m_returns = {}
    for i in range(252, len(trading_dates)):
        today = trading_dates[i]
        prior = trading_dates[i - 252]
        price_today = spy_by_date[today]["close"]
        price_prior = spy_by_date[prior]["close"]
        trailing_12m_returns[today] = (price_today / price_prior) - 1.0

    # Pre-calculate 20-day rolling volatility of daily returns
    print("Calculating 20-day rolling volatility...")
    rolling_volatility = {}
    for i in range(20, len(trading_dates)):
        today = trading_dates[i]
        # Calculate daily returns for past 20 days
        daily_returns = []
        for j in range(i - 19, i + 1):
            if j > 0:
                prev_close = spy_by_date[trading_dates[j - 1]]["close"]
                curr_close = spy_by_date[trading_dates[j]]["close"]
                daily_returns.append(curr_close / prev_close - 1.0)
        if daily_returns:
            vol = np.std(daily_returns, ddof=1)  # Sample std dev
            # Annualize: daily vol * sqrt(252)
            rolling_volatility[today] = vol * np.sqrt(252)

    print("Loading SPY dividends...")
    spy_dividends = client.fetch_spy_dividends(DATA_START, DATA_END)

    print("Loading SPY expirations...")
    all_exps = client.get_expirations("SPY")
    monthly_exps = [(e, datetime.strptime(e, "%Y-%m-%d").date())
                    for e in all_exps if is_monthly_opex(e)]
    monthly_exps.sort(key=lambda x: x[1])

    print(f"  SPY bars: {len(spy_bars)} ({trading_dates[0]} to {trading_dates[-1]})")
    print(f"  VIX days: {len(vix_data)}")
    print(f"  SPY dividends: {len(spy_dividends)} ex-dates")
    first_sma = sorted(sma200.keys())[0] if sma200 else "N/A"
    print(f"  SMA200 from: {first_sma}")
    first_ret = sorted(trailing_12m_returns.keys())[0] if trailing_12m_returns else "N/A"
    print(f"  Trailing 12m returns from: {first_ret}")
    first_vol = sorted(rolling_volatility.keys())[0] if rolling_volatility else "N/A"
    print(f"  Rolling volatility from: {first_vol}")
    print(f"  Monthly expirations: {len(monthly_exps)} "
          f"({monthly_exps[0][0]} to {monthly_exps[-1][0]})")

    return spy_by_date, trading_dates, vix_data, sma200, monthly_exps, trailing_12m_returns, rolling_volatility, spy_dividends


# ======================================================================
# SIMULATION ENGINE
# ======================================================================

def run_delta_capped_simulation(client, spy_by_date, trading_dates, vix_data,
                                 sma200, monthly_exps, trailing_12m_returns=None,
                                 rolling_volatility=None, spy_dividends=None,
                                 force_exit_below_sma=False,
                                 sell_covered_calls=False, label=""):
    """
    Daily portfolio simulation with delta-capped options.

    Portfolio components:
      1. SHARES SPY shares (fixed quantity, value changes with price)
      2. Cash for options trading
      3. Long call positions (delta capped at SHARES) - when above SMA
      4. Short covered call positions - when below SMA (if enabled)
    """
    # Initialize
    shares_held = SHARES
    options_cash = float(OPTIONS_CASH_ALLOCATION)
    pending_cash = 0.0
    positions = []  # Long calls
    cc_positions = []  # Short covered calls

    contract_eod = {}
    strikes_cache = {}

    daily_snapshots = []
    trade_log = []
    cc_trade_log = []  # Separate log for covered calls
    entry_skip_reasons = defaultdict(int)
    cc_skip_reasons = defaultdict(int)
    force_exit_count = 0

    cumulative_dividends = 0.0

    # Default empty dicts if not provided
    if trailing_12m_returns is None:
        trailing_12m_returns = {}
    if rolling_volatility is None:
        rolling_volatility = {}
    if spy_dividends is None:
        spy_dividends = {}

    # Start from SIM_START
    start_idx = next((i for i, d in enumerate(trading_dates) if d >= SIM_START), 0)

    mode_label = f"thresh-exit (>{SMA_EXIT_THRESHOLD:.0%} below SMA)" if force_exit_below_sma else "entry-only"
    cc_label = " + covered calls below SMA" if sell_covered_calls else ""
    print(f"\n{'='*70}")
    print(f"Config: {label or mode_label}")
    print(f"  Share holdings: {SHARES:,} SPY shares")
    print(f"  Options cash: ${OPTIONS_CASH_ALLOCATION:,}")
    print(f"  Delta cap: {SHARES:,} (= share count)")
    print(f"  SMA200 mode: {mode_label}{cc_label}")
    if sell_covered_calls:
        print(f"  Covered calls: {CC_DELTA:.0%}-delta, ~{CC_DTE_TARGET} DTE, PT={CC_PT:.0%}")
    print(f"  Period: {trading_dates[start_idx]} to {trading_dates[-1]}")
    print(f"{'='*70}")

    for day_idx in range(start_idx, len(trading_dates)):
        today = trading_dates[day_idx]
        bar = spy_by_date[today]
        spot = bar["close"]
        sma_val = sma200.get(today)
        above_sma = (spot > sma_val) if sma_val else False
        vix = vix_data.get(today, 20.0)
        iv_est = max(0.08, min(0.90, vix / 100.0))

        # Value of shares
        shares_value = shares_held * spot

        # 1. Settle yesterday's exit proceeds
        options_cash += pending_cash
        pending_cash = 0.0

        # 1b. Accumulate dividends (held as separate cash pool)
        dividend_today = 0.0
        if today in spy_dividends:
            dividend_today = shares_held * spy_dividends[today]
            cumulative_dividends += dividend_today

        # Calculate analysis fields for this day
        pct_above_sma = (spot - sma_val) / sma_val if sma_val else 0
        trailing_ret = trailing_12m_returns.get(today, 0)
        roll_vol = rolling_volatility.get(today, 0)
        # Standard deviations above SMA (using 20-day realized vol as proxy for daily move std)
        # SMA distance in $ / (daily vol * spot) gives approx # of daily std devs
        # But we want rolling vol context, so: pct_above_sma / (roll_vol / sqrt(252))
        std_above_sma = 0.0
        if roll_vol > 0 and sma_val:
            daily_vol = roll_vol / np.sqrt(252)  # Convert annual to daily
            if daily_vol > 0:
                std_above_sma = pct_above_sma / daily_vol

        # 2a. Force-exit all positions when SPY >2% below SMA200
        pct_below_sma = (sma_val - spot) / sma_val if sma_val and sma_val > 0 else 0
        if force_exit_below_sma and pct_below_sma >= SMA_EXIT_THRESHOLD and positions:
            for pos in positions:
                ckey = (pos["expiration"], pos["strike"])
                eod = contract_eod.get(ckey, {}).get(today)
                bid, _ = get_bid_ask(eod)
                if bid is None or bid <= 0:
                    intrinsic = max(0, spot - pos["strike"])
                    # Use B-S estimate instead of $0.001 to preserve time value
                    dte = (datetime.strptime(pos["expiration"], "%Y-%m-%d").date() -
                           datetime.strptime(today, "%Y-%m-%d").date()).days
                    bs_price = black_scholes_price(spot, pos["strike"], dte / 365.0, RATE, iv_est, "C")
                    bid = (bs_price * 0.98) if bs_price and bs_price > 0 else max(intrinsic * 0.998, 0.01)
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
                    # Analysis fields from entry
                    "entry_spot": pos.get("entry_spot", 0),
                    "entry_sma200": pos.get("entry_sma200", 0),
                    "entry_pct_above_sma": pos.get("entry_pct_above_sma", 0),
                    "entry_std_above_sma": pos.get("entry_std_above_sma", 0),
                    "entry_trailing_12m_return": pos.get("entry_trailing_12m_return", 0),
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
                dte = (datetime.strptime(pos["expiration"], "%Y-%m-%d").date() -
                       datetime.strptime(today, "%Y-%m-%d").date()).days
                bs_price = black_scholes_price(spot, pos["strike"], dte / 365.0, RATE, iv_est, "C")
                bid = (bs_price * 0.98) if bs_price and bs_price > 0 else max(intrinsic * 0.998, 0.01)

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
                    # Analysis fields from entry
                    "entry_spot": pos.get("entry_spot", 0),
                    "entry_sma200": pos.get("entry_sma200", 0),
                    "entry_pct_above_sma": pos.get("entry_pct_above_sma", 0),
                    "entry_std_above_sma": pos.get("entry_std_above_sma", 0),
                    "entry_trailing_12m_return": pos.get("entry_trailing_12m_return", 0),
                })
            else:
                still_open.append(pos)
        positions = still_open

        # 2c. Process covered call exits (buy back at profit or near expiration)
        cc_still_open = []
        cc_premium_collected = 0.0
        for cc in cc_positions:
            cc["days_held"] += 1
            ckey = (cc["expiration"], cc["strike"])
            eod = contract_eod.get(ckey, {}).get(today)
            _, ask = get_bid_ask(eod)  # We need to BUY back, so use ask
            if ask is None or ask <= 0:
                # For OTM calls, if no quote, estimate as small value
                intrinsic = max(0, spot - cc["strike"])
                ask = intrinsic + 0.10 if intrinsic > 0 else 0.05

            # Calculate profit (we sold at entry_price, buy back at current ask)
            # Profit = premium received - cost to close
            pnl_pct = (cc["entry_price"] - ask) / cc["entry_price"]  # % of premium kept

            dte = (datetime.strptime(cc["expiration"], "%Y-%m-%d").date() -
                   datetime.strptime(today, "%Y-%m-%d").date()).days

            exit_reason = None
            if pnl_pct >= CC_PT:  # Captured 50% of premium
                exit_reason = "PT"
            elif dte <= 5:  # Close near expiration to avoid assignment risk
                exit_reason = "EXP"
            elif above_sma:  # SPY back above SMA, close covered calls
                exit_reason = "SMA_UP"

            if exit_reason:
                # Buy back the call
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
                # Mark unrealized P&L
                cc["current_ask"] = ask
        cc_positions = cc_still_open

        # 3. Calculate current total options delta
        current_options_delta = 0.0
        for pos in positions:
            dte = (datetime.strptime(pos["expiration"], "%Y-%m-%d").date() -
                   datetime.strptime(today, "%Y-%m-%d").date()).days
            pos_delta = calculate_delta(spot, pos["strike"], dte, iv_est) * pos["quantity"] * 100
            current_options_delta += pos_delta

        # 4. Entry: buy contracts if above SMA200 and delta cap allows
        entered = False
        contracts_entered = 0
        delta_room = SHARES - current_options_delta

        if above_sma and sma_val is not None and delta_room > 80:  # Room for at least 1 contract
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
                        _, ask = get_bid_ask(eod)

                        if ask is None or ask <= 0:
                            entry_skip_reasons["no_ask"] += 1
                        else:
                            # Calculate actual delta of this option
                            option_delta = calculate_delta(spot, real_strike, dte_cal, iv_est)

                            # How many contracts can we buy within delta cap?
                            max_by_delta = int(delta_room / (option_delta * 100))

                            # How many can we afford?
                            contract_cost = ask * 100
                            max_by_cash = int(options_cash / contract_cost)

                            # Buy the minimum of delta cap and cash limit
                            qty = min(max_by_delta, max_by_cash, 1)  # Buy 1 at a time like original

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
                                    # Analysis fields at entry
                                    "entry_spot": spot,
                                    "entry_sma200": sma_val,
                                    "entry_pct_above_sma": pct_above_sma,
                                    "entry_std_above_sma": std_above_sma,
                                    "entry_trailing_12m_return": trailing_ret,
                                })
                                entered = True
                                contracts_entered = qty

        # 4b. Sell covered calls when BELOW SMA (if enabled)
        cc_entered = False
        cc_contracts_entered = 0
        if sell_covered_calls and not above_sma and sma_val is not None:
            # How many covered calls can we sell?
            current_cc_contracts = sum(cc["quantity"] for cc in cc_positions)
            cc_room = CC_MAX_CONTRACTS - current_cc_contracts

            if cc_room > 0:
                cc_exp, cc_dte = find_cc_expiration(today, monthly_exps)
                if not cc_exp:
                    cc_skip_reasons["no_expiration"] += 1
                else:
                    t_years = cc_dte / 365.0
                    # Find OTM strike for covered call (low delta = OTM)
                    cc_bs_strike = find_strike_for_delta(spot, t_years, RATE, iv_est, CC_DELTA, "C")
                    if not cc_bs_strike:
                        cc_skip_reasons["bs_fail"] += 1
                    else:
                        if cc_exp not in strikes_cache:
                            strikes_cache[cc_exp] = client.get_strikes("SPY", cc_exp)
                        strikes = strikes_cache[cc_exp]
                        if not strikes:
                            cc_skip_reasons["no_strikes"] += 1
                        else:
                            cc_real_strike = min(strikes, key=lambda s: abs(s - cc_bs_strike))
                            ckey = (cc_exp, cc_real_strike)
                            if ckey not in contract_eod:
                                data = client.prefetch_option_life(
                                    "SPY", cc_exp, cc_real_strike, "C", today
                                )
                                contract_eod[ckey] = {r["bar_date"]: r for r in data}
                            eod = contract_eod[ckey].get(today)
                            bid, _ = get_bid_ask(eod)  # We SELL at bid

                            if bid is None or bid <= 0.10:  # Minimum premium
                                cc_skip_reasons["low_premium"] += 1
                            else:
                                # Sell covered calls (1 at a time)
                                qty = min(cc_room, 1)
                                premium = bid * 100 * qty
                                options_cash += premium  # Receive premium
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

        # 5. Mark to market - LONG CALLS
        positions_value = 0.0
        total_options_delta = 0.0
        for pos in positions:
            ckey = (pos["expiration"], pos["strike"])
            eod = contract_eod.get(ckey, {}).get(today)
            bid, ask = get_bid_ask(eod)
            if bid and ask:
                mid = (bid + ask) / 2.0
            else:
                mid = max(0, spot - pos["strike"])
            positions_value += mid * 100 * pos["quantity"]

            # Update delta
            dte = (datetime.strptime(pos["expiration"], "%Y-%m-%d").date() -
                   datetime.strptime(today, "%Y-%m-%d").date()).days
            pos_delta = calculate_delta(spot, pos["strike"], dte, iv_est) * pos["quantity"] * 100
            total_options_delta += pos_delta

        # 5b. Mark to market - COVERED CALLS (liability)
        cc_liability = 0.0
        cc_delta = 0.0
        for cc in cc_positions:
            ckey = (cc["expiration"], cc["strike"])
            eod = contract_eod.get(ckey, {}).get(today)
            _, ask = get_bid_ask(eod)  # Cost to buy back
            if ask is None or ask <= 0:
                intrinsic = max(0, spot - cc["strike"])
                ask = intrinsic + 0.05 if intrinsic > 0 else 0.05
            cc_liability += ask * 100 * cc["quantity"]

            # Covered call delta (negative, reduces exposure)
            dte = (datetime.strptime(cc["expiration"], "%Y-%m-%d").date() -
                   datetime.strptime(today, "%Y-%m-%d").date()).days
            cc_pos_delta = calculate_delta(spot, cc["strike"], dte, iv_est) * cc["quantity"] * 100
            cc_delta += cc_pos_delta

        # Total portfolio value (subtract CC liability since we'd need to buy back)
        portfolio_value = shares_value + options_cash + pending_cash + positions_value - cc_liability + cumulative_dividends

        # Total delta: shares + long calls - covered calls (CC reduces delta exposure)
        net_options_delta = total_options_delta - cc_delta
        total_delta = shares_held + net_options_delta
        effective_shares = total_delta  # Equivalent share exposure

        # Capital deployed in options
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
            "dividend_today": dividend_today,
            "cumulative_dividends": cumulative_dividends,
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
            "spy_close": spot,
            "entered": entered,
            "contracts_entered": contracts_entered,
            "cc_entered": cc_entered,
            "cc_contracts_entered": cc_contracts_entered,
            # Analysis fields
            "sma200": sma_val,
            "pct_above_sma": pct_above_sma,
            "std_above_sma": std_above_sma,
            "trailing_12m_return": trailing_ret,
            "rolling_volatility": roll_vol,
        })

        # Progress
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
    if sell_covered_calls:
        print(f"  Covered Call Trades: {len(cc_trade_log)}")
        print(f"  CC skips: {dict(cc_skip_reasons)}")
    print(f"  Unique contracts: {len(contract_eod)}")

    return daily_snapshots, trade_log, cc_trade_log


# ======================================================================
# ANALYSIS
# ======================================================================

def compute_metrics(snapshots, trade_log, cc_trade_log=None, label=""):
    """Compute portfolio metrics including combined shares+options+covered calls."""
    df = pd.DataFrame(snapshots)
    tdf = pd.DataFrame(trade_log) if trade_log else pd.DataFrame()
    ccdf = pd.DataFrame(cc_trade_log) if cc_trade_log else pd.DataFrame()

    n_days = len(df)
    years = n_days / 252.0

    # Starting values
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

    downside = df["daily_ret"][df["daily_ret"] < 0]
    ds_std = downside.std() if len(downside) > 0 else 0
    sortino = (daily_mean / ds_std) * np.sqrt(252) if ds_std > 0 else 0

    cummax = df["portfolio_value"].cummax()
    drawdown = df["portfolio_value"] / cummax - 1
    max_dd = drawdown.min()

    # SPY B&H (price-only, unadjusted)
    spy_start = df["spy_close"].iloc[0]
    spy_end = df["spy_close"].iloc[-1]
    spy_total = spy_end / spy_start - 1
    spy_cagr = (spy_end / spy_start) ** (1 / years) - 1 if years > 0 else 0
    df["spy_ret"] = df["spy_close"].pct_change().fillna(0)
    spy_sharpe = (df["spy_ret"].mean() / df["spy_ret"].std()) * np.sqrt(252) if df["spy_ret"].std() > 0 else 0
    spy_dd = (df["spy_close"] / df["spy_close"].cummax() - 1).min()

    # Cumulative dividends
    total_dividends = df["cumulative_dividends"].iloc[-1] if "cumulative_dividends" in df.columns else 0

    # Shares-only B&H (shares + dividends, what we'd have without options)
    shares_only_start = start_shares
    shares_only_end = df["shares_value"].iloc[-1] + total_dividends
    shares_only_return = shares_only_end / shares_only_start - 1
    shares_only_cagr = (shares_only_end / shares_only_start) ** (1 / years) - 1 if years > 0 else 0

    # Options-only performance (isolate the options contribution)
    options_start = start_options_cash
    cc_liability_end = df["cc_liability"].iloc[-1] if "cc_liability" in df.columns else 0
    options_end = df["options_cash"].iloc[-1] + df["options_value"].iloc[-1] - cc_liability_end
    options_return = options_end / options_start - 1 if options_start > 0 else 0
    options_cagr = (options_end / options_start) ** (1 / years) - 1 if years > 0 and options_start > 0 else 0

    # Delta stats
    if "long_options_delta" in df.columns:
        avg_options_delta = df["long_options_delta"].mean()
        max_options_delta = df["long_options_delta"].max()
    else:
        avg_options_delta = df["options_delta"].mean() if "options_delta" in df.columns else 0
        max_options_delta = df["options_delta"].max() if "options_delta" in df.columns else 0
    avg_total_delta = df["total_delta"].mean()
    avg_leverage = df["effective_leverage"].mean()

    # Covered call stats
    avg_cc_delta = df["cc_delta"].mean() if "cc_delta" in df.columns else 0
    avg_cc_contracts = df["n_cc_contracts"].mean() if "n_cc_contracts" in df.columns else 0

    # Trade stats
    trade_stats = {}
    if len(tdf) > 0:
        wins = tdf[tdf["pnl_pct"] > 0]
        losses = tdf[tdf["pnl_pct"] <= 0]
        trade_stats = {
            "n_trades": len(tdf),
            "total_contracts": tdf["quantity"].sum(),
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
            "avg_qty": tdf["quantity"].mean(),
        }

    # Covered call trade stats
    cc_stats = {}
    if len(ccdf) > 0:
        cc_wins = ccdf[ccdf["pnl_dollar"] > 0]
        total_premium = ccdf["premium_received"].sum()
        total_buyback = ccdf["cost_to_close"].sum()
        net_cc_pnl = total_premium - total_buyback
        cc_stats = {
            "n_trades": len(ccdf),
            "total_contracts": ccdf["quantity"].sum(),
            "win_rate": len(cc_wins) / len(ccdf),
            "total_premium": total_premium,
            "total_buyback": total_buyback,
            "net_pnl": net_cc_pnl,
            "avg_premium": ccdf["premium_received"].mean() / ccdf["quantity"].mean() / 100,  # Per contract
            "pt_exits": len(ccdf[ccdf["exit_reason"] == "PT"]),
            "exp_exits": len(ccdf[ccdf["exit_reason"] == "EXP"]),
            "sma_up_exits": len(ccdf[ccdf["exit_reason"] == "SMA_UP"]),
            "avg_days": ccdf["days_held"].mean(),
        }

    # Yearly
    df["year"] = pd.to_datetime(df["date"]).dt.year
    yearly = {}
    for year in sorted(df["year"].unique()):
        ydf = df[df["year"] == year]
        y_start = ydf["portfolio_value"].iloc[0]
        y_end = ydf["portfolio_value"].iloc[-1]
        y_ret = y_end / y_start - 1

        # Shares-only for this year
        y_shares_start = ydf["shares_value"].iloc[0]
        y_shares_end = ydf["shares_value"].iloc[-1]
        y_shares_ret = y_shares_end / y_shares_start - 1

        y_trades = 0
        if len(tdf) > 0:
            y_trades = len(tdf[pd.to_datetime(tdf["exit_date"]).dt.year == year])

        # Get options delta (handle both old and new column names)
        if "long_options_delta" in ydf.columns:
            y_avg_delta = ydf["long_options_delta"].mean()
        elif "options_delta" in ydf.columns:
            y_avg_delta = ydf["options_delta"].mean()
        else:
            y_avg_delta = 0

        # CC trades for this year
        y_cc_trades = 0
        y_cc_income = 0
        if len(ccdf) > 0:
            y_cc = ccdf[pd.to_datetime(ccdf["exit_date"]).dt.year == year]
            y_cc_trades = len(y_cc)
            y_cc_income = y_cc["pnl_dollar"].sum() if len(y_cc) > 0 else 0

        yearly[year] = {
            "ret": y_ret,
            "shares_ret": y_shares_ret,
            "alpha": y_ret - y_shares_ret,
            "trades": y_trades,
            "cc_trades": y_cc_trades,
            "cc_income": y_cc_income,
            "avg_contracts": ydf["n_contracts"].mean(),
            "max_contracts": ydf["n_contracts"].max(),
            "avg_options_delta": y_avg_delta,
            "end_val": y_end,
        }

    return {
        "label": label,
        "years": years,
        "start_val": start_val,
        "end_val": end_val,
        "total_return": total_return,
        "cagr": cagr,
        "sharpe": sharpe,
        "sortino": sortino,
        "max_dd": max_dd,
        "spy_cagr": spy_cagr,
        "spy_sharpe": spy_sharpe,
        "spy_dd": spy_dd,
        "cumulative_dividends": total_dividends,
        "shares_only_return": shares_only_return,
        "shares_only_cagr": shares_only_cagr,
        "options_return": options_return,
        "options_cagr": options_cagr,
        "avg_options_delta": avg_options_delta,
        "max_options_delta": max_options_delta,
        "avg_total_delta": avg_total_delta,
        "avg_leverage": avg_leverage,
        "avg_cc_delta": avg_cc_delta,
        "avg_cc_contracts": avg_cc_contracts,
        "trades": trade_stats,
        "cc_trades": cc_stats,
        "yearly": yearly,
        "snapshots_df": df,
        "trade_df": tdf,
        "cc_trade_df": ccdf,
    }


# ======================================================================
# OUTPUT
# ======================================================================

def print_results(metrics):
    """Print comprehensive results."""
    m = metrics
    W = 80

    print(f"\n{'=' * W}")
    print("DELTA-CAPPED 80-DELTA CALL STRATEGY -- BACKTEST RESULTS")
    print(f"{'=' * W}")
    print(f"  Period:         {SIM_START} to {DATA_END}")
    print(f"  Share Holdings: {SHARES:,} SPY shares")
    print(f"  Options Cash:   ${OPTIONS_CASH_ALLOCATION:,}")
    print(f"  Delta Cap:      {SHARES:,} (max options delta = share count)")
    print(f"  Strategy:       {DELTA:.0%}-delta calls, ~{DTE_TARGET} DTE")
    print(f"  Rules:          PT=+{PT:.0%}, MH={MH}td, no stop-loss")
    print(f"  Config:         {m['label']}")

    # Portfolio Performance
    print(f"\n{'-' * W}")
    print("COMBINED PORTFOLIO PERFORMANCE (Shares + Options)")
    print(f"{'-' * W}")

    rows = [
        ("Starting Value", f"${m['start_val']:>15,.0f}"),
        ("Ending Value", f"${m['end_val']:>15,.0f}"),
        ("Total Return", f"{m['total_return']:>+15.1%}"),
        ("CAGR", f"{m['cagr']:>+15.1%}"),
        ("Sharpe Ratio", f"{m['sharpe']:>15.2f}"),
        ("Sortino Ratio", f"{m['sortino']:>15.2f}"),
        ("Max Drawdown", f"{m['max_dd']:>15.1%}"),
    ]
    for name, val in rows:
        print(f"  {name:<25} {val}")

    # Comparison to Benchmarks
    print(f"\n{'-' * W}")
    print("COMPARISON TO BENCHMARKS")
    print(f"{'-' * W}")
    print(f"  {'Metric':<25} {'Combined':>15} {'Shares-Only':>15} {'SPY B&H':>15}")
    print(f"  {'-' * 72}")
    print(f"  {'CAGR':<25} {m['cagr']:>+14.1%} {m['shares_only_cagr']:>+14.1%} {m['spy_cagr']:>+14.1%}")
    print(f"  {'Total Return':<25} {m['total_return']:>+14.1%} {m['shares_only_return']:>+14.1%} {'--':>15}")
    print(f"  {'Sharpe':<25} {m['sharpe']:>15.2f} {'--':>15} {m['spy_sharpe']:>15.2f}")
    print(f"  {'Max DD':<25} {m['max_dd']:>14.1%} {'--':>15} {m['spy_dd']:>14.1%}")

    # Dividends
    if m.get("cumulative_dividends", 0) > 0:
        print(f"\n{'-' * W}")
        print("DIVIDENDS")
        print(f"{'-' * W}")
        print(f"  Cumulative Dividends:   ${m['cumulative_dividends']:,.0f}")

    # Options Component Performance
    print(f"\n{'-' * W}")
    print("OPTIONS COMPONENT ONLY")
    print(f"{'-' * W}")
    print(f"  Starting Cash:          ${OPTIONS_CASH_ALLOCATION:,}")
    print(f"  Options CAGR:           {m['options_cagr']:+.1%}")
    print(f"  Options Total Return:   {m['options_return']:+.1%}")

    # Delta Exposure
    print(f"\n{'-' * W}")
    print("DELTA EXPOSURE")
    print(f"{'-' * W}")
    print(f"  Share Delta (constant): {SHARES:,}")
    print(f"  Avg Options Delta:      {m['avg_options_delta']:,.0f}")
    print(f"  Max Options Delta:      {m['max_options_delta']:,.0f}")
    print(f"  Avg Total Delta:        {m['avg_total_delta']:,.0f}")
    print(f"  Avg Effective Leverage: {m['avg_leverage']:.2f}x")
    print(f"  Options as % of Shares: {m['avg_options_delta']/SHARES*100:.1f}%")

    # Trade Statistics - Long Calls
    t = m["trades"]
    if t:
        print(f"\n{'-' * W}")
        print("LONG CALL TRADE STATISTICS")
        print(f"{'-' * W}")
        trade_rows = [
            ("Total Trades", f"{t['n_trades']}"),
            ("Total Contracts", f"{t['total_contracts']}"),
            ("Win Rate", f"{t['win_rate']:.1%}"),
            ("Mean Return", f"{t['mean_ret']:+.1%}"),
            ("Median Return", f"{t['med_ret']:+.1%}"),
            ("Avg Win", f"{t['avg_win']:+.1%}"),
            ("Avg Loss", f"{t['avg_loss']:+.1%}"),
            ("Total P&L", f"${t['total_pnl']:+,.0f}"),
            ("PT Exits", f"{t['pt_exits']} ({t['pt_exits']/t['n_trades']:.0%})"),
            ("MH Exits", f"{t['mh_exits']} ({t['mh_exits']/t['n_trades']:.0%})"),
            ("SMA Exits", f"{t['sma_exits']}"),
            ("Avg Days Held", f"{t['avg_days']:.0f}"),
            ("Avg Contract Cost", f"${t['avg_cost']:,.0f}"),
        ]
        for name, val in trade_rows:
            print(f"  {name:<25} {val}")

    # Trade Statistics - Covered Calls
    cc = m.get("cc_trades", {})
    if cc:
        print(f"\n{'-' * W}")
        print("COVERED CALL TRADE STATISTICS")
        print(f"{'-' * W}")
        cc_rows = [
            ("Total Trades", f"{cc['n_trades']}"),
            ("Total Contracts", f"{cc['total_contracts']}"),
            ("Win Rate", f"{cc['win_rate']:.1%}"),
            ("Total Premium Collected", f"${cc['total_premium']:,.0f}"),
            ("Total Buyback Cost", f"${cc['total_buyback']:,.0f}"),
            ("Net P&L", f"${cc['net_pnl']:+,.0f}"),
            ("Avg Premium/Contract", f"${cc['avg_premium']:.2f}"),
            ("PT Exits (50% captured)", f"{cc['pt_exits']}"),
            ("Near-Expiry Exits", f"{cc['exp_exits']}"),
            ("SMA-Up Exits", f"{cc['sma_up_exits']}"),
            ("Avg Days Held", f"{cc['avg_days']:.0f}"),
        ]
        for name, val in cc_rows:
            print(f"  {name:<25} {val}")

    # Year-by-Year
    print(f"\n{'-' * W}")
    print("YEAR-BY-YEAR PERFORMANCE")
    print(f"{'-' * W}")

    # Check if we have CC data
    has_cc = any(y.get("cc_trades", 0) > 0 for y in m["yearly"].values())

    if has_cc:
        print(f"\n  {'Year':<6} {'Combined':>10} {'Shares':>10} {'Alpha':>10} {'Calls':>7} {'CC':>5} {'CC Inc':>10}")
        print(f"  {'-' * 62}")
        for year, y in sorted(m["yearly"].items()):
            cc_inc = y.get("cc_income", 0)
            cc_trades = y.get("cc_trades", 0)
            print(f"  {year:<6} {y['ret']:>+9.1%} {y['shares_ret']:>+9.1%} {y['alpha']:>+9.1%} "
                  f"{y['trades']:>7} {cc_trades:>5} ${cc_inc:>9,.0f}")
    else:
        print(f"\n  {'Year':<6} {'Combined':>10} {'Shares':>10} {'Alpha':>10} {'Trades':>8} {'MaxContr':>10} {'AvgDelta':>10}")
        print(f"  {'-' * 66}")
        for year, y in sorted(m["yearly"].items()):
            print(f"  {year:<6} {y['ret']:>+9.1%} {y['shares_ret']:>+9.1%} {y['alpha']:>+9.1%} "
                  f"{y['trades']:>8} {y['max_contracts']:>10.0f} {y['avg_options_delta']:>10.0f}")

    # Monthly Returns
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
    print("MONTHLY RETURNS")
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

    # Summary
    print(f"\n{'=' * W}")
    print("SUMMARY")
    print(f"{'=' * W}")
    print(f"  Combined Portfolio ({SHARES:,} shares + delta-capped options):")
    print(f"    ${m['start_val']:,.0f} -> ${m['end_val']:,.0f}")
    print(f"    CAGR: {m['cagr']:+.1%}  |  Sharpe: {m['sharpe']:.2f}  |  Max DD: {m['max_dd']:.1%}")
    print(f"\n  Shares-Only B&H: CAGR {m['shares_only_cagr']:+.1%}")
    print(f"  Added by Options: {m['cagr'] - m['shares_only_cagr']:+.1%} CAGR")
    print(f"\n  Avg Effective Leverage: {m['avg_leverage']:.2f}x (capped at ~2x)")
    print(f"{'=' * W}")


def print_comparison(m_a, m_b):
    """Side-by-side comparison of entry-only vs thresh-exit."""
    W = 95
    label_a = m_a["label"]
    label_b = m_b["label"]

    print(f"\n{'=' * W}")
    print("DELTA-CAPPED STRATEGY -- A vs B COMPARISON")
    print(f"{'=' * W}")
    print(f"  A = {label_a}")
    print(f"  B = {label_b}")

    # Portfolio Performance
    print(f"\n{'-' * W}")
    print("COMBINED PORTFOLIO PERFORMANCE")
    print(f"{'-' * W}")

    rows = [
        ("Ending value", f"${m_a['end_val']:>12,.0f}", f"${m_b['end_val']:>12,.0f}"),
        ("Total return", f"{m_a['total_return']:>+12.1%}", f"{m_b['total_return']:>+12.1%}"),
        ("CAGR", f"{m_a['cagr']:>+12.1%}", f"{m_b['cagr']:>+12.1%}"),
        ("Sharpe", f"{m_a['sharpe']:>12.2f}", f"{m_b['sharpe']:>12.2f}"),
        ("Sortino", f"{m_a['sortino']:>12.2f}", f"{m_b['sortino']:>12.2f}"),
        ("Max drawdown", f"{m_a['max_dd']:>12.1%}", f"{m_b['max_dd']:>12.1%}"),
        ("Shares-only CAGR", f"{m_a['shares_only_cagr']:>+12.1%}", f"{m_b['shares_only_cagr']:>+12.1%}"),
        ("Alpha vs shares", f"{m_a['cagr'] - m_a['shares_only_cagr']:>+12.1%}",
                           f"{m_b['cagr'] - m_b['shares_only_cagr']:>+12.1%}"),
    ]

    print(f"  {'Metric':<25} {'A':>20} {'B':>20}")
    print(f"  {'-' * 67}")
    for name, va, vb in rows:
        print(f"  {name:<25} {va:>20} {vb:>20}")

    # Trade Stats
    ta = m_a["trades"]
    tb = m_b["trades"]
    if ta and tb:
        print(f"\n{'-' * W}")
        print("TRADE STATISTICS")
        print(f"{'-' * W}")
        trade_rows = [
            ("Total trades", f"{ta['n_trades']}", f"{tb['n_trades']}"),
            ("Win rate", f"{ta['win_rate']:.1%}", f"{tb['win_rate']:.1%}"),
            ("Mean return", f"{ta['mean_ret']:+.1%}", f"{tb['mean_ret']:+.1%}"),
            ("Total P&L", f"${ta['total_pnl']:+,.0f}", f"${tb['total_pnl']:+,.0f}"),
            ("PT exits", f"{ta['pt_exits']}", f"{tb['pt_exits']}"),
            ("SMA exits", f"{ta['sma_exits']}", f"{tb['sma_exits']}"),
        ]
        print(f"  {'Metric':<25} {'A':>20} {'B':>20}")
        print(f"  {'-' * 67}")
        for name, va, vb in trade_rows:
            print(f"  {name:<25} {va:>20} {vb:>20}")

    # Delta Stats
    print(f"\n{'-' * W}")
    print("DELTA EXPOSURE")
    print(f"{'-' * W}")
    delta_rows = [
        ("Avg options delta", f"{m_a['avg_options_delta']:,.0f}", f"{m_b['avg_options_delta']:,.0f}"),
        ("Max options delta", f"{m_a['max_options_delta']:,.0f}", f"{m_b['max_options_delta']:,.0f}"),
        ("Avg leverage", f"{m_a['avg_leverage']:.2f}x", f"{m_b['avg_leverage']:.2f}x"),
    ]
    print(f"  {'Metric':<25} {'A':>20} {'B':>20}")
    print(f"  {'-' * 67}")
    for name, va, vb in delta_rows:
        print(f"  {name:<25} {va:>20} {vb:>20}")

    # Year-by-Year
    print(f"\n{'-' * W}")
    print("YEAR-BY-YEAR")
    print(f"{'-' * W}")
    all_years = sorted(set(list(m_a["yearly"].keys()) + list(m_b["yearly"].keys())))
    print(f"\n  {'Year':<6}  {'--- A ---':^20}  {'--- B ---':^20}  {'Shares':>8}")
    print(f"  {'':6}  {'Return':>8} {'Alpha':>10}  {'Return':>8} {'Alpha':>10}  {'':>8}")
    print(f"  {'-' * 65}")

    for year in all_years:
        ya = m_a["yearly"].get(year, {})
        yb = m_b["yearly"].get(year, {})
        ra = ya.get("ret", 0)
        aa = ya.get("alpha", 0)
        rb = yb.get("ret", 0)
        ab = yb.get("alpha", 0)
        sr = ya.get("shares_ret", yb.get("shares_ret", 0))
        print(f"  {year:<6}  {ra:>+7.1%} {aa:>+9.1%}  {rb:>+7.1%} {ab:>+9.1%}  {sr:>+7.1%}")

    # Summary
    print(f"\n{'=' * W}")
    print("SUMMARY")
    print(f"{'=' * W}")
    for m in [m_a, m_b]:
        print(f"  {m['label']}:")
        print(f"    CAGR: {m['cagr']:+.1%}  |  Sharpe: {m['sharpe']:.2f}  |  Max DD: {m['max_dd']:.1%}")
        print(f"    Alpha vs shares-only: {m['cagr'] - m['shares_only_cagr']:+.1%}")
    print(f"\n  Shares-only B&H: CAGR {m_a['shares_only_cagr']:+.1%}")
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
    print("Delta-Capped 80-Delta Call Strategy Backtest")
    print("=" * 80)
    print(f"\nModeling {SHARES:,} SPY shares + options capped at {SHARES:,} delta")
    print(f"Options cash allocation: ${OPTIONS_CASH_ALLOCATION:,}")
    print(f"Covered calls when below SMA: {CC_DELTA:.0%}-delta, ~{CC_DTE_TARGET} DTE")
    print()

    client = ThetaDataClient()
    if not client.connect():
        print("\nERROR: Cannot connect to Theta Terminal.")
        return

    print("Connected to Theta Terminal.\n")

    spy_by_date, trading_dates, vix_data, sma200, monthly_exps, trailing_12m_returns, rolling_volatility, spy_dividends = load_all_data(client)

    # Run A: Thresh-exit WITHOUT covered calls (baseline)
    snaps_a, trades_a, cc_trades_a = run_delta_capped_simulation(
        client, spy_by_date, trading_dates, vix_data, sma200, monthly_exps,
        trailing_12m_returns=trailing_12m_returns,
        rolling_volatility=rolling_volatility,
        spy_dividends=spy_dividends,
        force_exit_below_sma=True,
        sell_covered_calls=False,
        label="A: Thresh-exit (no CC)",
    )

    # Run B: Thresh-exit WITH covered calls
    snaps_b, trades_b, cc_trades_b = run_delta_capped_simulation(
        client, spy_by_date, trading_dates, vix_data, sma200, monthly_exps,
        trailing_12m_returns=trailing_12m_returns,
        rolling_volatility=rolling_volatility,
        spy_dividends=spy_dividends,
        force_exit_below_sma=True,
        sell_covered_calls=True,
        label="B: Thresh-exit + Covered Calls",
    )

    if not snaps_a or not snaps_b:
        print("\nInsufficient data.")
        return

    m_a = compute_metrics(snaps_a, trades_a, cc_trades_a, "A: Thresh-exit (no CC)")
    m_b = compute_metrics(snaps_b, trades_b, cc_trades_b, "B: Thresh-exit + Covered Calls")

    # Print results for config WITH covered calls
    print_results(m_b)

    # Print comparison
    print_comparison(m_a, m_b)

    client.close()


if __name__ == "__main__":
    main()
