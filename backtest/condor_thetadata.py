#!/usr/bin/env python3
"""
ThetaData Iron Condor Backtester (2012-2026)
=============================================
Uses real historical bid/ask quotes from ThetaData for option pricing,
with Black-Scholes synthetic fallback for Jan-May 2012 when ThetaData
coverage is unavailable.

Replaces the Massive/Polygon-based condor_real_data.py with ThetaData
as the data source, extending coverage back to June 2012.

Usage:
    python condor_thetadata.py                         # full 2012-2026
    python condor_thetadata.py --year 2024             # single year smoke test
    python condor_thetadata.py --start 2020 --end 2025 # date range
    python condor_thetadata.py --synthetic-only         # B-S only (no ThetaData)
    python condor_thetadata.py --call-delta-offset -0.05  # asymmetric call wing
    python condor_thetadata.py --export-csv trades.csv  # export results
"""

import os
import sys
import math
import time
import csv
import logging
import argparse
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_this_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)
if _this_dir not in sys.path:
    sys.path.insert(0, _this_dir)

from backtest.black_scholes import (
    find_strike_for_delta,
    calculate_iv_rank,
    calculate_condor_price_realistic,
    price_condor_to_close,
    black_scholes_price,
)
from backtest.thetadata_client import ThetaDataClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("condor_theta")

# ---------------------------------------------------------------------------
# Strategy constants
# ---------------------------------------------------------------------------
IV_RANK_LOW = 0.15             # IV rank floor (was 0.30)
IV_RANK_MED = 0.50
IV_RANK_HIGH = 0.70
VIX_ABS_FLOOR = 18.0           # minimum absolute VIX level to enter

DELTA_BY_IV_TIER = {
    "low": None,
    "medium": 0.20,
    "high": 0.25,
    "very_high": 0.30,
}

DEFAULT_FLAT_DELTA = 0.20      # flat delta override (0 = use tier-based)

WING_WIDTH_PCT = 0.06          # 6% of spot (was 3%)
MIN_WING_WIDTH = 5.0           # minimum wing width in dollars
MAX_CREDIT_RATIO = 0.60        # reject if credit > 60% of wing width
ENTRY_INTERVAL = 5             # every 5 trading days
DTE_TARGET = 45                # 45 DTE entry (was 30)
DTE_MIN = 35
DTE_MAX = 55
DTE_EXIT = 21                  # time-based exit at 21 DTE
RISK_FREE_RATE = 0.05
TAKE_PROFIT_PCT = 0.25         # close when cost-to-close <= 25% of credit (i.e. 75% of credit captured)
STOP_LOSS_MULT = 3.0           # stop at 3x credit received (was 75% of max loss)
SYNTHETIC_SPREAD_PCT = 0.05    # 5% synthetic spread for Jan-May 2012
SMA_PERIOD = 200               # 200-day SMA trend filter

# Transaction costs
COMMISSION_PER_CONTRACT = 2.60  # $0.65/leg x 4 legs (one side; round-trip = x2 = $5.20)
SLIPPAGE_PER_SHARE = 0.02      # $0.02/share slippage per leg

# ThetaData coverage starts June 2012
THETADATA_START = "2012-06-01"

VIX_BUCKETS = [
    ("Very Low",  0, 15),
    ("Low",      15, 20),
    ("Medium",   20, 25),
    ("High",     25, 30),
    ("Very High", 30, 100),
]


# ===================================================================
# Step 1 -- IV Rank and Delta Selection
# ===================================================================

def compute_vix_iv_rank(vix_today: float, vix_history: Dict[str, float],
                        as_of: str, lookback: int = 252) -> Optional[float]:
    """IV Rank using VIX history (252-day range)."""
    sorted_dates = sorted(d for d in vix_history if d <= as_of)
    if len(sorted_dates) < 20:
        return None
    trail = [vix_history[d] for d in sorted_dates[-lookback:]]
    return calculate_iv_rank(vix_today, trail, lookback)


def select_delta_tier(iv_rank: float, flat_delta: float = 0.0,
                      iv_rank_low: float = None) -> Tuple[Optional[float], str]:
    """Select short delta and tier name from IV rank."""
    floor = iv_rank_low if iv_rank_low is not None else IV_RANK_LOW
    if iv_rank < floor:
        return None, "low"
    if flat_delta > 0:
        if iv_rank < IV_RANK_MED:
            return flat_delta, "medium"
        elif iv_rank < IV_RANK_HIGH:
            return flat_delta, "high"
        else:
            return flat_delta, "very_high"
    if iv_rank < IV_RANK_MED:
        return DELTA_BY_IV_TIER["medium"], "medium"
    elif iv_rank < IV_RANK_HIGH:
        return DELTA_BY_IV_TIER["high"], "high"
    else:
        return DELTA_BY_IV_TIER["very_high"], "very_high"


# ===================================================================
# Step 2 -- Strike Construction
# ===================================================================

def build_condor_strikes(spot: float, vix: float,
                         put_delta: float,
                         call_delta: float = None,
                         wing_width_pct: float = None,
                         put_wing_mult: float = 1.0,
                         call_wing_mult: float = 1.0,
                         mode: str = "condor",
                         dte_target: int = DTE_TARGET) -> Optional[Dict[str, float]]:
    """
    Build target strikes using B-S delta targeting.

    Args:
        spot: Current underlying price
        vix: VIX level (percentage, e.g. 20 for 20%)
        put_delta: Delta for put-side short strike (e.g. 0.20)
        call_delta: Delta for call-side short strike. If None, uses put_delta.
        wing_width_pct: Override wing width (fraction of spot). Defaults to WING_WIDTH_PCT.
        put_wing_mult: Multiplier for put wing width (>1.0 for broken-wing condor)
        call_wing_mult: Multiplier for call wing width
        mode: "condor" (4 legs), "put_only" (2 legs), or "call_only" (2 legs)
        dte_target: Target days to expiration for delta calculation

    Returns dict with strike keys, or None if delta solve fails.
    """
    if call_delta is None:
        call_delta = put_delta
    if wing_width_pct is None:
        wing_width_pct = WING_WIDTH_PCT

    vix_decimal = vix / 100.0
    dte_years = dte_target / 365.0
    base_wing = max(round(spot * wing_width_pct), MIN_WING_WIDTH)

    result = {}

    # Build put side
    if mode in ("condor", "put_only"):
        sp_raw = find_strike_for_delta(spot, dte_years, RISK_FREE_RATE,
                                       vix_decimal, -put_delta, "P")
        if sp_raw is None:
            return None
        sp_strike = round(sp_raw)
        if sp_strike >= spot:
            return None
        put_wing = max(round(base_wing * put_wing_mult), MIN_WING_WIDTH)
        lp_strike = sp_strike - put_wing
        if lp_strike >= sp_strike:
            return None
        result["short_put"] = float(sp_strike)
        result["long_put"] = float(lp_strike)

    # Build call side
    if mode in ("condor", "call_only"):
        sc_raw = find_strike_for_delta(spot, dte_years, RISK_FREE_RATE,
                                       vix_decimal, call_delta, "C")
        if sc_raw is None:
            return None
        sc_strike = round(sc_raw)
        if sc_strike <= spot:
            return None
        call_wing = max(round(base_wing * call_wing_mult), MIN_WING_WIDTH)
        lc_strike = sc_strike + call_wing
        if lc_strike <= sc_strike:
            return None
        result["short_call"] = float(sc_strike)
        result["long_call"] = float(lc_strike)

    return result if result else None


def validate_and_snap_strikes(client: ThetaDataClient, targets: Dict[str, float],
                               root: str, expiration: str,
                               spot: float = None) -> Optional[Dict[str, float]]:
    """
    Snap target strikes to nearest real ThetaData strikes.
    Supports 2-leg (put_only/call_only) and 4-leg (condor) configurations.

    Returns snapped strikes dict or None if validation fails.
    """
    snapped = {}
    for leg in ("long_put", "short_put", "short_call", "long_call"):
        if leg not in targets:
            continue
        s = client.snap_strike(root, expiration, targets[leg])
        if s is None:
            return None
        snapped[leg] = s

    has_puts = "short_put" in snapped and "long_put" in snapped
    has_calls = "short_call" in snapped and "long_call" in snapped

    # Validate put side
    if has_puts:
        if snapped["long_put"] >= snapped["short_put"]:
            return None
        if spot is not None and snapped["short_put"] >= spot:
            return None
        if snapped["short_put"] - snapped["long_put"] < MIN_WING_WIDTH:
            return None

    # Validate call side
    if has_calls:
        if snapped["short_call"] >= snapped["long_call"]:
            return None
        if spot is not None and snapped["short_call"] <= spot:
            return None
        if snapped["long_call"] - snapped["short_call"] < MIN_WING_WIDTH:
            return None

    # Validate condor structure (puts below calls)
    if has_puts and has_calls:
        if snapped["short_put"] >= snapped["short_call"]:
            return None

    return snapped


# ===================================================================
# Step 3 -- ThetaData Pricing
# ===================================================================

def price_condor_entry_thetadata(client: ThetaDataClient, root: str,
                                  expiration: str, strikes: Dict[str, float],
                                  entry_date: str) -> Optional[Dict]:
    """
    Price entry using real ThetaData bid/ask quotes.
    Supports 2-leg (put_only/call_only) and 4-leg (condor) configurations.

    Pre-fetches all quotes through expiration for later daily management.
    Returns dict with credit, max_loss, or None if data missing.
    """
    leg_defs = {
        "long_put":   ("P",), "short_put":  ("P",),
        "short_call": ("C",), "long_call":  ("C",),
    }
    legs = {}
    for leg_name, (right,) in leg_defs.items():
        if leg_name in strikes:
            legs[leg_name] = (right, strikes[leg_name])

    # Pre-fetch EOD data for all 4 legs (entry through expiration)
    prefetched = {}
    for leg_name, (right, strike) in legs.items():
        eod_data = client.prefetch_option_life(root, expiration, strike, right, entry_date)
        prefetched[leg_name] = eod_data

    # Read entry-date bid/ask for each leg
    # If exact entry date not available, use earliest available date from prefetch
    entry_quotes = {}
    for leg_name, (right, strike) in legs.items():
        q = client.get_bid_ask(root, expiration, strike, right, entry_date)
        if q is None or q["bid"] <= 0 or q["ask"] <= 0:
            # Try next few trading days as fallback
            dt = datetime.strptime(entry_date, "%Y-%m-%d").date()
            found = False
            for offset in range(1, 6):
                alt = (dt + timedelta(days=offset)).isoformat()
                q = client.get_bid_ask(root, expiration, strike, right, alt)
                if q and q["bid"] > 0 and q["ask"] > 0:
                    found = True
                    break
            # If still not found, use earliest row from prefetch
            if not found and prefetched[leg_name]:
                for row in prefetched[leg_name]:
                    bid = row.get("bid", 0)
                    ask = row.get("ask", 0)
                    if bid > 0 and ask > 0:
                        q = {"bid": bid, "ask": ask, "quote_date": row["bar_date"]}
                        found = True
                        break
            if not found:
                return None
        entry_quotes[leg_name] = q

    # Credit = sell shorts at bid, buy longs at ask
    credit = 0.0
    for leg_name in ("short_put", "short_call"):
        if leg_name in entry_quotes:
            credit += entry_quotes[leg_name]["bid"]
    for leg_name in ("long_put", "long_call"):
        if leg_name in entry_quotes:
            credit -= entry_quotes[leg_name]["ask"]

    if credit <= 0:
        return None

    put_width = (strikes["short_put"] - strikes["long_put"]) if "short_put" in strikes else 0
    call_width = (strikes["long_call"] - strikes["short_call"]) if "short_call" in strikes else 0
    max_width = max(put_width, call_width)

    # Reject if credit exceeds max ratio of wing width
    # (indicates ITM legs or stale/crossed quotes)
    if max_width > 0 and credit / max_width > MAX_CREDIT_RATIO:
        return None

    max_loss = (max_width - credit) * 100  # per contract

    return {
        "credit": round(credit, 4),
        "max_loss": round(max_loss, 2),
        "put_width": put_width,
        "call_width": call_width,
        "data_source": "thetadata",
        "entry_quotes": entry_quotes,
    }


# ===================================================================
# Step 4 -- Synthetic (B-S) Pricing (Jan-May 2012 fallback)
# ===================================================================

def price_condor_entry_synthetic(spot: float, strikes: Dict[str, float],
                                  vix: float, dte: int) -> Optional[Dict]:
    """
    Price condor/spread entry using Black-Scholes with synthetic bid/ask spread.
    Used for dates before ThetaData coverage (Jan-May 2012).
    Supports 2-leg (put_only/call_only) and 4-leg (condor) configurations.
    """
    vix_decimal = vix / 100.0
    t_years = dte / 365.0

    has_puts = "short_put" in strikes and "long_put" in strikes
    has_calls = "short_call" in strikes and "long_call" in strikes

    if has_puts and has_calls:
        # Full condor - use existing function
        result = calculate_condor_price_realistic(
            spot,
            strikes["long_put"],
            strikes["short_put"],
            strikes["short_call"],
            strikes["long_call"],
            t_years,
            RISK_FREE_RATE,
            vix_decimal,
            bid_ask_spread_pct=SYNTHETIC_SPREAD_PCT,
            use_skew=True,
        )
        if result is None or result["open_credit"] <= 0:
            return None
        credit = result["open_credit"]
        put_width = result["put_width"]
        call_width = result["call_width"]
    else:
        # 2-leg spread - price with B-S directly
        credit = 0.0
        if has_puts:
            sp_price = black_scholes_price(spot, strikes["short_put"], t_years,
                                           RISK_FREE_RATE, vix_decimal, "P")
            lp_price = black_scholes_price(spot, strikes["long_put"], t_years,
                                           RISK_FREE_RATE, vix_decimal, "P")
            mid_credit = sp_price - lp_price
            credit = mid_credit * (1 - SYNTHETIC_SPREAD_PCT)
        if has_calls:
            sc_price = black_scholes_price(spot, strikes["short_call"], t_years,
                                           RISK_FREE_RATE, vix_decimal, "C")
            lc_price = black_scholes_price(spot, strikes["long_call"], t_years,
                                           RISK_FREE_RATE, vix_decimal, "C")
            mid_credit = sc_price - lc_price
            credit = mid_credit * (1 - SYNTHETIC_SPREAD_PCT)
        put_width = (strikes["short_put"] - strikes["long_put"]) if has_puts else 0
        call_width = (strikes["long_call"] - strikes["short_call"]) if has_calls else 0

    if credit <= 0:
        return None

    max_width = max(put_width, call_width)
    if max_width > 0 and credit / max_width > MAX_CREDIT_RATIO:
        return None

    max_loss = (max_width - credit) * 100

    return {
        "credit": round(credit, 4),
        "max_loss": round(max_loss, 2),
        "put_width": put_width,
        "call_width": call_width,
        "data_source": "synthetic",
    }


# ===================================================================
# Step 5 -- Daily Re-pricing
# ===================================================================

def price_condor_on_date_thetadata(client: ThetaDataClient, root: str,
                                    expiration: str, strikes: Dict[str, float],
                                    price_date: str) -> Optional[float]:
    """
    Re-price condor/spread on a given date using cached ThetaData quotes.
    Supports 2-leg (put_only/call_only) and 4-leg (condor) configurations.

    Returns cost to close (debit) or None if data missing.
    All data should already be cached from prefetch_option_life.
    """
    leg_defs = {
        "long_put":   ("P",), "short_put":  ("P",),
        "short_call": ("C",), "long_call":  ("C",),
    }
    legs = {}
    for leg_name, (right,) in leg_defs.items():
        if leg_name in strikes:
            legs[leg_name] = (right, strikes[leg_name])

    quotes = {}
    for leg_name, (right, strike) in legs.items():
        q = client.get_bid_ask(root, expiration, strike, right, price_date)
        if q is None or (q["bid"] <= 0 and q["ask"] <= 0):
            return None
        quotes[leg_name] = q

    # Cost to close = buy shorts at ask, sell longs at bid
    close_debit = 0.0
    for leg_name in ("short_put", "short_call"):
        if leg_name in quotes:
            close_debit += quotes[leg_name]["ask"]
    for leg_name in ("long_put", "long_call"):
        if leg_name in quotes:
            close_debit -= quotes[leg_name]["bid"]

    return max(0.0, close_debit)


def price_condor_on_date_synthetic(spot: float, strikes: Dict[str, float],
                                    vix: float, dte: int) -> Optional[float]:
    """
    Re-price condor/spread on a given date using Black-Scholes.
    Supports 2-leg (put_only/call_only) and 4-leg (condor) configurations.
    Used for synthetic path (Jan-May 2012).
    """
    if dte <= 0:
        return None

    vix_decimal = vix / 100.0
    t_years = dte / 365.0

    has_puts = "short_put" in strikes and "long_put" in strikes
    has_calls = "short_call" in strikes and "long_call" in strikes

    if has_puts and has_calls:
        # Full condor - use existing function
        close_cost = price_condor_to_close(
            spot,
            strikes["long_put"],
            strikes["short_put"],
            strikes["short_call"],
            strikes["long_call"],
            t_years,
            RISK_FREE_RATE,
            vix_decimal,
            bid_ask_spread_pct=SYNTHETIC_SPREAD_PCT,
            use_skew=True,
        )
        return close_cost

    # 2-leg spread - cost to close = buy short at ask, sell long at bid
    close_cost = 0.0
    if has_puts:
        sp_price = black_scholes_price(spot, strikes["short_put"], t_years,
                                       RISK_FREE_RATE, vix_decimal, "P")
        lp_price = black_scholes_price(spot, strikes["long_put"], t_years,
                                       RISK_FREE_RATE, vix_decimal, "P")
        # close = buy back short at ask, sell long at bid
        close_cost = sp_price * (1 + SYNTHETIC_SPREAD_PCT) - lp_price * (1 - SYNTHETIC_SPREAD_PCT)
    if has_calls:
        sc_price = black_scholes_price(spot, strikes["short_call"], t_years,
                                       RISK_FREE_RATE, vix_decimal, "C")
        lc_price = black_scholes_price(spot, strikes["long_call"], t_years,
                                       RISK_FREE_RATE, vix_decimal, "C")
        close_cost = sc_price * (1 + SYNTHETIC_SPREAD_PCT) - lc_price * (1 - SYNTHETIC_SPREAD_PCT)

    return max(0.0, close_cost)


# ===================================================================
# Step 6 -- Intrinsic Settlement
# ===================================================================

def intrinsic_settlement(spot: float, strikes: Dict[str, float]) -> float:
    """Compute settlement value at expiration from intrinsic values.
    Supports 2-leg (put_only/call_only) and 4-leg (condor) configurations."""
    settlement = 0.0
    if "short_put" in strikes:
        settlement += max(0, strikes["short_put"] - spot)
    if "long_put" in strikes:
        settlement -= max(0, strikes["long_put"] - spot)
    if "short_call" in strikes:
        settlement += max(0, spot - strikes["short_call"])
    if "long_call" in strikes:
        settlement -= max(0, spot - strikes["long_call"])
    return max(0.0, settlement)


# ===================================================================
# Step 7 -- Trade Simulation
# ===================================================================

def simulate_condor_trade(
    client: ThetaDataClient,
    entry_date: str,
    spot: float,
    vix: float,
    vix_history: Dict[str, float],
    spy_bars: List[Dict],
    bar_idx_map: Dict[str, int],
    use_synthetic: bool = False,
    call_delta_offset: float = 0.0,
    flat_delta: float = DEFAULT_FLAT_DELTA,
    iv_rank_low: float = None,
    sma_value: Optional[float] = None,
    sma_period: int = SMA_PERIOD,
    wing_width_pct: float = None,
    put_wing_mult: float = 1.0,
    call_wing_mult: float = 1.0,
    stop_loss_mult: float = STOP_LOSS_MULT,
    take_profit_pct: float = TAKE_PROFIT_PCT,
    vix_abs_floor: float = VIX_ABS_FLOOR,
    root: str = "SPY",
    commission_per_contract: float = COMMISSION_PER_CONTRACT,
    slippage_per_share: float = SLIPPAGE_PER_SHARE,
    mode: str = "condor",
    dte_target: int = DTE_TARGET,
    dte_min: int = DTE_MIN,
    dte_max: int = DTE_MAX,
    dte_exit: int = DTE_EXIT,
) -> Optional[Dict]:
    """
    Simulate a single iron condor trade.

    Features:
        - SMA trend filter: above SMA = full condor, below SMA = skip (returns "skip_sma")
        - VIX absolute floor: skip if VIX < vix_abs_floor
        - Flat delta: fixed delta across all IV tiers
        - Credit-based stop loss (3x credit)
        - Time-based exit at DTE_EXIT days remaining
        - Transaction costs (commission + slippage)
        - Asymmetric (broken-wing) support via put_wing_mult/call_wing_mult

    Returns a result dict, "skip_sma", "skip_vix", or None.
    """

    # 0. VIX absolute floor check
    if vix < vix_abs_floor:
        return "skip_vix"

    # 1. IV rank from VIX history
    iv_rank = compute_vix_iv_rank(vix, vix_history, entry_date)
    if iv_rank is None:
        return None

    # 2. Delta selection (with flat delta support)
    short_delta, iv_tier = select_delta_tier(iv_rank, flat_delta=flat_delta,
                                              iv_rank_low=iv_rank_low)
    if short_delta is None:
        return None  # IV too low

    # 2b. Asymmetric call delta
    put_delta = short_delta
    call_delta = short_delta + call_delta_offset
    if call_delta <= 0:
        return None  # offset too aggressive

    # 3. Build target strikes via B-S delta
    targets = build_condor_strikes(spot, vix, put_delta, call_delta,
                                    wing_width_pct=wing_width_pct,
                                    put_wing_mult=put_wing_mult,
                                    call_wing_mult=call_wing_mult,
                                    mode=mode,
                                    dte_target=dte_target)
    if targets is None:
        return None

    # 4. Determine data path
    is_synthetic = use_synthetic or entry_date < THETADATA_START

    if is_synthetic:
        strikes = targets
        dte = dte_target
        expiration_date = (datetime.strptime(entry_date, "%Y-%m-%d").date()
                          + timedelta(days=dte)).isoformat()

        pricing = price_condor_entry_synthetic(spot, strikes, vix, dte)
        if pricing is None:
            return None

    else:
        expiration = client.find_nearest_expiration(root, entry_date,
                                                     dte_target, dte_min, dte_max)
        if expiration is None:
            return None

        strikes = validate_and_snap_strikes(client, targets, root, expiration, spot)
        if strikes is None:
            return None

        expiration_date = expiration

        pricing = price_condor_entry_thetadata(client, root, expiration,
                                                strikes, entry_date)
        if pricing is None:
            return None

    credit = pricing["credit"]
    max_loss = pricing["max_loss"]
    data_source = pricing["data_source"]

    # Transaction costs (variable leg count)
    num_legs = len(strikes)  # 2 for put_only/call_only, 4 for condor
    leg_commission = commission_per_contract * num_legs / 4  # scale from 4-leg base
    entry_slippage = slippage_per_share * num_legs
    effective_credit = credit - entry_slippage
    exit_slippage = slippage_per_share * num_legs
    round_trip_commission = leg_commission * 2
    total_transaction_costs = (entry_slippage + exit_slippage) * 100 + round_trip_commission

    # Exit thresholds (credit-based stop loss)
    tp_target = credit * take_profit_pct
    sl_threshold = stop_loss_mult * credit

    # 5. Walk each trading day from entry+1 to expiration
    entry_idx = bar_idx_map.get(entry_date)
    if entry_idx is None:
        return None

    exit_date = None
    exit_reason = None
    exit_pnl = None
    side_breached = None

    exp_dt = datetime.strptime(expiration_date, "%Y-%m-%d").date()

    for i in range(entry_idx + 1, len(spy_bars)):
        bar = spy_bars[i]
        d = bar["bar_date"]
        d_dt = datetime.strptime(d, "%Y-%m-%d").date()

        if d_dt > exp_dt:
            break

        current_spot = bar["close"]
        days_remaining = (exp_dt - d_dt).days

        # On expiration day: intrinsic settlement
        if d_dt == exp_dt:
            settle_cost = intrinsic_settlement(current_spot, strikes)
            pnl = (effective_credit - settle_cost - exit_slippage) * 100 - round_trip_commission
            exit_date = d
            exit_reason = "expiration"
            exit_pnl = pnl
            if "short_put" in strikes and current_spot < strikes["short_put"]:
                side_breached = "put"
            elif "short_call" in strikes and current_spot > strikes["short_call"]:
                side_breached = "call"
            break

        # Daily re-pricing
        if is_synthetic:
            vix_today = vix_history.get(d)
            if vix_today is None:
                continue
            close_cost = price_condor_on_date_synthetic(
                current_spot, strikes, vix_today, days_remaining)
        else:
            close_cost = price_condor_on_date_thetadata(
                client, root, expiration_date, strikes, d)

        if close_cost is None:
            continue

        raw_pnl = (credit - close_cost) * 100

        # Take profit
        if close_cost <= tp_target:
            exit_date = d
            exit_reason = "take_profit"
            exit_pnl = (effective_credit - close_cost - exit_slippage) * 100 - round_trip_commission
            break

        # Stop loss (credit-based)
        if raw_pnl <= -(sl_threshold * 100):
            exit_date = d
            exit_reason = "stop_loss"
            exit_pnl = (effective_credit - close_cost - exit_slippage) * 100 - round_trip_commission
            if "short_put" in strikes and current_spot < strikes["short_put"]:
                side_breached = "put"
            elif "short_call" in strikes and current_spot > strikes["short_call"]:
                side_breached = "call"
            break

        # Time-based exit at dte_exit
        if days_remaining <= dte_exit:
            exit_date = d
            exit_reason = "time_exit"
            exit_pnl = (effective_credit - close_cost - exit_slippage) * 100 - round_trip_commission
            break

    # Fallback: settle at intrinsic using last bar before expiration
    if exit_date is None:
        for i in range(min(entry_idx + dte_max + 10, len(spy_bars) - 1),
                       entry_idx, -1):
            bar = spy_bars[i]
            d_dt = datetime.strptime(bar["bar_date"], "%Y-%m-%d").date()
            if d_dt <= exp_dt:
                settle_cost = intrinsic_settlement(bar["close"], strikes)
                exit_pnl = (effective_credit - settle_cost - exit_slippage) * 100 - round_trip_commission
                exit_date = bar["bar_date"]
                exit_reason = "expiration_fallback"
                if "short_put" in strikes and bar["close"] < strikes["short_put"]:
                    side_breached = "put"
                elif "short_call" in strikes and bar["close"] > strikes["short_call"]:
                    side_breached = "call"
                break

    if exit_pnl is None:
        return None

    dte_actual = (exp_dt - datetime.strptime(entry_date, "%Y-%m-%d").date()).days

    return {
        "entry_date": entry_date,
        "expiration": expiration_date,
        "exit_date": exit_date,
        "spot": spot,
        "vix": vix,
        "iv_rank": round(iv_rank, 4),
        "iv_tier": iv_tier,
        "short_delta": short_delta,
        "put_delta": put_delta,
        "call_delta": round(call_delta, 4),
        "call_delta_offset": call_delta_offset,
        "long_put": strikes.get("long_put"),
        "short_put": strikes.get("short_put"),
        "short_call": strikes.get("short_call"),
        "long_call": strikes.get("long_call"),
        "credit": credit,
        "max_loss": max_loss,
        "pnl": round(exit_pnl, 2),
        "won": exit_pnl > 0,
        "transaction_costs": round(total_transaction_costs, 2),
        "exit_reason": exit_reason,
        "side_breached": side_breached,
        "put_width": pricing["put_width"],
        "call_width": pricing["call_width"],
        "data_source": data_source,
        "dte": dte_actual,
        "mode": mode,
        "sma_value": round(sma_value, 2) if sma_value is not None else None,
    }


# ===================================================================
# Step 8 -- Main Backtest Loop
# ===================================================================

def check_sma_filter(spy_bars: List[Dict], idx: int, sma_period: int) -> Tuple[bool, Optional[float]]:
    """
    Check if current price is above its SMA.
    Returns (passes_filter, sma_value).
    If sma_period <= 0, returns (True, None) (filter disabled).
    """
    if sma_period <= 0:
        return True, None
    if idx < sma_period:
        return False, None
    closes = [spy_bars[i]["close"] for i in range(idx - sma_period + 1, idx + 1)]
    sma = sum(closes) / len(closes)
    return spy_bars[idx]["close"] > sma, sma


def run_backtest(start_year: int = 2012, end_year: int = 2026,
                 synthetic_only: bool = False,
                 call_delta_offset: float = 0.0,
                 flat_delta: float = DEFAULT_FLAT_DELTA,
                 iv_rank_low: float = None,
                 sma_period: int = SMA_PERIOD,
                 wing_width_pct: float = None,
                 put_wing_mult: float = 1.0,
                 call_wing_mult: float = 1.0,
                 stop_loss_mult: float = STOP_LOSS_MULT,
                 take_profit_pct: float = TAKE_PROFIT_PCT,
                 vix_abs_floor: float = VIX_ABS_FLOOR,
                 root: str = "SPY",
                 commission_per_contract: float = COMMISSION_PER_CONTRACT,
                 slippage_per_share: float = SLIPPAGE_PER_SHARE,
                 mode: str = "condor",
                 dte_target: int = DTE_TARGET,
                 dte_min: int = DTE_MIN,
                 dte_max: int = DTE_MAX,
                 dte_exit: int = DTE_EXIT) -> tuple:
    """
    Run the full ThetaData iron condor backtest.

    Args:
        start_year: First year to backtest
        end_year: Last year to backtest
        synthetic_only: If True, use B-S for all dates (no ThetaData needed)
        call_delta_offset: Offset applied to call delta (e.g. -0.05)
        flat_delta: Fixed delta for all tiers (0 = use tier-based)
        iv_rank_low: IV rank floor override (None = use IV_RANK_LOW constant)
        sma_period: SMA trend filter period (0 = disabled)
        wing_width_pct: Wing width as fraction of spot (None = use WING_WIDTH_PCT)
        put_wing_mult: Put wing multiplier for broken-wing condors
        call_wing_mult: Call wing multiplier for broken-wing condors
        stop_loss_mult: Credit multiplier for stop loss (3.0 = stop at 3x credit)
        take_profit_pct: Close threshold as fraction of credit (0.25 = close when
            cost-to-close <= 25% of credit, capturing 75% profit)
        vix_abs_floor: Minimum VIX level to enter trades
        root: Ticker symbol
        commission_per_contract: Commission per contract per side
        slippage_per_share: Slippage per share per leg
    """
    start = f"{start_year}-01-01"
    end = f"{end_year}-12-31"
    effective_iv_rank_low = iv_rank_low if iv_rank_low is not None else IV_RANK_LOW

    mode_label = {"condor": "IRON CONDOR", "put_only": "PUT CREDIT SPREAD",
                   "call_only": "CALL CREDIT SPREAD",
                   "regime": "REGIME-BASED"}.get(mode, mode.upper())

    log.info("=" * 70)
    log.info("THETADATA %s BACKTEST  --  %s  %s to %s", mode_label, root, start, end)
    data_mode = "Synthetic only (Black-Scholes)" if synthetic_only else "ThetaData real quotes + synthetic fallback"
    log.info("  MODE: %s (%s)", mode, data_mode)
    log.info("  DELTA:      %s", f"flat {flat_delta:.2f}" if flat_delta > 0 else "tier-based")
    log.info("  SMA PERIOD: %s", f"{sma_period}-day" if sma_period > 0 else "OFF")
    log.info("  STOP LOSS:  %.1fx credit", stop_loss_mult)
    log.info("  TAKE PROFIT: close when cost-to-close <= %.0f%% of credit (%.0f%% captured)",
             take_profit_pct * 100, (1 - take_profit_pct) * 100)
    log.info("  IV RANK:    >= %.0f%%", effective_iv_rank_low * 100)
    log.info("  VIX FLOOR:  >= %.0f", vix_abs_floor)
    log.info("  DTE TARGET: %d (exit at %d DTE)", dte_target, dte_exit)
    wwp = wing_width_pct if wing_width_pct is not None else WING_WIDTH_PCT
    log.info("  WING WIDTH: %.0f%% of spot", wwp * 100)
    if put_wing_mult != 1.0 or call_wing_mult != 1.0:
        log.info("  BROKEN WING: put=%.1fx, call=%.1fx", put_wing_mult, call_wing_mult)
    if call_delta_offset != 0.0:
        log.info("  CALL DELTA OFFSET: %+.2f (asymmetric)", call_delta_offset)
    log.info("=" * 70)

    # Initialize client
    client = ThetaDataClient()

    if not synthetic_only:
        if not client.connect():
            log.warning("Theta Terminal not available. Falling back to synthetic-only mode.")
            synthetic_only = True

    # Data fetch with lookback for SMA
    lookback_days = max(400, sma_period * 2) if sma_period > 0 else 400
    lookback_start = (datetime.strptime(start, "%Y-%m-%d").date()
                      - timedelta(days=lookback_days)).isoformat()

    vix_history = client.fetch_vix_history(lookback_start, end)
    if not vix_history:
        log.error("Cannot proceed without VIX data")
        return [], 0, 0, 0, 0

    spy_bars = client.fetch_ticker_bars(root, lookback_start, end)
    if not spy_bars:
        log.error("No %s bars available", root)
        return [], 0, 0, 0, 0

    bar_idx_map = {b["bar_date"]: i for i, b in enumerate(spy_bars)}

    # Find the start index in the bar array
    start_idx = 0
    for i, b in enumerate(spy_bars):
        if b["bar_date"] >= start:
            start_idx = i
            break

    log.info("%s bars: %d total (%s to %s), backtest starts at idx %d (%s)",
             root, len(spy_bars), spy_bars[0]["bar_date"], spy_bars[-1]["bar_date"],
             start_idx, spy_bars[start_idx]["bar_date"])

    # Walk dates
    trades: List[Dict] = []
    skipped_sma = 0
    skipped_iv = 0
    skipped_vix = 0
    skipped_data = 0
    last_entry_idx = start_idx - ENTRY_INTERVAL

    for idx in range(start_idx, len(spy_bars)):
        if idx - last_entry_idx < ENTRY_INTERVAL:
            continue

        bar = spy_bars[idx]
        entry_date = bar["bar_date"]

        if entry_date > end:
            break

        spot = bar["close"]

        # SMA trend filter / regime selection
        passes_sma, sma_value = check_sma_filter(spy_bars, idx, sma_period)
        if mode == "regime":
            # Regime mode: above SMA = put_only, below SMA = call_only
            if sma_value is None:
                skipped_sma += 1
                last_entry_idx = idx
                continue
            trade_mode = "put_only" if passes_sma else "call_only"
        else:
            # Standard modes: block if below SMA
            if not passes_sma:
                skipped_sma += 1
                last_entry_idx = idx
                continue
            trade_mode = mode

        # Get VIX for this date
        vix = vix_history.get(entry_date)
        if vix is None:
            dt = datetime.strptime(entry_date, "%Y-%m-%d").date()
            for offset in (-1, 1, -2, 2):
                alt = (dt + timedelta(days=offset)).isoformat()
                vix = vix_history.get(alt)
                if vix is not None:
                    break
        if vix is None:
            continue

        # IV rank pre-check
        iv_rank = compute_vix_iv_rank(vix, vix_history, entry_date)
        if iv_rank is not None and iv_rank < effective_iv_rank_low:
            skipped_iv += 1
            last_entry_idx = idx
            continue

        # Attempt trade
        result = simulate_condor_trade(
            client, entry_date, spot, vix, vix_history,
            spy_bars, bar_idx_map, use_synthetic=synthetic_only,
            call_delta_offset=call_delta_offset,
            flat_delta=flat_delta,
            iv_rank_low=effective_iv_rank_low,
            sma_value=sma_value,
            sma_period=sma_period,
            wing_width_pct=wing_width_pct,
            put_wing_mult=put_wing_mult,
            call_wing_mult=call_wing_mult,
            stop_loss_mult=stop_loss_mult,
            take_profit_pct=take_profit_pct,
            vix_abs_floor=vix_abs_floor,
            root=root,
            commission_per_contract=commission_per_contract,
            slippage_per_share=slippage_per_share,
            mode=trade_mode,
            dte_target=dte_target,
            dte_min=dte_min,
            dte_max=dte_max,
            dte_exit=dte_exit,
        )

        if result == "skip_sma":
            skipped_sma += 1
            last_entry_idx = idx
            continue

        if result == "skip_vix":
            skipped_vix += 1
            last_entry_idx = idx
            continue

        if result is None:
            skipped_data += 1
            last_entry_idx = idx
            continue

        trades.append(result)
        last_entry_idx = idx

        if len(trades) % 20 == 0:
            log.info("  ... %d trades so far (entry %s, source=%s)",
                     len(trades), entry_date, result["data_source"])

    log.info("Backtest complete: %d trades  (skip: %d SMA, %d IV, %d VIX, %d data)",
             len(trades), skipped_sma, skipped_iv, skipped_vix, skipped_data)

    client.close()
    return trades, skipped_sma, skipped_iv, skipped_vix, skipped_data


# ===================================================================
# Step 9 -- Reporting
# ===================================================================

def get_vix_bucket(vix: float) -> str:
    for name, lo, hi in VIX_BUCKETS:
        if lo <= vix < hi:
            return name
    return "Unknown"


def print_results(trades: List[Dict],
                  skipped_sma: int = 0,
                  skipped_iv: int = 0,
                  skipped_vix: int = 0,
                  skipped_data: int = 0) -> None:
    """Print comprehensive backtest results."""
    if not trades:
        print("\nNo trades to report.")
        return

    wins = [t for t in trades if t["won"]]
    losses = [t for t in trades if not t["won"]]
    total_pnl = sum(t["pnl"] for t in trades)
    avg_pnl = total_pnl / len(trades)
    win_rate = len(wins) / len(trades)
    avg_credit = sum(t["credit"] for t in trades) / len(trades)
    avg_max_loss = sum(t["max_loss"] for t in trades) / len(trades)

    # Detect asymmetric delta from trade data
    sample_offset = trades[0].get("call_delta_offset", 0.0)
    is_asymmetric = sample_offset != 0.0

    trade_mode = trades[0].get("mode", "condor")
    mode_label = {"condor": "IRON CONDOR", "put_only": "PUT CREDIT SPREAD",
                  "call_only": "CALL CREDIT SPREAD"}.get(trade_mode, trade_mode.upper())

    print()
    print("=" * 70)
    print(f"THETADATA {mode_label} RESULTS  --  SPY")
    if is_asymmetric:
        print(f"  Call Delta Offset: {sample_offset:+.2f} (asymmetric wings)")
    sma_val = trades[0].get("sma_value")
    if sma_val is not None:
        print(f"  SMA Filter:      200-day SMA (active)")
    print("=" * 70)
    print(f"  Period:          {trades[0]['entry_date']} to {trades[-1]['entry_date']}")
    print(f"  Total trades:    {len(trades)}")
    print(f"  Winners:         {len(wins)}  ({win_rate:.1%})")
    print(f"  Losers:          {len(losses)}")
    print(f"  Total P&L:       ${total_pnl:>+,.2f}")
    print(f"  Avg P&L/trade:   ${avg_pnl:>+,.2f}")
    print(f"  Avg credit:      ${avg_credit:.4f} /share")
    print(f"  Avg max loss:    ${avg_max_loss:,.2f} /contract")

    # Exit reasons
    reasons = defaultdict(int)
    for t in trades:
        reasons[t["exit_reason"]] += 1
    print(f"\n  Exit reasons:")
    for r, n in sorted(reasons.items(), key=lambda x: -x[1]):
        print(f"    {r:<22} {n:>4}  ({n/len(trades):.1%})")

    # Side breached
    put_breaches = sum(1 for t in trades if t["side_breached"] == "put")
    call_breaches = sum(1 for t in trades if t["side_breached"] == "call")
    print(f"\n  Put breaches:    {put_breaches}")
    print(f"  Call breaches:   {call_breaches}")

    # Transaction cost summary
    total_costs = sum(t.get("transaction_costs", 0) for t in trades)
    if total_costs > 0:
        avg_cost = total_costs / len(trades)
        print(f"\n  --- Transaction Costs ---")
        print(f"  Total costs:     ${total_costs:>,.2f}")
        print(f"  Avg cost/trade:  ${avg_cost:>,.2f}")
        if total_pnl != 0:
            print(f"  Cost % of P&L:   {total_costs / abs(total_pnl) * 100:.1f}%")

    # Filter statistics
    total_opportunities = len(trades) + skipped_sma + skipped_iv + skipped_vix + skipped_data
    if skipped_sma > 0:
        print(f"\n  --- Filter Statistics ---")
        print(f"  Blocked by SMA:  {skipped_sma}")
    if skipped_iv > 0:
        print(f"  Blocked by IV:   {skipped_iv}")
    if skipped_vix > 0:
        print(f"  Blocked by VIX:  {skipped_vix}")
    if skipped_data > 0:
        print(f"  Blocked by data: {skipped_data}")
    if total_opportunities > 0:
        print(f"  Trade rate:      {len(trades) / total_opportunities:.1%} of opportunities")

    # --- Data source breakdown ---
    print()
    print("-" * 70)
    print("DATA SOURCE BREAKDOWN")
    print("-" * 70)
    sources = defaultdict(int)
    for t in trades:
        sources[t["data_source"]] += 1
    for src, n in sorted(sources.items()):
        src_trades = [t for t in trades if t["data_source"] == src]
        src_pnl = sum(t["pnl"] for t in src_trades)
        src_wins = sum(1 for t in src_trades if t["won"])
        src_wr = src_wins / n if n > 0 else 0
        print(f"  {src:<15} {n:>4} trades   Win rate: {src_wr:.1%}   "
              f"P&L: ${src_pnl:>+,.2f}   Avg: ${src_pnl/n:>+,.2f}")

    # --- By IV tier ---
    print()
    print("-" * 70)
    print("PERFORMANCE BY IV TIER")
    print("-" * 70)
    print(f"{'Tier':<12} | {'Trades':>7} | {'Win Rate':>9} | {'Total P&L':>12} | {'Avg P&L':>10}")
    print("-" * 70)
    for tier in ("medium", "high", "very_high"):
        tier_t = [t for t in trades if t["iv_tier"] == tier]
        if not tier_t:
            print(f"{tier:<12} |     N/A |")
            continue
        tw = sum(1 for t in tier_t if t["won"])
        tp = sum(t["pnl"] for t in tier_t)
        print(f"{tier:<12} | {len(tier_t):>7} | {tw/len(tier_t):>8.1%} | "
              f"${tp:>+10,.2f} | ${tp/len(tier_t):>+9.2f}")

    # --- By VIX bucket ---
    print()
    print("-" * 70)
    print("PERFORMANCE BY VIX AT ENTRY")
    print("-" * 70)
    print(f"{'VIX Bucket':<12} | {'Range':^9} | {'Trades':>7} | {'Win Rate':>9} | "
          f"{'Total P&L':>12} | {'Avg P&L':>10}")
    print("-" * 70)

    total_vix_trades = 0
    total_vix_pnl = 0
    total_vix_wins = 0

    for bname, blo, bhi in VIX_BUCKETS:
        bt = [t for t in trades if blo <= t["vix"] < bhi]
        if not bt:
            rng = f"{blo}-{bhi}" if bhi < 100 else f"{blo}+"
            print(f"{bname:<12} | {rng:^9} |     N/A |")
            continue
        bw = sum(1 for t in bt if t["won"])
        bp = sum(t["pnl"] for t in bt)
        rng = f"{blo}-{bhi}" if bhi < 100 else f"{blo}+"
        print(f"{bname:<12} | {rng:^9} | {len(bt):>7} | {bw/len(bt):>8.1%} | "
              f"${bp:>+10,.2f} | ${bp/len(bt):>+9.2f}")
        total_vix_trades += len(bt)
        total_vix_pnl += bp
        total_vix_wins += bw

    print("-" * 70)
    if total_vix_trades > 0:
        print(f"{'TOTAL':<12} | {'':^9} | {total_vix_trades:>7} | "
              f"{total_vix_wins/total_vix_trades:>8.1%} | "
              f"${total_vix_pnl:>+10,.2f} | ${total_vix_pnl/total_vix_trades:>+9.2f}")

    # --- Year-over-year summary ---
    print()
    print("-" * 70)
    print("YEAR-OVER-YEAR SUMMARY")
    print("-" * 70)
    print(f"{'Year':<6} | {'Trades':>7} | {'Win Rate':>9} | {'Total P&L':>12} | "
          f"{'Avg P&L':>10} | {'Source':>12}")
    print("-" * 70)

    years = sorted(set(t["entry_date"][:4] for t in trades))
    for yr in years:
        yr_trades = [t for t in trades if t["entry_date"][:4] == yr]
        yr_wins = sum(1 for t in yr_trades if t["won"])
        yr_pnl = sum(t["pnl"] for t in yr_trades)
        yr_sources = set(t["data_source"] for t in yr_trades)
        src_str = "/".join(sorted(yr_sources))
        print(f"{yr:<6} | {len(yr_trades):>7} | {yr_wins/len(yr_trades):>8.1%} | "
              f"${yr_pnl:>+10,.2f} | ${yr_pnl/len(yr_trades):>+9.2f} | {src_str:>12}")

    # --- Monthly P&L timeline ---
    print()
    print("-" * 70)
    print("MONTHLY P&L TIMELINE")
    print("-" * 70)
    monthly: Dict[str, float] = defaultdict(float)
    monthly_n: Dict[str, int] = defaultdict(int)
    for t in trades:
        mo = t["entry_date"][:7]
        monthly[mo] += t["pnl"]
        monthly_n[mo] += 1

    for mo in sorted(monthly):
        bar_len = int(abs(monthly[mo]) / 50)
        bar_char = "+" if monthly[mo] >= 0 else "-"
        bar = bar_char * min(bar_len, 40)
        print(f"  {mo}  {monthly_n[mo]:>3} trades  ${monthly[mo]:>+9.2f}  {bar}")

    # --- Sample trades ---
    print()
    print("-" * 70)
    print("FIRST 10 TRADES (detail)")
    print("-" * 70)
    for i, t in enumerate(trades[:10]):
        print(f"\n  Trade #{i+1}:")
        print(f"    Entry:     {t['entry_date']}   SPY ${t['spot']:.2f}   VIX {t['vix']:.1f}")
        if is_asymmetric:
            print(f"    IV Rank:   {t['iv_rank']:.2f}  ({t['iv_tier']})  "
                  f"put_delta={t.get('put_delta', t['short_delta'])}  "
                  f"call_delta={t.get('call_delta', t['short_delta'])}")
        else:
            print(f"    IV Rank:   {t['iv_rank']:.2f}  ({t['iv_tier']})  delta={t['short_delta']}")
        strike_parts = []
        if t.get('long_put') is not None:
            strike_parts.append(f"LP {t['long_put']}")
        if t.get('short_put') is not None:
            strike_parts.append(f"SP {t['short_put']}")
        if t.get('short_call') is not None:
            strike_parts.append(f"SC {t['short_call']}")
        if t.get('long_call') is not None:
            strike_parts.append(f"LC {t['long_call']}")
        print(f"    Strikes:   {'  '.join(strike_parts)}")
        print(f"    Credit:    ${t['credit']:.4f}/sh   Max loss: ${t['max_loss']:.2f}/ct")
        print(f"    Exp:       {t['expiration']}  DTE={t['dte']}")
        print(f"    Exit:      {t['exit_date']}  reason={t['exit_reason']}")
        print(f"    P&L:       ${t['pnl']:>+.2f}  {'WIN' if t['won'] else 'LOSS'}"
              + (f"  ({t['side_breached']} breached)" if t['side_breached'] else ""))
        print(f"    Source:    {t['data_source']}")


def export_csv(trades: List[Dict], filepath: str) -> None:
    """Export trades to CSV file."""
    if not trades:
        print("No trades to export.")
        return

    fieldnames = [
        "entry_date", "expiration", "exit_date", "spot", "vix",
        "iv_rank", "iv_tier", "short_delta",
        "put_delta", "call_delta", "call_delta_offset",
        "long_put", "short_put", "short_call", "long_call",
        "credit", "max_loss", "pnl", "won", "exit_reason",
        "side_breached", "put_width", "call_width", "data_source", "dte", "mode",
    ]

    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for t in trades:
            writer.writerow({k: t.get(k, "") for k in fieldnames})

    print(f"\nExported {len(trades)} trades to {filepath}")


# ===================================================================
# CLI entry point
# ===================================================================

def main():
    parser = argparse.ArgumentParser(
        description="ThetaData iron condor backtest (2012-2026)")
    parser.add_argument("--year", type=int, default=None,
                        help="Run for a single year (e.g. 2024)")
    parser.add_argument("--start", type=int, default=None,
                        help="Start year (e.g. 2020)")
    parser.add_argument("--end", type=int, default=None,
                        help="End year (e.g. 2025)")
    parser.add_argument("--synthetic-only", action="store_true",
                        help="Use Black-Scholes only (no ThetaData needed)")
    parser.add_argument("--ticker", type=str, default="SPY",
                        metavar="SYM", help="Ticker symbol (default SPY)")
    parser.add_argument("--flat-delta", type=float, default=DEFAULT_FLAT_DELTA,
                        metavar="DELTA",
                        help=f"Fixed delta for all tiers "
                             f"(default {DEFAULT_FLAT_DELTA}, 0 = tier-based)")
    parser.add_argument("--sma-period", type=int, default=SMA_PERIOD,
                        metavar="PERIOD",
                        help=f"SMA trend filter period (default {SMA_PERIOD}, 0 = disabled)")
    parser.add_argument("--stop-loss-mult", type=float, default=STOP_LOSS_MULT,
                        metavar="MULT",
                        help=f"Stop loss as multiple of credit (default {STOP_LOSS_MULT})")
    parser.add_argument("--take-profit-pct", type=float, default=TAKE_PROFIT_PCT * 100,
                        metavar="PCT",
                        help=f"Close when cost-to-close <= this %% of credit "
                             f"(default {TAKE_PROFIT_PCT * 100:.0f} = capture "
                             f"{(1 - TAKE_PROFIT_PCT) * 100:.0f}%% profit)")
    parser.add_argument("--iv-rank-low", type=float, default=IV_RANK_LOW * 100,
                        metavar="PCT",
                        help=f"Minimum IV rank to enter (default {IV_RANK_LOW * 100:.0f}%%)")
    parser.add_argument("--vix-floor", type=float, default=VIX_ABS_FLOOR,
                        metavar="VIX",
                        help=f"Minimum VIX to enter (default {VIX_ABS_FLOOR:.0f})")
    parser.add_argument("--wing-width", type=float, default=WING_WIDTH_PCT * 100,
                        metavar="PCT",
                        help=f"Wing width as %% of spot (default {WING_WIDTH_PCT * 100:.0f})")
    parser.add_argument("--put-wing-mult", type=float, default=1.0,
                        metavar="MULT",
                        help="Put wing multiplier for broken-wing (default 1.0)")
    parser.add_argument("--call-wing-mult", type=float, default=1.0,
                        metavar="MULT",
                        help="Call wing multiplier for broken-wing (default 1.0)")
    parser.add_argument("--call-delta-offset", type=float, default=0.0,
                        metavar="OFFSET",
                        help="Offset applied to call delta (e.g. -0.05). Default 0 = symmetric.")
    parser.add_argument("--mode", type=str, default="condor",
                        choices=["condor", "put_only", "call_only", "regime"],
                        help="Trade mode: condor (4-leg), put_only (2-leg put spread), "
                             "call_only (2-leg call spread), regime (puts above SMA, "
                             "calls below SMA). Default: condor")
    parser.add_argument("--dte-target", type=int, default=DTE_TARGET,
                        metavar="DAYS",
                        help=f"Target DTE for entry (default {DTE_TARGET})")
    parser.add_argument("--dte-exit", type=int, default=DTE_EXIT,
                        metavar="DAYS",
                        help=f"Time-based exit at this DTE (default {DTE_EXIT})")
    parser.add_argument("--export-csv", type=str, default=None,
                        metavar="FILE",
                        help="Export trades to CSV file")
    args = parser.parse_args()

    if args.year:
        start_year = args.year
        end_year = args.year
    elif args.start and args.end:
        start_year = args.start
        end_year = args.end
    else:
        start_year = 2012
        end_year = 2026

    t0 = time.time()
    result = run_backtest(
        start_year, end_year,
        synthetic_only=args.synthetic_only,
        call_delta_offset=args.call_delta_offset,
        flat_delta=args.flat_delta,
        iv_rank_low=args.iv_rank_low / 100.0,
        sma_period=args.sma_period,
        wing_width_pct=args.wing_width / 100.0,
        put_wing_mult=args.put_wing_mult,
        call_wing_mult=args.call_wing_mult,
        stop_loss_mult=args.stop_loss_mult,
        take_profit_pct=args.take_profit_pct / 100.0,
        vix_abs_floor=args.vix_floor,
        root=args.ticker.upper(),
        mode=args.mode,
        dte_target=args.dte_target,
        dte_exit=args.dte_exit,
        dte_min=args.dte_target - 10,
        dte_max=args.dte_target + 10,
    )
    trades, skipped_sma, skipped_iv, skipped_vix, skipped_data = result
    elapsed = time.time() - t0

    print_results(trades, skipped_sma, skipped_iv, skipped_vix, skipped_data)

    if args.export_csv:
        export_csv(trades, args.export_csv)

    print(f"\nCompleted in {elapsed:.1f}s  ({len(trades)} trades)")


if __name__ == "__main__":
    main()
