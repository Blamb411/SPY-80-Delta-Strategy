#!/usr/bin/env python3
"""
ThetaData Put Credit Spread Backtester (2012-2026)
====================================================
Sells put credit spreads on SPY capturing the Variance Risk Premium.
Uses a 200-day SMA trend filter to avoid selling puts in bear markets
and a credit-based stop loss (default 2x credit received).

Uses real historical bid/ask quotes from ThetaData for option pricing,
with Black-Scholes synthetic fallback for Jan-May 2012.

Usage:
    python put_spread_thetadata.py                            # full 2012-2026, SMA=200, SL=2.0x
    python put_spread_thetadata.py --year 2024                # single year
    python put_spread_thetadata.py --year 2024 --sma-period 0 # no SMA filter
    python put_spread_thetadata.py --start 2012 --end 2025    # date range
    python put_spread_thetadata.py --stop-loss-mult 2.5       # custom stop loss
    python put_spread_thetadata.py --synthetic-only            # B-S only (no ThetaData)
    python put_spread_thetadata.py --export-csv trades.csv     # export results
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
    calculate_spread_price_realistic,
    price_spread_to_close,
    black_scholes_price,
)
from backtest.thetadata_client import ThetaDataClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("put_spread_theta")

# ---------------------------------------------------------------------------
# Strategy constants
# ---------------------------------------------------------------------------
DEFAULT_IV_RANK_LOW = 0.30
IV_RANK_MED = 0.50
IV_RANK_HIGH = 0.70

DELTA_BY_IV_TIER = {
    "low": None,
    "medium": 0.20,
    "high": 0.25,
    "very_high": 0.30,
}

DEFAULT_WING_WIDTH_PCT = 0.03  # 3% of spot
MIN_WING_WIDTH = 5.0           # minimum wing width in dollars
MAX_CREDIT_RATIO = 0.60        # reject if credit > 60% of wing width
ENTRY_INTERVAL = 5             # every 5 trading days
DTE_TARGET = 30
DTE_MIN = 25
DTE_MAX = 45
RISK_FREE_RATE = 0.05
TAKE_PROFIT_PCT = 0.50         # close when position value <= 50% of credit
SYNTHETIC_SPREAD_PCT = 0.05    # 5% synthetic spread for Jan-May 2012

# Put spread specific
DEFAULT_STOP_LOSS_MULT = 2.0   # stop at 2x credit received
DEFAULT_SMA_PERIOD = 200       # 200-day SMA trend filter
DEFAULT_MIN_OPEN_INTEREST = 0  # minimum OI per leg (0 = disabled)
DEFAULT_MIN_CW_RATIO = 0.0    # minimum credit/width ratio (0 = disabled)
DEFAULT_WING_SIGMA = 0.0      # vol-scaled wing multiplier (0 = use percentage-based)
DEFAULT_IV_RANK_HIGH = 1.0    # IV rank ceiling (1.0 = no cap)
DEFAULT_FLAT_DELTA = 0.0      # flat delta override (0 = use tier-based)
# TAKE_PROFIT_PCT defined above (0.50)

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


def select_delta_tier(iv_rank: float,
                      iv_rank_low: float = DEFAULT_IV_RANK_LOW,
                      iv_rank_high: float = DEFAULT_IV_RANK_HIGH,
                      flat_delta: float = DEFAULT_FLAT_DELTA) -> Tuple[Optional[float], str]:
    """Select short delta and tier name from IV rank."""
    if iv_rank < iv_rank_low:
        return None, "low"
    if iv_rank > iv_rank_high:
        return None, "too_high"

    # Determine tier name
    if iv_rank < IV_RANK_MED:
        tier = "medium"
    elif iv_rank < IV_RANK_HIGH:
        tier = "high"
    else:
        tier = "very_high"

    # Use flat delta if set, otherwise tier-based
    if flat_delta > 0:
        return flat_delta, tier
    return DELTA_BY_IV_TIER[tier], tier


# ===================================================================
# Step 2 -- SMA Trend Filter
# ===================================================================

def check_sma_filter(spy_bars: List[Dict], bar_idx: int,
                     sma_period: int) -> Tuple[bool, Optional[float]]:
    """
    Check if SPY is above its SMA (bullish trend filter).

    Args:
        spy_bars: Full array of SPY daily bars
        bar_idx: Current bar index in spy_bars
        sma_period: SMA lookback period (0 = disabled)

    Returns:
        (passes_filter, sma_value)
        - passes_filter: True if spot > SMA or filter disabled
        - sma_value: Computed SMA or None if disabled/insufficient data
    """
    if sma_period <= 0:
        return (True, None)

    if bar_idx < sma_period:
        return (False, None)

    closes = [spy_bars[i]["close"]
              for i in range(bar_idx - sma_period + 1, bar_idx + 1)]
    sma_value = sum(closes) / len(closes)
    spot = spy_bars[bar_idx]["close"]

    return (spot > sma_value, sma_value)


# ===================================================================
# Step 3 -- Strike Construction
# ===================================================================

def find_short_put_strike(spot: float, vix: float,
                          short_delta: float) -> Optional[float]:
    """
    Find the short put strike via B-S delta targeting.
    Returns rounded strike or None if delta solve fails.
    """
    vix_decimal = vix / 100.0
    dte_years = DTE_TARGET / 365.0

    sp_raw = find_strike_for_delta(spot, dte_years, RISK_FREE_RATE,
                                   vix_decimal, -short_delta, "P")
    if sp_raw is None:
        return None

    sp_strike = round(sp_raw)

    if sp_strike >= spot:
        return None

    return float(sp_strike)


def build_spread_strikes(spot: float, vix: float,
                         short_delta: float,
                         wing_width_pct: float = DEFAULT_WING_WIDTH_PCT,
                         ) -> Optional[Dict[str, float]]:
    """
    Build 2 target strikes for a put credit spread using B-S delta targeting.

    Args:
        wing_width_pct: Wing width as fraction of spot (e.g. 0.03 = 3%)

    Returns dict with keys: short_put, long_put
    or None if delta solve fails.
    """
    sp_strike = find_short_put_strike(spot, vix, short_delta)
    if sp_strike is None:
        return None

    # Wing width with minimum floor
    wing = max(round(spot * wing_width_pct), MIN_WING_WIDTH)
    lp_strike = sp_strike - wing

    if lp_strike >= sp_strike:
        return None

    return {
        "short_put": sp_strike,
        "long_put": float(lp_strike),
    }


def build_strikes_with_wing(sp_strike: float, wing_dollars: float) -> Optional[Dict[str, float]]:
    """Build spread strikes from a known short put strike and wing width in dollars."""
    if wing_dollars < MIN_WING_WIDTH:
        return None
    lp_strike = sp_strike - wing_dollars
    if lp_strike >= sp_strike:
        return None
    return {
        "short_put": sp_strike,
        "long_put": float(lp_strike),
    }


# ===================================================================
# Step 4 -- Validate and Snap Strikes
# ===================================================================

def validate_and_snap_strikes(client: ThetaDataClient, targets: Dict[str, float],
                               root: str, expiration: str,
                               spot: float = None) -> Optional[Dict[str, float]]:
    """
    Snap 2 target strikes to nearest real ThetaData strikes.
    Validates LP < SP < spot after snapping, and minimum wing width.

    Returns snapped strikes dict or None if validation fails.
    """
    snapped = {}
    for leg in ("short_put", "long_put"):
        s = client.snap_strike(root, expiration, targets[leg])
        if s is None:
            return None
        snapped[leg] = s

    # Validate structure: long_put < short_put
    if not (snapped["long_put"] < snapped["short_put"]):
        return None

    # Validate short put is OTM
    if spot is not None:
        if snapped["short_put"] >= spot:
            return None

    # Validate minimum wing width after snapping
    put_width = snapped["short_put"] - snapped["long_put"]
    if put_width < MIN_WING_WIDTH:
        return None

    return snapped


# ===================================================================
# Step 4b -- Open Interest Filter
# ===================================================================

def check_open_interest(client: ThetaDataClient, root: str, expiration: str,
                        strikes: Dict[str, float], entry_date: str,
                        min_oi: int) -> Tuple[bool, Dict[str, float]]:
    """
    Check that both legs have sufficient open interest on the entry date.

    Args:
        min_oi: Minimum open interest threshold per leg (0 = disabled)

    Returns:
        (passes, oi_dict) where oi_dict maps leg name to OI value.
    """
    if min_oi <= 0:
        return (True, {})

    oi_values = {}
    for leg in ("short_put", "long_put"):
        eod = client.get_option_eod(root, expiration, strikes[leg], "P",
                                    entry_date, entry_date)
        oi = 0
        if eod:
            oi = eod[0].get("open_interest", 0)
        oi_values[leg] = oi

        if oi < min_oi:
            return (False, oi_values)

    return (True, oi_values)


# ===================================================================
# Step 5 -- ThetaData Pricing (Entry)
# ===================================================================

def price_spread_entry_thetadata(client: ThetaDataClient, root: str,
                                  expiration: str, strikes: Dict[str, float],
                                  entry_date: str) -> Optional[Dict]:
    """
    Price put credit spread entry using real ThetaData bid/ask quotes.

    Pre-fetches both legs through expiration for later daily management.
    Returns dict with credit, max_loss, or None if data missing.
    """
    legs = {
        "short_put": ("P", strikes["short_put"]),
        "long_put":  ("P", strikes["long_put"]),
    }

    # Pre-fetch EOD data for both legs (entry through expiration)
    prefetched = {}
    for leg_name, (right, strike) in legs.items():
        eod_data = client.prefetch_option_life(root, expiration, strike, right, entry_date)
        prefetched[leg_name] = eod_data

    # Read entry-date bid/ask for each leg
    entry_quotes = {}
    for leg_name, (right, strike) in legs.items():
        q = client.get_bid_ask(root, expiration, strike, right, entry_date)
        if q is None or q["bid"] <= 0 or q["ask"] <= 0:
            return None  # skip trade -- no entry-date quotes available
        entry_quotes[leg_name] = q

    # Credit = sell short put at bid, buy long put at ask
    credit = entry_quotes["short_put"]["bid"] - entry_quotes["long_put"]["ask"]

    if credit <= 0:
        return None

    put_width = strikes["short_put"] - strikes["long_put"]

    # Reject if credit exceeds max ratio of wing width
    if put_width > 0 and credit / put_width > MAX_CREDIT_RATIO:
        return None

    max_loss = (put_width - credit) * 100  # per contract

    return {
        "credit": round(credit, 4),
        "max_loss": round(max_loss, 2),
        "put_width": put_width,
        "data_source": "thetadata",
        "entry_quotes": entry_quotes,
    }


# ===================================================================
# Step 6 -- Synthetic (B-S) Pricing (Jan-May 2012 fallback)
# ===================================================================

def price_spread_entry_synthetic(spot: float, strikes: Dict[str, float],
                                  vix: float, dte: int) -> Optional[Dict]:
    """
    Price put spread entry using Black-Scholes with synthetic bid/ask spread.
    Used for dates before ThetaData coverage (Jan-May 2012).
    """
    vix_decimal = vix / 100.0
    t_years = dte / 365.0

    result = calculate_spread_price_realistic(
        spot,
        strikes["short_put"],
        strikes["long_put"],
        t_years,
        RISK_FREE_RATE,
        vix_decimal,
        "P",
        bid_ask_spread_pct=SYNTHETIC_SPREAD_PCT,
        use_skew=True,
    )

    if result is None or result["open_credit"] <= 0:
        return None

    put_width = result["width"]

    # Reject if credit exceeds max ratio of wing width
    if put_width > 0 and result["open_credit"] / put_width > MAX_CREDIT_RATIO:
        return None

    return {
        "credit": round(result["open_credit"], 4),
        "max_loss": round(result["max_loss"], 2),
        "put_width": put_width,
        "data_source": "synthetic",
    }


# ===================================================================
# Step 7 -- Daily Re-pricing
# ===================================================================

def price_spread_on_date_thetadata(client: ThetaDataClient, root: str,
                                    expiration: str, strikes: Dict[str, float],
                                    price_date: str) -> Optional[float]:
    """
    Re-price put spread on a given date using cached ThetaData quotes.

    Returns cost to close (debit) or None if data missing.
    """
    legs = {
        "short_put": ("P", strikes["short_put"]),
        "long_put":  ("P", strikes["long_put"]),
    }

    quotes = {}
    for leg_name, (right, strike) in legs.items():
        q = client.get_bid_ask(root, expiration, strike, right, price_date)
        if q is None or (q["bid"] <= 0 and q["ask"] <= 0):
            return None
        quotes[leg_name] = q

    # Cost to close = buy short put at ask, sell long put at bid
    close_debit = quotes["short_put"]["ask"] - quotes["long_put"]["bid"]

    return max(0.0, close_debit)


def price_spread_on_date_synthetic(spot: float, strikes: Dict[str, float],
                                    vix: float, dte: int) -> Optional[float]:
    """
    Re-price put spread on a given date using Black-Scholes.
    Used for synthetic path (Jan-May 2012).
    """
    if dte <= 0:
        return None

    vix_decimal = vix / 100.0
    t_years = dte / 365.0

    close_cost = price_spread_to_close(
        spot,
        strikes["short_put"],
        strikes["long_put"],
        t_years,
        RISK_FREE_RATE,
        vix_decimal,
        "P",
        bid_ask_spread_pct=SYNTHETIC_SPREAD_PCT,
        use_skew=True,
    )

    return close_cost


# ===================================================================
# Step 8 -- Intrinsic Settlement
# ===================================================================

def intrinsic_settlement_put_spread(spot: float, strikes: Dict[str, float]) -> float:
    """Compute settlement value at expiration from intrinsic values for put spread."""
    sp_intrinsic = max(0, strikes["short_put"] - spot)
    lp_intrinsic = max(0, strikes["long_put"] - spot)

    settlement = sp_intrinsic - lp_intrinsic
    return max(0.0, settlement)


# ===================================================================
# Step 9 -- Trade Simulation
# ===================================================================

def simulate_spread_trade(
    client: ThetaDataClient,
    entry_date: str,
    spot: float,
    vix: float,
    vix_history: Dict[str, float],
    spy_bars: List[Dict],
    bar_idx_map: Dict[str, int],
    use_synthetic: bool = False,
    stop_loss_mult: float = DEFAULT_STOP_LOSS_MULT,
    sma_value: Optional[float] = None,
    sma_period: int = DEFAULT_SMA_PERIOD,
    min_open_interest: int = DEFAULT_MIN_OPEN_INTEREST,
    wing_width_pct: float = DEFAULT_WING_WIDTH_PCT,
    min_credit_width_ratio: float = DEFAULT_MIN_CW_RATIO,
    iv_rank_low: float = DEFAULT_IV_RANK_LOW,
    iv_rank_high: float = DEFAULT_IV_RANK_HIGH,
    flat_delta: float = DEFAULT_FLAT_DELTA,
    wing_sigma: float = DEFAULT_WING_SIGMA,
    root: str = "SPY",
) -> Optional[Dict]:
    """
    Simulate a single put credit spread trade.

    Uses credit-based stop loss: triggers when loss >= stop_loss_mult * credit * 100.
    When min_credit_width_ratio > 0, dynamically narrows the wing from the initial
    percentage-based width until the credit/width ratio meets the target.

    Returns a result dict, or None if the trade cannot be constructed.
    Returns "skip_oi" string if rejected by open interest filter.
    Returns "skip_cw_ratio" string if no wing width meets the credit/width target.
    """

    # 1. IV rank from VIX history
    iv_rank = compute_vix_iv_rank(vix, vix_history, entry_date)
    if iv_rank is None:
        return None

    # 2. Delta selection
    short_delta, iv_tier = select_delta_tier(iv_rank, iv_rank_low=iv_rank_low,
                                              iv_rank_high=iv_rank_high,
                                              flat_delta=flat_delta)
    if short_delta is None:
        return None  # IV too low or too high

    # 3. Find the short put strike (independent of wing width)
    sp_strike = find_short_put_strike(spot, vix, short_delta)
    if sp_strike is None:
        return None

    # 4. Determine data path
    is_synthetic = use_synthetic or entry_date < THETADATA_START

    # For ThetaData, find expiration once (independent of wing width)
    expiration = None
    if not is_synthetic:
        expiration = client.find_nearest_expiration(root, entry_date,
                                                     DTE_TARGET, DTE_MIN, DTE_MAX)
        if expiration is None:
            return None

    # 5. Build wing widths to try
    if wing_sigma > 0:
        # Vol-scaled wing: expected move * sigma multiplier
        expected_move = spot * (vix / 100.0) * math.sqrt(DTE_TARGET / 365.0)
        initial_wing = max(round(expected_move * wing_sigma), MIN_WING_WIDTH)
    else:
        initial_wing = max(round(spot * wing_width_pct), MIN_WING_WIDTH)

    if min_credit_width_ratio <= 0:
        # No ratio constraint: single attempt at the initial wing
        wing_attempts = [int(initial_wing)]
    else:
        # Try from initial wing down to MIN_WING_WIDTH in $1 steps
        wing_attempts = list(range(int(initial_wing), int(MIN_WING_WIDTH) - 1, -1))

    # 6. Try each wing width until one meets the credit/width ratio
    strikes = None
    pricing = None
    expiration_date = None
    oi_rejected = False

    for wing_try in wing_attempts:
        targets = build_strikes_with_wing(sp_strike, float(wing_try))
        if targets is None:
            continue

        if is_synthetic:
            strikes_candidate = targets
            dte = DTE_TARGET
            exp_candidate = (datetime.strptime(entry_date, "%Y-%m-%d").date()
                             + timedelta(days=dte)).isoformat()

            pricing_candidate = price_spread_entry_synthetic(
                spot, strikes_candidate, vix, dte)
            if pricing_candidate is None:
                continue

        else:
            strikes_candidate = validate_and_snap_strikes(
                client, targets, root, expiration, spot)
            if strikes_candidate is None:
                continue

            # Open interest filter
            if min_open_interest > 0:
                passes_oi, oi_values = check_open_interest(
                    client, root, expiration, strikes_candidate,
                    entry_date, min_open_interest)
                if not passes_oi:
                    oi_rejected = True
                    continue

            exp_candidate = expiration

            pricing_candidate = price_spread_entry_thetadata(
                client, root, expiration, strikes_candidate, entry_date)
            if pricing_candidate is None:
                continue

        # Check credit/width ratio
        cw_ratio = (pricing_candidate["credit"] / pricing_candidate["put_width"]
                    if pricing_candidate["put_width"] > 0 else 0)

        if min_credit_width_ratio > 0 and cw_ratio < min_credit_width_ratio:
            continue  # try next narrower wing

        # Found a valid spread
        strikes = strikes_candidate
        pricing = pricing_candidate
        expiration_date = exp_candidate
        break

    if pricing is None:
        if oi_rejected:
            return "skip_oi"
        if min_credit_width_ratio > 0:
            return "skip_cw_ratio"
        return None

    credit = pricing["credit"]
    max_loss = pricing["max_loss"]
    data_source = pricing["data_source"]
    actual_cw_ratio = credit / pricing["put_width"] if pricing["put_width"] > 0 else 0

    # Exit thresholds -- credit-based stop loss
    tp_target = credit * TAKE_PROFIT_PCT
    sl_threshold = stop_loss_mult * credit  # per-share stop level

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

        # On expiration day: intrinsic settlement
        if d_dt == exp_dt:
            settle_cost = intrinsic_settlement_put_spread(current_spot, strikes)
            pnl = (credit - settle_cost) * 100
            exit_date = d
            exit_reason = "expiration"
            exit_pnl = pnl
            if current_spot < strikes["short_put"]:
                side_breached = "put"
            break

        # Daily re-pricing
        if is_synthetic:
            days_left = (exp_dt - d_dt).days
            vix_today = vix_history.get(d)
            if vix_today is None:
                continue
            close_cost = price_spread_on_date_synthetic(
                current_spot, strikes, vix_today, days_left)
        else:
            close_cost = price_spread_on_date_thetadata(
                client, root, expiration_date, strikes, d)

        if close_cost is None:
            continue

        pnl = (credit - close_cost) * 100

        # Take profit
        if close_cost <= tp_target:
            exit_date = d
            exit_reason = "take_profit"
            exit_pnl = pnl
            break

        # Stop loss (credit-based): trigger when loss >= sl_threshold * 100
        if pnl <= -(sl_threshold * 100):
            exit_date = d
            exit_reason = "stop_loss"
            exit_pnl = pnl
            if current_spot < strikes["short_put"]:
                side_breached = "put"
            break

    # Fallback: settle at intrinsic using last bar before expiration
    if exit_date is None:
        for i in range(min(entry_idx + DTE_MAX + 10, len(spy_bars) - 1),
                       entry_idx, -1):
            bar = spy_bars[i]
            d_dt = datetime.strptime(bar["bar_date"], "%Y-%m-%d").date()
            if d_dt <= exp_dt:
                settle_cost = intrinsic_settlement_put_spread(bar["close"], strikes)
                exit_pnl = (credit - settle_cost) * 100
                exit_date = bar["bar_date"]
                exit_reason = "expiration_fallback"
                if bar["close"] < strikes["short_put"]:
                    side_breached = "put"
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
        "short_put": strikes["short_put"],
        "long_put": strikes["long_put"],
        "put_width": pricing["put_width"],
        "credit": credit,
        "max_loss": max_loss,
        "pnl": round(exit_pnl, 2),
        "won": exit_pnl > 0,
        "exit_reason": exit_reason,
        "side_breached": side_breached,
        "data_source": data_source,
        "dte": dte_actual,
        "sma_value": round(sma_value, 2) if sma_value is not None else None,
        "sma_period": sma_period,
        "sma_filter_active": sma_period > 0,
        "stop_loss_mult": stop_loss_mult,
        "wing_width_pct": wing_width_pct,
        "credit_width_ratio": round(actual_cw_ratio, 4),
        "iv_rank_low": iv_rank_low,
        "iv_rank_high": iv_rank_high,
        "flat_delta": flat_delta,
        "wing_sigma": wing_sigma,
        "root": root,
    }


# ===================================================================
# Step 10 -- Main Backtest Loop
# ===================================================================

def run_backtest(start_year: int = 2012, end_year: int = 2026,
                 synthetic_only: bool = False,
                 stop_loss_mult: float = DEFAULT_STOP_LOSS_MULT,
                 sma_period: int = DEFAULT_SMA_PERIOD,
                 min_open_interest: int = DEFAULT_MIN_OPEN_INTEREST,
                 wing_width_pct: float = DEFAULT_WING_WIDTH_PCT,
                 min_credit_width_ratio: float = DEFAULT_MIN_CW_RATIO,
                 iv_rank_low: float = DEFAULT_IV_RANK_LOW,
                 iv_rank_high: float = DEFAULT_IV_RANK_HIGH,
                 flat_delta: float = DEFAULT_FLAT_DELTA,
                 wing_sigma: float = DEFAULT_WING_SIGMA,
                 root: str = "SPY",
                 entry_interval: int = ENTRY_INTERVAL) -> tuple:
    """
    Run the full ThetaData put credit spread backtest.

    Args:
        start_year: First year to backtest
        end_year: Last year to backtest
        synthetic_only: If True, use B-S for all dates (no ThetaData needed)
        stop_loss_mult: Credit multiplier for stop loss (e.g. 2.0 = stop at 2x credit)
        sma_period: SMA lookback period (0 = disabled)
        min_open_interest: Minimum open interest per leg (0 = disabled)
        wing_width_pct: Wing width as fraction of spot (e.g. 0.03 = 3%)
        min_credit_width_ratio: Min credit/width ratio (e.g. 0.15 = collect 15% of width)
        iv_rank_low: IV rank threshold below which we skip (e.g. 0.20 = skip if rank < 20%)
        iv_rank_high: IV rank ceiling above which we skip (e.g. 0.70 = skip if rank > 70%)
        flat_delta: Fixed delta for all tiers (0 = use tier-based scaling)
        wing_sigma: Vol-scaled wing multiplier (0 = use percentage-based wing_width_pct)
        root: Ticker symbol for options (e.g. "SPY", "QQQ", "IWM")
        entry_interval: Minimum trading days between new positions (default 5)
    """
    start = f"{start_year}-01-01"
    end = f"{end_year}-12-31"

    log.info("=" * 70)
    log.info("THETADATA PUT CREDIT SPREAD BACKTEST  --  %s  %s to %s", root, start, end)
    if synthetic_only:
        log.info("  MODE: Synthetic only (Black-Scholes)")
    else:
        log.info("  MODE: ThetaData real quotes + synthetic fallback")
    log.info("  SMA PERIOD: %s", f"{sma_period}-day" if sma_period > 0 else "OFF")
    log.info("  STOP LOSS:  %.1fx credit", stop_loss_mult)
    iv_high_str = f"<= {iv_rank_high*100:.0f}%" if iv_rank_high < 1.0 else "no cap"
    log.info("  IV RANK:    >= %.0f%% to enter (%s)", iv_rank_low * 100, iv_high_str)
    log.info("  DELTA:      %s", f"flat {flat_delta:.2f}" if flat_delta > 0 else "tier-based (0.20/0.25/0.30)")
    if wing_sigma > 0:
        log.info("  WING MODE:  Vol-scaled (sigma=%.2f)", wing_sigma)
    else:
        log.info("  WING WIDTH: %.0f%% of spot (max)", wing_width_pct * 100)
    log.info("  MIN C/W:    %s", f"{min_credit_width_ratio:.0%}" if min_credit_width_ratio > 0 else "OFF")
    log.info("  MIN OI:     %s", f"{min_open_interest}" if min_open_interest > 0 else "OFF")
    log.info("  TICKER:     %s", root)
    log.info("  INTERVAL:   %d trading days", entry_interval)
    log.info("=" * 70)

    # Initialize client
    client = ThetaDataClient()

    # Check ThetaData connection (not needed for synthetic-only)
    if not synthetic_only:
        if not client.connect():
            log.warning("Theta Terminal not available. Falling back to synthetic-only mode.")
            synthetic_only = True

    # Phase 0: One-time data fetch
    # Fetch enough lookback for SMA calculation before the start date
    lookback_days = max(400, sma_period * 2) if sma_period > 0 else 400
    lookback_start = (datetime.strptime(start, "%Y-%m-%d").date()
                      - timedelta(days=lookback_days)).isoformat()

    # Fetch volatility index: VXN for QQQ, VIX for everything else
    if root == "QQQ":
        vol_history = client.fetch_volatility_index("^VXN", lookback_start, end)
        if not vol_history:
            log.warning("VXN not available, falling back to VIX for QQQ")
            vol_history = client.fetch_vix_history(lookback_start, end)
    else:
        vol_history = client.fetch_vix_history(lookback_start, end)
    if not vol_history:
        log.error("Cannot proceed without volatility index data")
        return []

    # Fetch unified price bars array from lookback_start through end
    spy_bars = client.fetch_ticker_bars(root, lookback_start, end)
    if not spy_bars:
        log.error("No SPY bars available")
        return []

    bar_idx_map = {b["bar_date"]: i for i, b in enumerate(spy_bars)}

    # Find start_idx: first bar where bar_date >= start
    start_idx = 0
    for i, b in enumerate(spy_bars):
        if b["bar_date"] >= start:
            start_idx = i
            break

    log.info("%s bars: %d total (%s to %s), backtest starts at idx %d (%s)",
             root, len(spy_bars), spy_bars[0]["bar_date"], spy_bars[-1]["bar_date"],
             start_idx, spy_bars[start_idx]["bar_date"])

    # Phase 1: Walk dates
    trades: List[Dict] = []
    skipped_sma = 0
    skipped_iv = 0
    skipped_oi = 0
    skipped_cw = 0
    skipped_data = 0
    last_entry_idx = start_idx - entry_interval  # allow first bar

    for idx in range(start_idx, len(spy_bars)):
        if idx - last_entry_idx < entry_interval:
            continue

        bar = spy_bars[idx]
        entry_date = bar["bar_date"]

        # Respect end date
        if entry_date > end:
            break

        spot = bar["close"]

        # SMA filter check (before IV check)
        passes_sma, sma_value = check_sma_filter(spy_bars, idx, sma_period)
        if not passes_sma:
            skipped_sma += 1
            continue

        # Get volatility index value for this date
        vix = vol_history.get(entry_date)
        if vix is None:
            dt = datetime.strptime(entry_date, "%Y-%m-%d").date()
            for offset in (-1, 1, -2, 2):
                alt = (dt + timedelta(days=offset)).isoformat()
                vix = vol_history.get(alt)
                if vix is not None:
                    break
        if vix is None:
            continue

        # IV rank pre-check
        iv_rank = compute_vix_iv_rank(vix, vol_history, entry_date)
        if iv_rank is not None and (iv_rank < iv_rank_low or iv_rank > iv_rank_high):
            skipped_iv += 1
            continue

        # Attempt trade
        result = simulate_spread_trade(
            client, entry_date, spot, vix, vol_history,
            spy_bars, bar_idx_map, use_synthetic=synthetic_only,
            stop_loss_mult=stop_loss_mult,
            sma_value=sma_value,
            sma_period=sma_period,
            min_open_interest=min_open_interest,
            wing_width_pct=wing_width_pct,
            min_credit_width_ratio=min_credit_width_ratio,
            iv_rank_low=iv_rank_low,
            iv_rank_high=iv_rank_high,
            flat_delta=flat_delta,
            wing_sigma=wing_sigma,
            root=root,
        )

        if result == "skip_oi":
            skipped_oi += 1
            continue

        if result == "skip_cw_ratio":
            skipped_cw += 1
            continue

        if result is None:
            skipped_data += 1
            continue

        trades.append(result)
        last_entry_idx = idx

        if len(trades) % 20 == 0:
            log.info("  ... %d trades so far (entry %s, source=%s)",
                     len(trades), entry_date, result["data_source"])

    log.info("Backtest complete: %d trades  (skip: %d SMA, %d IV, %d OI, %d C/W, %d data)",
             len(trades), skipped_sma, skipped_iv, skipped_oi, skipped_cw, skipped_data)

    client.close()
    return trades, skipped_sma, skipped_iv, skipped_oi, skipped_cw, skipped_data


# ===================================================================
# Step 11 -- Risk-Adjusted Metrics
# ===================================================================

def _norm_cdf(x: float) -> float:
    """Standard normal CDF."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def compute_risk_metrics(trades: List[Dict]) -> Optional[Dict]:
    """
    Compute Sharpe, Probabilistic Sharpe (PSR), and Sortino ratios.

    Uses per-trade return on capital at risk (P&L / max_loss) so that
    results are comparable across different wing widths and credit sizes.

    Returns dict with all metrics or None if insufficient trades.
    """
    if len(trades) < 3:
        return None

    # Per-trade returns = P&L / max_loss (return on capital at risk)
    returns = []
    for t in trades:
        if t["max_loss"] > 0:
            returns.append(t["pnl"] / t["max_loss"])
    n = len(returns)
    if n < 3:
        return None

    mean_r = sum(returns) / n

    # Sample standard deviation
    var = sum((r - mean_r) ** 2 for r in returns) / (n - 1)
    std_r = math.sqrt(var) if var > 0 else 0

    # --- Sharpe Ratio (per-trade, no risk-free adjustment) ---
    sharpe = mean_r / std_r if std_r > 0 else 0.0

    # Annualize: estimate trades per year from date range
    first_dt = datetime.strptime(trades[0]["entry_date"], "%Y-%m-%d").date()
    last_dt = datetime.strptime(trades[-1]["entry_date"], "%Y-%m-%d").date()
    span_years = max((last_dt - first_dt).days / 365.25, 0.5)
    trades_per_year = n / span_years
    sharpe_annual = sharpe * math.sqrt(trades_per_year)

    # --- Skewness and excess kurtosis (for PSR) ---
    if std_r > 0:
        skew = (sum((r - mean_r) ** 3 for r in returns)
                / (n * std_r ** 3))
        kurt_excess = (sum((r - mean_r) ** 4 for r in returns)
                       / (n * std_r ** 4)) - 3.0
    else:
        skew = 0.0
        kurt_excess = 0.0

    # --- Probabilistic Sharpe Ratio ---
    # PSR = Phi( sqrt(n-1) * (SR - SR*) / sqrt(1 - skew*SR + (kurt/4)*SR^2) )
    # SR* = benchmark Sharpe (0 = break-even)
    sr_benchmark = 0.0
    denom_sq = 1.0 - skew * sharpe + (kurt_excess / 4.0) * sharpe ** 2
    if denom_sq > 0 and n > 1:
        z_psr = math.sqrt(n - 1) * (sharpe - sr_benchmark) / math.sqrt(denom_sq)
        psr = _norm_cdf(z_psr)
    else:
        psr = 0.5

    # --- Sortino Ratio (downside deviation, target = 0) ---
    downside_sq = [min(r, 0.0) ** 2 for r in returns]
    downside_dev = math.sqrt(sum(downside_sq) / n) if n > 0 else 0
    sortino = mean_r / downside_dev if downside_dev > 0 else 0.0
    sortino_annual = sortino * math.sqrt(trades_per_year)

    return {
        "sharpe": round(sharpe, 3),
        "sharpe_annual": round(sharpe_annual, 3),
        "psr": round(psr, 4),
        "sortino": round(sortino, 3),
        "sortino_annual": round(sortino_annual, 3),
        "mean_return": round(mean_r, 4),
        "std_return": round(std_r, 4),
        "skewness": round(skew, 3),
        "kurtosis_excess": round(kurt_excess, 3),
        "n_trades": n,
        "trades_per_year": round(trades_per_year, 2),
    }


# ===================================================================
# Step 12 -- Reporting
# ===================================================================

def get_vix_bucket(vix: float) -> str:
    for name, lo, hi in VIX_BUCKETS:
        if lo <= vix < hi:
            return name
    return "Unknown"


def print_results(trades: List[Dict],
                  skipped_sma: int = 0,
                  skipped_iv: int = 0,
                  skipped_oi: int = 0,
                  skipped_cw: int = 0,
                  skipped_data: int = 0) -> None:
    """Print comprehensive put credit spread backtest results."""
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

    sample_sma_period = trades[0].get("sma_period", 0)
    sample_stop_mult = trades[0].get("stop_loss_mult", DEFAULT_STOP_LOSS_MULT)

    print()
    sample_root = trades[0].get("root", "SPY")
    print("=" * 70)
    print(f"THETADATA PUT CREDIT SPREAD RESULTS  --  {sample_root}")
    sample_wing_pct = trades[0].get("wing_width_pct", DEFAULT_WING_WIDTH_PCT)
    sample_min_cw = trades[0].get("credit_width_ratio", 0)
    sample_iv_rank_low = trades[0].get("iv_rank_low", DEFAULT_IV_RANK_LOW)
    sample_iv_rank_high = trades[0].get("iv_rank_high", DEFAULT_IV_RANK_HIGH)
    sample_wing_sigma = trades[0].get("wing_sigma", DEFAULT_WING_SIGMA)
    avg_cw_ratio = sum(t.get("credit_width_ratio", 0) for t in trades) / len(trades)
    print(f"  SMA Filter:      {'OFF' if sample_sma_period <= 0 else f'{sample_sma_period}-day SMA'}")
    print(f"  Stop Loss:       {sample_stop_mult:.1f}x credit")
    iv_high_str = f", max {sample_iv_rank_high:.0%}" if sample_iv_rank_high < 1.0 else ""
    print(f"  IV Rank:         min {sample_iv_rank_low:.0%}{iv_high_str}")
    if sample_wing_sigma > 0:
        print(f"  Wing Mode:       Vol-scaled (sigma={sample_wing_sigma:.2f})")
    else:
        print(f"  Wing Width:      {sample_wing_pct * 100:.0f}% of spot (max)")
    print(f"  Avg C/W ratio:   {avg_cw_ratio:.1%}")
    print("=" * 70)
    print(f"  Period:          {trades[0]['entry_date']} to {trades[-1]['entry_date']}")
    print(f"  Total trades:    {len(trades)}")
    print(f"  Winners:         {len(wins)}  ({win_rate:.1%})")
    print(f"  Losers:          {len(losses)}")
    print(f"  Total P&L:       ${total_pnl:>+,.2f}")
    print(f"  Avg P&L/trade:   ${avg_pnl:>+,.2f}")
    print(f"  Avg credit:      ${avg_credit:.4f} /share")
    print(f"  Avg max loss:    ${avg_max_loss:,.2f} /contract")

    # Risk-adjusted metrics
    metrics = compute_risk_metrics(trades)
    if metrics:
        print(f"\n  --- Risk-Adjusted Metrics ---")
        print(f"  Sharpe Ratio:            {metrics['sharpe']:.3f}  "
              f"(annualized: {metrics['sharpe_annual']:.3f})")
        print(f"  Sortino Ratio:           {metrics['sortino']:.3f}  "
              f"(annualized: {metrics['sortino_annual']:.3f})")
        print(f"  Probabilistic Sharpe:    {metrics['psr']:.1%}")
        print(f"  Mean return on risk:     {metrics['mean_return']:.4f}")
        print(f"  Std dev of returns:      {metrics['std_return']:.4f}")
        print(f"  Skewness:                {metrics['skewness']:.3f}")
        print(f"  Excess kurtosis:         {metrics['kurtosis_excess']:.3f}")
        print(f"  Trades/year:             {metrics['trades_per_year']:.2f}")

    # Filter statistics
    total_opportunities = len(trades) + skipped_sma + skipped_iv + skipped_oi + skipped_cw + skipped_data
    if sample_sma_period > 0:
        print(f"\n  --- SMA Filter Statistics ---")
        print(f"  Entries blocked by SMA:  {skipped_sma}")
        if total_opportunities > 0:
            print(f"  SMA block rate:          {skipped_sma / total_opportunities:.1%}")
    if skipped_oi > 0:
        print(f"\n  --- Open Interest Filter ---")
        print(f"  Entries blocked by OI:   {skipped_oi}")
        if total_opportunities > 0:
            print(f"  OI block rate:           {skipped_oi / total_opportunities:.1%}")
    if skipped_cw > 0:
        print(f"\n  --- Credit/Width Ratio Filter ---")
        print(f"  Entries blocked by C/W:  {skipped_cw}")
        if total_opportunities > 0:
            print(f"  C/W block rate:          {skipped_cw / total_opportunities:.1%}")

    # Exit reasons
    reasons = defaultdict(int)
    for t in trades:
        reasons[t["exit_reason"]] += 1
    print(f"\n  Exit reasons:")
    for r, n in sorted(reasons.items(), key=lambda x: -x[1]):
        print(f"    {r:<22} {n:>4}  ({n/len(trades):.1%})")

    # Put breaches
    put_breaches = sum(1 for t in trades if t["side_breached"] == "put")
    print(f"\n  Put breaches:    {put_breaches}")

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
        sl_level = t["stop_loss_mult"] * t["credit"] * 100
        print(f"\n  Trade #{i+1}:")
        print(f"    Entry:     {t['entry_date']}   SPY ${t['spot']:.2f}   VIX {t['vix']:.1f}")
        print(f"    IV Rank:   {t['iv_rank']:.2f}  ({t['iv_tier']})  delta={t['short_delta']}")
        sma_str = f"${t['sma_value']:.2f}" if t['sma_value'] is not None else "N/A"
        print(f"    SMA({t['sma_period']}): {sma_str}")
        print(f"    Strikes:   LP {t['long_put']}  SP {t['short_put']}")
        print(f"    Credit:    ${t['credit']:.4f}/sh   Max loss: ${t['max_loss']:.2f}/ct")
        print(f"    Stop lvl:  ${sl_level:.2f}/ct  ({t['stop_loss_mult']:.1f}x credit)")
        print(f"    Exp:       {t['expiration']}  DTE={t['dte']}")
        print(f"    Exit:      {t['exit_date']}  reason={t['exit_reason']}")
        print(f"    P&L:       ${t['pnl']:>+.2f}  {'WIN' if t['won'] else 'LOSS'}"
              + (f"  (put breached)" if t['side_breached'] else ""))
        print(f"    Source:    {t['data_source']}")


# ===================================================================
# Step 12 -- CSV Export
# ===================================================================

def export_csv(trades: List[Dict], filepath: str) -> None:
    """Export trades to CSV file."""
    if not trades:
        print("No trades to export.")
        return

    fieldnames = [
        "entry_date", "expiration", "exit_date", "spot", "vix",
        "iv_rank", "iv_tier", "short_delta",
        "short_put", "long_put", "put_width",
        "credit", "max_loss", "pnl", "won", "exit_reason",
        "side_breached", "data_source", "dte",
        "sma_value", "sma_period", "sma_filter_active", "stop_loss_mult",
        "wing_width_pct", "credit_width_ratio", "iv_rank_low", "wing_sigma",
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
        description="ThetaData put credit spread backtest for SPY (2012-2026)")
    parser.add_argument("--year", type=int, default=None,
                        help="Run for a single year (e.g. 2024)")
    parser.add_argument("--start", type=int, default=None,
                        help="Start year (e.g. 2020)")
    parser.add_argument("--end", type=int, default=None,
                        help="End year (e.g. 2025)")
    parser.add_argument("--synthetic-only", action="store_true",
                        help="Use Black-Scholes only (no ThetaData needed)")
    parser.add_argument("--sma-period", type=int, default=DEFAULT_SMA_PERIOD,
                        metavar="PERIOD",
                        help=f"SMA trend filter period (default {DEFAULT_SMA_PERIOD}, "
                             f"0 = disabled)")
    parser.add_argument("--stop-loss-mult", type=float, default=DEFAULT_STOP_LOSS_MULT,
                        metavar="MULT",
                        help=f"Stop loss as multiple of credit received "
                             f"(default {DEFAULT_STOP_LOSS_MULT})")
    parser.add_argument("--min-cw-ratio", type=float, default=DEFAULT_MIN_CW_RATIO * 100,
                        metavar="PCT",
                        help=f"Minimum credit/width ratio in percent "
                             f"(default {DEFAULT_MIN_CW_RATIO * 100:.0f}, 0 = disabled)")
    parser.add_argument("--wing-width", type=float, default=DEFAULT_WING_WIDTH_PCT * 100,
                        metavar="PCT",
                        help=f"Wing width as percent of spot "
                             f"(default {DEFAULT_WING_WIDTH_PCT * 100:.0f})")
    parser.add_argument("--min-oi", type=int, default=DEFAULT_MIN_OPEN_INTEREST,
                        metavar="OI",
                        help=f"Minimum open interest per leg "
                             f"(default {DEFAULT_MIN_OPEN_INTEREST}, 0 = disabled)")
    parser.add_argument("--iv-rank-low", type=float, default=DEFAULT_IV_RANK_LOW * 100,
                        metavar="PCT",
                        help=f"Minimum IV rank to enter trades, in percent "
                             f"(default {DEFAULT_IV_RANK_LOW * 100:.0f})")
    parser.add_argument("--iv-rank-high", type=float, default=DEFAULT_IV_RANK_HIGH * 100,
                        metavar="PCT",
                        help=f"Maximum IV rank to enter trades, in percent "
                             f"(default {DEFAULT_IV_RANK_HIGH * 100:.0f}, 100 = no cap)")
    parser.add_argument("--flat-delta", type=float, default=DEFAULT_FLAT_DELTA,
                        metavar="DELTA",
                        help=f"Fixed delta for all tiers "
                             f"(default {DEFAULT_FLAT_DELTA}, 0 = tier-based)")
    parser.add_argument("--wing-sigma", type=float, default=DEFAULT_WING_SIGMA,
                        metavar="MULT",
                        help=f"Vol-scaled wing multiplier "
                             f"(default {DEFAULT_WING_SIGMA}, 0 = use percentage-based)")
    parser.add_argument("--ticker", "--root", type=str, default="SPY",
                        metavar="SYM",
                        help="Ticker symbol (default SPY)")
    parser.add_argument("--entry-interval", type=int, default=ENTRY_INTERVAL,
                        metavar="DAYS",
                        help=f"Min trading days between entries "
                             f"(default {ENTRY_INTERVAL})")
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
    trades, skipped_sma, skipped_iv, skipped_oi, skipped_cw, skipped_data = run_backtest(
        start_year, end_year,
        synthetic_only=args.synthetic_only,
        stop_loss_mult=args.stop_loss_mult,
        sma_period=args.sma_period,
        min_open_interest=args.min_oi,
        wing_width_pct=args.wing_width / 100.0,
        min_credit_width_ratio=args.min_cw_ratio / 100.0,
        iv_rank_low=args.iv_rank_low / 100.0,
        iv_rank_high=args.iv_rank_high / 100.0,
        flat_delta=args.flat_delta,
        wing_sigma=args.wing_sigma,
        root=args.ticker.upper(),
        entry_interval=args.entry_interval,
    )
    elapsed = time.time() - t0

    print_results(trades, skipped_sma, skipped_iv, skipped_oi, skipped_cw, skipped_data)

    if args.export_csv:
        export_csv(trades, args.export_csv)

    print(f"\nCompleted in {elapsed:.1f}s  ({len(trades)} trades)")


if __name__ == "__main__":
    main()
