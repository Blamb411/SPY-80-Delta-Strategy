"""
IV Engine
==========
Implied volatility solving, historical volatility, IV Rank, and delta
computation. Wraps functions from backtest/black_scholes.py.
"""

import sys
import os
import math
import logging
from typing import Optional, List

# Add project root to path so we can import from backtest/
_project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from backtest.black_scholes import (
    black_scholes_price,
    black_scholes_greeks,
    find_strike_for_delta,
    calculate_hv,
    calculate_iv_rank,
    norm_cdf,
)

import config
import cache_db

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------
# IV solving from market price
# -----------------------------------------------------------------------

def implied_vol_from_price(
    market_price: float,
    spot: float,
    strike: float,
    t_years: float,
    rate: float,
    right: str,
    tol: float = 1e-5,
    max_iter: int = 100,
) -> Optional[float]:
    """Back out implied volatility from an observed option price.

    Uses bisection search on Black-Scholes.
    Returns IV as decimal (e.g. 0.25 for 25%), or None if it can't converge.
    """
    if market_price <= 0 or spot <= 0 or strike <= 0 or t_years <= 0:
        return None

    # Intrinsic value check
    if right.upper() == "C":
        intrinsic = max(0, spot - strike)
    else:
        intrinsic = max(0, strike - spot)

    if market_price < intrinsic:
        return None  # price below intrinsic — bad data

    iv_low = 0.01
    iv_high = 5.0  # 500% — generous upper bound

    for _ in range(max_iter):
        iv_mid = (iv_low + iv_high) / 2
        price = black_scholes_price(spot, strike, t_years, rate, iv_mid, right)
        if price is None:
            return None

        if abs(price - market_price) < tol:
            return iv_mid

        if price > market_price:
            iv_high = iv_mid
        else:
            iv_low = iv_mid

    return (iv_low + iv_high) / 2  # best estimate


# -----------------------------------------------------------------------
# ATM IV estimation from option chain
# -----------------------------------------------------------------------

def estimate_atm_iv(
    spot: float,
    contracts: List[dict],
    price_date: str,
    t_years: float,
    rate: float = None,
) -> Optional[float]:
    """Estimate ATM implied volatility from available option contracts.

    Finds the contract closest to ATM, gets its market price, and
    backs out IV.
    """
    if rate is None:
        rate = config.RISK_FREE_RATE

    if not contracts or spot <= 0 or t_years <= 0:
        return None

    # Find contract closest to ATM
    best = None
    best_dist = float("inf")
    for c in contracts:
        dist = abs(c["strike_price"] - spot)
        if dist < best_dist:
            best_dist = dist
            best = c

    if best is None:
        return None

    # Get market price for this contract
    bar = cache_db.get_option_bar(best["option_ticker"], price_date)
    if bar is None or bar["close"] is None or bar["close"] <= 0:
        return None

    iv = implied_vol_from_price(
        market_price=bar["close"],
        spot=spot,
        strike=best["strike_price"],
        t_years=t_years,
        rate=rate,
        right="P" if best["contract_type"] == "put" else "C",
    )
    return iv


# -----------------------------------------------------------------------
# HV and IV Rank wrappers
# -----------------------------------------------------------------------

def compute_hv(ticker: str, as_of_date: str, period: int = None) -> Optional[float]:
    """Compute historical volatility for a ticker as of a date."""
    period = period or config.HV_PERIOD
    closes = cache_db.get_all_closes(ticker, as_of_date)
    if len(closes) < period + 1:
        return None
    return calculate_hv(closes, period)


def compute_iv_rank(ticker: str, as_of_date: str) -> Optional[float]:
    """Compute IV Rank using HV as proxy for IV.

    Calculates current 20-day HV and ranks it against trailing 252-day
    history of 20-day HV values.
    """
    closes = cache_db.get_all_closes(ticker, as_of_date)
    if len(closes) < config.IV_RANK_LOOKBACK + config.HV_PERIOD + 1:
        # Not enough history — try with what we have
        if len(closes) < config.HV_PERIOD + 21:
            return None

    # Build trailing HV history
    hv_history = []
    # We need at least HV_PERIOD + 1 prices to compute one HV value
    start_idx = config.HV_PERIOD + 1
    for i in range(start_idx, len(closes)):
        window = closes[:i]
        hv = calculate_hv(window, config.HV_PERIOD)
        if hv is not None:
            hv_history.append(hv)

    if not hv_history:
        return None

    current_hv = hv_history[-1]
    return calculate_iv_rank(current_hv, hv_history, config.IV_RANK_LOOKBACK)


# -----------------------------------------------------------------------
# Delta computation
# -----------------------------------------------------------------------

def compute_delta(
    spot: float,
    strike: float,
    t_years: float,
    iv: float,
    right: str,
    rate: float = None,
) -> Optional[float]:
    """Compute Black-Scholes delta for an option."""
    rate = rate or config.RISK_FREE_RATE
    result = black_scholes_greeks(spot, strike, t_years, rate, iv, right)
    if result is None:
        return None
    return result.delta


def find_strike_by_delta(
    spot: float,
    t_years: float,
    iv: float,
    target_delta: float,
    right: str,
    rate: float = None,
) -> Optional[float]:
    """Find strike that gives the target delta.

    For puts, target_delta should be negative (e.g. -0.25).
    Returns the raw strike (caller should round to available strikes).
    """
    rate = rate or config.RISK_FREE_RATE
    return find_strike_for_delta(spot, t_years, rate, iv, target_delta, right)


def find_strike_by_otm_pct(
    spot: float,
    otm_pct: float,
    right: str,
) -> float:
    """Fallback: find strike at a given OTM percentage.

    For puts:  strike = spot * (1 - otm_pct)
    For calls: strike = spot * (1 + otm_pct)
    """
    if right.upper() == "P":
        return spot * (1 - otm_pct)
    else:
        return spot * (1 + otm_pct)
