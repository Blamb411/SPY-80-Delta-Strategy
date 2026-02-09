"""
Option Selection Utilities
==========================
Strike and expiration selection logic.
Centralized to ensure consistent option selection across scripts.
"""

import math
from typing import List, Optional, Tuple
from scipy.stats import norm


def calculate_delta(
    spot: float,
    strike: float,
    dte: int,
    iv: float = 0.16,
    rate: float = 0.04,
    option_type: str = "C",
) -> float:
    """
    Calculate option delta using Black-Scholes formula.

    Args:
        spot: Current underlying price
        strike: Option strike price
        dte: Days to expiration
        iv: Implied volatility (decimal, e.g., 0.16 for 16%)
        rate: Risk-free rate (decimal)
        option_type: "C" for call, "P" for put

    Returns:
        Delta (0 to 1 for calls, -1 to 0 for puts)
    """
    if dte <= 0:
        # At expiration
        if option_type.upper() == "C":
            return 1.0 if spot > strike else 0.0
        else:
            return -1.0 if spot < strike else 0.0

    t = dte / 365.0
    sqrt_t = math.sqrt(t)

    if iv <= 0 or sqrt_t <= 0:
        return 0.5

    d1 = (math.log(spot / strike) + (rate + 0.5 * iv * iv) * t) / (iv * sqrt_t)

    if option_type.upper() == "C":
        return norm.cdf(d1)
    else:
        return norm.cdf(d1) - 1.0


def find_strike_for_delta(
    spot: float,
    dte: int,
    target_delta: float = 0.80,
    iv: float = 0.16,
    rate: float = 0.04,
    option_type: str = "C",
) -> float:
    """
    Find the strike price that gives a target delta.

    Uses binary search to find the strike.

    Args:
        spot: Current underlying price
        dte: Days to expiration
        target_delta: Target delta (e.g., 0.80)
        iv: Implied volatility
        rate: Risk-free rate
        option_type: "C" for call, "P" for put

    Returns:
        Strike price that gives approximately the target delta
    """
    if dte <= 0:
        return spot

    # For calls, lower strike = higher delta
    # Binary search bounds
    low_strike = spot * 0.5
    high_strike = spot * 1.5

    for _ in range(50):  # Max iterations
        mid_strike = (low_strike + high_strike) / 2
        mid_delta = calculate_delta(spot, mid_strike, dte, iv, rate, option_type)

        if option_type.upper() == "C":
            if mid_delta > target_delta:
                low_strike = mid_strike  # Need higher strike for lower delta
            else:
                high_strike = mid_strike
        else:
            if abs(mid_delta) > abs(target_delta):
                high_strike = mid_strike
            else:
                low_strike = mid_strike

        if abs(mid_delta - target_delta) < 0.001:
            break

    return mid_strike


def find_nearest_strike(
    target_strike: float,
    available_strikes: List[float],
) -> Optional[float]:
    """
    Find the nearest available strike to a target.

    Args:
        target_strike: Target strike price
        available_strikes: List of available strike prices

    Returns:
        Nearest strike or None if list is empty
    """
    if not available_strikes:
        return None

    return min(available_strikes, key=lambda s: abs(s - target_strike))


def find_strike_in_delta_band(
    spot: float,
    dte: int,
    available_strikes: List[float],
    delta_min: float = 0.70,
    delta_max: float = 0.90,
    delta_target: float = 0.80,
    iv: float = 0.16,
    rate: float = 0.04,
    option_type: str = "C",
) -> Tuple[Optional[float], float]:
    """
    Find the best strike within a delta band.

    Returns the strike closest to target delta that falls within the band.

    Args:
        spot: Current underlying price
        dte: Days to expiration
        available_strikes: List of available strike prices
        delta_min: Minimum acceptable delta
        delta_max: Maximum acceptable delta
        delta_target: Target delta (prefer strikes close to this)
        iv: Implied volatility
        rate: Risk-free rate
        option_type: "C" for call, "P" for put

    Returns:
        Tuple of (best_strike, actual_delta) or (None, 0) if none found
    """
    if not available_strikes:
        return None, 0.0

    candidates = []

    for strike in available_strikes:
        delta = calculate_delta(spot, strike, dte, iv, rate, option_type)

        if option_type.upper() == "P":
            delta = abs(delta)

        if delta_min <= delta <= delta_max:
            candidates.append((strike, delta, abs(delta - delta_target)))

    if not candidates:
        return None, 0.0

    # Sort by closeness to target delta
    candidates.sort(key=lambda x: x[2])

    best_strike, actual_delta, _ = candidates[0]
    return best_strike, actual_delta


def generate_strike_range(
    spot: float,
    pct_below: float = 0.20,
    pct_above: float = 0.10,
    step: float = 5.0,
) -> List[float]:
    """
    Generate a range of strikes around the current spot price.

    Useful for synthetic backtests where we don't have actual strike data.

    Args:
        spot: Current underlying price
        pct_below: How far below spot to go (decimal)
        pct_above: How far above spot to go (decimal)
        step: Strike increment (e.g., $5)

    Returns:
        List of strike prices
    """
    low = spot * (1 - pct_below)
    high = spot * (1 + pct_above)

    # Round to nearest step
    low = math.floor(low / step) * step
    high = math.ceil(high / step) * step

    strikes = []
    strike = low
    while strike <= high:
        strikes.append(strike)
        strike += step

    return strikes


def estimate_iv_from_vix(
    vix: float,
    dte: int,
    term_structure_factor: float = 1.1,
) -> float:
    """
    Estimate implied volatility from VIX.

    VIX is 30-day expected volatility. For longer-dated options,
    we apply a term structure adjustment.

    Args:
        vix: VIX value (e.g., 20 for 20%)
        dte: Days to expiration
        term_structure_factor: Multiplier for term structure (default 1.1)

    Returns:
        Estimated IV as decimal (e.g., 0.22 for 22%)
    """
    base_iv = vix / 100.0

    # Apply term structure adjustment for longer-dated options
    # VIX is 30-day; 120-day options typically have slightly higher IV
    if dte > 30:
        # Simple linear adjustment
        adjustment = 1 + (dte - 30) / 365 * (term_structure_factor - 1)
        base_iv *= adjustment

    # Clamp to reasonable range
    return max(0.05, min(0.90, base_iv))


def is_strike_liquid(
    strike: float,
    spot: float,
    option_type: str = "C",
    max_otm_pct: float = 0.20,
    max_itm_pct: float = 0.20,
) -> bool:
    """
    Check if a strike is likely to be liquid.

    Very deep ITM or far OTM strikes tend to be illiquid.

    Args:
        strike: Strike price
        spot: Current underlying price
        option_type: "C" for call, "P" for put
        max_otm_pct: Maximum OTM percentage
        max_itm_pct: Maximum ITM percentage

    Returns:
        True if strike is likely liquid
    """
    if option_type.upper() == "C":
        # Call is ITM when strike < spot
        if strike < spot * (1 - max_itm_pct):
            return False  # Too deep ITM
        if strike > spot * (1 + max_otm_pct):
            return False  # Too far OTM
    else:
        # Put is ITM when strike > spot
        if strike > spot * (1 + max_itm_pct):
            return False  # Too deep ITM
        if strike < spot * (1 - max_otm_pct):
            return False  # Too far OTM

    return True
