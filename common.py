"""
Shared Utilities for SPY 80-Delta Call Strategy
=================================================
Eliminates code duplication across scripts.
"""

from __future__ import annotations

import math
from datetime import datetime, date, timedelta
from dataclasses import dataclass, field
from typing import Optional

from config import (
    RISK_FREE_RATE, DEFAULT_IV, MAX_HOLD_DAYS, PROFIT_TARGET,
    DTE_TARGET, DTE_MIN, DTE_MAX,
)


# ============================================================================
# SHARED POSITION DATACLASS
# ============================================================================

@dataclass
class Position:
    """Represents an open option position. Used by monitor and alerts."""
    account: str
    entry_date: str
    symbol: str
    strike: float
    expiration: str
    right: str  # 'C' or 'P'
    quantity: int
    entry_price: float
    notes: str = ""

    @property
    def position_id(self) -> str:
        return f"{self.symbol}_{self.strike}_{self.expiration}_{self.account}"

    @property
    def total_cost(self) -> float:
        return self.entry_price * 100 * self.quantity

    @property
    def profit_target_price(self) -> float:
        return self.entry_price * (1 + PROFIT_TARGET)

    @property
    def profit_target_value(self) -> float:
        return self.profit_target_price * 100 * self.quantity

    @property
    def days_held(self) -> int:
        """Count trading days held (excluding entry date, including today)."""
        entry = datetime.strptime(self.entry_date, "%Y-%m-%d").date()
        today = date.today()
        days = 0
        current = entry
        while current < today:
            current += timedelta(days=1)
            if current.weekday() < 5:  # Monday=0, Friday=4
                days += 1
        return days

    @property
    def days_remaining(self) -> int:
        return max(0, MAX_HOLD_DAYS - self.days_held)

    @property
    def dte(self) -> int:
        exp = datetime.strptime(self.expiration, "%Y-%m-%d").date()
        return (exp - date.today()).days


# ============================================================================
# STANDARD NORMAL CDF
# ============================================================================

def norm_cdf(x: float) -> float:
    """Standard normal cumulative distribution function."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


# ============================================================================
# BLACK-SCHOLES DELTA
# ============================================================================

def calculate_delta(
    spot: float,
    strike: float,
    dte: int,
    iv: float = DEFAULT_IV,
    rate: float = RISK_FREE_RATE,
    right: str = "C",
) -> float:
    """
    Calculate option delta using Black-Scholes.

    Handles edge cases:
    - Expired options (dte <= 0): returns intrinsic delta
    - ATM at expiration (spot == strike): returns ~0.5 for calls, ~-0.5 for puts
    """
    if dte <= 0:
        if spot == strike:
            # ATM at expiry: delta is ~0.5, not 0.0
            return 0.5 if right.upper() == "C" else -0.5
        if right.upper() == "C":
            return 1.0 if spot > strike else 0.0
        else:
            return -1.0 if spot < strike else 0.0

    t = dte / 365.0
    d1 = (math.log(spot / strike) + (rate + 0.5 * iv * iv) * t) / (iv * math.sqrt(t))
    delta = norm_cdf(d1)

    if right.upper() == "P":
        delta = delta - 1.0

    return delta


# ============================================================================
# BLACK-SCHOLES GAMMA
# ============================================================================

def calculate_gamma(
    spot: float,
    strike: float,
    dte: int,
    iv: float = DEFAULT_IV,
    rate: float = RISK_FREE_RATE,
) -> float:
    """Calculate option gamma using Black-Scholes."""
    if dte <= 0 or spot <= 0 or strike <= 0 or iv <= 0:
        return 0.0

    t = dte / 365.0
    sqrt_t = math.sqrt(t)
    d1 = (math.log(spot / strike) + (rate + 0.5 * iv * iv) * t) / (iv * sqrt_t)

    # Normal PDF
    npd1 = math.exp(-0.5 * d1 * d1) / math.sqrt(2.0 * math.pi)

    return npd1 / (spot * iv * sqrt_t)


# ============================================================================
# OPTION PRICING (Black-Scholes)
# ============================================================================

def estimate_option_price(
    spot: float,
    strike: float,
    dte: int,
    iv: float = DEFAULT_IV,
    rate: float = RISK_FREE_RATE,
    right: str = "C",
) -> float:
    """
    Estimate option price using Black-Scholes.

    Returns:
        Estimated option price (always >= 0)
    """
    if dte <= 0:
        if right.upper() == "C":
            return max(0.0, spot - strike)
        else:
            return max(0.0, strike - spot)

    t = dte / 365.0
    sqrt_t = math.sqrt(t)
    d1 = (math.log(spot / strike) + (rate + 0.5 * iv * iv) * t) / (iv * sqrt_t)
    d2 = d1 - iv * sqrt_t

    if right.upper() == "C":
        price = spot * norm_cdf(d1) - strike * math.exp(-rate * t) * norm_cdf(d2)
    else:
        price = strike * math.exp(-rate * t) * norm_cdf(-d2) - spot * norm_cdf(-d1)

    return max(0.0, price)


# ============================================================================
# EXPIRATION HELPERS
# ============================================================================

def is_monthly_opex(exp_str: str) -> bool:
    """Check if a date string is a monthly options expiration (3rd Friday)."""
    exp_dt = datetime.strptime(exp_str, "%Y-%m-%d").date()
    return exp_dt.weekday() == 4 and 15 <= exp_dt.day <= 21


def find_best_expiration(
    entry_date_str: str,
    monthly_exps_dates: list,
    target: int = DTE_TARGET,
    dte_min: int = DTE_MIN,
    dte_max: int = DTE_MAX,
) -> tuple:
    """
    Find best expiration date for a given entry date.

    Args:
        entry_date_str: Entry date as "YYYY-MM-DD"
        monthly_exps_dates: List of (exp_str, exp_date) tuples
        target: Target DTE
        dte_min: Minimum acceptable DTE
        dte_max: Maximum acceptable DTE

    Returns:
        (best_exp_str, best_dte) or (None, 0) if no match
    """
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


# ============================================================================
# BID/ASK HELPERS
# ============================================================================

def get_bid_ask(eod_row: dict | None) -> tuple:
    """
    Extract bid/ask from an EOD data row.

    Falls back to close price with tight spread if bid/ask unavailable.
    Returns (bid, ask) or (None, None).
    """
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


# ============================================================================
# SCENARIO ANALYSIS (GAMMA-ADJUSTED)
# ============================================================================

def gamma_adjusted_option_value(
    current_value: float,
    total_delta: float,
    total_gamma: float,
    spot_change: float,
) -> float:
    """
    Estimate new options portfolio value using delta + gamma (2nd order Taylor).

    new_value ~= current_value + delta * dS + 0.5 * gamma * dS^2

    This is much more accurate than linear delta alone for large moves.

    Args:
        current_value: Current total options value
        total_delta: Portfolio total delta (sum of delta * qty * 100)
        total_gamma: Portfolio total gamma (sum of gamma * qty * 100)
        spot_change: Dollar change in underlying price

    Returns:
        Estimated new options value (floored at 0)
    """
    new_value = (
        current_value
        + total_delta * spot_change
        + 0.5 * total_gamma * spot_change * spot_change
    )
    return max(0.0, new_value)
