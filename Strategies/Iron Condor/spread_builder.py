"""
Spread Builder
===============
Strike selection, spread construction, and pricing.
Handles both real bid/ask quotes and synthetic pricing.
"""

import logging
from datetime import datetime, date
from typing import Optional, List, Dict, Tuple

import config
import cache_db
import data_fetcher
import iv_engine
from backtest.black_scholes import round_strike_to_standard

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------
# Strike selection
# -----------------------------------------------------------------------

def _nearest_strike(target: float, contracts: List[Dict]) -> Optional[Dict]:
    """Find the contract with strike closest to target."""
    if not contracts:
        return None
    best = min(contracts, key=lambda c: abs(c["strike_price"] - target))
    return best


def select_short_strike(
    spot: float,
    contracts: List[Dict],
    right: str,
    t_years: float,
    iv: Optional[float],
) -> Optional[Dict]:
    """Select the short leg strike targeting the desired delta.

    1. Try: compute delta-based strike using IV
    2. Fallback: use 5% OTM strike
    Then snap to nearest available contract.
    """
    target_strike = None

    # Try delta-based selection
    if iv and iv > 0 and t_years > 0:
        if right.upper() == "P":
            target_delta = -config.TARGET_DELTA
        else:
            target_delta = config.TARGET_DELTA

        raw_strike = iv_engine.find_strike_by_delta(
            spot, t_years, iv, target_delta, right,
        )
        if raw_strike is not None:
            target_strike = raw_strike

    # Fallback to OTM percentage
    if target_strike is None:
        target_strike = iv_engine.find_strike_by_otm_pct(
            spot, config.FALLBACK_OTM_PCT, right,
        )

    return _nearest_strike(target_strike, contracts)


def select_long_strike(
    short_contract: Dict,
    contracts: List[Dict],
    spot: float,
    width_pct: float,
    right: str,
) -> Optional[Dict]:
    """Select the long leg strike at the specified width from short.

    For puts:  long_strike = short_strike - width
    For calls: long_strike = short_strike + width
    """
    width_dollars = spot * width_pct
    short_strike = short_contract["strike_price"]

    if right.upper() == "P":
        target = short_strike - width_dollars
    else:
        target = short_strike + width_dollars

    # Must be a different strike from short
    candidates = [c for c in contracts
                  if c["option_ticker"] != short_contract["option_ticker"]]
    if not candidates:
        return None

    best = _nearest_strike(target, candidates)

    # Validate: long must be further OTM than short
    if best:
        if right.upper() == "P" and best["strike_price"] >= short_strike:
            return None
        if right.upper() == "C" and best["strike_price"] <= short_strike:
            return None

    return best


# -----------------------------------------------------------------------
# Spread pricing
# -----------------------------------------------------------------------

def price_spread_entry(
    short_ticker: str,
    long_ticker: str,
    price_date: str,
) -> Optional[float]:
    """Price a credit spread at entry (selling).

    credit = short_bid - long_ask

    Uses real quotes if available, otherwise synthetic from bar close.
    Returns credit received (positive = good), or None if pricing fails.
    """
    short_bid = data_fetcher.get_option_price(short_ticker, price_date, "bid")
    long_ask = data_fetcher.get_option_price(long_ticker, price_date, "ask")

    if short_bid is None or long_ask is None:
        return None

    credit = short_bid - long_ask
    if credit <= 0:
        return None

    return round(credit, 2)


def price_spread_to_close(
    short_ticker: str,
    long_ticker: str,
    price_date: str,
) -> Optional[float]:
    """Price a credit spread to close (buying back).

    cost_to_close = short_ask - long_bid

    Returns cost to close (positive number), or None.
    """
    short_ask = data_fetcher.get_option_price(short_ticker, price_date, "ask")
    long_bid = data_fetcher.get_option_price(long_ticker, price_date, "bid")

    if short_ask is None or long_bid is None:
        return None

    cost = short_ask - long_bid
    return max(0.0, round(cost, 2))


def intrinsic_value_at_expiration(
    spot: float,
    short_strike: float,
    long_strike: float,
    right: str,
) -> float:
    """Calculate the spread's settlement value at expiration.

    Returns the amount the spread-seller must pay (debit, >= 0).
    If both legs expire OTM, returns 0 (full profit).
    """
    if right.upper() == "P":
        short_intrinsic = max(0, short_strike - spot)
        long_intrinsic = max(0, long_strike - spot)
    else:
        short_intrinsic = max(0, spot - short_strike)
        long_intrinsic = max(0, spot - long_strike)

    # Net settlement = what short owes minus what long receives
    settlement = short_intrinsic - long_intrinsic
    return max(0.0, round(settlement, 2))


# -----------------------------------------------------------------------
# Full spread construction
# -----------------------------------------------------------------------

def build_spread(
    underlying: str,
    trade_date: str,
    right: str,
    width_pct: float,
    spot: float,
    iv: Optional[float],
) -> Optional[Dict]:
    """Construct a credit spread for a given entry signal.

    Steps:
        1. Fetch available contracts in DTE window
        2. Select short strike (delta or OTM fallback)
        3. Select long strike at specified width
        4. Fetch option bars for both legs
        5. Price the spread

    Returns dict with spread details or None if construction fails.
    """
    contract_type = "put" if right.upper() == "P" else "call"

    # Get contracts
    contracts = data_fetcher.fetch_options_contracts(
        underlying, trade_date, contract_type,
    )
    if not contracts:
        logger.debug(f"  No {contract_type} contracts for {underlying} on {trade_date}")
        return None

    # Find best expiration: closest to ideal DTE, but must have enough strikes.
    # Standard monthly expirations (3rd Friday) tend to have the most strikes.
    # Rank by: enough strikes first, then closeness to ideal DTE.
    MIN_STRIKES_FOR_ENTRY = 20  # need enough strikes to build a spread
    dt_trade = datetime.strptime(trade_date, "%Y-%m-%d").date()

    expirations = set(c["expiration_date"] for c in contracts)
    exp_candidates = []
    for exp_str in expirations:
        exp_dt = datetime.strptime(exp_str, "%Y-%m-%d").date()
        dte = (exp_dt - dt_trade).days
        count = sum(1 for c in contracts if c["expiration_date"] == exp_str)
        dist = abs(dte - config.IDEAL_DTE)
        exp_candidates.append((exp_str, dte, count, dist))

    # Sort: prefer expirations with enough strikes; among those, closest to ideal DTE
    exp_candidates.sort(key=lambda x: (0 if x[2] >= MIN_STRIKES_FOR_ENTRY else 1, x[3]))

    if not exp_candidates:
        return None

    best_exp = exp_candidates[0][0]

    # Filter to best expiration
    exp_contracts = [c for c in contracts if c["expiration_date"] == best_exp]
    exp_dt = datetime.strptime(best_exp, "%Y-%m-%d").date()
    dte = (exp_dt - dt_trade).days
    t_years = dte / 365.0

    # Select strikes
    short_contract = select_short_strike(spot, exp_contracts, right, t_years, iv)
    if short_contract is None:
        return None

    long_contract = select_long_strike(
        short_contract, exp_contracts, spot, width_pct, right,
    )
    if long_contract is None:
        return None

    short_ticker = short_contract["option_ticker"]
    long_ticker = long_contract["option_ticker"]
    short_strike = short_contract["strike_price"]
    long_strike = long_contract["strike_price"]

    # Ensure we have option bar data for both legs
    data_fetcher.ensure_option_data(short_ticker, trade_date, best_exp)
    data_fetcher.ensure_option_data(long_ticker, trade_date, best_exp)

    # Price the spread
    credit = price_spread_entry(short_ticker, long_ticker, trade_date)
    if credit is None or credit <= 0:
        return None

    width = abs(short_strike - long_strike)
    max_loss = (width - credit) * 100  # per contract

    return {
        "underlying": underlying,
        "trade_date": trade_date,
        "expiration_date": best_exp,
        "dte": dte,
        "right": right.upper(),
        "short_ticker": short_ticker,
        "long_ticker": long_ticker,
        "short_strike": short_strike,
        "long_strike": long_strike,
        "width": width,
        "credit": credit,
        "max_loss": max_loss,
        "spot": spot,
        "iv": iv,
        "has_real_quote": data_fetcher.has_real_quote(short_ticker, trade_date),
    }
