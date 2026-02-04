"""
Black-Scholes Option Pricing and Greeks
========================================
Provides theoretical option pricing for backtesting when historical
option prices are not available. Uses underlying price + IV to reconstruct
what options would have cost.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass
class OptionPrice:
    """Result of option pricing calculation."""
    price: float
    delta: float
    gamma: float
    vega: float
    theta: float
    iv: float
    # Inputs stored for reference
    spot: float
    strike: float
    t_years: float
    rate: float
    right: str  # 'C' or 'P'


def norm_cdf(x: float) -> float:
    """Standard normal cumulative distribution function."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def norm_pdf(x: float) -> float:
    """Standard normal probability density function."""
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def black_scholes_price(
    spot: float,
    strike: float,
    t_years: float,
    rate: float,
    iv: float,
    right: str,  # 'C' or 'P'
) -> Optional[float]:
    """
    Calculate Black-Scholes theoretical option price.

    Args:
        spot: Current underlying price
        strike: Option strike price
        t_years: Time to expiration in years
        rate: Risk-free interest rate (decimal, e.g., 0.05 for 5%)
        iv: Implied volatility (decimal, e.g., 0.25 for 25%)
        right: 'C' for call, 'P' for put

    Returns:
        Theoretical option price, or None if inputs invalid
    """
    if spot <= 0 or strike <= 0 or t_years <= 0 or iv <= 0:
        return None

    try:
        sqrt_t = math.sqrt(t_years)
        d1 = (math.log(spot / strike) + (rate + 0.5 * iv * iv) * t_years) / (iv * sqrt_t)
        d2 = d1 - iv * sqrt_t

        discount = math.exp(-rate * t_years)

        if right.upper() == 'C':
            price = spot * norm_cdf(d1) - strike * discount * norm_cdf(d2)
        else:  # Put
            price = strike * discount * norm_cdf(-d2) - spot * norm_cdf(-d1)

        return max(0.0, price)

    except (ValueError, ZeroDivisionError, OverflowError):
        return None


def black_scholes_greeks(
    spot: float,
    strike: float,
    t_years: float,
    rate: float,
    iv: float,
    right: str,
) -> Optional[OptionPrice]:
    """
    Calculate full Black-Scholes price and Greeks.

    Returns:
        OptionPrice dataclass with price and all Greeks, or None if invalid
    """
    if spot <= 0 or strike <= 0 or t_years <= 0 or iv <= 0:
        return None

    try:
        sqrt_t = math.sqrt(t_years)
        d1 = (math.log(spot / strike) + (rate + 0.5 * iv * iv) * t_years) / (iv * sqrt_t)
        d2 = d1 - iv * sqrt_t

        discount = math.exp(-rate * t_years)
        nd1 = norm_cdf(d1)
        nd2 = norm_cdf(d2)
        npd1 = norm_pdf(d1)

        # Price
        if right.upper() == 'C':
            price = spot * nd1 - strike * discount * nd2
            delta = nd1
            theta_term = norm_cdf(d2)
        else:
            price = strike * discount * norm_cdf(-d2) - spot * norm_cdf(-d1)
            delta = nd1 - 1.0  # Negative for puts
            theta_term = -norm_cdf(-d2)

        # Greeks (per share, standard conventions)
        gamma = npd1 / (spot * iv * sqrt_t)
        vega = spot * npd1 * sqrt_t / 100.0  # Per 1% IV move

        # Theta (per day, negative for long options)
        theta_part1 = -(spot * npd1 * iv) / (2.0 * sqrt_t)
        theta_part2 = rate * strike * discount * theta_term
        theta = (theta_part1 - theta_part2) / 365.0

        return OptionPrice(
            price=max(0.0, price),
            delta=delta,
            gamma=gamma,
            vega=vega,
            theta=theta,
            iv=iv,
            spot=spot,
            strike=strike,
            t_years=t_years,
            rate=rate,
            right=right.upper(),
        )

    except (ValueError, ZeroDivisionError, OverflowError):
        return None


def find_strike_for_delta(
    spot: float,
    t_years: float,
    rate: float,
    iv: float,
    target_delta: float,
    right: str,
    precision: float = 0.01,
) -> Optional[float]:
    """
    Find the strike price that gives approximately the target delta.
    Uses bisection search.

    Args:
        target_delta: Desired delta (negative for puts, e.g., -0.25)
        precision: How close to get to target delta

    Returns:
        Strike price, or None if not found
    """
    if spot <= 0 or t_years <= 0 or iv <= 0:
        return None

    # Define search bounds (generous range)
    k_low = spot * 0.5
    k_high = spot * 1.5

    # Bisection search
    for _ in range(50):  # Max iterations
        k_mid = (k_low + k_high) / 2.0
        result = black_scholes_greeks(spot, k_mid, t_years, rate, iv, right)

        if result is None:
            return None

        current_delta = result.delta

        if abs(current_delta - target_delta) < precision:
            return k_mid

        # For calls: higher strike = lower delta (delta decreases from ~1 to ~0)
        # For puts: higher strike = more negative delta (delta decreases from ~0 to ~-1)
        if right.upper() == 'C':
            if current_delta > target_delta:
                k_low = k_mid  # Need higher strike to lower delta
            else:
                k_high = k_mid
        else:  # Put
            # For puts: target_delta is negative (e.g., -0.25)
            # Higher strike = more ITM = more negative delta (closer to -1)
            # Lower strike = more OTM = less negative delta (closer to 0)
            if current_delta < target_delta:
                # Current is more negative, need less negative -> lower strike
                k_high = k_mid
            else:
                # Current is less negative, need more negative -> higher strike
                k_low = k_mid

    return (k_low + k_high) / 2.0  # Return best estimate


# =============================================================================
# VOLATILITY SKEW MODEL
# =============================================================================

def apply_put_skew(
    atm_iv: float,
    strike: float,
    spot: float,
    skew_slope: float = 0.0015,  # IV increase per 1% OTM
) -> float:
    """
    Apply volatility skew to OTM puts.

    Real markets show higher IV for OTM puts (the "volatility smile/skew").
    This models a linear approximation of that effect.

    Args:
        atm_iv: At-the-money implied volatility
        strike: Put strike price
        spot: Current underlying price
        skew_slope: How much IV increases per 1% OTM (default: 0.15% IV per 1% OTM)

    Returns:
        Adjusted IV for the OTM put
    """
    if strike >= spot:
        # ATM or ITM put - no skew adjustment
        return atm_iv

    # Calculate how far OTM the put is (as percentage)
    otm_pct = (spot - strike) / spot * 100  # e.g., 5% OTM = 5.0

    # Apply skew: IV increases as we go further OTM
    # Typical skew might add 0.10-0.20% IV per 1% OTM
    iv_adjustment = otm_pct * skew_slope

    return atm_iv * (1 + iv_adjustment)


# =============================================================================
# BID/ASK SPREAD MODEL
# =============================================================================

def get_bid_ask(
    mid_price: float,
    spread_pct: float = 0.05,  # 5% bid/ask spread
) -> tuple:
    """
    Calculate bid and ask prices from mid price.

    Args:
        mid_price: Theoretical mid price
        spread_pct: Total bid/ask spread as percentage of mid (default 5%)

    Returns:
        Tuple of (bid, ask) prices
    """
    if mid_price <= 0:
        return (0.0, 0.0)

    half_spread = spread_pct / 2
    bid = mid_price * (1 - half_spread)
    ask = mid_price * (1 + half_spread)

    # Minimum tick size ($0.01)
    bid = max(0.01, round(bid, 2))
    ask = max(0.01, round(ask, 2))

    return (bid, ask)


def round_strike_to_standard(strike: float, spot: float) -> float:
    """
    Round a strike to standard option strike intervals.

    Rules (approximate):
        - Under $50: $0.50 or $1 increments
        - $50-$200: $2.50 or $5 increments
        - Over $200: $5 or $10 increments
    """
    if spot < 50:
        step = 1.0
    elif spot < 200:
        step = 5.0
    else:
        step = 10.0

    return round(strike / step) * step


def calculate_spread_price(
    spot: float,
    short_strike: float,
    long_strike: float,
    t_years: float,
    rate: float,
    iv: float,
    right: str,
    slippage_per_leg: float = 0.02,
) -> Optional[Tuple[float, float, float]]:
    """
    Calculate theoretical credit spread price.

    For put credit spread: sell higher strike, buy lower strike
    For call credit spread: sell lower strike, buy higher strike

    Args:
        slippage_per_leg: Assumed slippage per leg (default $0.02)

    Returns:
        Tuple of (credit_mid, credit_conservative, max_loss) or None
    """
    short_opt = black_scholes_greeks(spot, short_strike, t_years, rate, iv, right)
    long_opt = black_scholes_greeks(spot, long_strike, t_years, rate, iv, right)

    if short_opt is None or long_opt is None:
        return None

    credit_mid = short_opt.price - long_opt.price
    credit_conservative = credit_mid - (2 * slippage_per_leg)  # Slippage on both legs

    width = abs(short_strike - long_strike)
    max_loss = (width - max(0, credit_conservative)) * 100  # Per contract

    return (credit_mid, max(0.0, credit_conservative), max_loss)


def calculate_spread_price_realistic(
    spot: float,
    short_strike: float,
    long_strike: float,
    t_years: float,
    rate: float,
    atm_iv: float,
    right: str,
    bid_ask_spread_pct: float = 0.05,  # 5% bid/ask spread
    use_skew: bool = True,
    skew_slope: float = 0.0015,
) -> Optional[dict]:
    """
    Calculate credit spread price with realistic execution assumptions.

    For OPENING a put credit spread:
    - SELL short put: receive BID (lower)
    - BUY long put: pay ASK (higher)

    For CLOSING:
    - BUY short put: pay ASK (higher)
    - SELL long put: receive BID (lower)

    Args:
        bid_ask_spread_pct: Total bid/ask spread as % of mid price (default 5%)
        use_skew: Apply volatility skew to OTM puts
        skew_slope: Skew intensity (IV increase per 1% OTM)

    Returns:
        Dict with open_credit, max_loss, short/long prices, IVs used
    """
    # Apply volatility skew for puts
    if right.upper() == 'P' and use_skew:
        short_iv = apply_put_skew(atm_iv, short_strike, spot, skew_slope)
        long_iv = apply_put_skew(atm_iv, long_strike, spot, skew_slope)
    else:
        short_iv = atm_iv
        long_iv = atm_iv

    # Calculate theoretical mid prices
    short_opt = black_scholes_greeks(spot, short_strike, t_years, rate, short_iv, right)
    long_opt = black_scholes_greeks(spot, long_strike, t_years, rate, long_iv, right)

    if short_opt is None or long_opt is None:
        return None

    # Get bid/ask for each leg
    short_bid, short_ask = get_bid_ask(short_opt.price, bid_ask_spread_pct)
    long_bid, long_ask = get_bid_ask(long_opt.price, bid_ask_spread_pct)

    # Opening credit = sell short (receive bid) - buy long (pay ask)
    open_credit = short_bid - long_ask

    # If we can't open for a credit, skip
    if open_credit <= 0:
        return None

    width = abs(short_strike - long_strike)
    max_loss = (width - open_credit) * 100  # Per contract

    return {
        'short_strike': short_strike,
        'long_strike': long_strike,
        'open_credit': open_credit,
        'max_loss': max_loss,
        'width': width,
        # Mid prices for reference
        'short_mid': short_opt.price,
        'long_mid': long_opt.price,
        'credit_mid': short_opt.price - long_opt.price,
        # Bid/ask used
        'short_bid': short_bid,
        'short_ask': short_ask,
        'long_bid': long_bid,
        'long_ask': long_ask,
        # IVs used
        'short_iv': short_iv,
        'long_iv': long_iv,
        'atm_iv': atm_iv,
        # Greeks
        'short_delta': short_opt.delta,
        'long_delta': long_opt.delta,
    }


def price_spread_to_close(
    spot: float,
    short_strike: float,
    long_strike: float,
    t_years: float,
    rate: float,
    atm_iv: float,
    right: str,
    bid_ask_spread_pct: float = 0.05,
    use_skew: bool = True,
    skew_slope: float = 0.0015,
) -> Optional[float]:
    """
    Calculate the cost to close a credit spread (debit to close).

    For CLOSING a put credit spread:
    - BUY short put: pay ASK (higher)
    - SELL long put: receive BID (lower)

    Returns:
        Cost to close (positive number), or None if pricing fails
    """
    # Apply volatility skew for puts
    if right.upper() == 'P' and use_skew:
        short_iv = apply_put_skew(atm_iv, short_strike, spot, skew_slope)
        long_iv = apply_put_skew(atm_iv, long_strike, spot, skew_slope)
    else:
        short_iv = atm_iv
        long_iv = atm_iv

    # Calculate theoretical mid prices
    short_price = black_scholes_price(spot, short_strike, t_years, rate, short_iv, right)
    long_price = black_scholes_price(spot, long_strike, t_years, rate, long_iv, right)

    if short_price is None or long_price is None:
        return None

    # Get bid/ask for each leg
    short_bid, short_ask = get_bid_ask(short_price, bid_ask_spread_pct)
    long_bid, long_ask = get_bid_ask(long_price, bid_ask_spread_pct)

    # Cost to close = buy short (pay ask) - sell long (receive bid)
    close_debit = short_ask - long_bid

    return max(0.0, close_debit)


def calculate_condor_price(
    spot: float,
    long_put_strike: float,
    short_put_strike: float,
    short_call_strike: float,
    long_call_strike: float,
    t_years: float,
    rate: float,
    iv: float,
    slippage_per_leg: float = 0.02,
) -> Optional[dict]:
    """
    Calculate theoretical iron condor price.

    Structure: Buy LP, Sell SP, Sell SC, Buy LC

    Returns:
        Dict with credit_mid, credit_conservative, max_loss, put_width, call_width
    """
    lp = black_scholes_greeks(spot, long_put_strike, t_years, rate, iv, 'P')
    sp = black_scholes_greeks(spot, short_put_strike, t_years, rate, iv, 'P')
    sc = black_scholes_greeks(spot, short_call_strike, t_years, rate, iv, 'C')
    lc = black_scholes_greeks(spot, long_call_strike, t_years, rate, iv, 'C')

    if any(x is None for x in [lp, sp, sc, lc]):
        return None

    # Credit = (sell short put + sell short call) - (buy long put + buy long call)
    credit_mid = (sp.price + sc.price) - (lp.price + lc.price)
    credit_conservative = credit_mid - (4 * slippage_per_leg)

    put_width = short_put_strike - long_put_strike
    call_width = long_call_strike - short_call_strike
    max_width = max(put_width, call_width)
    max_loss = (max_width - max(0, credit_conservative)) * 100

    # Net Greeks
    net_delta = (lp.delta + lc.delta) - (sp.delta + sc.delta)
    net_vega = (lp.vega + lc.vega) - (sp.vega + sc.vega)
    net_theta = (sp.theta + sc.theta) - (lp.theta + lc.theta)

    return {
        'credit_mid': credit_mid,
        'credit_conservative': max(0.0, credit_conservative),
        'max_loss': max_loss,
        'put_width': put_width,
        'call_width': call_width,
        'max_width': max_width,
        'net_delta': net_delta,
        'net_vega': net_vega,
        'net_theta': net_theta,
        # Individual deltas for reference
        'short_put_delta': sp.delta,
        'short_call_delta': sc.delta,
    }


def apply_call_skew(
    atm_iv: float,
    strike: float,
    spot: float,
    skew_slope: float = 0.0008,  # Calls have less skew than puts
) -> float:
    """
    Apply volatility skew to OTM calls.

    OTM calls typically have slight IV increase but less than puts.
    """
    if strike <= spot:
        return atm_iv

    otm_pct = (strike - spot) / spot * 100
    iv_adjustment = otm_pct * skew_slope

    return atm_iv * (1 + iv_adjustment)


def calculate_condor_price_realistic(
    spot: float,
    long_put_strike: float,
    short_put_strike: float,
    short_call_strike: float,
    long_call_strike: float,
    t_years: float,
    rate: float,
    atm_iv: float,
    bid_ask_spread_pct: float = 0.05,
    use_skew: bool = True,
    skew_slope_put: float = 0.0015,
    skew_slope_call: float = 0.0008,
) -> Optional[dict]:
    """
    Calculate iron condor price with realistic bid/ask and skew.

    For OPENING a condor:
    - SELL short put: receive BID
    - BUY long put: pay ASK
    - SELL short call: receive BID
    - BUY long call: pay ASK

    Returns:
        Dict with open_credit, max_loss, individual prices
    """
    # Apply volatility skew
    if use_skew:
        lp_iv = apply_put_skew(atm_iv, long_put_strike, spot, skew_slope_put)
        sp_iv = apply_put_skew(atm_iv, short_put_strike, spot, skew_slope_put)
        sc_iv = apply_call_skew(atm_iv, short_call_strike, spot, skew_slope_call)
        lc_iv = apply_call_skew(atm_iv, long_call_strike, spot, skew_slope_call)
    else:
        lp_iv = sp_iv = sc_iv = lc_iv = atm_iv

    # Calculate theoretical mid prices
    lp = black_scholes_greeks(spot, long_put_strike, t_years, rate, lp_iv, 'P')
    sp = black_scholes_greeks(spot, short_put_strike, t_years, rate, sp_iv, 'P')
    sc = black_scholes_greeks(spot, short_call_strike, t_years, rate, sc_iv, 'C')
    lc = black_scholes_greeks(spot, long_call_strike, t_years, rate, lc_iv, 'C')

    if any(x is None for x in [lp, sp, sc, lc]):
        return None

    # Get bid/ask for each leg
    lp_bid, lp_ask = get_bid_ask(lp.price, bid_ask_spread_pct)
    sp_bid, sp_ask = get_bid_ask(sp.price, bid_ask_spread_pct)
    sc_bid, sc_ask = get_bid_ask(sc.price, bid_ask_spread_pct)
    lc_bid, lc_ask = get_bid_ask(lc.price, bid_ask_spread_pct)

    # Opening credit = (sell shorts at bid) - (buy longs at ask)
    open_credit = (sp_bid + sc_bid) - (lp_ask + lc_ask)

    if open_credit <= 0:
        return None

    put_width = short_put_strike - long_put_strike
    call_width = long_call_strike - short_call_strike
    max_width = max(put_width, call_width)
    max_loss = (max_width - open_credit) * 100

    # Mid credit for reference
    credit_mid = (sp.price + sc.price) - (lp.price + lc.price)

    return {
        'open_credit': open_credit,
        'credit_mid': credit_mid,
        'max_loss': max_loss,
        'put_width': put_width,
        'call_width': call_width,
        'max_width': max_width,
        # Individual prices and IVs
        'lp_mid': lp.price, 'lp_bid': lp_bid, 'lp_ask': lp_ask, 'lp_iv': lp_iv,
        'sp_mid': sp.price, 'sp_bid': sp_bid, 'sp_ask': sp_ask, 'sp_iv': sp_iv,
        'sc_mid': sc.price, 'sc_bid': sc_bid, 'sc_ask': sc_ask, 'sc_iv': sc_iv,
        'lc_mid': lc.price, 'lc_bid': lc_bid, 'lc_ask': lc_ask, 'lc_iv': lc_iv,
        # Greeks
        'short_put_delta': sp.delta,
        'short_call_delta': sc.delta,
    }


def price_condor_to_close(
    spot: float,
    long_put_strike: float,
    short_put_strike: float,
    short_call_strike: float,
    long_call_strike: float,
    t_years: float,
    rate: float,
    atm_iv: float,
    bid_ask_spread_pct: float = 0.05,
    use_skew: bool = True,
    skew_slope_put: float = 0.0015,
    skew_slope_call: float = 0.0008,
) -> Optional[float]:
    """
    Calculate cost to close a condor position (debit).

    For CLOSING:
    - BUY short put: pay ASK
    - SELL long put: receive BID
    - BUY short call: pay ASK
    - SELL long call: receive BID

    Returns:
        Cost to close (positive = debit), or None
    """
    if use_skew:
        lp_iv = apply_put_skew(atm_iv, long_put_strike, spot, skew_slope_put)
        sp_iv = apply_put_skew(atm_iv, short_put_strike, spot, skew_slope_put)
        sc_iv = apply_call_skew(atm_iv, short_call_strike, spot, skew_slope_call)
        lc_iv = apply_call_skew(atm_iv, long_call_strike, spot, skew_slope_call)
    else:
        lp_iv = sp_iv = sc_iv = lc_iv = atm_iv

    lp_price = black_scholes_price(spot, long_put_strike, t_years, rate, lp_iv, 'P')
    sp_price = black_scholes_price(spot, short_put_strike, t_years, rate, sp_iv, 'P')
    sc_price = black_scholes_price(spot, short_call_strike, t_years, rate, sc_iv, 'C')
    lc_price = black_scholes_price(spot, long_call_strike, t_years, rate, lc_iv, 'C')

    if any(x is None for x in [lp_price, sp_price, sc_price, lc_price]):
        return None

    lp_bid, lp_ask = get_bid_ask(lp_price, bid_ask_spread_pct)
    sp_bid, sp_ask = get_bid_ask(sp_price, bid_ask_spread_pct)
    sc_bid, sc_ask = get_bid_ask(sc_price, bid_ask_spread_pct)
    lc_bid, lc_ask = get_bid_ask(lc_price, bid_ask_spread_pct)

    # Close debit = (buy shorts at ask) - (sell longs at bid)
    close_debit = (sp_ask + sc_ask) - (lp_bid + lc_bid)

    return max(0.0, close_debit)


def estimate_pop_lognormal(
    spot: float,
    low_strike: float,
    high_strike: float,
    iv: float,
    t_years: float,
    drift: float = 0.0,
) -> Optional[float]:
    """
    Estimate probability of underlying staying between two strikes
    under log-normal assumption.

    Args:
        drift: Expected return (default 0 for risk-neutral)

    Returns:
        Probability as decimal (0 to 1), or None if invalid
    """
    if spot <= 0 or low_strike <= 0 or high_strike <= 0:
        return None
    if iv <= 0 or t_years <= 0 or high_strike <= low_strike:
        return None

    try:
        sqrt_t = math.sqrt(t_years)
        sigma_sqrt_t = iv * sqrt_t

        mu = math.log(spot) + (drift - 0.5 * iv * iv) * t_years

        z_low = (math.log(low_strike) - mu) / sigma_sqrt_t
        z_high = (math.log(high_strike) - mu) / sigma_sqrt_t

        prob = norm_cdf(z_high) - norm_cdf(z_low)
        return max(0.0, min(1.0, prob))

    except (ValueError, ZeroDivisionError, OverflowError):
        return None


def estimate_prob_above(
    spot: float,
    strike: float,
    iv: float,
    t_years: float,
    drift: float = 0.0,
) -> Optional[float]:
    """
    Estimate probability of underlying finishing above a strike.
    Useful for put credit spread POP estimation.
    """
    if spot <= 0 or strike <= 0 or iv <= 0 or t_years <= 0:
        return None

    try:
        sqrt_t = math.sqrt(t_years)
        mu = math.log(spot) + (drift - 0.5 * iv * iv) * t_years
        z = (math.log(strike) - mu) / (iv * sqrt_t)
        return 1.0 - norm_cdf(z)
    except (ValueError, ZeroDivisionError, OverflowError):
        return None


# =============================================================================
# TECHNICAL INDICATORS (for entry signal validation)
# =============================================================================

def calculate_sma(prices: list, period: int) -> Optional[float]:
    """Calculate simple moving average."""
    if len(prices) < period:
        return None
    return sum(prices[-period:]) / period


def calculate_rsi(prices: list, period: int = 14) -> Optional[float]:
    """
    Calculate Relative Strength Index.

    Returns:
        RSI value (0-100), or None if insufficient data
    """
    if len(prices) < period + 1:
        return None

    deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]

    gains = [d if d > 0 else 0 for d in deltas[-period:]]
    losses = [-d if d < 0 else 0 for d in deltas[-period:]]

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    rsi = 100.0 - (100.0 / (1.0 + rs))

    return rsi


def calculate_hv(prices: list, period: int) -> Optional[float]:
    """
    Calculate historical volatility (annualized).

    Args:
        prices: List of closing prices
        period: Lookback period in days

    Returns:
        Annualized volatility as decimal, or None if insufficient data
    """
    if len(prices) < period + 1:
        return None

    # Calculate log returns
    returns = [math.log(prices[i] / prices[i-1])
               for i in range(len(prices) - period, len(prices))]

    if len(returns) < 2:
        return None

    # Standard deviation of returns
    mean_ret = sum(returns) / len(returns)
    variance = sum((r - mean_ret) ** 2 for r in returns) / (len(returns) - 1)

    # Annualize
    return math.sqrt(variance * 252)


def calculate_iv_rank(current_iv: float, iv_history: list, lookback: int = 252) -> Optional[float]:
    """
    Calculate IV Rank: where current IV sits relative to its range.

    Returns:
        IV Rank as decimal (0 to 1), or None if insufficient history
    """
    if not iv_history or len(iv_history) < 20:  # Need reasonable sample
        return None

    history = iv_history[-lookback:] if len(iv_history) > lookback else iv_history

    iv_low = min(history)
    iv_high = max(history)

    if iv_high <= iv_low:
        return 0.5  # Flat IV history

    rank = (current_iv - iv_low) / (iv_high - iv_low)
    return max(0.0, min(1.0, rank))


if __name__ == "__main__":
    # Quick test
    print("Black-Scholes Test:")
    print("-" * 50)

    spot = 100.0
    strike = 95.0
    t_years = 30 / 365.0
    rate = 0.05
    iv = 0.25

    put = black_scholes_greeks(spot, strike, t_years, rate, iv, 'P')
    if put:
        print(f"Put @ {strike}: ${put.price:.2f}, Delta={put.delta:.3f}, Vega={put.vega:.3f}")

    call = black_scholes_greeks(spot, 105.0, t_years, rate, iv, 'C')
    if call:
        print(f"Call @ 105: ${call.price:.2f}, Delta={call.delta:.3f}, Vega={call.vega:.3f}")

    # Find 25 delta put strike
    strike_25d = find_strike_for_delta(spot, t_years, rate, iv, -0.25, 'P')
    print(f"\n25-delta put strike: {strike_25d:.2f}")

    # Test POP
    pop = estimate_prob_above(spot, strike_25d, iv, t_years)
    print(f"POP (above {strike_25d:.2f}): {pop*100:.1f}%")
