"""
Execution Utilities
===================
Bid/ask handling, fill price calculation, and liquidity filters.
Centralized to ensure consistent execution assumptions across scripts.
"""

from typing import Optional, Tuple
from dataclasses import dataclass


@dataclass
class Quote:
    """Represents a bid/ask quote for an option."""
    bid: Optional[float] = None
    ask: Optional[float] = None
    close: Optional[float] = None
    volume: Optional[int] = None
    open_interest: Optional[int] = None

    @property
    def is_valid(self) -> bool:
        """Check if quote has valid bid/ask."""
        return (
            self.bid is not None
            and self.ask is not None
            and self.bid > 0
            and self.ask > 0
            and self.ask >= self.bid
        )

    @property
    def mid(self) -> Optional[float]:
        """Calculate midpoint price."""
        if self.is_valid:
            return (self.bid + self.ask) / 2.0
        return None

    @property
    def spread(self) -> Optional[float]:
        """Calculate bid-ask spread."""
        if self.is_valid:
            return self.ask - self.bid
        return None

    @property
    def spread_pct(self) -> Optional[float]:
        """Calculate spread as percentage of midpoint."""
        if self.is_valid and self.mid > 0:
            return self.spread / self.mid
        return None


def get_quote_from_eod(eod_row: dict) -> Quote:
    """
    Extract quote from an EOD data row.

    Args:
        eod_row: Dictionary with bid, ask, close, volume, open_interest

    Returns:
        Quote object
    """
    if eod_row is None:
        return Quote()

    return Quote(
        bid=eod_row.get("bid"),
        ask=eod_row.get("ask"),
        close=eod_row.get("close"),
        volume=eod_row.get("volume"),
        open_interest=eod_row.get("open_interest"),
    )


def is_quote_tradeable(
    quote: Quote,
    max_spread_pct: float = 0.01,
    skip_if_no_quote: bool = True,
) -> Tuple[bool, str]:
    """
    Check if a quote is tradeable based on liquidity rules.

    Args:
        quote: Quote object
        max_spread_pct: Maximum acceptable spread as percentage of mid
        skip_if_no_quote: If True, reject quotes without bid/ask

    Returns:
        Tuple of (is_tradeable, reason_if_rejected)
    """
    if not quote.is_valid:
        if skip_if_no_quote:
            return False, "no_quote"
        # If not skipping, we'd need synthetic fallback (not recommended)
        return False, "no_quote"

    if quote.spread_pct > max_spread_pct:
        return False, f"spread_too_wide ({quote.spread_pct:.1%})"

    return True, ""


def calculate_fill_price(
    quote: Quote,
    is_buy: bool,
    haircut_pct: float = 0.25,
    use_midpoint: bool = True,
) -> Optional[float]:
    """
    Calculate expected fill price with haircut.

    For buys: mid + (haircut * half_spread)
    For sells: mid - (haircut * half_spread)

    This models realistic execution where you typically don't get
    exactly the midpoint.

    Args:
        quote: Quote object
        is_buy: True for buy orders, False for sell orders
        haircut_pct: Percentage of half-spread worse than mid (default 25%)
        use_midpoint: If True, calculate from mid; if False, use bid/ask

    Returns:
        Fill price or None if quote invalid
    """
    if not quote.is_valid:
        return None

    if not use_midpoint:
        # Conservative: buy at ask, sell at bid
        return quote.ask if is_buy else quote.bid

    # Midpoint with haircut
    half_spread = quote.spread / 2.0

    if is_buy:
        return quote.mid + (haircut_pct * half_spread)
    else:
        return quote.mid - (haircut_pct * half_spread)


def calculate_intrinsic_value(spot: float, strike: float, option_type: str = "C") -> float:
    """
    Calculate intrinsic value of an option.

    Args:
        spot: Current underlying price
        strike: Option strike price
        option_type: "C" for call, "P" for put

    Returns:
        Intrinsic value (always >= 0)
    """
    if option_type.upper() == "C":
        return max(0, spot - strike)
    else:
        return max(0, strike - spot)


def get_bid_ask_fallback(
    quote: Quote,
    spot: float,
    strike: float,
    option_type: str = "C",
    use_synthetic: bool = False,
) -> Tuple[Optional[float], Optional[float]]:
    """
    Get bid/ask prices with optional fallback.

    IMPORTANT: Synthetic fallback is NOT recommended for production use.
    It can create unrealistic fills in precisely the cases where data
    quality is worst.

    Args:
        quote: Quote object
        spot: Current underlying price (for intrinsic calculation)
        strike: Option strike price
        option_type: "C" for call, "P" for put
        use_synthetic: If True, synthesize from close price (NOT RECOMMENDED)

    Returns:
        Tuple of (bid, ask) or (None, None) if unavailable
    """
    if quote.is_valid:
        return quote.bid, quote.ask

    if not use_synthetic:
        return None, None

    # FALLBACK (not recommended): synthesize from close
    # This can understate slippage when data quality is worst
    if quote.close and quote.close > 0:
        # Apply tiny synthetic spread
        synthetic_bid = quote.close * 0.998
        synthetic_ask = quote.close * 1.002
        return synthetic_bid, synthetic_ask

    # Last resort: use intrinsic value
    intrinsic = calculate_intrinsic_value(spot, strike, option_type)
    if intrinsic > 0:
        return intrinsic * 0.998, intrinsic * 1.002

    return None, None


@dataclass
class ExecutionResult:
    """Result of attempting to execute a trade."""
    success: bool
    fill_price: Optional[float] = None
    quantity: int = 0
    reason: str = ""
    quote: Optional[Quote] = None


def attempt_entry(
    quote: Quote,
    available_cash: float,
    max_contracts: int,
    max_spread_pct: float = 0.01,
    haircut_pct: float = 0.25,
    skip_if_no_quote: bool = True,
) -> ExecutionResult:
    """
    Attempt to enter a position with full execution logic.

    Args:
        quote: Quote object for the option
        available_cash: Cash available for purchase
        max_contracts: Maximum contracts allowed (e.g., from delta cap)
        max_spread_pct: Maximum acceptable spread
        haircut_pct: Fill haircut percentage
        skip_if_no_quote: If True, fail if no quote available

    Returns:
        ExecutionResult with success/failure and details
    """
    # Check if quote is tradeable
    tradeable, reason = is_quote_tradeable(quote, max_spread_pct, skip_if_no_quote)
    if not tradeable:
        return ExecutionResult(success=False, reason=reason, quote=quote)

    # Calculate fill price
    fill_price = calculate_fill_price(quote, is_buy=True, haircut_pct=haircut_pct)
    if fill_price is None:
        return ExecutionResult(success=False, reason="invalid_fill_price", quote=quote)

    # Calculate affordable quantity
    contract_cost = fill_price * 100
    max_by_cash = int(available_cash / contract_cost) if contract_cost > 0 else 0

    quantity = min(max_contracts, max_by_cash)
    if quantity <= 0:
        return ExecutionResult(
            success=False,
            reason="insufficient_cash_or_delta",
            fill_price=fill_price,
            quote=quote,
        )

    return ExecutionResult(
        success=True,
        fill_price=fill_price,
        quantity=quantity,
        quote=quote,
    )


def attempt_exit(
    quote: Quote,
    quantity: int,
    max_spread_pct: float = 0.01,
    haircut_pct: float = 0.25,
    skip_if_no_quote: bool = True,
    spot: float = None,
    strike: float = None,
) -> ExecutionResult:
    """
    Attempt to exit a position with full execution logic.

    For exits, we're more lenient about spreads since we may need to exit
    regardless of conditions (e.g., SMA breach).

    Args:
        quote: Quote object for the option
        quantity: Number of contracts to sell
        max_spread_pct: Maximum acceptable spread (may be ignored for forced exits)
        haircut_pct: Fill haircut percentage
        skip_if_no_quote: If True, fail if no quote available
        spot: Current spot price (for intrinsic fallback)
        strike: Strike price (for intrinsic fallback)

    Returns:
        ExecutionResult with success/failure and details
    """
    # Check if quote is tradeable
    tradeable, reason = is_quote_tradeable(quote, max_spread_pct, skip_if_no_quote)

    if tradeable:
        fill_price = calculate_fill_price(quote, is_buy=False, haircut_pct=haircut_pct)
        return ExecutionResult(
            success=True,
            fill_price=fill_price,
            quantity=quantity,
            quote=quote,
        )

    # For exits, we may need to use intrinsic value as fallback
    if spot is not None and strike is not None:
        intrinsic = calculate_intrinsic_value(spot, strike, "C")
        if intrinsic > 0:
            # Use intrinsic with a small haircut
            fill_price = intrinsic * 0.98
            return ExecutionResult(
                success=True,
                fill_price=fill_price,
                quantity=quantity,
                reason="intrinsic_fallback",
                quote=quote,
            )

    return ExecutionResult(success=False, reason=reason, quote=quote)
