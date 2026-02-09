"""
Strategy Configuration
======================
Centralized configuration for the 80-Delta Call Strategy.
All parameters in one place to prevent drift across scripts.
"""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class StrategyConfig:
    """
    Canonical configuration for the 80-Delta Call Strategy.

    Reference implementation: 1,000 SPY shares + $100,000 options cash
    Delta cap = share count (1,000)
    """

    # Portfolio structure
    shares: int = 1000
    options_cash: float = 100_000.0

    # Delta targeting (band, not single point)
    delta_target: float = 0.80
    delta_min: float = 0.70
    delta_max: float = 0.90
    delta_cap: Optional[int] = None  # Defaults to shares if None

    # Entry rules
    dte_target: int = 120
    dte_min: int = 90
    dte_max: int = 150
    sma_period: int = 200
    monthly_only: bool = True

    # Exit rules
    profit_target: float = 0.50  # 50% gain
    max_hold_days: int = 60
    sma_exit_threshold: float = 0.02  # Exit when 2% below SMA

    # Execution rules
    skip_if_no_quote: bool = True  # Skip trade if bid/ask missing
    spread_max_pct: float = 0.01  # Max 1% spread
    fill_haircut_pct: float = 0.25  # Expect 25% of spread worse than mid
    use_midpoint: bool = True  # Enter at midpoint (with haircut)

    # Backtest settings
    risk_free_rate: float = 0.04

    def __post_init__(self):
        """Set delta_cap to shares if not specified."""
        if self.delta_cap is None:
            self.delta_cap = self.shares

    def get_fill_price(self, bid: float, ask: float, is_buy: bool) -> float:
        """
        Calculate expected fill price with haircut.

        For buys: mid + (haircut * half_spread)
        For sells: mid - (haircut * half_spread)
        """
        if bid <= 0 or ask <= 0 or ask < bid:
            return None

        mid = (bid + ask) / 2.0
        half_spread = (ask - bid) / 2.0

        if is_buy:
            return mid + (self.fill_haircut_pct * half_spread)
        else:
            return mid - (self.fill_haircut_pct * half_spread)

    def is_spread_acceptable(self, bid: float, ask: float) -> bool:
        """Check if bid-ask spread is within tolerance."""
        if bid <= 0 or ask <= 0:
            return False
        mid = (bid + ask) / 2.0
        spread_pct = (ask - bid) / mid
        return spread_pct <= self.spread_max_pct

    def is_delta_in_band(self, delta: float) -> bool:
        """Check if delta is within acceptable band."""
        return self.delta_min <= delta <= self.delta_max

    def to_dict(self) -> dict:
        """Export config as dictionary for logging."""
        return {
            "shares": self.shares,
            "options_cash": self.options_cash,
            "delta_target": self.delta_target,
            "delta_band": f"{self.delta_min:.0%}-{self.delta_max:.0%}",
            "delta_cap": self.delta_cap,
            "dte_target": self.dte_target,
            "dte_range": f"{self.dte_min}-{self.dte_max}",
            "sma_period": self.sma_period,
            "monthly_only": self.monthly_only,
            "profit_target": f"{self.profit_target:.0%}",
            "max_hold_days": self.max_hold_days,
            "sma_exit_threshold": f"{self.sma_exit_threshold:.0%}",
            "skip_if_no_quote": self.skip_if_no_quote,
            "spread_max_pct": f"{self.spread_max_pct:.1%}",
            "fill_haircut_pct": f"{self.fill_haircut_pct:.0%}",
        }

    def print_manifest(self, engine_name: str, start_date: str, end_date: str):
        """Print reproducibility manifest for backtest runs."""
        print("=" * 70)
        print("BACKTEST MANIFEST")
        print("=" * 70)
        print(f"  Engine:             {engine_name}")
        print(f"  Period:             {start_date} to {end_date}")
        print(f"  Shares:             {self.shares:,}")
        print(f"  Options Cash:       ${self.options_cash:,.0f}")
        print(f"  Delta Cap:          {self.delta_cap:,}")
        print(f"  Delta Band:         {self.delta_min:.0%} - {self.delta_max:.0%}")
        print(f"  DTE Target:         {self.dte_target} (range: {self.dte_min}-{self.dte_max})")
        print(f"  SMA Period:         {self.sma_period}")
        print(f"  Monthly Only:       {self.monthly_only}")
        print(f"  Profit Target:      {self.profit_target:.0%}")
        print(f"  Max Hold:           {self.max_hold_days} days")
        print(f"  SMA Exit:           {self.sma_exit_threshold:.0%} below SMA")
        print(f"  Execution:          {'Skip if no quote' if self.skip_if_no_quote else 'Synthetic fallback'}")
        print(f"  Max Spread:         {self.spread_max_pct:.1%}")
        print(f"  Fill Haircut:       {self.fill_haircut_pct:.0%} of spread")
        print("=" * 70)


# Default configuration instance
DEFAULT_CONFIG = StrategyConfig()


# Variant configurations for testing
AGGRESSIVE_CONFIG = StrategyConfig(
    shares=1000,
    options_cash=200_000,
    delta_cap=2000,  # Allow 2x delta cap
)

CONSERVATIVE_CONFIG = StrategyConfig(
    shares=1000,
    options_cash=50_000,
    delta_cap=500,  # Only 50% delta cap
)
