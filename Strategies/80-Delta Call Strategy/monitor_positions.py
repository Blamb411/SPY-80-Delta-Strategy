"""
SPY 80-Delta Call Strategy - Position Monitor
==============================================
Connects to IBKR TWS to get real-time quotes and calculates P&L
for open positions.

Usage:
    python monitor_positions.py
    python monitor_positions.py --no-ibkr  (use estimates only)
"""

import argparse
import math
from datetime import datetime, date, timedelta
from dataclasses import dataclass
from typing import List, Optional

# Try to import ib_insync
try:
    from ib_insync import IB, Stock, Option
    HAS_IBKR = True
except ImportError:
    HAS_IBKR = False
    print("Note: ib_insync not installed. Using estimates only.")


# ============================================================================
# CONFIGURATION
# ============================================================================

IB_HOST = "127.0.0.1"
IB_PORT = 7497  # 7497 for TWS paper, 7496 for TWS live
IB_CLIENT_ID = 97

PROFIT_TARGET_PCT = 0.50  # +50%
MAX_HOLD_DAYS = 60  # trading days


# ============================================================================
# POSITION DATA
# ============================================================================

@dataclass
class Position:
    """Represents an open position."""
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
    def total_cost(self) -> float:
        return self.entry_price * 100 * self.quantity

    @property
    def profit_target_price(self) -> float:
        return self.entry_price * (1 + PROFIT_TARGET_PCT)

    @property
    def profit_target_value(self) -> float:
        return self.profit_target_price * 100 * self.quantity

    @property
    def days_held(self) -> int:
        entry = datetime.strptime(self.entry_date, "%Y-%m-%d").date()
        today = date.today()
        # Count trading days (approximate: exclude weekends)
        days = 0
        current = entry
        while current < today:
            current += timedelta(days=1)
            if current.weekday() < 5:  # Monday = 0, Friday = 4
                days += 1
        return days

    @property
    def days_remaining(self) -> int:
        return max(0, MAX_HOLD_DAYS - self.days_held)

    @property
    def dte(self) -> int:
        exp = datetime.strptime(self.expiration, "%Y-%m-%d").date()
        return (exp - date.today()).days


# Define open positions here
OPEN_POSITIONS = [
    Position(
        account="IRA",
        entry_date="2026-02-03",
        symbol="SPY",
        strike=660,
        expiration="2026-06-18",
        right="C",
        quantity=10,
        entry_price=51.60,
        notes="First trade - 73 delta at entry"
    ),
    Position(
        account="IRA",
        entry_date="2026-02-04",
        symbol="SPY",
        strike=650,
        expiration="2026-05-29",
        right="C",
        quantity=10,
        entry_price=55.41,
        notes="Second trade - down day entry"
    ),
    Position(
        account="IRA",
        entry_date="2026-02-06",
        symbol="SPY",
        strike=655,
        expiration="2026-05-15",
        right="C",
        quantity=10,
        entry_price=49.70,
        notes="Third trade - 76 delta, missed down day entry"
    ),
]


# ============================================================================
# IBKR CONNECTION
# ============================================================================

def connect_ibkr() -> Optional[IB]:
    """Connect to TWS/Gateway."""
    if not HAS_IBKR:
        return None

    ib = IB()
    try:
        ib.connect(IB_HOST, IB_PORT, clientId=IB_CLIENT_ID, timeout=10)
        return ib
    except Exception as e:
        print(f"Could not connect to IBKR: {e}")
        return None


def get_spy_price(ib: IB) -> Optional[float]:
    """Get current SPY price."""
    spy = Stock("SPY", "SMART", "USD")
    ib.qualifyContracts(spy)
    ticker = ib.reqMktData(spy, '', False, False)
    ib.sleep(2)
    price = ticker.marketPrice()
    ib.cancelMktData(spy)
    return price if price > 0 else None


def get_option_price(ib: IB, symbol: str, expiration: str, strike: float,
                     right: str) -> dict:
    """Get current option quote."""
    exp_str = expiration.replace("-", "")
    opt = Option(symbol, exp_str, strike, right, "SMART")

    try:
        ib.qualifyContracts(opt)
    except Exception as e:
        return {"error": str(e)}

    ticker = ib.reqMktData(opt, '', False, False)
    ib.sleep(2)

    result = {
        "bid": ticker.bid if ticker.bid > 0 else None,
        "ask": ticker.ask if ticker.ask > 0 else None,
        "last": ticker.last if ticker.last > 0 else None,
        "volume": ticker.volume if ticker.volume >= 0 else None,
    }

    if result["bid"] and result["ask"]:
        result["mid"] = (result["bid"] + result["ask"]) / 2
    elif result["last"]:
        result["mid"] = result["last"]
    else:
        result["mid"] = None

    ib.cancelMktData(opt)
    return result


def estimate_option_price(spot: float, strike: float, dte: int,
                          entry_price: float, iv: float = 0.18,
                          rate: float = 0.045) -> float:
    """
    Estimate current option price using Black-Scholes.
    This is used when IBKR is not available.

    Args:
        spot: Current underlying price
        strike: Option strike price
        dte: Days to expiration
        entry_price: Original entry price (used as sanity check)
        iv: Implied volatility estimate (default 18% for SPY)
        rate: Risk-free rate

    Returns:
        Estimated option price
    """
    # Handle expired or near-expiry options
    if dte <= 0:
        return max(0, spot - strike)

    t = dte / 365.0
    sqrt_t = math.sqrt(t)

    # Black-Scholes for call
    d1 = (math.log(spot / strike) + (rate + 0.5 * iv * iv) * t) / (iv * sqrt_t)
    d2 = d1 - iv * sqrt_t

    # Standard normal CDF approximation
    def norm_cdf(x):
        return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

    call_price = spot * norm_cdf(d1) - strike * math.exp(-rate * t) * norm_cdf(d2)

    return max(0, call_price)


# ============================================================================
# DISPLAY
# ============================================================================

def calculate_delta(spot: float, strike: float, dte: int, iv: float = 0.16,
                    rate: float = 0.045) -> float:
    """Calculate option delta using Black-Scholes."""
    if dte <= 0:
        return 1.0 if spot > strike else 0.0

    t = dte / 365.0
    d1 = (math.log(spot / strike) + (rate + 0.5 * iv * iv) * t) / (iv * math.sqrt(t))

    # Normal CDF
    return 0.5 * (1.0 + math.erf(d1 / math.sqrt(2.0)))


def print_position_status(pos: Position, current_price: float, spot: float,
                          source: str = "IBKR"):
    """Print detailed status for a position."""

    # Calculate P&L
    current_value = current_price * 100 * pos.quantity
    pnl_dollar = current_value - pos.total_cost
    pnl_pct = (current_price / pos.entry_price - 1) * 100

    # Distance to targets
    to_profit_target = (pos.profit_target_price - current_price) / current_price * 100

    # Delta
    delta = calculate_delta(spot, pos.strike, pos.dte)
    total_delta = delta * pos.quantity * 100

    # Status indicators
    if pnl_pct >= 50:
        status = "*** PROFIT TARGET HIT - SELL ***"
    elif pos.days_remaining <= 5:
        status = "*** MAX HOLD APPROACHING ***"
    elif pnl_pct >= 30:
        status = "Strong gain"
    elif pnl_pct >= 10:
        status = "Profitable"
    elif pnl_pct >= -10:
        status = "Flat"
    else:
        status = "Underwater"

    # Print
    W = 70
    print("=" * W)
    print(f"POSITION: {pos.symbol} ${pos.strike:.0f} {pos.right} {pos.expiration}")
    print(f"Account: {pos.account}")
    print("=" * W)
    print()

    print(f"  {'Quantity:':<20} {pos.quantity} contracts")
    print(f"  {'Entry Date:':<20} {pos.entry_date}")
    print(f"  {'Entry Price:':<20} ${pos.entry_price:.2f}")
    print(f"  {'Total Cost:':<20} ${pos.total_cost:,.0f}")
    print()

    print(f"  {'Current Price:':<20} ${current_price:.2f}  ({source})")
    print(f"  {'Current Value:':<20} ${current_value:,.0f}")
    print(f"  {'P&L ($):':<20} ${pnl_dollar:+,.0f}")
    print(f"  {'P&L (%):':<20} {pnl_pct:+.1f}%")
    print()

    print(f"  {'SPY Price:':<20} ${spot:.2f}")
    print(f"  {'Current Delta:':<20} {delta:.2f} ({int(delta*100)}-delta)")
    print(f"  {'Total Delta:':<20} {total_delta:.0f} (~{int(total_delta)} shares)")
    print(f"  {'Intrinsic:':<20} ${max(0, spot - pos.strike):.2f}")
    print()

    print(f"  {'Days Held:':<20} {pos.days_held} trading days")
    print(f"  {'Days to Max Hold:':<20} {pos.days_remaining} trading days")
    print(f"  {'Days to Expiry:':<20} {pos.dte} calendar days")
    print()

    print(f"  {'Profit Target:':<20} ${pos.profit_target_price:.2f} (+50%)")
    print(f"  {'Target Value:':<20} ${pos.profit_target_value:,.0f}")
    print(f"  {'To Target:':<20} {to_profit_target:+.1f}% (${pos.profit_target_price - current_price:+.2f})")
    print()

    print(f"  STATUS: {status}")
    print()


def print_summary(positions: List[Position], prices: dict, spot: float):
    """Print portfolio summary."""

    total_cost = sum(p.total_cost for p in positions)
    total_value = sum(prices[i] * 100 * p.quantity
                      for i, p in enumerate(positions) if i in prices)
    total_pnl = total_value - total_cost
    total_pnl_pct = (total_value / total_cost - 1) * 100 if total_cost > 0 else 0

    total_delta = sum(
        calculate_delta(spot, p.strike, p.dte) * p.quantity * 100
        for p in positions
    )

    W = 70
    print("=" * W)
    print("PORTFOLIO SUMMARY")
    print("=" * W)
    print()
    print(f"  {'Total Positions:':<25} {len(positions)}")
    print(f"  {'Total Contracts:':<25} {sum(p.quantity for p in positions)}")
    print(f"  {'Total Cost Basis:':<25} ${total_cost:,.0f}")
    print(f"  {'Total Current Value:':<25} ${total_value:,.0f}")
    print(f"  {'Total P&L ($):':<25} ${total_pnl:+,.0f}")
    print(f"  {'Total P&L (%):':<25} {total_pnl_pct:+.1f}%")
    print(f"  {'Total Delta Exposure:':<25} {total_delta:.0f} (~ {int(total_delta)} shares)")
    print()

    # By account
    accounts = set(p.account for p in positions)
    if len(accounts) > 1:
        print("  By Account:")
        for acct in sorted(accounts):
            acct_positions = [p for p in positions if p.account == acct]
            acct_cost = sum(p.total_cost for p in acct_positions)
            acct_value = sum(
                prices[i] * 100 * p.quantity
                for i, p in enumerate(positions)
                if p.account == acct and i in prices
            )
            acct_pnl = acct_value - acct_cost
            print(f"    {acct}: ${acct_value:,.0f} (P&L: ${acct_pnl:+,.0f})")
        print()


def main():
    parser = argparse.ArgumentParser(description="Monitor SPY Call Positions")
    parser.add_argument("--no-ibkr", action="store_true",
                        help="Skip IBKR connection, use estimates")
    args = parser.parse_args()

    print()
    print("=" * 70)
    print("SPY 80-DELTA CALL STRATEGY - POSITION MONITOR")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    print()

    if not OPEN_POSITIONS:
        print("No open positions.")
        return

    # Connect to IBKR
    ib = None
    spot = None
    use_ibkr = not args.no_ibkr and HAS_IBKR

    if use_ibkr:
        print("Connecting to IBKR...")
        ib = connect_ibkr()
        if ib:
            spot = get_spy_price(ib)
            print(f"SPY Price: ${spot:.2f}" if spot else "Could not get SPY price")
            print()

    if not spot:
        # Try ThetaData as fallback
        try:
            import sys
            import os
            _this_dir = os.path.dirname(os.path.abspath(__file__))
            _project_dir = os.path.dirname(os.path.dirname(_this_dir))
            sys.path.insert(0, _project_dir)
            from backtest.thetadata_client import ThetaDataClient
            from datetime import timedelta

            client = ThetaDataClient()
            if client.connect():
                end_date = date.today().strftime("%Y-%m-%d")
                start_date = (date.today() - timedelta(days=10)).strftime("%Y-%m-%d")
                spy_bars = client.fetch_spy_bars(start_date, end_date)
                if spy_bars:
                    spot = spy_bars[-1]["close"]
                    print(f"SPY Price (ThetaData): ${spot:.2f}")
                client.close()
        except Exception as e:
            pass

        if not spot:
            spot = 680.0  # Conservative fallback
            print(f"Using fallback SPY price: ${spot:.2f}")
        print()

    # Get prices for each position
    prices = {}
    for i, pos in enumerate(OPEN_POSITIONS):
        if ib:
            quote = get_option_price(ib, pos.symbol, pos.expiration,
                                     pos.strike, pos.right)
            if "error" not in quote and quote.get("mid"):
                prices[i] = quote["mid"]
                source = "IBKR"
            else:
                prices[i] = estimate_option_price(spot, pos.strike, pos.dte,
                                                   pos.entry_price)
                source = "Estimate"
        else:
            prices[i] = estimate_option_price(spot, pos.strike, pos.dte,
                                               pos.entry_price)
            source = "Estimate"

        print_position_status(pos, prices[i], spot, source)

    # Summary
    if len(OPEN_POSITIONS) >= 1:
        print_summary(OPEN_POSITIONS, prices, spot)

    # Disconnect
    if ib:
        ib.disconnect()
        print("Disconnected from IBKR")

    print()


if __name__ == "__main__":
    main()
