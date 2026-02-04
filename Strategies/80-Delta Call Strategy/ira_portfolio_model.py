"""
IRA Portfolio Model
====================
Models the current IRA portfolio: 3,125 SPY shares + call options.

Also backtests a "delta-capped" strategy where options are limited
to the equivalent delta of the underlying shares held.

Usage:
    python ira_portfolio_model.py
"""

import os
import sys
import math
from datetime import datetime, date, timedelta
from dataclasses import dataclass
from typing import Optional

_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_dir = os.path.dirname(os.path.dirname(_this_dir))
sys.path.insert(0, _project_dir)

# Try IBKR connection for live prices
try:
    from ib_insync import IB, Stock, Option
    HAS_IBKR = True
except ImportError:
    HAS_IBKR = False


# ============================================================================
# CURRENT HOLDINGS
# ============================================================================

@dataclass
class ShareHolding:
    account: str
    symbol: str
    shares: int
    cost_basis_per_share: Optional[float] = None  # If known


@dataclass
class OptionHolding:
    account: str
    symbol: str
    strike: float
    expiration: str
    right: str  # 'C' or 'P'
    quantity: int
    entry_price: float
    entry_date: str


# Current IRA Holdings
IRA_SHARES = ShareHolding(
    account="IRA",
    symbol="SPY",
    shares=3125,
    cost_basis_per_share=None,  # Long-term holding, cost basis not critical for IRA
)

IRA_OPTIONS = [
    OptionHolding(
        account="IRA",
        symbol="SPY",
        strike=660,
        expiration="2026-06-18",
        right="C",
        quantity=10,
        entry_price=51.60,
        entry_date="2026-02-03",
    ),
]


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def calculate_delta(spot: float, strike: float, dte: int, iv: float = 0.16,
                    rate: float = 0.045, right: str = "C") -> float:
    """Calculate option delta using Black-Scholes."""
    if dte <= 0:
        if right == "C":
            return 1.0 if spot > strike else 0.0
        else:
            return -1.0 if spot < strike else 0.0

    t = dte / 365.0
    d1 = (math.log(spot / strike) + (rate + 0.5 * iv * iv) * t) / (iv * math.sqrt(t))

    # Normal CDF
    delta = 0.5 * (1.0 + math.erf(d1 / math.sqrt(2.0)))

    if right == "P":
        delta = delta - 1.0

    return delta


def connect_ibkr() -> Optional[IB]:
    if not HAS_IBKR:
        return None
    ib = IB()
    try:
        ib.connect("127.0.0.1", 7497, clientId=95, timeout=10)
        return ib
    except:
        return None


def get_spy_price(ib: IB) -> Optional[float]:
    spy = Stock("SPY", "SMART", "USD")
    ib.qualifyContracts(spy)
    ticker = ib.reqMktData(spy, '', False, False)
    ib.sleep(2)
    price = ticker.marketPrice()
    ib.cancelMktData(spy)
    return price if price > 0 else None


def get_option_price(ib: IB, symbol: str, expiration: str, strike: float,
                     right: str) -> Optional[float]:
    exp_str = expiration.replace("-", "")
    opt = Option(symbol, exp_str, strike, right, "SMART")
    try:
        ib.qualifyContracts(opt)
    except:
        return None

    ticker = ib.reqMktData(opt, '', False, False)
    ib.sleep(2)

    mid = None
    if ticker.bid > 0 and ticker.ask > 0:
        mid = (ticker.bid + ticker.ask) / 2
    elif ticker.last > 0:
        mid = ticker.last

    ib.cancelMktData(opt)
    return mid


# ============================================================================
# PORTFOLIO ANALYSIS
# ============================================================================

def analyze_current_portfolio(spot: float, option_prices: dict):
    """Analyze current IRA portfolio."""

    print("=" * 80)
    print("CURRENT IRA PORTFOLIO ANALYSIS")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"SPY Price: ${spot:.2f}")
    print("=" * 80)
    print()

    # Shares
    shares = IRA_SHARES.shares
    shares_value = shares * spot
    shares_delta = shares  # Delta of shares = 1 per share

    print("SHARE HOLDINGS")
    print("-" * 40)
    print(f"  SPY Shares:        {shares:,}")
    print(f"  Current Value:     ${shares_value:,.0f}")
    print(f"  Delta:             {shares_delta:,}")
    print()

    # Options
    print("OPTION HOLDINGS")
    print("-" * 40)

    total_options_value = 0
    total_options_delta = 0
    total_options_cost = 0

    for opt in IRA_OPTIONS:
        exp_date = datetime.strptime(opt.expiration, "%Y-%m-%d").date()
        dte = (exp_date - date.today()).days

        # Get current price
        current_price = option_prices.get(
            f"{opt.symbol}_{opt.strike}_{opt.expiration}",
            opt.entry_price  # Fallback to entry price
        )

        # Calculate delta
        delta_per = calculate_delta(spot, opt.strike, dte, right=opt.right)
        total_delta = delta_per * opt.quantity * 100

        # Values
        current_value = current_price * 100 * opt.quantity
        cost_basis = opt.entry_price * 100 * opt.quantity
        pnl = current_value - cost_basis
        pnl_pct = (current_price / opt.entry_price - 1) * 100

        total_options_value += current_value
        total_options_delta += total_delta
        total_options_cost += cost_basis

        print(f"  {opt.symbol} ${opt.strike}{opt.right} {opt.expiration}")
        print(f"    Quantity:        {opt.quantity} contracts")
        print(f"    Entry Price:     ${opt.entry_price:.2f}")
        print(f"    Current Price:   ${current_price:.2f}")
        print(f"    Current Value:   ${current_value:,.0f}")
        print(f"    P&L:             ${pnl:+,.0f} ({pnl_pct:+.1f}%)")
        print(f"    Delta/contract:  {delta_per:.2f}")
        print(f"    Total Delta:     {total_delta:,.0f}")
        print(f"    DTE:             {dte} days")
        print()

    # Portfolio Summary
    total_value = shares_value + total_options_value
    total_delta = shares_delta + total_options_delta

    # Equivalent share exposure
    equivalent_shares = total_delta
    leverage = equivalent_shares / shares

    print("=" * 80)
    print("PORTFOLIO SUMMARY")
    print("=" * 80)
    print()
    print(f"  {'Component':<25} {'Value':>15} {'Delta':>12}")
    print(f"  {'-' * 52}")
    print(f"  {'SPY Shares':<25} ${shares_value:>14,.0f} {shares_delta:>11,}")
    print(f"  {'Call Options':<25} ${total_options_value:>14,.0f} {total_options_delta:>11,.0f}")
    print(f"  {'-' * 52}")
    print(f"  {'TOTAL':<25} ${total_value:>14,.0f} {total_delta:>11,.0f}")
    print()
    print(f"  Equivalent Share Exposure: {equivalent_shares:,.0f} shares")
    print(f"  Effective Leverage:        {leverage:.2f}x")
    print()

    # Risk metrics
    print("RISK ANALYSIS")
    print("-" * 40)

    # What happens if SPY moves
    scenarios = [
        ("SPY +5%", spot * 1.05),
        ("SPY +10%", spot * 1.10),
        ("SPY -5%", spot * 0.95),
        ("SPY -10%", spot * 0.90),
        ("SPY -20%", spot * 0.80),
    ]

    print(f"  {'Scenario':<15} {'Portfolio Value':>18} {'Change':>12}")
    print(f"  {'-' * 45}")

    for scenario_name, new_spot in scenarios:
        # Shares value
        new_shares_value = shares * new_spot

        # Options value (simplified - assume delta stays constant for small moves)
        # More accurate: recalculate option price
        spot_change = new_spot - spot
        new_options_value = total_options_value + (total_options_delta * spot_change)
        new_options_value = max(0, new_options_value)  # Can't go below 0

        new_total = new_shares_value + new_options_value
        change = new_total - total_value
        change_pct = change / total_value * 100

        print(f"  {scenario_name:<15} ${new_total:>17,.0f} {change_pct:>+11.1f}%")

    print()

    # Delta comparison
    print("DELTA ANALYSIS")
    print("-" * 40)
    print(f"  Share Delta:           {shares_delta:>10,} ({shares_delta/total_delta*100:.0f}% of total)")
    print(f"  Options Delta:         {total_options_delta:>10,.0f} ({total_options_delta/total_delta*100:.0f}% of total)")
    print(f"  Total Delta:           {total_delta:>10,.0f}")
    print()
    print(f"  Options as % of Share Delta: {total_options_delta/shares_delta*100:.1f}%")
    print(f"  Max Options Delta (= shares): {shares_delta:,} (current: {total_options_delta:,.0f})")
    print()

    # Room for more options
    room_for_delta = shares_delta - total_options_delta
    contracts_80_delta = room_for_delta / 80  # Assuming 80-delta calls

    if room_for_delta > 0:
        print(f"  Room for additional delta:    {room_for_delta:,.0f}")
        print(f"  Equivalent 80-delta contracts: ~{contracts_80_delta:.0f}")
    else:
        print(f"  Options delta EXCEEDS share delta by: {-room_for_delta:,.0f}")

    print()

    return {
        "shares": shares,
        "shares_value": shares_value,
        "shares_delta": shares_delta,
        "options_value": total_options_value,
        "options_delta": total_options_delta,
        "options_cost": total_options_cost,
        "total_value": total_value,
        "total_delta": total_delta,
        "leverage": leverage,
    }


# ============================================================================
# MAIN
# ============================================================================

def main():
    print()
    print("=" * 80)
    print("IRA PORTFOLIO MODEL")
    print("=" * 80)
    print()

    # Try to get live prices from IBKR
    ib = connect_ibkr()
    spot = None
    option_prices = {}

    if ib:
        print("Connected to IBKR - fetching live prices...")
        spot = get_spy_price(ib)

        if spot:
            print(f"SPY: ${spot:.2f}")

            # Get option prices
            for opt in IRA_OPTIONS:
                price = get_option_price(ib, opt.symbol, opt.expiration,
                                        opt.strike, opt.right)
                if price:
                    key = f"{opt.symbol}_{opt.strike}_{opt.expiration}"
                    option_prices[key] = price
                    print(f"{opt.symbol} ${opt.strike}C: ${price:.2f}")

        ib.disconnect()
        print()

    if not spot:
        # Fallback
        spot = 688.50  # Approximate current price
        print(f"Using estimated SPY price: ${spot:.2f}")
        print()

    # Analyze portfolio
    results = analyze_current_portfolio(spot, option_prices)

    print("=" * 80)
    print("END OF ANALYSIS")
    print("=" * 80)


if __name__ == "__main__":
    main()
