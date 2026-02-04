# region imports
from AlgorithmImports import *
from datetime import timedelta
# endregion

"""
Simple Put Credit Spread Test
==============================
A minimal version to verify QuantConnect setup works.
Trades SPY only with relaxed filters.

Use this to:
1. Verify your QuantConnect account can run options backtests
2. Confirm data access is working
3. See sample output before running the full strategy
"""


class SimplePutSpreadTest(QCAlgorithm):

    def initialize(self):
        # Short test period
        self.set_start_date(2023, 1, 1)
        self.set_end_date(2023, 6, 30)
        self.set_cash(50000)

        # Just SPY for simplicity
        self.equity = self.add_equity("SPY", Resolution.DAILY)
        self.option = self.add_option("SPY", Resolution.MINUTE)
        self.option.set_filter(self._filter)

        # Simple SMA filter
        self.sma = self.sma("SPY", 50, Resolution.DAILY)

        # Track positions
        self.spread_count = 0
        self.last_trade_date = None

        self.debug("Simple Put Spread Test initialized")

    def _filter(self, universe):
        """Filter to relevant put options."""
        return (universe
                .include_weeklys()
                .puts_only()
                .strikes(-15, -1)
                .expiration(timedelta(days=20), timedelta(days=40)))

    def on_data(self, data):
        """Look for entry opportunities."""

        # Only trade once per week
        if self.last_trade_date and (self.time - self.last_trade_date).days < 7:
            return

        # Wait for SMA to warm up
        if not self.sma.is_ready:
            return

        # Simple filter: price above 50 SMA
        price = self.securities["SPY"].price
        if price <= self.sma.current.value:
            return

        # Get option chain
        chain = data.option_chains.get(self.option.symbol)
        if not chain:
            return

        # Get puts at target expiration
        puts = sorted([c for c in chain if c.right == OptionRight.PUT],
                     key=lambda x: x.strike, reverse=True)

        if len(puts) < 5:
            return

        # Find ~5% OTM put for short leg
        spot = price
        target_short = spot * 0.95

        short_put = min(puts, key=lambda c: abs(c.strike - target_short))

        # Find long put ~3% below short
        target_long = short_put.strike * 0.97
        long_candidates = [c for c in puts if c.strike < short_put.strike]

        if not long_candidates:
            return

        long_put = min(long_candidates, key=lambda c: abs(c.strike - target_long))

        # Check for valid prices
        if short_put.bid_price <= 0 or long_put.ask_price <= 0:
            return

        credit = short_put.bid_price - long_put.ask_price

        if credit < 0.20:  # Minimum credit
            return

        # Place the spread
        self.debug(f"ENTRY: SPY {short_put.strike}/{long_put.strike} "
                  f"exp {short_put.expiry.date()} credit ${credit:.2f}")

        # Sell short put
        self.market_order(short_put.symbol, -1)
        # Buy long put
        self.market_order(long_put.symbol, 1)

        self.spread_count += 1
        self.last_trade_date = self.time

    def on_end_of_algorithm(self):
        self.debug("=" * 50)
        self.debug(f"Test Complete: {self.spread_count} spreads entered")
        self.debug(f"Final Portfolio Value: ${self.portfolio.total_portfolio_value:,.2f}")
        self.debug("=" * 50)
