# region imports
from AlgorithmImports import *
from datetime import timedelta
# endregion

"""
Debug Version - Put Credit Spread Test
=======================================
Added extensive logging to diagnose why no trades are being entered.
"""


class SimplePutSpreadDebug(QCAlgorithm):

    def initialize(self):
        # Longer test period to capture more opportunities
        self.set_start_date(2023, 1, 1)
        self.set_end_date(2023, 12, 31)  # Full year
        self.set_cash(50000)

        # IMPORTANT: Warm up indicators so they're ready from day 1
        self.set_warm_up(timedelta(days=60))  # 60 calendar days ~ 50 trading days

        # Add SPY equity
        self.spy = self.add_equity("SPY", Resolution.DAILY)
        self.spy.set_data_normalization_mode(DataNormalizationMode.RAW)

        # Add SPY options
        option = self.add_option("SPY", Resolution.MINUTE)
        self.option_symbol = option.symbol

        # Broader filter - include more contracts
        option.set_filter(lambda u: u
            .include_weeklys()
            .strikes(-30, 30)
            .expiration(timedelta(days=7), timedelta(days=60)))

        # Simple SMA filter
        self.sma = self.sma("SPY", 50, Resolution.DAILY)

        # Track
        self.spread_count = 0
        self.last_trade_date = self.start_date - timedelta(days=10)
        self.check_count = 0

        self.debug("=" * 60)
        self.debug("DEBUG: Simple Put Spread Test initialized")
        self.debug("=" * 60)

    def on_data(self, data):
        """Look for entry opportunities with detailed logging."""

        # Skip during warmup period
        if self.is_warming_up:
            return

        self.check_count += 1

        # Only check once per day (at 10am)
        if self.time.hour != 10 or self.time.minute != 0:
            return

        # Only log weekly to reduce noise
        if self.time.weekday() == 0:  # Monday
            self.debug(f"--- Week of {self.time.date()} ---")

        # Check trade spacing
        days_since = (self.time - self.last_trade_date).days
        if days_since < 7:
            return

        # Check SMA
        if not self.sma.is_ready:
            return

        price = self.securities["SPY"].price
        sma_val = self.sma.current.value

        if price <= sma_val:
            return

        # Passed filters - log this
        self.debug(f"{self.time.date()} SPY ${price:.2f} > SMA ${sma_val:.2f} - checking options...")

        # Check for option chain
        if not data.option_chains:
            return

        chain = data.option_chains.get(self.option_symbol)
        if chain is None:
            return

        contracts = list(chain)
        if len(contracts) == 0:
            return

        # Filter to puts only
        puts = [c for c in contracts if c.right == OptionRight.PUT]
        if len(puts) < 2:
            return

        # Find target expiration (~30 days out)
        target_expiry = self.time + timedelta(days=30)
        expirations = sorted(set(c.expiry for c in puts))
        if not expirations:
            return

        best_expiry = min(expirations, key=lambda x: abs((x - target_expiry).days))

        # Filter to selected expiration
        puts_at_exp = [c for c in puts if c.expiry == best_expiry]
        if len(puts_at_exp) < 2:
            return

        # Find short put (~5% OTM)
        target_short = price * 0.95
        short_put = min(puts_at_exp, key=lambda c: abs(c.strike - target_short))

        # Find long put (~3% below short)
        target_long = short_put.strike * 0.97
        long_candidates = [c for c in puts_at_exp if c.strike < short_put.strike]
        if not long_candidates:
            return

        long_put = min(long_candidates, key=lambda c: abs(c.strike - target_long))

        # Check prices
        if short_put.bid_price <= 0 or long_put.ask_price <= 0:
            return

        credit = short_put.bid_price - long_put.ask_price
        if credit < 0.10:
            return

        # PLACE THE TRADE
        self.debug(f"ENTRY {self.time.date()}: {short_put.strike}/{long_put.strike} "
                   f"exp {best_expiry.date()} credit ${credit:.2f}")

        # Sell short put, Buy long put
        self.market_order(short_put.symbol, -1)
        self.market_order(long_put.symbol, 1)

        self.spread_count += 1
        self.last_trade_date = self.time

    def on_end_of_algorithm(self):
        self.debug("=" * 60)
        self.debug("FINAL RESULTS")
        self.debug("=" * 60)
        self.debug(f"Total checks: {self.check_count}")
        self.debug(f"Spreads entered: {self.spread_count}")
        self.debug(f"Final Portfolio Value: ${self.portfolio.total_portfolio_value:,.2f}")
        self.debug("=" * 60)
