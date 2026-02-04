# region imports
from AlgorithmImports import *
from datetime import timedelta
from collections import deque
# endregion

"""
Put Credit Spread Strategy for QuantConnect
============================================
Ported from put_spread_backtest.py

Entry Criteria:
- Price > 200-day SMA (uptrend filter)
- RSI(14) < 75 (not overbought)
- IV Rank > 30% (elevated premium)

Spread Construction:
- Short put at ~25 delta
- Long put ~5% below short strike
- Target DTE: 30 days

Exit Rules:
- Take profit at 50% of max profit
- Stop loss at 200% of credit received (i.e., when loss = 2x credit)
- Hold to expiration if neither triggered

Entry Interval:
- Minimum 5 days between new entries per symbol
"""


class PutCreditSpreadStrategy(QCAlgorithm):
    """
    Put Credit Spread (Bull Put Spread) Strategy

    Uses real historical bid/ask data from QuantConnect's options dataset.
    """

    # ==========================================================================
    # CONFIGURATION - Match your backtest parameters
    # ==========================================================================

    # Entry filters
    SMA_PERIOD = 200
    RSI_PERIOD = 14
    RSI_MAX = 75.0
    IV_RANK_MIN = 0.30  # 30%
    IV_RANK_LOOKBACK = 252  # 1 year of trading days

    # Spread construction
    TARGET_DELTA = 0.25  # 25-delta short put
    SPREAD_WIDTH_PCT = 0.05  # Long strike 5% below short
    TARGET_DTE_MIN = 25
    TARGET_DTE_MAX = 35

    # Position sizing
    CONTRACTS_PER_TRADE = 1
    MAX_POSITIONS_PER_SYMBOL = 2  # Allow overlapping trades

    # Exit rules
    TAKE_PROFIT_PCT = 0.50  # Close at 50% of max profit
    STOP_LOSS_MULTIPLIER = 2.0  # Close if loss exceeds 2x credit received

    # Entry interval
    MIN_DAYS_BETWEEN_ENTRIES = 5

    # Symbols to trade (start with liquid names for testing)
    SYMBOLS = ["SPY", "QQQ", "IWM", "AAPL", "MSFT"]

    # ==========================================================================
    # INITIALIZATION
    # ==========================================================================

    def initialize(self):
        """Initialize the algorithm."""

        # Backtest period - adjust as needed
        self.set_start_date(2020, 1, 1)
        self.set_end_date(2024, 12, 31)
        self.set_cash(100000)

        # Set brokerage model for realistic fills
        self.set_brokerage_model(BrokerageName.INTERACTIVE_BROKERS_BROKERAGE, AccountType.MARGIN)

        # Track active positions and indicators per symbol
        self.symbol_data = {}

        # Add equities and options
        for ticker in self.SYMBOLS:
            equity = self.add_equity(ticker, Resolution.DAILY)
            equity.set_data_normalization_mode(DataNormalizationMode.RAW)

            # Add options with filter
            option = self.add_option(ticker, Resolution.MINUTE)
            option.set_filter(self._options_filter)

            # Store symbol data
            self.symbol_data[ticker] = SymbolData(
                self,
                equity.symbol,
                option.symbol,
                self.SMA_PERIOD,
                self.RSI_PERIOD,
                self.IV_RANK_LOOKBACK
            )

        # Schedule daily check (after market open to ensure data is available)
        self.schedule.on(
            self.date_rules.every_day(),
            self.time_rules.after_market_open(list(self.symbol_data.values())[0].equity, 30),
            self._check_for_entries
        )

        # Track active strategies for P&L monitoring
        self.active_strategies = []  # List of ActiveSpread objects

        self.debug("Put Credit Spread Strategy initialized")
        self.debug(f"Trading symbols: {self.SYMBOLS}")

    def _options_filter(self, universe: OptionFilterUniverse) -> OptionFilterUniverse:
        """Filter option contracts to relevant strikes and expirations."""
        return (universe
                .include_weeklys()
                .strikes(-20, 0)  # OTM puts only (below current price)
                .expiration(timedelta(days=self.TARGET_DTE_MIN),
                           timedelta(days=self.TARGET_DTE_MAX + 10)))

    # ==========================================================================
    # ENTRY LOGIC
    # ==========================================================================

    def _check_for_entries(self):
        """Daily check for new entry opportunities."""

        for ticker, data in self.symbol_data.items():
            # Skip if not ready (need enough history for indicators)
            if not data.is_ready():
                continue

            # Skip if too soon since last entry
            if not data.can_enter(self.time, self.MIN_DAYS_BETWEEN_ENTRIES):
                continue

            # Check entry filters
            if not self._passes_entry_filters(data):
                continue

            # Check position limits
            active_count = sum(1 for s in self.active_strategies
                              if s.ticker == ticker and not s.is_closed)
            if active_count >= self.MAX_POSITIONS_PER_SYMBOL:
                continue

            # Try to enter a spread
            self._enter_put_spread(data)

    def _passes_entry_filters(self, data: 'SymbolData') -> bool:
        """Check if entry filters pass."""

        price = self.securities[data.equity].price

        # Filter 1: Price > 200 SMA (uptrend)
        if price <= data.sma.current.value:
            return False

        # Filter 2: RSI < 75 (not overbought)
        if data.rsi.current.value >= self.RSI_MAX:
            return False

        # Filter 3: IV Rank > 30%
        iv_rank = data.get_iv_rank()
        if iv_rank is None or iv_rank < self.IV_RANK_MIN:
            return False

        return True

    def _enter_put_spread(self, data: 'SymbolData'):
        """Enter a put credit spread."""

        ticker = data.ticker
        chain = self.current_slice.option_chains.get(data.option)

        if chain is None:
            return

        # Get current price
        spot = self.securities[data.equity].price

        # Filter to puts only
        puts = [c for c in chain if c.right == OptionRight.PUT]

        if not puts:
            return

        # Find expiration closest to target DTE
        target_expiry = self.time + timedelta(days=30)
        expirations = sorted(set(c.expiry for c in puts))

        best_expiry = min(expirations,
                         key=lambda x: abs((x - target_expiry).days))

        # Filter to this expiration
        puts_at_expiry = [c for c in puts if c.expiry == best_expiry]

        if not puts_at_expiry:
            return

        # Find short put: closest to 25 delta
        # QuantConnect provides Greeks on contracts
        puts_with_greeks = [c for c in puts_at_expiry
                           if c.greeks is not None and c.greeks.delta is not None]

        if not puts_with_greeks:
            # Fallback: estimate based on strike distance
            # ~25 delta is roughly 5-8% OTM
            target_strike = spot * 0.94
            short_put = min(puts_at_expiry,
                           key=lambda c: abs(c.strike - target_strike))
        else:
            # Use actual delta
            short_put = min(puts_with_greeks,
                           key=lambda c: abs(abs(c.greeks.delta) - self.TARGET_DELTA))

        # Find long put: ~5% below short strike
        long_strike_target = short_put.strike * (1 - self.SPREAD_WIDTH_PCT)

        long_candidates = [c for c in puts_at_expiry
                          if c.strike < short_put.strike]

        if not long_candidates:
            return

        long_put = min(long_candidates,
                      key=lambda c: abs(c.strike - long_strike_target))

        # Ensure we have valid bid/ask
        if (short_put.bid_price <= 0 or long_put.ask_price <= 0):
            return

        # Calculate credit (sell short, buy long)
        # Use bid for selling, ask for buying (conservative)
        credit_per_share = short_put.bid_price - long_put.ask_price

        if credit_per_share <= 0.10:  # Minimum $0.10 credit
            return

        # Calculate max loss
        width = short_put.strike - long_put.strike
        max_loss_per_share = width - credit_per_share

        # Credit/width ratio check (minimum 10% of width)
        if credit_per_share / width < 0.10:
            return

        # Place the order using combo order
        legs = [
            Leg.create(short_put.symbol, -self.CONTRACTS_PER_TRADE),  # Sell short put
            Leg.create(long_put.symbol, self.CONTRACTS_PER_TRADE),    # Buy long put
        ]

        # Use limit order at the net credit
        ticket = self.combo_limit_order(legs, self.CONTRACTS_PER_TRADE, credit_per_share)

        if ticket is not None and ticket.status != OrderStatus.INVALID:
            # Track the position
            spread = ActiveSpread(
                ticker=ticker,
                entry_time=self.time,
                expiry=best_expiry,
                short_strike=short_put.strike,
                long_strike=long_put.strike,
                short_symbol=short_put.symbol,
                long_symbol=long_put.symbol,
                credit_per_share=credit_per_share,
                contracts=self.CONTRACTS_PER_TRADE,
                max_profit=credit_per_share * 100 * self.CONTRACTS_PER_TRADE,
                max_loss=max_loss_per_share * 100 * self.CONTRACTS_PER_TRADE,
            )
            self.active_strategies.append(spread)
            data.last_entry_time = self.time

            self.debug(f"ENTRY: {ticker} Put Spread {short_put.strike}/{long_put.strike} "
                      f"exp {best_expiry.date()} credit ${credit_per_share:.2f}")

    # ==========================================================================
    # EXIT LOGIC
    # ==========================================================================

    def on_data(self, data: Slice):
        """Monitor positions for exit conditions."""

        for spread in self.active_strategies:
            if spread.is_closed:
                continue

            # Check if expired
            if self.time.date() >= spread.expiry.date():
                self._close_spread(spread, "EXPIRY")
                continue

            # Get current prices
            short_contract = self.securities.get(spread.short_symbol)
            long_contract = self.securities.get(spread.long_symbol)

            if short_contract is None or long_contract is None:
                continue

            # Calculate current spread value (cost to close)
            # To close: buy back short (ask), sell long (bid)
            close_cost = short_contract.ask_price - long_contract.bid_price

            if close_cost <= 0:
                continue  # Invalid prices

            # Current P&L per share
            current_pnl_per_share = spread.credit_per_share - close_cost
            current_pnl = current_pnl_per_share * 100 * spread.contracts

            # Take profit check
            profit_target = spread.max_profit * self.TAKE_PROFIT_PCT
            if current_pnl >= profit_target:
                self._close_spread(spread, "TAKE_PROFIT")
                continue

            # Stop loss check
            loss_limit = spread.credit_per_share * self.STOP_LOSS_MULTIPLIER * 100 * spread.contracts
            if current_pnl <= -loss_limit:
                self._close_spread(spread, "STOP_LOSS")
                continue

    def _close_spread(self, spread: 'ActiveSpread', reason: str):
        """Close a spread position."""

        if spread.is_closed:
            return

        # Close both legs
        legs = [
            Leg.create(spread.short_symbol, spread.contracts),   # Buy back short
            Leg.create(spread.long_symbol, -spread.contracts),   # Sell long
        ]

        self.combo_market_order(legs, spread.contracts)

        spread.is_closed = True
        spread.close_time = self.time
        spread.close_reason = reason

        self.debug(f"EXIT ({reason}): {spread.ticker} "
                  f"{spread.short_strike}/{spread.long_strike}")

    # ==========================================================================
    # REPORTING
    # ==========================================================================

    def on_end_of_algorithm(self):
        """Generate summary report at end of backtest."""

        total_trades = len(self.active_strategies)
        closed_trades = [s for s in self.active_strategies if s.is_closed]

        if not closed_trades:
            self.debug("No closed trades to analyze")
            return

        # Calculate stats (would need to track actual fills for accurate P&L)
        take_profits = sum(1 for s in closed_trades if s.close_reason == "TAKE_PROFIT")
        stop_losses = sum(1 for s in closed_trades if s.close_reason == "STOP_LOSS")
        expirations = sum(1 for s in closed_trades if s.close_reason == "EXPIRY")

        self.debug("=" * 60)
        self.debug("PUT CREDIT SPREAD BACKTEST RESULTS")
        self.debug("=" * 60)
        self.debug(f"Total Trades: {total_trades}")
        self.debug(f"Take Profits: {take_profits}")
        self.debug(f"Stop Losses: {stop_losses}")
        self.debug(f"Held to Expiry: {expirations}")
        self.debug(f"Win Rate (TP only): {take_profits/len(closed_trades)*100:.1f}%")
        self.debug("=" * 60)


# ==============================================================================
# HELPER CLASSES
# ==============================================================================

class SymbolData:
    """Track indicators and state for a single symbol."""

    def __init__(self, algorithm, equity_symbol, option_symbol,
                 sma_period, rsi_period, iv_lookback):
        self.algorithm = algorithm
        self.equity = equity_symbol
        self.option = option_symbol
        self.ticker = str(equity_symbol).split()[0]

        # Indicators
        self.sma = algorithm.sma(equity_symbol, sma_period, Resolution.DAILY)
        self.rsi = algorithm.rsi(equity_symbol, rsi_period, MovingAverageType.WILDERS, Resolution.DAILY)

        # IV tracking for IV Rank
        self.iv_history = deque(maxlen=iv_lookback)
        self.last_entry_time = None

    def is_ready(self) -> bool:
        """Check if indicators are warmed up."""
        return self.sma.is_ready and self.rsi.is_ready

    def can_enter(self, current_time, min_days: int) -> bool:
        """Check if enough time has passed since last entry."""
        if self.last_entry_time is None:
            return True
        days_since = (current_time - self.last_entry_time).days
        return days_since >= min_days

    def update_iv(self, iv: float):
        """Update IV history for IV Rank calculation."""
        if iv and iv > 0:
            self.iv_history.append(iv)

    def get_iv_rank(self) -> float:
        """Calculate IV Rank (percentile of current IV vs history)."""
        if len(self.iv_history) < 20:
            return None

        current_iv = self.iv_history[-1] if self.iv_history else None
        if current_iv is None:
            return None

        iv_list = list(self.iv_history)
        iv_min = min(iv_list)
        iv_max = max(iv_list)

        if iv_max == iv_min:
            return 0.5

        return (current_iv - iv_min) / (iv_max - iv_min)


class ActiveSpread:
    """Track an active put credit spread position."""

    def __init__(self, ticker, entry_time, expiry, short_strike, long_strike,
                 short_symbol, long_symbol, credit_per_share, contracts,
                 max_profit, max_loss):
        self.ticker = ticker
        self.entry_time = entry_time
        self.expiry = expiry
        self.short_strike = short_strike
        self.long_strike = long_strike
        self.short_symbol = short_symbol
        self.long_symbol = long_symbol
        self.credit_per_share = credit_per_share
        self.contracts = contracts
        self.max_profit = max_profit
        self.max_loss = max_loss

        # Exit tracking
        self.is_closed = False
        self.close_time = None
        self.close_reason = None
