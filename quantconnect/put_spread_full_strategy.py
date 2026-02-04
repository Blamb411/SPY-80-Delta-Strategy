# region imports
from AlgorithmImports import *
from datetime import timedelta
from collections import deque
# endregion

"""
Put Credit Spread Strategy - Full Version
==========================================
Matches the parameters from put_spread_backtest.py

Entry Criteria:
- Price > 200-day SMA (uptrend filter)
- RSI(14) < 75 (not overbought)
- IV Rank > 30% (elevated premium)

Spread Construction:
- Short put at ~25 delta (or ~5% OTM as fallback)
- Long put 5% below short strike
- Target DTE: 25-35 days

Exit Rules:
- Take profit at 50% of max profit
- Stop loss at 200% of credit (when loss = 2x credit received)
- Otherwise hold to expiration

Position Management:
- Max 3 concurrent positions to manage margin
- 7-day minimum between new entries
- Check buying power before entry
"""


class PutSpreadFullStrategy(QCAlgorithm):

    # ==========================================================================
    # CONFIGURATION - Matching put_spread_backtest.py
    # ==========================================================================

    # Entry filters
    SMA_PERIOD = 200
    RSI_PERIOD = 14
    RSI_MAX = 75.0
    IV_RANK_MIN = 0.30  # 30%
    IV_LOOKBACK = 252   # 1 year for IV rank calculation

    # Spread construction
    TARGET_DELTA = 0.25          # 25-delta short put
    SPREAD_WIDTH_PCT = 0.05      # Long put 5% below short strike
    TARGET_DTE_MIN = 25
    TARGET_DTE_MAX = 40
    MIN_CREDIT = 0.25            # Minimum $0.25 credit per share
    MIN_CREDIT_WIDTH_RATIO = 0.10  # Credit must be >= 10% of width

    # Position management
    MAX_CONCURRENT_POSITIONS = 3
    MIN_DAYS_BETWEEN_ENTRIES = 7
    POSITION_SIZE_PCT = 0.15     # Use 15% of portfolio per position

    # Exit rules
    TAKE_PROFIT_PCT = 0.50       # Close at 50% of max profit
    STOP_LOSS_MULTIPLIER = 2.0   # Close if loss exceeds 2x credit

    # ==========================================================================
    # INITIALIZATION
    # ==========================================================================

    def initialize(self):
        # Backtest period: 2020-2024 for comprehensive testing
        self.set_start_date(2020, 1, 1)
        self.set_end_date(2024, 12, 31)
        self.set_cash(100000)

        # Warm up for 200-day SMA
        self.set_warm_up(timedelta(days=250))

        # Use IB brokerage model for realistic margin
        self.set_brokerage_model(BrokerageName.INTERACTIVE_BROKERS_BROKERAGE, AccountType.MARGIN)

        # Add SPY
        self.spy = self.add_equity("SPY", Resolution.DAILY)
        self.spy.set_data_normalization_mode(DataNormalizationMode.RAW)

        # Add SPY options
        option = self.add_option("SPY", Resolution.MINUTE)
        self.option_symbol = option.symbol
        option.set_filter(lambda u: u
            .include_weeklys()
            .strikes(-30, 5)
            .expiration(timedelta(days=self.TARGET_DTE_MIN),
                       timedelta(days=self.TARGET_DTE_MAX + 10)))

        # Indicators
        self.sma = self.sma("SPY", self.SMA_PERIOD, Resolution.DAILY)
        self.rsi = self.rsi("SPY", self.RSI_PERIOD, MovingAverageType.WILDERS, Resolution.DAILY)

        # IV tracking for IV Rank
        self.iv_history = deque(maxlen=self.IV_LOOKBACK)

        # Position tracking
        self.active_spreads = []  # List of ActiveSpread objects
        self.last_entry_date = self.start_date - timedelta(days=self.MIN_DAYS_BETWEEN_ENTRIES + 1)
        self.trade_log = []       # For analysis

        # Schedule daily position check
        self.schedule.on(
            self.date_rules.every_day("SPY"),
            self.time_rules.after_market_open("SPY", 30),
            self.daily_check
        )

        self.debug("=" * 60)
        self.debug("Put Credit Spread Strategy - Full Version")
        self.debug(f"Period: {self.start_date.date()} to {self.end_date.date()}")
        self.debug(f"Starting Capital: ${self.portfolio.cash:,.0f}")
        self.debug("=" * 60)

    # ==========================================================================
    # DAILY LOGIC
    # ==========================================================================

    def daily_check(self):
        """Daily check for entries and position management."""
        if self.is_warming_up:
            return

        # Update IV history
        self.update_iv_history()

        # Check for new entry
        self.check_entry()

    def on_data(self, data):
        """Monitor positions for exit conditions."""
        if self.is_warming_up:
            return

        self.manage_positions(data)

    # ==========================================================================
    # IV TRACKING
    # ==========================================================================

    def update_iv_history(self):
        """Update IV history from option chain for IV Rank calculation."""
        # Use ATM option IV as proxy for overall IV
        chain = self.current_slice.option_chains.get(self.option_symbol)
        if chain is None:
            return

        spot = self.securities["SPY"].price

        # Find ATM put
        puts = [c for c in chain if c.right == OptionRight.PUT]
        if not puts:
            return

        atm_put = min(puts, key=lambda c: abs(c.strike - spot))

        if atm_put.implied_volatility and atm_put.implied_volatility > 0:
            self.iv_history.append(atm_put.implied_volatility)

    def get_iv_rank(self) -> float:
        """Calculate IV Rank (percentile of current IV vs history)."""
        if len(self.iv_history) < 20:
            return None

        current_iv = self.iv_history[-1]
        iv_list = list(self.iv_history)
        iv_min = min(iv_list)
        iv_max = max(iv_list)

        if iv_max == iv_min:
            return 0.5

        return (current_iv - iv_min) / (iv_max - iv_min)

    # ==========================================================================
    # ENTRY LOGIC
    # ==========================================================================

    def check_entry(self):
        """Check if we should enter a new spread."""

        # Check position limit
        active_count = len([s for s in self.active_spreads if not s.is_closed])
        if active_count >= self.MAX_CONCURRENT_POSITIONS:
            return

        # Check time since last entry
        days_since = (self.time - self.last_entry_date).days
        if days_since < self.MIN_DAYS_BETWEEN_ENTRIES:
            return

        # Check indicators ready
        if not self.sma.is_ready or not self.rsi.is_ready:
            return

        spot = self.securities["SPY"].price
        sma_val = self.sma.current.value
        rsi_val = self.rsi.current.value
        iv_rank = self.get_iv_rank()

        # Filter 1: Price > 200 SMA
        if spot <= sma_val:
            return

        # Filter 2: RSI < 75
        if rsi_val >= self.RSI_MAX:
            return

        # Filter 3: IV Rank > 30%
        if iv_rank is None or iv_rank < self.IV_RANK_MIN:
            return

        # All filters passed - try to enter
        self.debug(f"{self.time.date()} FILTERS PASSED: SPY ${spot:.2f} > SMA ${sma_val:.2f}, "
                   f"RSI {rsi_val:.1f}, IV Rank {iv_rank:.1%}")

        self.enter_spread(spot, iv_rank)

    def enter_spread(self, spot: float, iv_rank: float):
        """Enter a put credit spread."""

        chain = self.current_slice.option_chains.get(self.option_symbol)
        if chain is None:
            return

        # Filter to puts
        puts = [c for c in chain if c.right == OptionRight.PUT]
        if len(puts) < 10:
            return

        # Find target expiration (~30 days)
        target_expiry = self.time + timedelta(days=30)
        expirations = sorted(set(c.expiry for c in puts))

        valid_expirations = [e for e in expirations
                           if self.TARGET_DTE_MIN <= (e - self.time).days <= self.TARGET_DTE_MAX]

        if not valid_expirations:
            return

        best_expiry = min(valid_expirations, key=lambda x: abs((x - target_expiry).days))
        puts_at_exp = [c for c in puts if c.expiry == best_expiry]

        if len(puts_at_exp) < 5:
            return

        # Find short put: target 25-delta or ~5% OTM
        target_short_strike = spot * 0.95  # 5% OTM as fallback

        # Try to use delta if available
        puts_with_greeks = [c for c in puts_at_exp
                          if c.greeks and c.greeks.delta and abs(c.greeks.delta) > 0.01]

        if puts_with_greeks:
            short_put = min(puts_with_greeks,
                           key=lambda c: abs(abs(c.greeks.delta) - self.TARGET_DELTA))
        else:
            short_put = min(puts_at_exp, key=lambda c: abs(c.strike - target_short_strike))

        # Find long put: 5% below short strike
        target_long_strike = short_put.strike * (1 - self.SPREAD_WIDTH_PCT)
        long_candidates = [c for c in puts_at_exp if c.strike < short_put.strike]

        if not long_candidates:
            return

        long_put = min(long_candidates, key=lambda c: abs(c.strike - target_long_strike))

        # Validate spread width (must be meaningful)
        width = short_put.strike - long_put.strike
        if width < 3:  # Minimum $3 width
            return

        # Check prices
        if short_put.bid_price <= 0 or long_put.ask_price <= 0:
            return

        credit = short_put.bid_price - long_put.ask_price

        # Validate credit
        if credit < self.MIN_CREDIT:
            self.debug(f"  Skip: Credit ${credit:.2f} < ${self.MIN_CREDIT} minimum")
            return

        if credit / width < self.MIN_CREDIT_WIDTH_RATIO:
            self.debug(f"  Skip: Credit/Width {credit/width:.1%} < {self.MIN_CREDIT_WIDTH_RATIO:.0%}")
            return

        # Calculate position size
        max_loss_per_contract = (width - credit) * 100
        available_capital = self.portfolio.total_portfolio_value * self.POSITION_SIZE_PCT
        contracts = max(1, int(available_capital / max_loss_per_contract))
        contracts = min(contracts, 5)  # Cap at 5 contracts

        # Check buying power
        required_margin = width * 100 * contracts
        if required_margin > self.portfolio.margin_remaining * 0.8:
            self.debug(f"  Skip: Insufficient margin (need ${required_margin:.0f}, have ${self.portfolio.margin_remaining:.0f})")
            return

        # Place orders
        self.debug(f"ENTRY {self.time.date()}: {short_put.strike}/{long_put.strike} "
                   f"exp {best_expiry.date()} credit ${credit:.2f} x{contracts}")

        # Sell short put
        ticket1 = self.market_order(short_put.symbol, -contracts)
        # Buy long put
        ticket2 = self.market_order(long_put.symbol, contracts)

        if ticket1.status == OrderStatus.INVALID or ticket2.status == OrderStatus.INVALID:
            self.debug(f"  Order failed!")
            return

        # Track the spread
        spread = ActiveSpread(
            entry_date=self.time,
            expiry=best_expiry,
            short_strike=short_put.strike,
            long_strike=long_put.strike,
            short_symbol=short_put.symbol,
            long_symbol=long_put.symbol,
            credit=credit,
            width=width,
            contracts=contracts,
            max_profit=credit * 100 * contracts,
            max_loss=(width - credit) * 100 * contracts,
            iv_rank_at_entry=iv_rank,
        )
        self.active_spreads.append(spread)
        self.last_entry_date = self.time

    # ==========================================================================
    # POSITION MANAGEMENT
    # ==========================================================================

    def manage_positions(self, data):
        """Check positions for exit conditions."""

        for spread in self.active_spreads:
            if spread.is_closed:
                continue

            # Check expiration
            if self.time.date() >= spread.expiry.date():
                self.close_spread(spread, "EXPIRY")
                continue

            # Get current prices
            short_security = self.securities.get(spread.short_symbol)
            long_security = self.securities.get(spread.long_symbol)

            if short_security is None or long_security is None:
                continue

            # Cost to close: buy short at ask, sell long at bid
            short_ask = short_security.ask_price
            long_bid = long_security.bid_price

            if short_ask <= 0 or long_bid <= 0:
                continue

            close_cost = short_ask - long_bid
            if close_cost < 0:
                close_cost = 0  # Can't have negative cost to close

            # Current P&L
            current_pnl = (spread.credit - close_cost) * 100 * spread.contracts

            # Take profit check: 50% of max profit
            take_profit_target = spread.max_profit * self.TAKE_PROFIT_PCT
            if current_pnl >= take_profit_target:
                spread.exit_pnl = current_pnl
                self.close_spread(spread, "TAKE_PROFIT")
                continue

            # Stop loss check: loss exceeds 2x credit
            stop_loss_limit = spread.credit * self.STOP_LOSS_MULTIPLIER * 100 * spread.contracts
            if current_pnl <= -stop_loss_limit:
                spread.exit_pnl = current_pnl
                self.close_spread(spread, "STOP_LOSS")
                continue

    def close_spread(self, spread, reason: str):
        """Close a spread position."""
        if spread.is_closed:
            return

        # Close positions
        short_holding = self.portfolio.get(spread.short_symbol)
        long_holding = self.portfolio.get(spread.long_symbol)

        if short_holding and short_holding.quantity != 0:
            self.market_order(spread.short_symbol, -short_holding.quantity)
        if long_holding and long_holding.quantity != 0:
            self.market_order(spread.long_symbol, -long_holding.quantity)

        spread.is_closed = True
        spread.exit_date = self.time
        spread.exit_reason = reason

        # Log the trade
        self.trade_log.append({
            'entry_date': spread.entry_date,
            'exit_date': spread.exit_date,
            'short_strike': spread.short_strike,
            'long_strike': spread.long_strike,
            'credit': spread.credit,
            'contracts': spread.contracts,
            'max_profit': spread.max_profit,
            'exit_reason': reason,
            'pnl': spread.exit_pnl if spread.exit_pnl else 0,
            'iv_rank': spread.iv_rank_at_entry,
        })

        self.debug(f"EXIT ({reason}) {self.time.date()}: {spread.short_strike}/{spread.long_strike} "
                   f"P&L: ${spread.exit_pnl:.2f}" if spread.exit_pnl else f"EXIT ({reason})")

    # ==========================================================================
    # REPORTING
    # ==========================================================================

    def on_end_of_algorithm(self):
        """Generate final report."""

        # Close any remaining positions
        for spread in self.active_spreads:
            if not spread.is_closed:
                self.close_spread(spread, "END_OF_BACKTEST")

        # Calculate statistics
        total_trades = len(self.trade_log)

        if total_trades == 0:
            self.debug("No trades executed")
            return

        wins = [t for t in self.trade_log if t['pnl'] and t['pnl'] > 0]
        losses = [t for t in self.trade_log if t['pnl'] and t['pnl'] <= 0]

        take_profits = len([t for t in self.trade_log if t['exit_reason'] == 'TAKE_PROFIT'])
        stop_losses = len([t for t in self.trade_log if t['exit_reason'] == 'STOP_LOSS'])
        expirations = len([t for t in self.trade_log if t['exit_reason'] == 'EXPIRY'])

        total_pnl = sum(t['pnl'] for t in self.trade_log if t['pnl'])
        avg_pnl = total_pnl / total_trades if total_trades > 0 else 0

        avg_win = sum(t['pnl'] for t in wins) / len(wins) if wins else 0
        avg_loss = sum(t['pnl'] for t in losses) / len(losses) if losses else 0

        win_rate = len(wins) / total_trades if total_trades > 0 else 0

        self.debug("=" * 70)
        self.debug("FINAL BACKTEST RESULTS - PUT CREDIT SPREAD STRATEGY")
        self.debug("=" * 70)
        self.debug(f"Period: {self.start_date.date()} to {self.end_date.date()}")
        self.debug(f"Starting Capital: $100,000")
        self.debug(f"Final Portfolio Value: ${self.portfolio.total_portfolio_value:,.2f}")
        self.debug(f"Total Return: {(self.portfolio.total_portfolio_value / 100000 - 1) * 100:.1f}%")
        self.debug("-" * 70)
        self.debug(f"Total Trades: {total_trades}")
        self.debug(f"Winning Trades: {len(wins)}")
        self.debug(f"Losing Trades: {len(losses)}")
        self.debug(f"Win Rate: {win_rate:.1%}")
        self.debug("-" * 70)
        self.debug(f"Total P&L: ${total_pnl:,.2f}")
        self.debug(f"Average P&L per Trade: ${avg_pnl:,.2f}")
        self.debug(f"Average Win: ${avg_win:,.2f}")
        self.debug(f"Average Loss: ${avg_loss:,.2f}")
        self.debug("-" * 70)
        self.debug(f"Take Profits: {take_profits}")
        self.debug(f"Stop Losses: {stop_losses}")
        self.debug(f"Held to Expiry: {expirations}")
        self.debug("=" * 70)


# ==============================================================================
# HELPER CLASS
# ==============================================================================

class ActiveSpread:
    """Track an active put credit spread."""

    def __init__(self, entry_date, expiry, short_strike, long_strike,
                 short_symbol, long_symbol, credit, width, contracts,
                 max_profit, max_loss, iv_rank_at_entry):
        self.entry_date = entry_date
        self.expiry = expiry
        self.short_strike = short_strike
        self.long_strike = long_strike
        self.short_symbol = short_symbol
        self.long_symbol = long_symbol
        self.credit = credit
        self.width = width
        self.contracts = contracts
        self.max_profit = max_profit
        self.max_loss = max_loss
        self.iv_rank_at_entry = iv_rank_at_entry

        self.is_closed = False
        self.exit_date = None
        self.exit_reason = None
        self.exit_pnl = None
