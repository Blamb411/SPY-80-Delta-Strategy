# region imports
from AlgorithmImports import *
from datetime import timedelta
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
# endregion

"""
Put Credit Spread - Comprehensive Exit Strategy & Width Test
============================================================

Tests 48 combinations:
- 4 Spread Widths: $5, $10, $20, $50
- 4 Take Profit Levels: 25%, 50%, 75%, 100% (hold to expiry)
- 3 Stop Loss Levels: 50%, 100%, 200% of credit

Uses shadow tracking to simulate all combinations from a single backtest run.

Position Management:
- Max 10 concurrent positions
- 7-day minimum between entries
"""


# =============================================================================
# CONFIGURATION
# =============================================================================

# Test Parameters
SPREAD_WIDTHS = [5, 10, 20, 50]  # Dollar widths to test
TAKE_PROFIT_PCTS = [0.25, 0.50, 0.75, 1.00]  # 1.00 = hold to expiry
STOP_LOSS_PCTS = [0.50, 1.00, 2.00]  # Multiplier of credit received

# Entry Filters (matching original strategy)
SMA_PERIOD = 200
RSI_PERIOD = 14
RSI_MAX = 75.0
IV_RANK_MIN = 0.30

# Trade Construction
TARGET_DELTA = 0.25
TARGET_DTE_MIN = 25
TARGET_DTE_MAX = 40
MIN_CREDIT_PCT = 0.10  # Credit must be >= 10% of width

# Position Management
MAX_CONCURRENT_POSITIONS = 10
MIN_DAYS_BETWEEN_ENTRIES = 7


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class ShadowPosition:
    """Tracks hypothetical P&L for one width/exit combination."""
    width: int
    tp_pct: float
    sl_pct: float
    entry_credit: float
    max_profit: float
    max_loss: float
    is_closed: bool = False
    exit_reason: str = ""
    exit_pnl: float = 0.0


@dataclass
class SpreadTrade:
    """Tracks a real spread position with shadow positions for all combinations."""
    entry_date: datetime
    expiry: datetime
    spot_at_entry: float
    short_strike: float
    short_symbol: any

    # Long strikes for each width
    long_strikes: Dict[int, float] = field(default_factory=dict)
    long_symbols: Dict[int, any] = field(default_factory=dict)

    # Entry credits for each width
    entry_credits: Dict[int, float] = field(default_factory=dict)

    # Shadow positions: key = (width, tp_pct, sl_pct)
    shadows: Dict[Tuple, ShadowPosition] = field(default_factory=dict)

    # Actual position tracking
    actual_width: int = 10  # The width we actually trade
    is_closed: bool = False
    contracts: int = 1


# =============================================================================
# MAIN ALGORITHM
# =============================================================================

class PutSpreadComprehensiveTest(QCAlgorithm):

    def initialize(self):
        # Backtest period
        self.set_start_date(2020, 1, 1)
        self.set_end_date(2024, 12, 31)
        self.set_cash(100000)

        # Warm up for indicators
        self.set_warm_up(timedelta(days=250))

        # Brokerage model
        self.set_brokerage_model(BrokerageName.INTERACTIVE_BROKERS_BROKERAGE, AccountType.MARGIN)

        # Add SPY
        self.spy = self.add_equity("SPY", Resolution.DAILY)
        self.spy.set_data_normalization_mode(DataNormalizationMode.RAW)

        # Add options
        option = self.add_option("SPY", Resolution.MINUTE)
        self.option_symbol = option.symbol
        option.set_filter(lambda u: u
            .include_weeklys()
            .strikes(-60, 5)  # Wider range for $50 spreads
            .expiration(timedelta(days=TARGET_DTE_MIN),
                       timedelta(days=TARGET_DTE_MAX + 10)))

        # Indicators
        self.sma = self.sma("SPY", SMA_PERIOD, Resolution.DAILY)
        self.rsi = self.rsi("SPY", RSI_PERIOD, MovingAverageType.WILDERS, Resolution.DAILY)
        self.iv_history = deque(maxlen=252)

        # Position tracking
        self.active_trades: List[SpreadTrade] = []
        self.last_entry_date = self.start_date - timedelta(days=MIN_DAYS_BETWEEN_ENTRIES + 1)

        # Results aggregation: key = (width, tp_pct, sl_pct)
        self.results: Dict[Tuple, List[float]] = {}
        for width in SPREAD_WIDTHS:
            for tp in TAKE_PROFIT_PCTS:
                for sl in STOP_LOSS_PCTS:
                    self.results[(width, tp, sl)] = []

        # Schedule daily check
        self.schedule.on(
            self.date_rules.every_day("SPY"),
            self.time_rules.after_market_open("SPY", 30),
            self.daily_check
        )

        self.debug("=" * 70)
        self.debug("PUT SPREAD COMPREHENSIVE TEST - 48 COMBINATIONS")
        self.debug(f"Widths: {SPREAD_WIDTHS}")
        self.debug(f"Take Profits: {[f'{x:.0%}' for x in TAKE_PROFIT_PCTS]}")
        self.debug(f"Stop Losses: {[f'{x:.0%}' for x in STOP_LOSS_PCTS]}")
        self.debug(f"Max Positions: {MAX_CONCURRENT_POSITIONS}")
        self.debug("=" * 70)

    # =========================================================================
    # DAILY LOGIC
    # =========================================================================

    def daily_check(self):
        """Daily check for new entries."""
        if self.is_warming_up:
            return

        self.update_iv_history()
        self.check_entry()

    def on_data(self, data):
        """Monitor positions and update shadow tracking."""
        if self.is_warming_up:
            return

        self.update_shadow_positions(data)

    # =========================================================================
    # IV TRACKING
    # =========================================================================

    def update_iv_history(self):
        """Update IV history for IV Rank calculation."""
        chain = self.current_slice.option_chains.get(self.option_symbol)
        if chain is None:
            return

        spot = self.securities["SPY"].price
        puts = [c for c in chain if c.right == OptionRight.PUT]
        if not puts:
            return

        atm_put = min(puts, key=lambda c: abs(c.strike - spot))
        if atm_put.implied_volatility and atm_put.implied_volatility > 0:
            self.iv_history.append(atm_put.implied_volatility)

    def get_iv_rank(self) -> Optional[float]:
        """Calculate IV Rank."""
        if len(self.iv_history) < 20:
            return None
        current_iv = self.iv_history[-1]
        iv_min, iv_max = min(self.iv_history), max(self.iv_history)
        if iv_max == iv_min:
            return 0.5
        return (current_iv - iv_min) / (iv_max - iv_min)

    # =========================================================================
    # ENTRY LOGIC
    # =========================================================================

    def check_entry(self):
        """Check for new entry opportunity."""

        # Position limit
        active_count = len([t for t in self.active_trades if not t.is_closed])
        if active_count >= MAX_CONCURRENT_POSITIONS:
            return

        # Time since last entry
        if (self.time - self.last_entry_date).days < MIN_DAYS_BETWEEN_ENTRIES:
            return

        # Indicators ready
        if not self.sma.is_ready or not self.rsi.is_ready:
            return

        spot = self.securities["SPY"].price
        sma_val = self.sma.current.value
        rsi_val = self.rsi.current.value
        iv_rank = self.get_iv_rank()

        # Entry filters
        if spot <= sma_val:
            return
        if rsi_val >= RSI_MAX:
            return
        if iv_rank is None or iv_rank < IV_RANK_MIN:
            return

        # Try to enter
        self.enter_spread(spot)

    def enter_spread(self, spot: float):
        """Enter a spread and set up shadow tracking for all combinations."""

        chain = self.current_slice.option_chains.get(self.option_symbol)
        if chain is None:
            return

        puts = [c for c in chain if c.right == OptionRight.PUT]
        if len(puts) < 20:
            return

        # Find expiration
        target_expiry = self.time + timedelta(days=30)
        expirations = sorted(set(c.expiry for c in puts))
        valid_exp = [e for e in expirations
                    if TARGET_DTE_MIN <= (e - self.time).days <= TARGET_DTE_MAX]
        if not valid_exp:
            return

        best_expiry = min(valid_exp, key=lambda x: abs((x - target_expiry).days))
        puts_at_exp = [c for c in puts if c.expiry == best_expiry]

        if len(puts_at_exp) < 10:
            return

        # Find short strike (~25 delta or 5% OTM)
        target_short = spot * 0.95
        puts_with_greeks = [c for c in puts_at_exp
                          if c.greeks and c.greeks.delta and abs(c.greeks.delta) > 0.01]

        if puts_with_greeks:
            short_put = min(puts_with_greeks,
                           key=lambda c: abs(abs(c.greeks.delta) - TARGET_DELTA))
        else:
            short_put = min(puts_at_exp, key=lambda c: abs(c.strike - target_short))

        if short_put.bid_price <= 0:
            return

        # Find long strikes for each width
        trade = SpreadTrade(
            entry_date=self.time,
            expiry=best_expiry,
            spot_at_entry=spot,
            short_strike=short_put.strike,
            short_symbol=short_put.symbol,
        )

        valid_widths = []

        for width in SPREAD_WIDTHS:
            target_long = short_put.strike - width
            long_candidates = [c for c in puts_at_exp
                              if c.strike <= target_long and c.strike >= target_long - 3]

            if not long_candidates:
                continue

            long_put = min(long_candidates, key=lambda c: abs(c.strike - target_long))

            if long_put.ask_price <= 0:
                continue

            actual_width = short_put.strike - long_put.strike
            if actual_width < width * 0.8:  # Must be at least 80% of target width
                continue

            credit = short_put.bid_price - long_put.ask_price

            if credit <= 0 or credit / actual_width < MIN_CREDIT_PCT:
                continue

            # Store this width's data
            trade.long_strikes[width] = long_put.strike
            trade.long_symbols[width] = long_put.symbol
            trade.entry_credits[width] = credit
            valid_widths.append(width)

            # Create shadow positions for all TP/SL combinations
            for tp in TAKE_PROFIT_PCTS:
                for sl in STOP_LOSS_PCTS:
                    max_profit = credit * 100
                    max_loss = (actual_width - credit) * 100

                    trade.shadows[(width, tp, sl)] = ShadowPosition(
                        width=width,
                        tp_pct=tp,
                        sl_pct=sl,
                        entry_credit=credit,
                        max_profit=max_profit,
                        max_loss=max_loss,
                    )

        if not valid_widths:
            return

        # Choose actual width to trade (use $10 as base, or smallest available)
        trade.actual_width = 10 if 10 in valid_widths else min(valid_widths)

        # Place actual orders
        actual_long_symbol = trade.long_symbols[trade.actual_width]

        self.market_order(short_put.symbol, -1)
        self.market_order(actual_long_symbol, 1)

        trade.contracts = 1
        self.active_trades.append(trade)
        self.last_entry_date = self.time

        self.debug(f"ENTRY {self.time.date()}: {short_put.strike} short, "
                   f"widths={valid_widths}, credits={[f'${trade.entry_credits[w]:.2f}' for w in valid_widths]}")

    # =========================================================================
    # POSITION MANAGEMENT
    # =========================================================================

    def update_shadow_positions(self, data):
        """Update all shadow positions and check for exits."""

        for trade in self.active_trades:
            if trade.is_closed:
                continue

            # Check expiration
            if self.time.date() >= trade.expiry.date():
                self.close_trade(trade, "EXPIRY")
                continue

            # Get current short put price
            short_security = self.securities.get(trade.short_symbol)
            if short_security is None:
                continue

            short_ask = short_security.ask_price
            if short_ask <= 0:
                continue

            # Update each shadow position
            for (width, tp, sl), shadow in trade.shadows.items():
                if shadow.is_closed:
                    continue

                # Get long put price for this width
                if width not in trade.long_symbols:
                    continue

                long_security = self.securities.get(trade.long_symbols[width])
                if long_security is None:
                    continue

                long_bid = long_security.bid_price
                if long_bid < 0:
                    long_bid = 0

                # Cost to close
                close_cost = short_ask - long_bid
                if close_cost < 0:
                    close_cost = 0

                # Current P&L
                current_pnl = (shadow.entry_credit - close_cost) * 100

                # Take profit check
                if tp < 1.0:  # 1.0 means hold to expiry
                    profit_target = shadow.max_profit * tp
                    if current_pnl >= profit_target:
                        shadow.is_closed = True
                        shadow.exit_reason = f"TP_{tp:.0%}"
                        shadow.exit_pnl = current_pnl
                        continue

                # Stop loss check
                loss_limit = shadow.entry_credit * sl * 100
                if current_pnl <= -loss_limit:
                    shadow.is_closed = True
                    shadow.exit_reason = f"SL_{sl:.0%}"
                    shadow.exit_pnl = current_pnl
                    continue

    def close_trade(self, trade: SpreadTrade, reason: str):
        """Close a trade and finalize all shadow positions."""

        if trade.is_closed:
            return

        # Close actual position
        short_holding = self.portfolio.get(trade.short_symbol)
        if short_holding and short_holding.quantity != 0:
            self.market_order(trade.short_symbol, -short_holding.quantity)

        actual_long = trade.long_symbols.get(trade.actual_width)
        if actual_long:
            long_holding = self.portfolio.get(actual_long)
            if long_holding and long_holding.quantity != 0:
                self.market_order(actual_long, -long_holding.quantity)

        trade.is_closed = True

        # Finalize shadow positions
        for (width, tp, sl), shadow in trade.shadows.items():
            if not shadow.is_closed:
                # Calculate expiry P&L
                spot = self.securities["SPY"].price
                short_strike = trade.short_strike
                long_strike = trade.long_strikes.get(width)

                if long_strike is None:
                    continue

                if spot >= short_strike:
                    # Expired OTM - full profit
                    shadow.exit_pnl = shadow.max_profit
                elif spot <= long_strike:
                    # Expired deep ITM - max loss
                    shadow.exit_pnl = -shadow.max_loss
                else:
                    # Between strikes
                    intrinsic = short_strike - spot
                    shadow.exit_pnl = (shadow.entry_credit - intrinsic) * 100

                shadow.is_closed = True
                shadow.exit_reason = reason

            # Record result
            self.results[(width, tp, sl)].append(shadow.exit_pnl)

    # =========================================================================
    # REPORTING
    # =========================================================================

    def on_end_of_algorithm(self):
        """Generate comprehensive comparison report."""

        # Close remaining trades
        for trade in self.active_trades:
            if not trade.is_closed:
                self.close_trade(trade, "END")

        self.debug("")
        self.debug("=" * 90)
        self.debug("COMPREHENSIVE EXIT STRATEGY & WIDTH COMPARISON")
        self.debug("=" * 90)
        self.debug(f"Period: {self.start_date.date()} to {self.end_date.date()}")
        self.debug(f"Total Entry Signals: {len(self.active_trades)}")
        self.debug("")

        # Header
        self.debug(f"{'Width':<8} {'TP%':<8} {'SL%':<8} {'Trades':<8} {'Win%':<8} {'Total P&L':<12} {'Avg P&L':<10} {'Avg Win':<10} {'Avg Loss':<10}")
        self.debug("-" * 90)

        # Collect all results for sorting
        all_results = []

        for width in SPREAD_WIDTHS:
            for tp in TAKE_PROFIT_PCTS:
                for sl in STOP_LOSS_PCTS:
                    pnls = self.results[(width, tp, sl)]

                    if not pnls:
                        continue

                    trades = len(pnls)
                    wins = [p for p in pnls if p > 0]
                    losses = [p for p in pnls if p <= 0]

                    win_rate = len(wins) / trades if trades > 0 else 0
                    total_pnl = sum(pnls)
                    avg_pnl = total_pnl / trades if trades > 0 else 0
                    avg_win = sum(wins) / len(wins) if wins else 0
                    avg_loss = sum(losses) / len(losses) if losses else 0

                    all_results.append({
                        'width': width,
                        'tp': tp,
                        'sl': sl,
                        'trades': trades,
                        'win_rate': win_rate,
                        'total_pnl': total_pnl,
                        'avg_pnl': avg_pnl,
                        'avg_win': avg_win,
                        'avg_loss': avg_loss,
                    })

                    tp_str = f"{tp:.0%}" if tp < 1.0 else "Hold"
                    self.debug(f"${width:<7} {tp_str:<8} {sl:.0%}x{'':<5} {trades:<8} {win_rate:<8.1%} ${total_pnl:<11,.0f} ${avg_pnl:<9,.2f} ${avg_win:<9,.2f} ${avg_loss:<9,.2f}")

            self.debug("")  # Blank line between widths

        # Find best combinations
        self.debug("=" * 90)
        self.debug("TOP 5 COMBINATIONS BY TOTAL P&L")
        self.debug("=" * 90)

        sorted_by_pnl = sorted(all_results, key=lambda x: x['total_pnl'], reverse=True)[:5]
        for i, r in enumerate(sorted_by_pnl, 1):
            tp_str = f"{r['tp']:.0%}" if r['tp'] < 1.0 else "Hold"
            self.debug(f"{i}. Width ${r['width']}, TP {tp_str}, SL {r['sl']:.0%}x: "
                      f"${r['total_pnl']:,.0f} total, {r['win_rate']:.1%} win rate, ${r['avg_pnl']:.2f} avg")

        self.debug("")
        self.debug("TOP 5 COMBINATIONS BY RISK-ADJUSTED (Avg P&L / Width)")
        self.debug("=" * 90)

        for r in all_results:
            r['risk_adj'] = r['avg_pnl'] / r['width'] if r['width'] > 0 else 0

        sorted_by_risk_adj = sorted(all_results, key=lambda x: x['risk_adj'], reverse=True)[:5]
        for i, r in enumerate(sorted_by_risk_adj, 1):
            tp_str = f"{r['tp']:.0%}" if r['tp'] < 1.0 else "Hold"
            self.debug(f"{i}. Width ${r['width']}, TP {tp_str}, SL {r['sl']:.0%}x: "
                      f"${r['avg_pnl']:.2f} avg / ${r['width']} width = {r['risk_adj']:.3f}")

        self.debug("")
        self.debug("=" * 90)
        self.debug("WIDTH COMPARISON (Averaged across all exit strategies)")
        self.debug("=" * 90)

        for width in SPREAD_WIDTHS:
            width_results = [r for r in all_results if r['width'] == width]
            if not width_results:
                continue
            avg_total = sum(r['total_pnl'] for r in width_results) / len(width_results)
            avg_win_rate = sum(r['win_rate'] for r in width_results) / len(width_results)
            avg_avg_pnl = sum(r['avg_pnl'] for r in width_results) / len(width_results)
            self.debug(f"${width} width: Avg Total P&L ${avg_total:,.0f}, Avg Win Rate {avg_win_rate:.1%}, Avg P&L/Trade ${avg_avg_pnl:.2f}")

        self.debug("")
        self.debug("=" * 90)
        self.debug(f"Final Portfolio Value: ${self.portfolio.total_portfolio_value:,.2f}")
        self.debug("=" * 90)
