# region imports
from AlgorithmImports import *
from datetime import timedelta
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
# endregion

"""
Multi-Ticker Options Spread Test - Shadow Tracking
====================================================

Shadow-tracks put credit spreads and call credit spreads across 12 Tier 1 ETFs
at daily resolution. No actual orders placed - all P&L calculated from bid/ask.
Iron condor results can be derived offline by combining put+call spread data.

Tickers: SPY, QQQ, IWM, DIA, XLF, XLE, XLK, XLV, GLD, SLV, TLT, HYG
Period:  2020-01-01 to 2025-12-31
Cash:    $100,000

16 shadow positions per entry signal:
  2 strategies (put spread, call spread)
  x 2 widths ($5, $10)
  x 2 take profit levels (50%, 75%)
  x 2 stop loss levels (100%, 200%)

No SMA directional filter - tests all market conditions.
Width validation: skips widths > 10% of underlying price.
"""


# =============================================================================
# CONFIGURATION
# =============================================================================

TICKERS = ["SPY", "QQQ", "IWM", "DIA", "XLF", "XLE", "XLK", "XLV",
           "GLD", "SLV", "TLT", "HYG"]

SPREAD_WIDTHS = [5, 10]
TAKE_PROFIT_PCTS = [0.50, 0.75]
STOP_LOSS_MULTS = [1.0, 2.0]

TARGET_DELTA = 0.25
OTM_FALLBACK_PCT = 0.05
TARGET_DTE_IDEAL = 35
TARGET_DTE_MIN = 25
TARGET_DTE_MAX = 45
IV_RANK_MIN = 0.25
MIN_DAYS_BETWEEN_ENTRIES = 7
MAX_CONCURRENT_PER_TICKER = 3
MAX_WIDTH_PCT = 0.10  # Width must be <= 10% of underlying price
MIN_CREDIT = 0.10     # Minimum $0.10 credit per share
MIN_CREDIT_WIDTH_RATIO = 0.08  # Credit >= 8% of width


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class ShadowLeg:
    """One option leg in a shadow position."""
    symbol: object
    strike: float
    right: int       # OptionRight.PUT or OptionRight.CALL
    quantity: int     # +1 for long, -1 for short
    entry_price: float


@dataclass
class ShadowPosition:
    """One shadow-tracked position for a specific (strategy, width, TP, SL) combo."""
    strategy: str     # "PUT" or "CALL"
    width: int
    tp_pct: float
    sl_mult: float
    short_leg: ShadowLeg
    long_leg: ShadowLeg
    entry_credit: float
    max_profit: float   # entry_credit * 100
    actual_width: float
    entry_date: object
    expiry: object
    spot_at_entry: float
    is_closed: bool = False
    exit_reason: str = ""
    exit_pnl: float = 0.0
    exit_date: object = None
    peak_pnl: float = 0.0
    trough_pnl: float = 0.0


class SymbolData:
    """Per-ticker state tracking."""

    def __init__(self, ticker: str, start_date):
        self.ticker = ticker
        self.iv_history = deque(maxlen=252)
        self.last_entry_date = start_date - timedelta(days=MIN_DAYS_BETWEEN_ENTRIES + 1)
        self.active_positions: List[ShadowPosition] = []
        self.option_symbol = None  # Set after add_option


# =============================================================================
# MAIN ALGORITHM
# =============================================================================

class MultiTickerSpreadTest(QCAlgorithm):

    def initialize(self):
        self.set_start_date(2020, 1, 1)
        self.set_end_date(2025, 12, 31)
        self.set_cash(100000)

        self.set_warm_up(timedelta(days=365))

        self.set_brokerage_model(BrokerageName.INTERACTIVE_BROKERS_BROKERAGE, AccountType.MARGIN)

        # Per-ticker tracking
        self.symbol_data: Dict[str, SymbolData] = {}

        # Results storage: list of lightweight dicts for completed trades
        self.completed_trades: List[dict] = []

        # Add all tickers
        for ticker in TICKERS:
            equity = self.add_equity(ticker, Resolution.DAILY)
            equity.set_data_normalization_mode(DataNormalizationMode.RAW)

            option = self.add_option(ticker, Resolution.DAILY)
            option.set_filter(lambda u: u
                .include_weeklys()
                .strikes(-30, 30)
                .expiration(timedelta(days=TARGET_DTE_MIN),
                           timedelta(days=TARGET_DTE_MAX + 5)))

            sd = SymbolData(ticker, self.start_date)
            sd.option_symbol = option.symbol
            self.symbol_data[ticker] = sd

        self.entry_signal_count = 0
        self.last_process_date = None  # Track to run entry check once per day

        self.debug("=" * 70)
        self.debug("MULTI-TICKER SPREAD TEST - SHADOW TRACKING")
        self.debug(f"Tickers: {', '.join(TICKERS)}")
        self.debug(f"Widths: {SPREAD_WIDTHS}, TP: {TAKE_PROFIT_PCTS}, SL: {STOP_LOSS_MULTS}")
        self.debug(f"Period: {self.start_date.date()} to {self.end_date.date()}")
        self.debug(f"Combos per entry: {len(SPREAD_WIDTHS) * len(TAKE_PROFIT_PCTS) * len(STOP_LOSS_MULTS)} per strategy")
        self.debug("=" * 70)

    # =========================================================================
    # IV TRACKING
    # =========================================================================

    def _update_iv_history(self, ticker: str, sd: SymbolData, data):
        """Update IV history for a ticker from its option chain."""
        chain = data.option_chains.get(sd.option_symbol)
        if chain is None:
            return

        spot = self.securities[ticker].price
        if spot <= 0:
            return

        puts = [c for c in chain if c.right == OptionRight.PUT]
        if not puts:
            return

        atm_put = min(puts, key=lambda c: abs(c.strike - spot))
        if atm_put.implied_volatility and atm_put.implied_volatility > 0:
            sd.iv_history.append(atm_put.implied_volatility)

    def _get_iv_rank(self, sd: SymbolData) -> Optional[float]:
        """Calculate IV Rank for a ticker."""
        if len(sd.iv_history) < 20:
            return None
        current_iv = sd.iv_history[-1]
        iv_min = min(sd.iv_history)
        iv_max = max(sd.iv_history)
        if iv_max == iv_min:
            return 0.5
        return (current_iv - iv_min) / (iv_max - iv_min)

    # =========================================================================
    # ENTRY LOGIC
    # =========================================================================

    def _check_entry(self, ticker: str, sd: SymbolData, data):
        """Check if we should create shadow positions for this ticker."""

        # Count active (non-closed) positions as entry signals
        # Each entry signal spawns multiple shadow positions, so count unique entry dates
        active_entry_dates = set()
        for pos in sd.active_positions:
            if not pos.is_closed:
                active_entry_dates.add(pos.entry_date)

        if len(active_entry_dates) >= MAX_CONCURRENT_PER_TICKER:
            return

        # Time since last entry
        if (self.time - sd.last_entry_date).days < MIN_DAYS_BETWEEN_ENTRIES:
            return

        # IV Rank filter
        iv_rank = self._get_iv_rank(sd)
        if iv_rank is None or iv_rank < IV_RANK_MIN:
            return

        # Try to build spreads
        self._build_entry(ticker, sd, iv_rank, data)

    def _build_entry(self, ticker: str, sd: SymbolData, iv_rank: float, data):
        """Build put and call spread shadow positions for an entry signal."""

        chain = data.option_chains.get(sd.option_symbol)
        if chain is None:
            return

        spot = self.securities[ticker].price
        if spot <= 0:
            return

        contracts = list(chain)
        if len(contracts) < 10:
            return

        puts = [c for c in contracts if c.right == OptionRight.PUT]
        calls = [c for c in contracts if c.right == OptionRight.CALL]

        if len(puts) < 5 or len(calls) < 5:
            return

        # Find best expiration (closest to TARGET_DTE_IDEAL within range)
        all_expiries = sorted(set(c.expiry for c in contracts))
        valid_expiries = [e for e in all_expiries
                         if TARGET_DTE_MIN <= (e - self.time).days <= TARGET_DTE_MAX]

        if not valid_expiries:
            return

        target_date = self.time + timedelta(days=TARGET_DTE_IDEAL)
        best_expiry = min(valid_expiries, key=lambda x: abs((x - target_date).days))

        puts_at_exp = [c for c in puts if c.expiry == best_expiry]
        calls_at_exp = [c for c in calls if c.expiry == best_expiry]

        if len(puts_at_exp) < 5 or len(calls_at_exp) < 5:
            return

        # Find short strikes
        short_put = self._find_short_strike(puts_at_exp, spot, OptionRight.PUT)
        short_call = self._find_short_strike(calls_at_exp, spot, OptionRight.CALL)

        if short_put is None and short_call is None:
            return

        created_any = False

        # Build put credit spreads for each width
        if short_put is not None and short_put.bid_price > 0:
            for width in SPREAD_WIDTHS:
                if self._build_spread_shadows(
                    ticker, sd, "PUT", short_put, puts_at_exp,
                    width, spot, best_expiry, iv_rank
                ):
                    created_any = True

        # Build call credit spreads for each width
        if short_call is not None and short_call.bid_price > 0:
            for width in SPREAD_WIDTHS:
                if self._build_spread_shadows(
                    ticker, sd, "CALL", short_call, calls_at_exp,
                    width, spot, best_expiry, iv_rank
                ):
                    created_any = True

        if created_any:
            sd.last_entry_date = self.time
            self.entry_signal_count += 1
            if self.entry_signal_count <= 50:
                self.debug(f"ENTRY #{self.entry_signal_count} {self.time.date()} {ticker} "
                           f"spot=${spot:.2f} IVR={iv_rank:.0%} exp={best_expiry.date()}")

    def _find_short_strike(self, options_at_exp: list, spot: float,
                           right: int) -> Optional[object]:
        """Find the short strike at ~0.25 delta or 5% OTM fallback."""

        # Try delta-based selection
        with_greeks = [c for c in options_at_exp
                       if c.greeks and c.greeks.delta
                       and abs(c.greeks.delta) > 0.01]

        if with_greeks:
            candidate = min(with_greeks,
                           key=lambda c: abs(abs(c.greeks.delta) - TARGET_DELTA))
            # Sanity check: short put should be below spot, short call above
            if right == OptionRight.PUT and candidate.strike < spot:
                return candidate
            elif right == OptionRight.CALL and candidate.strike > spot:
                return candidate

        # Fallback: 5% OTM
        if right == OptionRight.PUT:
            target = spot * (1 - OTM_FALLBACK_PCT)
            otm = [c for c in options_at_exp if c.strike < spot]
        else:
            target = spot * (1 + OTM_FALLBACK_PCT)
            otm = [c for c in options_at_exp if c.strike > spot]

        if not otm:
            return None

        return min(otm, key=lambda c: abs(c.strike - target))

    def _build_spread_shadows(self, ticker: str, sd: SymbolData,
                              strategy: str, short_contract: object,
                              options_at_exp: list, width: int,
                              spot: float, expiry: object,
                              iv_rank: float) -> bool:
        """Build shadow positions for one strategy+width combo. Returns True if created."""

        # Width validation: skip if width > 10% of underlying price
        if width > spot * MAX_WIDTH_PCT:
            return False

        # Find long leg
        if strategy == "PUT":
            target_long_strike = short_contract.strike - width
            long_candidates = [c for c in options_at_exp
                              if c.strike <= target_long_strike
                              and c.strike >= target_long_strike - 3]
        else:  # CALL
            target_long_strike = short_contract.strike + width
            long_candidates = [c for c in options_at_exp
                              if c.strike >= target_long_strike
                              and c.strike <= target_long_strike + 3]

        if not long_candidates:
            return False

        long_contract = min(long_candidates,
                           key=lambda c: abs(c.strike - target_long_strike))

        if long_contract.ask_price <= 0:
            return False

        # Calculate actual width and credit
        actual_width = abs(short_contract.strike - long_contract.strike)
        if actual_width < width * 0.8:
            return False

        credit = short_contract.bid_price - long_contract.ask_price
        if credit <= MIN_CREDIT:
            return False

        if credit / actual_width < MIN_CREDIT_WIDTH_RATIO:
            return False

        # Create shadow positions for all TP/SL combos
        max_profit = credit * 100
        short_leg = ShadowLeg(
            symbol=short_contract.symbol,
            strike=short_contract.strike,
            right=short_contract.right,
            quantity=-1,
            entry_price=short_contract.bid_price
        )
        long_leg = ShadowLeg(
            symbol=long_contract.symbol,
            strike=long_contract.strike,
            right=long_contract.right,
            quantity=1,
            entry_price=long_contract.ask_price
        )

        for tp in TAKE_PROFIT_PCTS:
            for sl in STOP_LOSS_MULTS:
                pos = ShadowPosition(
                    strategy=strategy,
                    width=width,
                    tp_pct=tp,
                    sl_mult=sl,
                    short_leg=short_leg,
                    long_leg=long_leg,
                    entry_credit=credit,
                    max_profit=max_profit,
                    actual_width=actual_width,
                    entry_date=self.time,
                    expiry=expiry,
                    spot_at_entry=spot,
                )
                sd.active_positions.append(pos)

        return True

    # =========================================================================
    # POSITION MONITORING (on_data, daily)
    # =========================================================================

    def on_data(self, data):
        """Update IV, check entries, and monitor shadow positions."""
        if self.is_warming_up:
            return

        # Run entry check once per day when data arrives
        today = self.time.date()
        run_entry = (self.last_process_date != today)
        if run_entry:
            self.last_process_date = today

        for ticker, sd in self.symbol_data.items():
            spot = self.securities[ticker].price
            if spot <= 0:
                continue

            if run_entry:
                self._update_iv_history(ticker, sd, data)
                self._check_entry(ticker, sd, data)

            self._monitor_positions(ticker, sd, spot, data)

    def _monitor_positions(self, ticker: str, sd: SymbolData,
                           spot: float, data):
        """Check all active positions for a ticker."""

        positions_to_keep = []

        for pos in sd.active_positions:
            if pos.is_closed:
                # Already closed, move to completed
                self._record_completed(ticker, pos)
                continue

            # Check expiration
            days_to_exp = (pos.expiry - self.time).days
            if days_to_exp <= 0 or self.time.date() >= pos.expiry.date():
                self._close_at_expiry(ticker, pos, spot)
                self._record_completed(ticker, pos)
                continue

            # Get current market prices for the legs
            short_sec = self.securities.get(pos.short_leg.symbol)
            long_sec = self.securities.get(pos.long_leg.symbol)

            if short_sec is None or long_sec is None:
                positions_to_keep.append(pos)
                continue

            short_ask = short_sec.ask_price
            long_bid = long_sec.bid_price

            # Need valid short ask to calculate cost to close
            if short_ask <= 0:
                positions_to_keep.append(pos)
                continue

            if long_bid < 0:
                long_bid = 0

            # Cost to close: buy back short at ask, sell long at bid
            cost_to_close = short_ask - long_bid
            if cost_to_close < 0:
                cost_to_close = 0

            # Current P&L = (entry_credit - cost_to_close) * 100
            current_pnl = (pos.entry_credit - cost_to_close) * 100

            # Track peak and trough
            if current_pnl > pos.peak_pnl:
                pos.peak_pnl = current_pnl
            if current_pnl < pos.trough_pnl:
                pos.trough_pnl = current_pnl

            # Take profit check
            profit_target = pos.max_profit * pos.tp_pct
            if current_pnl >= profit_target:
                pos.is_closed = True
                pos.exit_reason = f"TP_{pos.tp_pct:.0%}"
                pos.exit_pnl = current_pnl
                pos.exit_date = self.time
                self._record_completed(ticker, pos)
                continue

            # Stop loss check: loss exceeds sl_mult * credit * 100
            loss_limit = pos.entry_credit * pos.sl_mult * 100
            if current_pnl <= -loss_limit:
                pos.is_closed = True
                pos.exit_reason = f"SL_{pos.sl_mult:.0f}x"
                pos.exit_pnl = current_pnl
                pos.exit_date = self.time
                self._record_completed(ticker, pos)
                continue

            positions_to_keep.append(pos)

        sd.active_positions = positions_to_keep

    def _close_at_expiry(self, ticker: str, pos: ShadowPosition, spot: float):
        """Close position at expiration using intrinsic value."""

        # Try market prices first
        short_sec = self.securities.get(pos.short_leg.symbol)
        long_sec = self.securities.get(pos.long_leg.symbol)

        used_market = False
        if short_sec is not None and long_sec is not None:
            short_ask = short_sec.ask_price
            long_bid = long_sec.bid_price
            if short_ask > 0:
                if long_bid < 0:
                    long_bid = 0
                cost_to_close = short_ask - long_bid
                if cost_to_close < 0:
                    cost_to_close = 0
                pos.exit_pnl = (pos.entry_credit - cost_to_close) * 100
                used_market = True

        if not used_market:
            # Intrinsic value fallback
            if pos.strategy == "PUT":
                short_intrinsic = max(0, pos.short_leg.strike - spot)
                long_intrinsic = max(0, pos.long_leg.strike - spot)
            else:  # CALL
                short_intrinsic = max(0, spot - pos.short_leg.strike)
                long_intrinsic = max(0, spot - pos.long_leg.strike)

            net_intrinsic = short_intrinsic - long_intrinsic
            pos.exit_pnl = (pos.entry_credit - net_intrinsic) * 100

        pos.is_closed = True
        pos.exit_reason = "EXPIRY"
        pos.exit_date = self.time

        # Track peak/trough at exit
        if pos.exit_pnl > pos.peak_pnl:
            pos.peak_pnl = pos.exit_pnl
        if pos.exit_pnl < pos.trough_pnl:
            pos.trough_pnl = pos.exit_pnl

    def _record_completed(self, ticker: str, pos: ShadowPosition):
        """Store completed position as a lightweight dict."""
        self.completed_trades.append({
            'ticker': ticker,
            'strategy': pos.strategy,
            'width': pos.width,
            'tp_pct': pos.tp_pct,
            'sl_mult': pos.sl_mult,
            'entry_date': pos.entry_date,
            'exit_date': pos.exit_date,
            'expiry': pos.expiry,
            'entry_credit': pos.entry_credit,
            'actual_width': pos.actual_width,
            'short_strike': pos.short_leg.strike,
            'long_strike': pos.long_leg.strike,
            'spot_at_entry': pos.spot_at_entry,
            'exit_reason': pos.exit_reason,
            'exit_pnl': pos.exit_pnl,
            'peak_pnl': pos.peak_pnl,
            'trough_pnl': pos.trough_pnl,
        })

    # =========================================================================
    # END-OF-ALGORITHM REPORTING
    # =========================================================================

    def on_end_of_algorithm(self):
        """Generate comprehensive report across all tickers and combos."""

        # Close any remaining active positions
        for ticker, sd in self.symbol_data.items():
            spot = self.securities[ticker].price
            for pos in sd.active_positions:
                if not pos.is_closed:
                    self._close_at_expiry(ticker, pos, spot)
                    self._record_completed(ticker, pos)
            sd.active_positions = []

        trades = self.completed_trades
        total_trades = len(trades)

        self.debug("")
        self.debug("=" * 80)
        self.debug("MULTI-TICKER SPREAD TEST - FINAL REPORT")
        self.debug("=" * 80)
        self.debug(f"Period: {self.start_date.date()} to {self.end_date.date()}")
        self.debug(f"Entry Signals: {self.entry_signal_count}")
        self.debug(f"Total Shadow Trades: {total_trades}")
        self.debug("")

        if total_trades == 0:
            self.debug("NO TRADES RECORDED")
            return

        # ----- SECTION 1: By Strategy Type -----
        self.debug("-" * 80)
        self.debug("RESULTS BY STRATEGY TYPE")
        self.debug("-" * 80)
        for strat in ["PUT", "CALL"]:
            strat_trades = [t for t in trades if t['strategy'] == strat]
            if strat_trades:
                self._print_stats(f"{strat} Spread", strat_trades)

        # ----- SECTION 2: By Ticker (averaged across combos) -----
        self.debug("-" * 80)
        self.debug("RESULTS BY TICKER")
        self.debug("-" * 80)
        self.debug(f"{'Ticker':<8} {'Trades':<8} {'Win%':<8} {'Total P&L':<12} {'Avg P&L':<10} {'Avg Win':<10} {'Avg Loss':<10}")
        self.debug("-" * 80)

        ticker_summaries = {}
        for ticker in TICKERS:
            ticker_trades = [t for t in trades if t['ticker'] == ticker]
            if not ticker_trades:
                self.debug(f"{ticker:<8} {'0':<8} {'N/A':<8}")
                continue
            stats = self._calc_stats(ticker_trades)
            ticker_summaries[ticker] = stats
            self.debug(f"{ticker:<8} {stats['count']:<8} {stats['win_rate']:<8.1%} "
                       f"${stats['total_pnl']:<11,.0f} ${stats['avg_pnl']:<9,.2f} "
                       f"${stats['avg_win']:<9,.2f} ${stats['avg_loss']:<9,.2f}")

        # ----- SECTION 3: Top 10 Overall Combinations by Total P&L -----
        self.debug("")
        self.debug("-" * 80)
        self.debug("TOP 10 COMBINATIONS BY TOTAL P&L")
        self.debug("-" * 80)

        combo_results = {}
        for t in trades:
            key = (t['strategy'], t['width'], t['tp_pct'], t['sl_mult'])
            if key not in combo_results:
                combo_results[key] = []
            combo_results[key].append(t['exit_pnl'])

        combo_stats = []
        for key, pnls in combo_results.items():
            strat, width, tp, sl = key
            count = len(pnls)
            wins = [p for p in pnls if p > 0]
            losses = [p for p in pnls if p <= 0]
            total = sum(pnls)
            combo_stats.append({
                'strategy': strat,
                'width': width,
                'tp_pct': tp,
                'sl_mult': sl,
                'count': count,
                'win_rate': len(wins) / count if count > 0 else 0,
                'total_pnl': total,
                'avg_pnl': total / count if count > 0 else 0,
                'avg_win': sum(wins) / len(wins) if wins else 0,
                'avg_loss': sum(losses) / len(losses) if losses else 0,
            })

        top_10 = sorted(combo_stats, key=lambda x: x['total_pnl'], reverse=True)[:10]
        for i, r in enumerate(top_10, 1):
            self.debug(f"  {i:>2}. {r['strategy']} ${r['width']} TP={r['tp_pct']:.0%} "
                       f"SL={r['sl_mult']:.0f}x | {r['count']} trades | "
                       f"Win {r['win_rate']:.1%} | Total ${r['total_pnl']:,.0f} | "
                       f"Avg ${r['avg_pnl']:.2f}")

        # ----- SECTION 4: Best Combination per Ticker -----
        self.debug("")
        self.debug("-" * 80)
        self.debug("BEST COMBINATION PER TICKER (by Total P&L)")
        self.debug("-" * 80)

        for ticker in TICKERS:
            ticker_trades = [t for t in trades if t['ticker'] == ticker]
            if not ticker_trades:
                continue

            ticker_combos = {}
            for t in ticker_trades:
                key = (t['strategy'], t['width'], t['tp_pct'], t['sl_mult'])
                if key not in ticker_combos:
                    ticker_combos[key] = []
                ticker_combos[key].append(t['exit_pnl'])

            best_key = max(ticker_combos.keys(),
                          key=lambda k: sum(ticker_combos[k]))
            best_pnls = ticker_combos[best_key]
            strat, width, tp, sl = best_key
            total = sum(best_pnls)
            count = len(best_pnls)
            wins = len([p for p in best_pnls if p > 0])
            self.debug(f"  {ticker:<6} {strat} ${width} TP={tp:.0%} SL={sl:.0f}x | "
                       f"{count} trades | Win {wins/count:.1%} | Total ${total:,.0f}")

        # ----- SECTION 5: Overall Summary Stats -----
        self.debug("")
        self.debug("-" * 80)
        self.debug("OVERALL SUMMARY")
        self.debug("-" * 80)
        all_pnls = [t['exit_pnl'] for t in trades]
        all_wins = [p for p in all_pnls if p > 0]
        all_losses = [p for p in all_pnls if p <= 0]
        self.debug(f"  Total Shadow Trades: {len(all_pnls)}")
        self.debug(f"  Win Rate: {len(all_wins)/len(all_pnls):.1%}")
        self.debug(f"  Total P&L (all combos): ${sum(all_pnls):,.0f}")
        self.debug(f"  Avg P&L: ${sum(all_pnls)/len(all_pnls):.2f}")
        if all_wins:
            self.debug(f"  Avg Win: ${sum(all_wins)/len(all_wins):.2f}")
        if all_losses:
            self.debug(f"  Avg Loss: ${sum(all_losses)/len(all_losses):.2f}")

        # Max drawdown across all trades (sequential)
        sorted_trades = sorted(trades, key=lambda t: (t['exit_date'] or t['entry_date']))
        cumulative = 0
        peak = 0
        max_dd = 0
        for t in sorted_trades:
            cumulative += t['exit_pnl']
            if cumulative > peak:
                peak = cumulative
            dd = peak - cumulative
            if dd > max_dd:
                max_dd = dd
        self.debug(f"  Max Drawdown (sequential): ${max_dd:,.0f}")

        # Exit reason breakdown
        reasons = {}
        for t in trades:
            r = t['exit_reason']
            if r not in reasons:
                reasons[r] = 0
            reasons[r] += 1
        self.debug(f"  Exit Reasons: {reasons}")

        self.debug("")
        self.debug("=" * 80)

    # =========================================================================
    # REPORTING HELPERS
    # =========================================================================

    def _calc_stats(self, trade_list: list) -> dict:
        """Calculate stats for a list of trade dicts."""
        pnls = [t['exit_pnl'] for t in trade_list]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]
        count = len(pnls)
        return {
            'count': count,
            'win_rate': len(wins) / count if count > 0 else 0,
            'total_pnl': sum(pnls),
            'avg_pnl': sum(pnls) / count if count > 0 else 0,
            'avg_win': sum(wins) / len(wins) if wins else 0,
            'avg_loss': sum(losses) / len(losses) if losses else 0,
        }

    def _print_stats(self, label: str, trade_list: list):
        """Print formatted stats for a group of trades."""
        s = self._calc_stats(trade_list)
        self.debug(f"  {label}: {s['count']} trades | Win {s['win_rate']:.1%} | "
                   f"Total ${s['total_pnl']:,.0f} | Avg ${s['avg_pnl']:.2f} | "
                   f"AvgW ${s['avg_win']:.2f} | AvgL ${s['avg_loss']:.2f}")
