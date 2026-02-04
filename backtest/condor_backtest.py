"""
Iron Condor Backtester
======================
Simulates iron condor entries across historical data using
Black-Scholes theoretical pricing.

Implements the options_scanner_claude.py entry logic:
- Dynamic short delta based on IV Rank (20/25/30 delta)
- Symmetric iron condor structure
- Full premium capture if price stays between short strikes
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Dict, Tuple

from .black_scholes import (
    find_strike_for_delta,
    round_strike_to_standard,
    calculate_condor_price,
    calculate_condor_price_realistic,
    price_condor_to_close,
    estimate_pop_lognormal,
    calculate_iv_rank,
)
from .ibkr_data_fetcher import SymbolData, DailyBar, IVDataPoint

logger = logging.getLogger(__name__)


# =============================================================================
# CONFIGURATION
# =============================================================================

# IV Rank thresholds for delta selection
IV_RANK_LOW = 0.30   # Below this: don't trade (not enough premium)
IV_RANK_MED = 0.50   # Below this: use 20-delta
IV_RANK_HIGH = 0.70  # Below this: use 25-delta, above: use 30-delta

# Delta mappings
DELTA_BY_IV_TIER = {
    'low': None,      # Don't trade
    'medium': 0.20,   # Conservative
    'high': 0.25,     # Standard
    'very_high': 0.30 # Aggressive (wider profit zone)
}

# Wing width (how far OTM the long strikes are from shorts)
WING_WIDTH_PCT = 0.03  # 3% of spot for wing protection

DTE = 30  # Days to expiration
RISK_FREE_RATE = 0.05
SLIPPAGE_PER_LEG = 0.02

# Realistic pricing settings
BID_ASK_SPREAD_PCT = 0.05  # 5% bid-ask spread
USE_VOLATILITY_SKEW = True
SKEW_SLOPE_PUT = 0.0015  # IV increases 0.15% per 1% OTM for puts
SKEW_SLOPE_CALL = 0.0008  # IV increases 0.08% per 1% OTM for calls

# Early exit settings
TAKE_PROFIT_PCT = 0.50  # Close at 50% of max profit
STOP_LOSS_PCT = 0.75    # Close at 75% of max loss


# =============================================================================
# DATA STRUCTURES
# =============================================================================

@dataclass
class CondorTrade:
    """Record of a single iron condor trade."""
    symbol: str
    entry_date: str
    expiration_date: str
    spot_price: float

    # Structure
    long_put_strike: float
    short_put_strike: float
    short_call_strike: float
    long_call_strike: float

    credit: float  # Per share
    max_loss: float  # Per contract
    theoretical_pop: float

    # Entry conditions
    iv_rank: float
    iv_at_entry: float
    short_delta_used: float
    iv_tier: str

    # Outcome
    exit_price: Optional[float] = None
    pnl: Optional[float] = None
    won: Optional[bool] = None
    reason: str = ""
    side_breached: Optional[str] = None  # 'put', 'call', or None


@dataclass
class CondorBacktestResult:
    """Aggregated results of iron condor backtest."""
    symbol: str
    total_trades: int
    winning_trades: int
    losing_trades: int

    total_pnl: float
    avg_pnl_per_trade: float
    win_rate: float

    avg_credit: float
    avg_max_loss: float
    avg_theoretical_pop: float
    realized_pop: float

    # Breach analysis
    put_breaches: int
    call_breaches: int

    # By IV tier
    trades_by_iv_tier: Dict[str, int] = field(default_factory=dict)
    pnl_by_iv_tier: Dict[str, float] = field(default_factory=dict)
    winrate_by_iv_tier: Dict[str, float] = field(default_factory=dict)

    trades: List[CondorTrade] = field(default_factory=list)


# =============================================================================
# DELTA SELECTION
# =============================================================================

def select_short_delta(iv_rank: float) -> Tuple[Optional[float], str]:
    """
    Select short strike delta based on IV Rank.

    Higher IV Rank = wider strikes = higher probability of profit.

    Returns:
        Tuple of (delta, tier_name) or (None, tier_name) if shouldn't trade
    """
    if iv_rank < IV_RANK_LOW:
        return None, 'low'
    elif iv_rank < IV_RANK_MED:
        return DELTA_BY_IV_TIER['medium'], 'medium'
    elif iv_rank < IV_RANK_HIGH:
        return DELTA_BY_IV_TIER['high'], 'high'
    else:
        return DELTA_BY_IV_TIER['very_high'], 'very_high'


# =============================================================================
# CONDOR CONSTRUCTION
# =============================================================================

def construct_condor(
    spot: float,
    iv: float,
    dte_years: float,
    short_delta: float,
    wing_width_pct: float = WING_WIDTH_PCT,
    rate: float = RISK_FREE_RATE,
    slippage: float = SLIPPAGE_PER_LEG,
    use_realistic_pricing: bool = False,
    bid_ask_spread_pct: float = BID_ASK_SPREAD_PCT,
    use_skew: bool = USE_VOLATILITY_SKEW,
) -> Optional[Dict]:
    """
    Construct a symmetric iron condor at target delta.

    Returns:
        Dict with condor details, or None if construction fails
    """
    # Find short put strike (negative delta)
    short_put_raw = find_strike_for_delta(
        spot, dte_years, rate, iv, -short_delta, 'P'
    )
    if short_put_raw is None:
        return None

    # Find short call strike (positive delta)
    short_call_raw = find_strike_for_delta(
        spot, dte_years, rate, iv, short_delta, 'C'
    )
    if short_call_raw is None:
        return None

    # Round to standard strikes
    short_put = round_strike_to_standard(short_put_raw, spot)
    short_call = round_strike_to_standard(short_call_raw, spot)

    # Calculate wing strikes (further OTM for protection)
    wing_width = spot * wing_width_pct
    step = 5.0 if spot >= 100 else 2.5 if spot >= 50 else 1.0
    wing_width = max(wing_width, step)  # At least one strike step

    long_put_raw = short_put - wing_width
    long_call_raw = short_call + wing_width

    long_put = round_strike_to_standard(long_put_raw, spot)
    long_call = round_strike_to_standard(long_call_raw, spot)

    # Ensure proper structure
    if long_put >= short_put:
        long_put = short_put - step
    if long_call <= short_call:
        long_call = short_call + step

    # Price the condor
    if use_realistic_pricing:
        pricing = calculate_condor_price_realistic(
            spot, long_put, short_put, short_call, long_call,
            dte_years, rate, iv,
            bid_ask_spread_pct=bid_ask_spread_pct,
            use_skew=use_skew,
        )
        if pricing is None:
            return None

        # Estimate POP (probability of staying between short strikes)
        pop = estimate_pop_lognormal(spot, short_put, short_call, iv, dte_years)

        return {
            'long_put': long_put,
            'short_put': short_put,
            'short_call': short_call,
            'long_call': long_call,
            'credit_mid': pricing['credit_mid'],
            'credit': pricing['open_credit'],
            'max_loss': pricing['max_loss'],
            'put_width': pricing['put_width'],
            'call_width': pricing['call_width'],
            'net_delta': 0.0,  # Simplified
            'pop': pop or 0.65,
        }
    else:
        pricing = calculate_condor_price(
            spot, long_put, short_put, short_call, long_call,
            dte_years, rate, iv, slippage
        )
        if pricing is None:
            return None

        # Estimate POP (probability of staying between short strikes)
        pop = estimate_pop_lognormal(spot, short_put, short_call, iv, dte_years)

        return {
            'long_put': long_put,
            'short_put': short_put,
            'short_call': short_call,
            'long_call': long_call,
            'credit_mid': pricing['credit_mid'],
            'credit': pricing['credit_conservative'],
            'max_loss': pricing['max_loss'],
            'put_width': pricing['put_width'],
            'call_width': pricing['call_width'],
            'net_delta': pricing['net_delta'],
            'pop': pop or 0.65,
        }


# =============================================================================
# OUTCOME EVALUATION
# =============================================================================

def evaluate_condor_with_early_exit(
    trade: CondorTrade,
    price_bars: List[DailyBar],
    iv_data: List[IVDataPoint],
    entry_idx: int,
    dte_days: int = DTE,
    take_profit_pct: float = TAKE_PROFIT_PCT,
    stop_loss_pct: float = STOP_LOSS_PCT,
    rate: float = RISK_FREE_RATE,
    use_realistic_pricing: bool = True,
    bid_ask_spread_pct: float = BID_ASK_SPREAD_PCT,
    use_skew: bool = USE_VOLATILITY_SKEW,
) -> CondorTrade:
    """
    Evaluate condor with early exit management.

    Checks each day for:
    - Take profit: Close if can lock in X% of max profit
    - Stop loss: Close if loss reaches Y% of max loss
    """
    trade = CondorTrade(**trade.__dict__)

    open_credit = trade.credit
    max_profit = open_credit * 100  # Per contract
    max_loss = trade.max_loss

    take_profit_target = open_credit * take_profit_pct
    stop_loss_target = max_loss * stop_loss_pct

    # Build IV lookup
    iv_by_date = {iv.date: iv.iv for iv in iv_data}

    # Walk through each day
    for day_offset in range(1, dte_days + 1):
        check_idx = entry_idx + day_offset
        if check_idx >= len(price_bars):
            break

        bar = price_bars[check_idx]
        spot = bar.close
        days_remaining = dte_days - day_offset
        t_years = max(days_remaining / 365.0, 1/365.0)

        # Get IV for this day
        current_iv = iv_by_date.get(bar.date)
        if current_iv is None:
            # Use entry IV as fallback
            current_iv = trade.iv_at_entry

        # Price the condor to close
        close_cost = price_condor_to_close(
            spot,
            trade.long_put_strike,
            trade.short_put_strike,
            trade.short_call_strike,
            trade.long_call_strike,
            t_years,
            rate,
            current_iv,
            bid_ask_spread_pct=bid_ask_spread_pct if use_realistic_pricing else 0.001,
            use_skew=use_skew if use_realistic_pricing else False,
        )

        if close_cost is None:
            continue

        # P&L if we close now
        pnl = (open_credit - close_cost) * 100

        # Check take profit
        if close_cost <= take_profit_target:
            trade.exit_price = spot
            trade.pnl = pnl
            trade.won = True
            trade.reason = f"TP hit day {day_offset}"
            trade.side_breached = None
            return trade

        # Check stop loss
        if pnl <= -stop_loss_target:
            trade.exit_price = spot
            trade.pnl = pnl
            trade.won = False
            # Determine which side caused the loss
            if spot < trade.short_put_strike:
                trade.side_breached = 'put'
            elif spot > trade.short_call_strike:
                trade.side_breached = 'call'
            else:
                trade.side_breached = None
            trade.reason = f"SL hit day {day_offset}"
            return trade

    # Hold to expiration
    exp_idx = entry_idx + dte_days
    if exp_idx < len(price_bars):
        exp_price = price_bars[exp_idx].close
        return evaluate_condor_outcome(trade, exp_price)

    # Fallback
    trade.pnl = 0
    trade.won = False
    trade.reason = "No data at expiration"
    return trade


def evaluate_condor_outcome(
    trade: CondorTrade,
    price_at_expiration: float,
) -> CondorTrade:
    """
    Evaluate P&L of an iron condor at expiration.

    Simplified model:
    - Full profit if price between short strikes
    - Full loss if price outside long strikes
    - Partial loss if between short and long on either side
    """
    trade = CondorTrade(**trade.__dict__)  # Copy
    trade.exit_price = price_at_expiration

    short_put = trade.short_put_strike
    short_call = trade.short_call_strike
    long_put = trade.long_put_strike
    long_call = trade.long_call_strike
    credit = trade.credit

    price = price_at_expiration

    # Case 1: Full profit (between short strikes)
    if short_put <= price <= short_call:
        trade.pnl = credit * 100
        trade.won = True
        trade.reason = "Expired between short strikes"
        trade.side_breached = None

    # Case 2: Put side breached
    elif price < short_put:
        if price <= long_put:
            # Full loss on put side
            put_width = short_put - long_put
            trade.pnl = -(put_width - credit) * 100
            trade.reason = "Put side max loss"
        else:
            # Partial loss on put side
            intrinsic = short_put - price
            trade.pnl = (credit - intrinsic) * 100
            trade.reason = "Put side partial loss"
        trade.won = trade.pnl > 0
        trade.side_breached = 'put'

    # Case 3: Call side breached
    else:  # price > short_call
        if price >= long_call:
            # Full loss on call side
            call_width = long_call - short_call
            trade.pnl = -(call_width - credit) * 100
            trade.reason = "Call side max loss"
        else:
            # Partial loss on call side
            intrinsic = price - short_call
            trade.pnl = (credit - intrinsic) * 100
            trade.reason = "Call side partial loss"
        trade.won = trade.pnl > 0
        trade.side_breached = 'call'

    return trade


# =============================================================================
# MAIN BACKTEST LOGIC
# =============================================================================

def simulate_condor_entry(
    symbol: str,
    date_idx: int,
    price_bars: List[DailyBar],
    iv_data: List[IVDataPoint],
    dte_days: int = DTE,
    use_realistic_pricing: bool = False,
    bid_ask_spread_pct: float = BID_ASK_SPREAD_PCT,
    use_skew: bool = USE_VOLATILITY_SKEW,
) -> Optional[CondorTrade]:
    """
    Check if a specific date would trigger a condor entry.

    Returns:
        CondorTrade if entry triggers, None otherwise
    """
    if date_idx < 200:  # Need ~200 days for IV rank calculation
        return None

    if date_idx + dte_days >= len(price_bars):
        return None

    current_bar = price_bars[date_idx]
    spot = current_bar.close
    entry_date_str = current_bar.date

    # Get IV data up to entry date
    iv_history = []
    current_iv = None
    for iv_point in iv_data:
        if iv_point.date <= entry_date_str:
            iv_history.append(iv_point.iv)
            current_iv = iv_point.iv

    if current_iv is None or len(iv_history) < 50:
        return None

    # Calculate IV rank
    iv_rank = calculate_iv_rank(current_iv, iv_history)
    if iv_rank is None:
        return None

    # Select delta based on IV rank
    short_delta, iv_tier = select_short_delta(iv_rank)

    if short_delta is None:
        return None  # IV too low to trade

    # Construct condor
    dte_years = dte_days / 365.0
    condor = construct_condor(
        spot, current_iv, dte_years, short_delta,
        use_realistic_pricing=use_realistic_pricing,
        bid_ask_spread_pct=bid_ask_spread_pct,
        use_skew=use_skew,
    )

    if condor is None:
        return None

    # Get expiration date
    exp_idx = date_idx + dte_days
    exp_date_str = price_bars[exp_idx].date

    return CondorTrade(
        symbol=symbol,
        entry_date=entry_date_str,
        expiration_date=exp_date_str,
        spot_price=spot,
        long_put_strike=condor['long_put'],
        short_put_strike=condor['short_put'],
        short_call_strike=condor['short_call'],
        long_call_strike=condor['long_call'],
        credit=condor['credit'],
        max_loss=condor['max_loss'],
        theoretical_pop=condor['pop'],
        iv_rank=iv_rank,
        iv_at_entry=current_iv,
        short_delta_used=short_delta,
        iv_tier=iv_tier,
    )


def run_condor_backtest(
    data: SymbolData,
    entry_interval_days: int = 5,
    use_early_exit: bool = False,
    take_profit_pct: float = TAKE_PROFIT_PCT,
    stop_loss_pct: float = STOP_LOSS_PCT,
    use_realistic_pricing: bool = False,
    bid_ask_spread_pct: float = BID_ASK_SPREAD_PCT,
    use_skew: bool = USE_VOLATILITY_SKEW,
) -> Optional[CondorBacktestResult]:
    """
    Run iron condor backtest on a single symbol.
    """
    symbol = data.symbol
    price_bars = data.price_bars
    iv_data = data.iv_data

    if len(price_bars) < 200 + DTE + 10:
        logger.warning(f"{symbol}: Insufficient price history for condor backtest")
        return None

    if len(iv_data) < 100:
        logger.warning(f"{symbol}: Insufficient IV history for condor backtest")
        return None

    trades: List[CondorTrade] = []
    last_entry_idx = -entry_interval_days

    # Walk through dates
    for date_idx in range(200, len(price_bars) - DTE - 1):
        if date_idx - last_entry_idx < entry_interval_days:
            continue

        trade = simulate_condor_entry(
            symbol, date_idx, price_bars, iv_data,
            use_realistic_pricing=use_realistic_pricing,
            bid_ask_spread_pct=bid_ask_spread_pct,
            use_skew=use_skew,
        )

        if trade is not None:
            if use_early_exit:
                trade = evaluate_condor_with_early_exit(
                    trade, price_bars, iv_data, date_idx,
                    take_profit_pct=take_profit_pct,
                    stop_loss_pct=stop_loss_pct,
                    use_realistic_pricing=use_realistic_pricing,
                    bid_ask_spread_pct=bid_ask_spread_pct,
                    use_skew=use_skew,
                )
            else:
                exp_idx = date_idx + DTE
                exp_price = price_bars[exp_idx].close
                trade = evaluate_condor_outcome(trade, exp_price)
            trades.append(trade)
            last_entry_idx = date_idx

    if not trades:
        logger.info(f"{symbol}: No condor trades triggered")
        return CondorBacktestResult(
            symbol=symbol,
            total_trades=0,
            winning_trades=0,
            losing_trades=0,
            total_pnl=0.0,
            avg_pnl_per_trade=0.0,
            win_rate=0.0,
            avg_credit=0.0,
            avg_max_loss=0.0,
            avg_theoretical_pop=0.0,
            realized_pop=0.0,
            put_breaches=0,
            call_breaches=0,
            trades=[],
        )

    # Aggregate
    winning = [t for t in trades if t.won]
    losing = [t for t in trades if not t.won]
    total_pnl = sum(t.pnl for t in trades)

    put_breaches = sum(1 for t in trades if t.side_breached == 'put')
    call_breaches = sum(1 for t in trades if t.side_breached == 'call')

    # By IV tier
    trades_by_tier = {}
    pnl_by_tier = {}
    winrate_by_tier = {}

    for tier in ['medium', 'high', 'very_high']:
        tier_trades = [t for t in trades if t.iv_tier == tier]
        if tier_trades:
            trades_by_tier[tier] = len(tier_trades)
            pnl_by_tier[tier] = sum(t.pnl for t in tier_trades)
            winrate_by_tier[tier] = sum(1 for t in tier_trades if t.won) / len(tier_trades)

    return CondorBacktestResult(
        symbol=symbol,
        total_trades=len(trades),
        winning_trades=len(winning),
        losing_trades=len(losing),
        total_pnl=total_pnl,
        avg_pnl_per_trade=total_pnl / len(trades),
        win_rate=len(winning) / len(trades),
        avg_credit=sum(t.credit for t in trades) / len(trades),
        avg_max_loss=sum(t.max_loss for t in trades) / len(trades),
        avg_theoretical_pop=sum(t.theoretical_pop for t in trades) / len(trades),
        realized_pop=len(winning) / len(trades),
        put_breaches=put_breaches,
        call_breaches=call_breaches,
        trades_by_iv_tier=trades_by_tier,
        pnl_by_iv_tier=pnl_by_tier,
        winrate_by_iv_tier=winrate_by_tier,
        trades=trades,
    )


def run_condor_backtest_multi(
    symbol_data: Dict[str, SymbolData],
    entry_interval_days: int = 5,
    use_early_exit: bool = False,
    take_profit_pct: float = TAKE_PROFIT_PCT,
    stop_loss_pct: float = STOP_LOSS_PCT,
    use_realistic_pricing: bool = False,
    bid_ask_spread_pct: float = BID_ASK_SPREAD_PCT,
    use_skew: bool = USE_VOLATILITY_SKEW,
) -> Dict[str, CondorBacktestResult]:
    """
    Run iron condor backtest on multiple symbols.
    """
    results = {}

    for symbol, data in symbol_data.items():
        logger.info(f"Backtesting condors for {symbol}...")
        result = run_condor_backtest(
            data, entry_interval_days,
            use_early_exit=use_early_exit,
            take_profit_pct=take_profit_pct,
            stop_loss_pct=stop_loss_pct,
            use_realistic_pricing=use_realistic_pricing,
            bid_ask_spread_pct=bid_ask_spread_pct,
            use_skew=use_skew,
        )
        if result is not None:
            results[symbol] = result

    return results


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":
    import random

    print("Iron Condor Backtest - Synthetic Test")
    print("=" * 50)

    # Create synthetic price data (rangebound with some breakouts)
    base_price = 100.0
    prices = []
    for i in range(400):
        # Mean-reverting with occasional breaks
        cycle = 5 * math.sin(i / 30)  # Cyclical
        noise = random.gauss(0, 2)
        price = base_price + cycle + noise
        prices.append(DailyBar(
            date=f"2023-{((i // 30) % 12) + 1:02d}-{(i % 30) + 1:02d}",
            open=price - 0.5,
            high=price + 1,
            low=price - 1,
            close=price,
            volume=1000000,
        ))

    # Create synthetic IV data with varying IV rank
    import math
    iv_data = []
    for i, bar in enumerate(prices):
        # IV cycles between 0.15 and 0.45
        base_iv = 0.30
        iv_cycle = 0.15 * math.sin(i / 60)
        iv = max(0.10, base_iv + iv_cycle + 0.03 * random.random())
        iv_data.append(IVDataPoint(date=bar.date, iv=iv))

    data = SymbolData(
        symbol="TEST",
        fetch_timestamp="2024-01-01T00:00:00Z",
        price_bars=prices,
        iv_data=iv_data,
    )

    result = run_condor_backtest(data, entry_interval_days=10)

    if result:
        print(f"\nResults for {result.symbol}:")
        print(f"  Total trades: {result.total_trades}")
        print(f"  Winning: {result.winning_trades}")
        print(f"  Losing: {result.losing_trades}")
        print(f"  Win rate: {result.win_rate:.1%}")
        print(f"  Total P&L: ${result.total_pnl:.2f}")
        print(f"  Avg P&L/trade: ${result.avg_pnl_per_trade:.2f}")
        print(f"  Theoretical POP: {result.avg_theoretical_pop:.1%}")
        print(f"  Realized POP: {result.realized_pop:.1%}")
        print(f"\n  Put breaches: {result.put_breaches}")
        print(f"  Call breaches: {result.call_breaches}")
        print(f"\n  Trades by IV tier: {result.trades_by_iv_tier}")
        print(f"  Win rate by IV tier: {result.winrate_by_iv_tier}")
