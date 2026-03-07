"""
Put Credit Spread Backtester
============================
Simulates put credit spread entries across historical data using
Black-Scholes theoretical pricing.

Implements the yield_hunter.py entry logic:
- Price > 200 SMA (uptrend filter)
- RSI < 75 (not overbought)
- IV Rank > threshold (elevated premium)
- 25-delta short put with defined risk
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Dict, Tuple

from .black_scholes import (
    find_strike_for_delta,
    round_strike_to_standard,
    calculate_spread_price,
    calculate_spread_price_realistic,
    price_spread_to_close,
    apply_put_skew,
    estimate_prob_above,
    calculate_sma,
    calculate_rsi,
    calculate_iv_rank,
)
from .ibkr_data_fetcher import SymbolData, DailyBar, IVDataPoint

logger = logging.getLogger(__name__)


# =============================================================================
# CONFIGURATION
# =============================================================================

# Entry filters
SMA_PERIOD = 200
RSI_PERIOD = 14
RSI_MAX = 75.0  # Don't enter if RSI above this
IV_RANK_MIN = 0.30  # Minimum IV rank to enter (30%)

# Spread construction
SHORT_DELTA = -0.25  # 25-delta put
SPREAD_WIDTH_PCT = 0.05  # Long strike ~5% below short strike (OTM protection)
DTE = 30  # Days to expiration (target)
RISK_FREE_RATE = 0.05  # 5% risk-free rate assumption

# Slippage / Bid-Ask (OLD - kept for reference)
SLIPPAGE_PER_LEG = 0.02  # $0.02 per leg (old method)

# Realistic market assumptions
BID_ASK_SPREAD_PCT = 0.05  # 5% bid-ask spread
USE_VOLATILITY_SKEW = True  # Apply IV skew to OTM puts
SKEW_SLOPE = 0.0015  # IV increases 0.15% per 1% OTM


# =============================================================================
# DATA STRUCTURES
# =============================================================================

@dataclass
class PutSpreadTrade:
    """Record of a single put credit spread trade."""
    symbol: str
    entry_date: str
    expiration_date: str
    spot_price: float
    short_strike: float
    long_strike: float
    credit: float  # Per share
    max_loss: float  # Per contract (100 shares)
    theoretical_pop: float  # Probability of profit

    # Entry conditions
    sma_200: float
    rsi: float
    iv_rank: float
    iv_at_entry: float

    # Outcome (filled after expiration)
    exit_price: Optional[float] = None
    pnl: Optional[float] = None  # Per contract
    won: Optional[bool] = None
    reason: str = ""  # Why trade won/lost


@dataclass
class PutSpreadBacktestResult:
    """Aggregated results of put spread backtest."""
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
    realized_pop: float  # Actual win rate

    # By IV rank bucket
    trades_by_iv_bucket: Dict[str, int] = field(default_factory=dict)
    pnl_by_iv_bucket: Dict[str, float] = field(default_factory=dict)

    # All trades for detailed analysis
    trades: List[PutSpreadTrade] = field(default_factory=list)


# =============================================================================
# ENTRY SIGNAL CHECKING
# =============================================================================

def check_entry_filters(
    prices: List[float],
    current_iv: float,
    iv_history: List[float],
    sma_period: int = SMA_PERIOD,
    rsi_period: int = RSI_PERIOD,
    rsi_max: float = RSI_MAX,
    iv_rank_min: float = IV_RANK_MIN,
) -> Tuple[bool, Dict]:
    """
    Check if entry filters pass for put credit spread.

    Returns:
        Tuple of (passes, details_dict)
    """
    details = {
        'sma': None,
        'rsi': None,
        'iv_rank': None,
        'price_above_sma': False,
        'rsi_below_max': False,
        'iv_rank_above_min': False,
    }

    if len(prices) < sma_period + 1:
        return False, details

    current_price = prices[-1]

    # Calculate SMA
    sma = sum(prices[-sma_period:]) / sma_period
    details['sma'] = sma
    details['price_above_sma'] = current_price > sma

    # Calculate RSI
    rsi = calculate_rsi(prices, rsi_period)
    details['rsi'] = rsi
    details['rsi_below_max'] = rsi is not None and rsi < rsi_max

    # Calculate IV Rank
    iv_rank = calculate_iv_rank(current_iv, iv_history)
    details['iv_rank'] = iv_rank
    details['iv_rank_above_min'] = iv_rank is not None and iv_rank >= iv_rank_min

    # All filters must pass
    passes = (
        details['price_above_sma'] and
        details['rsi_below_max'] and
        details['iv_rank_above_min']
    )

    return passes, details


# =============================================================================
# SPREAD CONSTRUCTION
# =============================================================================

def construct_put_spread(
    spot: float,
    iv: float,
    dte_years: float,
    short_delta: float = SHORT_DELTA,
    spread_width_pct: float = SPREAD_WIDTH_PCT,
    rate: float = RISK_FREE_RATE,
    use_realistic_pricing: bool = True,
    bid_ask_spread_pct: float = BID_ASK_SPREAD_PCT,
    use_skew: bool = USE_VOLATILITY_SKEW,
    skew_slope: float = SKEW_SLOPE,
) -> Optional[Dict]:
    """
    Construct a put credit spread at target delta.

    Args:
        use_realistic_pricing: If True, use bid/ask spread and IV skew
        bid_ask_spread_pct: Bid/ask spread as % of mid (default 5%)
        use_skew: Apply volatility skew to OTM puts
        skew_slope: IV increase per 1% OTM

    Returns:
        Dict with spread details, or None if construction fails
    """
    # Find short strike at target delta (using ATM IV for delta calc)
    short_strike_raw = find_strike_for_delta(
        spot, dte_years, rate, iv, short_delta, 'P'
    )
    if short_strike_raw is None:
        return None

    short_strike = round_strike_to_standard(short_strike_raw, spot)

    # Long strike below short (further OTM for protection)
    long_strike_raw = short_strike * (1 - spread_width_pct)
    long_strike = round_strike_to_standard(long_strike_raw, spot)

    # Ensure strikes are different
    if long_strike >= short_strike:
        # Adjust based on price level
        step = 5.0 if spot >= 100 else 2.5 if spot >= 50 else 1.0
        long_strike = short_strike - step

    if use_realistic_pricing:
        # Use new realistic pricing with bid/ask and skew
        pricing = calculate_spread_price_realistic(
            spot, short_strike, long_strike, dte_years, rate, iv, 'P',
            bid_ask_spread_pct=bid_ask_spread_pct,
            use_skew=use_skew,
            skew_slope=skew_slope,
        )
        if pricing is None:
            return None

        # Estimate POP using skewed IV for short strike
        short_iv = pricing['short_iv']
        pop = estimate_prob_above(spot, short_strike, short_iv, dte_years)

        return {
            'short_strike': short_strike,
            'long_strike': long_strike,
            'credit_mid': pricing['credit_mid'],
            'credit': pricing['open_credit'],  # Realistic credit after bid/ask
            'max_loss': pricing['max_loss'],
            'width': short_strike - long_strike,
            'pop': pop or 0.75,
            # Additional info for debugging
            'short_iv': pricing['short_iv'],
            'long_iv': pricing['long_iv'],
            'short_bid': pricing['short_bid'],
            'long_ask': pricing['long_ask'],
        }
    else:
        # Legacy pricing (for comparison)
        pricing = calculate_spread_price(
            spot, short_strike, long_strike, dte_years, rate, iv, 'P', SLIPPAGE_PER_LEG
        )
        if pricing is None:
            return None

        credit_mid, credit_conservative, max_loss = pricing
        pop = estimate_prob_above(spot, short_strike, iv, dte_years)

        return {
            'short_strike': short_strike,
            'long_strike': long_strike,
            'credit_mid': credit_mid,
            'credit': credit_conservative,
            'max_loss': max_loss,
            'width': short_strike - long_strike,
            'pop': pop or 0.75,
        }


# =============================================================================
# OUTCOME EVALUATION
# =============================================================================

def evaluate_spread_outcome(
    trade: PutSpreadTrade,
    price_at_expiration: float,
) -> PutSpreadTrade:
    """
    Evaluate P&L of a put credit spread at expiration.

    Simplified model:
    - Full profit if price >= short strike
    - Full loss if price <= long strike
    - Partial if between (linear interpolation)
    """
    trade = PutSpreadTrade(**trade.__dict__)  # Copy
    trade.exit_price = price_at_expiration

    short_strike = trade.short_strike
    long_strike = trade.long_strike
    credit = trade.credit
    width = short_strike - long_strike

    if price_at_expiration >= short_strike:
        # Full profit - spread expires worthless
        trade.pnl = credit * 100  # Per contract
        trade.won = True
        trade.reason = "Expired OTM"

    elif price_at_expiration <= long_strike:
        # Full loss - both legs ITM
        trade.pnl = -trade.max_loss
        trade.won = False
        trade.reason = "Expired deep ITM"

    else:
        # Partial loss - price between strikes
        intrinsic = short_strike - price_at_expiration
        trade.pnl = (credit - intrinsic) * 100
        trade.won = trade.pnl > 0
        trade.reason = "Expired between strikes"

    return trade


def evaluate_spread_with_early_exit(
    trade: PutSpreadTrade,
    price_bars: List[DailyBar],
    iv_data: List[IVDataPoint],
    entry_idx: int,
    dte_days: int,
    take_profit_pct: float = 0.75,  # Close at 75% of max profit
    stop_loss_pct: float = 0.50,    # Close at 50% of max loss
    use_realistic_pricing: bool = True,
    bid_ask_spread_pct: float = BID_ASK_SPREAD_PCT,
    use_skew: bool = USE_VOLATILITY_SKEW,
    skew_slope: float = SKEW_SLOPE,
) -> PutSpreadTrade:
    """
    Evaluate P&L with early exit rules using Black-Scholes repricing.

    Walks through daily prices from entry to expiration, checking for:
    - Take profit: P&L reaches target % of max profit
    - Stop loss: P&L reaches target % of max loss (negative)

    With realistic pricing:
    - Applies volatility skew to OTM puts
    - Uses 5% bid/ask spread
    - Closing: BUY short put at ASK, SELL long put at BID
    """
    trade = PutSpreadTrade(**trade.__dict__)  # Copy

    short_strike = trade.short_strike
    long_strike = trade.long_strike
    credit = trade.credit
    max_profit = credit * 100
    max_loss = trade.max_loss

    # Calculate thresholds
    profit_target = max_profit * take_profit_pct
    loss_limit = max_loss * stop_loss_pct

    # Build IV lookup by date for faster access
    iv_by_date = {iv_point.date: iv_point.iv for iv_point in iv_data}
    entry_iv = trade.iv_at_entry

    # Walk through each day from entry to expiration
    for day_offset in range(1, dte_days + 1):
        bar_idx = entry_idx + day_offset
        if bar_idx >= len(price_bars):
            break

        current_bar = price_bars[bar_idx]
        current_price = current_bar.close
        current_date = current_bar.date
        days_remaining = dte_days - day_offset

        # Get IV for current date (fall back to entry IV if not found)
        current_iv = iv_by_date.get(current_date, entry_iv)

        # Convert to years (minimum 1 day to avoid division issues)
        dte_years = max(days_remaining / 365.0, 1/365.0)

        if use_realistic_pricing:
            # Use realistic close pricing with bid/ask and skew
            close_cost = price_spread_to_close(
                current_price, short_strike, long_strike, dte_years,
                RISK_FREE_RATE, current_iv, 'P',
                bid_ask_spread_pct=bid_ask_spread_pct,
                use_skew=use_skew,
                skew_slope=skew_slope,
            )
            if close_cost is None:
                continue

            # P&L = credit received - cost to close
            current_pnl = (credit - close_cost) * 100
        else:
            # Legacy simple pricing
            from .black_scholes import black_scholes_price
            short_put_price = black_scholes_price(
                current_price, short_strike, dte_years, RISK_FREE_RATE, current_iv, 'P'
            )
            long_put_price = black_scholes_price(
                current_price, long_strike, dte_years, RISK_FREE_RATE, current_iv, 'P'
            )
            if short_put_price is None or long_put_price is None:
                continue
            spread_value = short_put_price - long_put_price
            current_pnl = (credit - spread_value - SLIPPAGE_PER_LEG * 2) * 100

        # Check take profit
        if current_pnl >= profit_target:
            trade.exit_price = current_price
            trade.pnl = current_pnl
            trade.won = True
            trade.reason = f"Take profit ({take_profit_pct:.0%}) on day {day_offset}"
            return trade

        # Check stop loss
        if current_pnl <= -loss_limit:
            trade.exit_price = current_price
            trade.pnl = current_pnl
            trade.won = False
            trade.reason = f"Stop loss ({stop_loss_pct:.0%}) on day {day_offset}"
            return trade

    # Reached expiration - evaluate final outcome at intrinsic value
    # At expiration, options trade at intrinsic with minimal spread
    exp_idx = entry_idx + dte_days
    exp_price = price_bars[exp_idx].close if exp_idx < len(price_bars) else price_bars[-1].close
    trade.exit_price = exp_price

    if exp_price >= short_strike:
        # Spread expires worthless - full profit
        trade.pnl = max_profit
        trade.won = True
        trade.reason = "Expired OTM"
    elif exp_price <= long_strike:
        # Max loss - both legs ITM
        trade.pnl = -max_loss
        trade.won = False
        trade.reason = "Expired deep ITM"
    else:
        # Between strikes - partial loss (at expiration, settle at intrinsic)
        intrinsic = short_strike - exp_price
        trade.pnl = (credit - intrinsic) * 100
        trade.won = trade.pnl > 0
        trade.reason = "Expired between strikes"

    return trade


# =============================================================================
# MAIN BACKTEST LOGIC
# =============================================================================

def simulate_entry_date(
    symbol: str,
    date_idx: int,
    price_bars: List[DailyBar],
    iv_data: List[IVDataPoint],
    dte_days: int = DTE,
    use_realistic_pricing: bool = True,
    bid_ask_spread_pct: float = BID_ASK_SPREAD_PCT,
    use_skew: bool = USE_VOLATILITY_SKEW,
    skew_slope: float = SKEW_SLOPE,
) -> Optional[PutSpreadTrade]:
    """
    Check if a specific date would trigger an entry and construct the trade.

    Args:
        symbol: Ticker symbol
        date_idx: Index into price_bars for the entry date
        price_bars: Full price history
        iv_data: Full IV history
        dte_days: Target days to expiration

    Returns:
        PutSpreadTrade if entry triggers, None otherwise
    """
    if date_idx < SMA_PERIOD + 1:
        return None  # Not enough history

    if date_idx + dte_days >= len(price_bars):
        return None  # Not enough forward data for expiration

    # Get data up to entry date
    prices_to_date = [bar.close for bar in price_bars[:date_idx + 1]]
    current_bar = price_bars[date_idx]
    spot = current_bar.close

    # Find matching IV data
    entry_date_str = current_bar.date

    # Build IV history up to entry date
    iv_history = []
    current_iv = None
    for iv_point in iv_data:
        if iv_point.date <= entry_date_str:
            iv_history.append(iv_point.iv)
            current_iv = iv_point.iv

    if current_iv is None or len(iv_history) < 20:
        return None  # Insufficient IV data

    # Check entry filters
    passes, details = check_entry_filters(
        prices_to_date, current_iv, iv_history
    )

    if not passes:
        return None

    # Construct the spread
    dte_years = dte_days / 365.0
    spread = construct_put_spread(
        spot, current_iv, dte_years,
        use_realistic_pricing=use_realistic_pricing,
        bid_ask_spread_pct=bid_ask_spread_pct,
        use_skew=use_skew,
        skew_slope=skew_slope,
    )

    if spread is None:
        return None

    # Calculate expiration date
    exp_idx = date_idx + dte_days
    exp_date_str = price_bars[exp_idx].date

    return PutSpreadTrade(
        symbol=symbol,
        entry_date=entry_date_str,
        expiration_date=exp_date_str,
        spot_price=spot,
        short_strike=spread['short_strike'],
        long_strike=spread['long_strike'],
        credit=spread['credit'],
        max_loss=spread['max_loss'],
        theoretical_pop=spread['pop'],
        sma_200=details['sma'],
        rsi=details['rsi'],
        iv_rank=details['iv_rank'],
        iv_at_entry=current_iv,
    )


def run_put_spread_backtest(
    data: SymbolData,
    entry_interval_days: int = 5,  # Don't enter every day, space out entries
    use_early_exit: bool = False,
    take_profit_pct: float = 0.75,
    stop_loss_pct: float = 0.50,
    use_realistic_pricing: bool = True,
    bid_ask_spread_pct: float = BID_ASK_SPREAD_PCT,
    use_skew: bool = USE_VOLATILITY_SKEW,
    skew_slope: float = SKEW_SLOPE,
) -> Optional[PutSpreadBacktestResult]:
    """
    Run put credit spread backtest on a single symbol.

    Args:
        data: SymbolData with price and IV history
        entry_interval_days: Minimum days between entries
        use_early_exit: If True, use take profit and stop loss rules
        take_profit_pct: Close at this % of max profit (e.g., 0.75 = 75%)
        stop_loss_pct: Close at this % of max loss (e.g., 0.50 = 50%)
        use_realistic_pricing: Use bid/ask spread and volatility skew
        bid_ask_spread_pct: Bid/ask spread as % of mid (default 5%)
        use_skew: Apply volatility skew to OTM puts
        skew_slope: IV increase per 1% OTM

    Returns:
        PutSpreadBacktestResult or None if insufficient data
    """
    symbol = data.symbol
    price_bars = data.price_bars
    iv_data = data.iv_data

    if len(price_bars) < SMA_PERIOD + DTE + 10:
        logger.warning(f"{symbol}: Insufficient price history")
        return None

    if len(iv_data) < 50:
        logger.warning(f"{symbol}: Insufficient IV history")
        return None

    trades: List[PutSpreadTrade] = []
    last_entry_idx = -entry_interval_days  # Allow first entry

    # Walk through dates, looking for entry signals
    for date_idx in range(SMA_PERIOD, len(price_bars) - DTE - 1):
        # Respect minimum interval between entries
        if date_idx - last_entry_idx < entry_interval_days:
            continue

        # Check for entry
        trade = simulate_entry_date(
            symbol, date_idx, price_bars, iv_data, DTE,
            use_realistic_pricing=use_realistic_pricing,
            bid_ask_spread_pct=bid_ask_spread_pct,
            use_skew=use_skew,
            skew_slope=skew_slope,
        )

        if trade is not None:
            # Evaluate outcome
            if use_early_exit:
                trade = evaluate_spread_with_early_exit(
                    trade, price_bars, iv_data, date_idx, DTE,
                    take_profit_pct=take_profit_pct,
                    stop_loss_pct=stop_loss_pct,
                    use_realistic_pricing=use_realistic_pricing,
                    bid_ask_spread_pct=bid_ask_spread_pct,
                    use_skew=use_skew,
                    skew_slope=skew_slope,
                )
            else:
                exp_idx = date_idx + DTE
                exp_price = price_bars[exp_idx].close
                trade = evaluate_spread_outcome(trade, exp_price)
            trades.append(trade)
            last_entry_idx = date_idx

    if not trades:
        logger.info(f"{symbol}: No trades triggered")
        return PutSpreadBacktestResult(
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
            trades=[],
        )

    # Aggregate results
    winning = [t for t in trades if t.won]
    losing = [t for t in trades if not t.won]
    total_pnl = sum(t.pnl for t in trades)

    # Calculate IV rank buckets
    iv_buckets = {'low': [], 'medium': [], 'high': []}
    for t in trades:
        if t.iv_rank < 0.4:
            iv_buckets['low'].append(t)
        elif t.iv_rank < 0.6:
            iv_buckets['medium'].append(t)
        else:
            iv_buckets['high'].append(t)

    trades_by_iv = {k: len(v) for k, v in iv_buckets.items()}
    pnl_by_iv = {k: sum(t.pnl for t in v) for k, v in iv_buckets.items()}

    return PutSpreadBacktestResult(
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
        trades_by_iv_bucket=trades_by_iv,
        pnl_by_iv_bucket=pnl_by_iv,
        trades=trades,
    )


def run_put_spread_backtest_multi(
    symbol_data: Dict[str, SymbolData],
    entry_interval_days: int = 5,
    use_early_exit: bool = False,
    take_profit_pct: float = 0.75,
    stop_loss_pct: float = 0.50,
    use_realistic_pricing: bool = True,
    bid_ask_spread_pct: float = BID_ASK_SPREAD_PCT,
    use_skew: bool = USE_VOLATILITY_SKEW,
    skew_slope: float = SKEW_SLOPE,
) -> Dict[str, PutSpreadBacktestResult]:
    """
    Run put credit spread backtest on multiple symbols.

    Returns:
        Dict mapping symbol to backtest result
    """
    results = {}

    for symbol, data in symbol_data.items():
        logger.info(f"Backtesting put spreads for {symbol}...")
        result = run_put_spread_backtest(
            data, entry_interval_days,
            use_early_exit=use_early_exit,
            take_profit_pct=take_profit_pct,
            stop_loss_pct=stop_loss_pct,
            use_realistic_pricing=use_realistic_pricing,
            bid_ask_spread_pct=bid_ask_spread_pct,
            use_skew=use_skew,
            skew_slope=skew_slope,
        )
        if result is not None:
            results[symbol] = result

    return results


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":
    # Quick test with synthetic data
    import random

    print("Put Spread Backtest - Synthetic Test")
    print("=" * 50)

    # Create synthetic price data (uptrending with noise)
    base_price = 100.0
    prices = []
    for i in range(300):
        trend = i * 0.05  # Slight uptrend
        noise = random.gauss(0, 1)
        price = base_price + trend + noise
        prices.append(DailyBar(
            date=f"2024-{(i // 30) + 1:02d}-{(i % 30) + 1:02d}",
            open=price - 0.5,
            high=price + 1,
            low=price - 1,
            close=price,
            volume=1000000,
        ))

    # Create synthetic IV data
    iv_data = []
    for i, bar in enumerate(prices):
        iv = 0.25 + 0.1 * random.random()  # 25-35% IV
        iv_data.append(IVDataPoint(date=bar.date, iv=iv))

    # Create SymbolData
    data = SymbolData(
        symbol="TEST",
        fetch_timestamp="2024-01-01T00:00:00Z",
        price_bars=prices,
        iv_data=iv_data,
    )

    result = run_put_spread_backtest(data, entry_interval_days=10)

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
        print(f"\n  P&L by IV bucket: {result.pnl_by_iv_bucket}")
