"""
Backtest Engine
================
Main day-by-day simulation loop.

Flow:
    1. For each trading day in the backtest period:
        a. Check all open positions for exit conditions (TP, SL, expiration)
        b. For each ticker, check if an entry signal is valid:
           - IV Rank >= threshold
           - Not too many concurrent positions
           - Minimum days since last entry
        c. If valid, build spreads for all combos and add shadow positions
    2. At the end, close any remaining open positions at market
"""

import logging
import sys
from datetime import datetime, timedelta
from typing import List, Dict, Optional

import config
import cache_db
import data_fetcher
import iv_engine
import spread_builder
from shadow_tracker import ShadowTracker, ShadowPosition, create_combo_id

logger = logging.getLogger(__name__)


def run_backtest(
    tickers: List[str] = None,
    start_date: str = None,
    end_date: str = None,
    progress_callback=None,
) -> ShadowTracker:
    """Run the full backtest simulation.

    Args:
        tickers: List of underlying tickers (default: config.TICKERS)
        start_date: Start date YYYY-MM-DD (default: config.START_DATE)
        end_date: End date YYYY-MM-DD (default: config.END_DATE)
        progress_callback: Optional callable(day_num, total_days, message)

    Returns:
        ShadowTracker with all positions (open and closed)
    """
    tickers = tickers or config.TICKERS
    start_date = start_date or config.START_DATE.isoformat()
    end_date = end_date or config.END_DATE.isoformat()

    logger.info("=" * 70)
    logger.info("BACKTEST ENGINE — LOCAL MASSIVE API BACKTESTER")
    logger.info("=" * 70)
    logger.info(f"Tickers: {', '.join(tickers)}")
    logger.info(f"Period:  {start_date} to {end_date}")
    logger.info(f"Combos per entry: {config.COMBOS_PER_ENTRY}")
    logger.info("")

    # Initialize
    cache_db.init_db()
    tracker = ShadowTracker()

    # ---------------------------------------------------------------
    # Phase 1: Download underlying data
    # We need extra lookback before start_date for HV/IV Rank calculation
    # (IV_RANK_LOOKBACK + HV_PERIOD + buffer days)
    # ---------------------------------------------------------------
    logger.info("Phase 1: Fetching underlying daily bars ...")
    from datetime import timedelta as _td
    lookback_days = config.IV_RANK_LOOKBACK + config.HV_PERIOD + 30
    dt_start = datetime.strptime(start_date, "%Y-%m-%d").date()
    fetch_start = (dt_start - _td(days=int(lookback_days * 1.5))).isoformat()
    for t in tickers:
        data_fetcher.fetch_underlying_bars(t, fetch_start, end_date)
    logger.info("  Underlying data ready.\n")

    # ---------------------------------------------------------------
    # Phase 2: Build trading calendar from first ticker
    # ---------------------------------------------------------------
    # Use SPY as the calendar reference (most liquid, fewest holidays)
    ref_ticker = "SPY" if "SPY" in tickers else tickers[0]
    trading_dates = cache_db.get_trading_dates(ref_ticker, start_date, end_date)
    if not trading_dates:
        logger.error("No trading dates found. Check data.")
        return tracker

    total_days = len(trading_dates)
    logger.info(f"Phase 2: Simulating {total_days} trading days ...\n")

    # ---------------------------------------------------------------
    # Phase 3: Day-by-day simulation
    # ---------------------------------------------------------------
    for day_idx, trade_date in enumerate(trading_dates):
        if progress_callback:
            progress_callback(day_idx + 1, total_days, trade_date)
        elif day_idx % 50 == 0:
            open_count = len(tracker.open_positions)
            closed_count = len(tracker.closed_positions)
            logger.info(
                f"  Day {day_idx+1}/{total_days}: {trade_date}  "
                f"(open={open_count}, closed={closed_count})"
            )

        # --- Step A: Monitor open positions ---
        _monitor_positions(tracker, trade_date, tickers)

        # --- Step B: Check for new entries ---
        _check_entries(tracker, trade_date, tickers)

    # ---------------------------------------------------------------
    # Phase 4: Close any remaining open positions
    # ---------------------------------------------------------------
    logger.info("\nPhase 4: Closing remaining open positions ...")
    last_date = trading_dates[-1] if trading_dates else end_date
    _close_remaining(tracker, last_date, tickers)

    logger.info("\nBacktest complete.")
    summary = tracker.summary()
    logger.info(f"  Total positions:  {summary['total_positions']}")
    logger.info(f"  Closed:           {summary['closed']}")
    logger.info(f"  Win rate:         {summary['win_rate']:.1%}")
    logger.info(f"  Total P&L:        ${summary['total_pnl']:,.2f}")
    logger.info(f"  Avg P&L/trade:    ${summary['avg_pnl']:,.2f}")

    return tracker


# -----------------------------------------------------------------------
# Internal: monitor open positions
# -----------------------------------------------------------------------

def _monitor_positions(tracker: ShadowTracker, trade_date: str, tickers: List[str]) -> None:
    """Check all open positions for exit conditions."""
    positions_to_check = tracker.get_positions_needing_monitoring(trade_date)
    if not positions_to_check:
        return

    # Group by underlying for efficient spot lookup
    by_underlying: Dict[str, List[ShadowPosition]] = {}
    for p in positions_to_check:
        by_underlying.setdefault(p.underlying, []).append(p)

    for underlying, positions in by_underlying.items():
        spot = cache_db.get_underlying_close(underlying, trade_date)
        if spot is None:
            continue

        # Group by spread (short+long ticker pair) to avoid redundant pricing
        spread_prices: Dict[str, Optional[float]] = {}

        for pos in positions:
            spread_key = f"{pos.short_ticker}|{pos.long_ticker}"

            if spread_key not in spread_prices:
                # Ensure we have data for today
                _ensure_monitoring_data(pos, trade_date)
                cost = spread_builder.price_spread_to_close(
                    pos.short_ticker, pos.long_ticker, trade_date,
                )
                spread_prices[spread_key] = cost

            cost_to_close = spread_prices[spread_key]

            exit_info = tracker.check_exit_conditions(
                pos, trade_date, cost_to_close, spot,
            )
            if exit_info:
                tracker.close_position(
                    pos,
                    exit_date=trade_date,
                    exit_reason=exit_info["reason"],
                    exit_cost=exit_info["exit_cost"],
                )


def _ensure_monitoring_data(pos: ShadowPosition, trade_date: str) -> None:
    """Make sure we have option bar data for monitoring a position on this date."""
    for opt_ticker in [pos.short_ticker, pos.long_ticker]:
        bar = cache_db.get_option_bar(opt_ticker, trade_date)
        if bar is None:
            # Try to fetch it
            data_fetcher.fetch_option_bars(
                opt_ticker, trade_date, pos.expiration_date,
            )


# -----------------------------------------------------------------------
# Internal: check for new entries
# -----------------------------------------------------------------------

def _check_entries(tracker: ShadowTracker, trade_date: str, tickers: List[str]) -> None:
    """Check each ticker for valid entry signals and create shadow positions."""
    for ticker in tickers:
        # Quick checks before doing any work
        if not tracker.can_enter(ticker, trade_date):
            continue

        spot = cache_db.get_underlying_close(ticker, trade_date)
        if spot is None:
            continue

        # Compute IV Rank
        iv_rank = iv_engine.compute_iv_rank(ticker, trade_date)
        if iv_rank is None:
            continue
        if iv_rank < config.IV_RANK_MIN:
            continue

        # Compute current HV as IV proxy
        current_iv = iv_engine.compute_hv(ticker, trade_date)

        # Valid entry signal — build spreads for all combos
        _create_entry_combos(tracker, ticker, trade_date, spot, current_iv, iv_rank)


def _create_entry_combos(
    tracker: ShadowTracker,
    ticker: str,
    trade_date: str,
    spot: float,
    iv: Optional[float],
    iv_rank: float,
) -> None:
    """Create all 16 shadow positions for an entry signal.

    Iterates over: strategies x widths x take-profits x stop-losses
    """
    combos_created = 0

    for strategy_type in config.STRATEGY_TYPES:
        right = "P" if strategy_type == "PUT" else "C"

        for width_pct in config.WIDTH_PCTS:
            # Build the spread once per (strategy, width) — legs are same
            spread = spread_builder.build_spread(
                underlying=ticker,
                trade_date=trade_date,
                right=right,
                width_pct=width_pct,
                spot=spot,
                iv=iv,
            )

            if spread is None:
                continue

            for tp_pct in config.TAKE_PROFIT_PCTS:
                for sl_mult in config.STOP_LOSS_MULTS:
                    combo_id = create_combo_id(
                        ticker, trade_date, strategy_type,
                        width_pct, tp_pct, sl_mult,
                    )

                    # Compute exit thresholds
                    # TP: close when cost_to_close <= credit * (1 - tp_pct)
                    tp_target = spread["credit"] * (1 - tp_pct)
                    # SL: close when cost_to_close >= credit + credit * sl_mult
                    sl_trigger = spread["credit"] + spread["credit"] * sl_mult

                    pos = ShadowPosition(
                        combo_id=combo_id,
                        underlying=ticker,
                        entry_date=trade_date,
                        expiration_date=spread["expiration_date"],
                        strategy_type=strategy_type,
                        width_pct=width_pct,
                        tp_pct=tp_pct,
                        sl_mult=sl_mult,
                        short_ticker=spread["short_ticker"],
                        long_ticker=spread["long_ticker"],
                        short_strike=spread["short_strike"],
                        long_strike=spread["long_strike"],
                        spot_at_entry=spot,
                        credit=spread["credit"],
                        max_loss=spread["max_loss"],
                        iv_at_entry=iv,
                        iv_rank_at_entry=iv_rank,
                        has_real_quote=spread["has_real_quote"],
                        tp_target=tp_target,
                        sl_trigger=sl_trigger,
                        dte=spread["dte"],
                    )

                    tracker.add_position(pos)
                    combos_created += 1

    if combos_created > 0:
        logger.debug(
            f"    {ticker} {trade_date}: created {combos_created} combos "
            f"(IV Rank={iv_rank:.2f}, spot={spot:.2f})"
        )


# -----------------------------------------------------------------------
# Internal: close remaining positions at end of backtest
# -----------------------------------------------------------------------

def _close_remaining(tracker: ShadowTracker, last_date: str, tickers: List[str]) -> None:
    """Close any positions still open at end of backtest."""
    remaining = list(tracker.open_positions)
    if not remaining:
        logger.info("  No open positions to close.")
        return

    logger.info(f"  Closing {len(remaining)} remaining positions ...")

    for pos in remaining:
        spot = cache_db.get_underlying_close(pos.underlying, last_date)
        if spot is None:
            # Try the day before
            spot = pos.spot_at_entry  # last resort

        settlement = spread_builder.intrinsic_value_at_expiration(
            spot, pos.short_strike, pos.long_strike, pos.strategy_type,
        )

        tracker.close_position(
            pos,
            exit_date=last_date,
            exit_reason="end_of_backtest",
            exit_cost=settlement,
        )
