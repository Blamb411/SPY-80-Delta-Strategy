#!/usr/bin/env python3
"""
Options Backtest Runner
=======================
Main entry point for running put credit spread and iron condor backtests.

Usage:
    python run_backtest.py --csv magic_formula_results.csv --strategy both
    python run_backtest.py --symbols AAPL MSFT NVDA --strategy put_spread
    python run_backtest.py --no-cache --port 7497
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Dict

# Set up logging first
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Import our modules
from .ibkr_data_fetcher import (
    IBKRDataFetcher,
    SymbolData,
    load_symbols_from_csv,
    IB_PORT,
)
from .put_spread_backtest import (
    run_put_spread_backtest,
    run_put_spread_backtest_multi,
    PutSpreadBacktestResult,
)
from .condor_backtest import (
    run_condor_backtest,
    run_condor_backtest_multi,
    CondorBacktestResult,
)
from .backtest_report import (
    generate_comparison_report,
    print_console_report,
    export_trades_csv,
    export_summary_json,
)


# =============================================================================
# CONFIGURATION
# =============================================================================

DEFAULT_OUTPUT_DIR = Path(__file__).parent / "results"
DEFAULT_ENTRY_INTERVAL = 5  # Days between potential entries


# =============================================================================
# MAIN WORKFLOW
# =============================================================================

def load_symbols(
    symbols: Optional[List[str]],
    csv_path: Optional[str],
    ticker_column: Optional[str] = None,
) -> List[str]:
    """Load symbols from command line or CSV file."""

    if symbols:
        return [s.upper() for s in symbols]

    if csv_path:
        return load_symbols_from_csv(csv_path, ticker_column)

    # Default test symbols
    return ['AAPL', 'MSFT', 'NVDA', 'GOOGL', 'AMZN']


def fetch_data(
    fetcher: IBKRDataFetcher,
    symbols: List[str],
    use_cache: bool = True,
) -> Dict[str, SymbolData]:
    """Fetch historical data for all symbols."""

    logger.info(f"Fetching data for {len(symbols)} symbols...")

    def progress(symbol, current, total):
        pct = current / total * 100
        logger.info(f"[{pct:5.1f}%] Fetching {symbol} ({current}/{total})")

    results = fetcher.fetch_multiple_symbols(
        symbols,
        use_cache=use_cache,
        progress_callback=progress,
    )

    logger.info(f"Successfully fetched data for {len(results)}/{len(symbols)} symbols")
    return results


def run_backtest(
    symbol_data: Dict[str, SymbolData],
    strategy: str,
    entry_interval: int = DEFAULT_ENTRY_INTERVAL,
    use_early_exit: bool = False,
    take_profit_pct: float = 0.75,
    stop_loss_pct: float = 0.50,
) -> tuple:
    """
    Run backtests based on strategy selection.

    Returns:
        Tuple of (put_spread_results, condor_results)
    """
    put_results = None
    condor_results = None

    if strategy in ('put_spread', 'both'):
        if use_early_exit:
            logger.info(f"Running put credit spread backtest with early exit (TP={take_profit_pct:.0%}, SL={stop_loss_pct:.0%})...")
        else:
            logger.info("Running put credit spread backtest...")
        put_results = run_put_spread_backtest_multi(
            symbol_data, entry_interval,
            use_early_exit=use_early_exit,
            take_profit_pct=take_profit_pct,
            stop_loss_pct=stop_loss_pct,
        )

        # Quick summary
        total_trades = sum(r.total_trades for r in put_results.values())
        total_pnl = sum(r.total_pnl for r in put_results.values())
        logger.info(f"Put spreads: {total_trades} trades, ${total_pnl:,.0f} P&L")

    if strategy in ('condor', 'both'):
        logger.info("Running iron condor backtest...")
        condor_results = run_condor_backtest_multi(symbol_data, entry_interval)

        # Quick summary
        total_trades = sum(r.total_trades for r in condor_results.values())
        total_pnl = sum(r.total_pnl for r in condor_results.values())
        logger.info(f"Iron condors: {total_trades} trades, ${total_pnl:,.0f} P&L")

    return put_results, condor_results


def save_results(
    put_results: Optional[Dict[str, PutSpreadBacktestResult]],
    condor_results: Optional[Dict[str, CondorBacktestResult]],
    output_dir: Path,
):
    """Generate and save all reports."""

    output_dir.mkdir(parents=True, exist_ok=True)

    # Generate report
    report = generate_comparison_report(put_results, condor_results)

    # Print to console
    print_console_report(report)

    # Save files
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # Trades CSV
    csv_path = output_dir / f"trades_{timestamp}.csv"
    export_trades_csv(put_results, condor_results, str(csv_path))

    # Summary JSON
    json_path = output_dir / f"summary_{timestamp}.json"
    export_summary_json(report, str(json_path))

    logger.info(f"Results saved to {output_dir}")
    logger.info(f"  - Trades:  {csv_path.name}")
    logger.info(f"  - Summary: {json_path.name}")


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Run options backtests on historical data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m backtest.run_backtest --symbols AAPL MSFT --strategy put_spread
  python -m backtest.run_backtest --csv stocks.csv --strategy both
  python -m backtest.run_backtest --no-cache --port 7497
        """
    )

    # Symbol source
    symbol_group = parser.add_mutually_exclusive_group()
    symbol_group.add_argument(
        '--symbols', nargs='+',
        help='Specific symbols to backtest'
    )
    symbol_group.add_argument(
        '--csv',
        help='CSV file with ticker symbols'
    )

    parser.add_argument(
        '--ticker-column',
        help='Column name for tickers in CSV (auto-detected if not specified)'
    )

    # Strategy
    parser.add_argument(
        '--strategy',
        choices=['put_spread', 'condor', 'both'],
        default='both',
        help='Which strategy to backtest (default: both)'
    )

    # Data options
    parser.add_argument(
        '--no-cache', action='store_true',
        help='Fetch fresh data, ignore cache'
    )
    parser.add_argument(
        '--port', type=int, default=IB_PORT,
        help=f'IBKR port (default: {IB_PORT})'
    )

    # Backtest options
    parser.add_argument(
        '--entry-interval', type=int, default=DEFAULT_ENTRY_INTERVAL,
        help=f'Minimum days between entries (default: {DEFAULT_ENTRY_INTERVAL})'
    )

    # Early exit management
    parser.add_argument(
        '--early-exit', action='store_true',
        help='Enable early exit management (take profit / stop loss)'
    )
    parser.add_argument(
        '--take-profit', type=float, default=0.75,
        help='Take profit at this %% of max profit (default: 0.75 = 75%%)'
    )
    parser.add_argument(
        '--stop-loss', type=float, default=0.50,
        help='Stop loss at this %% of max loss (default: 0.50 = 50%%)'
    )

    # Output
    parser.add_argument(
        '--output-dir',
        default=str(DEFAULT_OUTPUT_DIR),
        help=f'Output directory for results (default: {DEFAULT_OUTPUT_DIR})'
    )

    # Dry run (no IBKR connection)
    parser.add_argument(
        '--offline', action='store_true',
        help='Run in offline mode using only cached data'
    )

    args = parser.parse_args()

    # Load symbols
    try:
        symbols = load_symbols(args.symbols, args.csv, args.ticker_column)
    except Exception as e:
        logger.error(f"Failed to load symbols: {e}")
        return 1

    if not symbols:
        logger.error("No symbols to process")
        return 1

    logger.info(f"Loaded {len(symbols)} symbols: {symbols[:5]}{'...' if len(symbols) > 5 else ''}")

    # Fetch data
    if args.offline:
        # Load from cache only
        logger.info("Running in offline mode - using cached data only")
        from .ibkr_data_fetcher import load_from_cache, is_cache_valid

        symbol_data = {}
        for symbol in symbols:
            if is_cache_valid(symbol, max_age_days=365):  # Accept old cache in offline mode
                data = load_from_cache(symbol)
                if data:
                    symbol_data[symbol] = data

        if not symbol_data:
            logger.error("No cached data found. Run with IBKR connection first.")
            return 1

        logger.info(f"Loaded {len(symbol_data)} symbols from cache")
    else:
        # Connect to IBKR and fetch
        fetcher = IBKRDataFetcher(port=args.port)

        if not fetcher.connect():
            logger.error("Failed to connect to IBKR. Is TWS/Gateway running?")
            logger.info("Use --offline to run with cached data only")
            return 1

        try:
            symbol_data = fetch_data(
                fetcher, symbols,
                use_cache=not args.no_cache
            )
        finally:
            fetcher.disconnect()

        if not symbol_data:
            logger.error("Failed to fetch any data")
            return 1

    # Run backtests
    put_results, condor_results = run_backtest(
        symbol_data,
        args.strategy,
        args.entry_interval,
        use_early_exit=args.early_exit,
        take_profit_pct=args.take_profit,
        stop_loss_pct=args.stop_loss,
    )

    # Save results
    save_results(
        put_results,
        condor_results,
        Path(args.output_dir),
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
