"""
Run Backtest — Entry Point
============================
CLI entry point for the Massive API local options backtester.

Usage:
    python run_backtest.py                      # Full 12-ticker, 2020-2025
    python run_backtest.py --tickers SPY        # Single ticker
    python run_backtest.py --tickers SPY QQQ    # Multiple tickers
    python run_backtest.py --start 2023-01-01 --end 2023-12-31   # Date range
    python run_backtest.py --tickers SPY --start 2023-01-01 --end 2023-12-31
    python run_backtest.py --no-csv             # Skip CSV export
    python run_backtest.py --no-json            # Skip JSON export
"""

import argparse
import logging
import os
import sys
from datetime import datetime

# Ensure this directory is on the path
_this_dir = os.path.dirname(os.path.abspath(__file__))
if _this_dir not in sys.path:
    sys.path.insert(0, _this_dir)

# Also add the project root so we can import from backtest/
_project_root = os.path.dirname(_this_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

import config
import cache_db
import backtest_engine
import report_generator


def main():
    parser = argparse.ArgumentParser(
        description="Massive API Local Options Backtester",
    )
    parser.add_argument(
        "--tickers", nargs="+", default=None,
        help="Tickers to backtest (default: all 12 ETFs)",
    )
    parser.add_argument(
        "--start", default=None,
        help="Start date YYYY-MM-DD (default: 2020-01-01)",
    )
    parser.add_argument(
        "--end", default=None,
        help="End date YYYY-MM-DD (default: 2025-12-31)",
    )
    parser.add_argument(
        "--no-csv", action="store_true",
        help="Skip CSV export",
    )
    parser.add_argument(
        "--no-json", action="store_true",
        help="Skip JSON export",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable debug logging",
    )
    args = parser.parse_args()

    # Logging setup
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    # Parameters
    tickers = args.tickers or config.TICKERS
    start = args.start or config.START_DATE.isoformat()
    end = args.end or config.END_DATE.isoformat()

    print("=" * 70)
    print("MASSIVE API LOCAL OPTIONS BACKTESTER")
    print("=" * 70)
    print(f"Tickers:  {', '.join(tickers)}")
    print(f"Period:   {start} to {end}")
    print(f"Combos:   {config.COMBOS_PER_ENTRY} per entry signal")
    print(f"DB:       {config.DB_PATH}")
    print()

    # Initialize database
    cache_db.init_db()

    # Progress display
    def progress(day_num, total_days, trade_date):
        if day_num % 25 == 0 or day_num == total_days:
            pct = day_num / total_days * 100
            sys.stdout.write(
                f"\r  Processing: {trade_date}  "
                f"[{day_num}/{total_days}] {pct:.0f}%   "
            )
            sys.stdout.flush()

    # Run backtest
    tracker = backtest_engine.run_backtest(
        tickers=tickers,
        start_date=start,
        end_date=end,
        progress_callback=progress,
    )
    print()  # newline after progress bar

    # Print report
    report_generator.print_report(tracker)

    # Export CSV
    if not args.no_csv:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_path = os.path.join(_this_dir, f"trades_{timestamp}.csv")
        report_generator.export_trades_csv(tracker, csv_path)

    # Export JSON
    if not args.no_json:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_path = os.path.join(_this_dir, f"summary_{timestamp}.json")
        report_generator.export_summary_json(tracker, json_path)


if __name__ == "__main__":
    main()
