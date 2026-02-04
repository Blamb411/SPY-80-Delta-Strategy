#!/usr/bin/env python3
"""
Fetch 3 years of historical data from IBKR for all Magic Formula symbols.
"""

import sys
import time
from pathlib import Path

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from backtest.ibkr_data_fetcher import (
    IBKRDataFetcher,
    load_symbols_from_csv,
    CACHE_DIR,
)

def main():
    print("=" * 70)
    print("FETCHING 3 YEARS OF HISTORICAL DATA FROM IBKR")
    print("=" * 70)
    print()

    # Load symbols from Magic Formula CSV
    csv_path = Path(r"C:\Users\Admin\OneDrive\Desktop\Investment Trading Programs\Magic Formula and Options Program (Gemini)\magic_formula_results.csv")
    if csv_path.exists():
        symbols = load_symbols_from_csv(str(csv_path))
        print(f"Loaded {len(symbols)} symbols from Magic Formula CSV")
    else:
        # Fallback to a default list
        symbols = ['SPY', 'AAPL', 'MSFT', 'NVDA', 'GOOGL', 'AMZN', 'META', 'TSLA']
        print(f"Using default {len(symbols)} symbols (CSV not found)")

    print()

    # Connect to IBKR
    fetcher = IBKRDataFetcher(port=4002)  # IB Gateway port

    if not fetcher.connect():
        print("ERROR: Failed to connect to IBKR. Is IB Gateway running?")
        return 1

    print("Connected to IBKR")
    print()

    # Fetch data with progress
    start_time = time.time()
    success_count = 0
    fail_count = 0

    try:
        for i, symbol in enumerate(symbols, 1):
            pct = i / len(symbols) * 100
            print(f"[{pct:5.1f}%] Fetching {symbol} ({i}/{len(symbols)})...", end=" ", flush=True)

            try:
                # use_cache=True means: save to cache after fetching
                # We delete cache beforehand so it will fetch fresh anyway
                data = fetcher.fetch_symbol_data(symbol, use_cache=True)
                if data and data.price_bars:
                    print(f"OK - {len(data.price_bars)} bars, {len(data.iv_data)} IV points")
                    success_count += 1
                else:
                    print("SKIP - No data returned")
                    fail_count += 1
            except Exception as e:
                print(f"ERROR - {e}")
                fail_count += 1

            # Small delay to respect rate limits
            time.sleep(0.5)

    finally:
        fetcher.disconnect()

    elapsed = time.time() - start_time
    print()
    print("=" * 70)
    print(f"COMPLETE: {success_count} symbols fetched, {fail_count} failed")
    print(f"Time elapsed: {elapsed/60:.1f} minutes")
    print(f"Data saved to: {CACHE_DIR}")
    print("=" * 70)

    return 0


if __name__ == "__main__":
    sys.exit(main())
