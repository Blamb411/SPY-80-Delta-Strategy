"""
IBKR Historical Data Fetcher
============================
Pulls historical underlying prices and implied volatility from Interactive Brokers.
Caches data locally to avoid repeated API calls.

Designed to run outside of RTH for backtesting purposes.
"""

from __future__ import annotations

import os
import json
import time
import logging
from dataclasses import dataclass, asdict
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Tuple, Any
from pathlib import Path

from ib_insync import IB, Stock, Contract

# =============================================================================
# CONFIGURATION
# =============================================================================

# IBKR Connection
IB_HOST = "127.0.0.1"
IB_PORT = 7497  # 7497 for TWS paper, 7496 for TWS live, 4001/4002 for Gateway
IB_CLIENT_ID = 50  # Use different ID than live trading scripts
CONNECT_TIMEOUT_SEC = 30

# Data fetch settings
HIST_DURATION = "3 Y"  # How far back to fetch (3 years for better IV rank calibration)
BAR_SIZE = "1 day"
USE_RTH = True  # Regular trading hours only

# Rate limiting - IBKR allows ~60 historical data requests per 10 minutes
REQUEST_DELAY_SEC = 1.0  # Delay between requests
MAX_RETRIES = 3
RETRY_DELAY_SEC = 5.0

# Cache settings
CACHE_DIR = Path(__file__).parent / "cache"
CACHE_MAX_AGE_DAYS = 1  # Re-fetch if cache older than this

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# =============================================================================
# DATA STRUCTURES
# =============================================================================

@dataclass
class DailyBar:
    """Single day of OHLCV data."""
    date: str  # YYYY-MM-DD
    open: float
    high: float
    low: float
    close: float
    volume: int


@dataclass
class IVDataPoint:
    """Single day of implied volatility data."""
    date: str  # YYYY-MM-DD
    iv: float  # Annualized IV as decimal


@dataclass
class SymbolData:
    """Complete historical data for a symbol."""
    symbol: str
    fetch_timestamp: str
    price_bars: List[DailyBar]
    iv_data: List[IVDataPoint]
    # Computed fields
    hv20: Optional[float] = None
    hv60: Optional[float] = None
    hv120: Optional[float] = None
    current_iv: Optional[float] = None
    iv_rank_252: Optional[float] = None


# =============================================================================
# CACHE MANAGEMENT
# =============================================================================

def ensure_cache_dir():
    """Create cache directory if it doesn't exist."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)


def get_cache_path(symbol: str) -> Path:
    """Get cache file path for a symbol."""
    return CACHE_DIR / f"{symbol.upper()}_hist.json"


def is_cache_valid(symbol: str, max_age_days: int = CACHE_MAX_AGE_DAYS) -> bool:
    """Check if cached data exists and is fresh enough."""
    cache_path = get_cache_path(symbol)
    if not cache_path.exists():
        return False

    # Check modification time
    mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)
    age = datetime.now() - mtime
    return age.days < max_age_days


def load_from_cache(symbol: str) -> Optional[SymbolData]:
    """Load symbol data from cache."""
    cache_path = get_cache_path(symbol)
    if not cache_path.exists():
        return None

    try:
        with open(cache_path, 'r') as f:
            data = json.load(f)

        return SymbolData(
            symbol=data['symbol'],
            fetch_timestamp=data['fetch_timestamp'],
            price_bars=[DailyBar(**bar) for bar in data['price_bars']],
            iv_data=[IVDataPoint(**iv) for iv in data['iv_data']],
            hv20=data.get('hv20'),
            hv60=data.get('hv60'),
            hv120=data.get('hv120'),
            current_iv=data.get('current_iv'),
            iv_rank_252=data.get('iv_rank_252'),
        )
    except Exception as e:
        logger.warning(f"Failed to load cache for {symbol}: {e}")
        return None


def save_to_cache(data: SymbolData):
    """Save symbol data to cache."""
    ensure_cache_dir()
    cache_path = get_cache_path(data.symbol)

    try:
        cache_dict = {
            'symbol': data.symbol,
            'fetch_timestamp': data.fetch_timestamp,
            'price_bars': [asdict(bar) for bar in data.price_bars],
            'iv_data': [asdict(iv) for iv in data.iv_data],
            'hv20': data.hv20,
            'hv60': data.hv60,
            'hv120': data.hv120,
            'current_iv': data.current_iv,
            'iv_rank_252': data.iv_rank_252,
        }
        with open(cache_path, 'w') as f:
            json.dump(cache_dict, f, indent=2)
        logger.info(f"Cached data for {data.symbol}")
    except Exception as e:
        logger.warning(f"Failed to cache data for {data.symbol}: {e}")


# =============================================================================
# IBKR DATA FETCHING
# =============================================================================

class IBKRDataFetcher:
    """Fetches historical data from Interactive Brokers."""

    def __init__(self, host: str = IB_HOST, port: int = IB_PORT, client_id: int = IB_CLIENT_ID):
        self.host = host
        self.port = port
        self.client_id = client_id
        self.ib: Optional[IB] = None
        self._request_count = 0
        self._last_request_time = 0.0

    def connect(self) -> bool:
        """Connect to IBKR."""
        if self.ib is not None and self.ib.isConnected():
            return True

        self.ib = IB()
        try:
            self.ib.connect(self.host, self.port, clientId=self.client_id,
                           timeout=CONNECT_TIMEOUT_SEC)
            if not self.ib.isConnected():
                raise RuntimeError("Connection returned but isConnected() is False")

            # Set market data type to delayed/frozen for outside RTH
            self.ib.reqMarketDataType(4)  # 4 = delayed frozen

            logger.info(f"Connected to IBKR at {self.host}:{self.port}")
            return True

        except Exception as e:
            logger.error(f"Failed to connect to IBKR: {e}")
            self.ib = None
            return False

    def disconnect(self):
        """Disconnect from IBKR."""
        if self.ib is not None:
            try:
                self.ib.disconnect()
            except Exception:
                pass
            self.ib = None
            logger.info("Disconnected from IBKR")

    def _rate_limit(self):
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < REQUEST_DELAY_SEC:
            time.sleep(REQUEST_DELAY_SEC - elapsed)
        self._last_request_time = time.time()
        self._request_count += 1

    def _qualify_stock(self, symbol: str) -> Optional[Stock]:
        """Qualify a stock contract."""
        stock = Stock(symbol, "SMART", "USD")
        try:
            self.ib.qualifyContracts(stock)
            if getattr(stock, 'conId', 0) > 0:
                return stock
        except Exception as e:
            logger.warning(f"Failed to qualify {symbol}: {e}")
        return None

    def fetch_price_history(self, symbol: str) -> List[DailyBar]:
        """Fetch historical daily price bars for a symbol."""
        if self.ib is None or not self.ib.isConnected():
            logger.error("Not connected to IBKR")
            return []

        stock = self._qualify_stock(symbol)
        if stock is None:
            return []

        for attempt in range(MAX_RETRIES):
            try:
                self._rate_limit()
                bars = self.ib.reqHistoricalData(
                    stock,
                    endDateTime='',
                    durationStr=HIST_DURATION,
                    barSizeSetting=BAR_SIZE,
                    whatToShow='TRADES',
                    useRTH=USE_RTH,
                    formatDate=1,
                )

                if not bars:
                    logger.warning(f"No price data returned for {symbol}")
                    return []

                result = []
                for bar in bars:
                    if bar.close is not None and bar.close > 0:
                        result.append(DailyBar(
                            date=bar.date.strftime('%Y-%m-%d') if hasattr(bar.date, 'strftime') else str(bar.date),
                            open=float(bar.open),
                            high=float(bar.high),
                            low=float(bar.low),
                            close=float(bar.close),
                            volume=int(bar.volume) if bar.volume else 0,
                        ))

                logger.info(f"Fetched {len(result)} price bars for {symbol}")
                return result

            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed for {symbol} prices: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY_SEC)

        return []

    def fetch_iv_history(self, symbol: str) -> List[IVDataPoint]:
        """Fetch historical implied volatility for a symbol."""
        if self.ib is None or not self.ib.isConnected():
            logger.error("Not connected to IBKR")
            return []

        stock = self._qualify_stock(symbol)
        if stock is None:
            return []

        for attempt in range(MAX_RETRIES):
            try:
                self._rate_limit()
                bars = self.ib.reqHistoricalData(
                    stock,
                    endDateTime='',
                    durationStr=HIST_DURATION,
                    barSizeSetting=BAR_SIZE,
                    whatToShow='OPTION_IMPLIED_VOLATILITY',
                    useRTH=USE_RTH,
                    formatDate=1,
                )

                if not bars:
                    logger.warning(f"No IV data returned for {symbol}")
                    return []

                result = []
                for bar in bars:
                    if bar.close is not None and bar.close > 0:
                        result.append(IVDataPoint(
                            date=bar.date.strftime('%Y-%m-%d') if hasattr(bar.date, 'strftime') else str(bar.date),
                            iv=float(bar.close),
                        ))

                logger.info(f"Fetched {len(result)} IV points for {symbol}")
                return result

            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed for {symbol} IV: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY_SEC)

        return []

    def fetch_symbol_data(self, symbol: str, use_cache: bool = True) -> Optional[SymbolData]:
        """
        Fetch complete historical data for a symbol.

        Args:
            symbol: Stock ticker symbol
            use_cache: If True, use cached data if available and fresh

        Returns:
            SymbolData object or None if fetch failed
        """
        symbol = symbol.upper().strip()

        # Check cache first
        if use_cache and is_cache_valid(symbol):
            cached = load_from_cache(symbol)
            if cached is not None:
                logger.info(f"Using cached data for {symbol}")
                return cached

        # Fetch fresh data
        logger.info(f"Fetching fresh data for {symbol}...")

        price_bars = self.fetch_price_history(symbol)
        if not price_bars:
            logger.error(f"Failed to fetch price data for {symbol}")
            return None

        iv_data = self.fetch_iv_history(symbol)
        if not iv_data:
            logger.warning(f"No IV data for {symbol} - will use HV-based estimates")

        # Compute derived values
        data = SymbolData(
            symbol=symbol,
            fetch_timestamp=datetime.utcnow().isoformat() + 'Z',
            price_bars=price_bars,
            iv_data=iv_data,
        )

        # Calculate HV
        closes = [bar.close for bar in price_bars]
        data.hv20 = _compute_hv(closes, 20)
        data.hv60 = _compute_hv(closes, 60)
        data.hv120 = _compute_hv(closes, 120)

        # Calculate IV rank
        if iv_data:
            ivs = [pt.iv for pt in iv_data]
            data.current_iv = ivs[-1] if ivs else None
            data.iv_rank_252 = _compute_iv_rank(data.current_iv, ivs)

        # Cache the data
        if use_cache:
            save_to_cache(data)

        return data

    def fetch_multiple_symbols(
        self,
        symbols: List[str],
        use_cache: bool = True,
        progress_callback=None,
    ) -> Dict[str, SymbolData]:
        """
        Fetch data for multiple symbols.

        Args:
            symbols: List of ticker symbols
            use_cache: Use cached data when available
            progress_callback: Optional callback(symbol, current, total)

        Returns:
            Dict mapping symbol to SymbolData (only successful fetches)
        """
        results = {}
        total = len(symbols)

        for i, symbol in enumerate(symbols, 1):
            if progress_callback:
                progress_callback(symbol, i, total)
            else:
                logger.info(f"Processing {symbol} ({i}/{total})...")

            data = self.fetch_symbol_data(symbol, use_cache=use_cache)
            if data is not None:
                results[symbol] = data

        return results


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

import math

def _compute_hv(closes: List[float], period: int) -> Optional[float]:
    """Compute historical volatility."""
    if len(closes) < period + 1:
        return None

    returns = [math.log(closes[i] / closes[i-1])
               for i in range(len(closes) - period, len(closes))]

    if len(returns) < 2:
        return None

    mean_ret = sum(returns) / len(returns)
    variance = sum((r - mean_ret) ** 2 for r in returns) / (len(returns) - 1)

    return math.sqrt(variance * 252)


def _compute_iv_rank(current_iv: Optional[float], iv_history: List[float],
                     lookback: int = 252) -> Optional[float]:
    """Compute IV rank."""
    if current_iv is None or not iv_history or len(iv_history) < 20:
        return None

    history = iv_history[-lookback:] if len(iv_history) > lookback else iv_history
    iv_low = min(history)
    iv_high = max(history)

    if iv_high <= iv_low:
        return 0.5

    rank = (current_iv - iv_low) / (iv_high - iv_low)
    return max(0.0, min(1.0, rank))


def load_symbols_from_csv(csv_path: str, ticker_column: str = None) -> List[str]:
    """
    Load ticker symbols from a CSV file.

    Args:
        csv_path: Path to CSV file
        ticker_column: Column name containing tickers (auto-detected if None)

    Returns:
        List of ticker symbols
    """
    import csv

    possible_columns = ['Ticker', 'ticker', 'Symbol', 'symbol', 'Stock', 'stock', 'Instrument']

    symbols = []
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)

        # Find the ticker column
        if ticker_column is None:
            for col in possible_columns:
                if col in reader.fieldnames:
                    ticker_column = col
                    break

        if ticker_column is None:
            raise ValueError(f"Could not find ticker column. Available: {reader.fieldnames}")

        for row in reader:
            ticker = row.get(ticker_column, '').strip().upper()
            if ticker and ticker != 'NAN':
                # Handle class shares (BRK.B, BF-B)
                # Remove exchange suffixes (.NS, .L, etc.) but keep class letters
                if '.' in ticker and len(ticker.split('.')[-1]) > 2:
                    ticker = ticker.split('.')[0]
                symbols.append(ticker)

    return symbols


# =============================================================================
# MAIN - Standalone execution for data fetching
# =============================================================================

def main():
    """Main entry point for standalone data fetching."""
    import argparse

    parser = argparse.ArgumentParser(description='Fetch historical data from IBKR')
    parser.add_argument('--symbols', nargs='+', help='Symbols to fetch')
    parser.add_argument('--csv', help='CSV file with symbols')
    parser.add_argument('--no-cache', action='store_true', help='Skip cache, fetch fresh')
    parser.add_argument('--port', type=int, default=IB_PORT, help='IBKR port')

    args = parser.parse_args()

    # Get symbols
    if args.symbols:
        symbols = [s.upper() for s in args.symbols]
    elif args.csv:
        symbols = load_symbols_from_csv(args.csv)
    else:
        # Default test symbols
        symbols = ['AAPL', 'MSFT', 'NVDA']

    print(f"Will fetch data for {len(symbols)} symbols: {symbols[:10]}{'...' if len(symbols) > 10 else ''}")

    fetcher = IBKRDataFetcher(port=args.port)

    if not fetcher.connect():
        print("Failed to connect to IBKR. Is TWS/Gateway running?")
        return 1

    try:
        results = fetcher.fetch_multiple_symbols(
            symbols,
            use_cache=not args.no_cache,
        )

        print(f"\nSuccessfully fetched data for {len(results)}/{len(symbols)} symbols")

        # Print summary
        print("\nSummary:")
        print("-" * 80)
        print(f"{'Symbol':<8} {'Bars':>6} {'IV Pts':>7} {'HV20':>8} {'HV60':>8} {'CurrIV':>8} {'IVRank':>8}")
        print("-" * 80)

        for symbol, data in sorted(results.items()):
            print(f"{symbol:<8} {len(data.price_bars):>6} {len(data.iv_data):>7} "
                  f"{data.hv20*100 if data.hv20 else 0:>7.1f}% "
                  f"{data.hv60*100 if data.hv60 else 0:>7.1f}% "
                  f"{data.current_iv*100 if data.current_iv else 0:>7.1f}% "
                  f"{data.iv_rank_252*100 if data.iv_rank_252 else 0:>7.1f}%")

    finally:
        fetcher.disconnect()

    return 0


if __name__ == "__main__":
    exit(main())
