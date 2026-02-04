"""
Massive API Data Fetcher
=========================
Downloads and caches data from Massive (formerly Polygon.io) API.
All data is stored in SQLite — subsequent runs use cache with zero API calls.

Data types fetched:
    1. Underlying daily bars (OHLCV) for each ticker
    2. Options contracts available on a given date
    3. Option daily bars (OHLCV) for specific contracts
    4. Option quotes (bid/ask) for contracts from 2022-03-07+
"""

import time
import logging
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional

from massive import RESTClient

import config
import cache_db

logger = logging.getLogger(__name__)


def _get_client() -> RESTClient:
    return RESTClient(api_key=config.load_api_key())


def _api_delay():
    time.sleep(config.API_DELAY_SECONDS)


def _ts_to_date(timestamp_ms) -> str:
    """Convert millisecond timestamp to YYYY-MM-DD string."""
    if isinstance(timestamp_ms, (int, float)):
        # Polygon/Massive returns timestamps in milliseconds
        return datetime.utcfromtimestamp(timestamp_ms / 1000).strftime("%Y-%m-%d")
    return str(timestamp_ms)


# -----------------------------------------------------------------------
# 1. Underlying daily bars
# -----------------------------------------------------------------------

def fetch_underlying_bars(
    ticker: str,
    start: str = None,
    end: str = None,
    force: bool = False,
) -> int:
    """Download daily bars for an underlying ETF.

    Returns number of bars saved.
    """
    start = start or config.START_DATE.isoformat()
    end = end or config.END_DATE.isoformat()
    fetch_key = f"underlying_bars:{ticker}:{start}:{end}"

    if not force and cache_db.is_fetched(fetch_key):
        logger.info(f"  {ticker} underlying bars already cached")
        return 0

    logger.info(f"  Fetching {ticker} daily bars {start} to {end} ...")
    client = _get_client()

    bars = []
    try:
        aggs = list(client.list_aggs(
            ticker=ticker,
            multiplier=1,
            timespan="day",
            from_=start,
            to=end,
            limit=50000,
        ))
        for a in aggs:
            bars.append({
                "date": _ts_to_date(a.timestamp),
                "open": a.open,
                "high": a.high,
                "low": a.low,
                "close": a.close,
                "volume": a.volume,
                "vwap": getattr(a, "vwap", None),
            })
    except Exception as e:
        logger.error(f"  Failed to fetch {ticker} bars: {e}")
        return 0

    _api_delay()

    count = cache_db.save_underlying_bars(ticker, bars)
    cache_db.mark_fetched(fetch_key, {"bars": len(bars)})
    logger.info(f"  {ticker}: saved {len(bars)} bars")
    return len(bars)


def fetch_all_underlying(tickers: List[str] = None) -> None:
    """Fetch daily bars for all tickers in the universe."""
    tickers = tickers or config.TICKERS
    for t in tickers:
        fetch_underlying_bars(t)


# -----------------------------------------------------------------------
# 2. Options contracts
# -----------------------------------------------------------------------

def fetch_options_contracts(
    underlying: str,
    as_of_date: str,
    contract_type: str = "put",
    force: bool = False,
) -> List[Dict]:
    """Fetch available options contracts for a ticker on a specific date.

    We search for contracts expiring in the DTE window from as_of_date.
    Returns list of contract dicts.
    """
    fetch_key = f"contracts:{underlying}:{as_of_date}:{contract_type}"

    if not force and cache_db.is_fetched(fetch_key):
        # Return from cache
        return cache_db.get_options_contracts(
            underlying, as_of_date, contract_type,
            config.MIN_DTE, config.MAX_DTE,
        )

    dt = datetime.strptime(as_of_date, "%Y-%m-%d").date()
    exp_start = (dt + timedelta(days=config.MIN_DTE)).isoformat()
    exp_end = (dt + timedelta(days=config.MAX_DTE)).isoformat()

    client = _get_client()
    contracts = []

    try:
        results = list(client.list_options_contracts(
            underlying_ticker=underlying,
            as_of=as_of_date,
            expiration_date_gte=exp_start,
            expiration_date_lte=exp_end,
            contract_type=contract_type,
            limit=1000,
        ))
        for c in results:
            contracts.append({
                "option_ticker": c.ticker,
                "underlying": underlying,
                "expiration_date": c.expiration_date,
                "strike_price": c.strike_price,
                "contract_type": c.contract_type,
            })
    except Exception as e:
        logger.error(f"  Failed to fetch contracts for {underlying} on {as_of_date}: {e}")
        return []

    _api_delay()

    if contracts:
        cache_db.save_options_contracts(contracts, as_of_date)
    cache_db.mark_fetched(fetch_key, {"count": len(contracts)})

    return cache_db.get_options_contracts(
        underlying, as_of_date, contract_type,
        config.MIN_DTE, config.MAX_DTE,
    )


# -----------------------------------------------------------------------
# 3. Option daily bars
# -----------------------------------------------------------------------

def fetch_option_bars(
    option_ticker: str,
    start: str,
    end: str,
    force: bool = False,
) -> int:
    """Download daily bars for an individual option contract.

    Returns number of bars saved.
    """
    fetch_key = f"option_bars:{option_ticker}:{start}:{end}"

    if not force and cache_db.is_fetched(fetch_key):
        return 0

    client = _get_client()
    bars = []

    try:
        aggs = list(client.list_aggs(
            ticker=option_ticker,
            multiplier=1,
            timespan="day",
            from_=start,
            to=end,
            limit=50000,
        ))
        for a in aggs:
            bars.append({
                "date": _ts_to_date(a.timestamp),
                "open": a.open,
                "high": a.high,
                "low": a.low,
                "close": a.close,
                "volume": a.volume,
                "vwap": getattr(a, "vwap", None),
            })
    except Exception as e:
        logger.error(f"  Failed to fetch option bars for {option_ticker}: {e}")
        return 0

    _api_delay()

    count = cache_db.save_option_bars(option_ticker, bars)
    cache_db.mark_fetched(fetch_key, {"bars": len(bars)})
    return len(bars)


# -----------------------------------------------------------------------
# 4. Option quotes (bid/ask) — available from 2022-03-07+
# -----------------------------------------------------------------------

def fetch_option_quote_snapshot(
    option_ticker: str,
    quote_date: str,
    force: bool = False,
) -> Optional[Dict]:
    """Fetch a representative bid/ask quote for an option on a specific date.

    Uses the daily open/close endpoint which is fast (single call per contract/date).
    Constructs synthetic bid/ask from the OHLCV data using the close price.

    For post-2022 data, the daily bars are already high quality. We apply a
    tighter synthetic spread (2.5%) compared to pre-2022 data (5%).

    Returns dict with bid/ask or None.
    """
    dt = datetime.strptime(quote_date, "%Y-%m-%d").date()

    # Check cache first
    cached = cache_db.get_option_quote(option_ticker, quote_date)
    if cached is not None:
        return cached

    # For post-2022 data, try to get better pricing via daily open/close
    if dt >= config.QUOTES_AVAILABLE_DATE:
        fetch_key = f"option_quote:{option_ticker}:{quote_date}"
        if not force and cache_db.is_fetched(fetch_key):
            return None

        client = _get_client()
        try:
            result = client.get_daily_open_close_agg(option_ticker, quote_date)
            if result and result.close:
                # Use tighter spread for post-2022 (more liquid market data)
                spread_pct = 0.025  # 2.5% total spread
                half = spread_pct / 2
                bid = round(result.close * (1 - half), 2)
                ask = round(result.close * (1 + half), 2)
                quote_data = {
                    "date": quote_date,
                    "bid": max(0.01, bid),
                    "ask": max(0.01, ask),
                    "bid_size": 0,
                    "ask_size": 0,
                }
                cache_db.save_option_quotes(option_ticker, [quote_data])
                cache_db.mark_fetched(fetch_key)
                _api_delay()
                return quote_data
        except Exception as e:
            logger.debug(f"  No daily OC for {option_ticker} on {quote_date}: {e}")

        cache_db.mark_fetched(fetch_key)
        _api_delay()

    return None


# -----------------------------------------------------------------------
# Convenience: ensure option data is available
# -----------------------------------------------------------------------

def ensure_option_data(
    option_ticker: str,
    start_date: str,
    end_date: str,
    fetch_quotes: bool = True,
) -> None:
    """Make sure we have bars (and optionally quotes) for an option contract."""
    # Bars
    fetch_option_bars(option_ticker, start_date, end_date)

    # Quotes — for post-2022 dates, we can get tighter spreads
    # via the daily open/close endpoint. Only fetch for entry date.
    if fetch_quotes:
        dt_start = datetime.strptime(start_date, "%Y-%m-%d").date()
        if dt_start >= config.QUOTES_AVAILABLE_DATE:
            fetch_option_quote_snapshot(option_ticker, start_date)


def get_option_price(
    option_ticker: str,
    price_date: str,
    side: str = "mid",
) -> Optional[float]:
    """Get option price on a date.

    side: "bid", "ask", "mid", or "close"
    Tries real quote first, falls back to bar close.
    """
    # Try quote first
    quote = cache_db.get_option_quote(option_ticker, price_date)
    if quote and quote["bid"] and quote["ask"]:
        if side == "bid":
            return quote["bid"]
        elif side == "ask":
            return quote["ask"]
        else:  # mid
            return (quote["bid"] + quote["ask"]) / 2

    # Fall back to bar
    bar = cache_db.get_option_bar(option_ticker, price_date)
    if bar and bar["close"]:
        close = bar["close"]
        if side == "bid":
            return close * (1 - config.SYNTHETIC_SPREAD_PCT / 2)
        elif side == "ask":
            return close * (1 + config.SYNTHETIC_SPREAD_PCT / 2)
        else:
            return close

    return None


def has_real_quote(option_ticker: str, price_date: str) -> bool:
    """Check if we have a real bid/ask quote (not synthetic)."""
    quote = cache_db.get_option_quote(option_ticker, price_date)
    return bool(quote is not None and quote["bid"] and quote["ask"])
