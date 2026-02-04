"""
Polygon/Massive API Client — Momentum Data
============================================
Wraps the existing fetch_prices.py and price_cache.db to provide
momentum factor data (3m, 6m, 12m returns).
"""

import os
import sys
import sqlite3
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
import db_schema


class PolygonClient:
    """Reads price data from the existing price_cache.db and fetches
    missing data via the Massive API."""

    def __init__(self, db_path: str = config.PRICE_CACHE_DB):
        self.price_db_path = db_path
        self.scoring_db_path = config.DB_PATH
        self._client = None

    def _get_rest_client(self):
        """Lazy-load the Massive REST client."""
        if self._client is None:
            from massive import RESTClient
            api_key = config.load_api_key(config.MASSIVE_API_KEY_FILE)
            self._client = RESTClient(api_key=api_key)
        return self._client

    def _get_price_conn(self) -> sqlite3.Connection:
        """Connect to the price cache database."""
        conn = sqlite3.connect(self.price_db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def get_prices(self, symbol: str, start_date: str, end_date: str) -> List[Tuple[str, float]]:
        """Get (date, close) pairs from price_cache.db."""
        conn = self._get_price_conn()
        rows = conn.execute(
            """SELECT date, close FROM daily_prices
               WHERE symbol = ? AND date >= ? AND date <= ?
               ORDER BY date""",
            (symbol, start_date, end_date),
        ).fetchall()
        conn.close()
        return [(row["date"], row["close"]) for row in rows]

    def get_close_on_date(self, symbol: str, target_date: str,
                          lookback_days: int = 5) -> Optional[float]:
        """Get closing price on or near a date (looks back up to lookback_days)."""
        conn = self._get_price_conn()
        row = conn.execute(
            """SELECT close FROM daily_prices
               WHERE symbol = ? AND date <= ? AND date >= date(?, ?)
               ORDER BY date DESC LIMIT 1""",
            (symbol, target_date, target_date, f"-{lookback_days} days"),
        ).fetchone()
        conn.close()
        return row["close"] if row else None

    def fetch_prices_if_needed(self, symbol: str, start_date: str, end_date: str) -> int:
        """Fetch prices from API if not cached. Returns bar count."""
        conn = self._get_price_conn()

        # Check if we have data for this range
        row = conn.execute(
            """SELECT COUNT(*) as cnt FROM daily_prices
               WHERE symbol = ? AND date >= ? AND date <= ?""",
            (symbol, start_date, end_date),
        ).fetchone()

        if row["cnt"] > 0:
            conn.close()
            return row["cnt"]

        conn.close()

        # Fetch from API
        client = self._get_rest_client()
        print(f"  Fetching prices for {symbol} ({start_date} to {end_date})...", end=" ", flush=True)

        try:
            aggs = list(client.list_aggs(
                ticker=symbol,
                multiplier=1,
                timespan="day",
                from_=start_date,
                to=end_date,
                limit=50000,
            ))
        except Exception as e:
            print(f"ERROR: {e}")
            return 0

        if not aggs:
            print("no data")
            return 0

        conn = self._get_price_conn()
        count = 0
        for bar in aggs:
            date_str = datetime.fromtimestamp(bar.timestamp / 1000).strftime("%Y-%m-%d")
            conn.execute(
                "INSERT OR REPLACE INTO daily_prices VALUES (?, ?, ?, ?, ?, ?, ?)",
                (symbol, date_str, bar.open, bar.high, bar.low, bar.close, bar.volume),
            )
            count += 1
        conn.commit()
        conn.close()

        print(f"{count} bars")
        time.sleep(config.POLYGON_DELAY_SECONDS)
        return count

    def compute_returns(self, symbol: str, as_of_date: str) -> Dict[str, Optional[float]]:
        """
        Compute 3m, 6m, 12m price returns as of a given date.
        Returns dict with keys: price_return_3m, price_return_6m, price_return_12m
        """
        as_of = datetime.strptime(as_of_date, "%Y-%m-%d")

        # Need prices going back 12 months + buffer
        start_date = (as_of - timedelta(days=400)).strftime("%Y-%m-%d")

        # Ensure data is available
        self.fetch_prices_if_needed(symbol, start_date, as_of_date)

        prices = self.get_prices(symbol, start_date, as_of_date)
        if not prices:
            return {
                "price_return_3m": None,
                "price_return_6m": None,
                "price_return_12m": None,
            }

        # Build a date->price lookup
        price_map = {d: p for d, p in prices}
        current_price = prices[-1][1] if prices else None

        if current_price is None:
            return {
                "price_return_3m": None,
                "price_return_6m": None,
                "price_return_12m": None,
            }

        returns = {}
        for label, months in [("price_return_3m", 3), ("price_return_6m", 6), ("price_return_12m", 12)]:
            target = as_of - timedelta(days=months * 30)
            # Find closest trading day
            past_price = self._find_nearest_price(price_map, target, lookback=10)
            if past_price and past_price > 0:
                returns[label] = (current_price / past_price - 1) * 100  # percentage
            else:
                returns[label] = None

        return returns

    def _find_nearest_price(self, price_map: Dict[str, float],
                            target: datetime, lookback: int = 10) -> Optional[float]:
        """Find the closest price to a target date within lookback days."""
        for offset in range(lookback + 1):
            date_str = (target - timedelta(days=offset)).strftime("%Y-%m-%d")
            if date_str in price_map:
                return price_map[date_str]
        return None

    def save_momentum_metrics(self, symbol: str, as_of_date: str,
                              returns: Dict[str, Optional[float]]) -> None:
        """Save computed return metrics to fundamental_data table."""
        conn = db_schema.get_connection(self.scoring_db_path)
        for metric_name, value in returns.items():
            if value is not None:
                conn.execute(
                    """INSERT OR REPLACE INTO fundamental_data
                       (symbol, as_of_date, metric_name, fiscal_period, metric_value, source, fetched_at)
                       VALUES (?, ?, ?, 'TTM', ?, 'polygon', ?)""",
                    (symbol, as_of_date, metric_name, float(value),
                     datetime.utcnow().isoformat()),
                )
        conn.commit()
        conn.close()

    def fetch_momentum_batch(self, symbols: List[str], as_of_date: str) -> int:
        """Compute and save momentum for a batch of symbols."""
        fetched = 0
        total = len(symbols)
        for i, symbol in enumerate(symbols, 1):
            try:
                returns = self.compute_returns(symbol, as_of_date)
                self.save_momentum_metrics(symbol, as_of_date, returns)
                fetched += 1
            except Exception as e:
                print(f"  Error on {symbol}: {e}")
            if i % 25 == 0:
                print(f"  Momentum progress: {i}/{total}")
        return fetched
