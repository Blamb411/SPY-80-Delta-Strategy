"""
FRED Client for Valuation Metrics
=================================
Fetches economic and valuation data from FRED (Federal Reserve Economic Data)
and other public sources.

Primary use: CAPE (Cyclically Adjusted P/E) / Shiller P/E ratio.

Note: CAPE is not directly available on FRED, so we use Shiller's Excel file
which is updated monthly at: http://www.econ.yale.edu/~shiller/data.htm

This module also provides a fallback using multpl.com data.

Usage:
    from backtest.fred_client import FREDClient
    client = FREDClient()
    cape_data = client.fetch_cape("2015-01-01", "2024-12-31")
"""

import os
import sqlite3
import logging
from datetime import datetime, date
from typing import Dict, Optional, List
import pandas as pd

log = logging.getLogger("fred_client")

# Database path (same directory as this file)
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "valuation_cache.db")

# Shiller data URL (Excel file updated monthly)
SHILLER_URL = "http://www.econ.yale.edu/~shiller/data/ie_data.xls"

# Alternative: multpl.com CSV export
MULTPL_CAPE_URL = "https://www.multpl.com/shiller-pe/table/by-month"


# ======================================================================
# DATABASE SCHEMA
# ======================================================================

_DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS cape_monthly (
    date TEXT PRIMARY KEY,
    cape REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS sp500_pe (
    date TEXT PRIMARY KEY,
    pe REAL,
    forward_pe REAL
);

CREATE TABLE IF NOT EXISTS fetch_log (
    fetch_key TEXT PRIMARY KEY,
    fetched_at TEXT NOT NULL,
    row_count INTEGER DEFAULT 0
);
"""


def _init_db(db_path: str = DB_PATH) -> sqlite3.Connection:
    """Initialize database and return connection."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(_DB_SCHEMA)
    conn.commit()
    return conn


def _fetch_logged(conn: sqlite3.Connection, key: str) -> bool:
    """Check if a fetch has already been completed."""
    row = conn.execute(
        "SELECT 1 FROM fetch_log WHERE fetch_key = ?", (key,)
    ).fetchone()
    return row is not None


def _log_fetch(conn: sqlite3.Connection, key: str, row_count: int = 0) -> None:
    """Record a completed fetch."""
    conn.execute(
        "INSERT OR REPLACE INTO fetch_log (fetch_key, fetched_at, row_count) VALUES (?, ?, ?)",
        (key, datetime.now().isoformat(), row_count),
    )
    conn.commit()


# ======================================================================
# FRED CLIENT
# ======================================================================

class FREDClient:
    """
    Client for fetching valuation metrics from public sources.

    Primary data sources:
    - Shiller's CAPE data from Yale (monthly, ~1880-present)
    - FRED API for other economic indicators

    All data is cached in SQLite for offline use.
    """

    def __init__(self, db_path: str = DB_PATH, api_key: str = None):
        """
        Initialize the FRED client.

        Args:
            db_path: Path to SQLite cache database
            api_key: FRED API key (optional, for advanced queries)
        """
        self.db_path = db_path
        self.api_key = api_key
        self.conn = _init_db(db_path)

    def fetch_cape(self, start: str = "2000-01-01", end: str = None) -> Dict[str, float]:
        """
        Fetch Shiller CAPE (Cyclically Adjusted P/E) data.

        Data is monthly. For daily trading, the previous month's value is used.

        Args:
            start: Start date (YYYY-MM-DD)
            end: End date (YYYY-MM-DD), defaults to today

        Returns:
            Dict mapping YYYY-MM-DD (first of month) -> CAPE value
        """
        if end is None:
            end = date.today().isoformat()

        cache_key = "cape_all"

        # Check cache first
        cached = self.conn.execute(
            "SELECT date, cape FROM cape_monthly WHERE date >= ? AND date <= ? ORDER BY date",
            (start[:7] + "-01", end),
        ).fetchall()

        if cached:
            return {r["date"]: r["cape"] for r in cached}

        # Try to fetch from Shiller's Excel file
        log.info("Fetching CAPE data from Shiller's Excel file...")
        cape_data = self._fetch_shiller_cape()

        if not cape_data:
            log.warning("Failed to fetch Shiller data. Trying alternative sources...")
            cape_data = self._fetch_multpl_cape()

        if cape_data:
            # Store in cache
            for dt, cape in cape_data.items():
                self.conn.execute(
                    "INSERT OR REPLACE INTO cape_monthly (date, cape) VALUES (?, ?)",
                    (dt, cape),
                )
            self.conn.commit()
            _log_fetch(self.conn, cache_key, len(cape_data))
            log.info("  CAPE: %d months loaded", len(cape_data))

        # Return requested range
        result = {}
        for dt, cape in cape_data.items():
            if start <= dt <= end:
                result[dt] = cape

        return result

    def _fetch_shiller_cape(self) -> Dict[str, float]:
        """Fetch CAPE from Shiller's Excel file."""
        try:
            import requests
            from io import BytesIO

            log.info("Downloading Shiller data from %s", SHILLER_URL)
            resp = requests.get(SHILLER_URL, timeout=30)
            resp.raise_for_status()

            # Read Excel file, sheet "Data", skip first few rows
            df = pd.read_excel(
                BytesIO(resp.content),
                sheet_name="Data",
                skiprows=7,  # Headers start at row 8
                usecols=[0, 1, 2, 25],  # Date, P, D, CAPE columns
            )

            # Rename columns
            df.columns = ["Date", "Price", "Dividend", "CAPE"]

            # Parse dates (format: YYYY.MM like 2024.01)
            result = {}
            for _, row in df.iterrows():
                date_val = row["Date"]
                cape_val = row["CAPE"]

                if pd.isna(date_val) or pd.isna(cape_val):
                    continue

                # Convert YYYY.MM to YYYY-MM-01
                try:
                    if isinstance(date_val, float):
                        year = int(date_val)
                        month = int(round((date_val - year) * 100))
                        if month == 0:
                            month = 1
                        dt = f"{year:04d}-{month:02d}-01"
                    else:
                        continue

                    result[dt] = float(cape_val)
                except (ValueError, TypeError):
                    continue

            return result

        except Exception as e:
            log.error("Error fetching Shiller data: %s", e)
            return {}

    def _fetch_multpl_cape(self) -> Dict[str, float]:
        """
        Fetch CAPE from multpl.com as fallback.

        Note: This is a scraping approach and may break if the site changes.
        """
        try:
            import requests
            from bs4 import BeautifulSoup

            log.info("Fetching CAPE from multpl.com...")
            headers = {"User-Agent": "Mozilla/5.0"}
            resp = requests.get(MULTPL_CAPE_URL, headers=headers, timeout=30)
            resp.raise_for_status()

            soup = BeautifulSoup(resp.text, "html.parser")
            table = soup.find("table", {"id": "datatable"})

            if not table:
                log.error("Could not find data table on multpl.com")
                return {}

            result = {}
            rows = table.find_all("tr")[1:]  # Skip header

            for row in rows:
                cells = row.find_all("td")
                if len(cells) >= 2:
                    date_str = cells[0].text.strip()
                    cape_str = cells[1].text.strip()

                    try:
                        # Parse "Jan 01, 2024" format
                        dt = datetime.strptime(date_str, "%b %d, %Y")
                        cape = float(cape_str)
                        result[dt.strftime("%Y-%m-01")] = cape
                    except (ValueError, TypeError):
                        continue

            return result

        except ImportError:
            log.error("BeautifulSoup not installed. pip install beautifulsoup4")
            return {}
        except Exception as e:
            log.error("Error fetching from multpl.com: %s", e)
            return {}

    def get_cape_for_date(self, query_date: str) -> Optional[float]:
        """
        Get CAPE value for a specific date.

        Since CAPE is monthly, returns the most recent month's value.

        Args:
            query_date: Date in YYYY-MM-DD format

        Returns:
            CAPE value or None if not available
        """
        # Find the most recent month on or before the query date
        row = self.conn.execute(
            "SELECT cape FROM cape_monthly WHERE date <= ? ORDER BY date DESC LIMIT 1",
            (query_date,),
        ).fetchone()

        return row["cape"] if row else None

    def get_cape_series(self, start: str, end: str) -> Dict[str, float]:
        """
        Get CAPE time series for a date range.

        Args:
            start: Start date (YYYY-MM-DD)
            end: End date (YYYY-MM-DD)

        Returns:
            Dict mapping date -> CAPE value (monthly, first of month)
        """
        rows = self.conn.execute(
            "SELECT date, cape FROM cape_monthly WHERE date >= ? AND date <= ? ORDER BY date",
            (start, end),
        ).fetchall()

        return {r["date"]: r["cape"] for r in rows}

    def interpolate_cape_daily(self, trading_dates: List[str]) -> Dict[str, float]:
        """
        Create a daily CAPE series by forward-filling monthly values.

        Args:
            trading_dates: List of trading dates (YYYY-MM-DD)

        Returns:
            Dict mapping each trading date -> CAPE value
        """
        if not trading_dates:
            return {}

        # Get all CAPE data
        start = trading_dates[0][:7] + "-01"
        end = trading_dates[-1]

        cape_monthly = self.get_cape_series(start, end)

        if not cape_monthly:
            return {}

        # Forward-fill to daily
        result = {}
        sorted_months = sorted(cape_monthly.keys())

        for trade_date in trading_dates:
            # Find the most recent month
            for month in reversed(sorted_months):
                if month <= trade_date:
                    result[trade_date] = cape_monthly[month]
                    break

        return result

    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None


# ======================================================================
# SELF-TEST
# ======================================================================

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)s  %(message)s",
        datefmt="%H:%M:%S",
    )

    print("=" * 60)
    print("FRED Client Self-Test")
    print("=" * 60)

    client = FREDClient()

    print("\n--- Test 1: Fetch CAPE Data ---")
    cape_data = client.fetch_cape("2020-01-01", "2024-12-31")
    print(f"  CAPE months: {len(cape_data)}")
    if cape_data:
        dates = sorted(cape_data.keys())
        print(f"  First: {dates[0]} = {cape_data[dates[0]]:.2f}")
        print(f"  Last:  {dates[-1]} = {cape_data[dates[-1]]:.2f}")

    print("\n--- Test 2: Get CAPE for Specific Date ---")
    test_date = "2024-06-15"
    cape_val = client.get_cape_for_date(test_date)
    if cape_val:
        print(f"  CAPE on {test_date}: {cape_val:.2f}")
    else:
        print(f"  No CAPE data for {test_date}")

    print("\n--- Test 3: Interpolate to Daily ---")
    # Generate some sample trading dates
    sample_dates = [
        "2024-01-02", "2024-01-15", "2024-02-01", "2024-02-15",
        "2024-03-01", "2024-03-15", "2024-04-01",
    ]
    daily_cape = client.interpolate_cape_daily(sample_dates)
    print(f"  Daily CAPE values: {len(daily_cape)}")
    for dt in sorted(daily_cape.keys())[:5]:
        print(f"    {dt}: {daily_cape[dt]:.2f}")

    print(f"\n  Database: {client.db_path}")
    if os.path.exists(client.db_path):
        print(f"  Size: {os.path.getsize(client.db_path) / 1024:.1f} KB")

    client.close()
    print("\nDone.")
