#!/usr/bin/env python3
"""
ThetaData REST API Client with SQLite Caching (v3 API)
========================================================
Robust client for ThetaData's historical options data API.
Provides cache-first access to bid/ask quotes, EOD data, Greeks,
expirations, and strikes. Uses SQLite for persistent caching so
re-runs require zero API calls.

Also includes Yahoo Finance helpers for VIX and SPY daily data.

Requires Theta Terminal v3 running locally.

API v3 changes from v2:
  - Endpoints: /v2/list/expirations -> /v3/option/list/expirations
  - Params: root -> symbol, exp -> expiration (YYYYMMDD)
  - Strikes returned in dollars (not millicents)
  - Response format: list of objects, not header+rows
  - Quotes: /v3/option/history/quote requires interval param
  - EOD: /v3/option/history/eod includes bid/ask directly
"""

import os
import sys
import json
import time
import sqlite3
import logging
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional, Dict, List, Any, Tuple

try:
    import requests
    import pandas as pd
except ImportError:
    print("Missing required packages. Install with:")
    print("  pip install requests pandas")
    sys.exit(1)

log = logging.getLogger("thetadata_client")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
ALTERNATE_PORTS = [25503, 25510, 25511]
INTER_CALL_DELAY = 0.05  # 50ms between API calls
MAX_RETRIES = 3
RETRY_BACKOFF = [1.0, 2.0, 4.0]

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "thetadata_cache.db")


# ---------------------------------------------------------------------------
# Database layer
# ---------------------------------------------------------------------------

_DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS underlying_bars (
    ticker TEXT NOT NULL,
    bar_date TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    PRIMARY KEY (ticker, bar_date)
);

CREATE TABLE IF NOT EXISTS vix_daily (
    bar_date TEXT PRIMARY KEY,
    close REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS option_expirations (
    root TEXT NOT NULL,
    as_of_date TEXT NOT NULL,
    expiration TEXT NOT NULL,
    PRIMARY KEY (root, as_of_date, expiration)
);

CREATE TABLE IF NOT EXISTS option_strikes (
    root TEXT NOT NULL,
    expiration TEXT NOT NULL,
    strike REAL NOT NULL,
    PRIMARY KEY (root, expiration, strike)
);

CREATE TABLE IF NOT EXISTS option_quotes (
    root TEXT NOT NULL,
    expiration TEXT NOT NULL,
    strike REAL NOT NULL,
    right TEXT NOT NULL,
    quote_date TEXT NOT NULL,
    bid REAL,
    ask REAL,
    bid_size INTEGER,
    ask_size INTEGER,
    PRIMARY KEY (root, expiration, strike, right, quote_date)
);

CREATE TABLE IF NOT EXISTS option_eod (
    root TEXT NOT NULL,
    expiration TEXT NOT NULL,
    strike REAL NOT NULL,
    right TEXT NOT NULL,
    bar_date TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    open_interest REAL,
    bid REAL,
    ask REAL,
    PRIMARY KEY (root, expiration, strike, right, bar_date)
);

CREATE TABLE IF NOT EXISTS option_greeks (
    root TEXT NOT NULL,
    expiration TEXT NOT NULL,
    strike REAL NOT NULL,
    right TEXT NOT NULL,
    greeks_date TEXT NOT NULL,
    iv REAL,
    delta REAL,
    gamma REAL,
    vega REAL,
    theta REAL,
    rho REAL,
    PRIMARY KEY (root, expiration, strike, right, greeks_date)
);

CREATE TABLE IF NOT EXISTS spy_dividends (
    ex_date TEXT PRIMARY KEY,
    amount REAL NOT NULL
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
    # Add bid/ask columns to option_eod if they don't exist (migration)
    try:
        conn.execute("SELECT bid FROM option_eod LIMIT 1")
    except sqlite3.OperationalError:
        conn.execute("ALTER TABLE option_eod ADD COLUMN bid REAL")
        conn.execute("ALTER TABLE option_eod ADD COLUMN ask REAL")
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


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def _fmt_date(d) -> str:
    """Convert various date formats to YYYY-MM-DD string."""
    if isinstance(d, str):
        # Already YYYY-MM-DD
        if len(d) == 10 and d[4] == "-":
            return d
        # YYYYMMDD string
        if len(d) == 8 and d.isdigit():
            return f"{d[:4]}-{d[4:6]}-{d[6:]}"
        return d
    if isinstance(d, (date, datetime)):
        return d.strftime("%Y-%m-%d")
    if isinstance(d, (int, float)):
        s = str(int(d))
        if len(s) == 8:
            return f"{s[:4]}-{s[4:6]}-{s[6:]}"
    return str(d)


def _to_int_date(d: str) -> int:
    """Convert YYYY-MM-DD to YYYYMMDD integer."""
    return int(d.replace("-", ""))


def _from_int_date(d) -> str:
    """Convert YYYYMMDD integer to YYYY-MM-DD string."""
    s = str(int(d))
    return f"{s[:4]}-{s[4:6]}-{s[6:]}"


# ---------------------------------------------------------------------------
# ThetaDataClient
# ---------------------------------------------------------------------------

class ThetaDataClient:
    """
    ThetaData REST API v3 client with SQLite caching.

    All data methods are cache-first: if the data exists in the local
    SQLite database, it is returned immediately. Otherwise, the API
    is called and results are cached for future use.
    """

    def __init__(self, db_path: str = DB_PATH, base_url: str = None):
        self.db_path = db_path
        self.base_url = base_url
        self.session = requests.Session()
        self.conn = _init_db(db_path)
        self._last_call_time = 0.0

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def find_active_port(self) -> Optional[str]:
        """Find the port where Theta Terminal is running."""
        for port in ALTERNATE_PORTS:
            url = f"http://localhost:{port}"
            try:
                # v2 status returns 200 on v2 terminal, 410 on v3 terminal
                # Either means the terminal is running
                resp = self.session.get(
                    f"{url}/v2/system/terminal/status", timeout=3
                )
                if resp.status_code in (200, 410):
                    log.info("Found Theta Terminal on port %d", port)
                    return url
            except requests.exceptions.ConnectionError:
                continue
        return None

    def connect(self) -> bool:
        """Test connection to Theta Terminal. Returns True on success."""
        if self.base_url is None:
            self.base_url = self.find_active_port()

        if self.base_url is None:
            print("\nERROR: Could not connect to Theta Terminal.")
            print("\nPlease ensure:")
            print("  1. Theta Terminal is installed")
            print("  2. Theta Terminal is running: java -jar ThetaTerminal.jar")
            print("  3. You are logged in with your ThetaData credentials")
            print("  4. creds.txt exists in the Theta Terminal folder")
            return False

        try:
            resp = self.session.get(
                f"{self.base_url}/v2/system/terminal/status", timeout=5
            )
            # v3 terminal returns 410 on v2 endpoints -- still means connected
            if resp.status_code in (200, 410):
                log.info("Connected to Theta Terminal (v3) at %s", self.base_url)
                return True
        except Exception as e:
            log.error("Connection error: %s", e)

        return False

    # ------------------------------------------------------------------
    # Low-level API call with retry + rate limiting
    # ------------------------------------------------------------------

    def _api_call(self, endpoint: str, params: Dict[str, Any],
                  timeout: int = 30) -> Optional[Any]:
        """
        Make a rate-limited API call with retry logic.

        Returns parsed JSON or None on failure.
        """
        if self.base_url is None:
            if not self.connect():
                return None

        url = f"{self.base_url}{endpoint}"

        for attempt in range(MAX_RETRIES):
            # Rate limiting
            elapsed = time.time() - self._last_call_time
            if elapsed < INTER_CALL_DELAY:
                time.sleep(INTER_CALL_DELAY - elapsed)

            try:
                self._last_call_time = time.time()
                resp = self.session.get(url, params=params, timeout=timeout)

                if resp.status_code == 200:
                    return resp.json()

                # 472 = "No data found" -- not an error, just no data
                if resp.status_code == 472:
                    return {"response": []}

                if resp.status_code in (429, 500, 502, 503, 504):
                    wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
                    log.warning(
                        "API %d on %s, retry %d/%d in %.0fs",
                        resp.status_code, endpoint, attempt + 1, MAX_RETRIES, wait,
                    )
                    time.sleep(wait)
                    continue

                log.error("API error %d on %s: %s",
                          resp.status_code, endpoint, resp.text[:300])
                return None

            except requests.exceptions.ConnectionError:
                if attempt < MAX_RETRIES - 1:
                    wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
                    log.warning(
                        "Connection error on %s, retry %d/%d in %.0fs",
                        endpoint, attempt + 1, MAX_RETRIES, wait,
                    )
                    time.sleep(wait)
                else:
                    log.error(
                        "Connection failed after %d retries. "
                        "Is Theta Terminal running?", MAX_RETRIES,
                    )
                    return None
            except requests.exceptions.Timeout:
                if attempt < MAX_RETRIES - 1:
                    wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
                    log.warning("Timeout on %s, retry %d/%d", endpoint, attempt + 1, MAX_RETRIES)
                    time.sleep(wait)
                else:
                    log.error("Timeout after %d retries on %s", MAX_RETRIES, endpoint)
                    return None

        return None

    # ------------------------------------------------------------------
    # Expirations
    # ------------------------------------------------------------------

    def get_expirations(self, root: str, as_of_date: str = None) -> List[str]:
        """
        Get available expiration dates for an option root.

        Returns list of expiration dates as YYYY-MM-DD strings.
        Results are cached per (root, as_of_date).
        """
        if as_of_date is None:
            as_of_date = date.today().isoformat()

        cache_key = f"expirations:{root}:{as_of_date}"

        # Check cache
        cached = self.conn.execute(
            "SELECT expiration FROM option_expirations WHERE root = ? AND as_of_date = ? ORDER BY expiration",
            (root, as_of_date),
        ).fetchall()
        if cached:
            return [r["expiration"] for r in cached]

        # v3 API call
        data = self._api_call("/v3/option/list/expirations", {
            "symbol": root,
            "format": "json",
        })
        if data is None:
            return []

        # v3 returns: {"response": [{"symbol":"SPY","expiration":"2012-06-01"}, ...]}
        response = data.get("response", [])
        expirations = []
        for item in response:
            exp = item.get("expiration", "")
            if exp:
                # Already YYYY-MM-DD format in v3
                expirations.append(exp)

        # Cache
        for exp in expirations:
            self.conn.execute(
                "INSERT OR IGNORE INTO option_expirations (root, as_of_date, expiration) VALUES (?, ?, ?)",
                (root, as_of_date, exp),
            )
        self.conn.commit()
        _log_fetch(self.conn, cache_key, len(expirations))

        return expirations

    # ------------------------------------------------------------------
    # Strikes
    # ------------------------------------------------------------------

    def get_strikes(self, root: str, expiration: str) -> List[float]:
        """
        Get available strikes for an option root and expiration.

        Returns list of strike prices as floats, sorted ascending.
        """
        # Check cache
        cached = self.conn.execute(
            "SELECT strike FROM option_strikes WHERE root = ? AND expiration = ? ORDER BY strike",
            (root, expiration),
        ).fetchall()
        if cached:
            return [r["strike"] for r in cached]

        # v3 API call
        exp_int = _to_int_date(expiration)
        data = self._api_call("/v3/option/list/strikes", {
            "symbol": root,
            "expiration": exp_int,
            "format": "json",
        })
        if data is None:
            return []

        # v3 returns: {"response": [{"symbol":"SPY","strike":533.000}, ...]}
        response = data.get("response", [])
        strikes = sorted(item.get("strike", 0) for item in response)

        # Cache
        for s in strikes:
            self.conn.execute(
                "INSERT OR IGNORE INTO option_strikes (root, expiration, strike) VALUES (?, ?, ?)",
                (root, expiration, s),
            )
        self.conn.commit()
        cache_key = f"strikes:{root}:{expiration}"
        _log_fetch(self.conn, cache_key, len(strikes))

        return strikes

    # ------------------------------------------------------------------
    # Option EOD (OHLCV + bid/ask) — primary data source
    # ------------------------------------------------------------------

    def get_option_eod(self, root: str, expiration: str, strike: float,
                       right: str, start: str, end: str) -> List[Dict]:
        """
        Get historical EOD data for an option contract.
        v3 EOD includes bid/ask alongside OHLCV.

        Returns list of dicts with keys:
            bar_date, open, high, low, close, volume, bid, ask
        """
        cache_key = f"eod:{root}:{expiration}:{strike}:{right}:{start}:{end}"

        if _fetch_logged(self.conn, cache_key):
            rows = self.conn.execute(
                """SELECT bar_date, open, high, low, close, volume,
                          open_interest, bid, ask
                   FROM option_eod
                   WHERE root=? AND expiration=? AND strike=? AND right=?
                     AND bar_date >= ? AND bar_date <= ?
                   ORDER BY bar_date""",
                (root, expiration, strike, right, start, end),
            ).fetchall()
            return [dict(r) for r in rows]

        # v3 API call — right must be "call" or "put"
        right_v3 = "call" if right.upper() == "C" else "put"
        exp_int = _to_int_date(expiration)
        start_int = _to_int_date(start)
        end_int = _to_int_date(end)

        data = self._api_call("/v3/option/history/eod", {
            "symbol": root,
            "expiration": exp_int,
            "strike": f"{strike:.3f}",
            "right": right_v3,
            "start_date": start_int,
            "end_date": end_int,
            "format": "json",
        })

        results = []
        if data is not None:
            # v3 response: {"response": [{"contract":{...}, "data":[{...}, ...]}, ...]}
            response = data.get("response", [])
            for contract_block in response:
                data_rows = contract_block.get("data", [])
                for rec in data_rows:
                    # Extract date from various possible fields
                    # v3 EOD uses 'created' or 'last_trade' for date
                    ts = rec.get("created", rec.get("last_trade", ""))
                    if ts:
                        bar_date = ts[:10]  # YYYY-MM-DD from timestamp
                    else:
                        continue

                    entry = {
                        "bar_date": bar_date,
                        "open": float(rec.get("open", 0)),
                        "high": float(rec.get("high", 0)),
                        "low": float(rec.get("low", 0)),
                        "close": float(rec.get("close", 0)),
                        "volume": float(rec.get("volume", 0)),
                        "open_interest": float(rec.get("open_interest", 0)),
                        "bid": float(rec.get("bid", 0)),
                        "ask": float(rec.get("ask", 0)),
                    }
                    results.append(entry)

                    self.conn.execute(
                        """INSERT OR REPLACE INTO option_eod
                           (root, expiration, strike, right, bar_date,
                            open, high, low, close, volume, open_interest,
                            bid, ask)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (root, expiration, strike, right,
                         entry["bar_date"], entry["open"], entry["high"],
                         entry["low"], entry["close"], entry["volume"],
                         entry["open_interest"], entry["bid"], entry["ask"]),
                    )

        self.conn.commit()
        _log_fetch(self.conn, cache_key, len(results))

        # Also populate option_quotes from EOD bid/ask for cache coherence
        for entry in results:
            self.conn.execute(
                """INSERT OR IGNORE INTO option_quotes
                   (root, expiration, strike, right, quote_date, bid, ask, bid_size, ask_size)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0)""",
                (root, expiration, strike, right,
                 entry["bar_date"], entry["bid"], entry["ask"]),
            )
        self.conn.commit()

        return results

    # ------------------------------------------------------------------
    # Option quotes (bid/ask) — uses EOD endpoint as primary source
    # ------------------------------------------------------------------

    def get_option_quotes(self, root: str, expiration: str, strike: float,
                          right: str, start: str, end: str) -> List[Dict]:
        """
        Get historical EOD bid/ask quotes for an option contract.

        Uses the v3 EOD endpoint which includes bid/ask data.
        Returns list of dicts with keys: quote_date, bid, ask, bid_size, ask_size.
        """
        cache_key = f"quotes:{root}:{expiration}:{strike}:{right}:{start}:{end}"

        # Check if this exact range was already fetched
        if _fetch_logged(self.conn, cache_key):
            rows = self.conn.execute(
                """SELECT quote_date, bid, ask, bid_size, ask_size
                   FROM option_quotes
                   WHERE root=? AND expiration=? AND strike=? AND right=?
                     AND quote_date >= ? AND quote_date <= ?
                   ORDER BY quote_date""",
                (root, expiration, strike, right, start, end),
            ).fetchall()
            return [dict(r) for r in rows]

        # Fetch via EOD endpoint (which includes bid/ask in v3)
        eod_data = self.get_option_eod(root, expiration, strike, right, start, end)

        results = []
        for eod in eod_data:
            entry = {
                "quote_date": eod["bar_date"],
                "bid": eod.get("bid", 0.0),
                "ask": eod.get("ask", 0.0),
                "bid_size": 0,
                "ask_size": 0,
            }
            results.append(entry)

        _log_fetch(self.conn, cache_key, len(results))
        return results

    # ------------------------------------------------------------------
    # Option Greeks
    # ------------------------------------------------------------------

    def get_option_greeks(self, root: str, expiration: str, strike: float,
                          right: str, start: str, end: str) -> List[Dict]:
        """
        Get historical EOD Greeks and IV for an option contract.

        Returns list of dicts with keys: greeks_date, iv, delta, gamma, vega, theta, rho.
        """
        cache_key = f"greeks:{root}:{expiration}:{strike}:{right}:{start}:{end}"

        if _fetch_logged(self.conn, cache_key):
            rows = self.conn.execute(
                """SELECT greeks_date, iv, delta, gamma, vega, theta, rho
                   FROM option_greeks
                   WHERE root=? AND expiration=? AND strike=? AND right=?
                     AND greeks_date >= ? AND greeks_date <= ?
                   ORDER BY greeks_date""",
                (root, expiration, strike, right, start, end),
            ).fetchall()
            return [dict(r) for r in rows]

        # v3 API call
        right_v3 = "call" if right.upper() == "C" else "put"
        exp_int = _to_int_date(expiration)
        start_int = _to_int_date(start)
        end_int = _to_int_date(end)

        data = self._api_call("/v3/option/history/greeks/eod", {
            "symbol": root,
            "expiration": exp_int,
            "strike": f"{strike:.3f}",
            "right": right_v3,
            "start_date": start_int,
            "end_date": end_int,
            "format": "json",
        })

        results = []
        if data is not None:
            response = data.get("response", [])
            for contract_block in response:
                data_rows = contract_block.get("data", [])
                for rec in data_rows:
                    ts = rec.get("date", rec.get("timestamp", ""))
                    if ts:
                        greeks_date = ts[:10]
                    else:
                        continue

                    entry = {
                        "greeks_date": greeks_date,
                        "iv": float(rec.get("implied_vol", rec.get("iv", 0))),
                        "delta": float(rec.get("delta", 0)),
                        "gamma": float(rec.get("gamma", 0)),
                        "vega": float(rec.get("vega", 0)),
                        "theta": float(rec.get("theta", 0)),
                        "rho": float(rec.get("rho", 0)),
                    }
                    results.append(entry)

                    self.conn.execute(
                        """INSERT OR REPLACE INTO option_greeks
                           (root, expiration, strike, right, greeks_date,
                            iv, delta, gamma, vega, theta, rho)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (root, expiration, strike, right,
                         entry["greeks_date"], entry["iv"], entry["delta"],
                         entry["gamma"], entry["vega"], entry["theta"],
                         entry["rho"]),
                    )

        self.conn.commit()
        _log_fetch(self.conn, cache_key, len(results))
        return results

    # ------------------------------------------------------------------
    # Convenience: single-date bid/ask
    # ------------------------------------------------------------------

    def get_bid_ask(self, root: str, expiration: str, strike: float,
                    right: str, query_date: str) -> Optional[Dict]:
        """
        Get bid/ask for a single date. Returns dict with bid, ask or None.

        Checks cache first; if not present, fetches that single date.
        """
        # Check cache
        row = self.conn.execute(
            """SELECT bid, ask FROM option_quotes
               WHERE root=? AND expiration=? AND strike=? AND right=? AND quote_date=?""",
            (root, expiration, strike, right, query_date),
        ).fetchone()
        if row:
            return {"bid": row["bid"], "ask": row["ask"], "quote_date": query_date}

        # Also check option_eod table
        row = self.conn.execute(
            """SELECT bid, ask FROM option_eod
               WHERE root=? AND expiration=? AND strike=? AND right=? AND bar_date=?""",
            (root, expiration, strike, right, query_date),
        ).fetchone()
        if row and row["bid"] is not None:
            return {"bid": row["bid"], "ask": row["ask"], "quote_date": query_date}

        # Fetch just this date via EOD
        eod = self.get_option_eod(root, expiration, strike, right,
                                  query_date, query_date)
        if eod:
            return {
                "bid": eod[0].get("bid", 0.0),
                "ask": eod[0].get("ask", 0.0),
                "quote_date": query_date,
            }
        return None

    # ------------------------------------------------------------------
    # Convenience: find nearest expiration to target DTE
    # ------------------------------------------------------------------

    def find_nearest_expiration(self, root: str, entry_date: str,
                                target_dte: int = 30,
                                dte_min: int = 25,
                                dte_max: int = 45) -> Optional[str]:
        """
        Find the expiration closest to target_dte from entry_date.

        Only returns expirations within [dte_min, dte_max] range.
        Returns expiration as YYYY-MM-DD or None.
        """
        expirations = self.get_expirations(root, entry_date)
        if not expirations:
            return None

        entry_dt = datetime.strptime(entry_date, "%Y-%m-%d").date()
        best_exp = None
        best_diff = 9999

        for exp_str in expirations:
            exp_dt = datetime.strptime(exp_str, "%Y-%m-%d").date()
            dte = (exp_dt - entry_dt).days
            if dte < dte_min or dte > dte_max:
                continue
            diff = abs(dte - target_dte)
            if diff < best_diff:
                best_diff = diff
                best_exp = exp_str

        return best_exp

    # ------------------------------------------------------------------
    # Convenience: snap strike to nearest real strike
    # ------------------------------------------------------------------

    def snap_strike(self, root: str, expiration: str,
                    target: float) -> Optional[float]:
        """
        Snap a target strike to the nearest real strike available
        for the given root and expiration.
        """
        strikes = self.get_strikes(root, expiration)
        if not strikes:
            return None
        return min(strikes, key=lambda s: abs(s - target))

    # ------------------------------------------------------------------
    # Pre-fetch: get quotes for an option's full life in one API call
    # ------------------------------------------------------------------

    def prefetch_option_life(self, root: str, expiration: str, strike: float,
                             right: str, entry_date: str) -> List[Dict]:
        """
        Pre-fetch EOD data (including bid/ask) from entry_date through expiration.

        This is one API call that returns ~20-30 rows, all cached for
        the daily trade management loop.
        """
        return self.get_option_eod(root, expiration, strike, right,
                                   entry_date, expiration)

    # ------------------------------------------------------------------
    # Yahoo Finance: VIX history
    # ------------------------------------------------------------------

    def fetch_vix_history(self, start: str = "2011-01-01",
                          end: str = "2026-01-31") -> Dict[str, float]:
        """
        Download VIX daily closes from Yahoo Finance.
        Cached in the SQLite database.

        Returns dict mapping YYYY-MM-DD -> VIX close.
        """
        cache_key = f"vix:{start}:{end}"

        # Check if already fetched
        if _fetch_logged(self.conn, cache_key):
            rows = self.conn.execute(
                "SELECT bar_date, close FROM vix_daily WHERE bar_date >= ? AND bar_date <= ? ORDER BY bar_date",
                (start, end),
            ).fetchall()
            if rows:
                return {r["bar_date"]: r["close"] for r in rows}

        import yfinance as yf

        log.info("Fetching VIX history from Yahoo Finance (%s to %s) ...", start, end)
        vix = yf.download("^VIX", start=start, end=end, progress=False)
        if vix.empty:
            log.error("No VIX data returned from Yahoo Finance")
            return {}

        if isinstance(vix.columns, pd.MultiIndex):
            vix.columns = vix.columns.get_level_values(0)

        result: Dict[str, float] = {}
        for idx, row in vix.iterrows():
            d = idx.strftime("%Y-%m-%d")
            c = float(row["Close"])
            result[d] = c
            self.conn.execute(
                "INSERT OR REPLACE INTO vix_daily (bar_date, close) VALUES (?, ?)",
                (d, c),
            )

        self.conn.commit()
        _log_fetch(self.conn, cache_key, len(result))
        log.info("  VIX: %d days, range %.1f - %.1f",
                 len(result), min(result.values()), max(result.values()))
        return result

    def fetch_volatility_index(self, symbol: str = "^VIX",
                               start: str = "2011-01-01",
                               end: str = "2026-01-31") -> Dict[str, float]:
        """
        Download volatility index daily closes from Yahoo Finance.

        Common indices:
          - ^VIX: S&P 500 volatility (for SPY)
          - ^VXN: Nasdaq-100 volatility (for QQQ)
          - ^RVX: Russell 2000 volatility (for IWM)

        Returns dict mapping YYYY-MM-DD -> index close.
        """
        # Normalize symbol
        if not symbol.startswith("^"):
            symbol = f"^{symbol}"

        cache_key = f"vol_index:{symbol}:{start}:{end}"

        # Check if already fetched (use vix_daily table with symbol prefix)
        # For backwards compatibility, VIX uses vix_daily directly
        if symbol == "^VIX":
            return self.fetch_vix_history(start, end)

        if _fetch_logged(self.conn, cache_key):
            # Check underlying_bars table where we store non-VIX indices
            rows = self.conn.execute(
                """SELECT bar_date, close FROM underlying_bars
                   WHERE ticker = ? AND bar_date >= ? AND bar_date <= ?
                   ORDER BY bar_date""",
                (symbol, start, end),
            ).fetchall()
            if rows:
                return {r["bar_date"]: r["close"] for r in rows}

        import yfinance as yf

        log.info("Fetching %s history from Yahoo Finance (%s to %s) ...",
                 symbol, start, end)
        data = yf.download(symbol, start=start, end=end, progress=False)
        if data.empty:
            log.error("No %s data returned from Yahoo Finance", symbol)
            return {}

        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)

        result: Dict[str, float] = {}
        for idx, row in data.iterrows():
            d = idx.strftime("%Y-%m-%d")
            c = float(row["Close"])
            result[d] = c
            # Store in underlying_bars table
            self.conn.execute(
                """INSERT OR REPLACE INTO underlying_bars
                   (ticker, bar_date, open, high, low, close, volume)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (symbol, d, float(row["Open"]), float(row["High"]),
                 float(row["Low"]), c, float(row.get("Volume", 0))),
            )

        self.conn.commit()
        _log_fetch(self.conn, cache_key, len(result))
        if result:
            log.info("  %s: %d days, range %.1f - %.1f",
                     symbol, len(result), min(result.values()), max(result.values()))
        return result

    # ------------------------------------------------------------------
    # Yahoo Finance: SPY bars
    # ------------------------------------------------------------------

    def fetch_spy_bars(self, start: str = "2011-01-01",
                       end: str = "2026-01-31") -> List[Dict]:
        """
        Download SPY daily OHLCV bars from Yahoo Finance.
        Cached in the SQLite database.

        Returns list of dicts sorted by date.
        """
        cache_key = f"spy_bars_v2:{start}:{end}"  # v2 = unadjusted close

        # Check if already fetched
        if _fetch_logged(self.conn, cache_key):
            rows = self.conn.execute(
                """SELECT bar_date, open, high, low, close, volume
                   FROM underlying_bars
                   WHERE ticker='SPY' AND bar_date >= ? AND bar_date <= ?
                   ORDER BY bar_date""",
                (start, end),
            ).fetchall()
            if rows:
                return [dict(r) for r in rows]

        import yfinance as yf

        log.info("Fetching SPY bars from Yahoo Finance (%s to %s) ...", start, end)
        spy = yf.download("SPY", start=start, end=end, progress=False, auto_adjust=False)
        if spy.empty:
            log.error("No SPY data returned from Yahoo Finance")
            return []

        if isinstance(spy.columns, pd.MultiIndex):
            spy.columns = spy.columns.get_level_values(0)

        results = []
        for idx, row in spy.iterrows():
            d = idx.strftime("%Y-%m-%d")
            entry = {
                "bar_date": d,
                "open": float(row["Open"]),
                "high": float(row["High"]),
                "low": float(row["Low"]),
                "close": float(row["Close"]),
                "volume": float(row["Volume"]),
            }
            results.append(entry)
            self.conn.execute(
                """INSERT OR REPLACE INTO underlying_bars
                   (ticker, bar_date, open, high, low, close, volume)
                   VALUES ('SPY', ?, ?, ?, ?, ?, ?)""",
                (d, entry["open"], entry["high"], entry["low"],
                 entry["close"], entry["volume"]),
            )

        self.conn.commit()
        _log_fetch(self.conn, cache_key, len(results))
        log.info("  SPY: %d bars loaded", len(results))
        return results

    def fetch_spy_dividends(self, start: str = "2011-01-01",
                            end: str = "2026-01-31") -> Dict[str, float]:
        """
        Fetch SPY ex-dividend dates and per-share amounts from Yahoo Finance.
        Cached in the spy_dividends SQLite table.

        Returns dict mapping ex-date string -> dividend amount per share.
        """
        cache_key = f"spy_dividends:{start}:{end}"

        if _fetch_logged(self.conn, cache_key):
            rows = self.conn.execute(
                """SELECT ex_date, amount FROM spy_dividends
                   WHERE ex_date >= ? AND ex_date <= ?
                   ORDER BY ex_date""",
                (start, end),
            ).fetchall()
            if rows:
                return {r["ex_date"]: r["amount"] for r in rows}

        import yfinance as yf

        log.info("Fetching SPY dividends from Yahoo Finance (%s to %s) ...", start, end)
        ticker = yf.Ticker("SPY")
        divs = ticker.dividends  # pd.Series indexed by datetime

        result = {}
        for dt, amount in divs.items():
            d = dt.strftime("%Y-%m-%d")
            if d < start or d > end:
                continue
            result[d] = float(amount)
            self.conn.execute(
                "INSERT OR REPLACE INTO spy_dividends (ex_date, amount) VALUES (?, ?)",
                (d, float(amount)),
            )

        self.conn.commit()
        _log_fetch(self.conn, cache_key, len(result))
        log.info("  SPY dividends: %d ex-dates loaded", len(result))
        return result

    def fetch_ticker_bars(self, ticker: str, start: str = "2011-01-01",
                          end: str = "2026-01-31") -> List[Dict]:
        """
        Download daily OHLCV bars for any ticker from Yahoo Finance.
        Cached in the SQLite underlying_bars table.

        Returns list of dicts sorted by date.
        """
        ticker_upper = ticker.upper()
        cache_key = f"ticker_bars:{ticker_upper}:{start}:{end}"

        # Check if already fetched
        if _fetch_logged(self.conn, cache_key):
            rows = self.conn.execute(
                """SELECT bar_date, open, high, low, close, volume
                   FROM underlying_bars
                   WHERE ticker=? AND bar_date >= ? AND bar_date <= ?
                   ORDER BY bar_date""",
                (ticker_upper, start, end),
            ).fetchall()
            if rows:
                return [dict(r) for r in rows]

        import yfinance as yf

        log.info("Fetching %s bars from Yahoo Finance (%s to %s) ...",
                 ticker_upper, start, end)
        data = yf.download(ticker_upper, start=start, end=end, progress=False)
        if data.empty:
            log.error("No %s data returned from Yahoo Finance", ticker_upper)
            return []

        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)

        results = []
        for idx, row in data.iterrows():
            d = idx.strftime("%Y-%m-%d")
            entry = {
                "bar_date": d,
                "open": float(row["Open"]),
                "high": float(row["High"]),
                "low": float(row["Low"]),
                "close": float(row["Close"]),
                "volume": float(row["Volume"]),
            }
            results.append(entry)
            self.conn.execute(
                """INSERT OR REPLACE INTO underlying_bars
                   (ticker, bar_date, open, high, low, close, volume)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (ticker_upper, d, entry["open"], entry["high"], entry["low"],
                 entry["close"], entry["volume"]),
            )

        self.conn.commit()
        _log_fetch(self.conn, cache_key, len(results))
        log.info("  %s: %d bars loaded", ticker_upper, len(results))
        return results

    # ------------------------------------------------------------------
    # Convenience: load cached SPY bars for a date range
    # ------------------------------------------------------------------

    def get_spy_bars(self, start: str, end: str) -> List[Dict]:
        """Load SPY bars from cache for the given date range."""
        rows = self.conn.execute(
            """SELECT bar_date, open, high, low, close, volume
               FROM underlying_bars
               WHERE ticker='SPY' AND bar_date >= ? AND bar_date <= ?
               ORDER BY bar_date""",
            (start, end),
        ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Convenience: load cached VIX for a date range
    # ------------------------------------------------------------------

    def get_vix_history(self, start: str, end: str) -> Dict[str, float]:
        """Load VIX closes from cache for the given date range."""
        rows = self.conn.execute(
            "SELECT bar_date, close FROM vix_daily WHERE bar_date >= ? AND bar_date <= ? ORDER BY bar_date",
            (start, end),
        ).fetchall()
        return {r["bar_date"]: r["close"] for r in rows}

    # ------------------------------------------------------------------
    # Close connection
    # ------------------------------------------------------------------

    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None


# ---------------------------------------------------------------------------
# Quick self-test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)s  %(message)s",
        datefmt="%H:%M:%S",
    )

    print("=" * 60)
    print("ThetaData Client Self-Test (v3 API)")
    print("=" * 60)

    client = ThetaDataClient()

    # Test 1: Connection
    print("\n--- Test 1: Connection ---")
    connected = client.connect()
    print(f"  Connected: {connected}")

    # Test 2: Yahoo Finance (works without Theta Terminal)
    print("\n--- Test 2: VIX History (Yahoo Finance) ---")
    vix = client.fetch_vix_history("2024-01-01", "2024-03-31")
    print(f"  VIX days: {len(vix)}")
    if vix:
        dates = sorted(vix.keys())
        print(f"  First: {dates[0]} = {vix[dates[0]]:.1f}")
        print(f"  Last:  {dates[-1]} = {vix[dates[-1]]:.1f}")

    print("\n--- Test 3: SPY Bars (Yahoo Finance) ---")
    bars = client.fetch_spy_bars("2024-01-01", "2024-03-31")
    print(f"  SPY bars: {len(bars)}")
    if bars:
        print(f"  First: {bars[0]['bar_date']} close={bars[0]['close']:.2f}")
        print(f"  Last:  {bars[-1]['bar_date']} close={bars[-1]['close']:.2f}")

    if connected:
        print("\n--- Test 4: Expirations ---")
        exps = client.get_expirations("SPY")
        print(f"  Expirations: {len(exps)}")
        if exps:
            print(f"  First 5: {exps[:5]}")
            print(f"  Last 5:  {exps[-5:]}")

        if exps:
            # Pick ~30 DTE
            exp = client.find_nearest_expiration("SPY", date.today().isoformat())
            if exp:
                print(f"\n--- Test 5: Strikes for {exp} ---")
                strikes = client.get_strikes("SPY", exp)
                print(f"  Strikes: {len(strikes)}")

                if strikes:
                    target = 580.0
                    snapped = client.snap_strike("SPY", exp, target)
                    print(f"  Snap {target} -> {snapped}")

                    print(f"\n--- Test 6: EOD data for SPY {exp} ${snapped} P ---")
                    today = date.today().isoformat()
                    start = (date.today() - timedelta(days=10)).isoformat()
                    eod_data = client.get_option_eod("SPY", exp, snapped, "P", start, today)
                    print(f"  EOD rows: {len(eod_data)}")
                    for row in eod_data[:5]:
                        print(f"    {row['bar_date']}: close={row['close']:.2f} "
                              f"bid={row.get('bid', 0):.2f} ask={row.get('ask', 0):.2f}")

                    # Test bid/ask convenience
                    if eod_data:
                        ba = client.get_bid_ask("SPY", exp, snapped, "P", eod_data[0]["bar_date"])
                        print(f"\n  get_bid_ask({eod_data[0]['bar_date']}): {ba}")

    print("\n--- Test 7: Re-run cache test ---")
    vix2 = client.fetch_vix_history("2024-01-01", "2024-03-31")
    print(f"  VIX days (from cache): {len(vix2)}")

    print(f"\n  Database: {client.db_path}")
    print(f"  Size: {os.path.getsize(client.db_path) / 1024:.1f} KB")

    client.close()
    print("\nDone.")
