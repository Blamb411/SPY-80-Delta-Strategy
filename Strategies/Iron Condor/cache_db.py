"""
SQLite Cache Layer
===================
Stores all downloaded Massive API data locally so re-runs
require zero API calls.

Tables:
    underlying_bars  — daily OHLCV for the 12 ETFs
    options_contracts — available contracts per date
    option_bars      — daily OHLCV for individual option contracts
    option_quotes    — historical bid/ask (2022-03-07+)
    fetch_log        — tracks completed downloads
"""

import sqlite3
import json
from typing import Optional, List, Dict, Any
from datetime import date

from config import DB_PATH


def get_connection(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: str = DB_PATH) -> None:
    """Create all tables if they don't exist."""
    conn = get_connection(db_path)
    cur = conn.cursor()

    cur.executescript("""
    CREATE TABLE IF NOT EXISTS underlying_bars (
        ticker       TEXT    NOT NULL,
        bar_date     TEXT    NOT NULL,   -- YYYY-MM-DD
        open         REAL,
        high         REAL,
        low          REAL,
        close        REAL,
        volume       REAL,
        vwap         REAL,
        PRIMARY KEY (ticker, bar_date)
    );

    CREATE TABLE IF NOT EXISTS options_contracts (
        option_ticker    TEXT    NOT NULL,
        underlying       TEXT    NOT NULL,
        as_of_date       TEXT    NOT NULL,   -- date we queried
        expiration_date  TEXT    NOT NULL,
        strike_price     REAL    NOT NULL,
        contract_type    TEXT    NOT NULL,   -- 'call' or 'put'
        PRIMARY KEY (option_ticker, as_of_date)
    );

    CREATE TABLE IF NOT EXISTS option_bars (
        option_ticker TEXT NOT NULL,
        bar_date      TEXT NOT NULL,
        open          REAL,
        high          REAL,
        low           REAL,
        close         REAL,
        volume        REAL,
        vwap          REAL,
        PRIMARY KEY (option_ticker, bar_date)
    );

    CREATE TABLE IF NOT EXISTS option_quotes (
        option_ticker TEXT NOT NULL,
        quote_date    TEXT NOT NULL,
        bid           REAL,
        ask           REAL,
        bid_size      REAL,
        ask_size      REAL,
        PRIMARY KEY (option_ticker, quote_date)
    );

    CREATE TABLE IF NOT EXISTS fetch_log (
        fetch_key    TEXT    PRIMARY KEY,  -- e.g. "underlying_bars:SPY"
        last_fetched TEXT    NOT NULL,     -- ISO timestamp
        details      TEXT                  -- optional JSON metadata
    );
    """)

    conn.commit()
    conn.close()


# -----------------------------------------------------------------------
# underlying_bars helpers
# -----------------------------------------------------------------------

def save_underlying_bars(ticker: str, bars: List[Dict], db_path: str = DB_PATH) -> int:
    conn = get_connection(db_path)
    inserted = 0
    for b in bars:
        try:
            conn.execute(
                """INSERT OR IGNORE INTO underlying_bars
                   (ticker, bar_date, open, high, low, close, volume, vwap)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (ticker, b["date"], b["open"], b["high"], b["low"],
                 b["close"], b["volume"], b.get("vwap")),
            )
            inserted += 1
        except sqlite3.IntegrityError:
            pass
    conn.commit()
    conn.close()
    return inserted


def get_underlying_bars(
    ticker: str,
    start: str,
    end: str,
    db_path: str = DB_PATH,
) -> List[Dict]:
    conn = get_connection(db_path)
    rows = conn.execute(
        """SELECT bar_date, open, high, low, close, volume, vwap
           FROM underlying_bars
           WHERE ticker = ? AND bar_date >= ? AND bar_date <= ?
           ORDER BY bar_date""",
        (ticker, start, end),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_underlying_close(ticker: str, bar_date: str, db_path: str = DB_PATH) -> Optional[float]:
    conn = get_connection(db_path)
    row = conn.execute(
        "SELECT close FROM underlying_bars WHERE ticker=? AND bar_date=?",
        (ticker, bar_date),
    ).fetchone()
    conn.close()
    return row["close"] if row else None


def get_all_closes(ticker: str, up_to_date: str, db_path: str = DB_PATH) -> List[float]:
    """Return ordered list of closes up to (and including) a date."""
    conn = get_connection(db_path)
    rows = conn.execute(
        """SELECT close FROM underlying_bars
           WHERE ticker=? AND bar_date <= ?
           ORDER BY bar_date""",
        (ticker, up_to_date),
    ).fetchall()
    conn.close()
    return [r["close"] for r in rows]


# -----------------------------------------------------------------------
# options_contracts helpers
# -----------------------------------------------------------------------

def save_options_contracts(contracts: List[Dict], as_of_date: str, db_path: str = DB_PATH) -> int:
    conn = get_connection(db_path)
    inserted = 0
    for c in contracts:
        try:
            conn.execute(
                """INSERT OR IGNORE INTO options_contracts
                   (option_ticker, underlying, as_of_date, expiration_date,
                    strike_price, contract_type)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (c["option_ticker"], c["underlying"], as_of_date,
                 c["expiration_date"], c["strike_price"], c["contract_type"]),
            )
            inserted += 1
        except sqlite3.IntegrityError:
            pass
    conn.commit()
    conn.close()
    return inserted


def get_options_contracts(
    underlying: str,
    as_of_date: str,
    contract_type: str,
    min_dte: int = 0,
    max_dte: int = 9999,
    db_path: str = DB_PATH,
) -> List[Dict]:
    """Get cached contracts for a given underlying, date, and type.

    Searches for contracts cached on the exact as_of_date.
    """
    conn = get_connection(db_path)
    rows = conn.execute(
        """SELECT option_ticker, underlying, expiration_date, strike_price, contract_type
           FROM options_contracts
           WHERE underlying = ? AND as_of_date = ? AND contract_type = ?
           ORDER BY strike_price""",
        (underlying, as_of_date, contract_type),
    ).fetchall()
    conn.close()

    results = []
    for r in rows:
        from datetime import datetime
        exp = datetime.strptime(r["expiration_date"], "%Y-%m-%d").date()
        as_of = datetime.strptime(as_of_date, "%Y-%m-%d").date()
        dte = (exp - as_of).days
        if min_dte <= dte <= max_dte:
            d = dict(r)
            d["dte"] = dte
            results.append(d)
    return results


# -----------------------------------------------------------------------
# option_bars helpers
# -----------------------------------------------------------------------

def save_option_bars(option_ticker: str, bars: List[Dict], db_path: str = DB_PATH) -> int:
    conn = get_connection(db_path)
    inserted = 0
    for b in bars:
        try:
            conn.execute(
                """INSERT OR IGNORE INTO option_bars
                   (option_ticker, bar_date, open, high, low, close, volume, vwap)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (option_ticker, b["date"], b["open"], b["high"], b["low"],
                 b["close"], b["volume"], b.get("vwap")),
            )
            inserted += 1
        except sqlite3.IntegrityError:
            pass
    conn.commit()
    conn.close()
    return inserted


def get_option_bar(option_ticker: str, bar_date: str, db_path: str = DB_PATH) -> Optional[Dict]:
    conn = get_connection(db_path)
    row = conn.execute(
        """SELECT bar_date, open, high, low, close, volume, vwap
           FROM option_bars WHERE option_ticker=? AND bar_date=?""",
        (option_ticker, bar_date),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def get_option_bars_range(
    option_ticker: str, start: str, end: str, db_path: str = DB_PATH
) -> List[Dict]:
    conn = get_connection(db_path)
    rows = conn.execute(
        """SELECT bar_date, open, high, low, close, volume, vwap
           FROM option_bars
           WHERE option_ticker=? AND bar_date>=? AND bar_date<=?
           ORDER BY bar_date""",
        (option_ticker, start, end),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# -----------------------------------------------------------------------
# option_quotes helpers
# -----------------------------------------------------------------------

def save_option_quotes(option_ticker: str, quotes: List[Dict], db_path: str = DB_PATH) -> int:
    conn = get_connection(db_path)
    inserted = 0
    for q in quotes:
        try:
            conn.execute(
                """INSERT OR IGNORE INTO option_quotes
                   (option_ticker, quote_date, bid, ask, bid_size, ask_size)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (option_ticker, q["date"], q["bid"], q["ask"],
                 q.get("bid_size"), q.get("ask_size")),
            )
            inserted += 1
        except sqlite3.IntegrityError:
            pass
    conn.commit()
    conn.close()
    return inserted


def get_option_quote(option_ticker: str, quote_date: str, db_path: str = DB_PATH) -> Optional[Dict]:
    conn = get_connection(db_path)
    row = conn.execute(
        """SELECT quote_date, bid, ask, bid_size, ask_size
           FROM option_quotes WHERE option_ticker=? AND quote_date=?""",
        (option_ticker, quote_date),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


# -----------------------------------------------------------------------
# fetch_log helpers
# -----------------------------------------------------------------------

def mark_fetched(fetch_key: str, details: Optional[Dict] = None, db_path: str = DB_PATH) -> None:
    from datetime import datetime
    conn = get_connection(db_path)
    conn.execute(
        """INSERT OR REPLACE INTO fetch_log (fetch_key, last_fetched, details)
           VALUES (?, ?, ?)""",
        (fetch_key, datetime.utcnow().isoformat(), json.dumps(details) if details else None),
    )
    conn.commit()
    conn.close()


def is_fetched(fetch_key: str, db_path: str = DB_PATH) -> bool:
    conn = get_connection(db_path)
    row = conn.execute(
        "SELECT 1 FROM fetch_log WHERE fetch_key=?", (fetch_key,)
    ).fetchone()
    conn.close()
    return row is not None


def get_trading_dates(ticker: str, start: str, end: str, db_path: str = DB_PATH) -> List[str]:
    """Return list of trading dates (dates with bars) for a ticker."""
    conn = get_connection(db_path)
    rows = conn.execute(
        """SELECT bar_date FROM underlying_bars
           WHERE ticker=? AND bar_date>=? AND bar_date<=?
           ORDER BY bar_date""",
        (ticker, start, end),
    ).fetchall()
    conn.close()
    return [r["bar_date"] for r in rows]
