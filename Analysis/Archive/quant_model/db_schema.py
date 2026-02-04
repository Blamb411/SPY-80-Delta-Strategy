"""
Quant Scoring Model — Database Schema
=======================================
SQLite table creation and migration for quant_scoring.db.
"""

import sqlite3
from config import DB_PATH


def get_connection(db_path: str = DB_PATH) -> sqlite3.Connection:
    """Get a connection with WAL mode and row factory."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: str = DB_PATH) -> None:
    """Create all tables if they don't exist."""
    conn = get_connection(db_path)
    cur = conn.cursor()

    cur.executescript("""
    -- Filtered universe per scoring date
    CREATE TABLE IF NOT EXISTS stock_universe (
        symbol        TEXT NOT NULL,
        as_of_date    TEXT NOT NULL,   -- YYYY-MM-DD
        company_name  TEXT,
        exchange      TEXT,
        sector        TEXT,
        industry      TEXT,
        market_cap    REAL,
        share_price   REAL,
        is_reit       INTEGER DEFAULT 0,
        passes_filter INTEGER DEFAULT 1,
        PRIMARY KEY (symbol, as_of_date)
    );

    -- Point-in-time fundamental metrics (EAV design)
    CREATE TABLE IF NOT EXISTS fundamental_data (
        symbol        TEXT NOT NULL,
        as_of_date    TEXT NOT NULL,
        metric_name   TEXT NOT NULL,
        fiscal_period TEXT NOT NULL DEFAULT 'TTM',
        metric_value  REAL,
        source        TEXT DEFAULT 'gurufocus',
        fetched_at    TEXT NOT NULL,
        PRIMARY KEY (symbol, as_of_date, metric_name, fiscal_period)
    );

    -- LSEG I/B/E/S consensus data (Phase 2)
    CREATE TABLE IF NOT EXISTS eps_estimates (
        symbol          TEXT NOT NULL,
        as_of_date      TEXT NOT NULL,
        period          TEXT NOT NULL,   -- 'FY1', 'FY2', 'NTM'
        consensus_mean  REAL,
        num_estimates   INTEGER,
        num_up_7d       INTEGER DEFAULT 0,
        num_down_7d     INTEGER DEFAULT 0,
        num_up_30d      INTEGER DEFAULT 0,
        num_down_30d    INTEGER DEFAULT 0,
        num_up_90d      INTEGER DEFAULT 0,
        num_down_90d    INTEGER DEFAULT 0,
        mean_7d_ago     REAL,
        mean_30d_ago    REAL,
        mean_90d_ago    REAL,
        surprise_last_q REAL,
        fetched_at      TEXT NOT NULL,
        PRIMARY KEY (symbol, as_of_date, period)
    );

    -- Individual factor scores per stock per date
    CREATE TABLE IF NOT EXISTS factor_scores (
        symbol              TEXT NOT NULL,
        score_date          TEXT NOT NULL,
        factor_name         TEXT NOT NULL,  -- 'value', 'growth', 'profitability', 'momentum', 'eps_revisions'
        raw_score           REAL,
        sector_percentile   REAL,
        universe_percentile REAL,
        grade               TEXT,
        sub_scores          TEXT,   -- JSON blob of sub-factor details
        PRIMARY KEY (symbol, score_date, factor_name)
    );

    -- Final composite rating per stock per date
    CREATE TABLE IF NOT EXISTS composite_scores (
        symbol              TEXT NOT NULL,
        score_date          TEXT NOT NULL,
        composite_score     REAL,       -- 1.0 to 5.0
        rating              TEXT,       -- 'Strong Buy', 'Buy', 'Hold', 'Sell', 'Strong Sell'
        value_grade         TEXT,
        growth_grade        TEXT,
        profitability_grade TEXT,
        momentum_grade      TEXT,
        eps_revisions_grade TEXT,
        circuit_breaker_hit TEXT,       -- which factor triggered cap, or NULL
        disqualified        INTEGER DEFAULT 0,
        PRIMARY KEY (symbol, score_date)
    );

    -- Streak tracking for 75-day persistence rule
    CREATE TABLE IF NOT EXISTS rating_persistence (
        symbol        TEXT NOT NULL,
        first_date    TEXT NOT NULL,
        rating        TEXT NOT NULL,
        last_date     TEXT NOT NULL,
        streak_days   INTEGER DEFAULT 0,
        streak_weeks  INTEGER DEFAULT 0,
        is_active     INTEGER DEFAULT 1,
        PRIMARY KEY (symbol, first_date)
    );

    -- API call dedup / cache tracking
    CREATE TABLE IF NOT EXISTS fetch_log (
        fetch_key    TEXT PRIMARY KEY,
        last_fetched TEXT NOT NULL,
        details      TEXT            -- optional JSON metadata
    );

    -- Resolved RIC codes (ticker -> LSEG RIC mapping)
    CREATE TABLE IF NOT EXISTS ric_cache (
        symbol      TEXT PRIMARY KEY,
        ric         TEXT NOT NULL,
        resolved_at TEXT NOT NULL
    );

    -- Indexes for common queries
    CREATE INDEX IF NOT EXISTS idx_fundamental_symbol_date
        ON fundamental_data (symbol, as_of_date);
    CREATE INDEX IF NOT EXISTS idx_factor_scores_date
        ON factor_scores (score_date);
    CREATE INDEX IF NOT EXISTS idx_composite_scores_date
        ON composite_scores (score_date);
    CREATE INDEX IF NOT EXISTS idx_composite_scores_rating
        ON composite_scores (score_date, rating);
    CREATE INDEX IF NOT EXISTS idx_persistence_active
        ON rating_persistence (is_active, rating);
    CREATE INDEX IF NOT EXISTS idx_universe_date
        ON stock_universe (as_of_date, passes_filter);
    """)

    conn.commit()
    conn.close()


def reset_db(db_path: str = DB_PATH) -> None:
    """Drop all tables and recreate. USE WITH CAUTION."""
    conn = get_connection(db_path)
    tables = [
        "stock_universe", "fundamental_data", "eps_estimates",
        "factor_scores", "composite_scores", "rating_persistence",
        "fetch_log", "ric_cache",
    ]
    for table in tables:
        conn.execute(f"DROP TABLE IF EXISTS {table}")
    conn.commit()
    conn.close()
    init_db(db_path)


if __name__ == "__main__":
    print(f"Initializing database at {DB_PATH}")
    init_db()
    print("Done. All tables created.")
