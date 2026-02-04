"""
Rating Persistence Tracker
============================
Tracks consecutive Strong Buy streaks for the 75-day persistence rule.
SA Alpha Picks require sustained Strong Buy ratings — not one-time spikes.
"""

import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
import db_schema


def update_persistence(score_date: str, db_path: str = config.DB_PATH) -> Dict[str, int]:
    """
    Update rating persistence streaks based on the latest scoring run.

    For each symbol with a composite score on score_date:
    - If rating matches the active streak, extend it
    - If rating changed or no active streak, start a new one
    - Close any active streaks for symbols not scored

    Returns: {symbol: streak_days} for active Strong Buy streaks
    """
    conn = db_schema.get_connection(db_path)

    # Get all scores for this date
    scores = conn.execute(
        """SELECT symbol, rating FROM composite_scores
           WHERE score_date = ? AND disqualified = 0""",
        (score_date,),
    ).fetchall()

    score_map = {row["symbol"]: row["rating"] for row in scores}
    active_streaks = {}

    for symbol, rating in score_map.items():
        # Check for existing active streak
        active = conn.execute(
            """SELECT rowid, first_date, rating, last_date, streak_days
               FROM rating_persistence
               WHERE symbol = ? AND is_active = 1
               ORDER BY first_date DESC LIMIT 1""",
            (symbol,),
        ).fetchone()

        if active and active["rating"] == rating:
            # Extend existing streak
            first = datetime.strptime(active["first_date"], "%Y-%m-%d")
            current = datetime.strptime(score_date, "%Y-%m-%d")
            streak_days = (current - first).days
            streak_weeks = streak_days // 7

            conn.execute(
                """UPDATE rating_persistence
                   SET last_date = ?, streak_days = ?, streak_weeks = ?
                   WHERE symbol = ? AND first_date = ? AND is_active = 1""",
                (score_date, streak_days, streak_weeks, symbol, active["first_date"]),
            )

            if rating == "Strong Buy":
                active_streaks[symbol] = streak_days
        else:
            # Close any existing active streak
            if active:
                conn.execute(
                    """UPDATE rating_persistence
                       SET is_active = 0
                       WHERE symbol = ? AND first_date = ? AND is_active = 1""",
                    (symbol, active["first_date"]),
                )

            # Start new streak
            conn.execute(
                """INSERT OR REPLACE INTO rating_persistence
                   (symbol, first_date, rating, last_date, streak_days, streak_weeks, is_active)
                   VALUES (?, ?, ?, ?, 0, 0, 1)""",
                (symbol, score_date, rating, score_date),
            )

            if rating == "Strong Buy":
                active_streaks[symbol] = 0

    # Close streaks for symbols not in this scoring run
    scored_symbols = set(score_map.keys())
    all_active = conn.execute(
        "SELECT DISTINCT symbol FROM rating_persistence WHERE is_active = 1"
    ).fetchall()

    for row in all_active:
        if row["symbol"] not in scored_symbols:
            conn.execute(
                "UPDATE rating_persistence SET is_active = 0 WHERE symbol = ? AND is_active = 1",
                (row["symbol"],),
            )

    conn.commit()
    conn.close()
    return active_streaks


def get_persistent_strong_buys(as_of_date: str, min_days: int = None,
                               db_path: str = config.DB_PATH) -> List[Dict]:
    """
    Get symbols with active Strong Buy streaks meeting the persistence threshold.

    Args:
        as_of_date: current scoring date
        min_days: minimum streak days (defaults to config.PERSISTENCE_DAYS = 75)

    Returns:
        List of {symbol, first_date, streak_days, streak_weeks}
    """
    if min_days is None:
        min_days = config.PERSISTENCE_DAYS

    conn = db_schema.get_connection(db_path)
    rows = conn.execute(
        """SELECT symbol, first_date, last_date, streak_days, streak_weeks
           FROM rating_persistence
           WHERE is_active = 1 AND rating = 'Strong Buy' AND streak_days >= ?
           ORDER BY streak_days DESC""",
        (min_days,),
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_streak_summary(db_path: str = config.DB_PATH) -> Dict[str, List[Dict]]:
    """Get summary of all active streaks grouped by rating."""
    conn = db_schema.get_connection(db_path)
    rows = conn.execute(
        """SELECT symbol, rating, first_date, last_date, streak_days, streak_weeks
           FROM rating_persistence
           WHERE is_active = 1
           ORDER BY rating, streak_days DESC"""
    ).fetchall()
    conn.close()

    grouped = {}
    for row in rows:
        rating = row["rating"]
        if rating not in grouped:
            grouped[rating] = []
        grouped[rating].append(dict(row))
    return grouped


def get_symbol_history(symbol: str, db_path: str = config.DB_PATH) -> List[Dict]:
    """Get full streak history for a symbol."""
    conn = db_schema.get_connection(db_path)
    rows = conn.execute(
        """SELECT rating, first_date, last_date, streak_days, streak_weeks, is_active
           FROM rating_persistence
           WHERE symbol = ?
           ORDER BY first_date""",
        (symbol,),
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]
