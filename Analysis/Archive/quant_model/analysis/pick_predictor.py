"""
Pick Predictor
===============
Predicts the next SA Alpha Picks by finding stocks that match
the model's Strong Buy criteria with sufficient persistence.
"""

import os
import sys
from typing import Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
import db_schema
from analysis.rating_persistence import get_persistent_strong_buys


def predict_next_picks(as_of_date: str, n: int = 10,
                       min_streak_days: int = None,
                       db_path: str = config.DB_PATH) -> List[Dict]:
    """
    Predict likely next Alpha Picks based on:
    1. Current Strong Buy rating
    2. Persistence (75+ day streak)
    3. Highest composite score
    4. No recent circuit breaker hits

    Args:
        as_of_date: scoring date
        n: number of candidates to return
        min_streak_days: minimum Strong Buy streak (default: 75)

    Returns:
        Ranked list of candidate picks with scores and reasoning.
    """
    if min_streak_days is None:
        min_streak_days = config.PERSISTENCE_DAYS

    # Get persistent Strong Buys
    persistent = get_persistent_strong_buys(as_of_date, min_streak_days, db_path)
    persistent_symbols = {p["symbol"] for p in persistent}
    streak_map = {p["symbol"]: p for p in persistent}

    # Get composite scores
    conn = db_schema.get_connection(db_path)
    scores = conn.execute(
        """SELECT cs.*, su.sector, su.industry, su.market_cap
           FROM composite_scores cs
           LEFT JOIN stock_universe su ON cs.symbol = su.symbol AND su.as_of_date = cs.score_date
           WHERE cs.score_date = ? AND cs.rating = 'Strong Buy' AND cs.disqualified = 0
           ORDER BY cs.composite_score DESC""",
        (as_of_date,),
    ).fetchall()
    conn.close()

    candidates = []
    for row in scores:
        symbol = row["symbol"]
        streak = streak_map.get(symbol, {})

        candidate = {
            "symbol": symbol,
            "composite_score": row["composite_score"],
            "rating": row["rating"],
            "value_grade": row["value_grade"],
            "growth_grade": row["growth_grade"],
            "profitability_grade": row["profitability_grade"],
            "momentum_grade": row["momentum_grade"],
            "eps_revisions_grade": row["eps_revisions_grade"],
            "sector": row["sector"] if row["sector"] else "Unknown",
            "industry": row["industry"],
            "market_cap": row["market_cap"],
            "streak_days": streak.get("streak_days", 0),
            "persistent": symbol in persistent_symbols,
            "confidence": _compute_confidence(row, streak),
        }
        candidates.append(candidate)

    # Sort by confidence then composite score
    candidates.sort(key=lambda x: (x["confidence"], x["composite_score"]), reverse=True)
    return candidates[:n]


def _compute_confidence(score_row, streak: Dict) -> float:
    """
    Compute a confidence score (0-100) for a pick prediction.
    Higher = more likely to be an actual SA pick.
    """
    confidence = 0.0

    # Base: composite score (max 30 pts)
    composite = score_row["composite_score"]
    confidence += min(30, (composite - 4.0) * 30)  # 4.0-5.0 → 0-30

    # Persistence (max 25 pts)
    streak_days = streak.get("streak_days", 0)
    if streak_days >= 75:
        confidence += 25
    elif streak_days >= 50:
        confidence += 15
    elif streak_days >= 25:
        confidence += 5

    # All factors B+ or better (max 20 pts)
    high_grades = {"A+", "A", "A-", "B+"}
    factor_grades = [
        score_row["value_grade"],
        score_row["growth_grade"],
        score_row["profitability_grade"],
        score_row["momentum_grade"],
    ]
    high_count = sum(1 for g in factor_grades if g in high_grades)
    confidence += high_count * 5  # 4 factors × 5 = 20

    # No weak factors (max 15 pts)
    weak_grades = {"D+", "D", "D-", "F"}
    has_weak = any(g in weak_grades for g in factor_grades)
    if not has_weak:
        confidence += 15

    # Market cap bonus — mid caps favored (max 10 pts)
    market_cap = score_row["market_cap"]
    if market_cap:
        cap_b = market_cap / 1e9
        if 2 <= cap_b <= 50:  # $2B-$50B mid-cap sweet spot
            confidence += 10
        elif 1 <= cap_b <= 100:
            confidence += 5

    return min(100, max(0, confidence))


def print_predictions(candidates: List[Dict], title: str = "PICK PREDICTIONS") -> None:
    """Print formatted prediction report."""
    print(f"\n{'=' * 80}")
    print(f" {title}")
    print(f"{'=' * 80}")
    print(f"{'Rank':<5} {'Symbol':<7} {'Score':<7} {'Conf':<6} "
          f"{'Val':>4} {'Gro':>4} {'Pro':>4} {'Mom':>4} "
          f"{'Streak':>7} {'Sector':<20}")
    print("-" * 80)

    for i, c in enumerate(candidates, 1):
        streak = f"{c['streak_days']}d" if c['persistent'] else f"{c['streak_days']}d*"
        sector = (c['sector'] or 'Unknown')[:20]
        print(f"{i:<5} {c['symbol']:<7} {c['composite_score']:<7.3f} {c['confidence']:<6.1f} "
              f"{c['value_grade']:>4} {c['growth_grade']:>4} {c['profitability_grade']:>4} "
              f"{c['momentum_grade']:>4} {streak:>7} {sector:<20}")

    print(f"\n* = streak below {config.PERSISTENCE_DAYS} days")
