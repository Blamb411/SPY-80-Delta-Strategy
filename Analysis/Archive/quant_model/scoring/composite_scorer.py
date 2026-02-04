"""
Composite Scorer
=================
Combines individual factor scores into a final 1.0-5.0 composite rating.
Applies circuit breakers and classifies into Strong Buy / Buy / Hold / Sell / Strong Sell.
"""

import os
import sys
import json
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
import db_schema
from scoring.percentile_ranker import grade_to_score, percentile_to_grade
from scoring.factor_value import score_value, save_scores as save_value
from scoring.factor_growth import score_growth, save_scores as save_growth
from scoring.factor_profitability import score_profitability, save_scores as save_profit
from scoring.factor_momentum import score_momentum, save_scores as save_momentum
from scoring.factor_eps_revisions import score_eps_revisions, save_scores as save_eps, is_available as eps_available


def compute_all_factors(symbols: List[str], as_of_date: str,
                        sector_map: Dict[str, str],
                        db_path: str = config.DB_PATH) -> Dict[str, Dict[str, Dict]]:
    """
    Score all factors for all symbols.

    Returns:
        {symbol: {factor_name: factor_result_dict}}
    """
    print("Scoring Value factor...")
    value_scores = score_value(symbols, as_of_date, sector_map, db_path)
    save_value(value_scores, as_of_date, db_path)

    print("Scoring Growth factor...")
    growth_scores = score_growth(symbols, as_of_date, sector_map, db_path)
    save_growth(growth_scores, as_of_date, db_path)

    print("Scoring Profitability factor...")
    profit_scores = score_profitability(symbols, as_of_date, sector_map, db_path)
    save_profit(profit_scores, as_of_date, db_path)

    print("Scoring Momentum factor...")
    momentum_scores = score_momentum(symbols, as_of_date, sector_map, db_path)
    save_momentum(momentum_scores, as_of_date, db_path)

    # EPS Revisions — only if LSEG data is available
    eps_scores = {}
    if eps_available(db_path):
        print("Scoring EPS Revisions factor...")
        eps_scores = score_eps_revisions(symbols, as_of_date, sector_map, db_path)
        save_eps(eps_scores, as_of_date, db_path)
    else:
        print("EPS Revisions not available — using 4-factor model")

    # Combine into per-symbol dict
    results = {}
    for symbol in symbols:
        results[symbol] = {
            "value": value_scores.get(symbol, {}),
            "growth": growth_scores.get(symbol, {}),
            "profitability": profit_scores.get(symbol, {}),
            "momentum": momentum_scores.get(symbol, {}),
        }
        if eps_scores:
            results[symbol]["eps_revisions"] = eps_scores.get(symbol, {})

    return results


def check_circuit_breakers(factor_results: Dict[str, Dict]) -> Optional[str]:
    """
    Check if any circuit breaker is triggered.

    Returns the factor name that triggered the breaker, or None.
    """
    for factor_name, min_grade in config.CIRCUIT_BREAKERS.items():
        factor_data = factor_results.get(factor_name, {})
        grade = factor_data.get("grade", "N/A")

        if grade == "N/A":
            continue

        min_numeric = config.GRADE_NUMERIC.get(min_grade, 0)
        actual_numeric = config.GRADE_NUMERIC.get(grade, 5)

        if actual_numeric <= min_numeric:
            return factor_name

    return None


def compute_composite(factor_results: Dict[str, Dict],
                      use_5f: bool = False) -> Tuple[float, str, Optional[str]]:
    """
    Compute weighted composite score from factor results.

    Args:
        factor_results: {factor_name: {grade, raw_score, ...}}
        use_5f: whether to use 5-factor weights (with EPS Revisions)

    Returns:
        (composite_score, rating, circuit_breaker_factor)
    """
    weights = config.FACTOR_WEIGHTS_5F if use_5f else config.FACTOR_WEIGHTS_4F

    # Compute weighted average
    total_weight = 0.0
    weighted_sum = 0.0

    for factor_name, weight in weights.items():
        factor_data = factor_results.get(factor_name, {})
        grade = factor_data.get("grade", "N/A")
        score = grade_to_score(grade)
        weighted_sum += weight * score
        total_weight += weight

    if total_weight > 0:
        composite = weighted_sum / total_weight
    else:
        composite = 2.5  # Default to Hold

    # Clamp to valid range
    composite = max(config.COMPOSITE_MIN, min(config.COMPOSITE_MAX, composite))

    # Check circuit breakers
    breaker = check_circuit_breakers(factor_results)

    # Classify rating
    rating = classify_rating(composite)

    # Apply circuit breaker: cap at Hold
    if breaker and rating in ("Strong Buy", "Buy"):
        rating = "Hold"
        composite = min(composite, 3.49)  # Cap below Buy threshold

    return composite, rating, breaker


def classify_rating(score: float) -> str:
    """Convert composite score to rating string."""
    for threshold, rating in config.RATING_THRESHOLDS:
        if score >= threshold:
            return rating
    return "Strong Sell"


def score_universe(symbols: List[str], as_of_date: str,
                   sector_map: Dict[str, str],
                   db_path: str = config.DB_PATH) -> List[Dict]:
    """
    Full scoring pipeline: compute all factors, combine, classify.

    Returns list of scored stocks sorted by composite score (descending).
    """
    # Step 1: Compute all factor scores
    all_factors = compute_all_factors(symbols, as_of_date, sector_map, db_path)

    # Step 2: Determine if using 5-factor model
    use_5f = eps_available(db_path) and any(
        "eps_revisions" in f for f in all_factors.values()
    )

    # Step 3: Compute composite scores
    print("Computing composite scores...")
    conn = db_schema.get_connection(db_path)
    results = []

    for symbol in symbols:
        factor_results = all_factors.get(symbol, {})
        composite, rating, breaker = compute_composite(factor_results, use_5f)

        # Get grades for each factor
        value_grade = factor_results.get("value", {}).get("grade", "N/A")
        growth_grade = factor_results.get("growth", {}).get("grade", "N/A")
        profit_grade = factor_results.get("profitability", {}).get("grade", "N/A")
        momentum_grade = factor_results.get("momentum", {}).get("grade", "N/A")
        eps_grade = factor_results.get("eps_revisions", {}).get("grade", "N/A")

        # Check if disqualified (insufficient data)
        valid_factors = sum(1 for f in factor_results.values()
                          if f.get("grade", "N/A") != "N/A")
        disqualified = 1 if valid_factors < 2 else 0

        if disqualified:
            rating = "Hold"
            composite = 2.5

        result = {
            "symbol": symbol,
            "composite_score": round(composite, 3),
            "rating": rating,
            "value_grade": value_grade,
            "growth_grade": growth_grade,
            "profitability_grade": profit_grade,
            "momentum_grade": momentum_grade,
            "eps_revisions_grade": eps_grade,
            "circuit_breaker_hit": breaker,
            "disqualified": disqualified,
        }
        results.append(result)

        # Save to database
        conn.execute(
            """INSERT OR REPLACE INTO composite_scores
               (symbol, score_date, composite_score, rating,
                value_grade, growth_grade, profitability_grade,
                momentum_grade, eps_revisions_grade,
                circuit_breaker_hit, disqualified)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (symbol, as_of_date, result["composite_score"], rating,
             value_grade, growth_grade, profit_grade,
             momentum_grade, eps_grade,
             breaker, disqualified),
        )

    conn.commit()
    conn.close()

    # Sort by composite score descending
    results.sort(key=lambda x: x["composite_score"], reverse=True)

    # Summary
    rating_counts = {}
    for r in results:
        rating_counts[r["rating"]] = rating_counts.get(r["rating"], 0) + 1

    print(f"\nScoring complete: {len(results)} stocks")
    for rating in ["Strong Buy", "Buy", "Hold", "Sell", "Strong Sell"]:
        count = rating_counts.get(rating, 0)
        print(f"  {rating}: {count}")

    breaker_count = sum(1 for r in results if r["circuit_breaker_hit"])
    if breaker_count:
        print(f"  Circuit breakers triggered: {breaker_count}")

    return results


def get_strong_buys(as_of_date: str, db_path: str = config.DB_PATH) -> List[Dict]:
    """Get all Strong Buy rated stocks for a date."""
    conn = db_schema.get_connection(db_path)
    rows = conn.execute(
        """SELECT * FROM composite_scores
           WHERE score_date = ? AND rating = 'Strong Buy' AND disqualified = 0
           ORDER BY composite_score DESC""",
        (as_of_date,),
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_top_n(as_of_date: str, n: int = 10,
              db_path: str = config.DB_PATH) -> List[Dict]:
    """Get top N rated stocks for a date."""
    conn = db_schema.get_connection(db_path)
    rows = conn.execute(
        """SELECT * FROM composite_scores
           WHERE score_date = ? AND disqualified = 0
           ORDER BY composite_score DESC LIMIT ?""",
        (as_of_date, n),
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]
