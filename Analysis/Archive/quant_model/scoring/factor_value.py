"""
Value Factor Scoring
=====================
Scores stocks on valuation: Forward P/E, PEG, P/S, P/B, EV/EBITDA.
Lower is better for all sub-factors (sector-relative).
"""

import os
import sys
import json
from typing import Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
import db_schema
from scoring.percentile_ranker import rank_within_sector, compute_weighted_score, percentile_to_grade


FACTOR_NAME = "value"


def score_value(symbols: List[str], as_of_date: str,
                sector_map: Dict[str, str],
                db_path: str = config.DB_PATH) -> Dict[str, Dict]:
    """
    Compute Value factor scores for all symbols.

    Returns:
        {symbol: {
            "raw_score": float (0-100 percentile),
            "sector_percentile": float,
            "universe_percentile": float,
            "grade": str,
            "sub_scores": {metric: {value, sector_pct, weight}},
        }}
    """
    conn = db_schema.get_connection(db_path)

    # Load metric values for all symbols
    metric_data = {}  # metric_name -> {symbol: value}
    for metric_name, weight, higher_is_better in config.VALUE_SUB_FACTORS:
        rows = conn.execute(
            """SELECT symbol, metric_value FROM fundamental_data
               WHERE as_of_date = ? AND metric_name = ? AND symbol IN ({})""".format(
                ",".join("?" * len(symbols))
            ),
            (as_of_date, metric_name, *symbols),
        ).fetchall()
        metric_data[metric_name] = {row["symbol"]: row["metric_value"] for row in rows}

    conn.close()

    # Rank each sub-factor within sector
    sub_rankings = {}
    for metric_name, weight, higher_is_better in config.VALUE_SUB_FACTORS:
        values = {}
        for s in symbols:
            values[s] = metric_data.get(metric_name, {}).get(s)
        sub_rankings[metric_name] = rank_within_sector(values, sector_map, higher_is_better)

    # Combine sub-factor percentiles into factor score
    results = {}
    for symbol in symbols:
        sub_scores = {}
        weighted_parts = []

        for metric_name, weight, higher_is_better in config.VALUE_SUB_FACTORS:
            ranking = sub_rankings[metric_name].get(symbol, {})
            sect_pct = ranking.get("sector_percentile")
            raw_val = metric_data.get(metric_name, {}).get(symbol)

            sub_scores[metric_name] = {
                "value": raw_val,
                "sector_percentile": sect_pct,
                "weight": weight,
            }
            weighted_parts.append((metric_name, weight, sect_pct))

        # Weighted average of sector percentiles
        factor_percentile = compute_weighted_score(weighted_parts)
        grade = percentile_to_grade(factor_percentile)

        results[symbol] = {
            "raw_score": factor_percentile,
            "sector_percentile": factor_percentile,
            "universe_percentile": factor_percentile,  # Updated later if needed
            "grade": grade,
            "sub_scores": sub_scores,
        }

    return results


def save_scores(scores: Dict[str, Dict], score_date: str,
                db_path: str = config.DB_PATH) -> None:
    """Save factor scores to the database."""
    conn = db_schema.get_connection(db_path)
    for symbol, data in scores.items():
        conn.execute(
            """INSERT OR REPLACE INTO factor_scores
               (symbol, score_date, factor_name, raw_score,
                sector_percentile, universe_percentile, grade, sub_scores)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (symbol, score_date, FACTOR_NAME,
             data["raw_score"], data["sector_percentile"],
             data["universe_percentile"], data["grade"],
             json.dumps(data["sub_scores"])),
        )
    conn.commit()
    conn.close()
