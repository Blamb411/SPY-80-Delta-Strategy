"""
Percentile Ranker
==================
Ranks stocks within their GICS sector (and across the full universe)
to produce sector-relative percentile scores for each sub-factor.
"""

import os
import sys
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config


def compute_percentile(value: float, values: List[float], higher_is_better: bool = True) -> float:
    """
    Compute the percentile rank of a value within a list of values.

    Returns a value from 0 to 100, where 100 is best.
    - If higher_is_better=True, higher values get higher percentiles.
    - If higher_is_better=False, lower values get higher percentiles.
    """
    if not values or len(values) < 2:
        return 50.0  # Default to median when insufficient data

    sorted_vals = sorted(values)
    n = len(sorted_vals)

    # Count values below and equal
    below = sum(1 for v in sorted_vals if v < value)
    equal = sum(1 for v in sorted_vals if v == value)

    # Percentile rank using midpoint method
    percentile = ((below + 0.5 * equal) / n) * 100

    if not higher_is_better:
        percentile = 100 - percentile

    return max(0.0, min(100.0, percentile))


def rank_within_sector(
    symbol_values: Dict[str, Optional[float]],
    sector_map: Dict[str, str],
    higher_is_better: bool = True,
) -> Dict[str, Dict[str, float]]:
    """
    Rank symbols within their sector and across the full universe.

    Args:
        symbol_values: {symbol: metric_value} (None for missing data)
        sector_map: {symbol: sector_name}
        higher_is_better: whether higher metric values are better

    Returns:
        {symbol: {"sector_percentile": float, "universe_percentile": float}}
    """
    # Group by sector
    sector_groups = defaultdict(dict)
    all_values = {}

    for symbol, value in symbol_values.items():
        if value is None:
            continue
        all_values[symbol] = value
        sector = sector_map.get(symbol, "Unknown")
        sector_groups[sector][symbol] = value

    # Compute percentiles
    results = {}
    universe_vals = list(all_values.values())

    for symbol, value in symbol_values.items():
        if value is None:
            results[symbol] = {"sector_percentile": None, "universe_percentile": None}
            continue

        sector = sector_map.get(symbol, "Unknown")
        sector_vals = list(sector_groups[sector].values())

        sector_pct = compute_percentile(value, sector_vals, higher_is_better)
        universe_pct = compute_percentile(value, universe_vals, higher_is_better)

        results[symbol] = {
            "sector_percentile": round(sector_pct, 2),
            "universe_percentile": round(universe_pct, 2),
        }

    return results


def percentile_to_grade(percentile: Optional[float]) -> str:
    """Convert a percentile (0-100) to a letter grade using config thresholds."""
    if percentile is None:
        return "N/A"

    for threshold, grade in config.GRADE_THRESHOLDS:
        if percentile >= threshold:
            return grade
    return "F"


def grade_to_score(grade: str) -> float:
    """Convert a letter grade to a numeric score on the 1.0-5.0 scale."""
    grade_scores = {
        "A+": 5.0, "A": 4.75, "A-": 4.5,
        "B+": 4.0, "B": 3.5, "B-": 3.0,
        "C+": 2.75, "C": 2.5, "C-": 2.25,
        "D+": 2.0, "D": 1.75, "D-": 1.5,
        "F": 1.0,
        "N/A": 2.5,  # Default to Hold-equivalent
    }
    return grade_scores.get(grade, 2.5)


def compute_weighted_score(
    sub_factor_percentiles: List[Tuple[str, float, Optional[float]]],
) -> Optional[float]:
    """
    Compute a weighted average score from sub-factor percentiles.

    Args:
        sub_factor_percentiles: [(name, weight, sector_percentile), ...]

    Returns:
        Weighted average percentile (0-100), or None if insufficient data.
    """
    total_weight = 0.0
    weighted_sum = 0.0

    for name, weight, percentile in sub_factor_percentiles:
        if percentile is not None:
            weighted_sum += weight * percentile
            total_weight += weight

    if total_weight == 0:
        return None

    # Normalize to account for missing sub-factors
    return weighted_sum / total_weight
