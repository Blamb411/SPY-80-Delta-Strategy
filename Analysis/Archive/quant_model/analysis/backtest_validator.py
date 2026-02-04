"""
Backtest Validator
===================
Compares our model's ratings against SA's actual 97 Alpha Picks.
Computes hit rate, precision@2, rank accuracy, and sector distribution.
"""

import os
import sys
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
import db_schema
from data.universe_builder import load_alpha_picks_from_excel, get_pick_dates


def validate_against_picks(db_path: str = config.DB_PATH) -> Dict:
    """
    Run full validation of model scores against known Alpha Picks.

    For each pick date:
    - Check if SA's actual picks received Strong Buy from our model
    - Measure where SA's picks rank in our sorted list
    - Track precision@2 (if our top 2 match SA's 2)

    Returns comprehensive validation report.
    """
    # Load actual Alpha Picks
    actual_picks = load_alpha_picks_from_excel()
    picks_by_date = defaultdict(list)
    for symbol, date in actual_picks:
        picks_by_date[date].append(symbol)

    conn = db_schema.get_connection(db_path)

    results = {
        "pick_dates_evaluated": 0,
        "total_picks": 0,
        "hits": 0,           # SA pick was Strong Buy in our model
        "near_hits": 0,      # SA pick was Buy in our model
        "misses": 0,         # SA pick was Hold or below
        "hit_rate": 0.0,
        "near_hit_rate": 0.0,
        "precision_at_2": 0.0,
        "avg_rank": 0.0,
        "median_rank": 0.0,
        "date_details": [],
        "sector_distribution": defaultdict(int),
        "missed_picks": [],
    }

    all_ranks = []
    p2_hits = 0
    p2_total = 0

    for pick_date in sorted(picks_by_date.keys()):
        sa_picks = picks_by_date[pick_date]

        # Get our model's scores for this date
        scores = conn.execute(
            """SELECT symbol, composite_score, rating
               FROM composite_scores
               WHERE score_date = ? AND disqualified = 0
               ORDER BY composite_score DESC""",
            (pick_date,),
        ).fetchall()

        if not scores:
            continue

        results["pick_dates_evaluated"] += 1
        ranked_symbols = [row["symbol"] for row in scores]
        score_map = {row["symbol"]: dict(row) for row in scores}

        # Our top 2 candidates
        our_top2 = ranked_symbols[:2]
        p2_total += 1
        p2_match = sum(1 for s in sa_picks if s in our_top2)
        p2_hits += p2_match

        date_detail = {
            "date": pick_date,
            "sa_picks": sa_picks,
            "our_top2": our_top2,
            "pick_results": [],
        }

        for symbol in sa_picks:
            results["total_picks"] += 1
            info = score_map.get(symbol)

            if info:
                rating = info["rating"]
                score = info["composite_score"]
                rank = ranked_symbols.index(symbol) + 1 if symbol in ranked_symbols else len(ranked_symbols)

                all_ranks.append(rank)

                if rating == "Strong Buy":
                    results["hits"] += 1
                elif rating == "Buy":
                    results["near_hits"] += 1
                else:
                    results["misses"] += 1
                    results["missed_picks"].append({
                        "symbol": symbol,
                        "date": pick_date,
                        "our_rating": rating,
                        "our_score": score,
                        "rank": rank,
                    })

                date_detail["pick_results"].append({
                    "symbol": symbol,
                    "our_rating": rating,
                    "our_score": score,
                    "rank": rank,
                    "total_ranked": len(ranked_symbols),
                })
            else:
                results["misses"] += 1
                results["missed_picks"].append({
                    "symbol": symbol,
                    "date": pick_date,
                    "our_rating": "N/A (not scored)",
                    "our_score": None,
                    "rank": None,
                })

                date_detail["pick_results"].append({
                    "symbol": symbol,
                    "our_rating": "Not scored",
                    "our_score": None,
                    "rank": None,
                    "total_ranked": len(ranked_symbols),
                })

        results["date_details"].append(date_detail)

    conn.close()

    # Compute summary metrics
    if results["total_picks"] > 0:
        results["hit_rate"] = results["hits"] / results["total_picks"]
        results["near_hit_rate"] = (results["hits"] + results["near_hits"]) / results["total_picks"]

    if p2_total > 0:
        results["precision_at_2"] = p2_hits / (p2_total * 2)  # normalize

    if all_ranks:
        results["avg_rank"] = sum(all_ranks) / len(all_ranks)
        sorted_ranks = sorted(all_ranks)
        mid = len(sorted_ranks) // 2
        results["median_rank"] = sorted_ranks[mid]

    return results


def print_validation_report(results: Dict) -> None:
    """Print a formatted validation report."""
    print("\n" + "=" * 70)
    print("BACKTEST VALIDATION REPORT")
    print("=" * 70)

    print(f"\nPick dates evaluated: {results['pick_dates_evaluated']}")
    print(f"Total SA picks evaluated: {results['total_picks']}")

    print(f"\n--- Hit Rates ---")
    print(f"Strong Buy hits:   {results['hits']}/{results['total_picks']} "
          f"({results['hit_rate']:.1%})")
    print(f"Buy or better:     {results['hits'] + results['near_hits']}/{results['total_picks']} "
          f"({results['near_hit_rate']:.1%})")
    print(f"Misses (Hold-):    {results['misses']}")

    print(f"\n--- Ranking ---")
    print(f"Precision@2:       {results['precision_at_2']:.1%}")
    print(f"Avg rank of picks: {results['avg_rank']:.1f}")
    print(f"Median rank:       {results['median_rank']}")

    if results["missed_picks"]:
        print(f"\n--- Top Misses (SA picked, we didn't rate Strong Buy) ---")
        for miss in results["missed_picks"][:10]:
            print(f"  {miss['date']} {miss['symbol']:6s} → "
                  f"{miss['our_rating']:12s} (score: {miss.get('our_score', 'N/A')}, "
                  f"rank: {miss.get('rank', 'N/A')})")

    print("\n" + "=" * 70)


def compute_sector_accuracy(db_path: str = config.DB_PATH) -> Dict[str, Dict]:
    """
    Analyze hit rate by sector to check if our model matches SA's
    known Tech/Industrials bias.
    """
    actual_picks = load_alpha_picks_from_excel()
    conn = db_schema.get_connection(db_path)

    sector_stats = defaultdict(lambda: {"total": 0, "hits": 0, "picks": []})

    for symbol, pick_date in actual_picks:
        # Get sector
        info = conn.execute(
            "SELECT sector FROM stock_universe WHERE symbol = ? AND as_of_date = ?",
            (symbol, pick_date),
        ).fetchone()
        sector = info["sector"] if info and info["sector"] else "Unknown"

        # Get our rating
        score = conn.execute(
            "SELECT rating FROM composite_scores WHERE symbol = ? AND score_date = ?",
            (symbol, pick_date),
        ).fetchone()

        sector_stats[sector]["total"] += 1
        sector_stats[sector]["picks"].append(symbol)
        if score and score["rating"] == "Strong Buy":
            sector_stats[sector]["hits"] += 1

    conn.close()

    # Compute hit rates
    for sector in sector_stats:
        total = sector_stats[sector]["total"]
        hits = sector_stats[sector]["hits"]
        sector_stats[sector]["hit_rate"] = hits / total if total > 0 else 0

    return dict(sector_stats)
