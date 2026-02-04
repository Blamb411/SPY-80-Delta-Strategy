"""
Report Generator
=================
Produces formatted reports for scoring results, backtests, and predictions.
"""

import os
import sys
import json
from datetime import datetime
from typing import Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
import db_schema


def generate_scoring_report(score_date: str, db_path: str = config.DB_PATH) -> str:
    """Generate a text report for a scoring run."""
    conn = db_schema.get_connection(db_path)

    # Overall stats
    all_scores = conn.execute(
        """SELECT rating, COUNT(*) as cnt FROM composite_scores
           WHERE score_date = ? AND disqualified = 0
           GROUP BY rating ORDER BY rating""",
        (score_date,),
    ).fetchall()

    # Top 20
    top = conn.execute(
        """SELECT cs.symbol, cs.composite_score, cs.rating,
                  cs.value_grade, cs.growth_grade, cs.profitability_grade,
                  cs.momentum_grade, cs.eps_revisions_grade,
                  cs.circuit_breaker_hit,
                  su.sector, su.market_cap
           FROM composite_scores cs
           LEFT JOIN stock_universe su ON cs.symbol = su.symbol AND su.as_of_date = cs.score_date
           WHERE cs.score_date = ? AND cs.disqualified = 0
           ORDER BY cs.composite_score DESC LIMIT 20""",
        (score_date,),
    ).fetchall()

    # Bottom 10 (potential shorts)
    bottom = conn.execute(
        """SELECT cs.symbol, cs.composite_score, cs.rating,
                  cs.value_grade, cs.growth_grade, cs.profitability_grade,
                  cs.momentum_grade, su.sector
           FROM composite_scores cs
           LEFT JOIN stock_universe su ON cs.symbol = su.symbol AND su.as_of_date = cs.score_date
           WHERE cs.score_date = ? AND cs.disqualified = 0
           ORDER BY cs.composite_score ASC LIMIT 10""",
        (score_date,),
    ).fetchall()

    conn.close()

    lines = []
    lines.append("=" * 80)
    lines.append(f" QUANT SCORING REPORT — {score_date}")
    lines.append("=" * 80)

    # Rating distribution
    lines.append("\n--- Rating Distribution ---")
    total = sum(row["cnt"] for row in all_scores)
    for row in all_scores:
        pct = row["cnt"] / total * 100 if total > 0 else 0
        bar = "#" * int(pct / 2)
        lines.append(f"  {row['rating']:<12} {row['cnt']:>4}  ({pct:5.1f}%)  {bar}")
    lines.append(f"  {'Total':<12} {total:>4}")

    # Top 20
    lines.append(f"\n--- Top 20 Stocks ---")
    lines.append(f"{'Rank':<5} {'Symbol':<7} {'Score':<7} {'Rating':<12} "
                 f"{'Val':>4} {'Gro':>4} {'Pro':>4} {'Mom':>4} {'EPS':>4} "
                 f"{'Sector':<18} {'MktCap':>10}")
    lines.append("-" * 90)

    for i, row in enumerate(top, 1):
        mkt = _fmt_market_cap(row["market_cap"])
        sector = (row["sector"] or "?")[:18]
        eps = row["eps_revisions_grade"] or "N/A"
        cb = " *CB" if row["circuit_breaker_hit"] else ""
        lines.append(f"{i:<5} {row['symbol']:<7} {row['composite_score']:<7.3f} "
                     f"{row['rating']:<12} "
                     f"{row['value_grade']:>4} {row['growth_grade']:>4} "
                     f"{row['profitability_grade']:>4} {row['momentum_grade']:>4} "
                     f"{eps:>4} {sector:<18} {mkt:>10}{cb}")

    # Bottom 10
    lines.append(f"\n--- Bottom 10 (Short Candidates) ---")
    lines.append(f"{'Rank':<5} {'Symbol':<7} {'Score':<7} {'Rating':<12} "
                 f"{'Val':>4} {'Gro':>4} {'Pro':>4} {'Mom':>4} {'Sector':<18}")
    lines.append("-" * 75)

    for i, row in enumerate(bottom, 1):
        sector = (row["sector"] or "?")[:18]
        lines.append(f"{i:<5} {row['symbol']:<7} {row['composite_score']:<7.3f} "
                     f"{row['rating']:<12} "
                     f"{row['value_grade']:>4} {row['growth_grade']:>4} "
                     f"{row['profitability_grade']:>4} {row['momentum_grade']:>4} "
                     f"{sector:<18}")

    lines.append("\n" + "=" * 80)
    return "\n".join(lines)


def generate_backtest_report(validation_results: Dict) -> str:
    """Generate a text report from backtest validation results."""
    r = validation_results
    lines = []
    lines.append("=" * 80)
    lines.append(" BACKTEST VALIDATION REPORT")
    lines.append("=" * 80)

    lines.append(f"\nPick dates evaluated:   {r['pick_dates_evaluated']}")
    lines.append(f"Total SA picks:         {r['total_picks']}")

    lines.append(f"\n--- Accuracy ---")
    lines.append(f"Hit rate (Strong Buy):  {r['hit_rate']:.1%}  ({r['hits']}/{r['total_picks']})")
    lines.append(f"Near-hit rate (Buy+):   {r['near_hit_rate']:.1%}  "
                 f"({r['hits'] + r['near_hits']}/{r['total_picks']})")
    lines.append(f"Precision@2:            {r['precision_at_2']:.1%}")
    lines.append(f"Average pick rank:      {r['avg_rank']:.1f}")
    lines.append(f"Median pick rank:       {r['median_rank']}")

    lines.append(f"\n--- Date-by-Date ---")
    for detail in r.get("date_details", []):
        lines.append(f"\n  {detail['date']}:")
        lines.append(f"    SA picked: {', '.join(detail['sa_picks'])}")
        lines.append(f"    Our top 2: {', '.join(detail['our_top2'])}")
        for pr in detail["pick_results"]:
            status = "HIT" if pr["our_rating"] == "Strong Buy" else \
                     "near" if pr["our_rating"] == "Buy" else "MISS"
            lines.append(f"    → {pr['symbol']:6s} {status:5s} "
                         f"(our: {pr['our_rating']}, rank: {pr.get('rank', '?')}/"
                         f"{pr.get('total_ranked', '?')})")

    lines.append("\n" + "=" * 80)
    return "\n".join(lines)


def save_report(content: str, filename: str) -> str:
    """Save report to file in the quant_model directory."""
    path = os.path.join(config.BASE_DIR, filename)
    with open(path, "w") as f:
        f.write(content)
    return path


def _fmt_market_cap(cap: Optional[float]) -> str:
    """Format market cap as $XB or $XM."""
    if cap is None:
        return "?"
    if cap >= 1e12:
        return f"${cap / 1e12:.1f}T"
    if cap >= 1e9:
        return f"${cap / 1e9:.1f}B"
    if cap >= 1e6:
        return f"${cap / 1e6:.0f}M"
    return f"${cap:,.0f}"
