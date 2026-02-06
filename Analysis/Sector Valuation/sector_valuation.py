"""
Sector Valuation Model
=======================
Scores GICS sectors as Cheap / Fair / Expensive using Damodaran data.

Valuation Framework:
  1. Forward PEG Score — growth-adjusted P/E; lower is cheaper (primary)
  2. Forward P/E Score — lower is cheaper
  3. CAPE Proxy — current Forward P/E vs 10-year historical average (z-score)
  4. Margin Mean-Reversion — current net margin vs 10-year history (z-score)
  5. P/B vs ROE Score — is book value generating adequate returns?
  6. EV/EBITDA Score — enterprise value relative to cash earnings
  7. Earnings Yield Gap — earnings yield minus risk-free rate
  8. Composite Score — weighted average of all dimensions

Scoring Method:
  Each metric is ranked across sectors (1 = cheapest, 11 = most expensive).
  Percentile ranks are converted to a 1-5 scale:
    1 = Very Cheap, 2 = Cheap, 3 = Fair, 4 = Expensive, 5 = Very Expensive

  For z-score based metrics (CAPE proxy, margin reversion), scores are assigned
  based on standard deviation bands rather than percentile ranks.

Usage:
    python sector_valuation.py
"""

import os
import sys
import numpy as np
import pandas as pd
from datetime import datetime

_this_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _this_dir)

from damodaran_fetcher import fetch_all_damodaran_data

try:
    from lseg_sector_enrichment import fetch_lseg_sector_data, print_lseg_comparison
    LSEG_AVAILABLE = True
except ImportError:
    LSEG_AVAILABLE = False

try:
    from damodaran_historical import (
        get_sector_historical_pe,
        get_sector_historical_margins,
        build_historical_database,
        DB_PATH as HISTORICAL_DB_PATH
    )
    HISTORICAL_AVAILABLE = True
except ImportError:
    HISTORICAL_AVAILABLE = False
    HISTORICAL_DB_PATH = None

# ---------------------------------------------------------------------------
# Scoring weights — Updated with CAPE proxy and margin reversion
# ---------------------------------------------------------------------------
METRIC_WEIGHTS = {
    "peg_ratio": 0.25,           # Primary growth-adjusted metric
    "forward_pe": 0.10,          # Reduced (captured by CAPE proxy)
    "cape_proxy": 0.20,          # NEW: Forward P/E vs 10-year history
    "margin_reversion": 0.15,    # NEW: Net margin vs 10-year history
    "pb_vs_roe": 0.10,           # Reduced
    "ev_ebitda": 0.10,           # Reduced
    "earnings_yield_gap": 0.10,  # Reduced
}

# Sector ETF tickers for reference
SECTOR_ETFS = {
    "Information Technology": "XLK",
    "Health Care": "XLV",
    "Financials": "XLF",
    "Consumer Discretionary": "XLY",
    "Communication Services": "XLC",
    "Industrials": "XLI",
    "Consumer Staples": "XLP",
    "Energy": "XLE",
    "Utilities": "XLU",
    "Real Estate": "XLRE",
    "Materials": "XLB",
}

# Historical average P/E ranges (approximate, from long-term Damodaran data)
# Used as reference points, not scoring inputs
HISTORICAL_PE_RANGES = {
    "Information Technology": (18, 25),
    "Health Care": (16, 22),
    "Financials": (10, 14),
    "Consumer Discretionary": (16, 22),
    "Communication Services": (14, 20),
    "Industrials": (16, 20),
    "Consumer Staples": (18, 22),
    "Energy": (10, 16),
    "Utilities": (14, 18),
    "Real Estate": (20, 35),
    "Materials": (14, 18),
}


def score_metric(values, lower_is_better=True):
    """
    Rank values across sectors and convert to 1-5 score.

    Args:
        values: Series of metric values
        lower_is_better: If True, lowest value gets score 1 (cheapest)

    Returns:
        Series of scores (1=Very Cheap, 5=Very Expensive)
    """
    valid = values.dropna()
    if len(valid) < 3:
        return pd.Series(np.nan, index=values.index)

    # Rank (1 = lowest value)
    ranks = values.rank(method="average", ascending=True)

    # Convert to percentile (0 to 1)
    n = ranks.count()
    percentiles = (ranks - 1) / (n - 1)

    if not lower_is_better:
        percentiles = 1 - percentiles

    # Map to 1-5 scale
    scores = 1 + percentiles * 4

    return scores


def compute_pb_vs_roe_score(sector_df):
    """
    Score P/B relative to ROE.
    A sector with high P/B is justified if ROE is also high.
    Expensive = high P/B with low ROE.
    """
    pb = sector_df["pbv"]
    roe = sector_df["roe"]

    # P/B per unit of ROE — lower means you're paying less for each % of ROE
    # This is similar in spirit to PEG but for book value
    pb_per_roe = pb / roe.replace(0, np.nan)

    return score_metric(pb_per_roe, lower_is_better=True)


def compute_earnings_yield_gap(sector_df):
    """
    Earnings yield (1/PE) minus risk-free rate.
    Higher gap = cheaper (more earnings yield above risk-free).
    Using 10Y Treasury ~4.5% as of early 2026.
    """
    risk_free = 4.5  # percent

    fwd_pe = sector_df["forward_pe"]
    earnings_yield = (1 / fwd_pe.replace(0, np.nan)) * 100

    gap = earnings_yield - risk_free

    return score_metric(gap, lower_is_better=False)  # higher gap = cheaper


def _zscore_to_score(z: float) -> float:
    """
    Convert z-score to valuation score (1-5 scale).

    Z-Score Range        | Interpretation                    | Score
    ---------------------|-----------------------------------|-------
    z < -3σ              | Extremely cheap vs history        | 1.0
    -3σ to -2σ           | Significantly cheap               | 1.5
    -2σ to -1σ           | Cheap                             | 2.0
    -1σ to +1σ           | Normal range                      | 3.0
    +1σ to +2σ           | Elevated                          | 4.0
    +2σ to +3σ           | Significantly elevated            | 4.5
    z > +3σ              | Extremely expensive vs history    | 5.0
    """
    if pd.isna(z):
        return 3.0  # Default to neutral

    if z < -3:
        return 1.0
    elif z < -2:
        return 1.5
    elif z < -1:
        return 2.0
    elif z <= 1:
        return 3.0
    elif z <= 2:
        return 4.0
    elif z <= 3:
        return 4.5
    else:
        return 5.0


def _zscore_to_band(z: float) -> str:
    """Convert z-score to descriptive band label."""
    if pd.isna(z):
        return "N/A"

    if z < -3:
        return "Extreme low (<-3 std)"
    elif z < -2:
        return "Signif. low (-3 to -2)"
    elif z < -1:
        return "Below normal (-2 to -1)"
    elif z <= 1:
        return "Normal (+/-1 std)"
    elif z <= 2:
        return "Elevated (1 to 2)"
    elif z <= 3:
        return "Signif. high (2 to 3)"
    else:
        return "Extreme high (>3 std)"


def compute_cape_proxy_score(sector_df):
    """
    CAPE Proxy: Compare current Forward P/E to 10-year historical average.

    z = (current_pe - mean_pe) / std_pe

    High z-score → expensive vs history → higher score (bad)
    Low z-score → cheap vs history → lower score (good)

    Returns:
        Tuple of (scores Series, details DataFrame with z-scores and bands)
    """
    if not HISTORICAL_AVAILABLE:
        # Return neutral scores if historical data not available
        return pd.Series(3.0, index=sector_df.index), None

    scores = []
    details = []

    for idx, row in sector_df.iterrows():
        sector = row["sector"]
        current_pe = row.get("forward_pe", np.nan)

        hist = get_sector_historical_pe(sector)

        if hist["count"] < 3 or pd.isna(current_pe) or hist["std"] == 0:
            z_score = np.nan
            score = 3.0
        else:
            z_score = (current_pe - hist["mean"]) / hist["std"]
            score = _zscore_to_score(z_score)

        scores.append(score)
        details.append({
            "sector": sector,
            "current_fwd_pe": current_pe,
            "hist_mean": hist["mean"],
            "hist_std": hist["std"],
            "z_score": z_score,
            "band": _zscore_to_band(z_score),
            "score": score,
            "years_of_data": hist["count"]
        })

    return pd.Series(scores, index=sector_df.index), pd.DataFrame(details)


def compute_margin_reversion_score(sector_df):
    """
    Margin Mean-Reversion: Compare current net margin to 10-year history.

    z = (current_margin - mean_margin) / std_margin

    High margins → likely to contract → adjust valuation upward (more expensive)
    Low margins → likely to expand → adjust valuation downward (cheaper)

    Based on Grantham's insight that profit margins are mean-reverting.

    Returns:
        Tuple of (scores Series, details DataFrame with z-scores and bands)
    """
    if not HISTORICAL_AVAILABLE:
        return pd.Series(3.0, index=sector_df.index), None

    scores = []
    details = []

    for idx, row in sector_df.iterrows():
        sector = row["sector"]
        current_margin = row.get("net_margin", np.nan)

        hist = get_sector_historical_margins(sector)

        if hist["count"] < 3 or pd.isna(current_margin) or hist["std"] == 0:
            z_score = np.nan
            score = 3.0
        else:
            z_score = (current_margin - hist["mean"]) / hist["std"]
            score = _zscore_to_score(z_score)

        scores.append(score)

        # Determine risk interpretation for margins
        if pd.isna(z_score):
            risk = "N/A"
        elif z_score > 1:
            risk = "Contraction likely"
        elif z_score < -1:
            risk = "Expansion likely"
        else:
            risk = "Normal"

        details.append({
            "sector": sector,
            "current_margin": current_margin,
            "hist_mean": hist["mean"],
            "hist_std": hist["std"],
            "z_score": z_score,
            "band": _zscore_to_band(z_score),
            "risk": risk,
            "score": score,
            "years_of_data": hist["count"]
        })

    return pd.Series(scores, index=sector_df.index), pd.DataFrame(details)


def score_sectors(sector_df):
    """
    Compute all valuation scores for each sector.
    Returns DataFrame with scores and composite rating, plus detail DataFrames
    for CAPE proxy and margin reversion.
    """
    scores = pd.DataFrame()
    scores["sector"] = sector_df["sector"]

    # Individual metric scores (1=Very Cheap, 5=Very Expensive)
    scores["fwd_pe_score"] = score_metric(sector_df["forward_pe"], lower_is_better=True)
    scores["peg_score"] = score_metric(sector_df["peg_ratio"], lower_is_better=True)
    scores["pb_roe_score"] = compute_pb_vs_roe_score(sector_df)

    if "ev_ebitda" in sector_df.columns:
        scores["ev_ebitda_score"] = score_metric(sector_df["ev_ebitda"], lower_is_better=True)
    elif "ev_invested_capital" in sector_df.columns:
        scores["ev_ebitda_score"] = score_metric(sector_df["ev_invested_capital"], lower_is_better=True)
    else:
        scores["ev_ebitda_score"] = np.nan

    scores["ey_gap_score"] = compute_earnings_yield_gap(sector_df)

    # NEW: CAPE Proxy and Margin Reversion (z-score based)
    cape_scores, cape_details = compute_cape_proxy_score(sector_df)
    margin_scores, margin_details = compute_margin_reversion_score(sector_df)

    scores["cape_proxy_score"] = cape_scores
    scores["margin_rev_score"] = margin_scores

    # Composite score (weighted average with new metrics)
    scores["composite"] = (
        scores["peg_score"] * METRIC_WEIGHTS["peg_ratio"]
        + scores["fwd_pe_score"] * METRIC_WEIGHTS["forward_pe"]
        + scores["cape_proxy_score"].fillna(3.0) * METRIC_WEIGHTS["cape_proxy"]
        + scores["margin_rev_score"].fillna(3.0) * METRIC_WEIGHTS["margin_reversion"]
        + scores["pb_roe_score"] * METRIC_WEIGHTS["pb_vs_roe"]
        + scores["ev_ebitda_score"].fillna(3.0) * METRIC_WEIGHTS["ev_ebitda"]
        + scores["ey_gap_score"] * METRIC_WEIGHTS["earnings_yield_gap"]
    )

    # Rating
    scores["rating"] = scores["composite"].apply(_composite_to_rating)

    return scores, cape_details, margin_details


def _composite_to_rating(score):
    """Convert composite score to text rating."""
    if pd.isna(score):
        return "N/A"
    if score <= 1.8:
        return "VERY CHEAP"
    elif score <= 2.5:
        return "CHEAP"
    elif score <= 3.5:
        return "FAIR"
    elif score <= 4.2:
        return "EXPENSIVE"
    else:
        return "VERY EXPENSIVE"


def _rating_indicator(rating):
    """Return a visual indicator for the rating."""
    indicators = {
        "VERY CHEAP": "<<<",
        "CHEAP": "<<",
        "FAIR": "--",
        "EXPENSIVE": ">>",
        "VERY EXPENSIVE": ">>>",
    }
    return indicators.get(rating, "  ")


def print_valuation_report(sector_df, scores, cape_details=None, margin_details=None, industry_df=None):
    """Print the full sector valuation report."""
    W = 90

    print()
    print("=" * W)
    print("SECTOR VALUATION REPORT")
    print(f"Data: Damodaran (NYU Stern) — {datetime.now().strftime('%B %Y')}")
    print("=" * W)

    # Weights explanation
    print(f"\nScoring Weights:")
    print(f"  Forward PEG (growth-adjusted):    {METRIC_WEIGHTS['peg_ratio']:.0%}")
    print(f"  Forward P/E:                      {METRIC_WEIGHTS['forward_pe']:.0%}")
    print(f"  CAPE Proxy (P/E vs 10yr history): {METRIC_WEIGHTS['cape_proxy']:.0%}")
    print(f"  Margin Mean-Reversion:            {METRIC_WEIGHTS['margin_reversion']:.0%}")
    print(f"  P/B vs ROE:                       {METRIC_WEIGHTS['pb_vs_roe']:.0%}")
    print(f"  EV/EBITDA:                        {METRIC_WEIGHTS['ev_ebitda']:.0%}")
    print(f"  Earnings Yield Gap:               {METRIC_WEIGHTS['earnings_yield_gap']:.0%}")

    # Main valuation table
    print(f"\n{'=' * W}")
    print("SECTOR VALUATIONS (sorted cheapest to most expensive)")
    print(f"{'=' * W}")

    merged = sector_df.merge(scores, on="sector")
    merged = merged.sort_values("composite")

    print(f"\n  {'Sector':<25} {'Fwd PE':>7} {'PEG':>7} {'Growth':>7} "
          f"{'P/B':>7} {'ROE':>7} {'Score':>7} {'Rating':<16}")
    print(f"  {'-' * 85}")

    for _, row in merged.iterrows():
        fwd_pe = f"{row['forward_pe']:.1f}" if pd.notna(row.get('forward_pe')) else "N/A"
        peg = f"{row['peg_ratio']:.2f}" if pd.notna(row.get('peg_ratio')) else "N/A"
        growth = f"{row['expected_growth_5yr']:.1f}%" if pd.notna(row.get('expected_growth_5yr')) else "N/A"
        pbv = f"{row['pbv']:.1f}" if pd.notna(row.get('pbv')) else "N/A"
        roe_val = f"{row['roe']:.1f}%" if pd.notna(row.get('roe')) else "N/A"
        score = f"{row['composite']:.2f}" if pd.notna(row.get('composite')) else "N/A"
        rating = row.get('rating', 'N/A')
        indicator = _rating_indicator(rating)

        print(f"  {row['sector']:<25} {fwd_pe:>7} {peg:>7} {growth:>7} "
              f"{pbv:>7} {roe_val:>7} {score:>7} {indicator} {rating}")

    # Detailed score breakdown
    print(f"\n{'=' * W}")
    print("SCORE BREAKDOWN (1=Very Cheap, 5=Very Expensive)")
    print(f"{'=' * W}")

    print(f"\n  {'Sector':<22} {'PEG':>5} {'FwdPE':>6} {'CAPE':>5} {'Mrgn':>5} "
          f"{'P/B':>5} {'EV':>5} {'EYG':>5} {'Total':>6}")
    print(f"  {'-' * 72}")

    for _, row in merged.iterrows():
        cape = row.get('cape_proxy_score', 3.0)
        mrgn = row.get('margin_rev_score', 3.0)
        print(f"  {row['sector']:<22} "
              f"{row['peg_score']:>5.1f} "
              f"{row['fwd_pe_score']:>6.1f} "
              f"{cape:>5.1f} "
              f"{mrgn:>5.1f} "
              f"{row['pb_roe_score']:>5.1f} "
              f"{row['ev_ebitda_score']:>5.1f} "
              f"{row['ey_gap_score']:>5.1f} "
              f"{row['composite']:>6.2f}")

    print(f"\n  Legend: PEG=Forward PEG, FwdPE=Forward P/E, CAPE=CAPE Proxy, Mrgn=Margin Reversion")
    print(f"          P/B=P/B vs ROE, EV=EV/EBITDA, EYG=Earnings Yield Gap")

    # PEG Analysis (user's preferred metric)
    print(f"\n{'=' * W}")
    print("PEG RATIO ANALYSIS (Growth-Adjusted Valuation)")
    print(f"{'=' * W}")
    print(f"\n  Damodaran PEG = Current (Trailing) P/E / Expected 5-Year Growth")
    print(f"  Forward PEG   = Forward P/E / Expected 5-Year Growth")
    print(f"  PEG < 1.0 = Cheap relative to growth")
    print(f"  PEG 1.0-2.0 = Fair")
    print(f"  PEG > 2.0 = Expensive relative to growth")

    # Calculate forward PEG
    merged["forward_peg"] = merged["forward_pe"] / merged["expected_growth_5yr"].replace(0, np.nan)

    peg_sorted = merged.sort_values("forward_peg")
    print(f"\n  {'Sector':<25} {'Fwd PEG':>8} {'Dam PEG':>8} {'Fwd PE':>7} {'Growth':>7} {'Assessment':<20}")
    print(f"  {'-' * 80}")

    for _, row in peg_sorted.iterrows():
        fwd_peg = row.get("forward_peg", np.nan)
        dam_peg = row.get("peg_ratio", np.nan)

        if pd.isna(fwd_peg):
            assessment = "Insufficient data"
        elif fwd_peg < 1.0:
            assessment = "Cheap vs growth"
        elif fwd_peg < 1.5:
            assessment = "Reasonable"
        elif fwd_peg < 2.0:
            assessment = "Fairly valued"
        elif fwd_peg < 3.0:
            assessment = "Getting expensive"
        else:
            assessment = "Expensive vs growth"

        fwd_peg_str = f"{fwd_peg:.2f}" if pd.notna(fwd_peg) else "N/A"
        dam_peg_str = f"{dam_peg:.2f}" if pd.notna(dam_peg) else "N/A"
        fwd_pe = f"{row['forward_pe']:.1f}" if pd.notna(row.get('forward_pe')) else "N/A"
        growth = f"{row['expected_growth_5yr']:.1f}%" if pd.notna(row.get('expected_growth_5yr')) else "N/A"

        print(f"  {row['sector']:<25} {fwd_peg_str:>8} {dam_peg_str:>8} {fwd_pe:>7} {growth:>7} {assessment:<20}")

    # CAPE Proxy Section (NEW)
    if cape_details is not None and not cape_details.empty:
        print(f"\n{'=' * W}")
        print("FORWARD P/E vs 10-YEAR HISTORY (CAPE Proxy)")
        print(f"{'=' * W}")
        print(f"\n  Compares current Forward P/E to 10-year historical average per sector.")
        print(f"  Z-score = (Current - Mean) / Std Dev")
        print(f"  High z-score = expensive vs history; Low z-score = cheap vs history")

        print(f"\n  {'Sector':<22} {'Fwd PE':>7} {'10yr Avg':>9} {'Std Dev':>8} {'Z-Score':>8} {'Band':<25}")
        print(f"  {'-' * 82}")

        cape_sorted = cape_details.sort_values("z_score", na_position='last')
        for _, row in cape_sorted.iterrows():
            current = f"{row['current_fwd_pe']:.1f}" if pd.notna(row['current_fwd_pe']) else "N/A"
            mean = f"{row['hist_mean']:.1f}" if pd.notna(row['hist_mean']) else "N/A"
            std = f"{row['hist_std']:.1f}" if pd.notna(row['hist_std']) else "N/A"
            z = f"{row['z_score']:+.2f}" if pd.notna(row['z_score']) else "N/A"
            band = row['band'] if row['band'] else "N/A"

            print(f"  {row['sector']:<22} {current:>7} {mean:>9} {std:>8} {z:>8} {band:<25}")

        print(f"\n  Legend: +/-1 std = Normal | 1-2 = Elevated/Depressed | 2-3 = Significant | >3 = Extreme")

    # Margin Mean-Reversion Section (NEW)
    if margin_details is not None and not margin_details.empty:
        print(f"\n{'=' * W}")
        print("PROFIT MARGIN vs 10-YEAR HISTORY (Mean Reversion Risk)")
        print(f"{'=' * W}")
        print(f"\n  Compares current net margin to 10-year historical average per sector.")
        print(f"  Based on Grantham's insight that profit margins are mean-reverting.")
        print(f"  High margins = likely to contract; Low margins = likely to expand")

        print(f"\n  {'Sector':<22} {'Net Mrgn':>9} {'10yr Avg':>9} {'Std Dev':>8} {'Z-Score':>8} {'Risk':<20}")
        print(f"  {'-' * 80}")

        margin_sorted = margin_details.sort_values("z_score", ascending=False, na_position='last')
        for _, row in margin_sorted.iterrows():
            current = f"{row['current_margin']:.1f}%" if pd.notna(row['current_margin']) else "N/A"
            mean = f"{row['hist_mean']:.1f}%" if pd.notna(row['hist_mean']) else "N/A"
            std = f"{row['hist_std']:.1f}%" if pd.notna(row['hist_std']) else "N/A"
            z = f"{row['z_score']:+.2f}" if pd.notna(row['z_score']) else "N/A"
            risk = row['risk'] if row['risk'] else "N/A"

            print(f"  {row['sector']:<22} {current:>9} {mean:>9} {std:>8} {z:>8} {risk:<20}")

        # Count sectors with elevated/depressed margins
        elevated = margin_details[margin_details['z_score'] > 1].shape[0]
        depressed = margin_details[margin_details['z_score'] < -1].shape[0]

        if elevated > 0:
            print(f"\n  NOTE: {elevated} sector(s) with margins >1 std above average -- earnings may be cyclically inflated.")
        if depressed > 0:
            print(f"  NOTE: {depressed} sector(s) with margins <-1 std below average -- earnings may be cyclically depressed.")

    # Earnings Yield vs Risk-Free Rate
    print(f"\n{'=' * W}")
    print("EARNINGS YIELD vs RISK-FREE RATE")
    print(f"{'=' * W}")
    print(f"\n  Earnings Yield = 1 / Forward P/E")
    print(f"  Risk-Free Rate = ~4.5% (10-Year Treasury)")
    print(f"  Gap > 0 = Earnings yield exceeds risk-free (favorable)")
    print(f"  Gap < 0 = Risk-free rate exceeds earnings yield (unfavorable)")

    ey_data = merged.copy()
    ey_data["earnings_yield"] = (1 / ey_data["forward_pe"].replace(0, np.nan)) * 100
    ey_data["ey_gap"] = ey_data["earnings_yield"] - 4.5
    ey_sorted = ey_data.sort_values("ey_gap", ascending=False)

    print(f"\n  {'Sector':<25} {'Fwd PE':>7} {'EY':>7} {'Gap':>7} {'Verdict':<20}")
    print(f"  {'-' * 70}")

    for _, row in ey_sorted.iterrows():
        ey = row.get("earnings_yield", np.nan)
        gap = row.get("ey_gap", np.nan)
        fwd_pe = f"{row['forward_pe']:.1f}" if pd.notna(row.get('forward_pe')) else "N/A"

        if pd.isna(gap):
            verdict = "N/A"
        elif gap > 2:
            verdict = "Attractive yield"
        elif gap > 0:
            verdict = "Adequate yield"
        elif gap > -2:
            verdict = "Thin yield"
        else:
            verdict = "Yield deficit"

        ey_str = f"{ey:.1f}%" if pd.notna(ey) else "N/A"
        gap_str = f"{gap:+.1f}%" if pd.notna(gap) else "N/A"

        print(f"  {row['sector']:<25} {fwd_pe:>7} {ey_str:>7} {gap_str:>7} {verdict:<20}")

    # Top industries by PEG within each "cheap" sector
    if industry_df is not None:
        print(f"\n{'=' * W}")
        print("CHEAPEST INDUSTRIES BY PEG RATIO")
        print(f"{'=' * W}")
        print(f"\n  Showing industries with PEG < 1.5 and positive growth:")

        cheap_industries = industry_df[
            (industry_df["peg_ratio"] < 1.5)
            & (industry_df["peg_ratio"] > 0)
            & (industry_df["expected_growth_5yr"] > 0)
        ].sort_values("peg_ratio")

        print(f"\n  {'Industry':<40} {'Sector':<20} {'PEG':>7} {'Fwd PE':>7} {'Growth':>7}")
        print(f"  {'-' * 85}")

        for _, row in cheap_industries.head(20).iterrows():
            peg = f"{row['peg_ratio']:.2f}" if pd.notna(row.get('peg_ratio')) else "N/A"
            fwd_pe = f"{row['forward_pe']:.1f}" if pd.notna(row.get('forward_pe')) else "N/A"
            growth = f"{row['expected_growth_5yr']:.1f}%" if pd.notna(row.get('expected_growth_5yr')) else "N/A"

            print(f"  {row['industry']:<40} {row['sector']:<20} {peg:>7} {fwd_pe:>7} {growth:>7}")

    # Most expensive industries
    if industry_df is not None:
        print(f"\n{'=' * W}")
        print("MOST EXPENSIVE INDUSTRIES BY PEG RATIO")
        print(f"{'=' * W}")
        print(f"\n  Showing industries with PEG > 3.0:")

        expensive_industries = industry_df[
            (industry_df["peg_ratio"] > 3.0)
            & (industry_df["peg_ratio"] < 100)
        ].sort_values("peg_ratio", ascending=False)

        print(f"\n  {'Industry':<40} {'Sector':<20} {'PEG':>7} {'Fwd PE':>7} {'Growth':>7}")
        print(f"  {'-' * 85}")

        for _, row in expensive_industries.head(15).iterrows():
            peg = f"{row['peg_ratio']:.2f}" if pd.notna(row.get('peg_ratio')) else "N/A"
            fwd_pe = f"{row['forward_pe']:.1f}" if pd.notna(row.get('forward_pe')) else "N/A"
            growth = f"{row['expected_growth_5yr']:.1f}%" if pd.notna(row.get('expected_growth_5yr')) else "N/A"

            print(f"  {row['industry']:<40} {row['sector']:<20} {peg:>7} {fwd_pe:>7} {growth:>7}")

    # Sector ETFs for reference
    print(f"\n{'=' * W}")
    print("SECTOR ETFS FOR REFERENCE")
    print(f"{'=' * W}")

    print(f"\n  {'Sector':<25} {'ETF':>6} {'Rating':<16}")
    print(f"  {'-' * 50}")

    for _, row in merged.iterrows():
        etf = SECTOR_ETFS.get(row["sector"], "???")
        rating = row.get("rating", "N/A")
        indicator = _rating_indicator(rating)
        print(f"  {row['sector']:<25} {etf:>6} {indicator} {rating}")

    # Summary
    print(f"\n{'=' * W}")
    print("SUMMARY")
    print(f"{'=' * W}")

    cheap = merged[merged["rating"].isin(["VERY CHEAP", "CHEAP"])]["sector"].tolist()
    fair = merged[merged["rating"] == "FAIR"]["sector"].tolist()
    expensive = merged[merged["rating"].isin(["EXPENSIVE", "VERY EXPENSIVE"])]["sector"].tolist()

    print(f"\n  Cheap sectors:     {', '.join(cheap) if cheap else 'None'}")
    print(f"  Fair sectors:      {', '.join(fair) if fair else 'None'}")
    print(f"  Expensive sectors: {', '.join(expensive) if expensive else 'None'}")

    print(f"\n  Note: Scores are relative rankings across sectors, not absolute")
    print(f"  valuations. A sector scored 'Fair' may still be historically")
    print(f"  expensive if all sectors are elevated. Compare Forward P/E to")
    print(f"  historical ranges for absolute assessment.")

    print(f"\n  Key insight from PEG analysis:")
    best_peg = peg_sorted.iloc[0]
    worst_peg = peg_sorted.iloc[-1]
    print(f"  - Best value:  {best_peg['sector']} (PEG {best_peg['peg_ratio']:.2f})")
    print(f"  - Most expensive: {worst_peg['sector']} (PEG {worst_peg['peg_ratio']:.2f})")

    print(f"\n{'=' * W}")
    print("Done!")
    print(f"{'=' * W}")


def main():
    # Build/update historical database if module is available
    if HISTORICAL_AVAILABLE:
        import os
        if not os.path.exists(HISTORICAL_DB_PATH):
            print("Building historical Damodaran database (first-time setup)...")
            print("This fetches 10 years of archived data and may take 1-2 minutes.\n")
            build_historical_database()
            print()

    sector_df, industry_df = fetch_all_damodaran_data()
    scores, cape_details, margin_details = score_sectors(sector_df)
    print_valuation_report(sector_df, scores, cape_details, margin_details, industry_df)

    # LSEG live data enrichment
    if LSEG_AVAILABLE:
        print(f"\n\n{'=' * 90}")
        print("LSEG LIVE DATA ENRICHMENT")
        print(f"{'=' * 90}")

        lseg_df = fetch_lseg_sector_data()
        if lseg_df is not None:
            print_lseg_comparison(lseg_df, sector_df)
        else:
            print("\n  LSEG data not available (is LSEG Workspace running?)")
    else:
        print("\n  [LSEG enrichment module not available — skipping live data]")

    if not HISTORICAL_AVAILABLE:
        print("\n  [Historical data module not available — CAPE proxy and margin reversion using defaults]")


if __name__ == "__main__":
    main()
