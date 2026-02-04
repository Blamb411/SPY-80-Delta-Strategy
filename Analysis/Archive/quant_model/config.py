"""
Quant Scoring Model — Configuration
=====================================
All constants, factor weights, sub-factor definitions, grade thresholds,
API paths, and database settings.
"""

import os

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SA_DIR = os.path.dirname(BASE_DIR)  # Seeking Alpha Backtests/
PROJECT_DIR = os.path.dirname(SA_DIR)  # Claude Options Trading Project/

DB_PATH = os.path.join(BASE_DIR, "quant_scoring.db")
PRICE_CACHE_DB = os.path.join(SA_DIR, "price_cache.db")

# API keys
GURUFOCUS_API_KEY_FILE = os.path.join(BASE_DIR, "gurufocus_api_key.txt")
MASSIVE_API_KEY_FILE = os.path.join(PROJECT_DIR, "Massive backtesting", "api_key.txt")

EXCEL_FILE = os.path.join(SA_DIR, "ProQuant History 1_29_2026.xlsx")

# ---------------------------------------------------------------------------
# API Endpoints
# ---------------------------------------------------------------------------
GURUFOCUS_BASE_URL = "https://api.gurufocus.com/public/user"

# ---------------------------------------------------------------------------
# Universe Filters
# ---------------------------------------------------------------------------
MIN_MARKET_CAP = 500_000_000  # $500M
MIN_PRICE = 10.0
EXCLUDE_REITS = True

# ---------------------------------------------------------------------------
# Factor Weights — 5-Factor (when LSEG EPS available)
# ---------------------------------------------------------------------------
FACTOR_WEIGHTS_5F = {
    "value": 0.15,
    "growth": 0.20,
    "profitability": 0.25,
    "momentum": 0.15,
    "eps_revisions": 0.25,
}

# ---------------------------------------------------------------------------
# Factor Weights — 4-Factor Fallback (no LSEG)
# ---------------------------------------------------------------------------
FACTOR_WEIGHTS_4F = {
    "value": 0.20,
    "growth": 0.25,
    "profitability": 0.30,
    "momentum": 0.25,
}

# Active weight set (switch to 5F when LSEG is integrated)
FACTOR_WEIGHTS = FACTOR_WEIGHTS_4F

# ---------------------------------------------------------------------------
# Sub-Factor Definitions
# ---------------------------------------------------------------------------
# Each entry: (metric_name, weight, higher_is_better)

VALUE_SUB_FACTORS = [
    ("forward_pe",    0.25, False),
    ("peg_ratio",     0.20, False),
    ("ps_ratio",      0.15, False),
    ("pb_ratio",      0.15, False),
    ("ev_to_ebitda",  0.25, False),
]

GROWTH_SUB_FACTORS = [
    ("revenue_growth_yoy",  0.25, True),   # RevenueMean now vs 1y ago
    ("revenue_growth_3y",   0.15, True),   # FY2/FY1 Revenue forward growth
    ("ebitda_growth_yoy",   0.20, True),   # EBITDAMean now vs 1y ago
    ("eps_growth_yoy",      0.25, True),   # FY2/FY1 EPS forward growth
    ("eps_growth_3y",       0.15, True),   # EPSMean now vs 1y ago (trailing)
]

PROFITABILITY_SUB_FACTORS = [
    ("gross_margin",  0.20, True),
    ("ebit_margin",   0.25, True),
    ("net_margin",    0.20, True),
    ("roe",           0.35, True),
    # ROA and ROIC not available on LSEG Workspace license;
    # weights redistributed to remaining sub-factors
]

MOMENTUM_SUB_FACTORS = [
    ("price_return_3m",   0.25, True),
    ("price_return_6m",   0.35, True),
    ("price_return_12m",  0.40, True),
]

EPS_REVISIONS_SUB_FACTORS = [
    ("eps_estimate_change_7d",   0.15, True),   # % change in FY1 consensus, 7d
    ("eps_estimate_change_30d",  0.25, True),   # % change in FY1 consensus, 30d
    ("eps_estimate_change_90d",  0.20, True),   # % change in FY1 consensus, 90d
    ("last_earnings_surprise",   0.25, True),   # (actual - est) / |est|
    ("estimate_dispersion",      0.15, True),   # -StdDev/|Mean| (inverted, higher=better)
]

# ---------------------------------------------------------------------------
# Grade Thresholds — sector percentile → letter grade
# ---------------------------------------------------------------------------
# Ordered from highest to lowest threshold
GRADE_THRESHOLDS = [
    (95, "A+"),
    (85, "A"),
    (75, "A-"),
    (65, "B+"),
    (55, "B"),
    (45, "B-"),
    (35, "C+"),
    (25, "C"),
    (15, "C-"),
    (10, "D+"),
    (5,  "D"),
    (2,  "D-"),
    (0,  "F"),
]

# Grade to numeric mapping for circuit breaker checks
GRADE_NUMERIC = {
    "A+": 12, "A": 11, "A-": 10,
    "B+": 9,  "B": 8,  "B-": 7,
    "C+": 6,  "C": 5,  "C-": 4,
    "D+": 3,  "D": 2,  "D-": 1,
    "F": 0,
}

# ---------------------------------------------------------------------------
# Circuit Breakers
# ---------------------------------------------------------------------------
# If any of these factors has grade at or below the threshold, cap at Hold
CIRCUIT_BREAKERS = {
    # factor_name: minimum_grade (grades below this trigger cap)
    "growth":         "D+",   # D+ or worse → cap at Hold
    "momentum":       "D+",
    "eps_revisions":  "D+",
    "value":          "D-",   # D- or worse → cap at Hold
    "profitability":  "D-",
}

# ---------------------------------------------------------------------------
# Rating Classification — composite score → rating
# ---------------------------------------------------------------------------
RATING_THRESHOLDS = [
    (4.5, "Strong Buy"),
    (4.0, "Buy"),
    (2.5, "Hold"),
    (1.5, "Sell"),
    (0.0, "Strong Sell"),
]

# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------
PERSISTENCE_DAYS = 75  # Minimum streak for Alpha Pick eligibility

# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------
COMPOSITE_MIN = 1.0
COMPOSITE_MAX = 5.0

# ---------------------------------------------------------------------------
# API Rate Limiting
# ---------------------------------------------------------------------------
GURUFOCUS_DELAY_SECONDS = 0.5
POLYGON_DELAY_SECONDS = 0.15

# ---------------------------------------------------------------------------
# GuruFocus Metric Mapping
# ---------------------------------------------------------------------------
# Maps our internal metric names to GuruFocus API field paths
# These are accessed via the /stock/{symbol}/summary and /financials endpoints
GURUFOCUS_METRIC_MAP = {
    # Value metrics
    "pe_ratio":       "ratios.P/E",
    "forward_pe":     "ratios.Forward P/E",
    "pb_ratio":       "ratios.P/B",
    "ps_ratio":       "ratios.P/S",
    "ev_to_ebitda":   "ratios.EV-to-EBITDA",
    "peg_ratio":      "ratios.PEG",
    # Profitability metrics
    "gross_margin":   "profitability.Gross Margin %",
    "ebit_margin":    "profitability.EBIT Margin %",
    "net_margin":     "profitability.Net Margin %",
    "roe":            "profitability.ROE %",
    "roa":            "profitability.ROA %",
    "roic":           "profitability.ROIC %",
    # Growth metrics
    "revenue_growth_yoy":  "growth.Revenue Growth (YoY)",
    "revenue_growth_3y":   "growth.3-Year Revenue Growth Rate",
    "eps_growth_yoy":      "growth.EPS Growth (YoY)",
    "eps_growth_3y":       "growth.3-Year EPS Growth Rate",
    "ebitda_growth_yoy":   "growth.EBITDA Growth (YoY)",
}

# ---------------------------------------------------------------------------
# Known Alpha Picks (from ProQuant History spreadsheet)
# Used for validation — these are SA's actual selections
# Format: (symbol, pick_date)
# ---------------------------------------------------------------------------
# Populated at runtime from the Excel file; see universe_builder.py


def load_api_key(path):
    """Load an API key from a text file."""
    with open(path) as f:
        return f.read().strip()
