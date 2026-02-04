"""
Options Scanner v8 - Enhanced Trading Algorithms
=================================================
Connects to Interactive Brokers via ib_insync to scan equity options,
compute volatility metrics, identify rich/cheap options, and build iron condor structures.

Enhancements from v7:
1. Added ±40Δ buckets for better granularity
2. Tightened MAX_DELTA_GAP for liquid names (0.12 -> 0.08)
3. Added volatility regime adjustment to HV weighting
4. Added vega-weighted scoring
5. Added term structure consideration (backwardation detection)
6. Added skew-adjusted scoring
7. Dynamic short strike selection based on IV rank
8. Asymmetric condor support based on skew
9. Expected value (EV) calculation for condors
10. Wing-specific IV for POP estimation with fat-tail adjustment
11. Earnings awareness (flags options spanning earnings)
12. Greeks-based position sizing suggestions
13. Calendar spread detection
14. Enhanced data models for new metrics
"""

from __future__ import annotations

import csv
import math
import statistics
import os
import json
import sqlite3
import logging
import time as pytime
from dataclasses import dataclass, asdict, field
from datetime import datetime, date, time as dt_time, timezone, timedelta
from typing import Optional, List, Dict, Any, Tuple, Iterable, Union

from ib_insync import IB, Stock, Option, Ticker

# =========================
# CONFIG
# =========================

SYMBOLS = ["AAPL", "MSFT", "NVDA"]

# Expiration targeting
TARGET_DTES = [30, 60, 90]
MIN_DTE = 20
MAX_DTE = 140

# Delta buckets per expiration - ENHANCED: Added ±40Δ buckets
DELTA_BUCKETS = [
    ("P10", -0.10),
    ("P25", -0.25),
    ("P40", -0.40),  # NEW
    ("P50", -0.50),
    ("C50", +0.50),
    ("C40", +0.40),  # NEW
    ("C25", +0.25),
    ("C10", +0.10),
]

# ENHANCED: Tightened for liquid names
MAX_DELTA_GAP = 0.08  # was 0.12

# Quote quality filters
MAX_REL_SPREAD = 0.30
MAX_ABS_SPREAD = 3.00

# IB connection
IB_HOST = "127.0.0.1"
IB_PORT = 7497
IB_CLIENT_ID = 12
CONNECT_TIMEOUT_SEC = 30
MARKET_DATA_TYPE = 1

USE_SNAPSHOT_QUOTES = True
TICKER_CHUNK_SIZE = 50
QUALIFY_CHUNK_SIZE = 40

SPOT_SNAPSHOT_WAIT_SEC = 2.0
SPOT_STREAM_WAIT_SEC = 2.0
SPOT_POLL_SEC = 0.2
SPOT_TRY_STREAM_FALLBACK = True

HIST_DAILY_DURATION = "1 Y"
HIST_TIMEOUT_SEC = 30
HV_CACHE_MAX_AGE_DAYS = 14
HIST_RETRIES = 2

ALLOW_STALE_SPOT_IF_EMPTY_DURING_RTH = True

# Strike range
RANGE_SIGMA_MULT = 2.5
RANGE_PCT_MIN = 0.10
RANGE_PCT_MAX = 0.60
MAX_STRIKES_TO_TRY = 50

# Strike grid probing
PREFERRED_STRIKE_STEPS = [5.0, 2.5, 1.0, 0.5]
STEP_PROBE_STEPS_EACH_SIDE = 8

# Output
OUTPUT_DIR = "./output"
WRITE_CSV = True
WRITE_SQLITE = True
SQLITE_PATH = os.path.join(OUTPUT_DIR, "options_scanner.db")
WRITE_TRADE_PLANS = True
TRADE_PLANS_DIR = OUTPUT_DIR

SUPPRESS_EXPECTED_IB_WARNINGS = True

# Timezone
try:
    from zoneinfo import ZoneInfo
    EXCHANGE_TZ = ZoneInfo("America/New_York")
except Exception:
    EXCHANGE_TZ = datetime.now().astimezone().tzinfo
    print("WARNING: Could not load ZoneInfo('America/New_York'). Using local timezone.")

AUTO_FREEZE_OUTSIDE_RTH = True
ALLOW_STALE_QUOTES_OUTSIDE_RTH = True
ALLOW_STALE_SPOT_OUTSIDE_RTH = True
ASSUMED_REL_SPREAD_WHEN_STALE = 0.10

# Ranking
RANKING_MAX_SPREAD_PCT = 3.0
RANKING_REQUIRE_BBO = True
RANKING_MAX_DELTAGAP = 0.05

# Wing selection
WING_MIN_LEVELS = 1
WING_MAX_LEVELS = 6
WING_MAX_SPREAD_PCT = 3.0
WING_REQUIRE_BBO = True

# Condor configuration
CONDOR_LIMIT_EDGE = 0.02
CONDOR_LIMIT_TICK = 0.01
CONDOR_MIN_WING_WIDTH = 10.0
CONDOR_MIN_CREDIT_TO_WIDTH = 0.20
CONDOR_SHORT_MAX_SPREAD_PCT = 3.0
PRINT_CONDOR_POP = True

# NEW: Dynamic short strike selection based on IV rank
CONDOR_HIGH_IV_RANK_THRESHOLD = 0.50  # Above this, use wider shorts (20Δ)
CONDOR_LOW_IV_RANK_THRESHOLD = 0.30   # Below this, use tighter shorts (30Δ)
CONDOR_HIGH_IV_SHORT_DELTA = 0.20     # Delta for shorts in high IV
CONDOR_NORMAL_SHORT_DELTA = 0.25      # Delta for shorts in normal IV
CONDOR_LOW_IV_SHORT_DELTA = 0.30      # Delta for shorts in low IV

# NEW: Asymmetric condor based on skew
CONDOR_SKEW_ASYMMETRY_THRESHOLD = 0.02  # If skew > 2%, widen put side
CONDOR_ASYMMETRIC_WIDTH_MULT = 1.5      # Multiply put wing width by this factor

# NEW: Expected value requirements
CONDOR_MIN_EXPECTED_VALUE = 0.0  # Only show condors with positive EV
CONDOR_MIN_EV_PER_WIDTH = 0.01   # Min EV as fraction of max width

# NEW: Position sizing configuration
POSITION_SIZING_ENABLED = True
DEFAULT_ACCOUNT_SIZE = 100000.0   # $100k default
MAX_POSITION_PCT = 0.05           # Max 5% of account per position
MAX_TOTAL_DELTA = 50.0            # Max absolute delta exposure
MAX_TOTAL_VEGA = 500.0            # Max vega exposure
MAX_LOSS_PER_TRADE = 500.0        # Max loss per single trade

# NEW: Calendar spread detection
CALENDAR_SPREAD_ENABLED = True
CALENDAR_BACKWARDATION_THRESHOLD = 1.10  # Near IV / Far IV > 1.10 triggers calendar suggestion

# NEW: Earnings awareness - TIERED APPROACH
# The key question is: how many days from TODAY to earnings?
# (not from expiry to earnings)
EARNINGS_AWARENESS_ENABLED = True
EARNINGS_SKIP_THRESHOLD_DAYS = 7      # SKIP condors if earnings within this many days
EARNINGS_WARN_THRESHOLD_DAYS = 21     # WARN STRONGLY if earnings within this many days
EARNINGS_HARD_SKIP = True             # If True, actually skip; if False, just warn
EARNINGS_EV_PENALTY_FACTOR = 0.5      # Multiply EV by this factor for near-earnings trades (warn tier)

# NEW: Fat tail adjustment for POP
FAT_TAIL_IV_MULTIPLIER = 1.15  # Increase IV by 15% for tail probability estimation

# NEW: Skew adjustment - expected skew by delta (empirical averages)
# Positive values mean puts typically trade richer than calls at that delta
EXPECTED_SKEW_BY_DELTA = {
    0.10: 0.03,   # 10Δ puts typically 3 vol points richer than 10Δ calls
    0.25: 0.02,   # 25Δ puts typically 2 vol points richer
    0.40: 0.01,   # 40Δ puts typically 1 vol point richer
    0.50: 0.00,   # ATM is the reference
}

# Smile outliers
PRINT_SMILE_OUTLIERS = True
OUTLIER_TOP_N = 5
OUTLIER_HALF_WINDOW = 2
OUTLIER_MIN_NEIGHBORS = 3
OUTLIER_MIN_POINTS = 12
OUTLIER_REQUIRE_BBO = True
OUTLIER_MAX_SPREAD_PCT = 3.0
OUTLIER_MIN_ABS_DELTA = 0.05
OUTLIER_MAX_ABS_DELTA = 0.95
MIN_OUTLIER_VOLPTS = 0.25

# Error handling
SUPPRESS_ERROR_10197_LOG_LINES = True
COUNT_ERROR_10197 = True
ERROR_10197_CODE = 10197

# Strike selection
WING_PCT_LADDER = [0.10, 0.15, 0.20, 0.25, 0.30, 0.35]
WING_EXTREMES_EACH_SIDE = 4
NEAR_ATM_STRIKES = 20

OUTSIDE_RTH_MARKET_DATA_TYPE = 4

# Quality thresholds
TRUE_RICH_LOG_TH = 0.10
TRUE_CHEAP_LOG_TH = -0.05
QUALITY_MAX_SPREAD_PCT = 3.0
QUALITY_MAX_DELTAGAP = 0.05


# =========================
# DATA MODELS - ENHANCED
# =========================

@dataclass
class BucketRow:
    """Represents a single option at a specific delta bucket."""
    scan_ts_utc: str
    scan_date: str
    symbol: str
    spot: float
    tenor_target: int
    expiration: str
    dte: int
    t_years: float
    bucket: str
    delta_target: float
    right: str
    strike: float
    delta: Optional[float]
    delta_gap: Optional[float]
    bid: Optional[float]
    ask: Optional[float]
    mid: Optional[float]
    spread: Optional[float]
    rel_spread: Optional[float]
    iv: Optional[float]
    hv20: Optional[float]
    hv60: Optional[float]
    hv120: Optional[float]
    hv_ref: Optional[float]
    w20: Optional[float]
    w60: Optional[float]
    w120: Optional[float]
    iv_hv_ref: Optional[float]
    label_ref: str
    conId: int
    localSymbol: str
    tradingClass: str
    multiplier: str
    exchange: str
    iv_rank_252: Optional[float]
    iv_pctile_252: Optional[float]
    # NEW fields
    vega: Optional[float] = None
    theta: Optional[float] = None
    gamma: Optional[float] = None
    vega_weighted_score: Optional[float] = None
    skew_adjusted_iv: Optional[float] = None
    days_to_earnings: Optional[int] = None
    spans_earnings: bool = False


@dataclass
class ExpiryMetricsRow:
    """Aggregate metrics for a single expiration."""
    scan_ts_utc: str
    scan_date: str
    symbol: str
    spot: float
    tenor_target: int
    expiration: str
    dte: int
    t_years: float
    hv20: Optional[float]
    hv60: Optional[float]
    hv120: Optional[float]
    hv_ref: Optional[float]
    w20: Optional[float]
    w60: Optional[float]
    w120: Optional[float]
    atm_iv: Optional[float]
    skew_25: Optional[float]
    tail_skew_10: Optional[float]
    curvature_25: Optional[float]
    straddle_mid: Optional[float]
    implied_move_pct: Optional[float]
    realized_move_est_pct: Optional[float]
    # NEW fields
    skew_40: Optional[float] = None
    iv_rank_252: Optional[float] = None
    iv_pctile_252: Optional[float] = None
    term_structure_ratio: Optional[float] = None  # Near IV / Far IV
    vol_regime: str = "NORMAL"  # RISING, FALLING, NORMAL
    days_to_earnings: Optional[int] = None
    calendar_spread_signal: bool = False


@dataclass
class CondorAnalysis:
    """Enhanced condor analysis with EV and position sizing."""
    symbol: str
    expiration: str
    tenor_target: int
    
    # Strikes
    long_put_strike: float
    short_put_strike: float
    short_call_strike: float
    long_call_strike: float
    
    # Greeks
    net_delta: float
    net_vega: float
    net_theta: float
    
    # Pricing
    credit_mid: Optional[float]
    credit_conservative: Optional[float]
    suggested_limit: Optional[float]
    
    # Widths
    put_width: float
    call_width: float
    max_width: float
    
    # Risk/Reward
    max_profit: float
    max_loss: float
    break_even_low: float
    break_even_high: float
    
    # Probabilities - ENHANCED with wing-specific IV
    pop_between_bes: Optional[float]
    pop_between_shorts: Optional[float]
    pop_fat_tail_adjusted: Optional[float]
    
    # NEW: Expected value
    expected_value: Optional[float]
    ev_per_width: Optional[float]
    
    # NEW: Position sizing
    suggested_quantity: int
    max_position_risk: float
    
    # NEW: Quality flags
    is_symmetric: bool
    spans_earnings: bool
    short_delta_type: str  # "HIGH_IV", "NORMAL", "LOW_IV"
    
    # NEW: Earnings risk tier
    days_to_earnings: Optional[int] = None
    earnings_risk_tier: str = "NONE"  # "SKIP", "WARN", "NOTE", "NONE"
    ev_pre_earnings_penalty: Optional[float] = None  # EV before penalty applied
    
    skip_reason: Optional[str] = None


@dataclass 
class CalendarSpreadSignal:
    """Signal for calendar spread opportunity."""
    symbol: str
    strike: float
    right: str  # 'C' or 'P'
    near_expiry: str
    far_expiry: str
    near_iv: float
    far_iv: float
    iv_ratio: float  # near/far
    near_mid: Optional[float]
    far_mid: Optional[float]
    net_debit: Optional[float]
    signal_strength: float  # Higher = stronger signal


@dataclass
class PositionSizingSuggestion:
    """Position sizing recommendation."""
    symbol: str
    strategy: str
    max_quantity_by_account: int
    max_quantity_by_delta: int
    max_quantity_by_vega: int
    max_quantity_by_loss: int
    suggested_quantity: int
    reasoning: str


@dataclass
class ScoreParts:
    """Components of the option edge score - ENHANCED."""
    score: float
    iv_hvref: float
    raw_log: float
    hist_term: Optional[float]
    signal: float
    spr_pen: float
    delta_pen: float
    # NEW fields
    vega_weighted_score: Optional[float] = None
    skew_adjustment: Optional[float] = None
    term_structure_adj: Optional[float] = None


@dataclass
class SmilePoint:
    """A single point on the volatility smile."""
    right: str
    strike: float
    delta: float
    abs_delta: float
    iv: float
    rel_spread: float
    bid: float
    ask: float


@dataclass
class SmileOutlier:
    """An outlier from the local volatility smile."""
    right: str
    strike: float
    delta: float
    abs_delta: float
    iv: float
    exp_iv: float
    resid: float
    resid_rel: float
    spr_pct: float


# =========================
# ERROR 10197 COUNTER
# =========================

ERROR_10197_TOTAL = 0
ERROR_10197_BY_SYMBOL_EXP: Dict[Tuple[str, str], int] = {}


class Suppress10197Filter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        return ("Error 10197" not in msg) and ("No market data during competing" not in msg)


def on_ib_error(reqId: int, errorCode: int, errorString: str, contract) -> None:
    global ERROR_10197_TOTAL, ERROR_10197_BY_SYMBOL_EXP
    if errorCode != ERROR_10197_CODE:
        return
    ERROR_10197_TOTAL += 1
    sym = getattr(contract, "symbol", "UNKNOWN") if contract is not None else "UNKNOWN"
    exp = getattr(contract, "lastTradeDateOrContractMonth", "") if contract is not None else ""
    key = (sym, exp)
    ERROR_10197_BY_SYMBOL_EXP[key] = ERROR_10197_BY_SYMBOL_EXP.get(key, 0) + 1


def print_10197_summary() -> None:
    if not COUNT_ERROR_10197 or ERROR_10197_TOTAL <= 0:
        return
    print("\n" + "-" * 96)
    print(f"Market-data warnings suppressed: Error 10197 (competing session) x {ERROR_10197_TOTAL}")
    for (sym, exp), n in sorted(ERROR_10197_BY_SYMBOL_EXP.items()):
        print(f"  {sym} {exp}: {n}")
    print("-" * 96 + "\n")


# =========================
# UTILITY FUNCTIONS
# =========================

def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        v = float(x)
        return None if math.isnan(v) else v
    except Exception:
        return None


def _is_valid_price(x: Optional[float], *, max_price: float = 1e7) -> bool:
    try:
        xf = float(x)
    except (TypeError, ValueError):
        return False
    return math.isfinite(xf) and (0 < xf < max_price)


def chunks(seq: List[Any], n: int):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def normalize_strike(x: float) -> float:
    return round(float(x), 4)


def is_good_quote(bid: Optional[float], ask: Optional[float]) -> bool:
    if bid is None or ask is None or bid <= 0 or ask <= 0 or ask < bid:
        return False
    spread = ask - bid
    mid = (bid + ask) / 2
    if mid <= 0:
        return False
    return spread <= MAX_ABS_SPREAD and (spread / mid) <= MAX_REL_SPREAD


def rel_spread(bid: Optional[float], ask: Optional[float]) -> Optional[float]:
    if bid is None or ask is None:
        return None
    try:
        bid_f, ask_f = float(bid), float(ask)
    except Exception:
        return None
    if bid_f <= 0 or ask_f <= 0 or ask_f < bid_f:
        return None
    mid = (bid_f + ask_f) / 2.0
    return (ask_f - bid_f) / mid if mid > 0 else None


def _fmt_price(x: Optional[float]) -> str:
    return "n/a" if x is None else f"{x:.2f}"


def _fmt_pct(x: Optional[float], digits: int = 2) -> str:
    return f"{x*100:.{digits}f}%" if x is not None else "n/a"


def _fmt(x: Optional[float], digits: int = 3) -> str:
    return f"{x:.{digits}f}" if x is not None else "n/a"


def _norm_cdf(x: float) -> float:
    """Standard normal CDF."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


# =========================
# EARNINGS AWARENESS - NEW
# =========================

def get_next_earnings_date(ib: IB, symbol: str) -> Optional[date]:
    """
    Query IB for the next earnings date for a symbol.
    Returns None if not available or on error.
    """
    try:
        stock = Stock(symbol, "SMART", "USD")
        ib.qualifyContracts(stock)
        
        # Request fundamental data - earnings calendar
        # Note: This requires appropriate IB data subscriptions
        calendar = ib.reqFundamentalData(stock, "CalendarReport")
        
        if calendar:
            # Parse the XML calendar data for next earnings
            # This is a simplified parser - actual XML parsing would be more robust
            import re
            match = re.search(r'<EarningsDate>(\d{4}-\d{2}-\d{2})</EarningsDate>', calendar)
            if match:
                return datetime.strptime(match.group(1), "%Y-%m-%d").date()
    except Exception as e:
        # Earnings data not available - this is common
        pass
    
    return None


def get_earnings_dates_cached(ib: IB, symbols: List[str]) -> Dict[str, Optional[date]]:
    """
    Get earnings dates for multiple symbols, with caching.
    """
    earnings_cache: Dict[str, Optional[date]] = {}
    
    for symbol in symbols:
        if EARNINGS_AWARENESS_ENABLED:
            earnings_cache[symbol] = get_next_earnings_date(ib, symbol)
        else:
            earnings_cache[symbol] = None
    
    return earnings_cache


def days_until_earnings(earnings_date: Optional[date], reference_date: date) -> Optional[int]:
    """Calculate days from reference_date to earnings_date."""
    if earnings_date is None:
        return None
    delta = (earnings_date - reference_date).days
    return delta if delta >= 0 else None


def option_spans_earnings(expiry_date: date, earnings_date: Optional[date], today: date) -> bool:
    """Check if an option's life spans an earnings date."""
    if earnings_date is None:
        return False
    return today <= earnings_date <= expiry_date


def classify_earnings_risk(
    days_to_earnings: Optional[int],
    expiry_date: date,
    earnings_date: Optional[date],
    today: date,
) -> Tuple[str, str]:
    """
    Classify earnings risk into tiers based on days from TODAY to earnings.
    
    Returns:
        (tier, description) where tier is one of:
        - "SKIP": Earnings within EARNINGS_SKIP_THRESHOLD_DAYS - too risky
        - "WARN": Earnings within EARNINGS_WARN_THRESHOLD_DAYS - elevated risk
        - "NOTE": Option spans earnings but > warn threshold - worth noting
        - "NONE": No earnings concern
    """
    if earnings_date is None or days_to_earnings is None:
        return ("NONE", "No earnings date available")
    
    # Check if option even spans earnings
    spans = option_spans_earnings(expiry_date, earnings_date, today)
    if not spans:
        return ("NONE", "Option expires before earnings")
    
    # Classify based on days from TODAY to earnings
    if days_to_earnings <= EARNINGS_SKIP_THRESHOLD_DAYS:
        return ("SKIP", f"Earnings in {days_to_earnings}d - BINARY EVENT RISK")
    elif days_to_earnings <= EARNINGS_WARN_THRESHOLD_DAYS:
        return ("WARN", f"Earnings in {days_to_earnings}d - elevated risk")
    else:
        return ("NOTE", f"Earnings in {days_to_earnings}d - noted")


def get_earnings_ev_adjustment(tier: str, base_ev: Optional[float]) -> Tuple[Optional[float], Optional[float]]:
    """
    Apply EV penalty based on earnings risk tier.
    
    Returns:
        (adjusted_ev, pre_penalty_ev)
    """
    if base_ev is None:
        return None, None
    
    if tier == "WARN":
        # Apply penalty factor for near-earnings trades
        adjusted = base_ev * EARNINGS_EV_PENALTY_FACTOR
        return adjusted, base_ev
    else:
        # No penalty for SKIP (won't trade anyway), NOTE, or NONE
        return base_ev, base_ev


# =========================
# VOLATILITY REGIME - NEW
# =========================

def detect_vol_regime(hv20: Optional[float], hv60: Optional[float], 
                      hv120: Optional[float]) -> str:
    """
    Detect volatility regime based on HV term structure.
    RISING: Short-term HV > Long-term HV (vol expanding)
    FALLING: Short-term HV < Long-term HV (vol contracting)
    NORMAL: Roughly flat term structure
    """
    if hv20 is None or hv60 is None:
        return "NORMAL"
    
    ratio = hv20 / hv60 if hv60 > 0 else 1.0
    
    if ratio > 1.15:
        return "RISING"
    elif ratio < 0.85:
        return "FALLING"
    else:
        return "NORMAL"


def adjust_hv_weights_for_regime(base_weights: Tuple[float, float, float], 
                                  regime: str) -> Tuple[float, float, float]:
    """
    Adjust HV weights based on volatility regime.
    In rising vol, favor shorter-term HV.
    In falling vol, favor longer-term HV.
    """
    w20, w60, w120 = base_weights
    
    if regime == "RISING":
        # Shift weight toward HV20
        adjustment = 0.15
        w20_new = min(1.0, w20 + adjustment)
        w60_new = max(0.0, w60 - adjustment * 0.6)
        w120_new = max(0.0, w120 - adjustment * 0.4)
    elif regime == "FALLING":
        # Shift weight toward HV60/HV120
        adjustment = 0.15
        w20_new = max(0.0, w20 - adjustment)
        w60_new = w60 + adjustment * 0.4
        w120_new = w120 + adjustment * 0.6
    else:
        return base_weights
    
    # Renormalize
    total = w20_new + w60_new + w120_new
    if total > 0:
        return (w20_new / total, w60_new / total, w120_new / total)
    return base_weights


# =========================
# SKEW ADJUSTMENT - NEW
# =========================

def get_expected_skew_for_delta(abs_delta: float) -> float:
    """
    Get the expected skew (put IV - call IV) for a given absolute delta.
    Interpolates between known points.
    """
    deltas = sorted(EXPECTED_SKEW_BY_DELTA.keys())
    
    if abs_delta <= deltas[0]:
        return EXPECTED_SKEW_BY_DELTA[deltas[0]]
    if abs_delta >= deltas[-1]:
        return EXPECTED_SKEW_BY_DELTA[deltas[-1]]
    
    # Linear interpolation
    for i in range(len(deltas) - 1):
        if deltas[i] <= abs_delta <= deltas[i + 1]:
            d0, d1 = deltas[i], deltas[i + 1]
            s0, s1 = EXPECTED_SKEW_BY_DELTA[d0], EXPECTED_SKEW_BY_DELTA[d1]
            t = (abs_delta - d0) / (d1 - d0)
            return s0 + t * (s1 - s0)
    
    return 0.0


def compute_skew_adjusted_iv(iv: float, delta: float, right: str) -> float:
    """
    Compute skew-adjusted IV by removing expected skew.
    This normalizes puts and calls to a common basis.
    """
    abs_delta = abs(delta)
    expected_skew = get_expected_skew_for_delta(abs_delta)
    
    if right == "P":
        # Puts typically trade richer, so subtract expected skew
        return iv - expected_skew / 2
    else:
        # Calls typically trade cheaper, so add expected skew
        return iv + expected_skew / 2


# =========================
# VEGA-WEIGHTED SCORING - NEW
# =========================

def compute_vega_weighted_score(raw_log_score: float, vega: Optional[float], 
                                 spot: float) -> Optional[float]:
    """
    Weight the IV/HV score by vega to reflect dollar impact.
    A 10% mispricing on a high-vega option matters more than on a low-vega option.
    
    Returns score in "dollar-equivalent" units per 1% IV move.
    """
    if vega is None or vega <= 0:
        return None
    
    # Vega is typically per 1% IV move, per share
    # Normalize by spot to make comparable across different priced underlyings
    normalized_vega = vega * 100 / spot  # Per $100 of underlying
    
    return raw_log_score * normalized_vega


# =========================
# TERM STRUCTURE - NEW
# =========================

def compute_term_structure_ratio(iv_near: Optional[float], iv_far: Optional[float]) -> Optional[float]:
    """
    Compute term structure ratio (near IV / far IV).
    > 1.0 = backwardation (near-term rich)
    < 1.0 = contango (far-term rich)
    """
    if iv_near is None or iv_far is None or iv_far <= 0:
        return None
    return iv_near / iv_far


def term_structure_score_adjustment(term_ratio: Optional[float], dte: int) -> float:
    """
    Adjust score based on term structure.
    In backwardation, near-term options should be penalized (they're rich).
    In contango, near-term options should be boosted (they're cheap).
    """
    if term_ratio is None:
        return 0.0
    
    # Only apply to shorter-dated options
    if dte > 60:
        return 0.0
    
    # Deviation from flat (1.0)
    deviation = term_ratio - 1.0
    
    # Scale adjustment by how short-dated the option is
    dte_factor = max(0, (60 - dte) / 60)
    
    # Negative adjustment for backwardation (rich), positive for contango (cheap)
    return -deviation * 0.5 * dte_factor


# =========================
# CALENDAR SPREAD DETECTION - NEW
# =========================

def detect_calendar_spread_opportunities(
    near_tickers: Dict[str, Ticker],
    far_tickers: Dict[str, Ticker],
    symbol: str,
    spot: float,
    near_exp: str,
    far_exp: str,
) -> List[CalendarSpreadSignal]:
    """
    Detect calendar spread opportunities when near-term IV is significantly
    higher than far-term IV (backwardation).
    """
    signals: List[CalendarSpreadSignal] = []
    
    if not CALENDAR_SPREAD_ENABLED:
        return signals
    
    # Focus on ATM strikes for calendars
    for bucket in ["P50", "C50", "P40", "C40"]:
        near_t = near_tickers.get(bucket)
        far_t = far_tickers.get(bucket)
        
        if near_t is None or far_t is None:
            continue
        
        near_mg = getattr(near_t, "modelGreeks", None)
        far_mg = getattr(far_t, "modelGreeks", None)
        
        if near_mg is None or far_mg is None:
            continue
        
        near_iv = safe_float(near_mg.impliedVol)
        far_iv = safe_float(far_mg.impliedVol)
        
        if near_iv is None or far_iv is None or far_iv <= 0:
            continue
        
        iv_ratio = near_iv / far_iv
        
        if iv_ratio >= CALENDAR_BACKWARDATION_THRESHOLD:
            near_c = near_t.contract
            far_c = far_t.contract
            
            near_bid = safe_float(near_t.bid)
            near_ask = safe_float(near_t.ask)
            far_bid = safe_float(far_t.bid)
            far_ask = safe_float(far_t.ask)
            
            near_mid = (near_bid + near_ask) / 2 if near_bid and near_ask else None
            far_mid = (far_bid + far_ask) / 2 if far_bid and far_ask else None
            
            # Calendar debit = buy far, sell near
            net_debit = None
            if near_mid and far_mid:
                net_debit = far_mid - near_mid
            
            signal = CalendarSpreadSignal(
                symbol=symbol,
                strike=float(near_c.strike),
                right=str(near_c.right),
                near_expiry=near_exp,
                far_expiry=far_exp,
                near_iv=near_iv,
                far_iv=far_iv,
                iv_ratio=iv_ratio,
                near_mid=near_mid,
                far_mid=far_mid,
                net_debit=net_debit,
                signal_strength=(iv_ratio - 1.0) * 10,  # Scale for readability
            )
            signals.append(signal)
    
    return signals


def print_calendar_spread_signals(signals: List[CalendarSpreadSignal]) -> None:
    """Print calendar spread opportunities."""
    if not signals:
        return
    
    print("\n" + "=" * 96)
    print("CALENDAR SPREAD OPPORTUNITIES (Backwardation Detected)")
    print("=" * 96)
    
    for s in sorted(signals, key=lambda x: -x.signal_strength):
        print(
            f"  {s.symbol} {s.strike:.2f}{s.right}  "
            f"Near={s.near_expiry} ({_fmt_pct(s.near_iv)}) / "
            f"Far={s.far_expiry} ({_fmt_pct(s.far_iv)})  "
            f"Ratio={s.iv_ratio:.2f}  "
            f"Debit={_fmt_price(s.net_debit)}  "
            f"Signal={s.signal_strength:.1f}"
        )


# =========================
# POSITION SIZING - NEW
# =========================

def compute_position_sizing(
    strategy: str,
    symbol: str,
    max_loss_per_contract: float,
    net_delta_per_contract: float,
    net_vega_per_contract: float,
    current_portfolio_delta: float = 0.0,
    current_portfolio_vega: float = 0.0,
    account_size: float = DEFAULT_ACCOUNT_SIZE,
) -> PositionSizingSuggestion:
    """
    Compute position sizing based on multiple constraints.
    """
    # Constraint 1: Account size (max % of account at risk)
    max_risk_dollars = account_size * MAX_POSITION_PCT
    qty_by_account = int(max_risk_dollars / max_loss_per_contract) if max_loss_per_contract > 0 else 0
    
    # Constraint 2: Max loss per trade
    qty_by_loss = int(MAX_LOSS_PER_TRADE / max_loss_per_contract) if max_loss_per_contract > 0 else 0
    
    # Constraint 3: Delta exposure
    remaining_delta = MAX_TOTAL_DELTA - abs(current_portfolio_delta)
    qty_by_delta = int(remaining_delta / abs(net_delta_per_contract)) if net_delta_per_contract != 0 else 999
    
    # Constraint 4: Vega exposure
    remaining_vega = MAX_TOTAL_VEGA - abs(current_portfolio_vega)
    qty_by_vega = int(remaining_vega / abs(net_vega_per_contract)) if net_vega_per_contract != 0 else 999
    
    # Take the minimum of all constraints
    suggested = max(1, min(qty_by_account, qty_by_loss, qty_by_delta, qty_by_vega))
    
    # Determine the binding constraint
    constraints = [
        (qty_by_account, "account size limit"),
        (qty_by_loss, "max loss per trade"),
        (qty_by_delta, "delta exposure limit"),
        (qty_by_vega, "vega exposure limit"),
    ]
    binding = min(constraints, key=lambda x: x[0])
    
    return PositionSizingSuggestion(
        symbol=symbol,
        strategy=strategy,
        max_quantity_by_account=qty_by_account,
        max_quantity_by_delta=qty_by_delta,
        max_quantity_by_vega=qty_by_vega,
        max_quantity_by_loss=qty_by_loss,
        suggested_quantity=suggested,
        reasoning=f"Limited by {binding[1]}",
    )


# =========================
# ENHANCED POP ESTIMATION - NEW
# =========================

def estimate_pop_with_smile(
    spot: float,
    low_strike: float,
    high_strike: float,
    low_iv: float,  # IV at the low strike (put side)
    high_iv: float,  # IV at the high strike (call side)
    t_years: float,
    use_fat_tails: bool = True,
) -> Optional[float]:
    """
    Estimate probability of profit using strike-specific IVs
    instead of a single ATM IV. Optionally applies fat-tail adjustment.
    """
    if not all(_is_valid_price(x) for x in [spot, low_strike, high_strike]):
        return None
    if low_iv is None or high_iv is None or low_iv <= 0 or high_iv <= 0:
        return None
    if t_years is None or t_years <= 0 or high_strike <= low_strike:
        return None
    
    # Apply fat tail adjustment if requested
    if use_fat_tails:
        low_iv = low_iv * FAT_TAIL_IV_MULTIPLIER
        high_iv = high_iv * FAT_TAIL_IV_MULTIPLIER
    
    # Use the relevant IV for each side
    # Probability of staying above low_strike uses put-side IV
    sd_low = low_iv * math.sqrt(t_years)
    mu_low = math.log(spot) - 0.5 * low_iv * low_iv * t_years
    z_low = (math.log(low_strike) - mu_low) / sd_low if sd_low > 0 else 0
    prob_above_low = 1.0 - _norm_cdf(z_low)
    
    # Probability of staying below high_strike uses call-side IV
    sd_high = high_iv * math.sqrt(t_years)
    mu_high = math.log(spot) - 0.5 * high_iv * high_iv * t_years
    z_high = (math.log(high_strike) - mu_high) / sd_high if sd_high > 0 else 0
    prob_below_high = _norm_cdf(z_high)
    
    # Approximate probability of staying between both
    # This is an approximation - true probability requires correlation
    pop = max(0.0, min(1.0, prob_above_low + prob_below_high - 1.0))
    
    return pop


def estimate_pop_log_normal(
    spot: float,
    low: float,
    high: float,
    iv: float,
    t_years: float,
    drift: float = 0.0,
) -> Optional[float]:
    """Original POP estimation for backward compatibility."""
    if not all(_is_valid_price(x) for x in [spot, low, high]):
        return None
    if iv is None or not math.isfinite(iv) or iv <= 0:
        return None
    if t_years is None or t_years <= 0 or high <= low:
        return None
    
    sd = iv * math.sqrt(t_years)
    if sd <= 0:
        return None
    
    mu = math.log(spot) + (drift - 0.5 * iv * iv) * t_years
    z_low = (math.log(low) - mu) / sd
    z_high = (math.log(high) - mu) / sd
    
    return max(0.0, min(1.0, _norm_cdf(z_high) - _norm_cdf(z_low)))


# =========================
# EXPECTED VALUE CALCULATION - NEW  
# =========================

def compute_condor_expected_value(
    credit: float,
    max_width: float,
    pop: float,
) -> Tuple[float, float]:
    """
    Compute expected value of an iron condor.
    
    Returns:
        (expected_value, ev_per_width)
    """
    max_profit = credit * 100  # Per contract
    max_loss = (max_width - credit) * 100
    
    # Simple EV: weighted average of outcomes
    # This assumes binary outcome (full profit or full loss) which is conservative
    ev = (pop * max_profit) - ((1 - pop) * max_loss)
    ev_per_width = ev / (max_width * 100) if max_width > 0 else 0
    
    return ev, ev_per_width


# =========================
# DYNAMIC SHORT STRIKE SELECTION - NEW
# =========================

def select_short_delta_for_iv_rank(iv_rank: Optional[float]) -> Tuple[float, str]:
    """
    Select the delta for short strikes based on IV rank.
    High IV -> wider shorts (lower delta) for more premium
    Low IV -> tighter shorts (higher delta) for better probability
    
    Returns: (target_delta, delta_type_label)
    """
    if iv_rank is None:
        return (CONDOR_NORMAL_SHORT_DELTA, "NORMAL")
    
    if iv_rank >= CONDOR_HIGH_IV_RANK_THRESHOLD:
        return (CONDOR_HIGH_IV_SHORT_DELTA, "HIGH_IV")
    elif iv_rank <= CONDOR_LOW_IV_RANK_THRESHOLD:
        return (CONDOR_LOW_IV_SHORT_DELTA, "LOW_IV")
    else:
        return (CONDOR_NORMAL_SHORT_DELTA, "NORMAL")


def should_use_asymmetric_condor(skew_25: Optional[float]) -> Tuple[bool, float]:
    """
    Determine if an asymmetric condor is warranted based on skew.
    If puts are significantly richer than calls, widen the put side.
    
    Returns: (use_asymmetric, put_width_multiplier)
    """
    if skew_25 is None:
        return (False, 1.0)
    
    if skew_25 > CONDOR_SKEW_ASYMMETRY_THRESHOLD:
        return (True, CONDOR_ASYMMETRIC_WIDTH_MULT)
    
    return (False, 1.0)


# =========================
# TRADE PLAN JSON
# =========================

def _parse_utc_ts(ts: Union[datetime, str]) -> datetime:
    if isinstance(ts, datetime):
        return ts
    if isinstance(ts, str):
        s = ts.strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    raise TypeError(f"Unsupported timestamp type: {type(ts)}")


def save_trade_plan_json(*, out_dir: str, plan: dict, scan_ts_utc: Union[datetime, str],
                         symbol: str, exp: str, tenor_days: int) -> str:
    scan_dt = _parse_utc_ts(scan_ts_utc)
    os.makedirs(out_dir, exist_ok=True)
    ts = scan_dt.strftime("%Y%m%d_%H%M%S")
    safe_symbol = symbol.replace("/", "_").replace(" ", "_")
    safe_exp = str(exp).replace("/", "_").replace(" ", "_").replace(":", "_")
    path = os.path.join(out_dir, f"trade_plan_{ts}_{safe_symbol}_{safe_exp}_{tenor_days}D.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(plan, f, indent=2)
    print(f"Saved trade plan JSON: {path}")
    return path


# =========================
# IB CONNECTION
# =========================

def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def connect_ib() -> IB:
    if SUPPRESS_ERROR_10197_LOG_LINES:
        logging.getLogger("ib_insync.wrapper").addFilter(Suppress10197Filter())

    last_exc: Optional[Exception] = None
    for attempt in range(1, 4):
        ib = IB()
        if COUNT_ERROR_10197:
            ib.errorEvent += on_ib_error
        try:
            ib.connect(IB_HOST, IB_PORT, clientId=IB_CLIENT_ID, timeout=CONNECT_TIMEOUT_SEC)
            if not ib.isConnected():
                raise RuntimeError("IB.connect returned but ib.isConnected() is False")
            try:
                ib.RequestTimeout = 60
            except Exception:
                pass
            return ib
        except Exception as e:
            last_exc = e
            try:
                ib.disconnect()
            except Exception:
                pass
            pytime.sleep(1.0)

    raise RuntimeError(
        f"IB connect failed after 3 attempts to {IB_HOST}:{IB_PORT}. Last error: {repr(last_exc)}"
    )


# =========================
# MARKET DATA
# =========================

def req_tickers_chunked(ib: IB, contracts: List[Any], chunk_size: int = TICKER_CHUNK_SIZE) -> List[Ticker]:
    out: List[Ticker] = []
    if not contracts:
        return out
    for chunk in chunks(contracts, chunk_size):
        out.extend(ib.reqTickers(*chunk))
    return out


def get_spot_anytime(ib: IB, underlying: Stock, *, allow_close: bool,
                     allow_hist_fallback: bool) -> Tuple[Optional[float], str]:
    try:
        if getattr(underlying, "conId", 0) in (0, None):
            ib.qualifyContracts(underlying)
    except Exception:
        ib.qualifyContracts(underlying)

    bid = ask = last = close_px = None
    try:
        ticks = ib.reqTickers(underlying)
    except Exception:
        ticks = []

    if ticks:
        t = ticks[0]
        bid, ask = getattr(t, "bid", None), getattr(t, "ask", None)
        last, close_px = getattr(t, "last", None), getattr(t, "close", None)

    if not _is_valid_price(last) and not (_is_valid_price(bid) and _is_valid_price(ask)) and not _is_valid_price(close_px):
        ib.sleep(0.2)
        try:
            ticks2 = ib.reqTickers(underlying)
        except Exception:
            ticks2 = []
        if ticks2:
            t2 = ticks2[0]
            bid, ask = getattr(t2, "bid", None), getattr(t2, "ask", None)
            last, close_px = getattr(t2, "last", None), getattr(t2, "close", None)

    if _is_valid_price(last):
        return float(last), "LAST"
    if _is_valid_price(bid) and _is_valid_price(ask) and float(ask) >= float(bid):
        return (float(bid) + float(ask)) / 2.0, "MID"
    if allow_close and _is_valid_price(close_px):
        return float(close_px), "CLOSE"

    if allow_hist_fallback:
        for use_rth, src in ((True, "HIST_CLOSE_RTH"), (False, "HIST_CLOSE_24H")):
            try:
                bars = ib.reqHistoricalData(underlying, endDateTime="", durationStr="10 D",
                                            barSizeSetting="1 day", whatToShow="TRADES",
                                            useRTH=use_rth, formatDate=1)
            except Exception:
                bars = None
            if bars:
                for b in reversed(bars):
                    close_val = getattr(b, "close", None)
                    if _is_valid_price(close_val):
                        return float(close_val), src
    return None, "NONE"


# =========================
# HISTORICAL VOLATILITY - ENHANCED
# =========================

def compute_hv(returns: List[float], lookback_days: int) -> float:
    if lookback_days < 2 or len(returns) < lookback_days:
        return float("nan")
    window = returns[-lookback_days:]
    mean_r = sum(window) / len(window)
    var = sum((x - mean_r) ** 2 for x in window) / (len(window) - 1)
    return math.sqrt(var) * math.sqrt(252)


def hv_anchor_weights(dte: int) -> Tuple[float, float, float]:
    """Base weights before regime adjustment."""
    anchors = [(30, (0.70, 0.30, 0.00)), (60, (0.00, 0.70, 0.30)),
               (90, (0.00, 0.40, 0.60)), (120, (0.00, 0.00, 1.00))]
    if dte <= anchors[0][0]:
        return anchors[0][1]
    if dte >= anchors[-1][0]:
        return anchors[-1][1]
    for (d0, w0), (d1, w1) in zip(anchors[:-1], anchors[1:]):
        if d0 < dte <= d1:
            t = (dte - d0) / (d1 - d0)
            w20 = (1 - t) * w0[0] + t * w1[0]
            w60 = (1 - t) * w0[1] + t * w1[1]
            w120 = (1 - t) * w0[2] + t * w1[2]
            s = w20 + w60 + w120
            return (w20/s, w60/s, w120/s) if s > 0 else (0.0, 1.0, 0.0)
    return anchors[-1][1]


def hv_weighted_mix_enhanced(
    dte: int, 
    hv20: Optional[float], 
    hv60: Optional[float],
    hv120: Optional[float]
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float], str]:
    """
    ENHANCED: Compute hv_ref with regime-adjusted weights.
    Returns (hv_ref, w20_used, w60_used, w120_used, regime)
    """
    # Detect regime
    regime = detect_vol_regime(hv20, hv60, hv120)
    
    # Get base weights for DTE
    base_weights = hv_anchor_weights(dte)
    
    # Adjust for regime
    adjusted_weights = adjust_hv_weights_for_regime(base_weights, regime)
    w20, w60, w120 = adjusted_weights
    
    parts = []
    if hv20 and hv20 > 0: parts.append((hv20, w20))
    if hv60 and hv60 > 0: parts.append((hv60, w60))
    if hv120 and hv120 > 0: parts.append((hv120, w120))
    
    if not parts:
        return None, 0.0, 0.0, 0.0, regime
    
    wsum = sum(w for _, w in parts)
    if wsum <= 0:
        if hv120 and hv120 > 0: return hv120, 0.0, 0.0, 1.0, regime
        if hv60 and hv60 > 0: return hv60, 0.0, 1.0, 0.0, regime
        return hv20, 1.0, 0.0, 0.0, regime
    
    hv_ref = sum(hv * w for hv, w in parts) / wsum
    w20_used = (w20 / wsum) if (hv20 and hv20 > 0) else 0.0
    w60_used = (w60 / wsum) if (hv60 and hv60 > 0) else 0.0
    w120_used = (w120 / wsum) if (hv120 and hv120 > 0) else 0.0
    
    return hv_ref, w20_used, w60_used, w120_used, regime


def load_cached_hv(conn: Optional[sqlite3.Connection], symbol: str) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[str]]:
    if conn is None:
        return None, None, None, None
    try:
        cur = conn.execute(
            "SELECT scan_ts_utc, hv20, hv60, hv120 FROM option_buckets WHERE symbol = ? "
            "AND (hv20 IS NOT NULL OR hv60 IS NOT NULL OR hv120 IS NOT NULL) "
            "ORDER BY scan_ts_utc DESC LIMIT 1", (symbol,))
        row = cur.fetchone()
        if not row:
            return None, None, None, None
        ts, hv20, hv60, hv120 = row
        try:
            dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
            if (datetime.now(timezone.utc) - dt).total_seconds() / 86400.0 > HV_CACHE_MAX_AGE_DAYS:
                return None, None, None, None
        except Exception:
            pass
        return safe_float(hv20), safe_float(hv60), safe_float(hv120), str(ts)
    except Exception:
        return None, None, None, None


# =========================
# CLASSIFICATION
# =========================

def classify_iv_ratio(ratio: float) -> str:
    if ratio != ratio: return "n/a"
    if ratio < 0.7: return "CHEAP"
    if ratio <= 1.3: return "FAIR"
    if ratio <= 2.0: return "RICH"
    return "VERY RICH"


def is_standard_monthly(dt: date) -> bool:
    return dt.weekday() == 4 and 15 <= dt.day <= 21


def is_rth_now() -> bool:
    now = datetime.now(EXCHANGE_TZ)
    t = now.time()
    rth = now.weekday() < 5 and dt_time(9, 30) <= t <= dt_time(16, 0)
    print(f"DEBUG is_rth_now: now={now.isoformat()} -> rth={rth}")
    return rth


# =========================
# EXPIRATION SELECTION
# =========================

def parse_expirations(expirations: Iterable[str], today: date) -> List[Tuple[str, date, int, bool]]:
    parsed = []
    for e in expirations:
        try:
            dt = datetime.strptime(e, "%Y%m%d").date()
        except ValueError:
            continue
        dte = (dt - today).days
        if dte > 0:
            parsed.append((e, dt, dte, is_standard_monthly(dt)))
    return parsed


def choose_three_expirations(expirations: List[str], today: date) -> List[Tuple[int, str, int]]:
    parsed = parse_expirations(expirations, today)
    if not parsed:
        return []
    in_window = [x for x in parsed if MIN_DTE <= x[2] <= MAX_DTE]
    pool = in_window if len(in_window) >= 3 else parsed
    chosen, used = [], set()

    for target in TARGET_DTES:
        cands = [x for x in pool if x[0] not in used]
        if not cands:
            break
        best = sorted(cands, key=lambda x: (abs(x[2] - target), 0 if x[3] else 1, x[2]))[0]
        chosen.append((target, best[0], best[2]))
        used.add(best[0])

    if len(chosen) < 3:
        remaining = sorted([x for x in pool if x[0] not in used], key=lambda x: (0 if x[3] else 1, x[2]))
        for item in remaining:
            if len(chosen) >= 3:
                break
            tenor = min(TARGET_DTES, key=lambda t: abs(item[2] - t))
            chosen.append((tenor, item[0], item[2]))
            used.add(item[0])

    return sorted(chosen, key=lambda x: x[0])[:3]


# =========================
# CHAIN HELPERS
# =========================

def select_best_chain(params_list, symbol: str):
    smart = [p for p in params_list if getattr(p, "exchange", "") == "SMART"]
    if not smart:
        return None
    return sorted(smart, key=lambda p: (
        0 if getattr(p, "tradingClass", "") == symbol else 1,
        0 if str(getattr(p, "multiplier", "")) == "100" else 1,
        0 if len(getattr(p, "expirations", []) or []) > 0 else 1,
    ))[0]


def grid_strikes_in_range(step: float, k_min: float, k_max: float, strikes_set: set) -> List[float]:
    out = []
    for n in range(math.floor(k_min / step), math.ceil(k_max / step) + 1):
        k = normalize_strike(n * step)
        if k in strikes_set:
            out.append(k)
    return sorted(set(out))


def probe_best_step_for_expiry(ib: IB, symbol: str, exp: str, chain, spot: float,
                               k_min: float, k_max: float, strikes_set: set) -> float:
    tc = getattr(chain, "tradingClass", "") or ""
    mult = str(getattr(chain, "multiplier", "") or "")
    best_step, best_count = PREFERRED_STRIKE_STEPS[0], -1

    for step in PREFERRED_STRIKE_STEPS:
        n0 = round(spot / step)
        strike_candidates = [normalize_strike((n0 + i) * step)
                            for i in range(-STEP_PROBE_STEPS_EACH_SIDE, STEP_PROBE_STEPS_EACH_SIDE + 1)
                            if k_min <= normalize_strike((n0 + i) * step) <= k_max
                            and normalize_strike((n0 + i) * step) in strikes_set]
        if not strike_candidates:
            continue
        opts = []
        for k in strike_candidates:
            opts.append(Option(symbol=symbol, lastTradeDateOrContractMonth=exp, strike=k, right="C",
                               exchange="SMART", currency="USD", tradingClass=tc, multiplier=mult))
            opts.append(Option(symbol=symbol, lastTradeDateOrContractMonth=exp, strike=k, right="P",
                               exchange="SMART", currency="USD", tradingClass=tc, multiplier=mult))
        qualified = []
        for chunk in chunks(opts, QUALIFY_CHUNK_SIZE):
            q = ib.qualifyContracts(*chunk)
            qualified.extend([c for c in q if int(getattr(c, "conId", 0) or 0) > 0])
        count = len(qualified)
        if count > best_count or (count == best_count and step > best_step):
            best_count, best_step = count, step
    return best_step


def nearest_strike_leq(strikes_sorted: List[float], target: float) -> float:
    import bisect
    i = bisect.bisect_right(strikes_sorted, target) - 1
    return strikes_sorted[max(0, i)]


def nearest_strike_geq(strikes_sorted: List[float], target: float) -> float:
    import bisect
    i = bisect.bisect_left(strikes_sorted, target)
    return strikes_sorted[min(i, len(strikes_sorted) - 1)]


def build_strike_selection_with_wings(chain_strikes: List[float], in_range_strikes: List[float],
                                      spot: float, max_n: int) -> List[float]:
    if not chain_strikes:
        return in_range_strikes[:max_n]
    base = in_range_strikes if in_range_strikes else chain_strikes
    picks = set()
    for pct in WING_PCT_LADDER:
        picks.add(nearest_strike_leq(chain_strikes, spot * (1.0 - pct)))
        picks.add(nearest_strike_geq(chain_strikes, spot * (1.0 + pct)))
    for k in sorted(base, key=lambda s: abs(s - spot))[:min(NEAR_ATM_STRIKES, max_n)]:
        picks.add(k)
    if base:
        picks.update(base[:WING_EXTREMES_EACH_SIDE])
        picks.update(base[-WING_EXTREMES_EACH_SIDE:])
    if len(picks) < max_n:
        for k in sorted(base, key=lambda s: abs(s - spot)):
            picks.add(k)
            if len(picks) >= max_n:
                break
    return sorted(picks)


def qualify_options_for_strikes(ib: IB, symbol: str, exp: str, chain, strikes: List[float]) -> List[Any]:
    tc = getattr(chain, "tradingClass", "") or ""
    mult = str(getattr(chain, "multiplier", "") or "")
    opts = []
    for k in strikes:
        opts.append(Option(symbol=symbol, lastTradeDateOrContractMonth=exp, strike=k, right="C",
                           exchange="SMART", currency="USD", tradingClass=tc, multiplier=mult))
        opts.append(Option(symbol=symbol, lastTradeDateOrContractMonth=exp, strike=k, right="P",
                           exchange="SMART", currency="USD", tradingClass=tc, multiplier=mult))
    qualified = []
    for chunk in chunks(opts, QUALIFY_CHUNK_SIZE):
        q = ib.qualifyContracts(*chunk)
        qualified.extend([c for c in q if int(getattr(c, "conId", 0) or 0) > 0])
    if qualified:
        return qualified
    # Fallback
    opts2 = []
    for k in strikes:
        opts2.append(Option(symbol=symbol, lastTradeDateOrContractMonth=exp, strike=k, right="C",
                            exchange="SMART", currency="USD"))
        opts2.append(Option(symbol=symbol, lastTradeDateOrContractMonth=exp, strike=k, right="P",
                            exchange="SMART", currency="USD"))
    qualified2 = []
    for chunk in chunks(opts2, QUALIFY_CHUNK_SIZE):
        q2 = ib.qualifyContracts(*chunk)
        qualified2.extend([c for c in q2 if int(getattr(c, "conId", 0) or 0) > 0])
    return qualified2


def best_contract_per_right_strike(contracts: List[Any], preferred_trading_class: str) -> List[Any]:
    by_key: Dict[Tuple[str, float], List[Any]] = {}
    for c in contracts:
        key = (str(getattr(c, "right", "")), float(getattr(c, "strike", 0.0)))
        by_key.setdefault(key, []).append(c)
    final = []
    for _, cands in by_key.items():
        primary = [x for x in cands if getattr(x, "tradingClass", "") == preferred_trading_class]
        final.append(primary[0] if primary else cands[0])
    return final


# =========================
# TICKER HELPERS
# =========================

def _contract_is_option(c: Any) -> bool:
    return getattr(c, "secType", None) == "OPT" and hasattr(c, "right") and hasattr(c, "strike")


def build_option_ticker_maps(tickers: List[Ticker]) -> Tuple[Dict[Tuple[str, float], Ticker], Dict[str, List[float]]]:
    ticker_map = {}
    strikes_by_right = {"P": set(), "C": set()}
    for t in tickers:
        c = getattr(t, "contract", None)
        if c is None or not _contract_is_option(c):
            continue
        right, strike = getattr(c, "right", None), getattr(c, "strike", None)
        if right not in ("P", "C") or strike is None:
            continue
        k = (right, round(float(strike), 4))
        ticker_map[k] = t
        strikes_by_right[right].add(k[1])
    return ticker_map, {"P": sorted(strikes_by_right["P"]), "C": sorted(strikes_by_right["C"])}


def _ticker_rel_spread_pct(t: Ticker) -> Optional[float]:
    rs = rel_spread(getattr(t, "bid", None), getattr(t, "ask", None))
    return rs * 100.0 if rs is not None else None


def _ticker_mid(t: Ticker) -> Optional[float]:
    bid, ask = getattr(t, "bid", None), getattr(t, "ask", None)
    if is_good_quote(bid, ask):
        return 0.5 * (float(bid) + float(ask))
    last = getattr(t, "last", None)
    if last is not None and float(last) > 0:
        return float(last)
    close = getattr(t, "close", None)
    return float(close) if close is not None and float(close) > 0 else None


# =========================
# WING SELECTION
# =========================

def pick_wing_ticker(ticker_map: Dict[Tuple[str, float], Ticker], strikes_by_right: Dict[str, List[float]], *,
                     right: str, short_strike: float, min_levels: int, max_levels: int,
                     max_spread_pct: float, require_bbo: bool) -> Optional[Ticker]:
    short_strike = round(float(short_strike), 4)
    strikes = strikes_by_right.get(right, [])
    if not strikes:
        return None
    if right == "P":
        candidates = sorted([s for s in strikes if s < short_strike], reverse=True)
    else:
        candidates = sorted([s for s in strikes if s > short_strike])
    best_t = None
    level = 0
    for s in candidates:
        level += 1
        if level < min_levels:
            continue
        if level > max_levels:
            break
        t = ticker_map.get((right, s))
        if t is None:
            continue
        bid, ask = getattr(t, "bid", None), getattr(t, "ask", None)
        if require_bbo and not is_good_quote(bid, ask):
            continue
        spr_pct = _ticker_rel_spread_pct(t)
        if spr_pct is not None and spr_pct <= max_spread_pct:
            best_t = t
    return best_t


def find_ticker_by_delta(
    tickers: List[Ticker],
    target_delta: float,
    spot: float,
    allow_stale: bool = False
) -> Optional[Ticker]:
    """Find ticker closest to target delta."""
    best_t = None
    best_gap = float('inf')
    
    is_put = target_delta < 0
    
    for t in tickers:
        c = getattr(t, "contract", None)
        if c is None or getattr(c, "secType", "") != "OPT":
            continue
        
        right = getattr(c, "right", "")
        if (is_put and right != "P") or (not is_put and right != "C"):
            continue
            
        mg = getattr(t, "modelGreeks", None)
        if mg is None:
            continue
        
        delta = safe_float(getattr(mg, "delta", None))
        if delta is None:
            continue
        
        bid, ask = safe_float(getattr(t, "bid", None)), safe_float(getattr(t, "ask", None))
        if not allow_stale and not is_good_quote(bid, ask):
            continue
        
        gap = abs(delta - target_delta)
        if gap < best_gap:
            best_gap = gap
            best_t = t
    
    return best_t


def pick_condor_wings_dynamic(
    tickers: List[Ticker],
    spot: float,
    iv_rank: Optional[float],
    skew_25: Optional[float],
    allow_stale: bool = False,
) -> Optional[Dict[str, Any]]:
    """
    ENHANCED: Pick condor wings with dynamic short strike selection
    based on IV rank and asymmetric wings based on skew.
    """
    # Determine short delta based on IV rank
    short_delta, delta_type = select_short_delta_for_iv_rank(iv_rank)
    
    # Find short strikes at the target delta
    short_put_t = find_ticker_by_delta(tickers, -short_delta, spot, allow_stale)
    short_call_t = find_ticker_by_delta(tickers, short_delta, spot, allow_stale)
    
    if short_put_t is None or short_call_t is None:
        return None
    
    sp_strike = float(short_put_t.contract.strike)
    sc_strike = float(short_call_t.contract.strike)
    
    # Determine if we should use asymmetric wings
    use_asymmetric, put_width_mult = should_use_asymmetric_condor(skew_25)
    
    # Build ticker map for wing selection
    ticker_map, strikes_by_right = build_option_ticker_maps(tickers)
    
    # Select wings - potentially asymmetric
    put_max_levels = int(WING_MAX_LEVELS * put_width_mult) if use_asymmetric else WING_MAX_LEVELS
    
    long_put_t = pick_wing_ticker(
        ticker_map, strikes_by_right,
        right="P", short_strike=sp_strike,
        min_levels=WING_MIN_LEVELS, max_levels=put_max_levels,
        max_spread_pct=WING_MAX_SPREAD_PCT, require_bbo=WING_REQUIRE_BBO,
    )
    long_call_t = pick_wing_ticker(
        ticker_map, strikes_by_right,
        right="C", short_strike=sc_strike,
        min_levels=WING_MIN_LEVELS, max_levels=WING_MAX_LEVELS,
        max_spread_pct=WING_MAX_SPREAD_PCT, require_bbo=WING_REQUIRE_BBO,
    )
    
    if long_put_t is None or long_call_t is None:
        return None
    
    return {
        "short_put": short_put_t,
        "long_put": long_put_t,
        "short_call": short_call_t,
        "long_call": long_call_t,
        "short_delta_type": delta_type,
        "is_asymmetric": use_asymmetric,
    }


# =========================
# DELTA BUCKETS + METRICS - ENHANCED
# =========================

def pick_delta_buckets(tickers: List[Ticker], spot: float, allow_stale: bool = False) -> Dict[str, Ticker]:
    calls, puts = [], []
    for t in tickers:
        c = getattr(t, "contract", None)
        if getattr(c, "secType", "") != "OPT":
            continue
        mg = getattr(t, "modelGreeks", None)
        if mg is None:
            continue
        delta, iv = safe_float(getattr(mg, "delta", None)), safe_float(getattr(mg, "impliedVol", None))
        if delta is None or iv is None or iv <= 0:
            continue
        strike = safe_float(getattr(c, "strike", None))
        if strike is None:
            continue
        bid, ask = safe_float(getattr(t, "bid", None)), safe_float(getattr(t, "ask", None))
        has_good_bbo = is_good_quote(bid, ask)
        if not has_good_bbo and not allow_stale:
            continue
        if not has_good_bbo and allow_stale:
            last, close = safe_float(getattr(t, "last", None)), safe_float(getattr(t, "close", None))
            if (last is None or last <= 0) and (close is None or close <= 0):
                continue
        right = getattr(c, "right", "")
        if right == "C":
            if abs(delta) < 0.35 and strike < spot:
                continue
            calls.append((t, float(delta)))
        elif right == "P":
            if abs(delta) < 0.35 and strike > spot:
                continue
            puts.append((t, float(delta)))
    chosen = {}
    for name, target in DELTA_BUCKETS:
        pool = calls if target > 0 else puts
        if pool:
            best_t, _ = min(pool, key=lambda td: abs(td[1] - target))
            chosen[name] = best_t
    return chosen


def compute_expiry_metrics_enhanced(
    chosen: Dict[str, Ticker], 
    spot: float, 
    t_years: float,
    hv_ref: Optional[float],
    iv_30d: Optional[float] = None,  # For term structure
) -> Dict[str, Any]:
    """ENHANCED: Compute expiry metrics including new fields."""
    
    def iv_of(bucket):
        t = chosen.get(bucket)
        return safe_float(t.modelGreeks.impliedVol) if t and t.modelGreeks else None

    def mid_of(bucket):
        t = chosen.get(bucket)
        if not t:
            return None
        bid, ask = safe_float(t.bid), safe_float(t.ask)
        return (bid + ask) / 2 if bid is not None and ask is not None else None

    def vega_of(bucket):
        t = chosen.get(bucket)
        return safe_float(t.modelGreeks.vega) if t and t.modelGreeks else None

    iv_p50, iv_c50 = iv_of("P50"), iv_of("C50")
    atm_iv = None
    if iv_p50 is not None and iv_c50 is not None:
        atm_iv = (iv_p50 + iv_c50) / 2
    elif iv_p50 is not None:
        atm_iv = iv_p50
    elif iv_c50 is not None:
        atm_iv = iv_c50

    iv_p25, iv_c25 = iv_of("P25"), iv_of("C25")
    skew_25 = iv_p25 - iv_c25 if iv_p25 is not None and iv_c25 is not None else None

    iv_p10, iv_c10 = iv_of("P10"), iv_of("C10")
    tail_skew_10 = iv_p10 - iv_c10 if iv_p10 is not None and iv_c10 is not None else None

    # NEW: 40 delta skew
    iv_p40, iv_c40 = iv_of("P40"), iv_of("C40")
    skew_40 = iv_p40 - iv_c40 if iv_p40 is not None and iv_c40 is not None else None

    curvature_25 = None
    if atm_iv is not None and iv_p25 is not None and iv_c25 is not None:
        curvature_25 = (iv_p25 + iv_c25) / 2 - atm_iv

    mid_c50, mid_p50 = mid_of("C50"), mid_of("P50")
    straddle_mid = implied_move_pct = None
    if mid_c50 is not None and mid_p50 is not None:
        straddle_mid = mid_c50 + mid_p50
        implied_move_pct = straddle_mid / spot if spot > 0 else None

    realized_move_est_pct = hv_ref * math.sqrt(max(t_years, 1e-6)) if hv_ref is not None else None

    # NEW: Term structure ratio
    term_structure_ratio = compute_term_structure_ratio(atm_iv, iv_30d) if iv_30d else None

    return {
        "atm_iv": atm_iv,
        "skew_25": skew_25,
        "skew_40": skew_40,
        "tail_skew_10": tail_skew_10,
        "curvature_25": curvature_25,
        "straddle_mid": straddle_mid,
        "implied_move_pct": implied_move_pct,
        "realized_move_est_pct": realized_move_est_pct,
        "term_structure_ratio": term_structure_ratio,
        "iv_p25": iv_p25,
        "iv_c25": iv_c25,
        "iv_p10": iv_p10,
        "iv_c10": iv_c10,
    }


# =========================
# SQLITE
# =========================

def ensure_columns(conn: sqlite3.Connection, table: str, needed: Dict[str, str]) -> None:
    cur = conn.execute(f"PRAGMA table_info({table})")
    existing = {row[1] for row in cur.fetchall()}
    for col, coltype in needed.items():
        if col not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype}")
    conn.commit()


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute('''CREATE TABLE IF NOT EXISTS option_buckets (
        scan_ts_utc TEXT NOT NULL, scan_date TEXT NOT NULL, symbol TEXT NOT NULL,
        tenor_target INTEGER NOT NULL, expiration TEXT NOT NULL, dte INTEGER NOT NULL,
        bucket TEXT NOT NULL, delta_target REAL NOT NULL, iv REAL, rel_spread REAL,
        PRIMARY KEY (scan_ts_utc, symbol, tenor_target, expiration, bucket, delta_target))''')
    conn.execute('''CREATE TABLE IF NOT EXISTS expiry_metrics (
        scan_ts_utc TEXT NOT NULL, scan_date TEXT NOT NULL, symbol TEXT NOT NULL,
        tenor_target INTEGER NOT NULL, expiration TEXT NOT NULL, dte INTEGER NOT NULL,
        atm_iv REAL, skew_25 REAL, tail_skew_10 REAL, curvature_25 REAL,
        implied_move_pct REAL, realized_move_est_pct REAL,
        PRIMARY KEY (scan_ts_utc, symbol, tenor_target, expiration))''')
    conn.commit()
    
    # Add new columns for v8
    ensure_columns(conn, "option_buckets", {
        "hv20": "REAL", "hv60": "REAL", "hv120": "REAL",
        "hv_ref": "REAL", "w20": "REAL", "w60": "REAL", "w120": "REAL", 
        "iv_hv_ref": "REAL",
        "vega": "REAL", "theta": "REAL", "gamma": "REAL",
        "vega_weighted_score": "REAL", "skew_adjusted_iv": "REAL",
        "days_to_earnings": "INTEGER", "spans_earnings": "INTEGER",
    })
    ensure_columns(conn, "expiry_metrics", {
        "hv20": "REAL", "hv60": "REAL", "hv120": "REAL",
        "hv_ref": "REAL", "w20": "REAL", "w60": "REAL", "w120": "REAL", 
        "straddle_mid": "REAL",
        "skew_40": "REAL", "iv_rank_252": "REAL", "iv_pctile_252": "REAL",
        "term_structure_ratio": "REAL", "vol_regime": "TEXT",
        "days_to_earnings": "INTEGER", "calendar_spread_signal": "INTEGER",
    })


def store_bucket_rows(conn: sqlite3.Connection, rows: List[BucketRow]) -> None:
    if not rows:
        return
    conn.executemany('''INSERT OR REPLACE INTO option_buckets
        (scan_ts_utc, scan_date, symbol, tenor_target, expiration, dte, bucket, delta_target,
         iv, rel_spread, hv20, hv60, hv120, hv_ref, w20, w60, w120, iv_hv_ref,
         vega, theta, gamma, vega_weighted_score, skew_adjusted_iv, days_to_earnings, spans_earnings)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
        [(r.scan_ts_utc, r.scan_date, r.symbol, r.tenor_target, r.expiration, r.dte,
          r.bucket, r.delta_target, r.iv, r.rel_spread, r.hv20, r.hv60, r.hv120,
          r.hv_ref, r.w20, r.w60, r.w120, r.iv_hv_ref,
          r.vega, r.theta, r.gamma, r.vega_weighted_score, r.skew_adjusted_iv,
          r.days_to_earnings, 1 if r.spans_earnings else 0) for r in rows])
    conn.commit()


def store_expiry_metrics(conn: sqlite3.Connection, rows: List[ExpiryMetricsRow]) -> None:
    if not rows:
        return
    conn.executemany('''INSERT OR REPLACE INTO expiry_metrics
        (scan_ts_utc, scan_date, symbol, tenor_target, expiration, dte,
         atm_iv, skew_25, tail_skew_10, curvature_25, implied_move_pct, realized_move_est_pct,
         hv20, hv60, hv120, hv_ref, w20, w60, w120, straddle_mid,
         skew_40, iv_rank_252, iv_pctile_252, term_structure_ratio, vol_regime,
         days_to_earnings, calendar_spread_signal)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
        [(r.scan_ts_utc, r.scan_date, r.symbol, r.tenor_target, r.expiration, r.dte,
          r.atm_iv, r.skew_25, r.tail_skew_10, r.curvature_25, r.implied_move_pct,
          r.realized_move_est_pct, r.hv20, r.hv60, r.hv120, r.hv_ref, r.w20, r.w60,
          r.w120, r.straddle_mid, r.skew_40, r.iv_rank_252, r.iv_pctile_252,
          r.term_structure_ratio, r.vol_regime, r.days_to_earnings,
          1 if r.calendar_spread_signal else 0) for r in rows])
    conn.commit()


def get_iv_history(conn: sqlite3.Connection, symbol: str, tenor_target: int,
                   bucket: str, lookback: int = 252) -> List[float]:
    cur = conn.execute(
        "SELECT iv FROM option_buckets WHERE symbol = ? AND tenor_target = ? AND bucket = ? "
        "AND iv IS NOT NULL ORDER BY scan_ts_utc DESC LIMIT ?",
        (symbol, tenor_target, bucket, lookback))
    return list(reversed([row[0] for row in cur.fetchall() if row[0] is not None]))


def iv_rank_and_percentile(history: List[float], current: float,
                           min_points: int = 20) -> Tuple[Optional[float], Optional[float]]:
    if current is None or not history or len(history) < min_points:
        return None, None
    lo, hi = min(history), max(history)
    rank = 0.5 if abs(hi - lo) < 1e-9 else (current - lo) / (hi - lo)
    pct = sum(1 for x in history if x < current) / len(history)
    return max(0.0, min(1.0, rank)), max(0.0, min(1.0, pct))


# =========================
# SCORING - ENHANCED
# =========================

def option_edge_parts_enhanced(r: BucketRow, term_ratio: Optional[float] = None) -> Optional[ScoreParts]:
    """ENHANCED: Score with vega-weighting, skew adjustment, and term structure."""
    if r.iv is None or r.hv_ref is None or r.hv_ref <= 0:
        return None
    
    iv_hvref = r.iv / r.hv_ref
    raw_log = math.log(iv_hvref)
    
    hist_term = None
    signal = raw_log
    if r.iv_pctile_252 is not None:
        hist_term = 2.0 * (r.iv_pctile_252 - 0.5)
        signal = 0.6 * raw_log + 0.4 * hist_term
    
    spr_pen = 2.0 * (r.rel_spread if r.rel_spread is not None else ASSUMED_REL_SPREAD_WHEN_STALE)
    delta_pen = 0.5 * (r.delta_gap if r.delta_gap is not None else 0.0)
    
    # NEW: Vega-weighted score
    vega_weighted = None
    if r.vega is not None and r.spot > 0:
        vega_weighted = compute_vega_weighted_score(raw_log, r.vega, r.spot)
    
    # NEW: Skew adjustment
    skew_adj = None
    if r.delta is not None and r.iv is not None:
        adj_iv = compute_skew_adjusted_iv(r.iv, r.delta, r.right)
        skew_adj = (adj_iv - r.iv) / r.iv if r.iv > 0 else 0
        signal = signal - skew_adj * 0.2
    
    # NEW: Term structure adjustment
    term_adj = term_structure_score_adjustment(term_ratio, r.dte)
    signal = signal + term_adj
    
    score = signal - spr_pen - delta_pen
    
    return ScoreParts(
        score=score,
        iv_hvref=iv_hvref,
        raw_log=raw_log,
        hist_term=hist_term,
        signal=signal,
        spr_pen=spr_pen,
        delta_pen=delta_pen,
        vega_weighted_score=vega_weighted,
        skew_adjustment=skew_adj,
        term_structure_adj=term_adj,
    )


def option_edge_score(r: BucketRow) -> Optional[float]:
    parts = option_edge_parts_enhanced(r)
    return parts.score if parts else None


def is_quality_candidate(r: BucketRow) -> bool:
    spr_ok = r.rel_spread is not None and (r.rel_spread * 100.0) <= QUALITY_MAX_SPREAD_PCT
    d_ok = r.delta_gap is None or r.delta_gap <= QUALITY_MAX_DELTAGAP
    return spr_ok and d_ok


# =========================
# OUTPUT FORMATTING
# =========================

def format_rank_line_global(r: BucketRow, p: ScoreParts) -> str:
    spr_pct = (r.rel_spread * 100.0) if r.rel_spread is not None else None
    pctile = (r.iv_pctile_252 * 100.0) if r.iv_pctile_252 is not None else None
    vega_str = f"vW={p.vega_weighted_score:.2f}" if p.vega_weighted_score else "vW=n/a"
    earnings_flag = " [EARN]" if r.spans_earnings else ""
    
    return (f"{r.symbol} {r.tenor_target:>3}D {r.expiration} {r.bucket:>3} "
            f"{r.right}{r.strike:.2f} delta={_fmt(r.delta,3)} "
            f"IV={_fmt_pct(r.iv,2)} HVref={_fmt_pct(r.hv_ref,2)} "
            f"iv/hv={p.iv_hvref:.2f} log={p.raw_log:+.3f} "
            f"spr={_fmt(spr_pct,2)}% {vega_str} "
            f"score={p.score:+.3f}{earnings_flag}")


def format_rank_line_compact(r: BucketRow, p: ScoreParts) -> str:
    spr_pct = (r.rel_spread * 100.0) if r.rel_spread is not None else None
    earnings_flag = " [EARN]" if r.spans_earnings else ""
    return (f"{r.expiration} {r.bucket:>3} {r.right}{r.strike:.2f} "
            f"IV={_fmt_pct(r.iv,2)} HVref={_fmt_pct(r.hv_ref,2)} "
            f"log={p.raw_log:+.3f} score={p.score:+.3f} (spr={_fmt(spr_pct,2)}%){earnings_flag}")


def write_csv(rows: List[Any], filename: str) -> Optional[str]:
    if not rows:
        return None
    ensure_output_dir()
    path = os.path.join(OUTPUT_DIR, filename)
    fieldnames = list(asdict(rows[0]).keys())
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(asdict(r))
    return path


def print_top_rich_cheap(rows: List[BucketRow], top_n: int = 10) -> None:
    scored = []
    for r in rows:
        if RANKING_REQUIRE_BBO and r.rel_spread is None:
            continue
        if r.rel_spread is not None and (r.rel_spread * 100.0) > RANKING_MAX_SPREAD_PCT:
            continue
        if r.delta_gap is None or r.delta_gap > RANKING_MAX_DELTAGAP:
            continue
        parts = option_edge_parts_enhanced(r)
        if parts:
            scored.append((parts, r))
    if not scored:
        print("\nNo scored rows.")
        return
    scored.sort(key=lambda x: x[0].score, reverse=True)
    
    print("\n" + "=" * 100)
    print(f"TOP {top_n} RICHEST (spr<={RANKING_MAX_SPREAD_PCT:.1f}%) - ENHANCED SCORING")
    print("=" * 100)
    for p, r in scored[:top_n]:
        print(format_rank_line_global(r, p))
    
    print("\n" + "=" * 100)
    print(f"TOP {top_n} CHEAPEST (spr<={RANKING_MAX_SPREAD_PCT:.1f}%) - ENHANCED SCORING")
    print("=" * 100)
    for p, r in list(reversed(scored[-top_n:])):
        print(format_rank_line_global(r, p))


def print_top_by_symbol_tenor(rows: List[BucketRow], top_n: int = 3) -> None:
    grouped: Dict[Tuple[str, int], List[Tuple[ScoreParts, BucketRow]]] = {}
    for r in rows:
        parts = option_edge_parts_enhanced(r)
        if parts:
            grouped.setdefault((r.symbol, r.tenor_target), []).append((parts, r))
    if not grouped:
        print("\nNo grouped scored rows.")
        return
    
    print("\n" + "=" * 100)
    print(f"TOP {top_n} RICH / CHEAP PER SYMBOL x TENOR - ENHANCED")
    print("=" * 100)
    
    for (sym, tenor) in sorted(grouped.keys()):
        items = grouped[(sym, tenor)]
        if not items:
            continue
        items_asc = sorted(items, key=lambda x: x[0].score)
        cheapest, richest = items_asc[:top_n], list(reversed(items_asc))[:top_n]
        exps = sorted({r.expiration for _, r in items})
        exp_note = exps[0] if len(exps) == 1 else ",".join(exps)
        
        # Check for earnings
        earnings_warning = any(r.spans_earnings for _, r in items)
        earn_str = " ⚠️ EARNINGS IN PERIOD" if earnings_warning else ""
        
        print(f"\n{sym}  {tenor}D  (exp: {exp_note}){earn_str}")
        
        logs_all = [p.raw_log for p, _ in items]
        med_log = statistics.median(logs_all) if logs_all else float("nan")
        med_ratio = math.exp(med_log) if med_log == med_log else float("nan")
        quality_items = [(p, r) for (p, r) in items if is_quality_candidate(r)]
        q_n = len(quality_items)
        rich_ct = sum(1 for p, _ in quality_items if p.raw_log >= TRUE_RICH_LOG_TH)
        cheap_ct = sum(1 for p, _ in quality_items if p.raw_log <= TRUE_CHEAP_LOG_TH)
        
        print(f"  Summary: median log(IV/HVref)={med_log:+.3f}  (median IV/HVref={med_ratio:.2f})")
        print(f"  True rich/cheap (quality): rich={rich_ct}/{q_n}, cheap={cheap_ct}/{q_n}")
        
        print("  Richest:")
        for p, r in richest:
            print("   ", format_rank_line_compact(r, p))
        print("  Cheapest:")
        for p, r in cheapest:
            print("   ", format_rank_line_compact(r, p))


# =========================
# CONDOR ANALYSIS - ENHANCED
# =========================

def analyze_condor_enhanced(
    condor_tickers: Dict[str, Any],
    symbol: str,
    exp: str,
    tenor_target: int,
    spot: float,
    t_years: float,
    atm_iv: Optional[float],
    iv_rank: Optional[float],
    skew_25: Optional[float],
    earnings_date: Optional[date],
    today: date,
    scan_ts_utc: str,
) -> Optional[CondorAnalysis]:
    """ENHANCED: Full condor analysis with EV, position sizing, and quality checks."""
    
    lp_t = condor_tickers["long_put"]
    sp_t = condor_tickers["short_put"]
    sc_t = condor_tickers["short_call"]
    lc_t = condor_tickers["long_call"]
    
    lp, sp = lp_t.contract, sp_t.contract
    sc, lc = sc_t.contract, lc_t.contract
    
    # Get greeks
    def get_greek(t, attr):
        mg = getattr(t, "modelGreeks", None)
        return safe_float(getattr(mg, attr, None)) if mg else None
    
    lp_delta = get_greek(lp_t, "delta") or 0
    sp_delta = get_greek(sp_t, "delta") or 0
    sc_delta = get_greek(sc_t, "delta") or 0
    lc_delta = get_greek(lc_t, "delta") or 0
    
    lp_vega = get_greek(lp_t, "vega") or 0
    sp_vega = get_greek(sp_t, "vega") or 0
    sc_vega = get_greek(sc_t, "vega") or 0
    lc_vega = get_greek(lc_t, "vega") or 0
    
    lp_theta = get_greek(lp_t, "theta") or 0
    sp_theta = get_greek(sp_t, "theta") or 0
    sc_theta = get_greek(sc_t, "theta") or 0
    lc_theta = get_greek(lc_t, "theta") or 0
    
    # Net greeks (long wings, short body)
    net_delta = (lp_delta + lc_delta) - (sp_delta + sc_delta)
    net_vega = (lp_vega + lc_vega) - (sp_vega + sc_vega)
    net_theta = (sp_theta + sc_theta) - (lp_theta + lc_theta)  # Positive theta for credit spreads
    
    # Pricing
    def get_mid(t):
        bid, ask = getattr(t, "bid", None), getattr(t, "ask", None)
        return (float(bid) + float(ask)) / 2 if is_good_quote(bid, ask) else None
    
    lp_mid = get_mid(lp_t)
    sp_mid = get_mid(sp_t)
    sc_mid = get_mid(sc_t)
    lc_mid = get_mid(lc_t)
    
    credit_mid = None
    if all(m is not None for m in [lp_mid, sp_mid, sc_mid, lc_mid]):
        credit_mid = (sp_mid + sc_mid) - (lp_mid + lc_mid)
    
    # Conservative credit
    lp_bid, lp_ask = getattr(lp_t, "bid", None), getattr(lp_t, "ask", None)
    sp_bid, sp_ask = getattr(sp_t, "bid", None), getattr(sp_t, "ask", None)
    sc_bid, sc_ask = getattr(sc_t, "bid", None), getattr(sc_t, "ask", None)
    lc_bid, lc_ask = getattr(lc_t, "bid", None), getattr(lc_t, "ask", None)
    
    credit_conservative = None
    if all(is_good_quote(b, a) for b, a in [(lp_bid, lp_ask), (sp_bid, sp_ask), 
                                             (sc_bid, sc_ask), (lc_bid, lc_ask)]):
        credit_conservative = (sp_bid + sc_bid) - (lp_ask + lc_ask)
    
    # Widths
    put_width = float(sp.strike) - float(lp.strike)
    call_width = float(lc.strike) - float(sc.strike)
    max_width = max(put_width, call_width)
    
    credit_est = credit_conservative if credit_conservative is not None else credit_mid
    
    # Quality checks
    skip_reason = None
    
    if put_width < CONDOR_MIN_WING_WIDTH or call_width < CONDOR_MIN_WING_WIDTH:
        skip_reason = f"wing too narrow: put={put_width:.2f}, call={call_width:.2f}"
    elif credit_est is None or credit_est <= 0:
        skip_reason = "no positive credit"
    elif (credit_est / max_width) < CONDOR_MIN_CREDIT_TO_WIDTH:
        skip_reason = f"credit/width too low: {(credit_est/max_width)*100:.1f}%"
    
    # ENHANCED: Tiered earnings risk check
    expiry_date = datetime.strptime(exp, "%Y%m%d").date()
    spans_earnings = option_spans_earnings(expiry_date, earnings_date, today)
    days_to_earn = days_until_earnings(earnings_date, today) if earnings_date else None
    
    earnings_tier, earnings_desc = classify_earnings_risk(
        days_to_earn, expiry_date, earnings_date, today
    )
    
    # Only hard-skip if configured to do so AND in SKIP tier
    if earnings_tier == "SKIP" and EARNINGS_HARD_SKIP:
        skip_reason = f"earnings too close: {earnings_desc}"
    
    if skip_reason:
        return CondorAnalysis(
            symbol=symbol, expiration=exp, tenor_target=tenor_target,
            long_put_strike=float(lp.strike), short_put_strike=float(sp.strike),
            short_call_strike=float(sc.strike), long_call_strike=float(lc.strike),
            net_delta=net_delta, net_vega=net_vega, net_theta=net_theta,
            credit_mid=credit_mid, credit_conservative=credit_conservative,
            suggested_limit=None,
            put_width=put_width, call_width=call_width, max_width=max_width,
            max_profit=0, max_loss=0, break_even_low=0, break_even_high=0,
            pop_between_bes=None, pop_between_shorts=None, pop_fat_tail_adjusted=None,
            expected_value=None, ev_per_width=None,
            suggested_quantity=0, max_position_risk=0,
            is_symmetric=abs(put_width - call_width) < 1.0,
            spans_earnings=spans_earnings,
            short_delta_type=condor_tickers.get("short_delta_type", "NORMAL"),
            days_to_earnings=days_to_earn,
            earnings_risk_tier=earnings_tier,
            ev_pre_earnings_penalty=None,
            skip_reason=skip_reason,
        )
    
    # Calculate P&L metrics
    max_profit = credit_est * 100
    max_loss = (max_width - credit_est) * 100
    be_low = float(sp.strike) - credit_est
    be_high = float(sc.strike) + credit_est
    
    # POP calculations - ENHANCED with wing-specific IV
    iv_put = get_greek(sp_t, "impliedVol") or atm_iv
    iv_call = get_greek(sc_t, "impliedVol") or atm_iv
    
    pop_bes = None
    pop_shorts = None
    pop_fat_tail = None
    
    if iv_put and iv_call and atm_iv:
        # Standard POP
        pop_bes = estimate_pop_log_normal(spot, be_low, be_high, atm_iv, t_years)
        pop_shorts = estimate_pop_log_normal(spot, float(sp.strike), float(sc.strike), atm_iv, t_years)
        
        # Enhanced POP with wing-specific IV and fat tails
        pop_fat_tail = estimate_pop_with_smile(
            spot, be_low, be_high, iv_put, iv_call, t_years, use_fat_tails=True
        )
    
    # Expected value - with earnings penalty if applicable
    ev, ev_per_width = None, None
    ev_pre_penalty = None
    if pop_fat_tail is not None:
        ev, ev_per_width = compute_condor_expected_value(credit_est, max_width, pop_fat_tail)
        ev_pre_penalty = ev
        
        # Apply earnings penalty for WARN tier
        ev, ev_pre_penalty = get_earnings_ev_adjustment(earnings_tier, ev)
        if ev is not None and max_width > 0:
            ev_per_width = ev / (max_width * 100)
    
    # Position sizing
    suggested_qty = 0
    max_position_risk = max_loss
    
    if POSITION_SIZING_ENABLED and max_loss > 0:
        sizing = compute_position_sizing(
            strategy="IRON_CONDOR",
            symbol=symbol,
            max_loss_per_contract=max_loss,
            net_delta_per_contract=abs(net_delta),
            net_vega_per_contract=abs(net_vega),
        )
        suggested_qty = sizing.suggested_quantity
        max_position_risk = max_loss * suggested_qty
    
    # Suggested limit
    suggested_limit = None
    if credit_mid is not None and credit_conservative is not None:
        suggested_limit = max(credit_conservative, credit_mid - CONDOR_LIMIT_EDGE)
    elif credit_conservative is not None:
        suggested_limit = credit_conservative
    elif credit_mid is not None:
        suggested_limit = credit_mid - CONDOR_LIMIT_EDGE
    
    if suggested_limit is not None:
        suggested_limit = math.floor(suggested_limit / CONDOR_LIMIT_TICK) * CONDOR_LIMIT_TICK
        suggested_limit = round(suggested_limit, 2)
    
    return CondorAnalysis(
        symbol=symbol, expiration=exp, tenor_target=tenor_target,
        long_put_strike=float(lp.strike), short_put_strike=float(sp.strike),
        short_call_strike=float(sc.strike), long_call_strike=float(lc.strike),
        net_delta=net_delta, net_vega=net_vega, net_theta=net_theta,
        credit_mid=credit_mid, credit_conservative=credit_conservative,
        suggested_limit=suggested_limit,
        put_width=put_width, call_width=call_width, max_width=max_width,
        max_profit=max_profit, max_loss=max_loss,
        break_even_low=be_low, break_even_high=be_high,
        pop_between_bes=pop_bes, pop_between_shorts=pop_shorts,
        pop_fat_tail_adjusted=pop_fat_tail,
        expected_value=ev, ev_per_width=ev_per_width,
        suggested_quantity=suggested_qty, max_position_risk=max_position_risk,
        is_symmetric=abs(put_width - call_width) < 1.0,
        spans_earnings=spans_earnings,
        short_delta_type=condor_tickers.get("short_delta_type", "NORMAL"),
        days_to_earnings=days_to_earn,
        earnings_risk_tier=earnings_tier,
        ev_pre_earnings_penalty=ev_pre_penalty,
        skip_reason=None,
    )


def print_condor_analysis(analysis: CondorAnalysis) -> None:
    """Print enhanced condor analysis."""
    if analysis.skip_reason:
        print(f"  Condor: SKIP ({analysis.skip_reason})")
        return
    
    print(f"\n  {'='*80}")
    print(f"  IRON CONDOR ANALYSIS - {analysis.symbol} {analysis.expiration}")
    print(f"  {'='*80}")
    
    # Structure
    sym_flag = "SYMMETRIC" if analysis.is_symmetric else "ASYMMETRIC"
    print(f"  Structure: {sym_flag} | Short Δ Type: {analysis.short_delta_type}")
    print(f"  PUT SPREAD:  BUY {analysis.long_put_strike:.2f}P / SELL {analysis.short_put_strike:.2f}P (width=${analysis.put_width:.2f})")
    print(f"  CALL SPREAD: SELL {analysis.short_call_strike:.2f}C / BUY {analysis.long_call_strike:.2f}C (width=${analysis.call_width:.2f})")
    
    # Earnings risk tier - prominent display
    if analysis.earnings_risk_tier != "NONE":
        tier = analysis.earnings_risk_tier
        days = analysis.days_to_earnings
        if tier == "SKIP":
            print(f"\n  🚫 EARNINGS RISK: {tier} - Earnings in {days} days - TOO RISKY FOR CONDORS")
        elif tier == "WARN":
            print(f"\n  ⚠️  EARNINGS RISK: {tier} - Earnings in {days} days - ELEVATED RISK (EV penalized {(1-EARNINGS_EV_PENALTY_FACTOR)*100:.0f}%)")
        elif tier == "NOTE":
            print(f"\n  📝 EARNINGS: Noted - Earnings in {days} days (> {EARNINGS_WARN_THRESHOLD_DAYS}d, minimal concern)")
    
    # Greeks
    print(f"\n  Net Greeks: Δ={analysis.net_delta:+.3f}  V={analysis.net_vega:+.2f}  Θ={analysis.net_theta:+.2f}/day")
    
    # Pricing
    print(f"\n  Credit (mid): {_fmt_price(analysis.credit_mid)}  Conservative: {_fmt_price(analysis.credit_conservative)}")
    print(f"  Suggested LIMIT: {_fmt_price(analysis.suggested_limit)}")
    
    # Risk/Reward
    print(f"\n  Max Profit: ${analysis.max_profit:,.0f}  Max Loss: ${analysis.max_loss:,.0f}")
    print(f"  Break-evens: {analysis.break_even_low:.2f} / {analysis.break_even_high:.2f}")
    
    # Probabilities
    print(f"\n  POP (between BEs): {_fmt_pct(analysis.pop_between_bes)}")
    print(f"  POP (between shorts): {_fmt_pct(analysis.pop_between_shorts)}")
    print(f"  POP (fat-tail adjusted): {_fmt_pct(analysis.pop_fat_tail_adjusted)}")
    
    # Expected value - with earnings penalty info
    if analysis.expected_value is not None:
        ev_sign = "+" if analysis.expected_value >= 0 else ""
        print(f"\n  Expected Value: {ev_sign}${analysis.expected_value:,.0f} per contract")
        
        # Show pre-penalty EV if different
        if analysis.ev_pre_earnings_penalty is not None and abs(analysis.ev_pre_earnings_penalty - analysis.expected_value) > 0.01:
            pre_sign = "+" if analysis.ev_pre_earnings_penalty >= 0 else ""
            print(f"    (Pre-earnings-penalty EV: {pre_sign}${analysis.ev_pre_earnings_penalty:,.0f})")
        
        print(f"  EV/Width: {(analysis.ev_per_width or 0)*100:.1f}%")
        
        if analysis.expected_value < CONDOR_MIN_EXPECTED_VALUE:
            print(f"  ⚠️  WARNING: Negative expected value!")
    
    # Position sizing
    if POSITION_SIZING_ENABLED:
        print(f"\n  Position Sizing:")
        print(f"    Suggested Quantity: {analysis.suggested_quantity} contracts")
        print(f"    Max Position Risk: ${analysis.max_position_risk:,.0f}")
    
    print(f"  {'='*80}\n")


# =========================
# SCAN SYMBOL - ENHANCED
# =========================

def scan_symbol_enhanced(
    ib: IB, 
    conn: Optional[sqlite3.Connection], 
    symbol: str,
    earnings_dates: Dict[str, Optional[date]],
) -> Tuple[List[BucketRow], List[ExpiryMetricsRow], List[CondorAnalysis], List[CalendarSpreadSignal]]:
    """ENHANCED: Scan with all new features."""
    
    print("\n" + "=" * 100)
    print(f"SCANNING: {symbol} (Enhanced v8)")
    print("=" * 100)

    scan_ts_utc_dt = datetime.now(timezone.utc).replace(microsecond=0)
    scan_ts_utc = scan_ts_utc_dt.isoformat().replace("+00:00", "Z")
    now_et = datetime.now(EXCHANGE_TZ)
    scan_date = now_et.date().isoformat()
    today_local = now_et.date()
    
    # Get earnings date for this symbol
    earnings_date = earnings_dates.get(symbol)
    if earnings_date:
        days_to_earn = (earnings_date - today_local).days
        print(f"📅 Next earnings: {earnings_date} ({days_to_earn} days)")
    else:
        days_to_earn = None
        print("📅 Earnings date: Not available")

    underlying = Stock(symbol, "SMART", "USD")
    ib.qualifyContracts(underlying)

    rth = is_rth_now()
    session = "RTH" if rth else "OUTSIDE_RTH"
    mdt = MARKET_DATA_TYPE if rth or not AUTO_FREEZE_OUTSIDE_RTH else OUTSIDE_RTH_MARKET_DATA_TYPE
    ib.reqMarketDataType(mdt)
    mdt_label = {1: "LIVE", 2: "FROZEN", 3: "DELAYED", 4: "DELAYED_FROZEN"}.get(mdt, str(mdt))
    print(f"Session: {session}   MarketDataType={mdt_label}")

    allow_stale_spot = (not rth) and ALLOW_STALE_SPOT_OUTSIDE_RTH
    spot, spot_src = get_spot_anytime(ib, underlying, allow_close=allow_stale_spot, allow_hist_fallback=allow_stale_spot)
    if not _is_valid_price(spot) and rth and ALLOW_STALE_SPOT_IF_EMPTY_DURING_RTH:
        spot, spot_src = get_spot_anytime(ib, underlying, allow_close=True, allow_hist_fallback=True)
    if not _is_valid_price(spot):
        print(f"Could not get ANY spot for {symbol}")
        return [], [], [], []
    print(f"{symbol} spot: {spot:.2f}  [{spot_src}]")

    # Historical volatility
    hv20_val = hv60_val = hv120_val = None
    try:
        bars = ib.reqHistoricalData(underlying, endDateTime="", durationStr="1 Y", barSizeSetting="1 day",
                                    whatToShow="TRADES", useRTH=True, formatDate=1, timeout=HIST_TIMEOUT_SEC)
    except Exception:
        bars = []
    
    if not bars:
        chv20, chv60, chv120, cts = load_cached_hv(conn, symbol)
        if any(v is not None for v in (chv20, chv60, chv120)):
            hv20_val, hv60_val, hv120_val = chv20, chv60, chv120
            print(f"WARNING: using cached HV from {cts}")
    else:
        closes = [float(b.close) for b in bars if b.close is not None and b.close == b.close and b.close > 0]
        if len(closes) >= 2:
            returns = [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes))]
            hv20 = compute_hv(returns, 20)
            hv60 = compute_hv(returns, 60)
            hv120 = compute_hv(returns, 120)
            hv20_val = None if hv20 != hv20 else float(hv20)
            hv60_val = None if hv60 != hv60 else float(hv60)
            hv120_val = None if hv120 != hv120 else float(hv120)
    
    # Detect volatility regime
    vol_regime = detect_vol_regime(hv20_val, hv60_val, hv120_val)
    print(f"HV20: {_fmt_pct(hv20_val)}   HV60: {_fmt_pct(hv60_val)}   HV120: {_fmt_pct(hv120_val)}   Regime: {vol_regime}")

    # Option chain
    params_list = ib.reqSecDefOptParams(underlying.symbol, "", underlying.secType, underlying.conId)
    chain = select_best_chain(params_list, symbol)
    if chain is None:
        print("No SMART chain for", symbol)
        return [], [], [], []

    exp_choices = choose_three_expirations(sorted(chain.expirations), today_local)
    if not exp_choices:
        print("Could not choose expirations for", symbol)
        return [], [], [], []

    chain_strikes = sorted(set(normalize_strike(float(s)) for s in chain.strikes))
    strikes_set = set(chain_strikes)

    bucket_rows: List[BucketRow] = []
    metrics_rows: List[ExpiryMetricsRow] = []
    condor_analyses: List[CondorAnalysis] = []
    calendar_signals: List[CalendarSpreadSignal] = []
    
    # Store tickers by expiry for calendar spread detection
    tickers_by_exp: Dict[str, Dict[str, Ticker]] = {}

    for tenor_target, exp, dte in exp_choices:
        bucket_rows_this_exp = []
        t_years = max(dte, 1) / 365.0
        
        # ENHANCED: HV with regime adjustment
        hv_ref, w20_used, w60_used, w120_used, regime = hv_weighted_mix_enhanced(
            dte, hv20_val, hv60_val, hv120_val
        )
        vol_est = hv_ref or hv60_val or hv20_val or 0.25
        
        range_pct = max(RANGE_PCT_MIN, min(RANGE_PCT_MAX, RANGE_SIGMA_MULT * vol_est * math.sqrt(max(t_years, 1e-6))))
        k_min, k_max = float(spot) * (1.0 - range_pct), float(spot) * (1.0 + range_pct)
        
        # Check if this expiry spans earnings
        expiry_date = datetime.strptime(exp, "%Y%m%d").date()
        spans_earnings = option_spans_earnings(expiry_date, earnings_date, today_local)
        earnings_warning = " ⚠️ SPANS EARNINGS" if spans_earnings else ""
        
        print(f"\nTenor {tenor_target}D  Exp {exp}  (DTE~{dte})  Regime: {regime}{earnings_warning}")
        print(f"HV_ref: {_fmt_pct(hv_ref)}   weights: ({w20_used or 0:.2f}/{w60_used or 0:.2f}/{w120_used or 0:.2f})")

        best_step = probe_best_step_for_expiry(ib, symbol, exp, chain, float(spot), k_min, k_max, strikes_set)
        grid_strikes = grid_strikes_in_range(best_step, k_min, k_max, strikes_set) or sorted(chain_strikes, key=lambda s: abs(s - float(spot)))
        strikes_try = build_strike_selection_with_wings(chain_strikes, grid_strikes, float(spot), MAX_STRIKES_TO_TRY)

        qualified = qualify_options_for_strikes(ib, symbol, exp, chain, strikes_try)
        qualified = best_contract_per_right_strike(qualified, getattr(chain, "tradingClass", "") or symbol)
        tickers = req_tickers_chunked(ib, qualified)
        chosen = pick_delta_buckets(tickers, float(spot), allow_stale=(ALLOW_STALE_QUOTES_OUTSIDE_RTH and not rth))
        
        # Store for calendar spread detection
        tickers_by_exp[exp] = chosen

        # Compute metrics
        metrics = compute_expiry_metrics_enhanced(chosen, float(spot), t_years, hv_ref)
        atm_iv = metrics["atm_iv"]
        skew_25 = metrics["skew_25"]
        
        # Get IV rank/percentile for this expiry
        iv_rank_252 = None
        iv_pctile_252 = None
        if conn and atm_iv:
            hist = get_iv_history(conn, symbol, tenor_target, "C50", lookback=252)
            iv_rank_252, iv_pctile_252 = iv_rank_and_percentile(hist, atm_iv, min_points=20)
        
        # Term structure ratio (compare to 30D IV if this isn't 30D)
        term_ratio = None
        if tenor_target != 30 and len(exp_choices) > 1:
            # Find 30D expiry's ATM IV
            for tt, e, d in exp_choices:
                if tt == 30 and e in tickers_by_exp:
                    chosen_30 = tickers_by_exp[e]
                    m30 = compute_expiry_metrics_enhanced(chosen_30, float(spot), d/365.0, hv_ref)
                    if m30["atm_iv"]:
                        term_ratio = compute_term_structure_ratio(atm_iv, m30["atm_iv"])
                    break

        print("Bucket  Right  Strike     Delta     Bid     Ask      IV   IV/HVref  Spr%   Vega  Note")
        for bucket_name, delta_target in DELTA_BUCKETS:
            t = chosen.get(bucket_name)
            if not t or not t.modelGreeks:
                print(f"{bucket_name:>5}    n/a     n/a      n/a      n/a     n/a     n/a     n/a    n/a   n/a   MISSING")
                continue
            
            c = t.contract
            bid, ask = safe_float(t.bid), safe_float(t.ask)
            mid = (bid + ask) / 2 if bid is not None and ask is not None else None
            spread = (ask - bid) if bid is not None and ask is not None else None
            rel_spr = (spread / mid) if spread is not None and mid is not None and mid > 0 else None

            delta = safe_float(t.modelGreeks.delta)
            iv = safe_float(t.modelGreeks.impliedVol)
            vega = safe_float(t.modelGreeks.vega)
            theta = safe_float(t.modelGreeks.theta)
            gamma = safe_float(t.modelGreeks.gamma)

            delta_gap = abs(delta - delta_target) if delta is not None else None
            note = "WEAK_DELTA" if (delta_gap is not None and delta_gap > MAX_DELTA_GAP) else ""
            if not is_good_quote(bid, ask):
                note = (note + " " if note else "") + "STALE"
            if spans_earnings:
                note = (note + " " if note else "") + "EARN"

            iv_hv_ref = (iv / hv_ref) if (iv is not None and hv_ref is not None and hv_ref > 0) else None
            label_ref = classify_iv_ratio(iv_hv_ref) if iv_hv_ref is not None else "n/a"
            spr_pct = (rel_spr * 100.0) if rel_spr is not None else None
            
            # Skew-adjusted IV
            skew_adj_iv = None
            if iv is not None and delta is not None:
                skew_adj_iv = compute_skew_adjusted_iv(iv, delta, str(c.right))
            
            # Vega-weighted score
            vega_weighted = None
            if iv_hv_ref and vega and spot > 0:
                raw_log = math.log(iv_hv_ref)
                vega_weighted = compute_vega_weighted_score(raw_log, vega, spot)

            print(
                f"{bucket_name:>5}   {c.right:>5}  {float(c.strike):7.2f}  "
                f"{(delta if delta is not None else float('nan')):8.3f}  "
                f"{(bid if bid is not None else float('nan')):6.2f}  "
                f"{(ask if ask is not None else float('nan')):6.2f}  "
                f"{((iv*100) if iv is not None else float('nan')):6.2f}%  "
                f"{(iv_hv_ref if iv_hv_ref is not None else float('nan')):7.2f}  "
                f"{(spr_pct if spr_pct is not None else float('nan')):5.2f}%  "
                f"{(vega if vega is not None else float('nan')):5.2f}  {note}"
            )

            # IV rank for this bucket
            bucket_iv_rank = None
            bucket_iv_pctile = None
            if conn and iv:
                hist = get_iv_history(conn, symbol, tenor_target, bucket_name, lookback=252)
                bucket_iv_rank, bucket_iv_pctile = iv_rank_and_percentile(hist, iv, min_points=20)

            row = BucketRow(
                scan_ts_utc=scan_ts_utc, scan_date=scan_date, symbol=symbol, spot=float(spot),
                tenor_target=int(tenor_target), expiration=str(exp), dte=int(dte), t_years=float(t_years),
                bucket=bucket_name, delta_target=float(delta_target),
                right=str(c.right), strike=float(c.strike), delta=delta, delta_gap=delta_gap,
                bid=bid, ask=ask, mid=mid, spread=spread, rel_spread=rel_spr, iv=iv,
                hv20=hv20_val, hv60=hv60_val, hv120=hv120_val,
                hv_ref=hv_ref, w20=w20_used, w60=w60_used, w120=w120_used,
                iv_hv_ref=iv_hv_ref, label_ref=label_ref,
                conId=int(getattr(c, "conId", 0) or 0),
                localSymbol=str(getattr(c, "localSymbol", "") or ""),
                tradingClass=str(getattr(c, "tradingClass", "") or ""),
                multiplier=str(getattr(c, "multiplier", "") or ""),
                exchange=str(getattr(c, "exchange", "") or ""),
                iv_rank_252=bucket_iv_rank, iv_pctile_252=bucket_iv_pctile,
                vega=vega, theta=theta, gamma=gamma,
                vega_weighted_score=vega_weighted, skew_adjusted_iv=skew_adj_iv,
                days_to_earnings=days_to_earn, spans_earnings=spans_earnings,
            )
            bucket_rows.append(row)
            bucket_rows_this_exp.append(row)

        # Print expiry metrics
        print("\nExpiry metrics:")
        if atm_iv is not None:
            print(f"  ATM IV: {atm_iv*100:.2f}%  Skew25: {_fmt_pct(skew_25)}  Skew40: {_fmt_pct(metrics.get('skew_40'))}")
            print(f"  IV Rank: {_fmt_pct(iv_rank_252)}  IV Pctile: {_fmt_pct(iv_pctile_252)}")
            if term_ratio:
                ts_label = "BACKWARDATION" if term_ratio > 1.05 else ("CONTANGO" if term_ratio < 0.95 else "FLAT")
                print(f"  Term Structure: {term_ratio:.2f} ({ts_label})")
        
        if metrics["straddle_mid"] and metrics["implied_move_pct"] and metrics["realized_move_est_pct"]:
            ratio = metrics["implied_move_pct"] / metrics["realized_move_est_pct"]
            print(f"  Straddle: ${metrics['straddle_mid']:.2f}  Impl Move: {metrics['implied_move_pct']*100:.2f}%  HV Est: {metrics['realized_move_est_pct']*100:.2f}%  Ratio: {ratio:.2f}")

        # ENHANCED: Dynamic condor selection
        condor_tickers = pick_condor_wings_dynamic(
            tickers, float(spot), iv_rank_252, skew_25,
            allow_stale=(ALLOW_STALE_QUOTES_OUTSIDE_RTH and not rth)
        )
        
        if condor_tickers:
            analysis = analyze_condor_enhanced(
                condor_tickers, symbol, exp, tenor_target,
                float(spot), t_years, atm_iv, iv_rank_252, skew_25,
                earnings_date, today_local, scan_ts_utc
            )
            condor_analyses.append(analysis)
            print_condor_analysis(analysis)
        else:
            print(f"  Condor: SKIP (could not find suitable wings)")

        # Store metrics row
        metrics_rows.append(ExpiryMetricsRow(
            scan_ts_utc=scan_ts_utc, scan_date=scan_date, symbol=symbol, spot=float(spot),
            tenor_target=int(tenor_target), expiration=str(exp), dte=int(dte), t_years=float(t_years),
            hv20=hv20_val, hv60=hv60_val, hv120=hv120_val,
            hv_ref=hv_ref, w20=w20_used, w60=w60_used, w120=w120_used,
            atm_iv=atm_iv, skew_25=skew_25, tail_skew_10=metrics.get("tail_skew_10"),
            curvature_25=metrics.get("curvature_25"),
            straddle_mid=metrics.get("straddle_mid"),
            implied_move_pct=metrics.get("implied_move_pct"),
            realized_move_est_pct=metrics.get("realized_move_est_pct"),
            skew_40=metrics.get("skew_40"),
            iv_rank_252=iv_rank_252, iv_pctile_252=iv_pctile_252,
            term_structure_ratio=term_ratio, vol_regime=vol_regime,
            days_to_earnings=days_to_earn,
            calendar_spread_signal=False,  # Updated below
        ))

    # Calendar spread detection (compare near vs far expiries)
    if CALENDAR_SPREAD_ENABLED and len(tickers_by_exp) >= 2:
        exp_list = sorted(tickers_by_exp.keys())
        if len(exp_list) >= 2:
            signals = detect_calendar_spread_opportunities(
                tickers_by_exp[exp_list[0]], tickers_by_exp[exp_list[-1]],
                symbol, float(spot), exp_list[0], exp_list[-1]
            )
            calendar_signals.extend(signals)
            if signals:
                print_calendar_spread_signals(signals)

    return bucket_rows, metrics_rows, condor_analyses, calendar_signals


# =========================
# MAIN
# =========================

def main():
    """Main entry point for the enhanced options scanner."""
    logging.basicConfig(level=logging.WARNING)

    print("\n" + "=" * 100)
    print("OPTIONS SCANNER v8 - ENHANCED TRADING ALGORITHMS")
    print("=" * 100)
    print("\nNew features in v8:")
    print("  ✓ Dynamic short strike selection based on IV rank")
    print("  ✓ Asymmetric condors based on skew")
    print("  ✓ Vega-weighted scoring")
    print("  ✓ Fat-tail adjusted POP estimates")
    print("  ✓ Expected value calculation")
    print("  ✓ Position sizing suggestions")
    print("  ✓ Earnings awareness")
    print("  ✓ Calendar spread detection")
    print("  ✓ Volatility regime detection")
    print("=" * 100)

    rth_now = is_rth_now()
    if not rth_now:
        print("\n" + "!" * 100)
        print("!!! WARNING: OUTSIDE REGULAR TRADING HOURS (RTH) — ANALYSIS ONLY !!!")
        print("!" * 100 + "\n")

    if SUPPRESS_EXPECTED_IB_WARNINGS:
        class SuppressIBNoise(logging.Filter):
            def filter(self, record: logging.LogRecord) -> bool:
                msg = record.getMessage()
                return "No security definition" not in msg and not msg.startswith("Unknown contract:")
        logging.getLogger("ib_insync.wrapper").addFilter(SuppressIBNoise())
        logging.getLogger("ib_insync.ib").addFilter(SuppressIBNoise())

    ensure_output_dir()

    conn: Optional[sqlite3.Connection] = None
    ib: Optional[IB] = None

    all_bucket_rows: List[BucketRow] = []
    all_metrics_rows: List[ExpiryMetricsRow] = []
    all_condor_analyses: List[CondorAnalysis] = []
    all_calendar_signals: List[CalendarSpreadSignal] = []

    try:
        if WRITE_SQLITE:
            conn = sqlite3.connect(SQLITE_PATH)
            init_db(conn)

        ib = connect_ib()
        
        # Get earnings dates for all symbols
        print("\nFetching earnings dates...")
        earnings_dates = get_earnings_dates_cached(ib, SYMBOLS)
        for sym, edate in earnings_dates.items():
            if edate:
                print(f"  {sym}: {edate}")
            else:
                print(f"  {sym}: Not available")

        for sym in SYMBOLS:
            b, m, c, cal = scan_symbol_enhanced(ib, conn, sym, earnings_dates)
            all_bucket_rows.extend(b)
            all_metrics_rows.extend(m)
            all_condor_analyses.extend(c)
            all_calendar_signals.extend(cal)

    finally:
        if ib is not None:
            try:
                ib.disconnect()
            except Exception:
                pass

        if conn is not None:
            try:
                store_bucket_rows(conn, all_bucket_rows)
                store_expiry_metrics(conn, all_metrics_rows)
            finally:
                conn.close()

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    if WRITE_CSV:
        p1 = write_csv(all_bucket_rows, f"options_buckets_{ts}.csv")
        p2 = write_csv(all_metrics_rows, f"options_expiry_metrics_{ts}.csv")
        print("\nSaved CSVs:")
        if p1: print(" -", p1)
        if p2: print(" -", p2)

    if WRITE_SQLITE:
        print(f"\nSQLite DB updated: {SQLITE_PATH}")

    # Print summaries
    print_top_rich_cheap(all_bucket_rows, top_n=10)
    print_top_by_symbol_tenor(all_bucket_rows, top_n=3)
    
    # Condor summary
    valid_condors = [c for c in all_condor_analyses if c.skip_reason is None]
    if valid_condors:
        print("\n" + "=" * 100)
        print("CONDOR SUMMARY - TRADEABLE CANDIDATES")
        print("=" * 100)
        
        # Sort by EV
        by_ev = sorted([c for c in valid_condors if c.expected_value], 
                       key=lambda x: x.expected_value or 0, reverse=True)
        
        print("\nTop Condors by Expected Value:")
        for c in by_ev[:5]:
            ev_str = f"+${c.expected_value:.0f}" if c.expected_value >= 0 else f"-${abs(c.expected_value):.0f}"
            earn_flag = ""
            if c.earnings_risk_tier == "WARN":
                earn_flag = f" ⚠️ EARN:{c.days_to_earnings}d"
            elif c.earnings_risk_tier == "NOTE":
                earn_flag = f" 📝{c.days_to_earnings}d"
            print(f"  {c.symbol} {c.expiration} {c.short_delta_type}: "
                  f"EV={ev_str} POP={_fmt_pct(c.pop_fat_tail_adjusted)} "
                  f"Credit={_fmt_price(c.suggested_limit)} Qty={c.suggested_quantity}{earn_flag}")
        
        # Show skipped condors due to earnings
        skipped_earnings = [c for c in all_condor_analyses 
                          if c.skip_reason and "earnings" in c.skip_reason.lower()]
        if skipped_earnings:
            print(f"\n  ({len(skipped_earnings)} condor(s) skipped due to imminent earnings)")
    
    print_10197_summary()
    
    print("\n" + "=" * 100)
    print("SCAN COMPLETE")
    print("=" * 100)


if __name__ == "__main__":
    main()
