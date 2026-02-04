"""
Configuration and Parameters
==============================
All strategy parameters, tickers, date ranges, and constants for the
Massive API local options backtester.
"""

from datetime import date
import os

# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------
API_KEY_FILE = os.path.join(os.path.dirname(__file__), "api_key.txt")

def load_api_key() -> str:
    with open(API_KEY_FILE) as f:
        return f.read().strip()

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
DB_PATH = os.path.join(os.path.dirname(__file__), "backtest_cache.db")

# ---------------------------------------------------------------------------
# Universe
# ---------------------------------------------------------------------------
TICKERS = [
    "SPY", "QQQ", "IWM", "DIA",
    "XLF", "XLE", "XLK", "XLV",
    "GLD", "SLV", "TLT", "HYG",
]

# ---------------------------------------------------------------------------
# Backtest period
# ---------------------------------------------------------------------------
START_DATE = date(2020, 1, 1)
END_DATE = date(2025, 12, 31)

# Historical bid/ask quotes available from this date onward
QUOTES_AVAILABLE_DATE = date(2022, 3, 7)

# ---------------------------------------------------------------------------
# Strategy parameters
# ---------------------------------------------------------------------------
# Spread widths as fraction of underlying price
WIDTH_PCTS = [0.05, 0.10]            # 5% and 10%

# Take-profit thresholds (fraction of max profit = credit received)
TAKE_PROFIT_PCTS = [0.50, 0.75]

# Stop-loss thresholds (multiple of credit received)
STOP_LOSS_MULTS = [1.0, 2.0]         # 100% and 200% of credit

# Strategy types
STRATEGY_TYPES = ["PUT", "CALL"]

# DTE targeting
MIN_DTE = 25
MAX_DTE = 45
IDEAL_DTE = 35

# Delta targeting
TARGET_DELTA = 0.25                   # absolute value
FALLBACK_OTM_PCT = 0.05              # 5% OTM if delta calc fails

# IV Rank filter
IV_RANK_MIN = 0.25                    # 25th percentile minimum

# Entry spacing / concurrency
MIN_DAYS_BETWEEN_ENTRIES = 7          # per ticker
MAX_CONCURRENT_PER_TICKER = 3         # entry signals (each spawns 16 combos)

# ---------------------------------------------------------------------------
# Pricing
# ---------------------------------------------------------------------------
RISK_FREE_RATE = 0.04                 # approximate average over period
SYNTHETIC_SPREAD_PCT = 0.05           # 5% bid/ask spread for pre-2022 data

# ---------------------------------------------------------------------------
# HV / IV Rank
# ---------------------------------------------------------------------------
HV_PERIOD = 20                        # 20-day rolling HV
IV_RANK_LOOKBACK = 252                # 1-year trailing HV history

# ---------------------------------------------------------------------------
# API rate limiting
# ---------------------------------------------------------------------------
API_DELAY_SECONDS = 0.15              # delay between API calls

# ---------------------------------------------------------------------------
# Combo count (derived — do not change)
# ---------------------------------------------------------------------------
# 2 strategies x 2 widths x 2 TP x 2 SL = 16 combos per entry signal
COMBOS_PER_ENTRY = (
    len(STRATEGY_TYPES) * len(WIDTH_PCTS)
    * len(TAKE_PROFIT_PCTS) * len(STOP_LOSS_MULTS)
)
