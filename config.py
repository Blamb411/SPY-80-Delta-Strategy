"""
Shared Configuration for SPY 80-Delta Call Strategy
====================================================
Centralizes all hardcoded constants used across scripts.
"""

# ============================================================================
# PORTFOLIO
# ============================================================================
SHARES = 3125                  # Number of SPY shares held (IRA)
OPTIONS_CASH_ALLOCATION = 100_000  # Starting cash for options

# ============================================================================
# STRATEGY - LONG CALLS (above SMA)
# ============================================================================
DELTA = 0.80                   # Target delta for long calls
DTE_TARGET = 120               # Calendar days to expiration
DTE_MIN = 90
DTE_MAX = 150
MAX_HOLD_DAYS = 60             # Max hold in trading days
PROFIT_TARGET = 0.50           # +50% profit target
SMA_EXIT_THRESHOLD = 0.02     # Force-exit when 2% below SMA

# ============================================================================
# STRATEGY - COVERED CALLS (below SMA)
# ============================================================================
CC_DELTA = 0.25                # Target delta for covered calls (OTM)
CC_DTE_TARGET = 45             # Shorter duration for covered calls
CC_DTE_MIN = 30
CC_DTE_MAX = 60
CC_PROFIT_TARGET = 0.50        # Buy back at 50% profit
CC_MAX_CONTRACTS = 31          # Max covered calls (3125 / 100)

# ============================================================================
# BLACK-SCHOLES DEFAULTS
# ============================================================================
RISK_FREE_RATE = 0.045         # Risk-free rate for B-S pricing
DEFAULT_IV = 0.16              # Default IV when unknown (SPY typical)
MIN_IV = 0.08                  # Floor for estimated IV
MAX_IV = 0.90                  # Ceiling for estimated IV

# ============================================================================
# IBKR CONNECTION
# ============================================================================
IB_HOST = "127.0.0.1"
IB_PORT = 7497                 # 7497 for TWS paper, 7496 for TWS live
IB_CLIENT_IDS = {
    "monitor": 97,
    "alerts": 96,
    "portfolio": 95,
}

# ============================================================================
# ALERTS
# ============================================================================
ALERT_PROFIT_TARGET = 0.50     # +50% - SELL
ALERT_APPROACHING_TARGET = 0.40  # +40% - Getting close
ALERT_STRONG_GAIN = 0.30       # +30% - Consider partial
ALERT_SIGNIFICANT_LOSS = -0.20  # -20% - Review
ALERT_MAX_HOLD_WARNING_DAYS = 5  # Days before max hold to warn

# ============================================================================
# MONITORING
# ============================================================================
CHECK_INTERVAL_SEC = 300       # 5 minutes
MARKET_OPEN_HOUR = 9
MARKET_OPEN_MIN = 30
MARKET_CLOSE_HOUR = 16
MARKET_CLOSE_MIN = 0

# ============================================================================
# BACKTEST DATA RANGES
# ============================================================================
# ThetaData backtest (real options data)
THETA_DATA_START = "2014-01-01"
THETA_DATA_END = "2026-01-31"
THETA_SIM_START = "2015-03-01"

# Synthetic backtest (Black-Scholes w/ VIX)
SYNTH_DATA_START = "2004-01-01"
SYNTH_DATA_END = "2014-12-31"
SYNTH_SIM_START = "2005-01-01"

# ============================================================================
# SCREENING
# ============================================================================
SCREEN_MIN_OI = 500
SCREEN_MAX_SPREAD_PCT = 0.02
SCREEN_MIN_5Y_RETURN = 0.0
SCREEN_MIN_SHARPE = 0.5
SCREEN_MAX_VOLATILITY = 0.25

# ============================================================================
# FALLBACK VALUES
# ============================================================================
FALLBACK_SPY_PRICE = 600.0     # Conservative fallback (updated periodically)
FALLBACK_VIX = 20.0
