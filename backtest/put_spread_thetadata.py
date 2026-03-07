#!/usr/bin/env python3
"""
DEPRECATED — This file is a compatibility shim.

The canonical PCS backtester now lives in the put-credit-spreads repo:
    C:/Users/Admin/Trading/repos/put-credit-spreads/

All imports are re-exported from there. This shim exists so that
existing scripts (run_flat_delta_comparison.py, portfolio_simulation.py,
etc.) continue to work without modification.
"""

import os
import sys
import warnings

warnings.warn(
    "backtest.put_spread_thetadata is deprecated. "
    "Use the put-credit-spreads repo directly.",
    DeprecationWarning,
    stacklevel=2,
)

# Add put-credit-spreads to sys.path
_pcs_repo = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "..", "..", "put-credit-spreads")
_pcs_repo = os.path.normpath(_pcs_repo)
if _pcs_repo not in sys.path:
    sys.path.insert(0, _pcs_repo)

from strategy import (  # noqa: E402,F401
    # Config constants
    DEFAULT_IV_RANK_LOW, IV_RANK_MED, IV_RANK_HIGH, DELTA_BY_IV_TIER,
    DEFAULT_WING_WIDTH_PCT, MIN_WING_WIDTH, MAX_CREDIT_RATIO,
    ENTRY_INTERVAL, DTE_TARGET, DTE_MIN, DTE_MAX, RISK_FREE_RATE,
    TAKE_PROFIT_PCT, SYNTHETIC_SPREAD_PCT,
    DEFAULT_STOP_LOSS_MULT, DEFAULT_SMA_PERIOD, DEFAULT_MIN_OPEN_INTEREST,
    DEFAULT_MIN_CW_RATIO, DEFAULT_WING_SIGMA, DEFAULT_IV_RANK_HIGH,
    DEFAULT_FLAT_DELTA,
    THETADATA_START, VIX_BUCKETS,
    # Filters
    compute_vix_iv_rank, select_delta_tier, check_sma_filter,
    # Construction
    find_short_put_strike, build_spread_strikes, build_strikes_with_wing,
    validate_and_snap_strikes, check_open_interest,
    # Pricing
    price_spread_entry_thetadata, price_spread_entry_synthetic,
    price_spread_on_date_thetadata, price_spread_on_date_synthetic,
    intrinsic_settlement_put_spread,
    # Simulation
    simulate_spread_trade,
    # Backtest
    run_backtest,
    # Metrics
    compute_risk_metrics,
    # Reporting
    print_results, export_csv, get_vix_bucket,
)
