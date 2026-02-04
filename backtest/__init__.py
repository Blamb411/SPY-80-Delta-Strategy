"""
Options Backtesting System
==========================
Backtests put credit spreads and iron condors using historical IBKR data
with Black-Scholes theoretical pricing.
"""

from .black_scholes import (
    OptionPrice,
    black_scholes_price,
    black_scholes_greeks,
    find_strike_for_delta,
    round_strike_to_standard,
    calculate_spread_price,
    calculate_condor_price,
    estimate_pop_lognormal,
    estimate_prob_above,
    calculate_sma,
    calculate_rsi,
    calculate_hv,
    calculate_iv_rank,
)

from .ibkr_data_fetcher import (
    DailyBar,
    IVDataPoint,
    SymbolData,
    IBKRDataFetcher,
    load_symbols_from_csv,
)

__all__ = [
    # Black-Scholes
    'OptionPrice',
    'black_scholes_price',
    'black_scholes_greeks',
    'find_strike_for_delta',
    'round_strike_to_standard',
    'calculate_spread_price',
    'calculate_condor_price',
    'estimate_pop_lognormal',
    'estimate_prob_above',
    'calculate_sma',
    'calculate_rsi',
    'calculate_hv',
    'calculate_iv_rank',
    # Data fetcher
    'DailyBar',
    'IVDataPoint',
    'SymbolData',
    'IBKRDataFetcher',
    'load_symbols_from_csv',
]
