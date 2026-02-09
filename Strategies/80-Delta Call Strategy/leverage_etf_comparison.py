"""
Leveraged ETF Comparison Test
=============================
Compares the 80-delta call strategy against simpler leveraged ETF alternatives:
- SSO (2x leveraged SPY) with SMA200 filter
- UPRO (3x leveraged SPY) with SMA200 filter

The question: Is the options strategy worth the complexity compared to just
holding leveraged ETFs with the same trend filter?

Usage:
    python leverage_etf_comparison.py
"""

import os
import sys
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import yfinance as yf

# Add project root to path
_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_dir = os.path.dirname(os.path.dirname(_this_dir))
sys.path.insert(0, _project_dir)

from backtest.metrics import calculate_all_metrics, PerformanceMetrics
from backtest.strategy_config import StrategyConfig

# Configuration
CONFIG = StrategyConfig()
START_DATE = "2010-07-01"  # UPRO inception was June 2009
END_DATE = "2026-01-31"
SIM_START = "2011-01-01"  # Allow SMA200 warmup

INITIAL_CAPITAL = 700_000  # Match our reference portfolio ($600k shares + $100k options)


def fetch_data(ticker: str, start: str, end: str) -> pd.DataFrame:
    """Fetch daily data from Yahoo Finance."""
    print(f"  Fetching {ticker}...")
    df = yf.download(ticker, start=start, end=end, progress=False)
    df = df.reset_index()
    # Handle multi-index columns from yfinance
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0] if col[1] == '' else col[0] for col in df.columns]
    # Flatten any remaining issues
    df.columns = [str(c).replace(f"('{ticker}', '')", "").strip("(),' ") for c in df.columns]
    if "Date" not in df.columns and "date" in df.columns:
        df = df.rename(columns={"date": "Date"})
    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
    # Ensure Close is a scalar column
    if "Close" in df.columns:
        df["Close"] = df["Close"].astype(float)
    return df


def calculate_sma(prices: pd.Series, period: int = 200) -> pd.Series:
    """Calculate simple moving average."""
    return prices.rolling(window=period).mean()


def run_trend_filtered_strategy(
    df: pd.DataFrame,
    spy_sma: pd.Series,
    spy_close: pd.Series,
    initial_capital: float,
    exit_threshold: float = 0.02,
    sim_start: str = SIM_START,
) -> Tuple[List[float], List[str]]:
    """
    Run a trend-filtered strategy: invest when SPY > SMA200, cash otherwise.

    Args:
        df: DataFrame with Date, Close columns for the instrument
        spy_sma: Series of SPY SMA200 values indexed by date
        spy_close: Series of SPY close prices indexed by date
        initial_capital: Starting capital
        exit_threshold: Exit when this % below SMA (default 2%)
        sim_start: Start date for simulation

    Returns:
        Tuple of (daily_values, dates)
    """
    daily_values = []
    dates = []

    capital = initial_capital
    shares_held = 0
    in_position = False

    for _, row in df.iterrows():
        date = row["Date"]
        if date < sim_start:
            continue

        price = row["Close"]
        sma_val = spy_sma.get(date)
        spy_price = spy_close.get(date)

        if pd.isna(sma_val) or pd.isna(spy_price):
            continue

        # Calculate position based on SPY vs SMA (not the leveraged ETF)
        pct_below = (sma_val - spy_price) / sma_val if sma_val > 0 else 0

        # Entry/exit logic
        if not in_position and spy_price > sma_val:
            # Enter position
            shares_held = capital / price
            capital = 0
            in_position = True
        elif in_position and pct_below >= exit_threshold:
            # Exit position
            capital = shares_held * price
            shares_held = 0
            in_position = False

        # Calculate portfolio value
        portfolio_value = capital + shares_held * price
        daily_values.append(portfolio_value)
        dates.append(date)

    return daily_values, dates


def run_buy_and_hold(
    df: pd.DataFrame,
    initial_capital: float,
    sim_start: str = SIM_START,
) -> Tuple[List[float], List[str]]:
    """
    Run simple buy-and-hold strategy.

    Args:
        df: DataFrame with Date, Close columns
        initial_capital: Starting capital
        sim_start: Start date for simulation

    Returns:
        Tuple of (daily_values, dates)
    """
    daily_values = []
    dates = []

    first_price = None

    for _, row in df.iterrows():
        date = row["Date"]
        if date < sim_start:
            continue

        price = row["Close"]

        if first_price is None:
            first_price = price

        # Calculate value as if we bought at first price
        portfolio_value = initial_capital * (price / first_price)
        daily_values.append(portfolio_value)
        dates.append(date)

    return daily_values, dates


def run_options_strategy_proxy(
    spy_df: pd.DataFrame,
    spy_sma: pd.Series,
    initial_capital: float,
    shares: int = 1000,
    options_allocation: float = 100_000,
    leverage_factor: float = 1.5,  # Approximate effective leverage of options
    exit_threshold: float = 0.02,
    sim_start: str = SIM_START,
) -> Tuple[List[float], List[str]]:
    """
    Proxy for the 80-delta options strategy.

    Since we don't have ThetaData here, we approximate the options component
    as a leveraged SPY position with the same trend filter.

    The leverage factor represents the average effective leverage of the
    options allocation (typically 1.5-2x for 80-delta calls with delta cap).

    Args:
        spy_df: SPY DataFrame
        spy_sma: SPY SMA200 series
        initial_capital: Total capital (shares + options)
        shares: Number of SPY shares held
        options_allocation: Cash for options
        leverage_factor: Effective leverage of options component
        exit_threshold: SMA exit threshold
        sim_start: Start date

    Returns:
        Tuple of (daily_values, dates)
    """
    daily_values = []
    dates = []

    spy_by_date = {row["Date"]: row["Close"] for _, row in spy_df.iterrows()}

    # Track components separately
    share_value = None
    options_cash = options_allocation
    options_shares = 0  # Synthetic shares representing options exposure
    in_options = False

    for date in sorted(spy_by_date.keys()):
        if date < sim_start:
            continue

        spy_price = spy_by_date[date]
        sma_val = spy_sma.get(date)

        if pd.isna(sma_val):
            continue

        # Initialize share value
        if share_value is None:
            first_price = spy_price
            share_value = shares * first_price

        # Update share value (always holding shares)
        share_value = shares * spy_price

        # Options component logic
        pct_below = (sma_val - spy_price) / sma_val if sma_val > 0 else 0

        if not in_options and spy_price > sma_val:
            # Enter options position
            # Options provide leveraged exposure
            options_shares = (options_cash * leverage_factor) / spy_price
            options_cash = 0
            in_options = True
        elif in_options and pct_below >= exit_threshold:
            # Exit options position
            options_cash = options_shares * spy_price / leverage_factor
            options_shares = 0
            in_options = False

        # Calculate total portfolio value
        options_value = options_shares * spy_price if in_options else options_cash
        portfolio_value = share_value + options_value
        daily_values.append(portfolio_value)
        dates.append(date)

    return daily_values, dates


def main():
    print("=" * 80)
    print("LEVERAGED ETF COMPARISON TEST")
    print("=" * 80)
    print()
    print("Comparing 80-delta call strategy against leveraged ETF alternatives:")
    print("  - SSO (2x leveraged SPY) with SMA200 filter")
    print("  - UPRO (3x leveraged SPY) with SMA200 filter")
    print()
    print("All strategies use the same trend filter: invest when SPY > SMA200,")
    print("exit to cash when SPY is 2% below SMA200.")
    print()

    # Fetch data
    print("Fetching data...")
    spy_df = fetch_data("SPY", START_DATE, END_DATE)
    sso_df = fetch_data("SSO", START_DATE, END_DATE)
    upro_df = fetch_data("UPRO", START_DATE, END_DATE)

    # Create SPY lookup and SMA
    spy_by_date = {}
    for _, row in spy_df.iterrows():
        date_val = row["Date"]
        close_val = row["Close"]
        # Handle potential Series/scalar issues
        if hasattr(date_val, 'iloc'):
            date_val = date_val.iloc[0]
        if hasattr(close_val, 'iloc'):
            close_val = close_val.iloc[0]
        spy_by_date[str(date_val)] = float(close_val)
    spy_close = pd.Series(spy_by_date)
    spy_sma = calculate_sma(spy_close, 200)

    print(f"\nData range: {spy_df['Date'].min()} to {spy_df['Date'].max()}")
    print(f"Simulation starts: {SIM_START}")
    print(f"Initial capital: ${INITIAL_CAPITAL:,}")
    print()

    # Run strategies
    print("Running strategies...")

    # 1. SPY Buy & Hold
    spy_bh_values, spy_bh_dates = run_buy_and_hold(spy_df, INITIAL_CAPITAL)
    spy_bh_metrics = calculate_all_metrics(spy_bh_values, spy_bh_dates)

    # 2. SSO Buy & Hold
    sso_bh_values, sso_bh_dates = run_buy_and_hold(sso_df, INITIAL_CAPITAL)
    sso_bh_metrics = calculate_all_metrics(sso_bh_values, sso_bh_dates)

    # 3. UPRO Buy & Hold
    upro_bh_values, upro_bh_dates = run_buy_and_hold(upro_df, INITIAL_CAPITAL)
    upro_bh_metrics = calculate_all_metrics(upro_bh_values, upro_bh_dates)

    # 4. SSO with SMA filter
    sso_filtered_values, sso_filtered_dates = run_trend_filtered_strategy(
        sso_df, spy_sma, spy_close, INITIAL_CAPITAL
    )
    sso_filtered_metrics = calculate_all_metrics(sso_filtered_values, sso_filtered_dates)

    # 5. UPRO with SMA filter
    upro_filtered_values, upro_filtered_dates = run_trend_filtered_strategy(
        upro_df, spy_sma, spy_close, INITIAL_CAPITAL
    )
    upro_filtered_metrics = calculate_all_metrics(upro_filtered_values, upro_filtered_dates)

    # 6. Options strategy proxy (1000 shares + $100k options with ~1.5x leverage)
    options_values, options_dates = run_options_strategy_proxy(
        spy_df, spy_sma, INITIAL_CAPITAL,
        shares=1000, options_allocation=100_000, leverage_factor=1.5
    )
    options_metrics = calculate_all_metrics(options_values, options_dates)

    # Print results
    print("\n" + "=" * 100)
    print("COMPARISON RESULTS")
    print("=" * 100)
    print(f"\nPeriod: {SIM_START} to {spy_df['Date'].max()}")
    print(f"Initial Capital: ${INITIAL_CAPITAL:,}")
    print()

    print("-" * 100)
    print("BUY & HOLD (No SMA Filter)")
    print("-" * 100)
    print(f"{'Strategy':<25} {'CAGR':>10} {'Sharpe':>10} {'Sortino':>10} {'Max DD':>10} {'End Value':>15}")
    print(f"{'-'*25} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*15}")
    print(f"{'SPY B&H':<25} {spy_bh_metrics.cagr:>+9.1%} {spy_bh_metrics.sharpe_ratio:>10.3f} "
          f"{spy_bh_metrics.sortino_ratio:>10.3f} {spy_bh_metrics.max_drawdown:>9.1%} "
          f"${spy_bh_values[-1]:>14,.0f}")
    print(f"{'SSO (2x) B&H':<25} {sso_bh_metrics.cagr:>+9.1%} {sso_bh_metrics.sharpe_ratio:>10.3f} "
          f"{sso_bh_metrics.sortino_ratio:>10.3f} {sso_bh_metrics.max_drawdown:>9.1%} "
          f"${sso_bh_values[-1]:>14,.0f}")
    print(f"{'UPRO (3x) B&H':<25} {upro_bh_metrics.cagr:>+9.1%} {upro_bh_metrics.sharpe_ratio:>10.3f} "
          f"{upro_bh_metrics.sortino_ratio:>10.3f} {upro_bh_metrics.max_drawdown:>9.1%} "
          f"${upro_bh_values[-1]:>14,.0f}")

    print()
    print("-" * 100)
    print("WITH SMA200 FILTER (Exit when 2% below SMA)")
    print("-" * 100)
    print(f"{'Strategy':<25} {'CAGR':>10} {'Sharpe':>10} {'Sortino':>10} {'Max DD':>10} {'End Value':>15}")
    print(f"{'-'*25} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*15}")
    print(f"{'SSO (2x) + SMA':<25} {sso_filtered_metrics.cagr:>+9.1%} {sso_filtered_metrics.sharpe_ratio:>10.3f} "
          f"{sso_filtered_metrics.sortino_ratio:>10.3f} {sso_filtered_metrics.max_drawdown:>9.1%} "
          f"${sso_filtered_values[-1]:>14,.0f}")
    print(f"{'UPRO (3x) + SMA':<25} {upro_filtered_metrics.cagr:>+9.1%} {upro_filtered_metrics.sharpe_ratio:>10.3f} "
          f"{upro_filtered_metrics.sortino_ratio:>10.3f} {upro_filtered_metrics.max_drawdown:>9.1%} "
          f"${upro_filtered_values[-1]:>14,.0f}")
    print(f"{'80-Delta Options*':<25} {options_metrics.cagr:>+9.1%} {options_metrics.sharpe_ratio:>10.3f} "
          f"{options_metrics.sortino_ratio:>10.3f} {options_metrics.max_drawdown:>9.1%} "
          f"${options_values[-1]:>14,.0f}")

    print()
    print("* Options strategy approximated as 1000 SPY shares + $100k at 1.5x leverage with SMA filter")
    print()

    # Analysis
    print("=" * 100)
    print("ANALYSIS")
    print("=" * 100)
    print()

    print("1. SMA FILTER IMPACT:")
    print(f"   SSO:  B&H Max DD {sso_bh_metrics.max_drawdown:.1%} -> Filtered {sso_filtered_metrics.max_drawdown:.1%} "
          f"(improved by {abs(sso_bh_metrics.max_drawdown) - abs(sso_filtered_metrics.max_drawdown):.1%})")
    print(f"   UPRO: B&H Max DD {upro_bh_metrics.max_drawdown:.1%} -> Filtered {upro_filtered_metrics.max_drawdown:.1%} "
          f"(improved by {abs(upro_bh_metrics.max_drawdown) - abs(upro_filtered_metrics.max_drawdown):.1%})")
    print()

    print("2. SHARPE RATIO COMPARISON:")
    best_sharpe = max([
        ("SPY B&H", spy_bh_metrics.sharpe_ratio),
        ("SSO + SMA", sso_filtered_metrics.sharpe_ratio),
        ("UPRO + SMA", upro_filtered_metrics.sharpe_ratio),
        ("80-Delta Options", options_metrics.sharpe_ratio),
    ], key=lambda x: x[1])
    print(f"   Best Sharpe: {best_sharpe[0]} ({best_sharpe[1]:.3f})")
    print()

    print("3. RISK-ADJUSTED RETURNS:")
    print(f"   SSO + SMA provides {sso_filtered_metrics.cagr - spy_bh_metrics.cagr:+.1%} excess CAGR vs SPY B&H")
    print(f"   UPRO + SMA provides {upro_filtered_metrics.cagr - spy_bh_metrics.cagr:+.1%} excess CAGR vs SPY B&H")
    print(f"   Options provides {options_metrics.cagr - spy_bh_metrics.cagr:+.1%} excess CAGR vs SPY B&H")
    print()

    print("=" * 100)
    print("CONCLUSION")
    print("=" * 100)
    print()

    if sso_filtered_metrics.sharpe_ratio > options_metrics.sharpe_ratio:
        print("SSO (2x) with SMA filter has BETTER risk-adjusted returns than the options strategy.")
        print("Consider whether the simplicity of SSO justifies using it instead of options.")
    elif upro_filtered_metrics.sharpe_ratio > options_metrics.sharpe_ratio:
        print("UPRO (3x) with SMA filter has BETTER risk-adjusted returns than the options strategy.")
        print("However, UPRO has much higher drawdown risk during crashes.")
    else:
        print("The 80-delta options strategy provides the best risk-adjusted returns.")
        print("The added complexity of options trading is justified by better Sharpe ratio.")
    print()

    print("KEY CONSIDERATIONS:")
    print("  - Leveraged ETFs suffer from volatility decay in choppy markets")
    print("  - Options provide more precise control over leverage and exposure")
    print("  - Leveraged ETFs are simpler to execute (no options knowledge needed)")
    print("  - Tax treatment may differ between options and ETF gains")
    print()

    # Export results
    results_df = pd.DataFrame({
        "Strategy": ["SPY B&H", "SSO B&H", "UPRO B&H", "SSO + SMA", "UPRO + SMA", "80-Delta Options"],
        "CAGR": [spy_bh_metrics.cagr, sso_bh_metrics.cagr, upro_bh_metrics.cagr,
                 sso_filtered_metrics.cagr, upro_filtered_metrics.cagr, options_metrics.cagr],
        "Sharpe": [spy_bh_metrics.sharpe_ratio, sso_bh_metrics.sharpe_ratio, upro_bh_metrics.sharpe_ratio,
                   sso_filtered_metrics.sharpe_ratio, upro_filtered_metrics.sharpe_ratio, options_metrics.sharpe_ratio],
        "Sortino": [spy_bh_metrics.sortino_ratio, sso_bh_metrics.sortino_ratio, upro_bh_metrics.sortino_ratio,
                    sso_filtered_metrics.sortino_ratio, upro_filtered_metrics.sortino_ratio, options_metrics.sortino_ratio],
        "Max_DD": [spy_bh_metrics.max_drawdown, sso_bh_metrics.max_drawdown, upro_bh_metrics.max_drawdown,
                   sso_filtered_metrics.max_drawdown, upro_filtered_metrics.max_drawdown, options_metrics.max_drawdown],
        "End_Value": [spy_bh_values[-1], sso_bh_values[-1], upro_bh_values[-1],
                      sso_filtered_values[-1], upro_filtered_values[-1], options_values[-1]],
    })

    output_file = os.path.join(_this_dir, "leverage_etf_comparison.csv")
    results_df.to_csv(output_file, index=False)
    print(f"Results exported to: {output_file}")


if __name__ == "__main__":
    main()
