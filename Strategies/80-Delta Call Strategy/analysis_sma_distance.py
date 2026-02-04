"""
SMA Distance Segmentation Analysis
==================================
Analyzes 80-delta call strategy performance segmented by distance from SMA200.

Two types of segmentation:
1. Percentage Bands - How far above SMA in percentage terms
2. Standard Deviation Bands - How far above SMA in volatility-adjusted terms

Usage:
    python analysis_sma_distance.py [TICKER]

    TICKER: SPY (default), QQQ, or RSP

Reads trade data from a saved backtest run or runs a fresh backtest.
"""

import os
import sys
import json
import argparse
import numpy as np
import pandas as pd
from collections import defaultdict

_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_dir = os.path.dirname(os.path.dirname(_this_dir))
sys.path.insert(0, _project_dir)

from backtest.thetadata_client import ThetaDataClient


# ======================================================================
# BAND DEFINITIONS
# ======================================================================

# Percentage bands (% above SMA200)
PCT_BANDS = [
    ("0-0.5%", 0.0, 0.005),
    ("0.5-1%", 0.005, 0.01),
    ("1-1.5%", 0.01, 0.015),
    ("1.5-2%", 0.015, 0.02),
    (">2%", 0.02, float('inf')),
]

# Standard deviation bands (# of daily std devs above SMA)
STD_BANDS = [
    ("0-0.5 SD", 0.0, 0.5),
    ("0.5-1 SD", 0.5, 1.0),
    ("1-1.5 SD", 1.0, 1.5),
    ("1.5-2 SD", 1.5, 2.0),
    (">2 SD", 2.0, float('inf')),
]


# ======================================================================
# ANALYSIS FUNCTIONS
# ======================================================================

def assign_pct_band(pct_above_sma):
    """Assign a trade to a percentage band based on entry % above SMA."""
    for band_name, low, high in PCT_BANDS:
        if low <= pct_above_sma < high:
            return band_name
    return PCT_BANDS[-1][0]  # Default to highest band


def assign_std_band(std_above_sma):
    """Assign a trade to a std dev band based on entry std devs above SMA."""
    for band_name, low, high in STD_BANDS:
        if low <= std_above_sma < high:
            return band_name
    return STD_BANDS[-1][0]  # Default to highest band


def compute_band_stats(trades_df, band_col):
    """
    Compute statistics for each band.

    Returns dict of band_name -> stats dict
    """
    results = {}

    for band_name in trades_df[band_col].unique():
        band_trades = trades_df[trades_df[band_col] == band_name]
        n = len(band_trades)

        if n == 0:
            continue

        wins = band_trades[band_trades["pnl_pct"] > 0]

        stats = {
            "n_trades": n,
            "win_rate": len(wins) / n if n > 0 else 0,
            "mean_return": band_trades["pnl_pct"].mean(),
            "median_return": band_trades["pnl_pct"].median(),
            "std_return": band_trades["pnl_pct"].std(),
            "total_pnl": band_trades["pnl_dollar"].sum(),
            "avg_pnl": band_trades["pnl_dollar"].mean(),
            "max_return": band_trades["pnl_pct"].max(),
            "min_return": band_trades["pnl_pct"].min(),
            "avg_days_held": band_trades["days_held"].mean(),
        }

        # Calculate Sharpe-like ratio if we have enough trades
        if n >= 10 and stats["std_return"] > 0:
            # Annualize: assume avg 40 trading days per trade cycle
            trades_per_year = 252 / stats["avg_days_held"] if stats["avg_days_held"] > 0 else 6
            annual_return = stats["mean_return"] * trades_per_year
            annual_vol = stats["std_return"] * np.sqrt(trades_per_year)
            stats["sharpe"] = annual_return / annual_vol if annual_vol > 0 else 0
        else:
            stats["sharpe"] = None

        # Exit reason breakdown
        stats["pt_exits"] = len(band_trades[band_trades["exit_reason"] == "PT"])
        stats["mh_exits"] = len(band_trades[band_trades["exit_reason"] == "MH"])
        stats["sma_exits"] = len(band_trades[band_trades["exit_reason"] == "SMA"])

        results[band_name] = stats

    return results


def print_band_analysis(band_stats, band_type, band_order):
    """Pretty print the band analysis results."""
    W = 90

    print(f"\n{'=' * W}")
    print(f"SEGMENTATION BY {band_type.upper()}")
    print(f"{'=' * W}")

    # Header
    print(f"\n  {'Band':<12} {'Trades':>8} {'Win%':>8} {'Mean':>10} {'Median':>10} "
          f"{'Total P&L':>12} {'Sharpe':>8}")
    print(f"  {'-' * 80}")

    # Data rows in order
    for band_name in band_order:
        if band_name not in band_stats:
            print(f"  {band_name:<12} {'--':>8} {'--':>8} {'--':>10} {'--':>10} "
                  f"{'--':>12} {'--':>8}")
            continue

        s = band_stats[band_name]
        sharpe_str = f"{s['sharpe']:.2f}" if s['sharpe'] is not None else "--"

        print(f"  {band_name:<12} {s['n_trades']:>8} {s['win_rate']:>7.1%} "
              f"{s['mean_return']:>+9.1%} {s['median_return']:>+9.1%} "
              f"${s['total_pnl']:>11,.0f} {sharpe_str:>8}")

    # Detailed breakdown
    print(f"\n  {'Band':<12} {'AvgDays':>8} {'MaxRet':>10} {'MinRet':>10} "
          f"{'PT%':>8} {'MH%':>8} {'SMA%':>8}")
    print(f"  {'-' * 68}")

    for band_name in band_order:
        if band_name not in band_stats:
            continue

        s = band_stats[band_name]
        n = s['n_trades']
        pt_pct = s['pt_exits'] / n if n > 0 else 0
        mh_pct = s['mh_exits'] / n if n > 0 else 0
        sma_pct = s['sma_exits'] / n if n > 0 else 0

        print(f"  {band_name:<12} {s['avg_days_held']:>8.1f} {s['max_return']:>+9.1%} "
              f"{s['min_return']:>+9.1%} {pt_pct:>7.0%} {mh_pct:>7.0%} {sma_pct:>7.0%}")


def load_backtest_module(ticker):
    """Dynamically load the appropriate backtest module for the ticker."""
    ticker = ticker.upper()
    if ticker == "SPY":
        from delta_capped_backtest import load_all_data, run_delta_capped_simulation
        return load_all_data, run_delta_capped_simulation, "SPY"
    elif ticker == "QQQ":
        from qqq_delta_capped_backtest import load_all_data, run_delta_capped_simulation
        return load_all_data, run_delta_capped_simulation, "QQQ"
    elif ticker == "RSP":
        from rsp_delta_capped_backtest import load_all_data, run_delta_capped_simulation
        return load_all_data, run_delta_capped_simulation, "RSP"
    else:
        raise ValueError(f"Unknown ticker: {ticker}. Supported: SPY, QQQ, RSP")


def run_analysis(ticker="SPY"):
    """Run the SMA distance segmentation analysis."""
    ticker = ticker.upper()

    print("=" * 80)
    print(f"SMA Distance Segmentation Analysis - {ticker}")
    print("=" * 80)

    # Load appropriate backtest module
    load_all_data, run_delta_capped_simulation, ticker_name = load_backtest_module(ticker)

    # Connect and load data
    client = ThetaDataClient()
    if not client.connect():
        print("\nERROR: Cannot connect to Theta Terminal.")
        return

    print("\nConnected to Theta Terminal.\n")

    # Load data
    data = load_all_data(client)
    if ticker == "RSP" and data[0] is None:
        print("\nFailed to load RSP data.")
        client.close()
        return

    bars_by_date, trading_dates, vol_data, sma200, monthly_exps, trailing_12m_returns, rolling_volatility = data

    # Run simulation (use thresh-exit, no covered calls for cleaner analysis)
    print("\nRunning backtest simulation...")
    snaps, trades, cc_trades = run_delta_capped_simulation(
        client, bars_by_date, trading_dates, vol_data, sma200, monthly_exps,
        trailing_12m_returns=trailing_12m_returns,
        rolling_volatility=rolling_volatility,
        force_exit_below_sma=True,
        sell_covered_calls=False,
        label=f"SMA Distance Analysis - {ticker}",
    )

    client.close()

    if not trades:
        print("\nNo trades to analyze.")
        return

    # Convert to DataFrame
    trades_df = pd.DataFrame(trades)

    print(f"\n{'=' * 80}")
    print(f"ANALYSIS SUMMARY")
    print(f"{'=' * 80}")
    print(f"  Total trades: {len(trades_df)}")
    print(f"  Date range: {trades_df['entry_date'].min()} to {trades_df['exit_date'].max()}")
    print(f"  Overall win rate: {(trades_df['pnl_pct'] > 0).mean():.1%}")
    print(f"  Overall mean return: {trades_df['pnl_pct'].mean():+.1%}")
    print(f"  Overall total P&L: ${trades_df['pnl_dollar'].sum():,.0f}")

    # Check if we have the analysis fields
    if "entry_pct_above_sma" not in trades_df.columns:
        print("\nWARNING: Trade log missing analysis fields. Re-run the backtest.")
        return

    # Show distribution of entry conditions
    print(f"\n  Entry conditions distribution:")
    print(f"    Mean % above SMA at entry: {trades_df['entry_pct_above_sma'].mean():.2%}")
    print(f"    Mean std devs above SMA: {trades_df['entry_std_above_sma'].mean():.2f}")
    print(f"    Mean trailing 12m return: {trades_df['entry_trailing_12m_return'].mean():.1%}")

    # Assign bands
    trades_df["pct_band"] = trades_df["entry_pct_above_sma"].apply(assign_pct_band)
    trades_df["std_band"] = trades_df["entry_std_above_sma"].apply(assign_std_band)

    # Percentage band analysis
    pct_stats = compute_band_stats(trades_df, "pct_band")
    pct_order = [b[0] for b in PCT_BANDS]
    print_band_analysis(pct_stats, "Percentage Above SMA200", pct_order)

    # Standard deviation band analysis
    std_stats = compute_band_stats(trades_df, "std_band")
    std_order = [b[0] for b in STD_BANDS]
    print_band_analysis(std_stats, "Standard Deviations Above SMA200", std_order)

    # Cross-tabulation analysis
    print(f"\n{'=' * 80}")
    print("CROSS-TABULATION: % Band vs Std Band")
    print(f"{'=' * 80}")

    cross_tab = pd.crosstab(trades_df["pct_band"], trades_df["std_band"])
    # Reorder
    cross_tab = cross_tab.reindex(index=pct_order, columns=std_order, fill_value=0)
    print(f"\n{cross_tab}")

    # Win rate cross-tab
    print(f"\n{'=' * 80}")
    print("WIN RATE BY CROSS-SECTION")
    print(f"{'=' * 80}")

    win_rates = trades_df.groupby(["pct_band", "std_band"]).apply(
        lambda x: (x["pnl_pct"] > 0).mean() if len(x) > 0 else np.nan
    ).unstack(fill_value=np.nan)
    win_rates = win_rates.reindex(index=pct_order, columns=std_order)

    print("\nWin rates (blank = no trades):")
    print(win_rates.round(2))

    # Key insights
    print(f"\n{'=' * 80}")
    print("KEY INSIGHTS")
    print(f"{'=' * 80}")

    # Find best and worst bands
    if pct_stats:
        best_pct = max(pct_stats.items(), key=lambda x: x[1]["mean_return"])
        worst_pct = min(pct_stats.items(), key=lambda x: x[1]["mean_return"])
        print(f"\n  Best % band: {best_pct[0]} (mean: {best_pct[1]['mean_return']:+.1%}, "
              f"win rate: {best_pct[1]['win_rate']:.0%})")
        print(f"  Worst % band: {worst_pct[0]} (mean: {worst_pct[1]['mean_return']:+.1%}, "
              f"win rate: {worst_pct[1]['win_rate']:.0%})")

    if std_stats:
        best_std = max(std_stats.items(), key=lambda x: x[1]["mean_return"])
        worst_std = min(std_stats.items(), key=lambda x: x[1]["mean_return"])
        print(f"\n  Best SD band: {best_std[0]} (mean: {best_std[1]['mean_return']:+.1%}, "
              f"win rate: {best_std[1]['win_rate']:.0%})")
        print(f"  Worst SD band: {worst_std[0]} (mean: {worst_std[1]['mean_return']:+.1%}, "
              f"win rate: {worst_std[1]['win_rate']:.0%})")

    # Save results to CSV
    output_file = os.path.join(_this_dir, f"analysis_sma_distance_{ticker.lower()}_results.csv")
    trades_df.to_csv(output_file, index=False)
    print(f"\n  Results saved to: {output_file}")

    print(f"\n{'=' * 80}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SMA Distance Segmentation Analysis")
    parser.add_argument("ticker", nargs="?", default="SPY",
                        help="Ticker to analyze: SPY (default), QQQ, or RSP")
    args = parser.parse_args()
    run_analysis(args.ticker)
