"""
Trailing Return Correlation Analysis
====================================
Analyzes 80-delta call strategy performance segmented by trailing 12-month
returns at the time of trade entry.

Buckets:
- <0% (negative trailing year)
- 0-10%
- 10-20%
- 20-30%
- >30%

Also calculates correlation between trailing returns and trade P&L.

Usage:
    python analysis_trailing_returns.py [TICKER]

    TICKER: SPY (default), QQQ, or RSP
"""

import os
import sys
import argparse
import numpy as np
import pandas as pd
from scipy import stats

_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_dir = os.path.dirname(os.path.dirname(_this_dir))
sys.path.insert(0, _project_dir)

from backtest.thetadata_client import ThetaDataClient


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


# ======================================================================
# BUCKET DEFINITIONS
# ======================================================================

TRAILING_RETURN_BUCKETS = [
    ("<0%", -float('inf'), 0.0),
    ("0-10%", 0.0, 0.10),
    ("10-20%", 0.10, 0.20),
    ("20-30%", 0.20, 0.30),
    (">30%", 0.30, float('inf')),
]


# ======================================================================
# ANALYSIS FUNCTIONS
# ======================================================================

def assign_bucket(trailing_return):
    """Assign a trade to a bucket based on trailing 12-month return at entry."""
    for bucket_name, low, high in TRAILING_RETURN_BUCKETS:
        if low <= trailing_return < high:
            return bucket_name
    return TRAILING_RETURN_BUCKETS[-1][0]


def compute_bucket_stats(trades_df, bucket_col):
    """Compute statistics for each bucket."""
    results = {}

    for bucket_name in trades_df[bucket_col].unique():
        bucket_trades = trades_df[trades_df[bucket_col] == bucket_name]
        n = len(bucket_trades)

        if n == 0:
            continue

        wins = bucket_trades[bucket_trades["pnl_pct"] > 0]

        stats = {
            "n_trades": n,
            "win_rate": len(wins) / n if n > 0 else 0,
            "mean_return": bucket_trades["pnl_pct"].mean(),
            "median_return": bucket_trades["pnl_pct"].median(),
            "std_return": bucket_trades["pnl_pct"].std(),
            "total_pnl": bucket_trades["pnl_dollar"].sum(),
            "avg_pnl": bucket_trades["pnl_dollar"].mean(),
            "max_return": bucket_trades["pnl_pct"].max(),
            "min_return": bucket_trades["pnl_pct"].min(),
            "avg_days_held": bucket_trades["days_held"].mean(),
            "avg_trailing_return": bucket_trades["entry_trailing_12m_return"].mean(),
        }

        # Sharpe-like ratio
        if n >= 10 and stats["std_return"] > 0:
            trades_per_year = 252 / stats["avg_days_held"] if stats["avg_days_held"] > 0 else 6
            annual_return = stats["mean_return"] * trades_per_year
            annual_vol = stats["std_return"] * np.sqrt(trades_per_year)
            stats["sharpe"] = annual_return / annual_vol if annual_vol > 0 else 0
        else:
            stats["sharpe"] = None

        # Exit reason breakdown
        stats["pt_exits"] = len(bucket_trades[bucket_trades["exit_reason"] == "PT"])
        stats["mh_exits"] = len(bucket_trades[bucket_trades["exit_reason"] == "MH"])
        stats["sma_exits"] = len(bucket_trades[bucket_trades["exit_reason"] == "SMA"])

        results[bucket_name] = stats

    return results


def print_bucket_analysis(bucket_stats, bucket_order):
    """Pretty print the bucket analysis results."""
    W = 95

    print(f"\n{'=' * W}")
    print("SEGMENTATION BY TRAILING 12-MONTH RETURN AT ENTRY")
    print(f"{'=' * W}")

    # Header
    print(f"\n  {'Bucket':<12} {'Trades':>8} {'Win%':>8} {'Mean':>10} {'Median':>10} "
          f"{'Total P&L':>12} {'Sharpe':>8} {'AvgT12m':>10}")
    print(f"  {'-' * 88}")

    for bucket_name in bucket_order:
        if bucket_name not in bucket_stats:
            print(f"  {bucket_name:<12} {'--':>8}")
            continue

        s = bucket_stats[bucket_name]
        sharpe_str = f"{s['sharpe']:.2f}" if s['sharpe'] is not None else "--"

        print(f"  {bucket_name:<12} {s['n_trades']:>8} {s['win_rate']:>7.1%} "
              f"{s['mean_return']:>+9.1%} {s['median_return']:>+9.1%} "
              f"${s['total_pnl']:>11,.0f} {sharpe_str:>8} {s['avg_trailing_return']:>+9.1%}")

    # Detailed breakdown
    print(f"\n  {'Bucket':<12} {'AvgDays':>8} {'MaxRet':>10} {'MinRet':>10} "
          f"{'PT%':>8} {'MH%':>8} {'SMA%':>8}")
    print(f"  {'-' * 68}")

    for bucket_name in bucket_order:
        if bucket_name not in bucket_stats:
            continue

        s = bucket_stats[bucket_name]
        n = s['n_trades']
        pt_pct = s['pt_exits'] / n if n > 0 else 0
        mh_pct = s['mh_exits'] / n if n > 0 else 0
        sma_pct = s['sma_exits'] / n if n > 0 else 0

        print(f"  {bucket_name:<12} {s['avg_days_held']:>8.1f} {s['max_return']:>+9.1%} "
              f"{s['min_return']:>+9.1%} {pt_pct:>7.0%} {mh_pct:>7.0%} {sma_pct:>7.0%}")


def compute_correlation_stats(trades_df):
    """Compute correlation between trailing return and trade P&L."""
    x = trades_df["entry_trailing_12m_return"].values
    y = trades_df["pnl_pct"].values

    # Remove any NaN values
    mask = ~(np.isnan(x) | np.isnan(y))
    x = x[mask]
    y = y[mask]

    if len(x) < 5:
        return None

    # Pearson correlation
    pearson_r, pearson_p = stats.pearsonr(x, y)

    # Spearman correlation (rank-based, more robust)
    spearman_r, spearman_p = stats.spearmanr(x, y)

    # Linear regression
    slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)

    return {
        "pearson_r": pearson_r,
        "pearson_p": pearson_p,
        "spearman_r": spearman_r,
        "spearman_p": spearman_p,
        "slope": slope,
        "intercept": intercept,
        "r_squared": r_value ** 2,
        "std_err": std_err,
        "n_points": len(x),
    }


def run_analysis(ticker="SPY"):
    """Run the trailing return correlation analysis."""
    ticker = ticker.upper()

    print("=" * 80)
    print(f"Trailing Return Correlation Analysis - {ticker}")
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

    # Run simulation
    print("\nRunning backtest simulation...")
    snaps, trades, cc_trades = run_delta_capped_simulation(
        client, bars_by_date, trading_dates, vol_data, sma200, monthly_exps,
        trailing_12m_returns=trailing_12m_returns,
        rolling_volatility=rolling_volatility,
        force_exit_below_sma=True,
        sell_covered_calls=False,
        label=f"Trailing Return Analysis - {ticker}",
    )

    client.close()

    if not trades:
        print("\nNo trades to analyze.")
        return

    # Convert to DataFrame
    trades_df = pd.DataFrame(trades)

    print(f"\n{'=' * 80}")
    print("ANALYSIS SUMMARY")
    print(f"{'=' * 80}")
    print(f"  Total trades: {len(trades_df)}")
    print(f"  Date range: {trades_df['entry_date'].min()} to {trades_df['exit_date'].max()}")
    print(f"  Overall win rate: {(trades_df['pnl_pct'] > 0).mean():.1%}")
    print(f"  Overall mean return: {trades_df['pnl_pct'].mean():+.1%}")
    print(f"  Overall total P&L: ${trades_df['pnl_dollar'].sum():,.0f}")

    # Check if we have the analysis fields
    if "entry_trailing_12m_return" not in trades_df.columns:
        print("\nWARNING: Trade log missing analysis fields. Re-run the backtest.")
        return

    # Distribution of trailing returns at entry
    print(f"\n  Trailing 12m return at entry:")
    print(f"    Mean:   {trades_df['entry_trailing_12m_return'].mean():+.1%}")
    print(f"    Median: {trades_df['entry_trailing_12m_return'].median():+.1%}")
    print(f"    Min:    {trades_df['entry_trailing_12m_return'].min():+.1%}")
    print(f"    Max:    {trades_df['entry_trailing_12m_return'].max():+.1%}")

    # Assign buckets
    trades_df["trailing_bucket"] = trades_df["entry_trailing_12m_return"].apply(assign_bucket)

    # Bucket analysis
    bucket_stats = compute_bucket_stats(trades_df, "trailing_bucket")
    bucket_order = [b[0] for b in TRAILING_RETURN_BUCKETS]
    print_bucket_analysis(bucket_stats, bucket_order)

    # Correlation analysis
    print(f"\n{'=' * 80}")
    print("CORRELATION ANALYSIS")
    print(f"{'=' * 80}")

    corr_stats = compute_correlation_stats(trades_df)
    if corr_stats:
        print(f"\n  Trailing 12m Return vs Trade P&L:")
        print(f"    Pearson correlation:  {corr_stats['pearson_r']:+.4f} (p={corr_stats['pearson_p']:.4f})")
        print(f"    Spearman correlation: {corr_stats['spearman_r']:+.4f} (p={corr_stats['spearman_p']:.4f})")
        print(f"\n  Linear Regression:")
        print(f"    Slope:     {corr_stats['slope']:+.4f} (trade return per 1% trailing return)")
        print(f"    Intercept: {corr_stats['intercept']:+.4f}")
        print(f"    R-squared: {corr_stats['r_squared']:.4f}")
        print(f"    Std Error: {corr_stats['std_err']:.4f}")
        print(f"    N points:  {corr_stats['n_points']}")

        # Interpretation
        print(f"\n  Interpretation:")
        if abs(corr_stats['pearson_r']) < 0.1:
            print("    - Correlation is very weak (|r| < 0.1)")
        elif abs(corr_stats['pearson_r']) < 0.3:
            print("    - Correlation is weak (0.1 <= |r| < 0.3)")
        elif abs(corr_stats['pearson_r']) < 0.5:
            print("    - Correlation is moderate (0.3 <= |r| < 0.5)")
        else:
            print("    - Correlation is strong (|r| >= 0.5)")

        if corr_stats['pearson_p'] < 0.05:
            print("    - Correlation is statistically significant (p < 0.05)")
        else:
            print("    - Correlation is NOT statistically significant (p >= 0.05)")

        if corr_stats['slope'] > 0:
            print("    - Higher trailing returns tend to predict better trade outcomes")
        else:
            print("    - Higher trailing returns tend to predict worse trade outcomes")
    else:
        print("\n  Insufficient data for correlation analysis.")

    # Year-by-year breakdown
    print(f"\n{'=' * 80}")
    print("YEAR-BY-YEAR BREAKDOWN")
    print(f"{'=' * 80}")

    trades_df["year"] = pd.to_datetime(trades_df["entry_date"]).dt.year

    print(f"\n  {'Year':<6} {'Trades':>8} {'AvgT12m':>10} {'MeanRet':>10} {'Win%':>8} {'TotalPnL':>12}")
    print(f"  {'-' * 60}")

    for year in sorted(trades_df["year"].unique()):
        year_trades = trades_df[trades_df["year"] == year]
        n = len(year_trades)
        avg_t12m = year_trades["entry_trailing_12m_return"].mean()
        mean_ret = year_trades["pnl_pct"].mean()
        win_rate = (year_trades["pnl_pct"] > 0).mean()
        total_pnl = year_trades["pnl_dollar"].sum()

        print(f"  {year:<6} {n:>8} {avg_t12m:>+9.1%} {mean_ret:>+9.1%} "
              f"{win_rate:>7.0%} ${total_pnl:>11,.0f}")

    # Key insights
    print(f"\n{'=' * 80}")
    print("KEY INSIGHTS")
    print(f"{'=' * 80}")

    if bucket_stats:
        best_bucket = max(bucket_stats.items(), key=lambda x: x[1]["mean_return"])
        worst_bucket = min(bucket_stats.items(), key=lambda x: x[1]["mean_return"])

        print(f"\n  Best bucket:  {best_bucket[0]} (mean: {best_bucket[1]['mean_return']:+.1%}, "
              f"win rate: {best_bucket[1]['win_rate']:.0%}, n={best_bucket[1]['n_trades']})")
        print(f"  Worst bucket: {worst_bucket[0]} (mean: {worst_bucket[1]['mean_return']:+.1%}, "
              f"win rate: {worst_bucket[1]['win_rate']:.0%}, n={worst_bucket[1]['n_trades']})")

        # Check for momentum effect
        neg_bucket = bucket_stats.get("<0%", {})
        high_bucket = bucket_stats.get(">30%", {})

        if neg_bucket and high_bucket:
            print(f"\n  Negative trailing year trades: {neg_bucket.get('n_trades', 0)}")
            print(f"    Mean return: {neg_bucket.get('mean_return', 0):+.1%}")

            print(f"\n  Strong momentum (>30%) trades: {high_bucket.get('n_trades', 0)}")
            print(f"    Mean return: {high_bucket.get('mean_return', 0):+.1%}")

    # Save results
    output_file = os.path.join(_this_dir, f"analysis_trailing_returns_{ticker.lower()}_results.csv")
    trades_df.to_csv(output_file, index=False)
    print(f"\n  Results saved to: {output_file}")

    # Save scatter plot data
    scatter_file = os.path.join(_this_dir, f"trailing_return_scatter_{ticker.lower()}.csv")
    scatter_df = trades_df[["entry_date", "entry_trailing_12m_return", "pnl_pct", "pnl_dollar"]].copy()
    scatter_df.to_csv(scatter_file, index=False)
    print(f"  Scatter plot data saved to: {scatter_file}")

    print(f"\n{'=' * 80}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Trailing Return Correlation Analysis")
    parser.add_argument("ticker", nargs="?", default="SPY",
                        help="Ticker to analyze: SPY (default), QQQ, or RSP")
    args = parser.parse_args()
    run_analysis(args.ticker)
