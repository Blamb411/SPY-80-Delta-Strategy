"""
Valuation Metrics Correlation Analysis
=======================================
Analyzes 80-delta call strategy performance segmented by market valuation
metrics at the time of trade entry.

Valuation Metrics:
- CAPE (Shiller P/E): 10-year cyclically adjusted P/E ratio
- Trailing P/E: Current price / trailing 12-month earnings
- Forward P/E: Current price / forward 12-month earnings estimates

Data Sources:
- CAPE: Fetched via fred_client.py from Shiller's data
- P/E metrics: Requires LSEG client (optional, in archive)

Usage:
    python analysis_valuation.py
"""

import os
import sys
import numpy as np
import pandas as pd
from scipy import stats

_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_dir = os.path.dirname(os.path.dirname(_this_dir))
sys.path.insert(0, _project_dir)

from delta_capped_backtest import (
    load_all_data, run_delta_capped_simulation, ThetaDataClient,
    DATA_START, DATA_END
)

# Try to import FRED client
try:
    from backtest.fred_client import FREDClient
    FRED_AVAILABLE = True
except ImportError:
    FRED_AVAILABLE = False
    print("Warning: FRED client not available for CAPE data")


# ======================================================================
# QUINTILE DEFINITIONS
# ======================================================================

def assign_quintile(value, thresholds):
    """
    Assign a value to a quintile (1-5) based on thresholds.
    thresholds should be [20th, 40th, 60th, 80th percentile]
    """
    if value is None or np.isnan(value):
        return None
    if value < thresholds[0]:
        return "Q1 (Low)"
    elif value < thresholds[1]:
        return "Q2"
    elif value < thresholds[2]:
        return "Q3"
    elif value < thresholds[3]:
        return "Q4"
    else:
        return "Q5 (High)"


def compute_quintile_thresholds(values):
    """Compute quintile thresholds (20th, 40th, 60th, 80th percentiles)."""
    clean_values = [v for v in values if v is not None and not np.isnan(v)]
    if not clean_values:
        return [0, 0, 0, 0]
    return [np.percentile(clean_values, p) for p in [20, 40, 60, 80]]


def compute_quintile_stats(trades_df, quintile_col):
    """Compute statistics for each quintile."""
    results = {}
    quintile_order = ["Q1 (Low)", "Q2", "Q3", "Q4", "Q5 (High)"]

    for quintile in quintile_order:
        q_trades = trades_df[trades_df[quintile_col] == quintile]
        n = len(q_trades)

        if n == 0:
            continue

        wins = q_trades[q_trades["pnl_pct"] > 0]

        stats = {
            "n_trades": n,
            "win_rate": len(wins) / n if n > 0 else 0,
            "mean_return": q_trades["pnl_pct"].mean(),
            "median_return": q_trades["pnl_pct"].median(),
            "std_return": q_trades["pnl_pct"].std(),
            "total_pnl": q_trades["pnl_dollar"].sum(),
            "avg_days_held": q_trades["days_held"].mean(),
        }

        # Sharpe-like ratio
        if n >= 10 and stats["std_return"] > 0:
            trades_per_year = 252 / stats["avg_days_held"] if stats["avg_days_held"] > 0 else 6
            annual_return = stats["mean_return"] * trades_per_year
            annual_vol = stats["std_return"] * np.sqrt(trades_per_year)
            stats["sharpe"] = annual_return / annual_vol if annual_vol > 0 else 0
        else:
            stats["sharpe"] = None

        results[quintile] = stats

    return results


def print_quintile_analysis(quintile_stats, metric_name):
    """Pretty print the quintile analysis results."""
    W = 95

    print(f"\n{'=' * W}")
    print(f"SEGMENTATION BY {metric_name.upper()}")
    print(f"{'=' * W}")

    quintile_order = ["Q1 (Low)", "Q2", "Q3", "Q4", "Q5 (High)"]

    print(f"\n  {'Quintile':<12} {'Trades':>8} {'Win%':>8} {'Mean':>10} {'Median':>10} "
          f"{'Total P&L':>12} {'Sharpe':>8}")
    print(f"  {'-' * 80}")

    for quintile in quintile_order:
        if quintile not in quintile_stats:
            print(f"  {quintile:<12} {'--':>8}")
            continue

        s = quintile_stats[quintile]
        sharpe_str = f"{s['sharpe']:.2f}" if s['sharpe'] is not None else "--"

        print(f"  {quintile:<12} {s['n_trades']:>8} {s['win_rate']:>7.1%} "
              f"{s['mean_return']:>+9.1%} {s['median_return']:>+9.1%} "
              f"${s['total_pnl']:>11,.0f} {sharpe_str:>8}")


def run_analysis():
    """Run the valuation metrics correlation analysis."""
    print("=" * 80)
    print("Valuation Metrics Correlation Analysis")
    print("=" * 80)

    # Connect and load data
    client = ThetaDataClient()
    if not client.connect():
        print("\nERROR: Cannot connect to Theta Terminal.")
        return

    print("\nConnected to Theta Terminal.\n")

    # Load data
    spy_by_date, trading_dates, vix_data, sma200, monthly_exps, trailing_12m_returns, rolling_volatility = load_all_data(client)

    # Run simulation
    print("\nRunning backtest simulation...")
    snaps, trades, cc_trades = run_delta_capped_simulation(
        client, spy_by_date, trading_dates, vix_data, sma200, monthly_exps,
        trailing_12m_returns=trailing_12m_returns,
        rolling_volatility=rolling_volatility,
        force_exit_below_sma=True,
        sell_covered_calls=False,
        label="Valuation Analysis",
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

    # Try to load CAPE data
    cape_data = {}
    if FRED_AVAILABLE:
        print("\n  Loading CAPE data...")
        fred = FREDClient()
        cape_data = fred.fetch_cape(DATA_START, DATA_END)
        if cape_data:
            print(f"  CAPE data: {len(cape_data)} months loaded")
            # Create daily interpolation
            daily_cape = fred.interpolate_cape_daily(list(trades_df["entry_date"]))
            trades_df["entry_cape"] = trades_df["entry_date"].map(daily_cape)
            print(f"  CAPE assigned to {trades_df['entry_cape'].notna().sum()} trades")
        else:
            print("  No CAPE data available")
        fred.close()
    else:
        print("\n  CAPE data not available (FRED client not installed)")

    # CAPE Analysis
    if "entry_cape" in trades_df.columns and trades_df["entry_cape"].notna().any():
        print(f"\n{'=' * 80}")
        print("CAPE (SHILLER P/E) ANALYSIS")
        print(f"{'=' * 80}")

        cape_values = trades_df["entry_cape"].dropna().values
        print(f"\n  CAPE at entry:")
        print(f"    Mean:   {np.mean(cape_values):.1f}")
        print(f"    Median: {np.median(cape_values):.1f}")
        print(f"    Min:    {np.min(cape_values):.1f}")
        print(f"    Max:    {np.max(cape_values):.1f}")

        # Compute quintile thresholds
        cape_thresholds = compute_quintile_thresholds(cape_values)
        print(f"\n  Quintile thresholds: {[f'{t:.1f}' for t in cape_thresholds]}")

        # Assign quintiles
        trades_df["cape_quintile"] = trades_df["entry_cape"].apply(
            lambda x: assign_quintile(x, cape_thresholds)
        )

        # Compute and print stats
        cape_stats = compute_quintile_stats(trades_df, "cape_quintile")
        print_quintile_analysis(cape_stats, "CAPE (Shiller P/E)")

        # Correlation analysis
        cape_trades = trades_df[trades_df["entry_cape"].notna()]
        if len(cape_trades) >= 10:
            x = cape_trades["entry_cape"].values
            y = cape_trades["pnl_pct"].values

            pearson_r, pearson_p = stats.pearsonr(x, y)
            spearman_r, spearman_p = stats.spearmanr(x, y)

            print(f"\n  Correlation: CAPE vs Trade Return")
            print(f"    Pearson:  r={pearson_r:+.4f}, p={pearson_p:.4f}")
            print(f"    Spearman: r={spearman_r:+.4f}, p={spearman_p:.4f}")

            if pearson_p < 0.05:
                print("    ** Statistically significant **")
            else:
                print("    (Not statistically significant)")

    # Alternative analysis: Use P/E proxy from trailing returns
    # Higher trailing returns often correlate with higher valuations
    print(f"\n{'=' * 80}")
    print("VALUATION PROXY ANALYSIS")
    print(f"{'=' * 80}")
    print("\n  Note: Using trailing 12m return as valuation proxy")
    print("  (Higher returns often correlate with higher valuations)")

    if "entry_trailing_12m_return" in trades_df.columns:
        trail_values = trades_df["entry_trailing_12m_return"].dropna().values

        # Use trailing return quintiles as valuation proxy
        trail_thresholds = compute_quintile_thresholds(trail_values)
        print(f"\n  Trailing return quintile thresholds: {[f'{t:.1%}' for t in trail_thresholds]}")

        trades_df["trail_quintile"] = trades_df["entry_trailing_12m_return"].apply(
            lambda x: assign_quintile(x, trail_thresholds)
        )

        trail_stats = compute_quintile_stats(trades_df, "trail_quintile")
        print_quintile_analysis(trail_stats, "Trailing 12m Return (Valuation Proxy)")

    # Key insights
    print(f"\n{'=' * 80}")
    print("KEY INSIGHTS")
    print(f"{'=' * 80}")

    if "entry_cape" in trades_df.columns and trades_df["entry_cape"].notna().any():
        cape_stats = compute_quintile_stats(trades_df, "cape_quintile")
        if cape_stats:
            best_q = max((q for q in cape_stats.items() if q[1]["n_trades"] >= 5),
                         key=lambda x: x[1]["mean_return"], default=None)
            worst_q = min((q for q in cape_stats.items() if q[1]["n_trades"] >= 5),
                          key=lambda x: x[1]["mean_return"], default=None)

            if best_q and worst_q:
                print(f"\n  CAPE Analysis:")
                print(f"    Best quintile:  {best_q[0]} (mean: {best_q[1]['mean_return']:+.1%})")
                print(f"    Worst quintile: {worst_q[0]} (mean: {worst_q[1]['mean_return']:+.1%})")

                if "Q1" in best_q[0] or "Q2" in best_q[0]:
                    print("    -> Strategy performs better in lower valuation environments")
                elif "Q4" in best_q[0] or "Q5" in best_q[0]:
                    print("    -> Strategy performs better in higher valuation environments")

    print(f"\n{'=' * 80}")
    print("DATA SOURCE NOTES")
    print(f"{'=' * 80}")
    print("""
  CAPE (Shiller P/E):
    - Source: Robert Shiller's dataset (Yale)
    - Updated monthly
    - 10-year cyclically adjusted earnings

  For P/E and Forward P/E:
    - Requires LSEG Workspace subscription
    - LSEG client available in Analysis/Archive/quant_model/data/lseg_client.py
    - Fields: TR.PE (trailing), TR.FwdPE (forward)

  Alternative approach:
    - Use trailing 12-month SPY return as valuation proxy
    - Higher returns often correlate with higher valuations
    - This is imperfect but doesn't require external data
""")

    # Save results
    output_file = os.path.join(_this_dir, "analysis_valuation_results.csv")
    trades_df.to_csv(output_file, index=False)
    print(f"\n  Results saved to: {output_file}")

    print(f"\n{'=' * 80}")


if __name__ == "__main__":
    run_analysis()
