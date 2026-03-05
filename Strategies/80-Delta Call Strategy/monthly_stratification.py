"""
Monthly Return Stratification Analysis
=======================================

Buckets SPY monthly returns by 1% intervals and shows how each
strategy (SPY B&H, UPRO B&H, UPRO DD25/Cool40, 80-Delta Opts-Only)
performs in each bucket.

Usage:
    python -u monthly_stratification.py
"""

import os
import sys
import math
from datetime import datetime, timedelta

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import yfinance as yf

# Import shared logic from strategy_comparison.py
_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_dir = os.path.dirname(os.path.dirname(_this_dir))
sys.path.insert(0, _project_dir)
sys.path.insert(0, _this_dir)

from strategy_comparison import (
    compute_metrics,
    run_upro_dd25,
    run_spy_bh,
    run_synthetic_options,
    INITIAL_CAPITAL,
    DATA_END,
)

# ======================================================================
# CONSTANTS
# ======================================================================
DATA_START = "2008-01-01"
SIM_START = "2009-06-23"  # UPRO inception


# ======================================================================
# UPRO B&H
# ======================================================================

def run_upro_bh(start_date):
    """Run UPRO Buy & Hold. Returns (dates, values, metrics)."""
    print(f"  Downloading UPRO ({start_date} to {DATA_END})...")
    upro = yf.download("UPRO", start=start_date, end=DATA_END, progress=False,
                       auto_adjust=True, multi_level_index=False)
    upro = upro[["Close"]].dropna()
    upro = upro.loc[upro.index >= start_date]
    closes = upro["Close"].values
    values = (INITIAL_CAPITAL / closes[0]) * closes
    dates = [d.strftime("%Y-%m-%d") for d in upro.index]
    return dates, values, compute_metrics(values, dates)


# ======================================================================
# MONTHLY RETURNS
# ======================================================================

def daily_to_monthly(dates, values):
    """Convert daily (date_str, value) series to monthly returns.

    Returns DataFrame with columns: year, month, monthly_return.
    """
    df = pd.DataFrame({"date": pd.to_datetime(dates), "value": values})
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month

    records = []
    for (y, m), g in df.groupby(["year", "month"]):
        if len(g) < 2:
            continue
        ret = g["value"].iloc[-1] / g["value"].iloc[0] - 1
        records.append({"year": y, "month": m, "return": ret})
    return pd.DataFrame(records)


def build_bins(min_val=-0.10, max_val=0.10, step=0.01):
    """Build bin edges from min_val to max_val in step increments.
    Returns list of (lo, hi, label) tuples. Outer bins capture tails.
    """
    bins = []
    # Tail bin: < min_val
    bins.append((-np.inf, min_val, f"< {min_val:+.0%}"))
    # Inner bins
    lo = min_val
    while lo < max_val - 1e-9:
        hi = lo + step
        bins.append((lo, hi, f"{lo:+.0%} to {hi:+.0%}"))
        lo = hi
    # Tail bin: >= max_val
    bins.append((max_val, np.inf, f">= {max_val:+.0%}"))
    return bins


def assign_bin(val, bins):
    """Assign a value to a bin. Returns bin index."""
    for i, (lo, hi, _) in enumerate(bins):
        if lo <= val < hi:
            return i
    return len(bins) - 1  # fallback to last bin


# ======================================================================
# OUTPUT
# ======================================================================

def print_distribution_table(spy_monthly, strategy_monthlies, bins, output):
    """Print the main distribution table."""
    # Merge all strategies on (year, month)
    merged = spy_monthly[["year", "month", "return"]].rename(columns={"return": "SPY"})
    for name, df in strategy_monthlies.items():
        merged = merged.merge(
            df[["year", "month", "return"]].rename(columns={"return": name}),
            on=["year", "month"], how="left"
        )

    # Assign bins based on SPY return
    merged["bin"] = merged["SPY"].apply(lambda x: assign_bin(x, bins))

    strategy_names = list(strategy_monthlies.keys())
    all_cols = ["SPY"] + strategy_names

    # Header
    hdr = f"{'SPY Bucket':<18} {'Count':>6} {'SPY Avg':>10}"
    for name in strategy_names:
        hdr += f" {name:>14}"
    output(hdr)
    output("-" * len(hdr))

    bin_data = []
    for i, (lo, hi, label) in enumerate(bins):
        rows = merged[merged["bin"] == i]
        count = len(rows)
        if count == 0:
            continue
        spy_avg = rows["SPY"].mean()
        line = f"{label:<18} {count:>6} {spy_avg:>+10.1%}"
        row_dict = {"label": label, "count": count, "SPY": spy_avg}
        for name in strategy_names:
            valid = rows[name].dropna()
            if len(valid) > 0:
                avg = valid.mean()
                line += f" {avg:>+14.1%}"
                row_dict[name] = avg
            else:
                line += f" {'N/A':>14}"
                row_dict[name] = np.nan
        output(line)
        bin_data.append(row_dict)

    return merged, bin_data


def print_leverage_table(bin_data, strategy_names, output):
    """Print leverage ratios (strategy return / SPY return)."""
    hdr = f"{'SPY Bucket':<18} {'Count':>6}"
    for name in strategy_names:
        hdr += f" {name:>14}"
    output(hdr)
    output("-" * len(hdr))

    for row in bin_data:
        spy_avg = row["SPY"]
        if abs(spy_avg) < 0.001:
            # Skip near-zero buckets (ratio undefined)
            continue
        line = f"{row['label']:<18} {row['count']:>6}"
        for name in strategy_names:
            val = row.get(name, np.nan)
            if np.isnan(val):
                line += f" {'N/A':>14}"
            else:
                ratio = val / spy_avg
                line += f" {ratio:>14.2f}x"
        output(line)


def print_regime_summary(merged, strategy_names, output):
    """Print summary stats by bear/flat/bull regime."""
    all_cols = ["SPY"] + strategy_names

    regimes = [
        ("Bear (SPY < -2%)", merged["SPY"] < -0.02),
        ("Flat (-2% to +2%)", (merged["SPY"] >= -0.02) & (merged["SPY"] < 0.02)),
        ("Bull (SPY >= +2%)", merged["SPY"] >= 0.02),
    ]

    hdr = f"{'Regime':<22} {'Count':>6} {'SPY Avg':>10}"
    for name in strategy_names:
        hdr += f" {name:>14}"
    output(hdr)
    output("-" * len(hdr))

    for regime_name, mask in regimes:
        subset = merged[mask]
        count = len(subset)
        if count == 0:
            continue
        spy_avg = subset["SPY"].mean()
        line = f"{regime_name:<22} {count:>6} {spy_avg:>+10.1%}"
        for name in strategy_names:
            valid = subset[name].dropna()
            if len(valid) > 0:
                avg = valid.mean()
                line += f" {avg:>+14.1%}"
            else:
                line += f" {'N/A':>14}"
        output(line)


# ======================================================================
# CHART GENERATION
# ======================================================================

_ARTICLE_CHART_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(_this_dir)))),
    "articles", "upro-timing", "charts"
)

COLORS = {
    "spy": "#555555",
    "upro_bh": "#1f77b4",
    "upro_dd25": "#9467bd",
    "opts_80d": "#2ca02c",
}

STRATEGY_LABELS = ["SPY", "UPRO B&H", "UPRO DD25", "80D Opts"]
STRATEGY_COLORS = [COLORS["spy"], COLORS["upro_bh"], COLORS["upro_dd25"], COLORS["opts_80d"]]


def _save_chart(fig, filename):
    """Save chart to both local dir and article charts dir."""
    for d in [_this_dir, _ARTICLE_CHART_DIR]:
        path = os.path.join(d, filename)
        os.makedirs(d, exist_ok=True)
        fig.savefig(path, dpi=200, bbox_inches="tight")
        print(f"  Saved: {path}")
    plt.close(fig)


def generate_stratification_chart(bin_data):
    """Chart 1: Grouped bar chart of average monthly returns by SPY bucket."""
    labels = [r["label"] for r in bin_data]
    x = np.arange(len(labels))
    width = 0.2

    fig, ax = plt.subplots(figsize=(14, 6))
    for i, (name, color) in enumerate(zip(STRATEGY_LABELS, STRATEGY_COLORS)):
        vals = [r.get(name, r.get("SPY", 0)) * 100 if name != "SPY" else r["SPY"] * 100
                for r in bin_data]
        # For SPY, values are in the dict as "SPY"
        if name == "SPY":
            vals = [r["SPY"] * 100 for r in bin_data]
        ax.bar(x + (i - 1.5) * width, vals, width, label=name, color=color, alpha=0.85)

    ax.set_xlabel("SPY Monthly Return Bucket")
    ax.set_ylabel("Average Monthly Return (%)")
    ax.set_title("Monthly Return Stratification by SPY Return Bucket (2009-2026)")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=8)
    ax.legend(loc="upper left")
    ax.axhline(y=0, color="black", linewidth=0.5)
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    _save_chart(fig, "10_stratification_returns.png")


def generate_leverage_chart(bin_data, strategy_names):
    """Chart 2: Line chart of leverage ratios with 3x reference line."""
    # Filter out near-zero buckets
    filtered = [r for r in bin_data if abs(r["SPY"]) >= 0.001]
    labels = [r["label"] for r in filtered]
    x = np.arange(len(labels))

    line_names = strategy_names  # UPRO B&H, UPRO DD25, 80D Opts
    line_colors = [COLORS["upro_bh"], COLORS["upro_dd25"], COLORS["opts_80d"]]

    fig, ax = plt.subplots(figsize=(14, 6))
    for name, color in zip(line_names, line_colors):
        ratios = []
        for r in filtered:
            val = r.get(name, np.nan)
            spy = r["SPY"]
            if not np.isnan(val) and abs(spy) > 0.001:
                ratios.append(val / spy)
            else:
                ratios.append(np.nan)
        ax.plot(x, ratios, marker="o", markersize=5, label=name, color=color, linewidth=2)

    ax.axhline(y=3.0, color="red", linewidth=1.5, linestyle="--", alpha=0.7, label="3.0x (UPRO theoretical)")
    ax.axhline(y=1.0, color="gray", linewidth=0.8, linestyle=":", alpha=0.5)
    ax.set_xlabel("SPY Monthly Return Bucket")
    ax.set_ylabel("Leverage Ratio (Strategy / SPY)")
    ax.set_title("Effective Leverage Ratios by SPY Return Bucket (2009-2026)")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=8)
    ax.legend(loc="upper right")
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    _save_chart(fig, "11_leverage_ratios.png")


def generate_regime_chart(merged, strategy_names):
    """Chart 3: Grouped bar chart for Bear/Flat/Bull regimes."""
    regimes = [
        ("Bear\n(SPY < -2%)", merged["SPY"] < -0.02),
        ("Flat\n(-2% to +2%)", (merged["SPY"] >= -0.02) & (merged["SPY"] < 0.02)),
        ("Bull\n(SPY >= +2%)", merged["SPY"] >= 0.02),
    ]

    all_names = ["SPY"] + strategy_names
    all_colors = STRATEGY_COLORS

    regime_labels = []
    regime_data = {n: [] for n in all_names}

    for regime_name, mask in regimes:
        subset = merged[mask]
        regime_labels.append(regime_name)
        regime_data["SPY"].append(subset["SPY"].mean() * 100)
        for name in strategy_names:
            valid = subset[name].dropna()
            regime_data[name].append(valid.mean() * 100 if len(valid) > 0 else 0)

    x = np.arange(len(regime_labels))
    width = 0.18

    fig, ax = plt.subplots(figsize=(10, 6))
    for i, (name, color) in enumerate(zip(all_names, all_colors)):
        vals = regime_data[name]
        ax.bar(x + (i - 1.5) * width, vals, width, label=name, color=color, alpha=0.85)

    ax.set_xlabel("Market Regime")
    ax.set_ylabel("Average Monthly Return (%)")
    ax.set_title("Strategy Performance by Market Regime (2009-2026)")
    ax.set_xticks(x)
    ax.set_xticklabels(regime_labels, fontsize=11)
    ax.legend()
    ax.axhline(y=0, color="black", linewidth=0.5)
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    _save_chart(fig, "12_regime_summary.png")


# ======================================================================
# MAIN
# ======================================================================

def main():
    output_lines = []

    def log(s=""):
        print(s)
        output_lines.append(s)

    W = 90
    log("=" * W)
    log("MONTHLY RETURN STRATIFICATION ANALYSIS")
    log(f"Period: {SIM_START} to {DATA_END} (UPRO inception onward)")
    log("=" * W)

    # ---- Run strategies ----
    log("\n[1/4] SPY Buy & Hold...")
    spy_dates, spy_vals, spy_m = run_spy_bh(SIM_START)

    log("[2/4] UPRO Buy & Hold...")
    upro_bh_dates, upro_bh_vals, upro_bh_m = run_upro_bh(SIM_START)

    log("[3/4] UPRO DD25/Cool40...")
    upro_dd_dates, upro_dd_vals, upro_dd_m = run_upro_dd25(SIM_START)

    log("[4/4] Synthetic 80-Delta Options-Only...")
    opts_dates, opts_vals, opts_m, _ = run_synthetic_options(SIM_START)

    # ---- Convert to monthly returns ----
    log("\nComputing monthly returns...")
    spy_monthly = daily_to_monthly(spy_dates, spy_vals)
    upro_bh_monthly = daily_to_monthly(upro_bh_dates, upro_bh_vals)
    upro_dd_monthly = daily_to_monthly(upro_dd_dates, upro_dd_vals)
    opts_monthly = daily_to_monthly(opts_dates, opts_vals)

    log(f"  SPY months: {len(spy_monthly)}")
    log(f"  UPRO B&H months: {len(upro_bh_monthly)}")
    log(f"  UPRO DD25 months: {len(upro_dd_monthly)}")
    log(f"  80D Opts months: {len(opts_monthly)}")

    strategy_monthlies = {
        "UPRO B&H": upro_bh_monthly,
        "UPRO DD25": upro_dd_monthly,
        "80D Opts": opts_monthly,
    }
    strategy_names = list(strategy_monthlies.keys())

    # ---- Build bins ----
    bins = build_bins(min_val=-0.08, max_val=0.08, step=0.01)

    # ---- Table 1: Distribution ----
    log(f"\n{'=' * W}")
    log("MONTHLY RETURN DISTRIBUTION BY SPY RETURN BUCKET")
    log(f"{'=' * W}")
    merged, bin_data = print_distribution_table(
        spy_monthly, strategy_monthlies, bins, log
    )

    # ---- Table 2: Leverage ratios ----
    log(f"\n{'=' * W}")
    log("LEVERAGE RATIOS (Strategy Monthly Return / SPY Monthly Return)")
    log(f"{'=' * W}")
    print_leverage_table(bin_data, strategy_names, log)

    # ---- Table 3: Regime summary ----
    log(f"\n{'=' * W}")
    log("SUMMARY STATISTICS BY REGIME")
    log(f"{'=' * W}")
    print_regime_summary(merged, strategy_names, log)

    # ---- Key insights ----
    log(f"\n{'=' * W}")
    log("KEY INSIGHTS")
    log(f"{'=' * W}")

    # UPRO B&H leverage ratio
    upro_ratios = []
    for row in bin_data:
        spy_avg = row["SPY"]
        upro_avg = row.get("UPRO B&H", np.nan)
        if abs(spy_avg) > 0.005 and not np.isnan(upro_avg):
            upro_ratios.append(upro_avg / spy_avg)
    if upro_ratios:
        avg_ratio = np.mean(upro_ratios)
        log(f"\n1. UPRO B&H average leverage ratio: {avg_ratio:.2f}x (theoretical: 3.0x)")
        log(f"   Range: {min(upro_ratios):.2f}x to {max(upro_ratios):.2f}x")

    # DD25 in bear months
    bear = merged[merged["SPY"] < -0.02]
    if len(bear) > 0:
        spy_bear_avg = bear["SPY"].mean()
        dd25_bear = bear["UPRO DD25"].dropna()
        upro_bear = bear["UPRO B&H"].dropna()
        if len(dd25_bear) > 0 and len(upro_bear) > 0:
            log(f"\n2. Bear months (SPY < -2%): {len(bear)} months")
            log(f"   SPY avg: {spy_bear_avg:+.1%}")
            log(f"   UPRO B&H avg: {upro_bear.mean():+.1%}")
            log(f"   UPRO DD25 avg: {dd25_bear.mean():+.1%}")
            log(f"   DD25 loss mitigation: {upro_bear.mean() - dd25_bear.mean():+.1%} better than UPRO B&H")

    # Options convexity
    bull = merged[merged["SPY"] >= 0.02]
    if len(bull) > 0:
        opts_bull = bull["80D Opts"].dropna()
        spy_bull_avg = bull["SPY"].mean()
        if len(opts_bull) > 0:
            opts_ratio = opts_bull.mean() / spy_bull_avg
            log(f"\n3. 80-Delta options in bull months: {opts_ratio:.2f}x leverage ratio")
            log(f"   Bull months: {len(bull)}, avg SPY return: {spy_bull_avg:+.1%}, avg 80D return: {opts_bull.mean():+.1%}")

    # Overall stats
    log(f"\n{'=' * W}")
    log("STRATEGY SUMMARY (Full Period)")
    log(f"{'=' * W}")
    for name, m in [("SPY B&H", spy_m), ("UPRO B&H", upro_bh_m),
                     ("UPRO DD25/Cool40", upro_dd_m), ("Syn 80D Opts", opts_m)]:
        log(f"  {name:<22} CAGR: {m['cagr']:+.1%}  Sharpe: {m['sharpe']:.2f}  MaxDD: {m['max_dd']:+.1%}")

    # ---- Generate charts ----
    log(f"\n{'=' * W}")
    log("GENERATING CHARTS")
    log(f"{'=' * W}")
    generate_stratification_chart(bin_data)
    generate_leverage_chart(bin_data, strategy_names)
    generate_regime_chart(merged, strategy_names)
    log("All charts generated.")

    # ---- Save output ----
    output_path = os.path.join(_this_dir, "monthly_stratification_output.txt")
    with open(output_path, "w") as f:
        f.write("\n".join(output_lines))
    log(f"\nOutput saved to: {output_path}")


if __name__ == "__main__":
    main()
