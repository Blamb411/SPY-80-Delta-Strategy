"""
Cash Strategy Analysis
======================
Models cash management on idle capital in the options strategy:

  - BIL (1-3mo T-bills / SGOV proxy) on all idle cash
  - TLT (20+ yr Treasuries) during below-SMA200 periods
  - GLD (gold) during below-SMA200 periods

Above SMA200: idle cash always earns BIL (short-term, liquid, ready to deploy)
Below SMA200: idle cash rotates into the alternative (TLT or GLD)

Shows standalone strategy and combined ($1M SPY B&H + $1M strategy).

Usage:
    python cash_strategy_analysis.py
"""

import os
import sys
import logging

import numpy as np
import pandas as pd
import yfinance as yf

_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_dir = os.path.dirname(os.path.dirname(_this_dir))
sys.path.insert(0, _project_dir)
sys.path.insert(0, _this_dir)

from backtest.thetadata_client import ThetaDataClient

import spy_deployment_sim as spy_sim
import qqq_deployment_sim as qqq_sim

ALLOC = 1_000_000
SCALE = ALLOC / 100_000  # sims use $100K

DATA_START = "2014-01-01"
DATA_END = "2026-01-31"


# ======================================================================
# ETF DATA
# ======================================================================

def fetch_etf_returns(ticker, start, end):
    """Fetch daily total returns for an ETF from Yahoo Finance."""
    print(f"  Fetching {ticker}...")
    df = yf.download(ticker, start=start, end=end, auto_adjust=True, progress=False)
    if df.empty:
        print(f"    WARNING: No data for {ticker}")
        return {}
    close = df["Close"]
    if isinstance(close, pd.DataFrame):
        close = close.iloc[:, 0]
    rets = close.pct_change().fillna(0)
    result = {}
    for idx in rets.index:
        date_str = idx.strftime("%Y-%m-%d")
        result[date_str] = float(rets.loc[idx])

    # Summary
    cum = (1 + pd.Series(list(result.values()))).prod() - 1
    n_years = len(result) / 252.0
    cagr = (1 + cum) ** (1 / n_years) - 1 if n_years > 0 else 0
    print(f"    {ticker}: {len(result)} days, cumulative {cum:+.1%}, CAGR {cagr:+.1%}")
    return result


# ======================================================================
# ADJUSTED PORTFOLIO COMPUTATION
# ======================================================================

def compute_adjusted_values(snapshots, bil_rets, alt_rets=None):
    """
    Recompute portfolio values with cash yield applied.

    bil_rets: daily returns applied to idle cash (above SMA, or always if no alt)
    alt_rets: if provided, applied to idle cash when below SMA200

    Returns: numpy array of adjusted portfolio values (in $100K-base terms).
    """
    cum_yield = 0.0
    adjusted = []

    for snap in snapshots:
        d = snap["date"]
        cash = snap["cash"]
        above = snap["above_sma"]

        if alt_rets is not None and not above:
            r = alt_rets.get(d, 0.0)
        else:
            r = bil_rets.get(d, 0.0)

        effective_cash = max(0, cash + cum_yield)
        daily_yield = effective_cash * r
        cum_yield += daily_yield

        adjusted.append(snap["portfolio_value"] + cum_yield)

    return np.array(adjusted)


def compute_stats(daily_values, label=""):
    """Compute key stats from a daily portfolio value series."""
    pv = np.array(daily_values, dtype=float)
    n = len(pv)
    years = n / 252.0

    rets = np.diff(pv) / pv[:-1]
    rets = rets[np.isfinite(rets)]

    cagr = (pv[-1] / pv[0]) ** (1 / years) - 1 if years > 0 and pv[0] > 0 else 0
    total_ret = pv[-1] / pv[0] - 1 if pv[0] > 0 else 0

    mean_r = rets.mean()
    std_r = rets.std()
    sharpe = (mean_r / std_r) * np.sqrt(252) if std_r > 0 else 0

    down = rets[rets < 0]
    ds_std = down.std() if len(down) > 0 else 0
    sortino = (mean_r / ds_std) * np.sqrt(252) if ds_std > 0 else 0

    cummax = np.maximum.accumulate(pv)
    dd = pv / cummax - 1
    max_dd = dd.min()

    calmar = cagr / abs(max_dd) if max_dd < 0 else 0
    vol = std_r * np.sqrt(252)

    return {
        "label": label,
        "start": pv[0], "end": pv[-1],
        "total_ret": total_ret, "cagr": cagr,
        "sharpe": sharpe, "sortino": sortino,
        "max_dd": max_dd, "calmar": calmar, "vol": vol,
        "daily_rets": rets,
    }


# ======================================================================
# OUTPUT
# ======================================================================

def print_section(rows, title, w=110):
    """Print a formatted comparison table."""
    print(f"\n  {title}")
    print(f"  {'-' * (w - 4)}")

    hdr = (f"  {'Cash Strategy':<30} {'End Value':>12} {'CAGR':>8} "
           f"{'Sharpe':>8} {'Sortino':>8} {'Max DD':>8} {'Calmar':>8} {'Vol':>8}")
    print(hdr)
    print(f"  {'-' * (w - 4)}")

    for r in rows:
        end_str = f"${r['end']:>11,.0f}"
        print(f"  {r['label']:<30} {end_str} {r['cagr']:>+7.1%} "
              f"{r['sharpe']:>8.2f} {r['sortino']:>8.2f} "
              f"{r['max_dd']:>7.1%} {r['calmar']:>8.2f} {r['vol']:>7.1%}")


def analyze_ticker(snapshots_a, snapshots_b, spy_by_date,
                   bil_rets, tlt_rets, gld_rets, ticker):
    """Analyze all cash strategy variants for one ticker."""

    W = 110
    print(f"\n{'=' * W}")
    print(f"  {ticker} OPTIONS STRATEGY -- CASH MANAGEMENT ANALYSIS")
    print(f"{'=' * W}")

    all_stats = []

    for snaps, config_label, config_tag in [
        (snapshots_a, "Config A: Entry-only SMA filter", "A"),
        (snapshots_b, "Config B: Thresh-exit (>2% below SMA)", "B"),
    ]:
        dates = [s["date"] for s in snaps]

        # Original (no yield)
        pv_orig = np.array([s["portfolio_value"] for s in snaps]) * SCALE
        # BIL on all cash
        pv_bil = compute_adjusted_values(snaps, bil_rets) * SCALE
        # BIL + TLT below SMA
        pv_tlt = compute_adjusted_values(snaps, bil_rets, tlt_rets) * SCALE
        # BIL + GLD below SMA
        pv_gld = compute_adjusted_values(snaps, bil_rets, gld_rets) * SCALE

        variants = [
            (pv_orig, "No cash yield"),
            (pv_bil, "BIL on all cash"),
            (pv_tlt, "BIL + TLT (below SMA)"),
            (pv_gld, "BIL + GLD (below SMA)"),
        ]

        rows = []
        for pv, cash_lbl in variants:
            s = compute_stats(pv, cash_lbl)
            rows.append(s)
            s["_full_label"] = f"{ticker} {config_tag}: {cash_lbl}"
            s["_pv"] = pv
            s["_dates"] = dates
            s["_config"] = config_tag
            all_stats.append(s)

        print_section(rows, config_label)

    # --- Combined portfolios: $1M SPY B&H + $1M strategy ---
    print(f"\n{'-' * W}")
    print(f"  COMBINED PORTFOLIOS: $1M SPY B&H + $1M {ticker} Options")
    print(f"{'-' * W}")

    for snaps, config_label, config_tag in [
        (snapshots_a, "Config A: Entry-only", "A"),
        (snapshots_b, "Config B: Thresh-exit", "B"),
    ]:
        dates = [s["date"] for s in snaps]

        # SPY B&H component ($1M)
        spy_prices = np.array([spy_by_date.get(d, {}).get("close", np.nan)
                               for d in dates], dtype=float)
        mask = np.isnan(spy_prices)
        if mask.any():
            idx = np.where(~mask, np.arange(len(spy_prices)), 0)
            np.maximum.accumulate(idx, out=idx)
            spy_prices = spy_prices[idx]
        spy_bh = ALLOC * spy_prices / spy_prices[0]

        pv_orig = np.array([s["portfolio_value"] for s in snaps]) * SCALE
        pv_bil = compute_adjusted_values(snaps, bil_rets) * SCALE
        pv_tlt = compute_adjusted_values(snaps, bil_rets, tlt_rets) * SCALE
        pv_gld = compute_adjusted_values(snaps, bil_rets, gld_rets) * SCALE

        variants = [
            (spy_bh + pv_orig, "No cash yield"),
            (spy_bh + pv_bil, "BIL on all cash"),
            (spy_bh + pv_tlt, "BIL + TLT (below SMA)"),
            (spy_bh + pv_gld, "BIL + GLD (below SMA)"),
        ]

        rows = []
        for pv, cash_lbl in variants:
            s = compute_stats(pv, cash_lbl)
            rows.append(s)
            s["_full_label"] = f"SPY+{ticker} {config_tag}: {cash_lbl}"
            all_stats.append(s)

        print_section(rows, f"Combined {config_label} ($2M total)")

    return all_stats


# ======================================================================
# BELOW-SMA PERIOD ANALYSIS
# ======================================================================

def analyze_below_sma_periods(snapshots, bil_rets, tlt_rets, gld_rets, ticker):
    """Show what BIL/TLT/GLD returned during specific below-SMA periods."""

    # Find contiguous below-SMA periods
    periods = []
    in_period = False
    start_date = None

    for snap in snapshots:
        if not snap["above_sma"] and not in_period:
            in_period = True
            start_date = snap["date"]
        elif snap["above_sma"] and in_period:
            in_period = False
            periods.append((start_date, snap["date"]))

    if in_period:
        periods.append((start_date, snapshots[-1]["date"]))

    # Only show periods > 10 days
    sig_periods = [(s, e) for s, e in periods if len([
        sn for sn in snapshots if s <= sn["date"] <= e
    ]) > 10]

    if not sig_periods:
        return

    print(f"\n  Below-SMA200 Periods (>{10} trading days) -- ETF returns during each:")
    print(f"  {'Period':<30} {'Days':>6} {'BIL':>8} {'TLT':>8} {'GLD':>8}")
    print(f"  {'-' * 68}")

    for start, end in sig_periods:
        dates_in = [sn["date"] for sn in snapshots if start <= sn["date"] <= end]
        n_days = len(dates_in)

        bil_cum = 1.0
        tlt_cum = 1.0
        gld_cum = 1.0
        for d in dates_in:
            bil_cum *= (1 + bil_rets.get(d, 0.0))
            tlt_cum *= (1 + tlt_rets.get(d, 0.0))
            gld_cum *= (1 + gld_rets.get(d, 0.0))

        print(f"  {start} to {end:<14} {n_days:>6} "
              f"{bil_cum - 1:>+7.1%} {tlt_cum - 1:>+7.1%} {gld_cum - 1:>+7.1%}")


# ======================================================================
# MAIN
# ======================================================================

def main():
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    W = 110
    print("=" * W)
    print("  Cash Strategy Analysis: BIL / TLT / GLD on Idle Capital")
    print("=" * W)

    # --- Fetch ETF data ---
    print("\nLoading ETF data from Yahoo Finance...")
    bil_rets = fetch_etf_returns("BIL", DATA_START, DATA_END)
    tlt_rets = fetch_etf_returns("TLT", DATA_START, DATA_END)
    gld_rets = fetch_etf_returns("GLD", DATA_START, DATA_END)

    if not bil_rets or not tlt_rets or not gld_rets:
        print("ERROR: Could not fetch ETF data.")
        return

    # --- Connect to ThetaData ---
    client = ThetaDataClient()
    if not client.connect():
        print("\nERROR: Cannot connect to Theta Terminal.")
        return
    print("\nConnected to Theta Terminal.")

    # --- Run SPY simulations ---
    print(f"\n{'-' * 50}")
    print("Running SPY simulations...")
    print(f"{'-' * 50}")
    spy_data = spy_sim.load_all_data(client)
    spy_by_date, spy_dates, spy_vix, spy_sma200, spy_exps = spy_data

    spy_snaps_a, _ = spy_sim.run_simulation(
        client, spy_by_date, spy_dates, spy_vix, spy_sma200, spy_exps,
        force_exit_below_sma=False, label="A: Entry-only SMA filter",
    )
    spy_snaps_b, _ = spy_sim.run_simulation(
        client, spy_by_date, spy_dates, spy_vix, spy_sma200, spy_exps,
        force_exit_below_sma=True, label="B: Thresh-exit (>2% below SMA)",
    )

    # --- Run QQQ simulations ---
    print(f"\n{'-' * 50}")
    print("Running QQQ simulations...")
    print(f"{'-' * 50}")
    qqq_data = qqq_sim.load_all_data(client)
    qqq_by_date, qqq_spy_by_date, qqq_dates, qqq_vix, qqq_sma200, qqq_exps = qqq_data

    qqq_snaps_a, _ = qqq_sim.run_simulation(
        client, qqq_by_date, qqq_dates, qqq_vix, qqq_sma200, qqq_exps,
        force_exit_below_sma=False, label="A: Entry-only SMA filter",
    )
    qqq_snaps_b, _ = qqq_sim.run_simulation(
        client, qqq_by_date, qqq_dates, qqq_vix, qqq_sma200, qqq_exps,
        force_exit_below_sma=True, label="B: Thresh-exit (>2% below SMA)",
    )

    # --- Analyze SPY strategy ---
    spy_stats = analyze_ticker(
        spy_snaps_a, spy_snaps_b, spy_by_date,
        bil_rets, tlt_rets, gld_rets, "SPY",
    )
    print(f"\n{'-' * W}")
    print(f"  SPY Below-SMA200 Period Detail")
    print(f"{'-' * W}")
    analyze_below_sma_periods(spy_snaps_a, bil_rets, tlt_rets, gld_rets, "SPY")

    # --- Analyze QQQ strategy ---
    qqq_stats = analyze_ticker(
        qqq_snaps_a, qqq_snaps_b, qqq_spy_by_date,
        bil_rets, tlt_rets, gld_rets, "QQQ",
    )
    print(f"\n{'-' * W}")
    print(f"  QQQ Below-SMA200 Period Detail")
    print(f"{'-' * W}")
    analyze_below_sma_periods(qqq_snaps_a, bil_rets, tlt_rets, gld_rets, "QQQ")

    # --- SPY B&H reference ---
    spy_dates_a = [s["date"] for s in spy_snaps_a]
    spy_prices = np.array([spy_by_date.get(d, {}).get("close", np.nan)
                           for d in spy_dates_a], dtype=float)
    mask = np.isnan(spy_prices)
    if mask.any():
        idx = np.where(~mask, np.arange(len(spy_prices)), 0)
        np.maximum.accumulate(idx, out=idx)
        spy_prices = spy_prices[idx]
    spy_bh_values = ALLOC * spy_prices / spy_prices[0]
    spy_bh_stats = compute_stats(spy_bh_values, "SPY Buy & Hold ($1M)")

    # --- Cross-comparison ---
    print(f"\n\n{'=' * W}")
    print("  CROSS-COMPARISON -- ALL STRATEGIES RANKED BY SHARPE")
    print(f"{'=' * W}")

    # Collect the most interesting variants
    ranking = []

    # SPY B&H
    ranking.append(("SPY Buy & Hold", spy_bh_stats))

    # Strategy-only variants (pick BIL and best alternative for each)
    for stats_list in [spy_stats, qqq_stats]:
        for s in stats_list:
            if "_full_label" in s:
                ranking.append((s["_full_label"], s))

    # Sort by Sharpe descending
    ranking.sort(key=lambda x: x[1]["sharpe"], reverse=True)

    print(f"\n  {'Portfolio':<42} {'End Value':>12} {'CAGR':>8} "
          f"{'Sharpe':>8} {'Sortino':>8} {'Max DD':>8} {'Calmar':>8}")
    print(f"  {'-' * (W - 4)}")

    for name, s in ranking[:25]:
        end_str = f"${s['end']:>11,.0f}"
        print(f"  {name:<42} {end_str} {s['cagr']:>+7.1%} "
              f"{s['sharpe']:>8.2f} {s['sortino']:>8.2f} "
              f"{s['max_dd']:>7.1%} {s['calmar']:>8.2f}")

    print(f"\n{'=' * W}")

    client.close()
    print("\nDone!")


if __name__ == "__main__":
    main()
