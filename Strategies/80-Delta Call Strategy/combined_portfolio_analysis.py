"""
Combined Portfolio Analysis
============================
Computes risk-adjusted metrics for:
  - SPY B&H alone
  - Options strategy alone (Config A / Config B)
  - Combined: $1M SPY B&H + $1M Options strategy

Does this for both SPY and QQQ option strategies.

Usage:
    python combined_portfolio_analysis.py
"""

import os
import sys
import logging

import numpy as np
import pandas as pd

_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_dir = os.path.dirname(os.path.dirname(_this_dir))
sys.path.insert(0, _project_dir)
sys.path.insert(0, _this_dir)

from backtest.thetadata_client import ThetaDataClient

import spy_deployment_sim as spy_sim
import qqq_deployment_sim as qqq_sim

ALLOC = 1_000_000  # $1M per component
SCALE = ALLOC / 100_000


def compute_stats(daily_values, label=""):
    """Compute key stats from a daily portfolio value series."""
    pv = np.array(daily_values, dtype=float)
    n = len(pv)
    years = n / 252.0

    rets = np.diff(pv) / pv[:-1]
    rets = rets[np.isfinite(rets)]

    cagr = (pv[-1] / pv[0]) ** (1 / years) - 1 if years > 0 else 0
    total_ret = pv[-1] / pv[0] - 1

    mean_r = rets.mean()
    std_r = rets.std()
    sharpe = (mean_r / std_r) * np.sqrt(252) if std_r > 0 else 0

    down = rets[rets < 0]
    ds_std = down.std() if len(down) > 0 else 0
    sortino = (mean_r / ds_std) * np.sqrt(252) if ds_std > 0 else 0

    cummax = np.maximum.accumulate(pv)
    dd = pv / cummax - 1
    max_dd = dd.min()

    # Calmar = CAGR / |max DD|
    calmar = cagr / abs(max_dd) if max_dd < 0 else 0

    # Daily correlation with itself is 1, but we return rets for cross-corr
    return {
        "label": label,
        "start": pv[0],
        "end": pv[-1],
        "total_ret": total_ret,
        "cagr": cagr,
        "sharpe": sharpe,
        "sortino": sortino,
        "max_dd": max_dd,
        "calmar": calmar,
        "annual_vol": std_r * np.sqrt(252),
        "daily_rets": rets,
    }


def print_table(rows, title):
    """Print a formatted comparison table."""
    W = 105
    print(f"\n{'=' * W}")
    print(title)
    print(f"{'=' * W}")

    hdr = (f"  {'Portfolio':<38} {'End Value':>12} {'CAGR':>8} {'Sharpe':>8} "
           f"{'Sortino':>8} {'Max DD':>8} {'Calmar':>8} {'Vol':>8}")
    print(hdr)
    print(f"  {'-' * (W - 2)}")

    for r in rows:
        print(f"  {r['label']:<38} "
              f"${r['end']:>11,.0f} "
              f"{r['cagr']:>+7.1%} "
              f"{r['sharpe']:>8.2f} "
              f"{r['sortino']:>8.2f} "
              f"{r['max_dd']:>7.1%} "
              f"{r['calmar']:>8.2f} "
              f"{r['annual_vol']:>7.1%}")

    print(f"  {'-' * (W - 2)}")


def print_correlations(stats_list):
    """Print correlation matrix of daily returns."""
    labels = [s["label"] for s in stats_list]
    n = len(labels)
    rets = [s["daily_rets"] for s in stats_list]

    # Trim to same length
    min_len = min(len(r) for r in rets)
    rets = [r[:min_len] for r in rets]

    print(f"\n  Daily Return Correlations:")
    # Header
    short_labels = []
    for l in labels:
        if "Combined" in l:
            short_labels.append("Comb")
        elif "B&H" in l:
            short_labels.append("B&H")
        elif "Config A" in l:
            short_labels.append("CfgA")
        elif "Config B" in l:
            short_labels.append("CfgB")
        else:
            short_labels.append(l[:6])

    print(f"  {'':30}", end="")
    for sl in short_labels:
        print(f" {sl:>7}", end="")
    print()

    for i in range(n):
        print(f"  {labels[i]:<30}", end="")
        for j in range(n):
            corr = np.corrcoef(rets[i], rets[j])[0, 1]
            print(f" {corr:>7.3f}", end="")
        print()


def analyze_ticker(client, ticker, sim_module, load_data_fn):
    """Run sims and compute combined portfolio stats for one ticker."""

    print(f"\n{'#' * 60}")
    print(f"  {ticker} Strategy Analysis")
    print(f"{'#' * 60}")

    data = load_data_fn(client)

    if ticker == "SPY":
        by_date, trading_dates, vix, sma200, exps = data
        spy_by_date = by_date
    else:
        by_date, spy_by_date, trading_dates, vix, sma200, exps = data

    # Run simulations
    snaps_a, _ = sim_module.run_simulation(
        client, by_date, trading_dates, vix, sma200, exps,
        force_exit_below_sma=False, label="A: Entry-only SMA filter",
    )
    snaps_b, _ = sim_module.run_simulation(
        client, by_date, trading_dates, vix, sma200, exps,
        force_exit_below_sma=True, label="B: Thresh-exit (>2% below SMA)",
    )

    df_a = pd.DataFrame(snaps_a)
    df_b = pd.DataFrame(snaps_b)
    dates_str = df_a["date"].tolist()

    # Options strategy values (scaled to $1M)
    opt_a = df_a["portfolio_value"].values * SCALE
    opt_b = df_b["portfolio_value"].values * SCALE

    # SPY B&H equity curve ($1M start)
    spy_prices = np.array([spy_by_date.get(d, {}).get("close", np.nan)
                           for d in dates_str], dtype=float)
    mask = np.isnan(spy_prices)
    if mask.any():
        idx = np.where(~mask, np.arange(len(spy_prices)), 0)
        np.maximum.accumulate(idx, out=idx)
        spy_prices = spy_prices[idx]
    spy_bh = ALLOC * spy_prices / spy_prices[0]

    # Combined portfolios ($2M total: $1M B&H + $1M options)
    combined_a = spy_bh + opt_a
    combined_b = spy_bh + opt_b

    # Compute stats
    s_spy = compute_stats(spy_bh, "SPY Buy & Hold ($1M)")
    s_opt_a = compute_stats(opt_a, f"{ticker} Options Config A ($1M)")
    s_opt_b = compute_stats(opt_b, f"{ticker} Options Config B ($1M)")
    s_comb_a = compute_stats(combined_a, f"Combined: SPY B&H + {ticker} Cfg A ($2M)")
    s_comb_b = compute_stats(combined_b, f"Combined: SPY B&H + {ticker} Cfg B ($2M)")

    print_table(
        [s_spy, s_opt_a, s_opt_b, s_comb_a, s_comb_b],
        f"{ticker} OPTIONS STRATEGY -- STANDALONE vs COMBINED WITH SPY B&H",
    )

    # Show how combined compares
    print(f"\n  Ratio Improvement (Combined vs Options Alone):")
    for opt_s, comb_s, lbl in [(s_opt_a, s_comb_a, "Config A"),
                                (s_opt_b, s_comb_b, "Config B")]:
        sh_delta = comb_s["sharpe"] - opt_s["sharpe"]
        so_delta = comb_s["sortino"] - opt_s["sortino"]
        dd_improve = abs(opt_s["max_dd"]) - abs(comb_s["max_dd"])
        print(f"    {lbl}: Sharpe {opt_s['sharpe']:.2f} -> {comb_s['sharpe']:.2f} ({sh_delta:+.2f})  |  "
              f"Sortino {opt_s['sortino']:.2f} -> {comb_s['sortino']:.2f} ({so_delta:+.2f})  |  "
              f"Max DD {opt_s['max_dd']:.1%} -> {comb_s['max_dd']:.1%} ({dd_improve:+.1%} pts)")

    print_correlations([s_spy, s_opt_a, s_opt_b, s_comb_a, s_comb_b])

    return s_spy, s_opt_a, s_opt_b, s_comb_a, s_comb_b


def main():
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    print("=" * 70)
    print("Combined Portfolio Analysis: SPY B&H + Options Strategy")
    print("  $1M in each component ($2M total)")
    print("=" * 70)

    client = ThetaDataClient()
    if not client.connect():
        print("\nERROR: Cannot connect to Theta Terminal.")
        return

    print("Connected to Theta Terminal.")

    spy_results = analyze_ticker(client, "SPY", spy_sim, spy_sim.load_all_data)
    qqq_results = analyze_ticker(client, "QQQ", qqq_sim, qqq_sim.load_all_data)

    # Cross-comparison summary
    W = 105
    print(f"\n\n{'=' * W}")
    print("CROSS-COMPARISON SUMMARY")
    print(f"{'=' * W}")
    print(f"\n  Best risk-adjusted portfolios across both tickers:\n")

    all_results = [
        ("SPY B&H only", spy_results[0]),
        ("SPY Options A only", spy_results[1]),
        ("SPY Options B only", spy_results[2]),
        ("SPY B&H + SPY Opts A", spy_results[3]),
        ("SPY B&H + SPY Opts B", spy_results[4]),
        ("QQQ Options A only", qqq_results[1]),
        ("QQQ Options B only", qqq_results[2]),
        ("SPY B&H + QQQ Opts A", qqq_results[3]),
        ("SPY B&H + QQQ Opts B", qqq_results[4]),
    ]

    print(f"  {'Portfolio':<28} {'CAGR':>8} {'Sharpe':>8} {'Sortino':>8} {'Max DD':>8} {'Calmar':>8}")
    print(f"  {'-' * 72}")
    # Sort by Sharpe
    all_results.sort(key=lambda x: x[1]["sharpe"], reverse=True)
    for name, s in all_results:
        print(f"  {name:<28} {s['cagr']:>+7.1%} {s['sharpe']:>8.2f} "
              f"{s['sortino']:>8.2f} {s['max_dd']:>7.1%} {s['calmar']:>8.2f}")

    print(f"\n{'=' * W}")

    client.close()


if __name__ == "__main__":
    main()
