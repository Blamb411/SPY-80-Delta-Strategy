#!/usr/bin/env python3
"""
Monte Carlo Block Bootstrap for 80-Delta Call Strategy
========================================================
Runs the delta-capped backtest once to produce daily portfolio snapshots,
then block-bootstraps the daily return series to estimate the distribution
of Sharpe, CAGR, max drawdown, and Sortino.

Block bootstrap preserves temporal autocorrelation (vol clustering) by
resampling contiguous blocks of N trading days rather than individual days.

Usage:
    python monte_carlo.py                        # IID bootstrap (default)
    python monte_carlo.py --block-size 20        # block bootstrap
    python monte_carlo.py --iterations 20000 --block-size 20
"""

import os
import sys
import time
import random
import math
import logging
import argparse

import numpy as np

_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_dir = os.path.dirname(os.path.dirname(_this_dir))
sys.path.insert(0, _project_dir)
sys.path.insert(0, _this_dir)

from backtest.thetadata_client import ThetaDataClient
from delta_capped_backtest import (
    load_all_data, run_delta_capped_simulation, compute_metrics,
)

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s  %(message)s",
    datefmt="%H:%M:%S",
)


def extract_daily_returns(snapshots):
    """Extract daily portfolio returns from snapshots."""
    values = [s["portfolio_value"] for s in snapshots]
    returns = []
    for i in range(1, len(values)):
        if values[i - 1] > 0:
            returns.append(values[i] / values[i - 1] - 1.0)
    return returns


def extract_options_only_returns(snapshots):
    """Extract daily returns for the options-only component (cash + positions)."""
    values = [s["options_cash"] + s["options_value"] for s in snapshots]
    returns = []
    for i in range(1, len(values)):
        if values[i - 1] > 0:
            returns.append(values[i] / values[i - 1] - 1.0)
    return returns


def bootstrap_daily_returns(daily_returns, n_iterations=10000, block_size=0, seed=42):
    """
    Bootstrap resample daily returns (IID or block).

    For each iteration, builds a synthetic return series of the same length,
    then computes annualized Sharpe, Sortino, CAGR, and max drawdown.
    """
    random.seed(seed)
    n = len(daily_returns)

    results = {
        "cagr": [],
        "sharpe": [],
        "sortino": [],
        "max_dd": [],
        "total_return": [],
    }

    for _ in range(n_iterations):
        # Build synthetic return series
        if block_size > 0:
            bs = min(block_size, n)
            max_start = n - bs
            n_blocks = math.ceil(n / bs)
            sample = []
            for _ in range(n_blocks):
                start = random.randint(0, max_start)
                sample.extend(daily_returns[start:start + bs])
            sample = sample[:n]
        else:
            sample = random.choices(daily_returns, k=n)

        # Compute cumulative equity curve
        equity = [1.0]
        for r in sample:
            equity.append(equity[-1] * (1 + r))

        total_return = equity[-1] / equity[0] - 1
        years = n / 252.0
        cagr = (equity[-1] ** (1 / years) - 1) if years > 0 and equity[-1] > 0 else 0

        # Annualized Sharpe
        mean_r = np.mean(sample)
        std_r = np.std(sample, ddof=1)
        sharpe = (mean_r / std_r) * np.sqrt(252) if std_r > 0 else 0

        # Annualized Sortino
        downside = [r for r in sample if r < 0]
        ds_std = np.std(downside, ddof=1) if len(downside) > 1 else 0
        sortino = (mean_r / ds_std) * np.sqrt(252) if ds_std > 0 else 0

        # Max drawdown
        peak = equity[0]
        max_dd = 0
        for v in equity:
            if v > peak:
                peak = v
            dd = (peak - v) / peak
            if dd > max_dd:
                max_dd = dd

        results["cagr"].append(cagr)
        results["sharpe"].append(sharpe)
        results["sortino"].append(sortino)
        results["max_dd"].append(max_dd)
        results["total_return"].append(total_return)

    return results


def percentile(data, pct):
    """Compute the pct-th percentile."""
    sorted_data = sorted(data)
    idx = pct / 100.0 * (len(sorted_data) - 1)
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return sorted_data[lo]
    frac = idx - lo
    return sorted_data[lo] * (1 - frac) + sorted_data[hi] * frac


def print_report(results, n_days, n_iterations, block_size=0, hist_metrics=None,
                 label="Combined Portfolio"):
    """Print Monte Carlo results."""
    print("=" * 70)
    print(f"MONTE CARLO SIMULATION RESULTS  --  {label}")
    print("=" * 70)
    print(f"  Trading days:      {n_days}")
    print(f"  Iterations:        {n_iterations:,}")
    if block_size > 0:
        print(f"  Bootstrap mode:    Block (size={block_size} days)")
    else:
        print(f"  Bootstrap mode:    IID (independent)")

    if hist_metrics:
        print(f"\n  --- Historical (actual) ---")
        print(f"  CAGR:       {hist_metrics['cagr']:.1%}")
        print(f"  Sharpe:     {hist_metrics['sharpe']:.3f}")
        print(f"  Sortino:    {hist_metrics['sortino']:.3f}")
        print(f"  Max DD:     {hist_metrics['max_dd']:.1%}")

    print()
    metrics = [
        ("CAGR", "cagr", " {:>12.1%}"),
        ("Sharpe (ann.)", "sharpe", " {:>12.3f}"),
        ("Sortino (ann.)", "sortino", " {:>12.3f}"),
        ("Max Drawdown", "max_dd", " {:>12.1%}"),
        ("Total Return", "total_return", " {:>12.1%}"),
    ]

    print(f"  {'Metric':<22} {'Median':>13} {'5th Pctl':>13} {'95th Pctl':>13} "
          f"{'Mean':>13}")
    print("  " + "-" * 78)

    for label, key, fmt in metrics:
        data = results[key]
        med = percentile(data, 50)
        p5 = percentile(data, 5)
        p95 = percentile(data, 95)
        mean = sum(data) / len(data)
        print(f"  {label:<22} {fmt.format(med):>13} {fmt.format(p5):>13} "
              f"{fmt.format(p95):>13} {fmt.format(mean):>13}")

    # Probability of negative CAGR
    prob_neg = sum(1 for c in results["cagr"] if c < 0) / len(results["cagr"])
    print(f"\n  Probability of negative CAGR: {prob_neg:.1%}")

    # Probability of Sharpe > 0
    prob_pos_sharpe = sum(1 for s in results["sharpe"] if s > 0) / len(results["sharpe"])
    print(f"  Probability of Sharpe > 0: {prob_pos_sharpe:.1%}")

    # Probability of beating SPY Sharpe (~0.8)
    prob_beat_spy = sum(1 for s in results["sharpe"] if s > 0.8) / len(results["sharpe"])
    print(f"  Probability of Sharpe > 0.8 (SPY benchmark): {prob_beat_spy:.1%}")

    # VaR on max drawdown (95th percentile = worst 5%)
    dd_95 = percentile(results["max_dd"], 95)
    print(f"  Max Drawdown VaR (95th pctl): {dd_95:.1%}")


def main():
    parser = argparse.ArgumentParser(
        description="Monte Carlo bootstrap for 80-delta call strategy")
    parser.add_argument("--iterations", type=int, default=10000,
                        help="Number of bootstrap iterations (default 10000)")
    parser.add_argument("--block-size", type=int, default=0, metavar="N",
                        help="Block size in trading days (0 = IID). "
                             "Recommended: 20 (~1 month).")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed (default 42)")
    parser.add_argument("--no-cc", action="store_true",
                        help="Run without covered calls")
    args = parser.parse_args()

    print("Connecting to Theta Terminal...")
    client = ThetaDataClient()
    if not client.connect():
        print("ERROR: Cannot connect to Theta Terminal.")
        return

    print("Loading data...")
    t0 = time.time()
    spy_by_date, trading_dates, vix_data, sma200, monthly_exps, \
        trailing_12m_returns, rolling_volatility, spy_dividends = load_all_data(client)

    # Run simulation
    label = "no CC" if args.no_cc else "with CC"
    print(f"\nRunning 80-delta simulation ({label})...")
    snapshots, trade_log, cc_trade_log = run_delta_capped_simulation(
        client, spy_by_date, trading_dates, vix_data, sma200, monthly_exps,
        trailing_12m_returns=trailing_12m_returns,
        rolling_volatility=rolling_volatility,
        spy_dividends=spy_dividends,
        force_exit_below_sma=True,
        sell_covered_calls=not args.no_cc,
        label=label,
    )

    if not snapshots or len(snapshots) < 10:
        print("Insufficient snapshot data.")
        client.close()
        return

    # Get historical metrics for comparison
    hist = compute_metrics(snapshots, trade_log, cc_trade_log, label)
    hist_metrics = {
        "cagr": hist["cagr"],
        "sharpe": hist["sharpe"],
        "sortino": hist["sortino"],
        "max_dd": hist["max_dd"],
    }

    sim_time = time.time() - t0
    print(f"Simulation complete: {len(snapshots)} days, "
          f"{len(trade_log)} trades in {sim_time:.1f}s")

    # Extract daily returns (combined and options-only)
    daily_returns = extract_daily_returns(snapshots)
    options_returns = extract_options_only_returns(snapshots)
    print(f"Daily returns: {len(daily_returns)} combined, "
          f"{len(options_returns)} options-only observations")

    # Run bootstrap
    if args.block_size > 0:
        mode_str = f"block bootstrap (block_size={args.block_size})"
    else:
        mode_str = "IID bootstrap"
    print(f"\nRunning {args.iterations:,} {mode_str} iterations...")
    mc_t0 = time.time()

    # Combined portfolio bootstrap
    results = bootstrap_daily_returns(
        daily_returns,
        n_iterations=args.iterations,
        block_size=args.block_size,
        seed=args.seed,
    )

    # Options-only bootstrap
    options_results = bootstrap_daily_returns(
        options_returns,
        n_iterations=args.iterations,
        block_size=args.block_size,
        seed=args.seed + 1,
    )

    mc_time = time.time() - mc_t0

    # Options-only historical metrics
    opt_values = [s["options_cash"] + s["options_value"] for s in snapshots]
    opt_start, opt_end = opt_values[0], opt_values[-1]
    n_days = len(daily_returns)
    years = n_days / 252.0
    opt_cagr = (opt_end / opt_start) ** (1 / years) - 1 if years > 0 and opt_end > 0 else 0
    opt_mean = np.mean(options_returns)
    opt_std = np.std(options_returns, ddof=1)
    opt_sharpe = (opt_mean / opt_std) * np.sqrt(252) if opt_std > 0 else 0
    opt_downside = [r for r in options_returns if r < 0]
    opt_ds_std = np.std(opt_downside, ddof=1) if len(opt_downside) > 1 else 0
    opt_sortino = (opt_mean / opt_ds_std) * np.sqrt(252) if opt_ds_std > 0 else 0
    opt_peak = opt_values[0]
    opt_max_dd = 0
    for v in opt_values:
        if v > opt_peak:
            opt_peak = v
        dd = (opt_peak - v) / opt_peak if opt_peak > 0 else 0
        if dd > opt_max_dd:
            opt_max_dd = dd
    opt_hist_metrics = {
        "cagr": opt_cagr,
        "sharpe": opt_sharpe,
        "sortino": opt_sortino,
        "max_dd": opt_max_dd,
    }

    print_report(results, len(daily_returns), args.iterations,
                 block_size=args.block_size, hist_metrics=hist_metrics,
                 label="Combined Portfolio (Shares + Options)")

    print("\n")
    print_report(options_results, len(options_returns), args.iterations,
                 block_size=args.block_size, hist_metrics=opt_hist_metrics,
                 label="Options-Only Portfolio")

    print(f"\nMonte Carlo completed in {mc_time:.1f}s (total {time.time()-t0:.1f}s)")

    client.close()


if __name__ == "__main__":
    main()
