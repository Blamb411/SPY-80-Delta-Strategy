#!/usr/bin/env python3
"""
Monte Carlo Block Bootstrap for DD25%/Cool40 variants:
  - T-bill cash (baseline)
  - TLT cash (bond alternative)
  - TMF cash (3x bond alternative)

Computes the DD25/Cool40 strategy with each cash vehicle, then
block-bootstraps the daily returns to estimate Sharpe, CAGR, max DD.
"""

import random
import math
import numpy as np
import pandas as pd
import yfinance as yf

INITIAL_CAPITAL = 100_000
UPRO_INCEPTION = "2009-06-25"
END_DATE = "2026-03-08"
DATA_START = "2008-01-01"

DD_THRESHOLD = 0.25
COOL_DAYS = 40


def load_data():
    """Load UPRO, TLT, TMF from yfinance."""
    tickers = {"UPRO": UPRO_INCEPTION, "TLT": DATA_START, "TMF": UPRO_INCEPTION}
    data = {}
    for ticker, start in tickers.items():
        df = yf.download(ticker, start=start, end=END_DATE, progress=False, auto_adjust=True)
        if hasattr(df.columns, 'levels'):
            df.columns = df.columns.get_level_values(0)
        data[ticker] = df
    return data


def run_dd_exit_variant(upro_df, alt_df=None, variant_name="T-bill"):
    """
    DD25/Cool40 with configurable cash vehicle.
    alt_df=None means T-bill (flat cash). Otherwise use alt_df Close prices.
    Returns (dates, daily_values_array, metrics_dict).
    """
    if alt_df is not None:
        common = upro_df.index.intersection(alt_df.index)
        upro = upro_df.loc[common]
        alt = alt_df.loc[common]
    else:
        upro = upro_df
        alt = None
        common = upro_df.index

    prices = upro["Close"].values
    alt_prices = alt["Close"].values if alt is not None else None

    portfolio = INITIAL_CAPITAL
    upro_shares = INITIAL_CAPITAL / prices[0]
    alt_shares = 0.0
    invested = True
    ath = prices[0]
    cool_counter = 0
    in_cool = False
    values = [INITIAL_CAPITAL]

    for i in range(1, len(prices)):
        if invested:
            val = upro_shares * prices[i]
            values.append(val)
            ath = max(ath, prices[i])
            dd = prices[i] / ath - 1
            if dd < -DD_THRESHOLD:
                # Exit UPRO
                portfolio = upro_shares * prices[i]
                upro_shares = 0.0
                if alt_prices is not None:
                    alt_shares = portfolio / alt_prices[i]
                invested = False
                in_cool = True
                cool_counter = 0
        else:
            if alt_prices is not None:
                val = alt_shares * alt_prices[i]
            else:
                val = portfolio  # T-bill: flat
            values.append(val)

            if in_cool:
                cool_counter += 1
                if cool_counter >= COOL_DAYS or prices[i] >= ath:
                    # Re-enter UPRO
                    if alt_prices is not None:
                        portfolio = alt_shares * alt_prices[i]
                        alt_shares = 0.0
                    upro_shares = portfolio / prices[i]
                    invested = True
                    in_cool = False
                    ath = prices[i]

    values = np.array(values, dtype=float)
    end_val = values[-1]
    n = len(values) - 1
    years = n / 252.0
    cagr = (end_val / INITIAL_CAPITAL) ** (1 / years) - 1 if years > 0 else 0

    daily_rets = np.diff(values) / values[:-1]
    mean_r = np.mean(daily_rets)
    std_r = np.std(daily_rets, ddof=1)
    sharpe = (mean_r / std_r) * np.sqrt(252) if std_r > 0 else 0

    downside = daily_rets[daily_rets < 0]
    ds_std = np.std(downside, ddof=1) if len(downside) > 1 else 0
    sortino = (mean_r / ds_std) * np.sqrt(252) if ds_std > 0 else 0

    peak = values[0]
    max_dd = 0
    for v in values:
        if v > peak:
            peak = v
        dd = (peak - v) / peak
        if dd > max_dd:
            max_dd = dd

    metrics = {
        "name": f"DD25/Cool40+{variant_name}",
        "end_val": end_val,
        "cagr": cagr,
        "sharpe": sharpe,
        "sortino": sortino,
        "max_dd": max_dd,
    }
    return common, values, metrics, daily_rets


def block_bootstrap(daily_returns, n_iter=10000, block_size=20, seed=42):
    """Block-bootstrap daily returns, compute metrics per iteration."""
    random.seed(seed)
    n = len(daily_returns)
    results = {"cagr": [], "sharpe": [], "sortino": [], "max_dd": [], "total_return": []}

    for _ in range(n_iter):
        bs = min(block_size, n)
        max_start = n - bs
        n_blocks = math.ceil(n / bs)
        sample = []
        for _ in range(n_blocks):
            start = random.randint(0, max_start)
            sample.extend(daily_returns[start:start + bs])
        sample = sample[:n]

        equity = [1.0]
        for r in sample:
            equity.append(equity[-1] * (1 + r))

        total_return = equity[-1] - 1
        years = n / 252.0
        cagr = (equity[-1] ** (1 / years) - 1) if years > 0 and equity[-1] > 0 else 0

        mean_r = np.mean(sample)
        std_r = np.std(sample, ddof=1)
        sharpe = (mean_r / std_r) * np.sqrt(252) if std_r > 0 else 0

        downside = [r for r in sample if r < 0]
        ds_std = np.std(downside, ddof=1) if len(downside) > 1 else 0
        sortino = (mean_r / ds_std) * np.sqrt(252) if ds_std > 0 else 0

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
    s = sorted(data)
    idx = pct / 100.0 * (len(s) - 1)
    lo, hi = int(math.floor(idx)), int(math.ceil(idx))
    if lo == hi:
        return s[lo]
    frac = idx - lo
    return s[lo] * (1 - frac) + s[hi] * frac


def print_report(name, hist, results, n_iter, block_size):
    print(f"\n{'='*70}")
    print(f"  {name}")
    print(f"{'='*70}")
    print(f"  Historical: CAGR={hist['cagr']:.1%}, Sharpe={hist['sharpe']:.3f}, "
          f"Sortino={hist['sortino']:.3f}, MaxDD={hist['max_dd']:.1%}")
    print(f"  Bootstrap:  {n_iter:,} iterations, block_size={block_size}")
    print()
    print(f"  {'Metric':<20} {'Median':>10} {'5th':>10} {'95th':>10} {'Mean':>10}")
    print(f"  {'-'*60}")
    for label, key, fmt in [("CAGR", "cagr", ".1%"), ("Sharpe", "sharpe", ".3f"),
                             ("Sortino", "sortino", ".3f"), ("Max DD", "max_dd", ".1%"),
                             ("Total Return", "total_return", ".1%")]:
        d = results[key]
        med = format(percentile(d, 50), fmt)
        p5 = format(percentile(d, 5), fmt)
        p95 = format(percentile(d, 95), fmt)
        mn = format(np.mean(d), fmt)
        print(f"  {label:<20} {med:>10} {p5:>10} {p95:>10} {mn:>10}")

    prob_neg = sum(1 for c in results["cagr"] if c < 0) / len(results["cagr"])
    prob_sharpe_pos = sum(1 for s in results["sharpe"] if s > 0) / len(results["sharpe"])
    print(f"\n  P(negative CAGR): {prob_neg:.1%}")
    print(f"  P(Sharpe > 0):    {prob_sharpe_pos:.1%}")


def main():
    n_iter = 10000
    block_size = 20

    print("Loading data...")
    data = load_data()
    upro_df = data["UPRO"]
    tlt_df = data["TLT"]
    tmf_df = data["TMF"]

    # Run three variants
    print("\nRunning DD25/Cool40 variants...")
    _, _, tb_hist, tb_rets = run_dd_exit_variant(upro_df, alt_df=None, variant_name="T-bill")
    _, _, tlt_hist, tlt_rets = run_dd_exit_variant(upro_df, alt_df=tlt_df, variant_name="TLT")
    _, _, tmf_hist, tmf_rets = run_dd_exit_variant(upro_df, alt_df=tmf_df, variant_name="TMF")

    print(f"\n  T-bill: CAGR={tb_hist['cagr']:.1%}, Sharpe={tb_hist['sharpe']:.3f}, MaxDD={tb_hist['max_dd']:.1%}")
    print(f"  TLT:    CAGR={tlt_hist['cagr']:.1%}, Sharpe={tlt_hist['sharpe']:.3f}, MaxDD={tlt_hist['max_dd']:.1%}")
    print(f"  TMF:    CAGR={tmf_hist['cagr']:.1%}, Sharpe={tmf_hist['sharpe']:.3f}, MaxDD={tmf_hist['max_dd']:.1%}")

    # Bootstrap each
    print(f"\nRunning {n_iter:,} block-bootstrap iterations (block_size={block_size})...")

    tb_results = block_bootstrap(tb_rets, n_iter=n_iter, block_size=block_size, seed=42)
    tlt_results = block_bootstrap(tlt_rets, n_iter=n_iter, block_size=block_size, seed=43)
    tmf_results = block_bootstrap(tmf_rets, n_iter=n_iter, block_size=block_size, seed=44)

    print_report("DD25/Cool40 + T-bill Cash", tb_hist, tb_results, n_iter, block_size)
    print_report("DD25/Cool40 + TLT Cash", tlt_hist, tlt_results, n_iter, block_size)
    print_report("DD25/Cool40 + TMF Cash", tmf_hist, tmf_results, n_iter, block_size)

    # Comparative summary
    print(f"\n{'='*70}")
    print("  COMPARATIVE SUMMARY")
    print(f"{'='*70}")
    print(f"  {'Variant':<25} {'Hist Sharpe':>12} {'Boot Median':>12} {'Boot 5th':>10} {'P(S>0)':>8}")
    print(f"  {'-'*67}")
    for name, hist, res in [("T-bill", tb_hist, tb_results),
                             ("TLT", tlt_hist, tlt_results),
                             ("TMF", tmf_hist, tmf_results)]:
        med = percentile(res["sharpe"], 50)
        p5 = percentile(res["sharpe"], 5)
        p_pos = sum(1 for s in res["sharpe"] if s > 0) / len(res["sharpe"])
        print(f"  {name:<25} {hist['sharpe']:>12.3f} {med:>12.3f} {p5:>10.3f} {p_pos:>7.1%}")

    print()


if __name__ == "__main__":
    main()
