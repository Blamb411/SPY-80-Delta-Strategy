#!/usr/bin/env python3
"""
Monte Carlo Block Bootstrap for DD25%/Cool40 variants:
  - T-bill cash (baseline)
  - TLT cash (bond alternative)
  - TMF cash (3x bond alternative)

Methodology:
  - Reshuffles raw market returns (not realized strategy returns) in
    20-day overlapping blocks, then re-runs the DD exit strategy on
    each shuffled path. This tests regime robustness, not just
    statistical stability of realized returns.
  - For bond variants (TLT/TMF): paired (UPRO, bond) blocks preserve
    cross-asset correlation within each block.
  - Historical runs use next-open execution; bootstrap uses close-only
    model for simplicity (signal and execution at same close).
  - Excess Sharpe: daily returns minus T-bill rate
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
TRADING_DAYS_PER_YEAR = 252

DD_THRESHOLD = 0.25
COOL_DAYS = 40


def load_data():
    """Load UPRO, TLT, TMF, IRX from yfinance. Returns Open+Close for
    tradeable instruments, Series for IRX."""
    tickers = {"UPRO": UPRO_INCEPTION, "TLT": DATA_START, "TMF": UPRO_INCEPTION}
    data = {}
    for ticker, start in tickers.items():
        df = yf.download(ticker, start=start, end=END_DATE, progress=False,
                         auto_adjust=True, multi_level_index=False)
        df = df.dropna(subset=["Close"])
        data[ticker] = df[["Open", "Close"]].copy()
        print(f"  {ticker}: {len(data[ticker])} days")

    irx = yf.download("^IRX", start=DATA_START, end=END_DATE, progress=False,
                       auto_adjust=True, multi_level_index=False)
    irx = irx["Close"].squeeze().dropna()
    data["IRX"] = irx
    print(f"  IRX: {len(irx)} days")
    return data


def get_daily_tbill_rate(irx_series):
    """Convert ^IRX (13-week T-bill yield, e.g., 5.2 = 5.2%) to daily return."""
    return (1 + irx_series / 100) ** (1 / TRADING_DAYS_PER_YEAR) - 1


def avg_rf_annual(tbill_daily, dates):
    """Compute average annualized risk-free rate over a date range."""
    if tbill_daily is None or dates is None or len(dates) == 0:
        return 0.0
    tb = tbill_daily.reindex(dates).dropna()
    if len(tb) == 0:
        return 0.0
    return float(tb.mean()) * TRADING_DAYS_PER_YEAR


def run_dd_exit_tbill(upro_df, tbill_daily):
    """DD25/Cool40 with T-bill cash. Next-open execution."""
    dates = upro_df.index
    upro_open = upro_df["Open"].values
    upro_close = upro_df["Close"].values
    tbill = tbill_daily.reindex(dates).fillna(0).values

    shares = INITIAL_CAPITAL / upro_close[0]
    portfolio = INITIAL_CAPITAL
    invested = True
    ath = upro_close[0]
    cool_counter = 0
    in_cool = False
    exit_signal = False
    enter_signal = False
    values = [INITIAL_CAPITAL]
    trades = 1
    days_invested = 0

    for i in range(1, len(upro_close)):
        # Execute pending signals at today's open
        if exit_signal:
            portfolio = shares * upro_open[i]
            shares = 0.0
            invested = False
            exit_signal = False
            in_cool = True
            cool_counter = 0
        elif enter_signal:
            shares = portfolio / upro_open[i]
            invested = True
            enter_signal = False
            ath = upro_open[i]

        # Mark to close
        if invested:
            val = shares * upro_close[i]
            values.append(val)
            days_invested += 1
            ath = max(ath, upro_close[i])
            dd = upro_close[i] / ath - 1
            if dd < -DD_THRESHOLD:
                exit_signal = True
                trades += 1
        else:
            portfolio *= (1 + tbill[i])
            values.append(portfolio)
            if in_cool:
                cool_counter += 1
                if cool_counter >= COOL_DAYS or upro_close[i] >= ath:
                    enter_signal = True
                    in_cool = False
                    trades += 1

    return dates, np.array(values, dtype=float), trades, days_invested


def run_dd_exit_bond(upro_df, bond_df, tbill_daily, bond_name="TLT"):
    """DD25/Cool40 with bond ETF as cash vehicle. Next-open execution."""
    common = upro_df.index.intersection(bond_df.index)
    upro_open = upro_df.loc[common, "Open"].values
    upro_close = upro_df.loc[common, "Close"].values
    bond_open = bond_df.loc[common, "Open"].values
    bond_close = bond_df.loc[common, "Close"].values

    upro_shares = INITIAL_CAPITAL / upro_close[0]
    bond_shares = 0.0
    portfolio = INITIAL_CAPITAL
    in_upro = True
    ath = upro_close[0]
    cool_counter = 0
    in_cool = False
    exit_signal = False
    enter_signal = False
    values = [INITIAL_CAPITAL]
    trades = 1
    days_in_upro = 0

    for i in range(1, len(upro_close)):
        if exit_signal:
            portfolio = upro_shares * upro_open[i]
            upro_shares = 0.0
            bond_shares = portfolio / bond_open[i]
            in_upro = False
            exit_signal = False
            in_cool = True
            cool_counter = 0
        elif enter_signal:
            portfolio = bond_shares * bond_open[i]
            bond_shares = 0.0
            upro_shares = portfolio / upro_open[i]
            in_upro = True
            enter_signal = False
            ath = upro_open[i]

        if in_upro:
            val = upro_shares * upro_close[i]
            values.append(val)
            days_in_upro += 1
            ath = max(ath, upro_close[i])
            dd = upro_close[i] / ath - 1
            if dd < -DD_THRESHOLD:
                exit_signal = True
                trades += 1
        else:
            val = bond_shares * bond_close[i]
            values.append(val)
            if in_cool:
                cool_counter += 1
                if cool_counter >= COOL_DAYS or upro_close[i] >= ath:
                    enter_signal = True
                    in_cool = False
                    trades += 1

    return common, np.array(values, dtype=float), trades, days_in_upro


def compute_metrics(values, name, dates, trades, pct_invested, rf_annual):
    """Compute metrics using excess returns (matching build_docx.py)."""
    vals = np.array(values, dtype=float)
    n_years = len(vals) / TRADING_DAYS_PER_YEAR
    cagr = (vals[-1] / vals[0]) ** (1.0 / n_years) - 1

    daily_rets = np.diff(vals) / vals[:-1]
    daily_rf = rf_annual / TRADING_DAYS_PER_YEAR
    excess_rets = daily_rets - daily_rf

    std_r = np.std(daily_rets, ddof=1)
    sharpe = (np.mean(excess_rets) / std_r * np.sqrt(TRADING_DAYS_PER_YEAR)
              if std_r > 0 else 0)

    neg_excess = excess_rets[excess_rets < 0]
    sortino = (np.mean(excess_rets) / np.std(neg_excess, ddof=1)
               * np.sqrt(TRADING_DAYS_PER_YEAR)
               if len(neg_excess) > 0 and np.std(neg_excess, ddof=1) > 0 else 0)

    cummax = np.maximum.accumulate(vals)
    max_dd = (vals / cummax - 1).min()

    return {
        "name": name,
        "end_val": vals[-1],
        "cagr": cagr,
        "sharpe": sharpe,
        "sortino": sortino,
        "max_dd": abs(max_dd),
        "pct_invested": pct_invested,
        "trades": trades,
    }


def _run_dd_on_prices(prices, threshold, cool_days, cash_rets=None,
                      daily_rf=0.0):
    """Run DD exit strategy on a synthetic price path (close-only model).

    Args:
        prices: synthetic UPRO price array
        threshold: drawdown exit threshold (e.g. 0.25)
        cool_days: minimum days before re-entry
        cash_rets: daily returns for cash vehicle (bond). If None, uses daily_rf.
        daily_rf: constant daily risk-free rate (used when cash_rets is None)

    Returns: strategy portfolio value array
    """
    shares = INITIAL_CAPITAL / prices[0]
    portfolio = INITIAL_CAPITAL
    invested = True
    ath = prices[0]
    cool_counter = 0
    in_cool = False
    exit_flag = False
    enter_flag = False
    values = [INITIAL_CAPITAL]

    for i in range(1, len(prices)):
        if exit_flag:
            portfolio = shares * prices[i]
            shares = 0.0
            invested = False
            exit_flag = False
            in_cool = True
            cool_counter = 0
        elif enter_flag:
            shares = portfolio / prices[i]
            invested = True
            enter_flag = False
            ath = prices[i]

        if invested:
            val = shares * prices[i]
            values.append(val)
            ath = max(ath, prices[i])
            dd = prices[i] / ath - 1
            if dd < -threshold:
                exit_flag = True
        else:
            if cash_rets is not None:
                portfolio *= (1 + cash_rets[i - 1])
            else:
                portfolio *= (1 + daily_rf)
            values.append(portfolio)
            if in_cool:
                cool_counter += 1
                if cool_counter >= cool_days or prices[i] >= ath:
                    enter_flag = True
                    in_cool = False

    return np.array(values)


def _prices_from_returns(rets):
    """Build price series from daily returns."""
    prices = np.zeros(len(rets) + 1)
    prices[0] = 100.0
    for i, r in enumerate(rets):
        prices[i + 1] = prices[i] * (1 + r)
        if prices[i + 1] <= 0:
            prices[i + 1:] = 0.001
            break
    return prices


def _compute_sim_metrics(strat_vals, daily_rf):
    """Compute bootstrap metrics for one simulation."""
    n_years = len(strat_vals) / TRADING_DAYS_PER_YEAR
    cagr = ((strat_vals[-1] / strat_vals[0]) ** (1.0 / n_years) - 1
            if n_years > 0 and strat_vals[-1] > 0 else 0)

    daily_rets = np.diff(strat_vals) / strat_vals[:-1]
    excess = daily_rets - daily_rf
    std_r = np.std(daily_rets, ddof=1)
    sharpe = (np.mean(excess) / std_r * np.sqrt(252)) if std_r > 0 else 0

    neg_excess = excess[excess < 0]
    ds_std = np.std(neg_excess, ddof=1) if len(neg_excess) > 1 else 0
    sortino = (np.mean(excess) / ds_std * np.sqrt(252)) if ds_std > 0 else 0

    cummax = np.maximum.accumulate(strat_vals)
    max_dd = abs((strat_vals / cummax - 1).min())

    return {
        "cagr": cagr,
        "sharpe": sharpe,
        "sortino": sortino,
        "max_dd": max_dd,
        "total_return": strat_vals[-1] / strat_vals[0] - 1,
    }


def block_bootstrap_paired(upro_rets, bond_rets, threshold=0.25,
                            cool_days=40, block_size=20, n_iter=10000,
                            seed=42, rf_annual=0.0):
    """Block bootstrap with paired (UPRO, bond) return blocks.

    Reshuffles 20-day blocks of UPRO returns, keeping bond returns paired
    with their corresponding UPRO block (preserving cross-asset correlation
    within each block). Re-runs the DD exit strategy on each shuffled UPRO
    path, using the paired bond returns during cash periods.
    """
    random.seed(seed)
    n = len(upro_rets)
    daily_rf = rf_annual / TRADING_DAYS_PER_YEAR

    # Build overlapping paired blocks
    bs = min(block_size, n)
    blocks = [(upro_rets[i:i + bs], bond_rets[i:i + bs])
              for i in range(n - bs + 1)]
    max_idx = len(blocks) - 1

    results = {"cagr": [], "sharpe": [], "sortino": [], "max_dd": [],
               "total_return": []}

    for _ in range(n_iter):
        upro_path = []
        bond_path = []
        while len(upro_path) < n:
            idx = random.randint(0, max_idx)
            ub, bb = blocks[idx]
            upro_path.extend(ub.tolist())
            bond_path.extend(bb.tolist())
        upro_path = np.array(upro_path[:n])
        bond_path = np.array(bond_path[:n])

        upro_prices = _prices_from_returns(upro_path)
        strat_vals = _run_dd_on_prices(
            upro_prices, threshold, cool_days,
            cash_rets=bond_path, daily_rf=daily_rf)

        m = _compute_sim_metrics(strat_vals, daily_rf)
        for k in results:
            results[k].append(m[k])

    return results


def block_bootstrap_tbill(upro_rets, threshold=0.25, cool_days=40,
                          block_size=20, n_iter=10000, seed=42,
                          rf_annual=0.0):
    """Block bootstrap for T-bill cash variant.

    Reshuffles 20-day blocks of UPRO returns and re-runs the DD exit
    strategy on each shuffled path. Cash earns constant T-bill rate.
    """
    random.seed(seed)
    n = len(upro_rets)
    daily_rf = rf_annual / TRADING_DAYS_PER_YEAR

    bs = min(block_size, n)
    blocks = [upro_rets[i:i + bs] for i in range(n - bs + 1)]
    max_idx = len(blocks) - 1

    results = {"cagr": [], "sharpe": [], "sortino": [], "max_dd": [],
               "total_return": []}

    for _ in range(n_iter):
        path = []
        while len(path) < n:
            idx = random.randint(0, max_idx)
            path.extend(blocks[idx].tolist())
        path = np.array(path[:n])

        upro_prices = _prices_from_returns(path)
        strat_vals = _run_dd_on_prices(
            upro_prices, threshold, cool_days,
            cash_rets=None, daily_rf=daily_rf)

        m = _compute_sim_metrics(strat_vals, daily_rf)
        for k in results:
            results[k].append(m[k])

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
    irx = data["IRX"]

    tbill_daily = get_daily_tbill_rate(irx)

    # Run three variants with next-open execution
    print("\nRunning DD25/Cool40 variants (next-open execution, excess Sharpe)...")

    # T-bill variant
    tb_dates, tb_vals, tb_trades, tb_days = run_dd_exit_tbill(upro_df, tbill_daily)
    tb_pct = tb_days / max(len(tb_vals) - 1, 1)
    tb_rf = avg_rf_annual(tbill_daily, tb_dates)
    tb_hist = compute_metrics(tb_vals, "DD25/Cool40+T-bill", tb_dates,
                              tb_trades, tb_pct, tb_rf)
    # TLT variant
    tlt_dates, tlt_vals, tlt_trades, tlt_days = run_dd_exit_bond(
        upro_df, tlt_df, tbill_daily, "TLT")
    tlt_pct = tlt_days / max(len(tlt_vals) - 1, 1)
    tlt_rf = avg_rf_annual(tbill_daily, tlt_dates)
    tlt_hist = compute_metrics(tlt_vals, "DD25/Cool40+TLT", tlt_dates,
                               tlt_trades, tlt_pct, tlt_rf)

    # TMF variant
    tmf_dates, tmf_vals, tmf_trades, tmf_days = run_dd_exit_bond(
        upro_df, tmf_df, tbill_daily, "TMF")
    tmf_pct = tmf_days / max(len(tmf_vals) - 1, 1)
    tmf_rf = avg_rf_annual(tbill_daily, tmf_dates)
    tmf_hist = compute_metrics(tmf_vals, "DD25/Cool40+TMF", tmf_dates,
                               tmf_trades, tmf_pct, tmf_rf)

    print(f"\n  T-bill: CAGR={tb_hist['cagr']:.1%}, Sharpe={tb_hist['sharpe']:.3f}, "
          f"MaxDD={tb_hist['max_dd']:.1%}")
    print(f"  TLT:    CAGR={tlt_hist['cagr']:.1%}, Sharpe={tlt_hist['sharpe']:.3f}, "
          f"MaxDD={tlt_hist['max_dd']:.1%}")
    print(f"  TMF:    CAGR={tmf_hist['cagr']:.1%}, Sharpe={tmf_hist['sharpe']:.3f}, "
          f"MaxDD={tmf_hist['max_dd']:.1%}")
    print(f"\n  Avg risk-free rate: {tb_rf:.2%}")

    # Compute raw market returns for bootstrap (not strategy returns)
    # T-bill: just UPRO close-to-close returns
    upro_close_all = upro_df["Close"].values
    upro_rets_raw = np.diff(upro_close_all) / upro_close_all[:-1]

    # TLT: paired UPRO + TLT returns on common dates
    common_tlt = upro_df.index.intersection(tlt_df.index)
    upro_c_tlt = upro_df.loc[common_tlt, "Close"].values
    tlt_c = tlt_df.loc[common_tlt, "Close"].values
    upro_rets_tlt = np.diff(upro_c_tlt) / upro_c_tlt[:-1]
    tlt_rets_raw = np.diff(tlt_c) / tlt_c[:-1]

    # TMF: paired UPRO + TMF returns on common dates
    common_tmf = upro_df.index.intersection(tmf_df.index)
    upro_c_tmf = upro_df.loc[common_tmf, "Close"].values
    tmf_c = tmf_df.loc[common_tmf, "Close"].values
    upro_rets_tmf = np.diff(upro_c_tmf) / upro_c_tmf[:-1]
    tmf_rets_raw = np.diff(tmf_c) / tmf_c[:-1]

    # Bootstrap: reshuffle raw market returns and re-run DD exit strategy
    print(f"\nRunning {n_iter:,} block-bootstrap iterations (block_size={block_size})...")
    print("  (reshuffling market returns & re-running strategy on each path)")

    tb_results = block_bootstrap_tbill(upro_rets_raw, threshold=DD_THRESHOLD,
                                        cool_days=COOL_DAYS, block_size=block_size,
                                        n_iter=n_iter, seed=42, rf_annual=tb_rf)
    tlt_results = block_bootstrap_paired(upro_rets_tlt, tlt_rets_raw,
                                          threshold=DD_THRESHOLD, cool_days=COOL_DAYS,
                                          block_size=block_size, n_iter=n_iter,
                                          seed=43, rf_annual=tlt_rf)
    tmf_results = block_bootstrap_paired(upro_rets_tmf, tmf_rets_raw,
                                          threshold=DD_THRESHOLD, cool_days=COOL_DAYS,
                                          block_size=block_size, n_iter=n_iter,
                                          seed=44, rf_annual=tmf_rf)

    print_report("DD25/Cool40 + T-bill Cash", tb_hist, tb_results, n_iter, block_size)
    print_report("DD25/Cool40 + TLT Cash", tlt_hist, tlt_results, n_iter, block_size)
    print_report("DD25/Cool40 + TMF Cash", tmf_hist, tmf_results, n_iter, block_size)

    # Comparative summary
    print(f"\n{'='*70}")
    print("  COMPARATIVE SUMMARY (next-open execution, excess Sharpe)")
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
