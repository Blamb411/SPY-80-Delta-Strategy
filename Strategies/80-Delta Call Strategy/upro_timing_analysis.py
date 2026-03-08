"""
UPRO Timing Analysis
====================
Tests 5 UPRO timing strategies against plain UPRO buy-and-hold.

Strategies:
  1. VIX-Based Regime Filter
  2. Dual Momentum (Antonacci-style)
  3. UPRO/TMF Rotation (HFEA)
  4. Drawdown-Triggered Exit
  5. Composite Signal (SMA200 + VIX + Momentum)

Period: UPRO inception (2009-06-25) through 2026-03-02
Initial capital: $100,000

Usage:
    python upro_timing_analysis.py
"""

import os
import pandas as pd
import numpy as np
import yfinance as yf
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime

# Force unbuffered output
import functools
print = functools.partial(print, flush=True)

_this_dir = os.path.dirname(os.path.abspath(__file__))

# ======================================================================
# PARAMETERS
# ======================================================================

INITIAL_CAPITAL = 100_000
UPRO_INCEPTION = "2009-06-25"
END_DATE = "2026-03-03"
# Start SPY/VIX data earlier for SMA200 warm-up
DATA_START = "2008-01-01"
TRADING_DAYS_PER_YEAR = 252

# Module-level risk-free rate, set from ^IRX in main()
_rf_annual = 0.0

# ======================================================================
# DATA LOADING
# ======================================================================

def load_all_data():
    """Load SPY, UPRO, TMF, TLT, and VIX data from yfinance."""
    print("Loading market data from yfinance...")

    tickers = {
        "SPY": DATA_START,
        "UPRO": UPRO_INCEPTION,
        "TMF": UPRO_INCEPTION,
        "TLT": DATA_START,
        "^VIX": DATA_START,
        "^IRX": DATA_START,
    }

    data = {}
    for ticker, start in tickers.items():
        print(f"  Downloading {ticker}...")
        df = yf.download(ticker, start=start, end=END_DATE, progress=False, auto_adjust=True)
        if df.empty:
            print(f"    WARNING: No data for {ticker}")
            data[ticker] = pd.DataFrame()
            continue

        # Handle multi-level columns from yfinance
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]

        # Find the Close column
        close_col = None
        for col in df.columns:
            if "Close" in str(col) or "close" in str(col):
                close_col = col
                break

        if close_col is None:
            print(f"    WARNING: No close column for {ticker}")
            data[ticker] = pd.DataFrame()
            continue

        result = pd.DataFrame(index=df.index)
        result["Close"] = df[close_col].values if hasattr(df[close_col], "values") else df[close_col]
        result.index.name = "Date"
        data[ticker] = result

        print(f"    {ticker}: {len(result)} days, {result.index[0].strftime('%Y-%m-%d')} to {result.index[-1].strftime('%Y-%m-%d')}")

    return data


# ======================================================================
# RISK-FREE RATE HELPERS
# ======================================================================

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


# ======================================================================
# METRICS COMPUTATION
# ======================================================================

def compute_metrics(portfolio_values, name="", num_trades=0, days_invested=0,
                    total_days=0, rf_annual=None):
    """Compute performance metrics for a portfolio value series.
    rf_annual: annualized risk-free rate (e.g. 0.02 for 2%) used for
               Sharpe, Sortino, and Calmar excess-return calculations.
               If None, uses the module-level _rf_annual (set from ^IRX in main).
    """
    if rf_annual is None:
        rf_annual = _rf_annual
    values = np.array(portfolio_values, dtype=float)
    if len(values) < 2 or values[0] <= 0:
        return {
            "name": name, "end_value": 0, "cagr": 0, "sharpe": 0,
            "sortino": 0, "calmar": 0, "max_dd": 0, "num_trades": 0,
            "pct_invested": 0, "cagr_invested": 0,
        }

    years = len(values) / TRADING_DAYS_PER_YEAR
    end_val = values[-1]
    cagr = (end_val / values[0]) ** (1 / years) - 1 if years > 0 and end_val > 0 else -1

    daily_rets = np.diff(values) / values[:-1]
    daily_rets = daily_rets[np.isfinite(daily_rets)]

    daily_rf = rf_annual / TRADING_DAYS_PER_YEAR
    excess_rets = daily_rets - daily_rf

    std_ret = np.std(daily_rets, ddof=1) if len(daily_rets) > 1 else 1e-10
    sharpe = (np.mean(excess_rets) / std_ret) * np.sqrt(TRADING_DAYS_PER_YEAR) if std_ret > 0 else 0

    neg_excess = excess_rets[excess_rets < 0]
    down_std = np.std(neg_excess, ddof=1) if len(neg_excess) > 1 else 1e-10
    sortino = (np.mean(excess_rets) / down_std) * np.sqrt(TRADING_DAYS_PER_YEAR) if down_std > 0 else 0

    cummax = np.maximum.accumulate(values)
    drawdown = values / cummax - 1
    max_dd = np.min(drawdown)

    # Calmar ratio (excess CAGR / |MaxDD|)
    excess_cagr = cagr - rf_annual
    calmar = excess_cagr / abs(max_dd) if max_dd != 0 else 0

    pct_invested = days_invested / total_days * 100 if total_days > 0 else 100.0

    # CAGR while invested
    if days_invested > 0 and end_val > 0:
        years_invested = days_invested / TRADING_DAYS_PER_YEAR
        total_return = end_val / values[0]
        cagr_invested = total_return ** (1 / years_invested) - 1 if years_invested > 0 else 0
    else:
        cagr_invested = 0

    return {
        "name": name,
        "end_value": end_val,
        "cagr": cagr,
        "sharpe": sharpe,
        "sortino": sortino,
        "calmar": calmar,
        "max_dd": max_dd,
        "num_trades": num_trades,
        "pct_invested": pct_invested,
        "cagr_invested": cagr_invested,
    }


# ======================================================================
# BENCHMARK: UPRO BUY & HOLD
# ======================================================================

def run_upro_bh(upro_close):
    """Plain UPRO buy-and-hold from inception."""
    prices = upro_close.values
    shares = INITIAL_CAPITAL / prices[0]
    portfolio_values = shares * prices
    dates = upro_close.index

    m = compute_metrics(
        portfolio_values, name="UPRO B&H",
        num_trades=1, days_invested=len(prices), total_days=len(prices),
    )
    return dates, portfolio_values, m


# ======================================================================
# LEVERAGE COMPARISON: SPY B&H vs SYNTHETIC 3x vs UPRO
# ======================================================================

def run_spy_bh(spy_close, upro_start_date):
    """Plain SPY buy-and-hold from UPRO inception date (unlevered baseline)."""
    spy = spy_close.loc[spy_close.index >= upro_start_date]
    prices = spy.values
    shares = INITIAL_CAPITAL / prices[0]
    portfolio_values = shares * prices
    dates = spy.index

    m = compute_metrics(
        portfolio_values, name="SPY B&H (1x)",
        num_trades=1, days_invested=len(prices), total_days=len(prices),
    )
    return dates, portfolio_values, m


def run_synthetic_3x(spy_close, upro_start_date, annual_margin_rate=0.0):
    """Synthetic 3x daily-rebalanced SPY (like UPRO but without expense ratio).

    Daily return = 3 * SPY_daily_return - daily_borrowing_cost
    Borrowing cost applies to the 2x borrowed portion.
    Set annual_margin_rate=0 for frictionless 3x, or ~0.06 for realistic margin.
    """
    spy = spy_close.loc[spy_close.index >= upro_start_date]
    prices = spy.values
    daily_rets = np.diff(prices) / prices[:-1]

    # Daily borrowing cost on the 2x borrowed portion
    daily_borrow = (annual_margin_rate * 2.0) / TRADING_DAYS_PER_YEAR

    portfolio_values = np.zeros(len(prices))
    portfolio_values[0] = INITIAL_CAPITAL

    for i in range(1, len(prices)):
        lev_return = 3.0 * daily_rets[i - 1] - daily_borrow
        portfolio_values[i] = portfolio_values[i - 1] * (1 + lev_return)
        # Floor at zero (margin call wipes you out)
        if portfolio_values[i] <= 0:
            portfolio_values[i:] = 0
            break

    dates = spy.index

    cost_label = "no cost" if annual_margin_rate == 0 else f"{annual_margin_rate:.0%} margin"
    name = f"Synthetic 3x ({cost_label})"

    m = compute_metrics(
        portfolio_values, name=name,
        num_trades=1, days_invested=len(prices), total_days=len(prices),
    )
    return dates, portfolio_values, m


def run_static_3x(spy_close, upro_start_date, annual_margin_rate=0.06,
                  maintenance_margin=0.25):
    """Static 3x leveraged SPY: $100K equity + $200K borrowed, buy & hold.

    Unlike UPRO or synthetic daily-rebalanced 3x, leverage is NOT reset daily.
    You buy $300K of SPY on day 1 and hold.  Leverage ratio drifts with price:
    it rises as SPY falls (making crashes worse) and falls as SPY rises.

    Margin call triggered when:
        equity < maintenance_margin * position_value
    where equity = position_value - debt.

    After a margin call the position is liquidated and the simulation
    continues flat at the remaining equity (which may be zero or negative,
    floored at zero).

    Parameters
    ----------
    annual_margin_rate : float
        Annual interest rate on the $200K borrowed portion (default 6%).
    maintenance_margin : float
        Reg-T maintenance margin requirement (default 25%).
    """
    spy = spy_close.loc[spy_close.index >= upro_start_date]
    prices = spy.values
    dates = spy.index

    initial_equity = INITIAL_CAPITAL          # $100K
    debt = initial_equity * 2.0               # $200K borrowed
    shares = (initial_equity + debt) / prices[0]  # buy $300K of SPY

    daily_interest = annual_margin_rate / TRADING_DAYS_PER_YEAR

    portfolio_values = np.zeros(len(prices))
    portfolio_values[0] = initial_equity
    margin_called = False
    margin_call_day = None

    for i in range(1, len(prices)):
        if margin_called:
            # Already liquidated — flat at whatever equity remained
            portfolio_values[i] = portfolio_values[i - 1]
            continue

        # Accrue daily interest on the debt
        debt *= (1.0 + daily_interest)

        # Current position and equity
        position_value = shares * prices[i]
        equity = position_value - debt

        # Check margin call
        if equity <= 0 or equity < maintenance_margin * position_value:
            # Forced liquidation: sell everything, repay debt
            remaining = max(0.0, position_value - debt)
            portfolio_values[i] = remaining
            margin_called = True
            margin_call_day = i
        else:
            portfolio_values[i] = equity

    name = f"Static 3x ({annual_margin_rate:.0%} margin)"
    m = compute_metrics(
        portfolio_values, name=name,
        num_trades=1, days_invested=len(prices), total_days=len(prices),
    )
    # Attach margin-call info as extra fields
    if margin_called:
        m["margin_call_day"] = int(margin_call_day)
        m["margin_call_date"] = str(dates[margin_call_day].date())
        m["days_to_margin_call"] = int(margin_call_day)
    else:
        m["margin_call_day"] = None

    return dates, portfolio_values, m


# ======================================================================
# STRATEGY 1: VIX-BASED REGIME FILTER
# ======================================================================

def run_vix_filter(upro_close, vix_close, threshold):
    """Hold UPRO when prior-day VIX < threshold, else cash."""
    # Align dates
    common = upro_close.index.intersection(vix_close.index)
    upro = upro_close.loc[common]
    vix = vix_close.loc[common]

    prices = upro.values
    vix_vals = vix.values

    portfolio = INITIAL_CAPITAL
    shares = 0.0
    invested = False
    values = []
    num_trades = 0
    days_invested = 0

    for i in range(len(prices)):
        if i == 0:
            # First day: check nothing, start in cash
            values.append(portfolio)
            continue

        # Use prior day's VIX for today's signal
        prior_vix = vix_vals[i - 1]
        want_in = prior_vix < threshold

        if want_in and not invested:
            # Buy UPRO
            shares = portfolio / prices[i]
            invested = True
            num_trades += 1
        elif not want_in and invested:
            # Sell UPRO
            portfolio = shares * prices[i]
            shares = 0.0
            invested = False

        if invested:
            values.append(shares * prices[i])
            days_invested += 1
        else:
            values.append(portfolio)

    name = f"VIX<{threshold}"
    m = compute_metrics(
        values, name=name,
        num_trades=num_trades, days_invested=days_invested,
        total_days=len(prices),
    )
    return upro.index, np.array(values), m


# ======================================================================
# STRATEGY 2: DUAL MOMENTUM (ANTONACCI-STYLE)
# ======================================================================

def run_dual_momentum(upro_close, spy_close, tlt_close):
    """
    Hold UPRO when:
      a) SPY 12-month return > 0 (absolute momentum)
      b) SPY 12-month return > TLT 12-month return (relative momentum)
    Else cash.
    Uses prior day's signal.
    """
    lookback = 252  # ~12 months

    # Align all three to common dates starting from UPRO inception
    common = upro_close.index.intersection(spy_close.index).intersection(tlt_close.index)
    upro = upro_close.loc[common]
    spy = spy_close.loc[common]
    tlt = tlt_close.loc[common]

    prices = upro.values
    spy_vals = spy.values
    tlt_vals = tlt.values

    portfolio = INITIAL_CAPITAL
    shares = 0.0
    invested = False
    values = []
    num_trades = 0
    days_invested = 0

    for i in range(len(prices)):
        if i < lookback:
            # Not enough history for signal, stay in cash
            if invested:
                portfolio = shares * prices[i]
                shares = 0.0
                invested = False
            values.append(portfolio if not invested else shares * prices[i])
            continue

        # Prior day's signals (use i-1 to avoid look-ahead)
        spy_ret_12m = (spy_vals[i - 1] / spy_vals[i - 1 - lookback]) - 1
        tlt_ret_12m = (tlt_vals[i - 1] / tlt_vals[i - 1 - lookback]) - 1

        abs_mom = spy_ret_12m > 0
        rel_mom = spy_ret_12m > tlt_ret_12m
        want_in = abs_mom and rel_mom

        if want_in and not invested:
            shares = portfolio / prices[i]
            invested = True
            num_trades += 1
        elif not want_in and invested:
            portfolio = shares * prices[i]
            shares = 0.0
            invested = False

        if invested:
            values.append(shares * prices[i])
            days_invested += 1
        else:
            values.append(portfolio)

    m = compute_metrics(
        values, name="Dual Momentum",
        num_trades=num_trades, days_invested=days_invested,
        total_days=len(prices),
    )
    return upro.index, np.array(values), m


# ======================================================================
# STRATEGY 3: UPRO/TMF ROTATION (HFEA)
# ======================================================================

def run_hfea(upro_close, tmf_close, upro_pct=0.55, tmf_pct=0.45, rebal_days=63):
    """
    Hold 55% UPRO + 45% TMF, rebalanced every 63 trading days (quarterly).
    """
    # Align dates
    common = upro_close.index.intersection(tmf_close.index)
    upro = upro_close.loc[common]
    tmf = tmf_close.loc[common]

    upro_prices = upro.values
    tmf_prices = tmf.values

    # Initial allocation
    upro_cash = INITIAL_CAPITAL * upro_pct
    tmf_cash = INITIAL_CAPITAL * tmf_pct

    upro_shares = upro_cash / upro_prices[0]
    tmf_shares = tmf_cash / tmf_prices[0]

    values = [INITIAL_CAPITAL]
    last_rebal = 0

    for i in range(1, len(upro_prices)):
        # Current values
        upro_val = upro_shares * upro_prices[i]
        tmf_val = tmf_shares * tmf_prices[i]
        total = upro_val + tmf_val

        # Rebalance check
        if (i - last_rebal) >= rebal_days:
            upro_shares = (total * upro_pct) / upro_prices[i]
            tmf_shares = (total * tmf_pct) / tmf_prices[i]
            last_rebal = i

        values.append(total)

    num_rebalances = (len(upro_prices) - 1) // rebal_days

    m = compute_metrics(
        values, name="HFEA 55/45",
        num_trades=num_rebalances,
        days_invested=len(upro_prices),
        total_days=len(upro_prices),
    )
    return upro.index, np.array(values), m


# ======================================================================
# STRATEGY 4: DRAWDOWN-TRIGGERED EXIT
# ======================================================================

def run_drawdown_exit(upro_close, dd_threshold, cooling_period):
    """
    Exit UPRO when it drops dd_threshold% from peak.
    Re-enter when UPRO makes new ATH or after cooling_period trading days.
    """
    prices = upro_close.values
    dates = upro_close.index

    portfolio = INITIAL_CAPITAL
    shares = INITIAL_CAPITAL / prices[0]
    invested = True
    values = [INITIAL_CAPITAL]
    num_trades = 1
    days_invested = 1

    peak_price = prices[0]
    days_in_cash = 0
    exit_price_peak = 0.0  # Track the ATH at time of exit

    for i in range(1, len(prices)):
        if invested:
            # Track peak price while invested
            if prices[i] > peak_price:
                peak_price = prices[i]

            # Check drawdown from peak
            current_dd = (prices[i] / peak_price) - 1
            if current_dd <= -dd_threshold:
                # Exit
                portfolio = shares * prices[i]
                shares = 0.0
                invested = False
                days_in_cash = 0
                exit_price_peak = peak_price
            else:
                days_invested += 1
                values.append(shares * prices[i])
                continue
        else:
            days_in_cash += 1

            # Re-enter conditions: new ATH OR cooling period elapsed
            new_ath = prices[i] > exit_price_peak
            cooling_done = days_in_cash >= cooling_period

            if new_ath or cooling_done:
                shares = portfolio / prices[i]
                invested = True
                num_trades += 1
                peak_price = prices[i]
                days_invested += 1
                values.append(shares * prices[i])
                continue

        # If we get here, we're in cash
        values.append(portfolio)

    name = f"DD{int(dd_threshold*100)}%/Cool{cooling_period}"
    m = compute_metrics(
        values, name=name,
        num_trades=num_trades, days_invested=days_invested,
        total_days=len(prices),
    )
    return dates, np.array(values), m


# ======================================================================
# STRATEGY 5: COMPOSITE SIGNAL (SMA200 + VIX + MOMENTUM)
# ======================================================================

def run_composite(upro_close, spy_close, vix_close, min_signals=2):
    """
    Three binary signals:
      a) SPY > SMA200 (trend)
      b) VIX < 25 (low fear)
      c) SPY 3-month return > 0 (momentum)
    Hold UPRO when at least min_signals agree.
    """
    sma_period = 200
    mom_period = 63  # ~3 months

    # Align dates
    common = upro_close.index.intersection(spy_close.index).intersection(vix_close.index)
    upro = upro_close.loc[common]
    spy = spy_close.loc[common]
    vix = vix_close.loc[common]

    prices = upro.values
    spy_vals = spy.values
    vix_vals = vix.values

    # Pre-compute SMA200 on SPY (use full SPY series for warm-up)
    spy_full = spy_close.copy()
    spy_sma200 = spy_full.rolling(window=sma_period).mean()
    # Map SMA200 to common dates
    sma_vals = spy_sma200.reindex(common).values

    portfolio = INITIAL_CAPITAL
    shares = 0.0
    invested = False
    values = []
    num_trades = 0
    days_invested = 0

    for i in range(len(prices)):
        # Need enough history for SMA200 and 3-month momentum
        min_warmup = max(sma_period, mom_period)
        if i < 1:
            # First day - stay in cash
            values.append(portfolio)
            continue

        # Prior day's signals (avoid look-ahead)
        idx = i - 1

        # Signal 1: SPY > SMA200
        sma_val = sma_vals[idx]
        if np.isnan(sma_val):
            signal_trend = False
        else:
            signal_trend = spy_vals[idx] > sma_val

        # Signal 2: VIX < 25
        signal_vix = vix_vals[idx] < 25

        # Signal 3: SPY 3-month return > 0
        if idx >= mom_period:
            spy_3m_ret = (spy_vals[idx] / spy_vals[idx - mom_period]) - 1
            signal_mom = spy_3m_ret > 0
        else:
            signal_mom = False

        num_agree = int(signal_trend) + int(signal_vix) + int(signal_mom)
        want_in = num_agree >= min_signals

        if want_in and not invested:
            shares = portfolio / prices[i]
            invested = True
            num_trades += 1
        elif not want_in and invested:
            portfolio = shares * prices[i]
            shares = 0.0
            invested = False

        if invested:
            values.append(shares * prices[i])
            days_invested += 1
        else:
            values.append(portfolio)

    suffix = "2of3" if min_signals == 2 else "3of3"
    name = f"Composite {suffix}"
    m = compute_metrics(
        values, name=name,
        num_trades=num_trades, days_invested=days_invested,
        total_days=len(prices),
    )
    return upro.index, np.array(values), m


# ======================================================================
# PRINTING HELPERS
# ======================================================================

def print_strategy_table(title, results):
    """Print a formatted results table for a strategy group."""
    print(f"\n{'=' * 110}")
    print(f"  {title}")
    print(f"{'=' * 110}")
    header = (
        f"  {'Variant':<22} {'End Value':>14} {'CAGR':>8} {'Sharpe':>8} "
        f"{'Sortino':>8} {'Calmar':>8} {'Max DD':>9} {'Trades':>7} {'%Invest':>8}"
    )
    print(header)
    print(f"  {'-' * 106}")

    for m in results:
        line = (
            f"  {m['name']:<22} ${m['end_value']:>12,.0f} {m['cagr']:>+7.1%} "
            f"{m['sharpe']:>8.2f} {m['sortino']:>8.2f} {m['calmar']:>8.2f} {m['max_dd']:>8.1%} "
            f"{m['num_trades']:>7} {m['pct_invested']:>7.1f}%"
        )
        print(line)


def print_final_comparison(all_best):
    """Print the best-of-each comparison."""
    print(f"\n{'#' * 110}")
    print(f"  BEST OF EACH STRATEGY vs UPRO BUY & HOLD")
    print(f"{'#' * 110}")
    header = (
        f"  {'Strategy':<26} {'End Value':>14} {'CAGR':>8} {'Sharpe':>8} "
        f"{'Sortino':>8} {'Calmar':>8} {'Max DD':>9} {'Trades':>7} {'%Invest':>8}"
    )
    print(header)
    print(f"  {'-' * 106}")

    for m in all_best:
        line = (
            f"  {m['name']:<26} ${m['end_value']:>12,.0f} {m['cagr']:>+7.1%} "
            f"{m['sharpe']:>8.2f} {m['sortino']:>8.2f} {m['calmar']:>8.2f} {m['max_dd']:>8.1%} "
            f"{m['num_trades']:>7} {m['pct_invested']:>7.1f}%"
        )
        print(line)
    print()


# ======================================================================
# CHARTING
# ======================================================================

def create_charts(benchmark_dates, benchmark_values, benchmark_metrics,
                  best_strategies, all_results, output_path):
    """
    Create a 2x2 multi-panel chart:
      Top-left:    Portfolio value (log) - UPRO B&H vs best of each strategy
      Top-right:   Drawdown comparison
      Bottom-left: CAGR bar chart for all strategies
      Bottom-right: Risk/return scatter (Max DD vs CAGR)
    """
    print("\nCreating charts...")

    fig, axes = plt.subplots(2, 2, figsize=(18, 13))
    fig.suptitle("UPRO Timing Strategy Analysis (2009-2026)", fontsize=16, fontweight="bold", y=0.98)

    colors = [
        "#1f77b4",  # UPRO B&H
        "#d62728",  # VIX
        "#2ca02c",  # Dual Momentum
        "#ff7f0e",  # HFEA
        "#9467bd",  # Drawdown Exit
        "#8c564b",  # Composite
    ]

    # ---- TOP LEFT: Portfolio Value (log scale) ----
    ax1 = axes[0, 0]
    ax1.semilogy(benchmark_dates, benchmark_values, label="UPRO B&H",
                 linewidth=2.5, color=colors[0], alpha=0.9)

    for idx, (label, dates, values, _) in enumerate(best_strategies):
        ax1.semilogy(dates, values, label=label,
                     linewidth=1.8, color=colors[idx + 1], alpha=0.85)

    ax1.set_title("Portfolio Value ($100K, Log Scale)", fontweight="bold", fontsize=11)
    ax1.set_ylabel("Portfolio Value ($)")
    ax1.legend(loc="upper left", fontsize=7.5, framealpha=0.9)
    ax1.grid(True, alpha=0.3, which="both")
    ax1.xaxis.set_major_locator(mdates.YearLocator(2))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax1.tick_params(axis="x", rotation=30)

    # ---- TOP RIGHT: Drawdown Comparison ----
    ax2 = axes[0, 1]

    # Benchmark drawdown
    bm_cummax = np.maximum.accumulate(benchmark_values)
    bm_dd = (benchmark_values / bm_cummax - 1) * 100
    ax2.fill_between(benchmark_dates, bm_dd, 0, alpha=0.3, color=colors[0], label="UPRO B&H")

    for idx, (label, dates, values, _) in enumerate(best_strategies):
        vals = np.array(values)
        cm = np.maximum.accumulate(vals)
        dd = (vals / cm - 1) * 100
        ax2.plot(dates, dd, label=label, linewidth=1.2, color=colors[idx + 1], alpha=0.8)

    ax2.set_title("Drawdown Comparison", fontweight="bold", fontsize=11)
    ax2.set_ylabel("Drawdown (%)")
    ax2.legend(loc="lower left", fontsize=7, framealpha=0.9)
    ax2.grid(True, alpha=0.3)
    ax2.xaxis.set_major_locator(mdates.YearLocator(2))
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax2.tick_params(axis="x", rotation=30)

    # ---- BOTTOM LEFT: CAGR Bar Chart ----
    ax3 = axes[1, 0]

    # Collect all strategy variants plus benchmark
    bar_names = ["UPRO B&H"] + [m["name"] for m in all_results]
    bar_cagrs = [benchmark_metrics["cagr"] * 100] + [m["cagr"] * 100 for m in all_results]

    # Color bars: benchmark blue, others by strategy group
    bar_colors = ["#1f77b4"]
    strategy_color_map = {}
    color_idx = 1
    for m in all_results:
        name = m["name"]
        # Determine strategy group
        if "VIX<" in name:
            grp = "VIX"
        elif "Dual" in name:
            grp = "Dual"
        elif "HFEA" in name:
            grp = "HFEA"
        elif "DD" in name:
            grp = "DD"
        elif "Composite" in name:
            grp = "Comp"
        else:
            grp = "Other"

        if grp not in strategy_color_map:
            strategy_color_map[grp] = colors[min(color_idx, len(colors) - 1)]
            color_idx += 1
        bar_colors.append(strategy_color_map[grp])

    y_pos = np.arange(len(bar_names))
    ax3.barh(y_pos, bar_cagrs, color=bar_colors, alpha=0.8, height=0.7)
    ax3.set_yticks(y_pos)
    ax3.set_yticklabels(bar_names, fontsize=7)
    ax3.set_xlabel("CAGR (%)")
    ax3.set_title("CAGR Comparison (All Variants)", fontweight="bold", fontsize=11)
    ax3.axvline(x=benchmark_metrics["cagr"] * 100, color="#1f77b4", linestyle="--", alpha=0.5)
    ax3.grid(True, alpha=0.3, axis="x")
    ax3.invert_yaxis()

    # ---- BOTTOM RIGHT: Risk/Return Scatter ----
    ax4 = axes[1, 1]

    # Plot benchmark
    ax4.scatter(abs(benchmark_metrics["max_dd"]) * 100, benchmark_metrics["cagr"] * 100,
                s=200, color=colors[0], marker="*", zorder=5, label="UPRO B&H")
    ax4.annotate("UPRO B&H",
                 (abs(benchmark_metrics["max_dd"]) * 100, benchmark_metrics["cagr"] * 100),
                 textcoords="offset points", xytext=(8, 5), fontsize=7, fontweight="bold")

    # Plot all variants
    for m in all_results:
        name = m["name"]
        if "VIX<" in name:
            grp_color = colors[1]
        elif "Dual" in name:
            grp_color = colors[2]
        elif "HFEA" in name:
            grp_color = colors[3]
        elif "DD" in name:
            grp_color = colors[4]
        elif "Composite" in name:
            grp_color = colors[5]
        else:
            grp_color = "#333333"

        ax4.scatter(abs(m["max_dd"]) * 100, m["cagr"] * 100,
                    s=80, color=grp_color, alpha=0.7, zorder=3)
        ax4.annotate(name, (abs(m["max_dd"]) * 100, m["cagr"] * 100),
                     textcoords="offset points", xytext=(5, 3), fontsize=6)

    ax4.set_xlabel("Max Drawdown (%)")
    ax4.set_ylabel("CAGR (%)")
    ax4.set_title("Risk vs Return (All Variants)", fontweight="bold", fontsize=11)
    ax4.grid(True, alpha=0.3)

    # Add a legend for strategy groups
    from matplotlib.lines import Line2D
    legend_elements = [
        Line2D([0], [0], marker="*", color="w", markerfacecolor=colors[0], markersize=12, label="UPRO B&H"),
        Line2D([0], [0], marker="o", color="w", markerfacecolor=colors[1], markersize=8, label="VIX Filter"),
        Line2D([0], [0], marker="o", color="w", markerfacecolor=colors[2], markersize=8, label="Dual Momentum"),
        Line2D([0], [0], marker="o", color="w", markerfacecolor=colors[3], markersize=8, label="HFEA"),
        Line2D([0], [0], marker="o", color="w", markerfacecolor=colors[4], markersize=8, label="DD Exit"),
        Line2D([0], [0], marker="o", color="w", markerfacecolor=colors[5], markersize=8, label="Composite"),
    ]
    ax4.legend(handles=legend_elements, loc="lower left", fontsize=7)

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Chart saved to: {output_path}")


# ======================================================================
# CSV OUTPUT
# ======================================================================

def save_results_csv(benchmark_metrics, all_results, output_path):
    """Save all results to a CSV file."""
    rows = [benchmark_metrics] + all_results
    df = pd.DataFrame(rows)
    df = df.rename(columns={
        "name": "Strategy",
        "end_value": "End Value",
        "cagr": "CAGR",
        "sharpe": "Sharpe",
        "sortino": "Sortino",
        "calmar": "Calmar",
        "max_dd": "Max Drawdown",
        "num_trades": "Num Trades",
        "pct_invested": "Pct Invested",
        "cagr_invested": "CAGR While Invested",
    })
    df.to_csv(output_path, index=False, float_format="%.6f")
    print(f"Results CSV saved to: {output_path}")


# ======================================================================
# MAIN
# ======================================================================

def main():
    print("=" * 110)
    print("  UPRO TIMING ANALYSIS")
    print("  Testing 5 timing strategies vs UPRO Buy & Hold")
    print(f"  Period: {UPRO_INCEPTION} to {END_DATE} | Initial Capital: ${INITIAL_CAPITAL:,}")
    print("=" * 110)

    # Load data
    data = load_all_data()

    spy_close = data["SPY"]["Close"]
    upro_close = data["UPRO"]["Close"]
    vix_close = data["^VIX"]["Close"]
    tlt_close = data["TLT"]["Close"]
    tmf_close = data["TMF"]["Close"]
    irx_close = data["^IRX"]["Close"] if "^IRX" in data and not data["^IRX"].empty else None

    # Ensure all series have DatetimeIndex
    for name, series in [("SPY", spy_close), ("UPRO", upro_close),
                         ("VIX", vix_close), ("TLT", tlt_close), ("TMF", tmf_close)]:
        if not isinstance(series.index, pd.DatetimeIndex):
            series.index = pd.to_datetime(series.index)

    # Compute risk-free rate from ^IRX (13-week T-bill yield)
    global _rf_annual
    if irx_close is not None and len(irx_close) > 0:
        tbill_daily = get_daily_tbill_rate(irx_close)
        _rf_annual = avg_rf_annual(tbill_daily, upro_close.index)
        print(f"  Risk-free rate (avg ^IRX over UPRO period): {_rf_annual:.2%} annualized")
    else:
        _rf_annual = 0.0
        print("  WARNING: ^IRX data not available, using 0% risk-free rate")

    # ---- BENCHMARK ----
    print("\n" + "-" * 80)
    print("  BENCHMARK: UPRO Buy & Hold")
    print("-" * 80)
    bm_dates, bm_values, bm_metrics = run_upro_bh(upro_close)
    print(f"  End Value: ${bm_metrics['end_value']:,.0f} | CAGR: {bm_metrics['cagr']:+.1%} | "
          f"Sharpe: {bm_metrics['sharpe']:.2f} | Max DD: {bm_metrics['max_dd']:.1%}")

    all_results = []
    all_curves = {}       # name -> (dates, values)
    best_per_strategy = []  # (label, dates, values, metrics) for best variant per strategy

    # ==================================================================
    # LEVERAGE COMPARISON: SPY B&H vs Synthetic 3x vs UPRO
    # ==================================================================
    print("\n" + "=" * 80)
    print("  LEVERAGE COMPARISON: SPY vs Synthetic 3x vs UPRO")
    print("=" * 80)

    spy_dates, spy_values, spy_metrics = run_spy_bh(spy_close, upro_close.index[0])
    s3_dates, s3_values, s3_metrics = run_synthetic_3x(spy_close, upro_close.index[0], annual_margin_rate=0.0)
    s3m_dates, s3m_values, s3m_metrics = run_synthetic_3x(spy_close, upro_close.index[0], annual_margin_rate=0.06)
    stat_dates, stat_values, stat_metrics = run_static_3x(spy_close, upro_close.index[0], annual_margin_rate=0.06)

    lev_results = [spy_metrics, s3_metrics, s3m_metrics, stat_metrics, bm_metrics]
    print_strategy_table("LEVERAGE COMPARISON", lev_results)

    # Report margin call info for static 3x
    if stat_metrics.get("margin_call_day") is not None:
        print(f"\n  ** Static 3x MARGIN CALL on {stat_metrics['margin_call_date']} "
              f"(day {stat_metrics['days_to_margin_call']}) **")

    # Add to curves for charting
    all_curves[spy_metrics["name"]] = (spy_dates, spy_values)
    all_curves[s3_metrics["name"]] = (s3_dates, s3_values)
    all_curves[s3m_metrics["name"]] = (s3m_dates, s3m_values)
    all_curves[stat_metrics["name"]] = (stat_dates, stat_values)

    # Store leverage comparison metrics for final output
    leverage_comparison = [spy_metrics, s3_metrics, s3m_metrics, stat_metrics, bm_metrics]

    # ==================================================================
    # STRATEGY 1: VIX-BASED REGIME FILTER
    # ==================================================================
    print("\n" + "=" * 80)
    print("  STRATEGY 1: VIX-Based Regime Filter")
    print("=" * 80)

    vix_results = []
    vix_curves = {}
    for thresh in [15, 20, 25, 30]:
        dates, values, m = run_vix_filter(upro_close, vix_close, thresh)
        vix_results.append(m)
        all_results.append(m)
        vix_curves[m["name"]] = (dates, values)
        all_curves[m["name"]] = (dates, values)

    print_strategy_table("STRATEGY 1: VIX-Based Regime Filter", vix_results)

    # Best VIX variant by Sharpe
    best_vix = max(vix_results, key=lambda x: x["sharpe"])
    bv_dates, bv_values = vix_curves[best_vix["name"]]
    best_per_strategy.append((f"VIX ({best_vix['name']})", bv_dates, bv_values, best_vix))

    # ==================================================================
    # STRATEGY 2: DUAL MOMENTUM
    # ==================================================================
    print("\n" + "=" * 80)
    print("  STRATEGY 2: Dual Momentum (Antonacci-style)")
    print("=" * 80)

    dm_dates, dm_values, dm_metrics = run_dual_momentum(upro_close, spy_close, tlt_close)
    all_results.append(dm_metrics)
    all_curves[dm_metrics["name"]] = (dm_dates, dm_values)
    print_strategy_table("STRATEGY 2: Dual Momentum", [dm_metrics])
    best_per_strategy.append(("Dual Momentum", dm_dates, dm_values, dm_metrics))

    # ==================================================================
    # STRATEGY 3: HFEA (UPRO/TMF)
    # ==================================================================
    print("\n" + "=" * 80)
    print("  STRATEGY 3: UPRO/TMF Rotation (HFEA 55/45)")
    print("=" * 80)

    hfea_dates, hfea_values, hfea_metrics = run_hfea(upro_close, tmf_close)
    all_results.append(hfea_metrics)
    all_curves[hfea_metrics["name"]] = (hfea_dates, hfea_values)
    print_strategy_table("STRATEGY 3: HFEA 55/45", [hfea_metrics])
    best_per_strategy.append(("HFEA 55/45", hfea_dates, hfea_values, hfea_metrics))

    # ==================================================================
    # STRATEGY 4: DRAWDOWN-TRIGGERED EXIT
    # ==================================================================
    print("\n" + "=" * 80)
    print("  STRATEGY 4: Drawdown-Triggered Exit")
    print("=" * 80)

    dd_results = []
    dd_curves = {}
    for dd_thresh in [0.10, 0.15, 0.20, 0.25]:
        for cool in [20, 40, 60]:
            dates, values, m = run_drawdown_exit(upro_close, dd_thresh, cool)
            dd_results.append(m)
            all_results.append(m)
            dd_curves[m["name"]] = (dates, values)
            all_curves[m["name"]] = (dates, values)

    print_strategy_table("STRATEGY 4: Drawdown-Triggered Exit", dd_results)

    # Best DD variant by Sharpe
    best_dd = max(dd_results, key=lambda x: x["sharpe"])
    bd_dates, bd_values = dd_curves[best_dd["name"]]
    best_per_strategy.append((f"DD Exit ({best_dd['name']})", bd_dates, bd_values, best_dd))

    # ==================================================================
    # STRATEGY 5: COMPOSITE SIGNAL
    # ==================================================================
    print("\n" + "=" * 80)
    print("  STRATEGY 5: Composite Signal (SMA200 + VIX + Momentum)")
    print("=" * 80)

    comp_results = []
    comp_curves = {}
    for min_sig in [2, 3]:
        dates, values, m = run_composite(upro_close, spy_close, vix_close, min_signals=min_sig)
        comp_results.append(m)
        all_results.append(m)
        comp_curves[m["name"]] = (dates, values)
        all_curves[m["name"]] = (dates, values)

    print_strategy_table("STRATEGY 5: Composite Signal", comp_results)

    # Best composite by Sharpe
    best_comp = max(comp_results, key=lambda x: x["sharpe"])
    bc_dates, bc_values = comp_curves[best_comp["name"]]
    best_per_strategy.append((f"Composite ({best_comp['name']})", bc_dates, bc_values, best_comp))

    # ==================================================================
    # FINAL COMPARISON
    # ==================================================================
    final_best = [bm_metrics] + [item[3] for item in best_per_strategy]
    # Rename for clarity
    for i, item in enumerate(best_per_strategy):
        final_best[i + 1] = dict(final_best[i + 1])
        final_best[i + 1]["name"] = item[0]

    print_final_comparison(final_best)

    # ==================================================================
    # CHARTS & CSV
    # ==================================================================
    chart_path = os.path.join(_this_dir, "upro_timing_analysis.png")
    csv_path = os.path.join(_this_dir, "upro_timing_results.csv")

    create_charts(bm_dates, bm_values, bm_metrics,
                  best_per_strategy, all_results, chart_path)
    save_results_csv(bm_metrics, all_results, csv_path)

    print("\n" + "=" * 110)
    print("  ANALYSIS COMPLETE")
    print("=" * 110)


if __name__ == "__main__":
    main()
