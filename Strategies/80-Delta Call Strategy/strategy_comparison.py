"""
Apples-to-Apples Comparison: 80-Delta Options-Only vs UPRO DD25/Cool40
======================================================================

Part A: 2015-2026 (actual ThetaData options vs actual UPRO)
Part B: 2009-2026 (synthetic B-S options vs actual UPRO)

Usage:
    python -u strategy_comparison.py
"""

import os
import sys
import math
from datetime import datetime, timedelta
from collections import defaultdict

import numpy as np
import pandas as pd
import yfinance as yf

_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_dir = os.path.dirname(os.path.dirname(_this_dir))
sys.path.insert(0, _project_dir)

from backtest.black_scholes import black_scholes_price, find_strike_for_delta

# ======================================================================
# CONSTANTS
# ======================================================================
INITIAL_CAPITAL = 100_000
DATA_END = "2026-03-03"
TRADING_DAYS_PER_YEAR = 252

# Part A dates
PART_A_START = "2015-03-01"

# Part B dates (UPRO inception)
PART_B_DATA_START = "2008-01-01"   # SMA200 warmup
PART_B_SIM_START = "2009-06-23"    # UPRO inception

# Synthetic options parameters
DELTA = 0.80
DTE_TARGET = 120
DTE_MIN = 90
DTE_MAX = 150
MH = 60           # max hold in trading days
PT = 0.50          # +50% profit target
RATE = 0.04        # risk-free rate
SMA_EXIT_THRESHOLD = 0.02
SHARES = 999_999   # effectively uncapped


# ======================================================================
# HELPERS
# ======================================================================

def compute_metrics(values, dates=None):
    """Compute CAGR, Sharpe, Sortino, MaxDD, Calmar, worst 12m."""
    vals = np.array(values, dtype=float)
    n_years = len(vals) / TRADING_DAYS_PER_YEAR
    cagr = (vals[-1] / vals[0]) ** (1.0 / n_years) - 1 if n_years > 0 else 0

    daily_rets = np.diff(vals) / vals[:-1]
    mu = np.mean(daily_rets)
    sd = np.std(daily_rets)
    sharpe = mu / sd * np.sqrt(252) if sd > 0 else 0

    neg = daily_rets[daily_rets < 0]
    ds = np.std(neg) if len(neg) > 0 else 0
    sortino = mu / ds * np.sqrt(252) if ds > 0 else 0

    cummax = np.maximum.accumulate(vals)
    dd = vals / cummax - 1
    max_dd = float(dd.min())

    calmar = cagr / abs(max_dd) if max_dd != 0 else 0

    worst_12m = 0.0
    if len(vals) > 252:
        r12 = vals[252:] / vals[:-252] - 1
        worst_12m = float(r12.min())

    return {
        "end_value": vals[-1],
        "cagr": cagr,
        "sharpe": sharpe,
        "sortino": sortino,
        "max_dd": max_dd,
        "calmar": calmar,
        "worst_12m": worst_12m,
    }


def yearly_returns(values, dates):
    """Compute year-by-year returns from daily values & dates."""
    df = pd.DataFrame({"date": dates, "val": values})
    df["year"] = pd.to_datetime(df["date"]).dt.year
    result = {}
    for year, g in df.groupby("year"):
        result[year] = g["val"].iloc[-1] / g["val"].iloc[0] - 1
    return result


# ======================================================================
# UPRO DD25/Cool40 — reimplemented inline
# ======================================================================

def run_upro_dd25(start_date):
    """Run UPRO DD25/Cool40 from start_date. Returns (dates, values, metrics)."""
    # Download from before start_date to ensure coverage
    dl_start = "2008-01-01"
    print(f"  Downloading UPRO & IRX (from {dl_start} to {DATA_END})...")
    upro = yf.download("UPRO", start=dl_start, end=DATA_END, progress=False,
                       auto_adjust=True, multi_level_index=False)
    irx = yf.download("^IRX", start=dl_start, end=DATA_END, progress=False,
                      auto_adjust=True, multi_level_index=False)
    upro = upro[["Open", "Close"]].dropna()
    irx_close = irx["Close"].squeeze().dropna()

    # T-bill daily rate
    tbill_daily = (1 + irx_close / 100) ** (1 / 252) - 1

    # Slice to start_date
    upro = upro.loc[upro.index >= start_date]
    if len(upro) == 0:
        print("ERROR: No UPRO data after", start_date)
        return None, None, None

    dates = upro.index
    upro_open = upro["Open"].values
    upro_close = upro["Close"].values
    tbill = tbill_daily.reindex(dates).fillna(0).values

    # DD25/Cool40 logic (same as build_docx.py:run_dd_exit)
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

    for i in range(1, len(upro_close)):
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

        if invested:
            val = shares * upro_close[i]
            values.append(val)
            ath = max(ath, upro_close[i])
            dd = upro_close[i] / ath - 1
            if dd < -0.25:
                exit_signal = True
                trades += 1
        else:
            portfolio *= (1 + tbill[i])
            values.append(portfolio)
            if in_cool:
                cool_counter += 1
                if cool_counter >= 40 or upro_close[i] >= ath:
                    enter_signal = True
                    in_cool = False
                    trades += 1

    values = np.array(values)
    date_strs = [d.strftime("%Y-%m-%d") for d in dates]
    metrics = compute_metrics(values, date_strs)
    metrics["trades"] = trades
    return date_strs, values, metrics


# ======================================================================
# SPY B&H
# ======================================================================

def run_spy_bh(start_date):
    """SPY Buy & Hold from start_date."""
    print(f"  Downloading SPY ({start_date} to {DATA_END})...")
    spy = yf.download("SPY", start=start_date, end=DATA_END, progress=False,
                      auto_adjust=True, multi_level_index=False)
    spy = spy[["Close"]].dropna()
    closes = spy["Close"].values
    values = (INITIAL_CAPITAL / closes[0]) * closes
    dates = [d.strftime("%Y-%m-%d") for d in spy.index]
    return dates, values, compute_metrics(values, dates)


# ======================================================================
# SYNTHETIC 80-DELTA OPTIONS-ONLY BACKTEST
# ======================================================================

def generate_monthly_expirations(start_date, end_date):
    """3rd Friday of each month."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    exps = []
    cur = start.replace(day=1)
    while cur <= end:
        first_day = cur.replace(day=1)
        days_until_friday = (4 - first_day.weekday()) % 7
        third_friday = first_day + timedelta(days=days_until_friday + 14)
        if start <= third_friday <= end:
            exps.append((third_friday.strftime("%Y-%m-%d"), third_friday.date()))
        if cur.month == 12:
            cur = cur.replace(year=cur.year + 1, month=1)
        else:
            cur = cur.replace(month=cur.month + 1)
    return exps


def calculate_delta(spot, strike, dte, iv=0.16, rate=RATE):
    """B-S call delta."""
    if dte <= 0:
        return 1.0 if spot > strike else (0.5 if spot == strike else 0.0)
    t = dte / 365.0
    d1 = (math.log(spot / strike) + (rate + 0.5 * iv ** 2) * t) / (iv * math.sqrt(t))
    return 0.5 * (1.0 + math.erf(d1 / math.sqrt(2.0)))


def synthetic_option_price(spot, strike, dte, iv, rate=RATE):
    """B-S call mid price."""
    if dte <= 0:
        return max(0, spot - strike)
    t = dte / 365.0
    price = black_scholes_price(spot, strike, t, rate, iv, "C")
    return price if price is not None else max(0, spot - strike)


def apply_spread(mid, spread_pct=0.02):
    """2% synthetic bid-ask."""
    half = spread_pct / 2
    return max(0.01, mid * (1 - half)), max(0.01, mid * (1 + half))


def find_best_exp(entry_str, monthly_exps):
    """Closest monthly exp to DTE_TARGET within DTE_MIN..DTE_MAX."""
    entry_dt = datetime.strptime(entry_str, "%Y-%m-%d").date()
    best_exp, best_dte, best_diff = None, 0, 9999
    for exp_str, exp_dt in monthly_exps:
        dte = (exp_dt - entry_dt).days
        if dte < DTE_MIN or dte > DTE_MAX:
            continue
        diff = abs(dte - DTE_TARGET)
        if diff < best_diff:
            best_diff, best_exp, best_dte = diff, exp_str, dte
    return best_exp, best_dte


def generate_strikes(spot, step=5.0, n=40):
    """Synthetic strike grid around ATM."""
    atm = round(spot / step) * step
    return sorted([atm + i * step for i in range(-n // 2, n // 2 + 1) if atm + i * step > 0])


def run_synthetic_options(sim_start, data_start=PART_B_DATA_START):
    """Run synthetic 80-delta options-only backtest. Returns (dates, values, metrics, trade_log)."""
    print(f"  Downloading SPY & VIX ({data_start} to {DATA_END})...")
    spy_raw = yf.download("SPY", start=data_start, end=DATA_END, progress=False,
                          auto_adjust=True, multi_level_index=False)
    vix_raw = yf.download("^VIX", start=data_start, end=DATA_END, progress=False,
                          auto_adjust=True, multi_level_index=False)

    if isinstance(spy_raw.columns, pd.MultiIndex):
        spy_raw.columns = spy_raw.columns.get_level_values(0)
    if isinstance(vix_raw.columns, pd.MultiIndex):
        vix_raw.columns = vix_raw.columns.get_level_values(0)

    spy_by_date = {}
    for idx, row in spy_raw.iterrows():
        d = idx.strftime("%Y-%m-%d")
        spy_by_date[d] = {"close": float(row["Close"]), "open": float(row["Open"])}

    vix_data = {}
    for idx, row in vix_raw.iterrows():
        vix_data[idx.strftime("%Y-%m-%d")] = float(row["Close"])

    trading_dates = sorted(spy_by_date.keys())

    # SMA200
    sma200 = {}
    for i in range(199, len(trading_dates)):
        window = [spy_by_date[trading_dates[j]]["close"] for j in range(i - 199, i + 1)]
        sma200[trading_dates[i]] = sum(window) / 200.0

    monthly_exps = generate_monthly_expirations(data_start, DATA_END)

    # Simulation
    options_cash = float(INITIAL_CAPITAL)
    pending_cash = 0.0
    positions = []
    trade_log = []
    snapshots = []

    start_idx = next((i for i, d in enumerate(trading_dates) if d >= sim_start), 0)

    print(f"  Running synthetic backtest {trading_dates[start_idx]} to {trading_dates[-1]}...")

    for day_idx in range(start_idx, len(trading_dates)):
        today = trading_dates[day_idx]
        spot = spy_by_date[today]["close"]
        sma_val = sma200.get(today)
        above_sma = (spot > sma_val) if sma_val else False

        vix_close = vix_data.get(today, 20.0)
        iv_est = max(0.08, min(0.90, vix_close / 100.0 * 0.95))

        # Settle pending cash
        options_cash += pending_cash
        pending_cash = 0.0

        # Force-exit below SMA
        pct_below = (sma_val - spot) / sma_val if sma_val and sma_val > 0 else 0
        if pct_below >= SMA_EXIT_THRESHOLD and positions:
            for pos in positions:
                dte = (datetime.strptime(pos["expiration"], "%Y-%m-%d").date() -
                       datetime.strptime(today, "%Y-%m-%d").date()).days
                mid = synthetic_option_price(spot, pos["strike"], dte, iv_est)
                bid, _ = apply_spread(mid)
                proceeds = bid * 100 * pos["quantity"]
                pending_cash += proceeds
                trade_log.append({
                    "entry_date": pos["entry_date"], "exit_date": today,
                    "pnl_pct": bid / pos["entry_price"] - 1,
                    "exit_reason": "SMA",
                })
            positions = []

        # Normal exits (PT / MH)
        still_open = []
        for pos in positions:
            pos["days_held"] += 1
            dte = (datetime.strptime(pos["expiration"], "%Y-%m-%d").date() -
                   datetime.strptime(today, "%Y-%m-%d").date()).days
            mid = synthetic_option_price(spot, pos["strike"], dte, iv_est)
            bid, _ = apply_spread(mid)
            pnl_pct = bid / pos["entry_price"] - 1

            exit_reason = None
            if pnl_pct >= PT:
                exit_reason = "PT"
            elif pos["days_held"] >= MH:
                exit_reason = "MH"

            if exit_reason:
                proceeds = bid * 100 * pos["quantity"]
                pending_cash += proceeds
                trade_log.append({
                    "entry_date": pos["entry_date"], "exit_date": today,
                    "pnl_pct": pnl_pct, "exit_reason": exit_reason,
                })
            else:
                still_open.append(pos)
        positions = still_open

        # Current delta
        current_delta = 0.0
        for pos in positions:
            dte = (datetime.strptime(pos["expiration"], "%Y-%m-%d").date() -
                   datetime.strptime(today, "%Y-%m-%d").date()).days
            current_delta += calculate_delta(spot, pos["strike"], dte, iv_est) * pos["quantity"] * 100

        # Entry
        delta_room = SHARES - current_delta
        if above_sma and sma_val is not None and delta_room > 80:
            best_exp, dte_cal = find_best_exp(today, monthly_exps)
            if best_exp:
                t_years = dte_cal / 365.0
                bs_strike = find_strike_for_delta(spot, t_years, RATE, iv_est, DELTA, "C")
                if bs_strike:
                    strikes = generate_strikes(spot)
                    real_strike = min(strikes, key=lambda s: abs(s - bs_strike))
                    mid = synthetic_option_price(spot, real_strike, dte_cal, iv_est)
                    _, ask = apply_spread(mid)
                    if ask > 0.01:
                        option_delta = calculate_delta(spot, real_strike, dte_cal, iv_est)
                        max_by_delta = int(delta_room / (option_delta * 100))
                        contract_cost = ask * 100
                        max_by_cash = int(options_cash / contract_cost)
                        qty = min(max_by_delta, max_by_cash, 1)
                        if qty > 0:
                            total_cost = contract_cost * qty
                            options_cash -= total_cost
                            positions.append({
                                "entry_date": today, "expiration": best_exp,
                                "strike": real_strike, "entry_price": ask,
                                "quantity": qty, "contract_cost": total_cost,
                                "days_held": 0, "entry_delta": option_delta,
                            })

        # Mark to market
        positions_value = 0.0
        for pos in positions:
            dte = (datetime.strptime(pos["expiration"], "%Y-%m-%d").date() -
                   datetime.strptime(today, "%Y-%m-%d").date()).days
            mid = synthetic_option_price(spot, pos["strike"], dte, iv_est)
            positions_value += mid * 100 * pos["quantity"]

        total_value = options_cash + pending_cash + positions_value
        snapshots.append({"date": today, "value": total_value})

        # Progress
        real_idx = day_idx - start_idx
        total_days = len(trading_dates) - start_idx
        if (real_idx + 1) % 1000 == 0 or real_idx == 0:
            print(f"    [{real_idx+1}/{total_days}] {today}  ${total_value:,.0f}")

    dates = [s["date"] for s in snapshots]
    values = np.array([s["value"] for s in snapshots])
    metrics = compute_metrics(values, dates)
    metrics["trades"] = len(trade_log)

    # Win rate
    if trade_log:
        wins = sum(1 for t in trade_log if t["pnl_pct"] > 0)
        metrics["win_rate"] = wins / len(trade_log)
    else:
        metrics["win_rate"] = 0.0

    print(f"    Done. {len(trade_log)} trades, end value ${values[-1]:,.0f}")
    return dates, values, metrics, trade_log


# ======================================================================
# OUTPUT FORMATTING
# ======================================================================

def print_table(title, headers, rows, col_widths=None):
    """Print a formatted comparison table."""
    if col_widths is None:
        col_widths = [22] + [22] * (len(headers) - 1)

    print(f"\n{title}")
    print("=" * sum(col_widths))
    header_line = ""
    for h, w in zip(headers, col_widths):
        header_line += f"{h:<{w}}" if h == headers[0] else f"{h:>{w}}"
    print(header_line)
    print("-" * sum(col_widths))
    for row in rows:
        line = ""
        for val, w in zip(row, col_widths):
            line += f"{val:<{w}}" if val == row[0] else f"{val:>{w}}"
        print(line)
    print("=" * sum(col_widths))


def fmt_pct(v):
    return f"{v:+.1%}"


def fmt_dollar(v):
    return f"${v:,.0f}"


def fmt_f(v, d=2):
    return f"{v:.{d}f}"


# ======================================================================
# MAIN
# ======================================================================

def main():
    output_lines = []

    def log(s=""):
        print(s)
        output_lines.append(s)

    W = 80
    log("=" * W)
    log("APPLES-TO-APPLES: 80-Delta Options-Only vs UPRO DD25/Cool40")
    log("=" * W)

    # ==================================================================
    # PART A: 2015-2026 (actual data for both)
    # ==================================================================
    log(f"\n{'=' * W}")
    log("PART A: 2015-2026 COMPARISON (Actual ThetaData Options vs Actual UPRO)")
    log(f"{'=' * W}")

    # UPRO DD25/Cool40 sliced to 2015+
    log("\n[1/3] UPRO DD25/Cool40 (2015-03-01 to 2026-03-03)...")
    upro_a_dates, upro_a_vals, upro_a_m = run_upro_dd25(PART_A_START)

    # SPY B&H for same period
    log("[2/3] SPY B&H...")
    spy_a_dates, spy_a_vals, spy_a_m = run_spy_bh(PART_A_START)

    # 80-delta options-only: hardcoded from ThetaData backtest
    log("[3/3] 80-Delta Options-Only: using known ThetaData results")
    opts_a_m = {
        "end_value": 1_108_423,
        "cagr": 0.247,
        "sharpe": 0.85,
        "sortino": 0.95,
        "max_dd": -0.391,
        "calmar": 0.63,
        "worst_12m": -0.40,  # approximate from year-by-year
    }

    log("")
    headers = ["Metric", "UPRO DD25/Cool40", "80-Delta Opts-Only", "SPY B&H"]
    widths = [22, 22, 22, 22]
    rows = [
        ["End Value ($100K)", fmt_dollar(upro_a_m["end_value"]), fmt_dollar(opts_a_m["end_value"]), fmt_dollar(spy_a_m["end_value"])],
        ["CAGR", fmt_pct(upro_a_m["cagr"]), fmt_pct(opts_a_m["cagr"]), fmt_pct(spy_a_m["cagr"])],
        ["Sharpe", fmt_f(upro_a_m["sharpe"]), fmt_f(opts_a_m["sharpe"]), fmt_f(spy_a_m["sharpe"])],
        ["Sortino", fmt_f(upro_a_m["sortino"]), fmt_f(opts_a_m["sortino"]), "--"],
        ["Max Drawdown", fmt_pct(upro_a_m["max_dd"]), fmt_pct(opts_a_m["max_dd"]), fmt_pct(spy_a_m["max_dd"])],
        ["Calmar", fmt_f(upro_a_m["calmar"]), fmt_f(opts_a_m["calmar"]), fmt_f(spy_a_m["calmar"])],
        ["Worst 12mo", fmt_pct(upro_a_m["worst_12m"]), fmt_pct(opts_a_m["worst_12m"]), fmt_pct(spy_a_m["worst_12m"])],
    ]
    print_table("PART A: 2015-2026 COMPARISON", headers, rows, widths)
    for r in rows:
        output_lines.append("  ".join(f"{v:>22}" if i > 0 else f"{v:<22}" for i, v in enumerate(r)))

    # Year-by-year for Part A
    upro_a_yy = yearly_returns(upro_a_vals, upro_a_dates)
    spy_a_yy = yearly_returns(spy_a_vals, spy_a_dates)
    # Options-only year-by-year (hardcoded from ThetaData)
    opts_a_yy = {
        2015: -0.233, 2016: 0.298, 2017: 1.010, 2018: -0.056,
        2019: 0.692, 2020: 0.543, 2021: 1.000, 2022: -0.230,
        2023: 0.036, 2024: 0.306, 2025: 0.085,
    }

    log("\nYear-by-Year Returns (Part A)")
    log(f"  {'Year':<6} {'UPRO DD25/C40':>15} {'80D Opts-Only':>15} {'SPY B&H':>15}")
    log(f"  {'-' * 55}")
    for year in sorted(set(list(upro_a_yy.keys()) + list(opts_a_yy.keys()))):
        u = fmt_pct(upro_a_yy[year]) if year in upro_a_yy else "--"
        o = fmt_pct(opts_a_yy[year]) if year in opts_a_yy else "--"
        s = fmt_pct(spy_a_yy[year]) if year in spy_a_yy else "--"
        log(f"  {year:<6} {u:>15} {o:>15} {s:>15}")

    # ==================================================================
    # PART B: 2009-2026 (actual UPRO, synthetic options)
    # ==================================================================
    log(f"\n{'=' * W}")
    log("PART B: 2009-2026 COMPARISON (Actual UPRO, Synthetic 80-Delta Options)")
    log(f"{'=' * W}")

    # UPRO DD25/Cool40 from inception
    log("\n[1/3] UPRO DD25/Cool40 (2009-06-23 to 2026-03-03)...")
    upro_b_dates, upro_b_vals, upro_b_m = run_upro_dd25(PART_B_SIM_START)

    # SPY B&H for same period
    log("[2/3] SPY B&H...")
    spy_b_dates, spy_b_vals, spy_b_m = run_spy_bh(PART_B_SIM_START)

    # Synthetic 80-delta options-only
    log("[3/3] Synthetic 80-Delta Options-Only...")
    opts_b_dates, opts_b_vals, opts_b_m, opts_b_trades = run_synthetic_options(PART_B_SIM_START)

    log("")
    headers = ["Metric", "UPRO DD25/Cool40", "Syn 80-Delta Opts", "SPY B&H"]
    rows = [
        ["End Value ($100K)", fmt_dollar(upro_b_m["end_value"]), fmt_dollar(opts_b_m["end_value"]), fmt_dollar(spy_b_m["end_value"])],
        ["CAGR", fmt_pct(upro_b_m["cagr"]), fmt_pct(opts_b_m["cagr"]), fmt_pct(spy_b_m["cagr"])],
        ["Sharpe", fmt_f(upro_b_m["sharpe"]), fmt_f(opts_b_m["sharpe"]), fmt_f(spy_b_m["sharpe"])],
        ["Sortino", fmt_f(upro_b_m["sortino"]), fmt_f(opts_b_m["sortino"]), "--"],
        ["Max Drawdown", fmt_pct(upro_b_m["max_dd"]), fmt_pct(opts_b_m["max_dd"]), fmt_pct(spy_b_m["max_dd"])],
        ["Calmar", fmt_f(upro_b_m["calmar"]), fmt_f(opts_b_m["calmar"]), fmt_f(spy_b_m["calmar"])],
        ["Worst 12mo", fmt_pct(upro_b_m["worst_12m"]), fmt_pct(opts_b_m["worst_12m"]), fmt_pct(spy_b_m["worst_12m"])],
        ["Trades", str(upro_b_m.get("trades", "--")), str(opts_b_m.get("trades", "--")), "1"],
    ]
    print_table("PART B: 2009-2026 COMPARISON", headers, rows, widths)
    for r in rows:
        output_lines.append("  ".join(f"{v:>22}" if i > 0 else f"{v:<22}" for i, v in enumerate(r)))

    # Year-by-year for Part B
    upro_b_yy = yearly_returns(upro_b_vals, upro_b_dates)
    spy_b_yy = yearly_returns(spy_b_vals, spy_b_dates)
    opts_b_yy = yearly_returns(opts_b_vals, opts_b_dates)

    log("\nYear-by-Year Returns (Part B)")
    log(f"  {'Year':<6} {'UPRO DD25/C40':>15} {'Syn 80D Opts':>15} {'SPY B&H':>15}")
    log(f"  {'-' * 55}")
    all_years = sorted(set(list(upro_b_yy.keys()) + list(opts_b_yy.keys()) + list(spy_b_yy.keys())))
    for year in all_years:
        u = fmt_pct(upro_b_yy[year]) if year in upro_b_yy else "--"
        o = fmt_pct(opts_b_yy[year]) if year in opts_b_yy else "--"
        s = fmt_pct(spy_b_yy[year]) if year in spy_b_yy else "--"
        log(f"  {year:<6} {u:>15} {o:>15} {s:>15}")

    # Save output
    output_path = os.path.join(_this_dir, "strategy_comparison_output.txt")
    with open(output_path, "w") as f:
        f.write("\n".join(output_lines))
    log(f"\nOutput saved to: {output_path}")

    # Return metrics for STRATEGY_EXPLANATION.md update reference
    return {
        "part_a": {"upro": upro_a_m, "opts": opts_a_m, "spy": spy_a_m,
                    "upro_yy": upro_a_yy, "spy_yy": spy_a_yy, "opts_yy": opts_a_yy},
        "part_b": {"upro": upro_b_m, "opts": opts_b_m, "spy": spy_b_m,
                    "upro_yy": upro_b_yy, "spy_yy": spy_b_yy, "opts_yy": opts_b_yy},
    }


if __name__ == "__main__":
    main()
