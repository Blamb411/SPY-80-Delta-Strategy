"""
Delta Comparison Analysis
=========================
Compare $100K invested in SPY call strategies across different delta levels.

Delta levels tested: 50, 55, 60, 70, 80, 90, 95
Benchmarks: SPY Buy-and-Hold, SSO (2x Leveraged ETF)

Output:
- Portfolio growth chart (log scale)
- Summary statistics table (CAGR, Sharpe, Sortino, Max DD)
- Drawdown highlighting (2008, 2020, 2022)

Usage:
    python delta_comparison_analysis.py
"""

import os
import sys
import math
import logging
from datetime import datetime, timedelta
from collections import defaultdict

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_dir = os.path.dirname(os.path.dirname(_this_dir))
sys.path.insert(0, _project_dir)

from backtest.thetadata_client import ThetaDataClient
from backtest.black_scholes import black_scholes_price, find_strike_for_delta

log = logging.getLogger("delta_comparison")

# Force unbuffered output for progress tracking
import functools
print = functools.partial(print, flush=True)

# ======================================================================
# PARAMETERS
# ======================================================================

DELTAS_TO_TEST = [0.50, 0.55, 0.60, 0.70, 0.80, 0.90, 0.95]
INITIAL_CAPITAL = 100_000

# Strategy parameters (same for all delta levels)
DTE_TARGET = 120
DTE_MIN = 90
DTE_MAX = 150
MH = 60                    # Max hold in trading days
PT = 0.50                  # +50% profit target
RATE = 0.04                # Risk-free rate
SMA_EXIT_THRESHOLD = 0.02  # 2% below SMA200

# Data range
DATA_START = "2004-01-01"  # Extra year for SMA200 calculation
DATA_END = "2026-01-31"
SIM_START = "2005-01-03"   # Start simulation from 2005

# ======================================================================
# HELPERS
# ======================================================================

def is_monthly_opex(exp_str):
    exp_dt = datetime.strptime(exp_str, "%Y-%m-%d").date()
    if exp_dt.weekday() != 4:
        return False
    return 15 <= exp_dt.day <= 21


def find_best_expiration(entry_date_str, monthly_exps_dates, target=DTE_TARGET):
    entry_dt = datetime.strptime(entry_date_str, "%Y-%m-%d").date()
    best_exp = None
    best_dte = 0
    best_diff = 9999
    for exp_str, exp_dt in monthly_exps_dates:
        dte = (exp_dt - entry_dt).days
        if dte < DTE_MIN or dte > DTE_MAX:
            continue
        diff = abs(dte - target)
        if diff < best_diff:
            best_diff = diff
            best_exp = exp_str
            best_dte = dte
    return best_exp, best_dte


def get_bid_ask(eod_row):
    if eod_row is None:
        return None, None
    bid = eod_row.get("bid", 0) or 0
    ask = eod_row.get("ask", 0) or 0
    if bid > 0 and ask > 0 and ask >= bid:
        return bid, ask
    close = eod_row.get("close", 0) or 0
    if close > 0:
        return close * 0.998, close * 1.002
    return None, None


def calculate_delta(spot, strike, dte, iv=0.16, rate=0.04):
    if dte <= 0:
        return 1.0 if spot > strike else 0.0
    t = dte / 365.0
    d1 = (math.log(spot / strike) + (rate + 0.5 * iv * iv) * t) / (iv * math.sqrt(t))
    delta = 0.5 * (1.0 + math.erf(d1 / math.sqrt(2.0)))
    return delta


def black_scholes_call(spot, strike, t, r, sigma):
    """Calculate call option price using Black-Scholes."""
    if t <= 0:
        return max(0, spot - strike)
    d1 = (math.log(spot / strike) + (r + 0.5 * sigma ** 2) * t) / (sigma * math.sqrt(t))
    d2 = d1 - sigma * math.sqrt(t)
    nd1 = 0.5 * (1 + math.erf(d1 / math.sqrt(2)))
    nd2 = 0.5 * (1 + math.erf(d2 / math.sqrt(2)))
    return spot * nd1 - strike * math.exp(-r * t) * nd2


# ======================================================================
# DATA LOADING
# ======================================================================

def load_spy_data():
    """Load SPY price data from Yahoo Finance for full period."""
    import yfinance as yf

    print("Loading SPY data from Yahoo Finance...")
    spy = yf.download("SPY", start=DATA_START, end=DATA_END, progress=False)
    spy = spy.reset_index()

    # Handle multi-level columns from yfinance
    if isinstance(spy.columns, pd.MultiIndex):
        spy.columns = [c[0] for c in spy.columns]

    # Normalize column names
    spy.columns = [str(c).replace(" ", "_") for c in spy.columns]

    spy_by_date = {}
    for _, row in spy.iterrows():
        date_val = row.get("Date") or row.get("date")
        if hasattr(date_val, "strftime"):
            date_str = date_val.strftime("%Y-%m-%d")
        else:
            date_str = str(date_val)[:10]

        # Try different column name formats
        close = row.get("Adj_Close") or row.get("Adj Close") or row.get("Close") or row.get("close")
        open_ = row.get("Open") or row.get("open")
        high = row.get("High") or row.get("high")
        low = row.get("Low") or row.get("low")

        spy_by_date[date_str] = {
            "bar_date": date_str,
            "close": float(close) if close else 0,
            "open": float(open_) if open_ else 0,
            "high": float(high) if high else 0,
            "low": float(low) if low else 0,
        }

    trading_dates = sorted(spy_by_date.keys())
    print(f"  SPY: {len(trading_dates)} days ({trading_dates[0]} to {trading_dates[-1]})")
    return spy_by_date, trading_dates


def load_sso_data():
    """Load SSO (2x leveraged SPY) data."""
    import yfinance as yf

    print("Loading SSO data from Yahoo Finance...")
    # SSO started in 2006, so we'll need to synthesize earlier data
    sso = yf.download("SSO", start="2006-06-01", end=DATA_END, progress=False)
    sso = sso.reset_index()

    # Handle multi-level columns
    if isinstance(sso.columns, pd.MultiIndex):
        sso.columns = [c[0] for c in sso.columns]
    sso.columns = [str(c).replace(" ", "_") for c in sso.columns]

    sso_by_date = {}
    for _, row in sso.iterrows():
        date_val = row.get("Date") or row.get("date")
        if hasattr(date_val, "strftime"):
            date_str = date_val.strftime("%Y-%m-%d")
        else:
            date_str = str(date_val)[:10]

        close = row.get("Adj_Close") or row.get("Adj Close") or row.get("Close") or row.get("close")
        if close:
            sso_by_date[date_str] = float(close)

    print(f"  SSO: {len(sso_by_date)} days")
    return sso_by_date


def load_upro_data():
    """Load UPRO (3x leveraged SPY) data."""
    import yfinance as yf

    print("Loading UPRO data from Yahoo Finance...")
    # UPRO started in June 2009
    upro = yf.download("UPRO", start="2009-06-01", end=DATA_END, progress=False)
    upro = upro.reset_index()

    # Handle multi-level columns
    if isinstance(upro.columns, pd.MultiIndex):
        upro.columns = [c[0] for c in upro.columns]
    upro.columns = [str(c).replace(" ", "_") for c in upro.columns]

    upro_by_date = {}
    for _, row in upro.iterrows():
        date_val = row.get("Date") or row.get("date")
        if hasattr(date_val, "strftime"):
            date_str = date_val.strftime("%Y-%m-%d")
        else:
            date_str = str(date_val)[:10]

        close = row.get("Adj_Close") or row.get("Adj Close") or row.get("Close") or row.get("close")
        if close:
            upro_by_date[date_str] = float(close)

    print(f"  UPRO: {len(upro_by_date)} days")
    return upro_by_date


def load_vix_data():
    """Load VIX data for IV estimation."""
    import yfinance as yf

    print("Loading VIX data...")
    vix = yf.download("^VIX", start=DATA_START, end=DATA_END, progress=False)
    vix = vix.reset_index()

    # Handle multi-level columns
    if isinstance(vix.columns, pd.MultiIndex):
        vix.columns = [c[0] for c in vix.columns]
    vix.columns = [str(c).replace(" ", "_") for c in vix.columns]

    vix_by_date = {}
    for _, row in vix.iterrows():
        date_val = row.get("Date") or row.get("date")
        if hasattr(date_val, "strftime"):
            date_str = date_val.strftime("%Y-%m-%d")
        else:
            date_str = str(date_val)[:10]

        close = row.get("Close") or row.get("close")
        if close:
            vix_by_date[date_str] = float(close)

    print(f"  VIX: {len(vix_by_date)} days")
    return vix_by_date


def compute_sma200(spy_by_date, trading_dates):
    """Calculate SMA200 for all dates."""
    sma200 = {}
    for i in range(199, len(trading_dates)):
        window = [spy_by_date[trading_dates[j]]["close"] for j in range(i - 199, i + 1)]
        sma200[trading_dates[i]] = sum(window) / 200.0
    return sma200


# ======================================================================
# SYNTHETIC BACKTEST (2005-2014)
# ======================================================================

def run_synthetic_backtest(spy_by_date, trading_dates, vix_by_date, sma200, target_delta, label=""):
    """
    Run synthetic backtest using Black-Scholes pricing.
    Used for 2005-2014 period before ThetaData availability.
    """
    cash = float(INITIAL_CAPITAL)
    positions = []
    daily_values = []
    trade_log = []

    # Filter to simulation period
    sim_dates = [d for d in trading_dates if d >= SIM_START and d < "2015-03-01"]

    for day_idx, today in enumerate(sim_dates):
        bar = spy_by_date.get(today)
        if not bar:
            continue

        spot = bar["close"]
        sma_val = sma200.get(today)
        above_sma = (spot > sma_val) if sma_val else False
        vix = vix_by_date.get(today, 20.0)
        iv_est = max(0.10, min(0.80, vix / 100.0))

        # Mark positions to market
        positions_value = 0.0
        still_open = []

        for pos in positions:
            pos["days_held"] += 1
            dte = (datetime.strptime(pos["expiration"], "%Y-%m-%d").date() -
                   datetime.strptime(today, "%Y-%m-%d").date()).days

            if dte <= 0:
                # Expired - calculate intrinsic value
                intrinsic = max(0, spot - pos["strike"])
                proceeds = intrinsic * 100 * pos["quantity"]
                cash += proceeds
                pnl_pct = (intrinsic / pos["entry_price"]) - 1 if pos["entry_price"] > 0 else 0
                trade_log.append({
                    "entry_date": pos["entry_date"],
                    "exit_date": today,
                    "pnl_pct": pnl_pct,
                    "exit_reason": "EXP",
                })
                continue

            # Calculate current option value
            t_years = dte / 365.0
            current_price = black_scholes_call(spot, pos["strike"], t_years, RATE, iv_est)

            # Apply bid-ask spread (synthetic)
            bid = current_price * 0.97
            ask = current_price * 1.03
            mid = current_price

            pnl_pct = mid / pos["entry_price"] - 1

            # Check exit conditions
            exit_reason = None

            # SMA breach exit
            pct_below_sma = (sma_val - spot) / sma_val if sma_val and sma_val > 0 else 0
            if pct_below_sma >= SMA_EXIT_THRESHOLD:
                exit_reason = "SMA"
            elif pnl_pct >= PT:
                exit_reason = "PT"
            elif pos["days_held"] >= MH:
                exit_reason = "MH"

            if exit_reason:
                proceeds = bid * 100 * pos["quantity"]
                cash += proceeds
                trade_log.append({
                    "entry_date": pos["entry_date"],
                    "exit_date": today,
                    "pnl_pct": bid / pos["entry_price"] - 1,
                    "exit_reason": exit_reason,
                })
            else:
                positions_value += mid * 100 * pos["quantity"]
                still_open.append(pos)

        positions = still_open

        # Entry logic
        if above_sma and sma_val is not None:
            # Find synthetic expiration ~120 days out
            entry_dt = datetime.strptime(today, "%Y-%m-%d").date()
            exp_dt = entry_dt + timedelta(days=DTE_TARGET)
            # Adjust to third Friday
            while exp_dt.weekday() != 4:
                exp_dt += timedelta(days=1)
            if exp_dt.day < 15:
                exp_dt += timedelta(days=7)
            elif exp_dt.day > 21:
                exp_dt -= timedelta(days=7)

            exp_str = exp_dt.strftime("%Y-%m-%d")
            dte_cal = (exp_dt - entry_dt).days

            if DTE_MIN <= dte_cal <= DTE_MAX:
                t_years = dte_cal / 365.0

                # Find strike for target delta
                strike = find_strike_for_delta(spot, t_years, RATE, iv_est, target_delta, "C")

                if strike:
                    # Calculate option price
                    option_price = black_scholes_call(spot, strike, t_years, RATE, iv_est)
                    ask = option_price * 1.03  # Synthetic ask

                    contract_cost = ask * 100

                    if cash >= contract_cost and contract_cost > 0:
                        cash -= contract_cost
                        positions.append({
                            "entry_date": today,
                            "expiration": exp_str,
                            "strike": strike,
                            "entry_price": ask,
                            "quantity": 1,
                            "days_held": 0,
                        })

        # Record daily value
        portfolio_value = cash + positions_value
        daily_values.append({
            "date": today,
            "portfolio_value": portfolio_value,
            "cash": cash,
            "positions_value": positions_value,
            "n_positions": len(positions),
            "spot": spot,
        })

        # Progress update
        if (day_idx + 1) % 500 == 0:
            print(f"    [{label}] Synthetic {day_idx+1}/{len(sim_dates)}: ${portfolio_value:,.0f}")

    return daily_values, trade_log


# ======================================================================
# THETADATA BACKTEST (2015-2025)
# ======================================================================

def run_thetadata_backtest(client, spy_by_date, trading_dates, vix_by_date, sma200,
                           monthly_exps, target_delta, label=""):
    """
    Run backtest using actual ThetaData options quotes.
    Used for 2015-2025 period.
    """
    cash = float(INITIAL_CAPITAL)
    positions = []
    daily_values = []
    trade_log = []
    contract_eod = {}
    strikes_cache = {}

    # Filter to simulation period
    sim_dates = [d for d in trading_dates if d >= "2015-03-01"]

    for day_idx, today in enumerate(sim_dates):
        bar = spy_by_date.get(today)
        if not bar:
            continue

        spot = bar["close"]
        sma_val = sma200.get(today)
        above_sma = (spot > sma_val) if sma_val else False
        vix = vix_by_date.get(today, 20.0)
        iv_est = max(0.08, min(0.90, vix / 100.0))

        # Mark positions to market and check exits
        positions_value = 0.0
        still_open = []

        # Check SMA breach for all positions
        pct_below_sma = (sma_val - spot) / sma_val if sma_val and sma_val > 0 else 0
        force_exit_all = pct_below_sma >= SMA_EXIT_THRESHOLD

        for pos in positions:
            pos["days_held"] += 1
            ckey = (pos["expiration"], pos["strike"])
            eod = contract_eod.get(ckey, {}).get(today)
            bid, ask = get_bid_ask(eod)

            if bid is None or bid <= 0:
                intrinsic = max(0, spot - pos["strike"])
                bid = intrinsic * 0.998 if intrinsic > 0 else 0.001
                ask = intrinsic * 1.002 if intrinsic > 0 else 0.01

            mid = (bid + ask) / 2 if bid and ask else bid or 0
            pnl_pct = mid / pos["entry_price"] - 1 if pos["entry_price"] > 0 else 0

            exit_reason = None
            if force_exit_all:
                exit_reason = "SMA"
            elif pnl_pct >= PT:
                exit_reason = "PT"
            elif pos["days_held"] >= MH:
                exit_reason = "MH"

            if exit_reason:
                proceeds = bid * 100 * pos["quantity"]
                cash += proceeds
                trade_log.append({
                    "entry_date": pos["entry_date"],
                    "exit_date": today,
                    "pnl_pct": bid / pos["entry_price"] - 1,
                    "exit_reason": exit_reason,
                })
            else:
                positions_value += mid * 100 * pos["quantity"]
                still_open.append(pos)

        positions = still_open

        # Entry logic
        if above_sma and sma_val is not None:
            best_exp, dte_cal = find_best_expiration(today, monthly_exps)

            if best_exp:
                t_years = dte_cal / 365.0
                bs_strike = find_strike_for_delta(spot, t_years, RATE, iv_est, target_delta, "C")

                if bs_strike:
                    if best_exp not in strikes_cache:
                        strikes_cache[best_exp] = client.get_strikes("SPY", best_exp)
                    strikes = strikes_cache.get(best_exp, [])

                    if strikes:
                        real_strike = min(strikes, key=lambda s: abs(s - bs_strike))
                        ckey = (best_exp, real_strike)

                        if ckey not in contract_eod:
                            data = client.prefetch_option_life("SPY", best_exp, real_strike, "C", today)
                            contract_eod[ckey] = {r["bar_date"]: r for r in data}

                        eod = contract_eod[ckey].get(today)
                        _, ask = get_bid_ask(eod)

                        if ask and ask > 0:
                            contract_cost = ask * 100

                            if cash >= contract_cost:
                                cash -= contract_cost
                                positions.append({
                                    "entry_date": today,
                                    "expiration": best_exp,
                                    "strike": real_strike,
                                    "entry_price": ask,
                                    "quantity": 1,
                                    "days_held": 0,
                                })

        # Record daily value
        portfolio_value = cash + positions_value
        daily_values.append({
            "date": today,
            "portfolio_value": portfolio_value,
            "cash": cash,
            "positions_value": positions_value,
            "n_positions": len(positions),
            "spot": spot,
        })

        # Progress update
        if (day_idx + 1) % 500 == 0:
            print(f"    [{label}] ThetaData {day_idx+1}/{len(sim_dates)}: ${portfolio_value:,.0f}")

    return daily_values, trade_log


# ======================================================================
# BENCHMARK CALCULATIONS
# ======================================================================

def calculate_spy_buyhold(spy_by_date, trading_dates):
    """Calculate SPY buy-and-hold from $100K."""
    sim_dates = [d for d in trading_dates if d >= SIM_START]

    first_price = spy_by_date[sim_dates[0]]["close"]
    shares = INITIAL_CAPITAL / first_price

    daily_values = []
    for today in sim_dates:
        spot = spy_by_date[today]["close"]
        portfolio_value = shares * spot
        daily_values.append({
            "date": today,
            "portfolio_value": portfolio_value,
            "spot": spot,
        })

    return daily_values


def calculate_sso_buyhold(sso_by_date, trading_dates):
    """Calculate SSO buy-and-hold from $100K (starts 2006-06)."""
    sim_dates = [d for d in trading_dates if d >= "2006-06-21" and d in sso_by_date]

    if not sim_dates:
        return []

    first_price = sso_by_date[sim_dates[0]]
    shares = INITIAL_CAPITAL / first_price

    daily_values = []
    for today in sim_dates:
        if today in sso_by_date:
            price = sso_by_date[today]
            portfolio_value = shares * price
            daily_values.append({
                "date": today,
                "portfolio_value": portfolio_value,
            })

    return daily_values


def calculate_upro_buyhold(upro_by_date, trading_dates):
    """Calculate UPRO buy-and-hold from $100K (starts 2009-06)."""
    sim_dates = [d for d in trading_dates if d >= "2009-06-25" and d in upro_by_date]

    if not sim_dates:
        return []

    first_price = upro_by_date[sim_dates[0]]
    shares = INITIAL_CAPITAL / first_price

    daily_values = []
    for today in sim_dates:
        if today in upro_by_date:
            price = upro_by_date[today]
            portfolio_value = shares * price
            daily_values.append({
                "date": today,
                "portfolio_value": portfolio_value,
            })

    return daily_values


# ======================================================================
# METRICS CALCULATION
# ======================================================================

def compute_metrics(daily_values, label=""):
    """Compute CAGR, Sharpe, Sortino, Max DD."""
    if not daily_values:
        return None

    df = pd.DataFrame(daily_values)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    n_days = len(df)
    years = n_days / 252.0

    start_val = df["portfolio_value"].iloc[0]
    end_val = df["portfolio_value"].iloc[-1]

    total_return = end_val / start_val - 1
    cagr = (end_val / start_val) ** (1 / years) - 1 if years > 0 else 0

    df["daily_ret"] = df["portfolio_value"].pct_change().fillna(0)
    daily_mean = df["daily_ret"].mean()
    daily_std = df["daily_ret"].std()

    sharpe = (daily_mean / daily_std) * np.sqrt(252) if daily_std > 0 else 0

    downside = df["daily_ret"][df["daily_ret"] < 0]
    ds_std = downside.std() if len(downside) > 0 else 0
    sortino = (daily_mean / ds_std) * np.sqrt(252) if ds_std > 0 else 0

    cummax = df["portfolio_value"].cummax()
    drawdown = df["portfolio_value"] / cummax - 1
    max_dd = drawdown.min()

    return {
        "label": label,
        "start_val": start_val,
        "end_val": end_val,
        "total_return": total_return,
        "cagr": cagr,
        "sharpe": sharpe,
        "sortino": sortino,
        "max_dd": max_dd,
        "years": years,
        "n_days": n_days,
    }


# ======================================================================
# VISUALIZATION
# ======================================================================

def create_comparison_chart(all_results, output_path):
    """Create portfolio growth comparison chart."""

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 12),
                                    gridspec_kw={'height_ratios': [3, 1]})

    # Color palette
    colors = {
        "SPY B&H": "#1f77b4",
        "SSO B&H": "#ff7f0e",
        "UPRO B&H": "#d62728",
        "50-Delta": "#2ca02c",
        "55-Delta": "#17becf",
        "60-Delta": "#9467bd",
        "70-Delta": "#8c564b",
        "80-Delta": "#e377c2",
        "90-Delta": "#7f7f7f",
        "95-Delta": "#bcbd22",
    }

    # Plot each strategy
    for label, data in all_results.items():
        if not data["daily_values"]:
            continue
        df = pd.DataFrame(data["daily_values"])
        df["date"] = pd.to_datetime(df["date"])

        color = colors.get(label, "#333333")
        linewidth = 2.5 if label in ["SPY B&H", "80-Delta"] else 1.5
        alpha = 1.0 if label in ["SPY B&H", "80-Delta", "SSO B&H"] else 0.8

        ax1.semilogy(df["date"], df["portfolio_value"],
                     label=label, color=color, linewidth=linewidth, alpha=alpha)

    # Highlight crisis periods
    crisis_periods = [
        ("2008-09-01", "2009-03-09", "2008 Crisis", "#ffcccc"),
        ("2020-02-19", "2020-03-23", "COVID", "#ffe6cc"),
        ("2022-01-03", "2022-10-12", "2022 Bear", "#fff2cc"),
    ]

    for start, end, name, color in crisis_periods:
        ax1.axvspan(pd.to_datetime(start), pd.to_datetime(end),
                    alpha=0.3, color=color, label=f"_{name}")

    ax1.set_ylabel("Portfolio Value ($, log scale)", fontsize=12)
    ax1.set_title("$100,000 Investment: Delta Comparison (2005-2025)", fontsize=14, fontweight="bold")
    ax1.legend(loc="upper left", fontsize=9)
    ax1.grid(True, alpha=0.3)
    ax1.set_xlim(pd.to_datetime("2005-01-01"), pd.to_datetime("2026-01-01"))

    # Add crisis labels
    ax1.annotate("2008\nCrisis", xy=(pd.to_datetime("2008-10-01"), 30000), fontsize=8, ha="center")
    ax1.annotate("COVID", xy=(pd.to_datetime("2020-03-01"), 80000), fontsize=8, ha="center")
    ax1.annotate("2022\nBear", xy=(pd.to_datetime("2022-06-01"), 150000), fontsize=8, ha="center")

    # Bottom panel: Summary statistics table
    ax2.axis("off")

    # Prepare table data
    table_data = []
    headers = ["Strategy", "End Value", "CAGR", "Sharpe", "Sortino", "Max DD"]

    for label in ["SPY B&H", "SSO B&H", "50-Delta", "55-Delta", "60-Delta",
                  "70-Delta", "80-Delta", "90-Delta", "95-Delta"]:
        if label in all_results and all_results[label]["metrics"]:
            m = all_results[label]["metrics"]
            table_data.append([
                label,
                f"${m['end_val']:,.0f}",
                f"{m['cagr']:+.1%}",
                f"{m['sharpe']:.2f}",
                f"{m['sortino']:.2f}",
                f"{m['max_dd']:.1%}",
            ])

    table = ax2.table(cellText=table_data, colLabels=headers,
                      loc="center", cellLoc="center",
                      colColours=["#f0f0f0"] * len(headers))
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1.2, 1.8)

    # Highlight best values
    for i, row in enumerate(table_data):
        # Highlight 80-Delta row
        if row[0] == "80-Delta":
            for j in range(len(headers)):
                table[(i+1, j)].set_facecolor("#e6ffe6")

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    print(f"\nChart saved to: {output_path}")

    return fig


# ======================================================================
# MAIN
# ======================================================================

def create_monthly_chart(all_results, output_path):
    """Create chart showing monthly accrued portfolio values."""
    import matplotlib.dates as mdates

    fig, ax = plt.subplots(figsize=(16, 10))

    # Color palette
    colors = {
        "SPY B&H": "#1f77b4",
        "SSO B&H": "#ff7f0e",
        "UPRO B&H": "#d62728",
        "50-Delta": "#2ca02c",
        "55-Delta": "#17becf",
        "60-Delta": "#9467bd",
        "70-Delta": "#8c564b",
        "80-Delta": "#e377c2",
        "90-Delta": "#7f7f7f",
        "95-Delta": "#bcbd22",
    }

    for label, data in all_results.items():
        if not data["daily_values"]:
            continue

        df = pd.DataFrame(data["daily_values"])
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date")

        # Resample to month-end values
        monthly = df["portfolio_value"].resample("ME").last()

        color = colors.get(label, "#333333")
        linewidth = 2.5 if label in ["SPY B&H", "70-Delta", "80-Delta", "UPRO B&H"] else 1.5
        linestyle = "-" if "Delta" in label else "--"

        ax.plot(monthly.index, monthly.values,
                label=label, color=color, linewidth=linewidth, linestyle=linestyle)

    ax.set_ylabel("Portfolio Value ($)", fontsize=12)
    ax.set_xlabel("Date", fontsize=12)
    ax.set_title("Monthly Accrued Values: $100K Investment Comparison",
                 fontsize=14, fontweight="bold")
    ax.legend(loc="upper left", fontsize=10, ncol=2)
    ax.grid(True, alpha=0.3)

    # Format y-axis
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x/1e6:.1f}M' if x >= 1e6 else f'${x/1e3:.0f}K'))

    # Format x-axis
    ax.xaxis.set_major_locator(mdates.YearLocator(2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    print(f"Monthly chart saved to: {output_path}")
    plt.close()


def main():
    logging.basicConfig(level=logging.WARNING)

    print("=" * 80)
    print("Delta Comparison Analysis")
    print("=" * 80)
    print(f"\nComparing $100K invested in various delta levels")
    print(f"Deltas: {DELTAS_TO_TEST}")
    print(f"Period: {SIM_START} to {DATA_END}")
    print()

    # Load data
    spy_by_date, trading_dates = load_spy_data()
    vix_by_date = load_vix_data()
    sso_by_date = load_sso_data()
    upro_by_date = load_upro_data()
    sma200 = compute_sma200(spy_by_date, trading_dates)

    print(f"  SMA200 available from: {min(sma200.keys())}")

    # Connect to ThetaData
    print("\nConnecting to ThetaData...")
    client = ThetaDataClient()
    if not client.connect():
        print("WARNING: Cannot connect to ThetaData. Will only run synthetic backtest.")
        client = None
    else:
        print("Connected to ThetaData.")
        # Load expirations
        all_exps = client.get_expirations("SPY")
        monthly_exps = [(e, datetime.strptime(e, "%Y-%m-%d").date())
                        for e in all_exps if is_monthly_opex(e)]
        monthly_exps.sort(key=lambda x: x[1])
        print(f"  Monthly expirations: {len(monthly_exps)}")

    all_results = {}

    # 1. SPY Buy-and-Hold
    print("\n" + "=" * 60)
    print("Running SPY Buy-and-Hold...")
    spy_daily = calculate_spy_buyhold(spy_by_date, trading_dates)
    spy_metrics = compute_metrics(spy_daily, "SPY B&H")
    all_results["SPY B&H"] = {"daily_values": spy_daily, "metrics": spy_metrics}
    print(f"  SPY B&H: ${spy_metrics['end_val']:,.0f} | CAGR: {spy_metrics['cagr']:+.1%} | Sharpe: {spy_metrics['sharpe']:.2f} | Sortino: {spy_metrics['sortino']:.2f} | Max DD: {spy_metrics['max_dd']:.1%}")

    # 2. SSO Buy-and-Hold
    print("\nRunning SSO Buy-and-Hold...")
    sso_daily = calculate_sso_buyhold(sso_by_date, trading_dates)
    if sso_daily:
        sso_metrics = compute_metrics(sso_daily, "SSO B&H")
        all_results["SSO B&H"] = {"daily_values": sso_daily, "metrics": sso_metrics}
        print(f"  SSO B&H: ${sso_metrics['end_val']:,.0f} | CAGR: {sso_metrics['cagr']:+.1%} | Sharpe: {sso_metrics['sharpe']:.2f} | Sortino: {sso_metrics['sortino']:.2f} | Max DD: {sso_metrics['max_dd']:.1%}")

    # 3. UPRO Buy-and-Hold
    print("\nRunning UPRO Buy-and-Hold...")
    upro_daily = calculate_upro_buyhold(upro_by_date, trading_dates)
    if upro_daily:
        upro_metrics = compute_metrics(upro_daily, "UPRO B&H")
        all_results["UPRO B&H"] = {"daily_values": upro_daily, "metrics": upro_metrics}
        print(f"  UPRO B&H: ${upro_metrics['end_val']:,.0f} | CAGR: {upro_metrics['cagr']:+.1%} | Sharpe: {upro_metrics['sharpe']:.2f} | Sortino: {upro_metrics['sortino']:.2f} | Max DD: {upro_metrics['max_dd']:.1%}")

    # 3. Run each delta level
    for delta in DELTAS_TO_TEST:
        label = f"{int(delta*100)}-Delta"
        print("\n" + "=" * 60)
        print(f"Running {label} Strategy...")

        combined_daily = []
        combined_trades = []

        # Synthetic backtest (2005-2014)
        print(f"  Phase 1: Synthetic backtest (2005-2014)...")
        synth_daily, synth_trades = run_synthetic_backtest(
            spy_by_date, trading_dates, vix_by_date, sma200, delta, label
        )
        combined_daily.extend(synth_daily)
        combined_trades.extend(synth_trades)

        if synth_daily:
            print(f"    Synthetic complete: {len(synth_daily)} days, {len(synth_trades)} trades")
            print(f"    End value: ${synth_daily[-1]['portfolio_value']:,.0f}")

        # ThetaData backtest (2015-2025)
        if client:
            print(f"  Phase 2: ThetaData backtest (2015-2025)...")

            # Carry forward ending cash from synthetic period
            if synth_daily:
                starting_capital = synth_daily[-1]["portfolio_value"]
            else:
                starting_capital = INITIAL_CAPITAL

            # Run ThetaData backtest with carried-forward capital
            theta_daily, theta_trades = run_thetadata_backtest(
                client, spy_by_date, trading_dates, vix_by_date, sma200,
                monthly_exps, delta, label
            )

            # Scale ThetaData results to continue from synthetic end value
            if theta_daily and synth_daily:
                scale_factor = starting_capital / INITIAL_CAPITAL
                for day in theta_daily:
                    day["portfolio_value"] *= scale_factor
                    day["cash"] *= scale_factor
                    day["positions_value"] *= scale_factor

            combined_daily.extend(theta_daily)
            combined_trades.extend(theta_trades)

            if theta_daily:
                print(f"    ThetaData complete: {len(theta_daily)} days, {len(theta_trades)} trades")
                print(f"    End value: ${theta_daily[-1]['portfolio_value']:,.0f}")

        # Calculate combined metrics
        if combined_daily:
            metrics = compute_metrics(combined_daily, label)
            all_results[label] = {
                "daily_values": combined_daily,
                "trades": combined_trades,
                "metrics": metrics,
            }

            print(f"\n  {label} TOTAL:")
            print(f"    ${INITIAL_CAPITAL:,} -> ${metrics['end_val']:,.0f}")
            print(f"    CAGR: {metrics['cagr']:+.1%} | Sharpe: {metrics['sharpe']:.2f} | Sortino: {metrics['sortino']:.2f} | Max DD: {metrics['max_dd']:.1%}")

    # Close ThetaData connection
    if client:
        client.close()

    # Create comparison chart
    print("\n" + "=" * 60)
    print("Creating comparison chart...")
    output_path = os.path.join(_this_dir, "delta_comparison_chart.png")
    create_comparison_chart(all_results, output_path)

    # Create monthly accrued values chart
    print("\nCreating monthly accrued values chart...")
    monthly_path = os.path.join(_this_dir, "monthly_accrued_values.png")
    create_monthly_chart(all_results, monthly_path)

    # Save daily values to CSV for each strategy
    print("\nSaving daily values to CSV...")
    for label, data in all_results.items():
        if data["daily_values"]:
            safe_label = label.replace(" ", "_").replace("/", "_")
            daily_csv = os.path.join(_this_dir, f"daily_values_{safe_label}.csv")
            daily_df = pd.DataFrame(data["daily_values"])
            daily_df.to_csv(daily_csv, index=False)
            print(f"  Saved: {daily_csv}")

    # Print summary table
    print("\n" + "=" * 80)
    print("SUMMARY: $100K Investment Comparison (2005-2025)")
    print("=" * 80)
    print(f"\n{'Strategy':<12} {'End Value':>14} {'CAGR':>10} {'Sharpe':>10} {'Sortino':>10} {'Max DD':>10}")
    print("-" * 70)

    for label in ["SPY B&H", "SSO B&H", "UPRO B&H", "50-Delta", "55-Delta", "60-Delta",
                  "70-Delta", "80-Delta", "90-Delta", "95-Delta"]:
        if label in all_results and all_results[label]["metrics"]:
            m = all_results[label]["metrics"]
            print(f"{label:<12} ${m['end_val']:>12,.0f} {m['cagr']:>+9.1%} {m['sharpe']:>10.2f} {m['sortino']:>10.2f} {m['max_dd']:>9.1%}")

    print("=" * 80)

    # Save results to CSV
    csv_path = os.path.join(_this_dir, "delta_comparison_results.csv")
    results_df = pd.DataFrame([
        {
            "Strategy": label,
            "End_Value": data["metrics"]["end_val"],
            "CAGR": data["metrics"]["cagr"],
            "Sharpe": data["metrics"]["sharpe"],
            "Sortino": data["metrics"]["sortino"],
            "Max_DD": data["metrics"]["max_dd"],
            "Total_Return": data["metrics"]["total_return"],
        }
        for label, data in all_results.items()
        if data["metrics"]
    ])
    results_df.to_csv(csv_path, index=False)
    print(f"\nResults saved to: {csv_path}")


if __name__ == "__main__":
    main()
