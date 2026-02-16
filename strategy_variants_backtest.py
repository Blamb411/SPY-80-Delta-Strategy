"""
Strategy Variants Backtest - 80-Delta Call Strategy
=====================================================
Tests improvements 2-7 from the code review:

  2. Gamma-adjusted scenario analysis (already fixed in ira_portfolio_model.py,
     but here we track VRP = realized vs implied vol)
  3. VIX < 12 entry filter (skip entries when IV is abnormally low)
  4. Roll near-expiry winners (roll positions with >30% gain and < 20 DTE)
  5. EMA200 vs SMA200 trend filter
  6. Realized vs Implied Vol tracking (Variance Risk Premium)
  7. Robust error handling (already fixed in live scripts)

Each variant is backtested independently and compared to the baseline.
Uses synthetic Black-Scholes pricing (2005-2014) so we don't need ThetaData.

Usage:
    python strategy_variants_backtest.py
    python strategy_variants_backtest.py --data-source stooq   # Use stooq instead of yfinance
    python strategy_variants_backtest.py --cached               # Use cached CSV data

Requirements:
    pip install yfinance pandas numpy
    OR
    pip install pandas-datareader pandas numpy  (for stooq)
"""

import argparse
import math
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_dir = os.path.dirname(os.path.dirname(_this_dir))
# The 'backtest' package lives in 'Claude Options Trading Project/'
_claude_dir = os.path.join(_project_dir, "Claude Options Trading Project")
if os.path.isdir(os.path.join(_claude_dir, "backtest")):
    sys.path.insert(0, _claude_dir)
else:
    sys.path.insert(0, _project_dir)

from backtest.black_scholes import black_scholes_price, find_strike_for_delta


# ======================================================================
# PARAMETERS (matching baseline)
# ======================================================================

SHARES = 3125
DELTA = 0.80
DTE_TARGET = 120
DTE_MIN = 90
DTE_MAX = 150
MH = 60
PT = 0.50
RATE = 0.04
SMA_EXIT_THRESHOLD = 0.02
OPTIONS_CASH_ALLOCATION = 100_000

DATA_START = "2004-01-01"
DATA_END = "2014-12-31"
SIM_START = "2005-01-01"

# Variant-specific parameters
VIX_LOW_THRESHOLD = 12.0      # Variant 3: Skip entry when VIX < 12
ROLL_MIN_GAIN = 0.30          # Variant 4: Roll when gain >= 30%
ROLL_MAX_DTE = 20             # Variant 4: Roll when DTE <= 20
EMA_PERIOD = 200              # Variant 5: EMA period
VRP_LOOKBACK = 20             # Variant 6: Days for realized vol calc


# ======================================================================
# DATA LOADING
# ======================================================================

def load_data_yfinance():
    """Load SPY and VIX from Yahoo Finance."""
    import yfinance as yf
    print("Fetching SPY data from Yahoo Finance...")
    spy = yf.download("SPY", start=DATA_START, end=DATA_END, progress=False)
    print("Fetching VIX data from Yahoo Finance...")
    vix = yf.download("^VIX", start=DATA_START, end=DATA_END, progress=False)
    return _process_downloaded_data(spy, vix)


def load_data_stooq():
    """Load SPY and VIX from Stooq via pandas-datareader."""
    from pandas_datareader import data as pdr
    print("Fetching SPY data from Stooq...")
    spy = pdr.DataReader("SPY.US", "stooq", DATA_START, DATA_END).sort_index()
    print("Fetching VIX data from Stooq...")
    vix = pdr.DataReader("^VIX", "stooq", DATA_START, DATA_END).sort_index()
    return _process_downloaded_data(spy, vix)


def load_data_cached():
    """Load from previously saved CSV files."""
    csv_dir = _this_dir
    spy_path = os.path.join(csv_dir, "spy_data_cache.csv")
    vix_path = os.path.join(csv_dir, "vix_data_cache.csv")
    if not os.path.exists(spy_path) or not os.path.exists(vix_path):
        print(f"ERROR: Cached data not found at {spy_path}")
        print("Run once with --save-cache to create the cache files.")
        return None
    print("Loading cached SPY data...")
    spy = pd.read_csv(spy_path, index_col=0, parse_dates=True)
    print("Loading cached VIX data...")
    vix = pd.read_csv(vix_path, index_col=0, parse_dates=True)
    return _process_downloaded_data(spy, vix)


def save_data_cache(spy_df, vix_df):
    """Save downloaded data to CSV for future use."""
    csv_dir = _this_dir
    spy_df.to_csv(os.path.join(csv_dir, "spy_data_cache.csv"))
    vix_df.to_csv(os.path.join(csv_dir, "vix_data_cache.csv"))
    print("Data cached to spy_data_cache.csv and vix_data_cache.csv")


def _process_downloaded_data(spy, vix):
    """Convert downloaded DataFrames to the dict format used by the sim."""
    if spy.empty or vix.empty:
        print("ERROR: No data loaded")
        return None

    # Handle MultiIndex columns from yfinance
    if isinstance(spy.columns, pd.MultiIndex):
        spy.columns = spy.columns.get_level_values(0)
    if isinstance(vix.columns, pd.MultiIndex):
        vix.columns = vix.columns.get_level_values(0)

    # Normalize column names (Stooq uses different casing)
    spy.columns = [c.capitalize() for c in spy.columns]
    vix.columns = [c.capitalize() for c in vix.columns]

    spy_by_date = {}
    for idx, row in spy.iterrows():
        date_str = idx.strftime("%Y-%m-%d")
        spy_by_date[date_str] = {
            "bar_date": date_str,
            "open": float(row["Open"]),
            "high": float(row["High"]),
            "low": float(row["Low"]),
            "close": float(row["Close"]),
            "volume": int(row["Volume"]) if "Volume" in row and not pd.isna(row["Volume"]) else 0,
        }

    vix_data = {}
    for idx, row in vix.iterrows():
        date_str = idx.strftime("%Y-%m-%d")
        vix_data[date_str] = float(row["Close"])

    trading_dates = sorted(spy_by_date.keys())

    # SMA200
    sma200 = {}
    for i in range(199, len(trading_dates)):
        window = [spy_by_date[trading_dates[j]]["close"] for j in range(i - 199, i + 1)]
        sma200[trading_dates[i]] = sum(window) / 200.0

    # EMA200
    ema200 = _calculate_ema(spy_by_date, trading_dates, EMA_PERIOD)

    # Generate monthly expirations
    monthly_exps = generate_monthly_expirations(DATA_START, DATA_END)

    # Rolling realized volatility (20-day)
    realized_vol = {}
    for i in range(VRP_LOOKBACK, len(trading_dates)):
        returns = []
        for j in range(i - VRP_LOOKBACK + 1, i + 1):
            if j > 0:
                prev = spy_by_date[trading_dates[j - 1]]["close"]
                curr = spy_by_date[trading_dates[j]]["close"]
                returns.append(math.log(curr / prev))
        if returns:
            realized_vol[trading_dates[i]] = np.std(returns, ddof=1) * math.sqrt(252)

    print(f"  SPY bars: {len(spy_by_date)} ({trading_dates[0]} to {trading_dates[-1]})")
    print(f"  VIX days: {len(vix_data)}")
    print(f"  SMA200 from: {sorted(sma200.keys())[0] if sma200 else 'N/A'}")
    print(f"  EMA200 from: {sorted(ema200.keys())[0] if ema200 else 'N/A'}")
    print(f"  Monthly expirations: {len(monthly_exps)}")

    return {
        "spy_by_date": spy_by_date,
        "trading_dates": trading_dates,
        "vix_data": vix_data,
        "sma200": sma200,
        "ema200": ema200,
        "monthly_exps": monthly_exps,
        "realized_vol": realized_vol,
        "spy_df": spy,
        "vix_df": vix,
    }


def _calculate_ema(spy_by_date, trading_dates, period):
    """Calculate Exponential Moving Average."""
    ema = {}
    multiplier = 2.0 / (period + 1)

    # Seed with SMA of first `period` days
    if len(trading_dates) < period:
        return ema
    seed = sum(spy_by_date[trading_dates[i]]["close"] for i in range(period)) / period
    ema[trading_dates[period - 1]] = seed

    prev_ema = seed
    for i in range(period, len(trading_dates)):
        close = spy_by_date[trading_dates[i]]["close"]
        new_ema = (close - prev_ema) * multiplier + prev_ema
        ema[trading_dates[i]] = new_ema
        prev_ema = new_ema

    return ema


# ======================================================================
# SYNTHETIC HELPERS
# ======================================================================

def generate_monthly_expirations(start_date, end_date):
    """Generate synthetic monthly option expiration dates (3rd Friday)."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    expirations = []
    current = start.replace(day=1)

    while current <= end:
        first_day = current.replace(day=1)
        days_until_friday = (4 - first_day.weekday()) % 7
        first_friday = first_day + timedelta(days=days_until_friday)
        third_friday = first_friday + timedelta(days=14)

        if start <= third_friday <= end:
            expirations.append((third_friday.strftime("%Y-%m-%d"), third_friday.date()))

        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)

    return expirations


def generate_strikes(spot, step=5.0, num_strikes=40):
    """Generate synthetic strike prices around spot."""
    atm = round(spot / step) * step
    return sorted([atm + i * step for i in range(-num_strikes // 2, num_strikes // 2 + 1) if atm + i * step > 0])


def calculate_delta(spot, strike, dte, iv=0.16, rate=0.04, right="C"):
    """Calculate option delta using Black-Scholes."""
    if dte <= 0:
        if spot == strike:
            return 0.5 if right == "C" else -0.5
        if right == "C":
            return 1.0 if spot > strike else 0.0
        else:
            return -1.0 if spot < strike else 0.0

    t = dte / 365.0
    d1 = (math.log(spot / strike) + (rate + 0.5 * iv * iv) * t) / (iv * math.sqrt(t))
    delta = 0.5 * (1.0 + math.erf(d1 / math.sqrt(2.0)))

    if right == "P":
        delta = delta - 1.0
    return delta


def synthetic_option_price(spot, strike, dte, iv, rate=RATE, right="C"):
    """Calculate synthetic option price using Black-Scholes."""
    if dte <= 0:
        if right == "C":
            return max(0, spot - strike)
        else:
            return max(0, strike - spot)

    t_years = dte / 365.0
    price = black_scholes_price(spot, strike, t_years, rate, iv, right)
    if price is None:
        if right == "C":
            return max(0, spot - strike)
        else:
            return max(0, strike - spot)
    return price


def apply_synthetic_spread(mid_price, spread_pct=0.02):
    """Apply synthetic bid-ask spread."""
    half_spread = spread_pct / 2
    bid = mid_price * (1 - half_spread)
    ask = mid_price * (1 + half_spread)
    return max(0.01, bid), max(0.01, ask)


def find_best_expiration(entry_date_str, monthly_exps, target=DTE_TARGET,
                         dte_min=DTE_MIN, dte_max=DTE_MAX):
    """Find best expiration date for entry."""
    entry_dt = datetime.strptime(entry_date_str, "%Y-%m-%d").date()
    best_exp, best_dte, best_diff = None, 0, 9999
    for exp_str, exp_dt in monthly_exps:
        dte = (exp_dt - entry_dt).days
        if dte < dte_min or dte > dte_max:
            continue
        diff = abs(dte - target)
        if diff < best_diff:
            best_diff, best_exp, best_dte = diff, exp_str, dte
    return best_exp, best_dte


# ======================================================================
# SIMULATION ENGINE
# ======================================================================

def run_variant(data, variant_name, **variant_opts):
    """
    Run a single strategy variant.

    variant_opts:
        use_ema: bool - Use EMA200 instead of SMA200 (variant 5)
        vix_low_filter: bool - Skip entry when VIX < 12 (variant 3)
        roll_winners: bool - Roll near-expiry winners (variant 4)
        track_vrp: bool - Track variance risk premium (variant 6)
    """
    use_ema = variant_opts.get("use_ema", False)
    vix_low_filter = variant_opts.get("vix_low_filter", False)
    roll_winners = variant_opts.get("roll_winners", False)
    track_vrp = variant_opts.get("track_vrp", False)

    spy_by_date = data["spy_by_date"]
    trading_dates = data["trading_dates"]
    vix_data = data["vix_data"]
    sma200 = data["sma200"]
    ema200 = data["ema200"]
    monthly_exps = data["monthly_exps"]
    realized_vol = data["realized_vol"]

    # Pick trend filter
    trend_filter = ema200 if use_ema else sma200
    trend_label = "EMA200" if use_ema else "SMA200"

    # Initialize
    shares_held = SHARES
    options_cash = float(OPTIONS_CASH_ALLOCATION)
    pending_cash = 0.0
    positions = []
    daily_snapshots = []
    trade_log = []
    entry_skip_reasons = defaultdict(int)
    roll_count = 0

    start_idx = next((i for i, d in enumerate(trading_dates) if d >= SIM_START), 0)

    print(f"\n  Running variant: {variant_name}...")

    for day_idx in range(start_idx, len(trading_dates)):
        today = trading_dates[day_idx]
        bar = spy_by_date[today]
        spot = bar["close"]
        trend_val = trend_filter.get(today)
        above_trend = (spot > trend_val) if trend_val else False

        vix_close = vix_data.get(today, 20.0)
        iv_est = max(0.08, min(0.90, vix_close / 100.0 * 0.95))

        # VRP tracking (variant 6)
        rv = realized_vol.get(today, 0)
        vrp = (iv_est - rv) if rv > 0 else 0  # Positive = IV > RV (typical)

        shares_value = shares_held * spot

        # Settle yesterday's exits
        options_cash += pending_cash
        pending_cash = 0.0

        # Force-exit below SMA/EMA threshold
        pct_below = (trend_val - spot) / trend_val if trend_val and trend_val > 0 else 0
        if pct_below >= SMA_EXIT_THRESHOLD and positions:
            for pos in positions:
                dte = (datetime.strptime(pos["expiration"], "%Y-%m-%d").date() -
                       datetime.strptime(today, "%Y-%m-%d").date()).days
                mid_price = synthetic_option_price(spot, pos["strike"], dte, iv_est)
                bid, _ = apply_synthetic_spread(mid_price)
                proceeds = bid * 100 * pos["quantity"]
                pending_cash += proceeds
                pnl_pct = bid / pos["entry_price"] - 1
                trade_log.append({
                    "entry_date": pos["entry_date"], "exit_date": today,
                    "expiration": pos["expiration"], "strike": pos["strike"],
                    "quantity": pos["quantity"], "entry_price": pos["entry_price"],
                    "exit_price": bid, "pnl_pct": pnl_pct,
                    "pnl_dollar": proceeds - pos["contract_cost"],
                    "days_held": pos["days_held"] + 1, "exit_reason": "TREND",
                    "contract_cost": pos["contract_cost"],
                    "entry_delta": pos["entry_delta"],
                })
            positions = []

        # Normal exits (PT / MH) + rolling (variant 4)
        still_open = []
        for pos in positions:
            pos["days_held"] += 1
            dte = (datetime.strptime(pos["expiration"], "%Y-%m-%d").date() -
                   datetime.strptime(today, "%Y-%m-%d").date()).days
            mid_price = synthetic_option_price(spot, pos["strike"], dte, iv_est)
            bid, _ = apply_synthetic_spread(mid_price)
            pnl_pct = bid / pos["entry_price"] - 1
            exit_reason = None

            if pnl_pct >= PT:
                exit_reason = "PT"
            elif pos["days_held"] >= MH:
                exit_reason = "MH"
            # Variant 4: Roll near-expiry winners
            elif roll_winners and dte <= ROLL_MAX_DTE and pnl_pct >= ROLL_MIN_GAIN:
                # Close current position and open a new one
                proceeds = bid * 100 * pos["quantity"]
                pending_cash += proceeds
                trade_log.append({
                    "entry_date": pos["entry_date"], "exit_date": today,
                    "expiration": pos["expiration"], "strike": pos["strike"],
                    "quantity": pos["quantity"], "entry_price": pos["entry_price"],
                    "exit_price": bid, "pnl_pct": pnl_pct,
                    "pnl_dollar": proceeds - pos["contract_cost"],
                    "days_held": pos["days_held"], "exit_reason": "ROLL",
                    "contract_cost": pos["contract_cost"],
                    "entry_delta": pos["entry_delta"],
                })
                roll_count += 1
                # The capital returns to cash and will be re-deployed in the entry step
                continue  # Don't add to still_open

            if exit_reason:
                proceeds = bid * 100 * pos["quantity"]
                pending_cash += proceeds
                trade_log.append({
                    "entry_date": pos["entry_date"], "exit_date": today,
                    "expiration": pos["expiration"], "strike": pos["strike"],
                    "quantity": pos["quantity"], "entry_price": pos["entry_price"],
                    "exit_price": bid, "pnl_pct": pnl_pct,
                    "pnl_dollar": proceeds - pos["contract_cost"],
                    "days_held": pos["days_held"], "exit_reason": exit_reason,
                    "contract_cost": pos["contract_cost"],
                    "entry_delta": pos["entry_delta"],
                })
            else:
                still_open.append(pos)
        positions = still_open

        # Current options delta
        current_options_delta = 0.0
        for pos in positions:
            dte = (datetime.strptime(pos["expiration"], "%Y-%m-%d").date() -
                   datetime.strptime(today, "%Y-%m-%d").date()).days
            pos_delta = calculate_delta(spot, pos["strike"], dte, iv_est) * pos["quantity"] * 100
            current_options_delta += pos_delta

        # Entry
        entered = False
        delta_room = SHARES - current_options_delta

        entry_allowed = above_trend and trend_val is not None and delta_room > 80

        # Variant 3: VIX low filter
        if entry_allowed and vix_low_filter and vix_close < VIX_LOW_THRESHOLD:
            entry_allowed = False
            entry_skip_reasons["vix_too_low"] += 1

        if entry_allowed:
            best_exp, dte_cal = find_best_expiration(today, monthly_exps)
            if not best_exp:
                entry_skip_reasons["no_expiration"] += 1
            else:
                t_years = dte_cal / 365.0
                bs_strike = find_strike_for_delta(spot, t_years, RATE, iv_est, DELTA, "C")
                if not bs_strike:
                    entry_skip_reasons["bs_fail"] += 1
                else:
                    strikes = generate_strikes(spot)
                    real_strike = min(strikes, key=lambda s: abs(s - bs_strike))
                    mid_price = synthetic_option_price(spot, real_strike, dte_cal, iv_est)
                    _, ask = apply_synthetic_spread(mid_price)

                    if ask <= 0.01:
                        entry_skip_reasons["no_ask"] += 1
                    else:
                        option_delta = calculate_delta(spot, real_strike, dte_cal, iv_est)
                        max_by_delta = int(delta_room / (option_delta * 100))
                        max_by_cash = int(options_cash / (ask * 100))
                        qty = min(max_by_delta, max_by_cash, 1)

                        if qty <= 0:
                            entry_skip_reasons["delta_cap" if max_by_delta <= 0 else "no_capital"] += 1
                        else:
                            total_cost = ask * 100 * qty
                            options_cash -= total_cost
                            positions.append({
                                "entry_date": today, "expiration": best_exp,
                                "strike": real_strike, "entry_price": ask,
                                "quantity": qty, "contract_cost": total_cost,
                                "days_held": 0, "entry_delta": option_delta,
                                "entry_iv": iv_est, "entry_spot": spot,
                            })
                            entered = True

        # Mark to market
        positions_value = 0.0
        total_options_delta = 0.0
        for pos in positions:
            dte = (datetime.strptime(pos["expiration"], "%Y-%m-%d").date() -
                   datetime.strptime(today, "%Y-%m-%d").date()).days
            mid_price = synthetic_option_price(spot, pos["strike"], dte, iv_est)
            positions_value += mid_price * 100 * pos["quantity"]
            pos_delta = calculate_delta(spot, pos["strike"], dte, iv_est) * pos["quantity"] * 100
            total_options_delta += pos_delta

        portfolio_value = shares_value + options_cash + pending_cash + positions_value
        total_delta = shares_held + total_options_delta

        daily_snapshots.append({
            "date": today,
            "portfolio_value": portfolio_value,
            "shares_value": shares_value,
            "options_value": positions_value,
            "options_cash": options_cash + pending_cash,
            "total_delta": total_delta,
            "effective_leverage": total_delta / shares_held if shares_held > 0 else 0,
            "above_trend": above_trend,
            "spy_close": spot,
            "vix_close": vix_close,
            "iv_est": iv_est,
            "realized_vol": rv,
            "vrp": vrp,
            "trend_val": trend_val or 0,
        })

    return daily_snapshots, trade_log, entry_skip_reasons, roll_count


# ======================================================================
# METRICS
# ======================================================================

def compute_metrics(snapshots, trade_log, variant_name):
    """Compute portfolio metrics for a variant."""
    df = pd.DataFrame(snapshots)
    tdf = pd.DataFrame(trade_log) if trade_log else pd.DataFrame()

    n_days = len(df)
    years = n_days / 252.0

    start_val = df["portfolio_value"].iloc[0]
    end_val = df["portfolio_value"].iloc[-1]

    total_return = end_val / start_val - 1
    cagr = (end_val / start_val) ** (1 / years) - 1 if years > 0 else 0

    df["daily_ret"] = df["portfolio_value"].pct_change().fillna(0)
    daily_std = df["daily_ret"].std()
    sharpe = (df["daily_ret"].mean() / daily_std) * np.sqrt(252) if daily_std > 0 else 0

    downside = df["daily_ret"][df["daily_ret"] < 0]
    ds_std = downside.std() if len(downside) > 0 else 0
    sortino = (df["daily_ret"].mean() / ds_std) * np.sqrt(252) if ds_std > 0 else 0

    cummax = df["portfolio_value"].cummax()
    drawdown = df["portfolio_value"] / cummax - 1
    max_dd = drawdown.min()

    # SPY benchmark
    spy_start = df["spy_close"].iloc[0]
    spy_end = df["spy_close"].iloc[-1]
    spy_cagr = (spy_end / spy_start) ** (1 / years) - 1 if years > 0 else 0

    # Shares-only
    shares_start = df["shares_value"].iloc[0]
    shares_end = df["shares_value"].iloc[-1]
    shares_cagr = (shares_end / shares_start) ** (1 / years) - 1 if years > 0 else 0

    # Trade stats
    n_trades = len(tdf)
    win_rate = 0
    avg_return = 0
    total_pnl = 0
    pt_exits = mh_exits = trend_exits = roll_exits = 0

    if n_trades > 0:
        wins = tdf[tdf["pnl_pct"] > 0]
        win_rate = len(wins) / n_trades
        avg_return = tdf["pnl_pct"].mean()
        total_pnl = tdf["pnl_dollar"].sum()
        pt_exits = len(tdf[tdf["exit_reason"] == "PT"])
        mh_exits = len(tdf[tdf["exit_reason"] == "MH"])
        trend_exits = len(tdf[tdf["exit_reason"] == "TREND"])
        roll_exits = len(tdf[tdf["exit_reason"] == "ROLL"])

    # VRP stats
    avg_vrp = df["vrp"].mean() if "vrp" in df.columns else 0

    # Yearly performance
    df["year"] = pd.to_datetime(df["date"]).dt.year
    yearly = {}
    for year in sorted(df["year"].unique()):
        ydf = df[df["year"] == year]
        y_start = ydf["portfolio_value"].iloc[0]
        y_end = ydf["portfolio_value"].iloc[-1]
        y_ret = y_end / y_start - 1

        y_shares_start = ydf["shares_value"].iloc[0]
        y_shares_end = ydf["shares_value"].iloc[-1]
        y_shares_ret = y_shares_end / y_shares_start - 1

        yearly[year] = {"ret": y_ret, "shares_ret": y_shares_ret, "alpha": y_ret - y_shares_ret}

    return {
        "name": variant_name,
        "start_val": start_val,
        "end_val": end_val,
        "total_return": total_return,
        "cagr": cagr,
        "sharpe": sharpe,
        "sortino": sortino,
        "max_dd": max_dd,
        "spy_cagr": spy_cagr,
        "shares_cagr": shares_cagr,
        "alpha_vs_shares": cagr - shares_cagr,
        "n_trades": n_trades,
        "win_rate": win_rate,
        "avg_return": avg_return,
        "total_pnl": total_pnl,
        "pt_exits": pt_exits,
        "mh_exits": mh_exits,
        "trend_exits": trend_exits,
        "roll_exits": roll_exits,
        "avg_vrp": avg_vrp,
        "yearly": yearly,
        "snapshots_df": df,
    }


# ======================================================================
# OUTPUT
# ======================================================================

def print_comparison(all_metrics):
    """Print side-by-side comparison of all variants."""
    W = 110

    print(f"\n{'=' * W}")
    print("STRATEGY VARIANT COMPARISON -- 80-DELTA CALL STRATEGY")
    print(f"Period: {SIM_START} to {DATA_END} (Synthetic B-S Pricing)")
    print(f"{'=' * W}")

    # Summary table
    print(f"\n  {'Variant':<35} {'CAGR':>8} {'Sharpe':>8} {'Sortino':>8} "
          f"{'MaxDD':>8} {'Alpha':>8} {'Trades':>7} {'WinRate':>8}")
    print(f"  {'-' * 100}")

    for m in all_metrics:
        print(f"  {m['name']:<35} {m['cagr']:>+7.1%} {m['sharpe']:>8.2f} {m['sortino']:>8.2f} "
              f"{m['max_dd']:>7.1%} {m['alpha_vs_shares']:>+7.1%} {m['n_trades']:>7} {m['win_rate']:>7.0%}")

    # Trade details
    print(f"\n{'=' * W}")
    print("TRADE STATISTICS")
    print(f"{'=' * W}")

    print(f"\n  {'Variant':<35} {'AvgRet':>8} {'TotalPnL':>12} {'PT':>5} {'MH':>5} "
          f"{'Trend':>6} {'Roll':>5}")
    print(f"  {'-' * 82}")

    for m in all_metrics:
        print(f"  {m['name']:<35} {m['avg_return']:>+7.1%} ${m['total_pnl']:>11,.0f} "
              f"{m['pt_exits']:>5} {m['mh_exits']:>5} {m['trend_exits']:>6} {m['roll_exits']:>5}")

    # Dollar comparison
    print(f"\n{'=' * W}")
    print("ENDING VALUE COMPARISON")
    print(f"{'=' * W}")

    baseline = all_metrics[0]
    print(f"\n  {'Variant':<35} {'Start':>14} {'End':>14} {'vs Baseline':>14}")
    print(f"  {'-' * 80}")

    for m in all_metrics:
        diff = m["end_val"] - baseline["end_val"]
        diff_str = f"${diff:>+13,.0f}" if m != baseline else f"{'(baseline)':>14}"
        print(f"  {m['name']:<35} ${m['start_val']:>13,.0f} ${m['end_val']:>13,.0f} {diff_str}")

    # Year-by-year
    print(f"\n{'=' * W}")
    print("YEAR-BY-YEAR RETURNS")
    print(f"{'=' * W}")

    all_years = sorted(set(y for m in all_metrics for y in m["yearly"]))

    # Header
    header = f"\n  {'Year':<6}"
    for m in all_metrics:
        short_name = m["name"][:12]
        header += f" {short_name:>12}"
    header += f" {'Shares':>10}"
    print(header)
    print(f"  {'-' * (6 + 12 * len(all_metrics) + 12)}")

    for year in all_years:
        row = f"  {year:<6}"
        for m in all_metrics:
            y = m["yearly"].get(year, {})
            ret = y.get("ret", 0)
            row += f" {ret:>+11.1%}"
        # Shares
        sr = all_metrics[0]["yearly"].get(year, {}).get("shares_ret", 0)
        row += f" {sr:>+9.1%}"
        print(row)

    # Crisis analysis
    print(f"\n{'=' * W}")
    print("CRISIS PERIOD: 2008-2009")
    print(f"{'=' * W}")

    print(f"\n  {'Variant':<35} {'2008':>10} {'2009':>10} {'Combined':>10}")
    print(f"  {'-' * 68}")

    for m in all_metrics:
        r08 = m["yearly"].get(2008, {}).get("ret", 0)
        r09 = m["yearly"].get(2009, {}).get("ret", 0)
        combined = (1 + r08) * (1 + r09) - 1
        print(f"  {m['name']:<35} {r08:>+9.1%} {r09:>+9.1%} {combined:>+9.1%}")

    # Summary
    print(f"\n{'=' * W}")
    print("SUMMARY")
    print(f"{'=' * W}")

    best_cagr = max(all_metrics, key=lambda m: m["cagr"])
    best_sharpe = max(all_metrics, key=lambda m: m["sharpe"])
    best_dd = max(all_metrics, key=lambda m: m["max_dd"])  # Least negative
    best_alpha = max(all_metrics, key=lambda m: m["alpha_vs_shares"])

    print(f"\n  Best CAGR:   {best_cagr['name']} ({best_cagr['cagr']:+.1%})")
    print(f"  Best Sharpe: {best_sharpe['name']} ({best_sharpe['sharpe']:.2f})")
    print(f"  Best MaxDD:  {best_dd['name']} ({best_dd['max_dd']:.1%})")
    print(f"  Best Alpha:  {best_alpha['name']} ({best_alpha['alpha_vs_shares']:+.1%})")

    print(f"\n  *** REMINDER: Synthetic B-S pricing -- approximate results only ***")
    print(f"{'=' * W}")


def export_results(all_metrics):
    """Export comparison results to CSV."""
    rows = []
    for m in all_metrics:
        rows.append({
            "variant": m["name"],
            "cagr": m["cagr"],
            "sharpe": m["sharpe"],
            "sortino": m["sortino"],
            "max_dd": m["max_dd"],
            "alpha_vs_shares": m["alpha_vs_shares"],
            "n_trades": m["n_trades"],
            "win_rate": m["win_rate"],
            "avg_return": m["avg_return"],
            "total_pnl": m["total_pnl"],
            "start_val": m["start_val"],
            "end_val": m["end_val"],
        })

    df = pd.DataFrame(rows)
    path = os.path.join(_this_dir, "variant_comparison_results.csv")
    df.to_csv(path, index=False)
    print(f"\nResults exported to: {path}")


# ======================================================================
# MAIN
# ======================================================================

def main():
    parser = argparse.ArgumentParser(description="Strategy Variants Backtest")
    parser.add_argument("--data-source", choices=["yfinance", "stooq", "cached"],
                        default="yfinance", help="Data source")
    parser.add_argument("--cached", action="store_true",
                        help="Use cached CSV data (shortcut for --data-source cached)")
    parser.add_argument("--save-cache", action="store_true",
                        help="Save downloaded data to CSV cache")
    args = parser.parse_args()

    if args.cached:
        args.data_source = "cached"

    print("=" * 80)
    print("Strategy Variants Backtest - 80-Delta Call Strategy")
    print(f"Period: {SIM_START} to {DATA_END} (Synthetic B-S Pricing)")
    print("=" * 80)
    print()
    print("Testing variants:")
    print("  0. BASELINE: SMA200 filter, standard rules")
    print("  3. VIX<12 FILTER: Skip entries when VIX < 12")
    print("  4. ROLL WINNERS: Roll positions with >30% gain and <20 DTE")
    print("  5. EMA200: Use EMA200 instead of SMA200")
    print("  6. VRP TRACKING: Track realized vs implied vol (informational)")
    print("  3+4: VIX FILTER + ROLL combined")
    print("  3+4+5: VIX FILTER + ROLL + EMA200 combined")
    print()

    # Load data
    print("Loading market data...")
    if args.data_source == "cached":
        data = load_data_cached()
    elif args.data_source == "stooq":
        data = load_data_stooq()
    else:
        data = load_data_yfinance()

    if data is None:
        print("\nERROR: Failed to load data. Try --data-source stooq or --cached")
        return

    if args.save_cache:
        save_data_cache(data["spy_df"], data["vix_df"])

    # Run variants
    variants = [
        ("0. Baseline (SMA200)", {}),
        ("3. VIX<12 Filter", {"vix_low_filter": True}),
        ("4. Roll Winners", {"roll_winners": True}),
        ("5. EMA200", {"use_ema": True}),
        ("3+4. VIX Filter + Roll", {"vix_low_filter": True, "roll_winners": True}),
        ("3+4+5. VIX+Roll+EMA200", {"vix_low_filter": True, "roll_winners": True, "use_ema": True}),
        ("6. VRP Tracking (baseline)", {"track_vrp": True}),
    ]

    all_metrics = []
    for name, opts in variants:
        snaps, trades, skips, rolls = run_variant(data, name, **opts)
        metrics = compute_metrics(snaps, trades, name)
        all_metrics.append(metrics)

        if rolls > 0:
            print(f"    Rolls: {rolls}")
        if skips:
            skip_str = ", ".join(f"{k}={v}" for k, v in sorted(skips.items()))
            print(f"    Skips: {skip_str}")

    # Print comparison
    print_comparison(all_metrics)

    # Export
    export_results(all_metrics)

    print("\nStrategy variants backtest complete.")


if __name__ == "__main__":
    main()
