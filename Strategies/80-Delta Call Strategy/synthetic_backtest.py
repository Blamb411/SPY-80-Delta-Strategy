"""
Synthetic Options Backtester for 80-Delta Call Strategy (2005-2014)
====================================================================

*** IMPORTANT DISCLAIMER ***
This backtester uses SYNTHETIC (simulated) option prices, NOT actual historical
market data. Option prices are computed using Black-Scholes with VIX as an
implied volatility proxy. This approach has significant limitations:

1. No bid-ask spreads - Uses theoretical mid-price (unrealistic)
2. No liquidity constraints - Assumes unlimited liquidity
3. VIX != SPY options IV - VIX is a 30-day forward-looking measure
4. No volatility smile/skew - Uses flat IV across all strikes
5. No early exercise premium - Assumes European-style pricing
6. Perfect execution - No slippage or market impact

RESULTS SHOULD BE INTERPRETED WITH CAUTION. This is a stress-test and
directional indicator, not a precise historical simulation.

The main purpose is to test strategy behavior through the 2008-2009
financial crisis, which predates ThetaData's options coverage.

Strategy Logic (matches delta_capped_backtest.py):
  - Buy 80-delta calls when SPY is above SMA200
  - Exit at: +50% profit target, 60-day max hold, or 2% below SMA200
  - Delta cap of 3,125 (matching share count)
  - $100,000 options allocation
  - 3,125 shares baseline

Usage:
    python synthetic_backtest.py

Requirements:
    pip install yfinance pandas numpy
"""

import os
import sys
import math
from datetime import datetime, timedelta
from collections import defaultdict

import numpy as np
import pandas as pd

try:
    import yfinance as yf
except ImportError:
    print("ERROR: yfinance not installed. Run: pip install yfinance")
    sys.exit(1)

_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_dir = os.path.dirname(os.path.dirname(_this_dir))
sys.path.insert(0, _project_dir)

from backtest.black_scholes import black_scholes_price, black_scholes_greeks, find_strike_for_delta

# ======================================================================
# PARAMETERS (matching delta_capped_backtest.py)
# ======================================================================

# Share holdings
SHARES = 3125              # Number of SPY shares held (IRA amount)

# Strategy parameters - LONG CALLS
DELTA = 0.80               # Target delta for long calls
DTE_TARGET = 120           # Calendar days to expiration
DTE_MIN = 90
DTE_MAX = 150
MH = 60                    # Max hold in trading days
PT = 0.50                  # +50% profit target
RATE = 0.04                # Risk-free rate for B-S
SMA_EXIT_THRESHOLD = 0.02  # Force-exit when 2% below SMA

# Cash allocation for options
OPTIONS_CASH_ALLOCATION = 100_000

# Data range for synthetic backtest (pre-ThetaData era)
DATA_START = "2004-01-01"  # Extra year for SMA warmup
DATA_END = "2014-12-31"
SIM_START = "2005-01-01"   # Start simulation here


# ======================================================================
# SYNTHETIC STRIKE GENERATION
# ======================================================================

def generate_monthly_expirations(start_date: str, end_date: str) -> list:
    """
    Generate synthetic monthly option expiration dates (3rd Friday of each month).
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    expirations = []
    current = start.replace(day=1)

    while current <= end:
        # Find 3rd Friday of this month
        first_day = current.replace(day=1)
        # Find first Friday
        days_until_friday = (4 - first_day.weekday()) % 7
        first_friday = first_day + timedelta(days=days_until_friday)
        # Third Friday is 14 days later
        third_friday = first_friday + timedelta(days=14)

        if start <= third_friday <= end:
            exp_str = third_friday.strftime("%Y-%m-%d")
            expirations.append((exp_str, third_friday.date()))

        # Move to next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)

    return expirations


def generate_strikes(spot: float, step: float = 5.0, num_strikes: int = 40) -> list:
    """
    Generate synthetic strike prices around the current spot price.

    SPY historically has had $5 strike increments for most of its history,
    though $1 increments became more common later.
    """
    # Center around ATM, rounded to nearest step
    atm = round(spot / step) * step

    # Generate strikes above and below
    strikes = []
    for i in range(-num_strikes // 2, num_strikes // 2 + 1):
        strike = atm + i * step
        if strike > 0:
            strikes.append(strike)

    return sorted(strikes)


# ======================================================================
# DATA LOADING
# ======================================================================

def load_yahoo_data():
    """
    Load SPY and VIX historical data from Yahoo Finance.
    """
    print("Fetching SPY data from Yahoo Finance...")
    spy = yf.download("SPY", start=DATA_START, end=DATA_END, progress=False)

    if spy.empty:
        print("ERROR: Failed to fetch SPY data")
        return None, None, None, None, None

    print("Fetching VIX data from Yahoo Finance...")
    vix = yf.download("^VIX", start=DATA_START, end=DATA_END, progress=False)

    if vix.empty:
        print("ERROR: Failed to fetch VIX data")
        return None, None, None, None, None

    # Handle MultiIndex columns from yfinance
    if isinstance(spy.columns, pd.MultiIndex):
        spy.columns = spy.columns.get_level_values(0)
    if isinstance(vix.columns, pd.MultiIndex):
        vix.columns = vix.columns.get_level_values(0)

    # Convert to dict format matching ThetaData client
    spy_by_date = {}
    for idx, row in spy.iterrows():
        date_str = idx.strftime("%Y-%m-%d")
        spy_by_date[date_str] = {
            "bar_date": date_str,
            "open": float(row["Open"]),
            "high": float(row["High"]),
            "low": float(row["Low"]),
            "close": float(row["Close"]),
            "volume": int(row["Volume"]) if not pd.isna(row["Volume"]) else 0,
        }

    vix_data = {}
    for idx, row in vix.iterrows():
        date_str = idx.strftime("%Y-%m-%d")
        vix_data[date_str] = float(row["Close"])

    trading_dates = sorted(spy_by_date.keys())

    # Calculate SMA200
    print("Calculating SMA200...")
    sma200 = {}
    for i in range(199, len(trading_dates)):
        window = [spy_by_date[trading_dates[j]]["close"] for j in range(i - 199, i + 1)]
        sma200[trading_dates[i]] = sum(window) / 200.0

    # Generate monthly expirations
    print("Generating synthetic monthly expirations...")
    monthly_exps = generate_monthly_expirations(DATA_START, DATA_END)

    print(f"  SPY bars: {len(spy_by_date)} ({trading_dates[0]} to {trading_dates[-1]})")
    print(f"  VIX days: {len(vix_data)}")
    first_sma = sorted(sma200.keys())[0] if sma200 else "N/A"
    print(f"  SMA200 from: {first_sma}")
    print(f"  Monthly expirations: {len(monthly_exps)}")

    return spy_by_date, trading_dates, vix_data, sma200, monthly_exps


# ======================================================================
# SYNTHETIC OPTION PRICING
# ======================================================================

def calculate_delta(spot, strike, dte, iv=0.16, rate=0.04, right="C"):
    """Calculate option delta using Black-Scholes."""
    if dte <= 0:
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
    """
    Calculate synthetic option price using Black-Scholes.

    Returns mid price (no bid-ask spread simulation).
    """
    if dte <= 0:
        # Expired - return intrinsic value
        if right == "C":
            return max(0, spot - strike)
        else:
            return max(0, strike - spot)

    t_years = dte / 365.0
    price = black_scholes_price(spot, strike, t_years, rate, iv, right)

    if price is None:
        # Fallback to intrinsic
        if right == "C":
            return max(0, spot - strike)
        else:
            return max(0, strike - spot)

    return price


def apply_synthetic_spread(mid_price, spread_pct=0.02):
    """
    Apply synthetic bid-ask spread to mid price.

    For synthetic testing, we use a 2% spread (tighter than real markets
    during 2008 crisis but reasonable for normal times).
    """
    half_spread = spread_pct / 2
    bid = mid_price * (1 - half_spread)
    ask = mid_price * (1 + half_spread)
    return max(0.01, bid), max(0.01, ask)


# ======================================================================
# EXPIRATION HELPERS
# ======================================================================

def find_best_expiration(entry_date_str, monthly_exps, target=DTE_TARGET,
                         dte_min=DTE_MIN, dte_max=DTE_MAX):
    """Find best expiration date for a given entry date."""
    entry_dt = datetime.strptime(entry_date_str, "%Y-%m-%d").date()
    best_exp = None
    best_dte = 0
    best_diff = 9999

    for exp_str, exp_dt in monthly_exps:
        dte = (exp_dt - entry_dt).days
        if dte < dte_min or dte > dte_max:
            continue
        diff = abs(dte - target)
        if diff < best_diff:
            best_diff = diff
            best_exp = exp_str
            best_dte = dte

    return best_exp, best_dte


# ======================================================================
# SIMULATION ENGINE
# ======================================================================

def run_synthetic_simulation(spy_by_date, trading_dates, vix_data, sma200, monthly_exps):
    """
    Run the synthetic options backtest simulation.

    This simulates the same strategy as delta_capped_backtest.py but uses
    Black-Scholes pricing with VIX-derived IV instead of historical quotes.
    """
    # Initialize
    shares_held = SHARES
    options_cash = float(OPTIONS_CASH_ALLOCATION)
    pending_cash = 0.0
    positions = []

    daily_snapshots = []
    trade_log = []
    entry_skip_reasons = defaultdict(int)
    force_exit_count = 0

    # Start from SIM_START
    start_idx = next((i for i, d in enumerate(trading_dates) if d >= SIM_START), 0)

    print(f"\n{'='*70}")
    print("SYNTHETIC BACKTEST - 80-Delta Call Strategy")
    print(f"{'='*70}")
    print(f"  *** USING SYNTHETIC BLACK-SCHOLES PRICING ***")
    print(f"  Share holdings: {SHARES:,} SPY shares")
    print(f"  Options cash: ${OPTIONS_CASH_ALLOCATION:,}")
    print(f"  Delta cap: {SHARES:,} (= share count)")
    print(f"  Strategy: {DELTA:.0%}-delta calls, ~{DTE_TARGET} DTE")
    print(f"  Rules: PT=+{PT:.0%}, MH={MH}td, SMA exit at -{SMA_EXIT_THRESHOLD:.0%}")
    print(f"  Period: {trading_dates[start_idx]} to {trading_dates[-1]}")
    print(f"{'='*70}")

    for day_idx in range(start_idx, len(trading_dates)):
        today = trading_dates[day_idx]
        bar = spy_by_date[today]
        spot = bar["close"]
        sma_val = sma200.get(today)
        above_sma = (spot > sma_val) if sma_val else False

        # Get VIX for IV estimate
        # VIX is quoted in percentage points (e.g., 20 = 20%)
        # Convert to decimal and apply scaling factor for longer-dated options
        vix_close = vix_data.get(today, 20.0)
        # VIX is 30-day IV, scale down slightly for 120-day options (term structure)
        iv_est = max(0.08, min(0.90, vix_close / 100.0 * 0.95))

        # Value of shares
        shares_value = shares_held * spot

        # 1. Settle yesterday's exit proceeds
        options_cash += pending_cash
        pending_cash = 0.0

        # 2a. Force-exit all positions when SPY >2% below SMA200
        pct_below_sma = (sma_val - spot) / sma_val if sma_val and sma_val > 0 else 0
        if pct_below_sma >= SMA_EXIT_THRESHOLD and positions:
            for pos in positions:
                dte = (datetime.strptime(pos["expiration"], "%Y-%m-%d").date() -
                       datetime.strptime(today, "%Y-%m-%d").date()).days

                # Calculate synthetic exit price
                mid_price = synthetic_option_price(spot, pos["strike"], dte, iv_est)
                bid, _ = apply_synthetic_spread(mid_price)

                proceeds = bid * 100 * pos["quantity"]
                pending_cash += proceeds
                pnl_pct = bid / pos["entry_price"] - 1

                trade_log.append({
                    "entry_date": pos["entry_date"],
                    "exit_date": today,
                    "expiration": pos["expiration"],
                    "strike": pos["strike"],
                    "quantity": pos["quantity"],
                    "entry_price": pos["entry_price"],
                    "exit_price": bid,
                    "pnl_pct": pnl_pct,
                    "pnl_dollar": proceeds - pos["contract_cost"],
                    "days_held": pos["days_held"] + 1,
                    "exit_reason": "SMA",
                    "contract_cost": pos["contract_cost"],
                    "entry_delta": pos["entry_delta"],
                    "entry_iv": pos.get("entry_iv", 0),
                    "exit_iv": iv_est,
                })
                force_exit_count += 1
            positions = []

        # 2b. Process normal exits (PT / MH)
        still_open = []
        for pos in positions:
            pos["days_held"] += 1
            dte = (datetime.strptime(pos["expiration"], "%Y-%m-%d").date() -
                   datetime.strptime(today, "%Y-%m-%d").date()).days

            # Calculate synthetic current price
            mid_price = synthetic_option_price(spot, pos["strike"], dte, iv_est)
            bid, _ = apply_synthetic_spread(mid_price)

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
                    "entry_date": pos["entry_date"],
                    "exit_date": today,
                    "expiration": pos["expiration"],
                    "strike": pos["strike"],
                    "quantity": pos["quantity"],
                    "entry_price": pos["entry_price"],
                    "exit_price": bid,
                    "pnl_pct": pnl_pct,
                    "pnl_dollar": proceeds - pos["contract_cost"],
                    "days_held": pos["days_held"],
                    "exit_reason": exit_reason,
                    "contract_cost": pos["contract_cost"],
                    "entry_delta": pos["entry_delta"],
                    "entry_iv": pos.get("entry_iv", 0),
                    "exit_iv": iv_est,
                })
            else:
                still_open.append(pos)
        positions = still_open

        # 3. Calculate current total options delta
        current_options_delta = 0.0
        for pos in positions:
            dte = (datetime.strptime(pos["expiration"], "%Y-%m-%d").date() -
                   datetime.strptime(today, "%Y-%m-%d").date()).days
            pos_delta = calculate_delta(spot, pos["strike"], dte, iv_est) * pos["quantity"] * 100
            current_options_delta += pos_delta

        # 4. Entry: buy contracts if above SMA200 and delta cap allows
        entered = False
        contracts_entered = 0
        delta_room = SHARES - current_options_delta

        if above_sma and sma_val is not None and delta_room > 80:
            best_exp, dte_cal = find_best_expiration(today, monthly_exps)

            if not best_exp:
                entry_skip_reasons["no_expiration"] += 1
            else:
                t_years = dte_cal / 365.0
                bs_strike = find_strike_for_delta(spot, t_years, RATE, iv_est, DELTA, "C")

                if not bs_strike:
                    entry_skip_reasons["bs_fail"] += 1
                else:
                    # Generate synthetic strikes and find closest
                    strikes = generate_strikes(spot)
                    real_strike = min(strikes, key=lambda s: abs(s - bs_strike))

                    # Calculate synthetic entry price
                    mid_price = synthetic_option_price(spot, real_strike, dte_cal, iv_est)
                    _, ask = apply_synthetic_spread(mid_price)

                    if ask <= 0.01:
                        entry_skip_reasons["no_ask"] += 1
                    else:
                        # Calculate actual delta of this option
                        option_delta = calculate_delta(spot, real_strike, dte_cal, iv_est)

                        # How many contracts can we buy within delta cap?
                        max_by_delta = int(delta_room / (option_delta * 100))

                        # How many can we afford?
                        contract_cost = ask * 100
                        max_by_cash = int(options_cash / contract_cost)

                        # Buy the minimum of delta cap and cash limit (1 at a time)
                        qty = min(max_by_delta, max_by_cash, 1)

                        if qty <= 0:
                            if max_by_delta <= 0:
                                entry_skip_reasons["delta_cap"] += 1
                            else:
                                entry_skip_reasons["no_capital"] += 1
                        else:
                            total_cost = contract_cost * qty
                            options_cash -= total_cost
                            positions.append({
                                "entry_date": today,
                                "expiration": best_exp,
                                "strike": real_strike,
                                "entry_price": ask,
                                "quantity": qty,
                                "contract_cost": total_cost,
                                "days_held": 0,
                                "entry_delta": option_delta,
                                "entry_iv": iv_est,
                                "entry_spot": spot,
                            })
                            entered = True
                            contracts_entered = qty

        # 5. Mark to market
        positions_value = 0.0
        total_options_delta = 0.0
        for pos in positions:
            dte = (datetime.strptime(pos["expiration"], "%Y-%m-%d").date() -
                   datetime.strptime(today, "%Y-%m-%d").date()).days

            mid_price = synthetic_option_price(spot, pos["strike"], dte, iv_est)
            positions_value += mid_price * 100 * pos["quantity"]

            # Update delta
            pos_delta = calculate_delta(spot, pos["strike"], dte, iv_est) * pos["quantity"] * 100
            total_options_delta += pos_delta

        # Total portfolio value
        portfolio_value = shares_value + options_cash + pending_cash + positions_value

        # Total delta
        total_delta = shares_held + total_options_delta
        effective_shares = total_delta

        # Capital deployed in options
        capital_deployed = sum(p["contract_cost"] for p in positions)
        n_contracts = sum(p["quantity"] for p in positions)

        daily_snapshots.append({
            "date": today,
            "portfolio_value": portfolio_value,
            "shares_value": shares_value,
            "options_value": positions_value,
            "options_cash": options_cash + pending_cash,
            "n_positions": len(positions),
            "n_contracts": n_contracts,
            "capital_deployed": capital_deployed,
            "shares_delta": shares_held,
            "options_delta": total_options_delta,
            "total_delta": total_delta,
            "effective_leverage": total_delta / shares_held if shares_held > 0 else 0,
            "above_sma": above_sma,
            "spy_close": spot,
            "vix_close": vix_close,
            "iv_used": iv_est,
            "entered": entered,
            "contracts_entered": contracts_entered,
            "sma200": sma_val,
        })

        # Progress
        real_idx = day_idx - start_idx
        total_days = len(trading_dates) - start_idx
        if (real_idx + 1) % 500 == 0 or real_idx == 0:
            print(f"  [{real_idx+1}/{total_days}] {today}  "
                  f"Portfolio=${portfolio_value:,.0f}  "
                  f"Shares=${shares_value:,.0f}  Options=${positions_value:,.0f}  "
                  f"VIX={vix_close:.1f}  Delta={total_delta:,.0f}")

    print(f"\n  Trades: {len(trade_log)}  |  Force-exits: {force_exit_count}")
    print(f"  Entry skips: {dict(entry_skip_reasons)}")

    return daily_snapshots, trade_log


# ======================================================================
# METRICS CALCULATION
# ======================================================================

def compute_metrics(snapshots, trade_log):
    """Compute portfolio metrics."""
    df = pd.DataFrame(snapshots)
    tdf = pd.DataFrame(trade_log) if trade_log else pd.DataFrame()

    n_days = len(df)
    years = n_days / 252.0

    # Starting values
    start_shares = df["shares_value"].iloc[0]
    start_options_cash = df["options_cash"].iloc[0]
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
    max_dd_date = df.loc[drawdown.idxmin(), "date"]

    # Find drawdown periods
    df["drawdown"] = drawdown

    # SPY B&H (shares only)
    spy_start = df["spy_close"].iloc[0]
    spy_end = df["spy_close"].iloc[-1]
    spy_total = spy_end / spy_start - 1
    spy_cagr = (spy_end / spy_start) ** (1 / years) - 1 if years > 0 else 0
    df["spy_ret"] = df["spy_close"].pct_change().fillna(0)
    spy_sharpe = (df["spy_ret"].mean() / df["spy_ret"].std()) * np.sqrt(252) if df["spy_ret"].std() > 0 else 0
    spy_dd = (df["spy_close"] / df["spy_close"].cummax() - 1).min()

    # Shares-only B&H
    shares_only_start = start_shares
    shares_only_end = df["shares_value"].iloc[-1]
    shares_only_return = shares_only_end / shares_only_start - 1
    shares_only_cagr = (shares_only_end / shares_only_start) ** (1 / years) - 1 if years > 0 else 0

    # Options-only performance
    options_start = start_options_cash
    options_end = df["options_cash"].iloc[-1] + df["options_value"].iloc[-1]
    options_return = options_end / options_start - 1 if options_start > 0 else 0
    options_cagr = (options_end / options_start) ** (1 / years) - 1 if years > 0 and options_start > 0 else 0

    # Delta stats
    avg_options_delta = df["options_delta"].mean()
    max_options_delta = df["options_delta"].max()
    avg_total_delta = df["total_delta"].mean()
    avg_leverage = df["effective_leverage"].mean()

    # VIX stats
    avg_vix = df["vix_close"].mean()
    max_vix = df["vix_close"].max()

    # Trade stats
    trade_stats = {}
    if len(tdf) > 0:
        wins = tdf[tdf["pnl_pct"] > 0]
        losses = tdf[tdf["pnl_pct"] <= 0]
        trade_stats = {
            "n_trades": len(tdf),
            "total_contracts": tdf["quantity"].sum(),
            "win_rate": len(wins) / len(tdf),
            "mean_ret": tdf["pnl_pct"].mean(),
            "med_ret": tdf["pnl_pct"].median(),
            "avg_win": wins["pnl_pct"].mean() if len(wins) > 0 else 0,
            "avg_loss": losses["pnl_pct"].mean() if len(losses) > 0 else 0,
            "total_pnl": tdf["pnl_dollar"].sum(),
            "pt_exits": len(tdf[tdf["exit_reason"] == "PT"]),
            "mh_exits": len(tdf[tdf["exit_reason"] == "MH"]),
            "sma_exits": len(tdf[tdf["exit_reason"] == "SMA"]),
            "avg_days": tdf["days_held"].mean(),
            "avg_cost": tdf["contract_cost"].mean(),
            "avg_qty": tdf["quantity"].mean(),
        }

    # Yearly performance
    df["year"] = pd.to_datetime(df["date"]).dt.year
    yearly = {}
    for year in sorted(df["year"].unique()):
        ydf = df[df["year"] == year]
        y_start = ydf["portfolio_value"].iloc[0]
        y_end = ydf["portfolio_value"].iloc[-1]
        y_ret = y_end / y_start - 1

        # Shares-only for this year
        y_shares_start = ydf["shares_value"].iloc[0]
        y_shares_end = ydf["shares_value"].iloc[-1]
        y_shares_ret = y_shares_end / y_shares_start - 1

        # SPY for this year
        y_spy_start = ydf["spy_close"].iloc[0]
        y_spy_end = ydf["spy_close"].iloc[-1]
        y_spy_ret = y_spy_end / y_spy_start - 1

        # Max drawdown for year
        y_cummax = ydf["portfolio_value"].cummax()
        y_dd = (ydf["portfolio_value"] / y_cummax - 1).min()

        y_trades = 0
        if len(tdf) > 0:
            y_trades = len(tdf[pd.to_datetime(tdf["exit_date"]).dt.year == year])

        yearly[year] = {
            "ret": y_ret,
            "shares_ret": y_shares_ret,
            "spy_ret": y_spy_ret,
            "alpha": y_ret - y_shares_ret,
            "max_dd": y_dd,
            "trades": y_trades,
            "avg_contracts": ydf["n_contracts"].mean(),
            "avg_options_delta": ydf["options_delta"].mean(),
            "avg_vix": ydf["vix_close"].mean(),
            "end_val": y_end,
        }

    return {
        "years": years,
        "start_val": start_val,
        "end_val": end_val,
        "total_return": total_return,
        "cagr": cagr,
        "sharpe": sharpe,
        "sortino": sortino,
        "max_dd": max_dd,
        "max_dd_date": max_dd_date,
        "spy_cagr": spy_cagr,
        "spy_sharpe": spy_sharpe,
        "spy_dd": spy_dd,
        "shares_only_return": shares_only_return,
        "shares_only_cagr": shares_only_cagr,
        "options_return": options_return,
        "options_cagr": options_cagr,
        "avg_options_delta": avg_options_delta,
        "max_options_delta": max_options_delta,
        "avg_total_delta": avg_total_delta,
        "avg_leverage": avg_leverage,
        "avg_vix": avg_vix,
        "max_vix": max_vix,
        "trades": trade_stats,
        "yearly": yearly,
        "snapshots_df": df,
        "trade_df": tdf,
    }


# ======================================================================
# OUTPUT
# ======================================================================

def print_results(metrics):
    """Print comprehensive results."""
    m = metrics
    W = 80

    print(f"\n{'=' * W}")
    print("SYNTHETIC BACKTEST RESULTS -- 80-DELTA CALL STRATEGY")
    print(f"{'=' * W}")
    print()
    print("  *** CAUTION: SYNTHETIC PRICING -- NOT ACTUAL HISTORICAL QUOTES ***")
    print("  Uses Black-Scholes with VIX as IV proxy. Results are approximate.")
    print()
    print(f"  Period:         {SIM_START} to {DATA_END}")
    print(f"  Share Holdings: {SHARES:,} SPY shares")
    print(f"  Options Cash:   ${OPTIONS_CASH_ALLOCATION:,}")
    print(f"  Delta Cap:      {SHARES:,} (max options delta = share count)")
    print(f"  Strategy:       {DELTA:.0%}-delta calls, ~{DTE_TARGET} DTE")
    print(f"  Rules:          PT=+{PT:.0%}, MH={MH}td, SMA exit at -{SMA_EXIT_THRESHOLD:.0%}")

    # Portfolio Performance
    print(f"\n{'-' * W}")
    print("COMBINED PORTFOLIO PERFORMANCE (Shares + Options)")
    print(f"{'-' * W}")

    rows = [
        ("Starting Value", f"${m['start_val']:>15,.0f}"),
        ("Ending Value", f"${m['end_val']:>15,.0f}"),
        ("Total Return", f"{m['total_return']:>+15.1%}"),
        ("CAGR", f"{m['cagr']:>+15.1%}"),
        ("Sharpe Ratio", f"{m['sharpe']:>15.2f}"),
        ("Sortino Ratio", f"{m['sortino']:>15.2f}"),
        ("Max Drawdown", f"{m['max_dd']:>15.1%}"),
        ("Max DD Date", f"{m['max_dd_date']:>15}"),
    ]
    for name, val in rows:
        print(f"  {name:<25} {val}")

    # Comparison to Benchmarks
    print(f"\n{'-' * W}")
    print("COMPARISON TO BENCHMARKS")
    print(f"{'-' * W}")
    print(f"  {'Metric':<25} {'Combined':>15} {'Shares-Only':>15} {'SPY B&H':>15}")
    print(f"  {'-' * 72}")
    print(f"  {'CAGR':<25} {m['cagr']:>+14.1%} {m['shares_only_cagr']:>+14.1%} {m['spy_cagr']:>+14.1%}")
    print(f"  {'Total Return':<25} {m['total_return']:>+14.1%} {m['shares_only_return']:>+14.1%} {'--':>15}")
    print(f"  {'Sharpe':<25} {m['sharpe']:>15.2f} {'--':>15} {m['spy_sharpe']:>15.2f}")
    print(f"  {'Max DD':<25} {m['max_dd']:>14.1%} {'--':>15} {m['spy_dd']:>14.1%}")

    # Options Component
    print(f"\n{'-' * W}")
    print("OPTIONS COMPONENT ONLY")
    print(f"{'-' * W}")
    print(f"  Starting Cash:          ${OPTIONS_CASH_ALLOCATION:,}")
    print(f"  Options CAGR:           {m['options_cagr']:+.1%}")
    print(f"  Options Total Return:   {m['options_return']:+.1%}")

    # Delta Exposure
    print(f"\n{'-' * W}")
    print("DELTA EXPOSURE")
    print(f"{'-' * W}")
    print(f"  Share Delta (constant): {SHARES:,}")
    print(f"  Avg Options Delta:      {m['avg_options_delta']:,.0f}")
    print(f"  Max Options Delta:      {m['max_options_delta']:,.0f}")
    print(f"  Avg Total Delta:        {m['avg_total_delta']:,.0f}")
    print(f"  Avg Effective Leverage: {m['avg_leverage']:.2f}x")
    print(f"  Options as % of Shares: {m['avg_options_delta']/SHARES*100:.1f}%")

    # VIX/IV Stats
    print(f"\n{'-' * W}")
    print("VOLATILITY ENVIRONMENT")
    print(f"{'-' * W}")
    print(f"  Average VIX:            {m['avg_vix']:.1f}")
    print(f"  Max VIX:                {m['max_vix']:.1f}")

    # Trade Statistics
    t = m["trades"]
    if t:
        print(f"\n{'-' * W}")
        print("TRADE STATISTICS")
        print(f"{'-' * W}")
        trade_rows = [
            ("Total Trades", f"{t['n_trades']}"),
            ("Total Contracts", f"{t['total_contracts']}"),
            ("Win Rate", f"{t['win_rate']:.1%}"),
            ("Mean Return", f"{t['mean_ret']:+.1%}"),
            ("Median Return", f"{t['med_ret']:+.1%}"),
            ("Avg Win", f"{t['avg_win']:+.1%}"),
            ("Avg Loss", f"{t['avg_loss']:+.1%}"),
            ("Total P&L", f"${t['total_pnl']:+,.0f}"),
            ("PT Exits", f"{t['pt_exits']} ({t['pt_exits']/t['n_trades']:.0%})"),
            ("MH Exits", f"{t['mh_exits']} ({t['mh_exits']/t['n_trades']:.0%})"),
            ("SMA Exits", f"{t['sma_exits']}"),
            ("Avg Days Held", f"{t['avg_days']:.0f}"),
            ("Avg Contract Cost", f"${t['avg_cost']:,.0f}"),
        ]
        for name, val in trade_rows:
            print(f"  {name:<25} {val}")

    # Year-by-Year
    print(f"\n{'-' * W}")
    print("YEAR-BY-YEAR PERFORMANCE")
    print(f"{'-' * W}")
    print(f"\n  {'Year':<6} {'Combined':>10} {'Shares':>10} {'Alpha':>10} {'Max DD':>10} {'Trades':>8} {'Avg VIX':>10}")
    print(f"  {'-' * 70}")
    for year, y in sorted(m["yearly"].items()):
        print(f"  {year:<6} {y['ret']:>+9.1%} {y['shares_ret']:>+9.1%} {y['alpha']:>+9.1%} "
              f"{y['max_dd']:>9.1%} {y['trades']:>8} {y['avg_vix']:>10.1f}")

    # Crisis Period Analysis
    print(f"\n{'-' * W}")
    print("CRISIS PERIOD ANALYSIS (2008-2009)")
    print(f"{'-' * W}")
    crisis_years = [2008, 2009]
    for year in crisis_years:
        if year in m["yearly"]:
            y = m["yearly"][year]
            print(f"\n  {year}:")
            print(f"    Combined Return:  {y['ret']:+.1%}")
            print(f"    Shares-Only:      {y['shares_ret']:+.1%}")
            print(f"    Alpha:            {y['alpha']:+.1%}")
            print(f"    Max Drawdown:     {y['max_dd']:.1%}")
            print(f"    Trades:           {y['trades']}")
            print(f"    Avg VIX:          {y['avg_vix']:.1f}")

    # Monthly Returns Table
    df = m["snapshots_df"]
    df["month"] = pd.to_datetime(df["date"]).dt.to_period("M")
    monthly = df.groupby("month").agg(
        start_val=("portfolio_value", "first"),
        end_val=("portfolio_value", "last"),
    )
    monthly["return"] = monthly["end_val"] / monthly["start_val"] - 1
    mdf = monthly.reset_index()
    mdf["year"] = mdf["month"].dt.year
    mdf["mon"] = mdf["month"].dt.month

    month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

    print(f"\n{'-' * W}")
    print("MONTHLY RETURNS")
    print(f"{'-' * W}")
    print(f"\n  {'Year':<6}", end="")
    for mn in month_names:
        print(f" {mn:>6}", end="")
    print(f" {'Total':>8}")
    print(f"  {'-' * 85}")

    for year in sorted(mdf["year"].unique()):
        ydata = mdf[mdf["year"] == year]
        print(f"  {year:<6}", end="")
        ytot = 1.0
        for mo in range(1, 13):
            mrow = ydata[ydata["mon"] == mo]
            if len(mrow) > 0:
                r = mrow["return"].iloc[0]
                ytot *= (1 + r)
                print(f" {r:>+5.1%}", end="")
            else:
                print(f" {'--':>6}", end="")
        print(f" {ytot - 1:>+7.1%}")

    # Summary
    print(f"\n{'=' * W}")
    print("SUMMARY (SYNTHETIC BACKTEST)")
    print(f"{'=' * W}")
    print(f"  Combined Portfolio ({SHARES:,} shares + delta-capped options):")
    print(f"    ${m['start_val']:,.0f} -> ${m['end_val']:,.0f}")
    print(f"    CAGR: {m['cagr']:+.1%}  |  Sharpe: {m['sharpe']:.2f}  |  Max DD: {m['max_dd']:.1%}")
    print(f"\n  Shares-Only B&H: CAGR {m['shares_only_cagr']:+.1%}")
    print(f"  Added by Options: {m['cagr'] - m['shares_only_cagr']:+.1%} CAGR")
    print(f"\n  *** REMINDER: These are SYNTHETIC results, not historical quotes ***")
    print(f"  The 2008-2009 crisis behavior provides directional guidance only.")
    print(f"{'=' * W}")


def export_to_csv(metrics, output_dir=None):
    """Export results to CSV files for further analysis."""
    if output_dir is None:
        output_dir = _this_dir

    # Daily snapshots
    df = metrics["snapshots_df"]
    snapshots_path = os.path.join(output_dir, "synthetic_backtest_daily.csv")
    df.to_csv(snapshots_path, index=False)
    print(f"\nExported daily snapshots to: {snapshots_path}")

    # Trade log
    tdf = metrics["trade_df"]
    if len(tdf) > 0:
        trades_path = os.path.join(output_dir, "synthetic_backtest_trades.csv")
        tdf.to_csv(trades_path, index=False)
        print(f"Exported trade log to: {trades_path}")

    # Yearly summary
    yearly_data = []
    for year, y in sorted(metrics["yearly"].items()):
        yearly_data.append({
            "year": year,
            "combined_return": y["ret"],
            "shares_return": y["shares_ret"],
            "spy_return": y["spy_ret"],
            "alpha": y["alpha"],
            "max_dd": y["max_dd"],
            "trades": y["trades"],
            "avg_contracts": y["avg_contracts"],
            "avg_options_delta": y["avg_options_delta"],
            "avg_vix": y["avg_vix"],
            "end_value": y["end_val"],
        })
    yearly_df = pd.DataFrame(yearly_data)
    yearly_path = os.path.join(output_dir, "synthetic_backtest_yearly.csv")
    yearly_df.to_csv(yearly_path, index=False)
    print(f"Exported yearly summary to: {yearly_path}")


# ======================================================================
# MAIN
# ======================================================================

def main():
    print("=" * 80)
    print("Synthetic Options Backtester - 80-Delta Call Strategy")
    print("Pre-ThetaData Era (2005-2014)")
    print("=" * 80)
    print()
    print("*** THIS IS A SYNTHETIC BACKTEST USING BLACK-SCHOLES PRICING ***")
    print("*** VIX is used as an IV proxy - results are approximate only ***")
    print()

    # Load data from Yahoo Finance
    data = load_yahoo_data()
    if data[0] is None:
        print("\nERROR: Failed to load data from Yahoo Finance")
        return

    spy_by_date, trading_dates, vix_data, sma200, monthly_exps = data

    # Run simulation
    snapshots, trade_log = run_synthetic_simulation(
        spy_by_date, trading_dates, vix_data, sma200, monthly_exps
    )

    if not snapshots:
        print("\nInsufficient data for simulation.")
        return

    # Compute and print metrics
    metrics = compute_metrics(snapshots, trade_log)
    print_results(metrics)

    # Export to CSV
    export_to_csv(metrics)

    print("\nSynthetic backtest complete.")


if __name__ == "__main__":
    main()
