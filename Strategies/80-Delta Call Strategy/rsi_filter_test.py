"""
RSI Filter Test for 80-Delta Call Strategy
===========================================
Tests whether adding an RSI filter improves the strategy by avoiding
entries when RSI is elevated (overbought).

Hypothesis: Avoiding entries when RSI > 70 or > 80 may reduce drawdowns
by not buying into short-term overbought conditions.

Counter-hypothesis: The SMA200 filter already captures trend, and RSI
may cause missed entries during strong rallies.

Test scenarios:
  A: Base strategy (no RSI filter)
  B: Skip entries when RSI >= 70
  C: Skip entries when RSI >= 80

Usage:
    python rsi_filter_test.py
"""

import os
import sys
import math
import logging
from datetime import datetime, date, timedelta
from collections import defaultdict

import numpy as np
import pandas as pd

_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_dir = os.path.dirname(os.path.dirname(_this_dir))
sys.path.insert(0, _project_dir)

from backtest.thetadata_client import ThetaDataClient
from backtest.black_scholes import black_scholes_price, find_strike_for_delta

log = logging.getLogger("rsi_filter")

# ======================================================================
# PARAMETERS
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

DATA_START = "2014-01-01"
DATA_END = "2026-01-31"
SIM_START = "2015-03-01"

RSI_PERIOD = 14  # Standard RSI period


# ======================================================================
# RSI CALCULATION
# ======================================================================

def calculate_rsi(prices, period=14):
    """
    Calculate RSI (Relative Strength Index) for a price series.

    Args:
        prices: List of closing prices (oldest first)
        period: RSI period (default 14)

    Returns:
        RSI value (0-100) or None if insufficient data
    """
    if len(prices) < period + 1:
        return None

    # Calculate price changes
    changes = [prices[i] - prices[i-1] for i in range(1, len(prices))]

    # Get last 'period' changes
    recent_changes = changes[-(period):]

    gains = [c if c > 0 else 0 for c in recent_changes]
    losses = [-c if c < 0 else 0 for c in recent_changes]

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))

    return rsi


def calculate_rsi_series(spy_by_date, trading_dates, period=14):
    """
    Calculate RSI for all trading dates.

    Returns:
        Dict mapping date -> RSI value
    """
    rsi_by_date = {}

    for i in range(period + 1, len(trading_dates)):
        today = trading_dates[i]

        # Get last (period + 1) closing prices
        prices = []
        for j in range(i - period, i + 1):
            d = trading_dates[j]
            prices.append(spy_by_date[d]["close"])

        rsi = calculate_rsi(prices, period)
        if rsi is not None:
            rsi_by_date[today] = rsi

    return rsi_by_date


# ======================================================================
# HELPERS (copied from delta_capped_backtest.py)
# ======================================================================

def is_monthly_opex(exp_str):
    exp_dt = datetime.strptime(exp_str, "%Y-%m-%d").date()
    if exp_dt.weekday() != 4:
        return False
    return 15 <= exp_dt.day <= 21


def find_best_expiration(entry_date_str, monthly_exps_dates, target=DTE_TARGET,
                         dte_min=DTE_MIN, dte_max=DTE_MAX):
    entry_dt = datetime.strptime(entry_date_str, "%Y-%m-%d").date()
    best_exp = None
    best_dte = 0
    best_diff = 9999
    for exp_str, exp_dt in monthly_exps_dates:
        dte = (exp_dt - entry_dt).days
        if dte < dte_min or dte > dte_max:
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


def calculate_delta(spot, strike, dte, iv=0.16, rate=0.04, right="C"):
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


# ======================================================================
# DATA LOADING
# ======================================================================

def load_all_data(client):
    print("Loading SPY bars...")
    spy_bars = client.fetch_spy_bars(DATA_START, DATA_END)
    spy_by_date = {b["bar_date"]: b for b in spy_bars}
    trading_dates = sorted(spy_by_date.keys())

    print("Loading VIX history...")
    vix_data = client.fetch_vix_history(DATA_START, DATA_END)

    # SMA200
    sma200 = {}
    for i in range(199, len(trading_dates)):
        window = [spy_by_date[trading_dates[j]]["close"] for j in range(i - 199, i + 1)]
        sma200[trading_dates[i]] = sum(window) / 200.0

    # RSI
    print("Calculating RSI(14)...")
    rsi_data = calculate_rsi_series(spy_by_date, trading_dates, RSI_PERIOD)

    print("Loading SPY expirations...")
    all_exps = client.get_expirations("SPY")
    monthly_exps = [(e, datetime.strptime(e, "%Y-%m-%d").date())
                    for e in all_exps if is_monthly_opex(e)]
    monthly_exps.sort(key=lambda x: x[1])

    print(f"  SPY bars: {len(spy_bars)} ({trading_dates[0]} to {trading_dates[-1]})")
    print(f"  VIX days: {len(vix_data)}")
    first_sma = sorted(sma200.keys())[0] if sma200 else "N/A"
    print(f"  SMA200 from: {first_sma}")
    first_rsi = sorted(rsi_data.keys())[0] if rsi_data else "N/A"
    print(f"  RSI(14) from: {first_rsi}")
    print(f"  Monthly expirations: {len(monthly_exps)}")

    return spy_by_date, trading_dates, vix_data, sma200, rsi_data, monthly_exps


# ======================================================================
# SIMULATION ENGINE
# ======================================================================

def run_simulation(client, spy_by_date, trading_dates, vix_data, sma200,
                   rsi_data, monthly_exps, rsi_max=None, label=""):
    """
    Run the delta-capped simulation with optional RSI filter.

    Args:
        rsi_max: Maximum RSI for entry (None = no filter, 70 = skip if RSI >= 70)
    """
    shares_held = SHARES
    options_cash = float(OPTIONS_CASH_ALLOCATION)
    pending_cash = 0.0
    positions = []

    contract_eod = {}
    strikes_cache = {}

    daily_snapshots = []
    trade_log = []
    entry_skip_reasons = defaultdict(int)
    force_exit_count = 0
    rsi_skip_count = 0

    start_idx = next((i for i, d in enumerate(trading_dates) if d >= SIM_START), 0)

    rsi_label = f"RSI < {rsi_max}" if rsi_max else "No RSI filter"
    print(f"\n{'='*70}")
    print(f"Config: {label or rsi_label}")
    print(f"  Share holdings: {SHARES:,} SPY shares")
    print(f"  Options cash: ${OPTIONS_CASH_ALLOCATION:,}")
    print(f"  RSI filter: {rsi_label}")
    print(f"  Period: {trading_dates[start_idx]} to {trading_dates[-1]}")
    print(f"{'='*70}")

    for day_idx in range(start_idx, len(trading_dates)):
        today = trading_dates[day_idx]
        bar = spy_by_date[today]
        spot = bar["close"]
        sma_val = sma200.get(today)
        above_sma = (spot > sma_val) if sma_val else False
        vix = vix_data.get(today, 20.0)
        iv_est = max(0.08, min(0.90, vix / 100.0))
        rsi = rsi_data.get(today)

        shares_value = shares_held * spot

        # Settle yesterday's exit proceeds
        options_cash += pending_cash
        pending_cash = 0.0

        # Force-exit all positions when SPY >2% below SMA200
        pct_below_sma = (sma_val - spot) / sma_val if sma_val and sma_val > 0 else 0
        if pct_below_sma >= SMA_EXIT_THRESHOLD and positions:
            for pos in positions:
                ckey = (pos["expiration"], pos["strike"])
                eod = contract_eod.get(ckey, {}).get(today)
                bid, _ = get_bid_ask(eod)
                if bid is None or bid <= 0:
                    intrinsic = max(0, spot - pos["strike"])
                    bid = intrinsic * 0.998 if intrinsic > 0 else 0.001
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
                    "entry_rsi": pos.get("entry_rsi"),
                })
                force_exit_count += 1
            positions = []

        # Process normal exits (PT / MH)
        still_open = []
        for pos in positions:
            pos["days_held"] += 1
            ckey = (pos["expiration"], pos["strike"])
            eod = contract_eod.get(ckey, {}).get(today)
            bid, _ = get_bid_ask(eod)
            if bid is None or bid <= 0:
                intrinsic = max(0, spot - pos["strike"])
                bid = intrinsic * 0.998 if intrinsic > 0 else 0.001

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
                    "entry_rsi": pos.get("entry_rsi"),
                })
            else:
                still_open.append(pos)
        positions = still_open

        # Calculate current total options delta
        current_options_delta = 0.0
        for pos in positions:
            dte = (datetime.strptime(pos["expiration"], "%Y-%m-%d").date() -
                   datetime.strptime(today, "%Y-%m-%d").date()).days
            pos_delta = calculate_delta(spot, pos["strike"], dte, iv_est) * pos["quantity"] * 100
            current_options_delta += pos_delta

        # Entry logic
        entered = False
        contracts_entered = 0
        delta_room = SHARES - current_options_delta

        # Check RSI filter
        rsi_ok = True
        if rsi_max is not None and rsi is not None:
            if rsi >= rsi_max:
                rsi_ok = False
                rsi_skip_count += 1

        if above_sma and sma_val is not None and delta_room > 80 and rsi_ok:
            best_exp, dte_cal = find_best_expiration(today, monthly_exps)
            if not best_exp:
                entry_skip_reasons["no_expiration"] += 1
            else:
                t_years = dte_cal / 365.0
                bs_strike = find_strike_for_delta(spot, t_years, RATE, iv_est, DELTA, "C")
                if not bs_strike:
                    entry_skip_reasons["bs_fail"] += 1
                else:
                    if best_exp not in strikes_cache:
                        strikes_cache[best_exp] = client.get_strikes("SPY", best_exp)
                    strikes = strikes_cache[best_exp]
                    if not strikes:
                        entry_skip_reasons["no_strikes"] += 1
                    else:
                        real_strike = min(strikes, key=lambda s: abs(s - bs_strike))
                        ckey = (best_exp, real_strike)
                        if ckey not in contract_eod:
                            data = client.prefetch_option_life(
                                "SPY", best_exp, real_strike, "C", today
                            )
                            contract_eod[ckey] = {r["bar_date"]: r for r in data}
                        eod = contract_eod[ckey].get(today)
                        _, ask = get_bid_ask(eod)

                        if ask is None or ask <= 0:
                            entry_skip_reasons["no_ask"] += 1
                        else:
                            option_delta = calculate_delta(spot, real_strike, dte_cal, iv_est)
                            max_by_delta = int(delta_room / (option_delta * 100))
                            contract_cost = ask * 100
                            max_by_cash = int(options_cash / contract_cost)
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
                                    "entry_rsi": rsi,
                                })
                                entered = True
                                contracts_entered = qty

        # Mark to market
        positions_value = 0.0
        total_options_delta = 0.0
        for pos in positions:
            ckey = (pos["expiration"], pos["strike"])
            eod = contract_eod.get(ckey, {}).get(today)
            bid, ask = get_bid_ask(eod)
            if bid and ask:
                mid = (bid + ask) / 2.0
            else:
                mid = max(0, spot - pos["strike"])
            positions_value += mid * 100 * pos["quantity"]

            dte = (datetime.strptime(pos["expiration"], "%Y-%m-%d").date() -
                   datetime.strptime(today, "%Y-%m-%d").date()).days
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
            "n_positions": len(positions),
            "n_contracts": sum(p["quantity"] for p in positions),
            "total_delta": total_delta,
            "above_sma": above_sma,
            "spy_close": spot,
            "rsi": rsi,
            "entered": entered,
        })

        # Progress
        real_idx = day_idx - start_idx
        total_days = len(trading_dates) - start_idx
        if (real_idx + 1) % 500 == 0 or real_idx == 0:
            rsi_str = f"RSI={rsi:.0f}" if rsi else "RSI=N/A"
            print(f"  [{real_idx+1}/{total_days}] {today}  "
                  f"Portfolio=${portfolio_value:,.0f}  {rsi_str}  "
                  f"Delta={total_delta:,.0f}")

    print(f"\n  Trades: {len(trade_log)}  |  Force-exits: {force_exit_count}")
    print(f"  Entry skips: {dict(entry_skip_reasons)}")
    print(f"  RSI skips: {rsi_skip_count}")

    return daily_snapshots, trade_log


# ======================================================================
# ANALYSIS
# ======================================================================

def compute_metrics(snapshots, trade_log, label=""):
    df = pd.DataFrame(snapshots)
    tdf = pd.DataFrame(trade_log) if trade_log else pd.DataFrame()

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

    # Shares-only
    shares_start = df["shares_value"].iloc[0]
    shares_end = df["shares_value"].iloc[-1]
    shares_cagr = (shares_end / shares_start) ** (1 / years) - 1 if years > 0 else 0

    # Trade stats
    trade_stats = {}
    if len(tdf) > 0:
        wins = tdf[tdf["pnl_pct"] > 0]
        losses = tdf[tdf["pnl_pct"] <= 0]
        trade_stats = {
            "n_trades": len(tdf),
            "win_rate": len(wins) / len(tdf),
            "mean_ret": tdf["pnl_pct"].mean(),
            "total_pnl": tdf["pnl_dollar"].sum(),
            "pt_exits": len(tdf[tdf["exit_reason"] == "PT"]),
            "mh_exits": len(tdf[tdf["exit_reason"] == "MH"]),
            "sma_exits": len(tdf[tdf["exit_reason"] == "SMA"]),
        }

    # RSI stats at entry
    rsi_stats = {}
    if len(tdf) > 0 and "entry_rsi" in tdf.columns:
        valid_rsi = tdf[tdf["entry_rsi"].notna()]
        if len(valid_rsi) > 0:
            rsi_stats = {
                "avg_entry_rsi": valid_rsi["entry_rsi"].mean(),
                "min_entry_rsi": valid_rsi["entry_rsi"].min(),
                "max_entry_rsi": valid_rsi["entry_rsi"].max(),
            }

    return {
        "label": label,
        "years": years,
        "start_val": start_val,
        "end_val": end_val,
        "total_return": total_return,
        "cagr": cagr,
        "sharpe": sharpe,
        "sortino": sortino,
        "max_dd": max_dd,
        "shares_cagr": shares_cagr,
        "trades": trade_stats,
        "rsi_stats": rsi_stats,
        "snapshots_df": df,
        "trade_df": tdf,
    }


# ======================================================================
# OUTPUT
# ======================================================================

def print_comparison(metrics_list):
    W = 95

    print(f"\n{'=' * W}")
    print("RSI FILTER TEST -- COMPARISON")
    print(f"{'=' * W}")

    for m in metrics_list:
        print(f"  {m['label']}")

    # Portfolio Performance
    print(f"\n{'-' * W}")
    print("PORTFOLIO PERFORMANCE")
    print(f"{'-' * W}")

    headers = ["Metric"] + [m["label"].split(":")[0] for m in metrics_list]
    print(f"  {headers[0]:<25}", end="")
    for h in headers[1:]:
        print(f" {h:>18}", end="")
    print()
    print(f"  {'-' * 80}")

    rows = [
        ("Ending Value", [f"${m['end_val']:,.0f}" for m in metrics_list]),
        ("Total Return", [f"{m['total_return']:+.1%}" for m in metrics_list]),
        ("CAGR", [f"{m['cagr']:+.1%}" for m in metrics_list]),
        ("Sharpe", [f"{m['sharpe']:.2f}" for m in metrics_list]),
        ("Sortino", [f"{m['sortino']:.2f}" for m in metrics_list]),
        ("Max Drawdown", [f"{m['max_dd']:.1%}" for m in metrics_list]),
        ("Alpha vs Shares", [f"{m['cagr'] - m['shares_cagr']:+.1%}" for m in metrics_list]),
    ]

    for name, vals in rows:
        print(f"  {name:<25}", end="")
        for v in vals:
            print(f" {v:>18}", end="")
        print()

    # Trade Statistics
    print(f"\n{'-' * W}")
    print("TRADE STATISTICS")
    print(f"{'-' * W}")

    trade_rows = [
        ("Total Trades", [str(m["trades"].get("n_trades", 0)) for m in metrics_list]),
        ("Win Rate", [f"{m['trades'].get('win_rate', 0):.1%}" for m in metrics_list]),
        ("Mean Return", [f"{m['trades'].get('mean_ret', 0):+.1%}" for m in metrics_list]),
        ("Total P&L", [f"${m['trades'].get('total_pnl', 0):+,.0f}" for m in metrics_list]),
        ("PT Exits", [str(m["trades"].get("pt_exits", 0)) for m in metrics_list]),
        ("SMA Exits", [str(m["trades"].get("sma_exits", 0)) for m in metrics_list]),
    ]

    print(f"  {headers[0]:<25}", end="")
    for h in headers[1:]:
        print(f" {h:>18}", end="")
    print()
    print(f"  {'-' * 80}")

    for name, vals in trade_rows:
        print(f"  {name:<25}", end="")
        for v in vals:
            print(f" {v:>18}", end="")
        print()

    # RSI at Entry
    print(f"\n{'-' * W}")
    print("RSI AT ENTRY (for trades that occurred)")
    print(f"{'-' * W}")

    rsi_rows = [
        ("Avg Entry RSI", [f"{m['rsi_stats'].get('avg_entry_rsi', 0):.1f}" for m in metrics_list]),
        ("Min Entry RSI", [f"{m['rsi_stats'].get('min_entry_rsi', 0):.1f}" for m in metrics_list]),
        ("Max Entry RSI", [f"{m['rsi_stats'].get('max_entry_rsi', 0):.1f}" for m in metrics_list]),
    ]

    print(f"  {headers[0]:<25}", end="")
    for h in headers[1:]:
        print(f" {h:>18}", end="")
    print()
    print(f"  {'-' * 80}")

    for name, vals in rsi_rows:
        print(f"  {name:<25}", end="")
        for v in vals:
            print(f" {v:>18}", end="")
        print()

    # Summary
    print(f"\n{'=' * W}")
    print("SUMMARY")
    print(f"{'=' * W}")

    base = metrics_list[0]
    print(f"\n  Base Strategy (no RSI filter):")
    print(f"    CAGR: {base['cagr']:+.1%}  |  Sharpe: {base['sharpe']:.2f}  |  Max DD: {base['max_dd']:.1%}")
    print(f"    Trades: {base['trades'].get('n_trades', 0)}")

    for m in metrics_list[1:]:
        diff_cagr = m['cagr'] - base['cagr']
        diff_sharpe = m['sharpe'] - base['sharpe']
        diff_dd = m['max_dd'] - base['max_dd']
        diff_trades = m['trades'].get('n_trades', 0) - base['trades'].get('n_trades', 0)

        print(f"\n  {m['label']}:")
        print(f"    CAGR: {m['cagr']:+.1%} ({diff_cagr:+.2%} vs base)")
        print(f"    Sharpe: {m['sharpe']:.2f} ({diff_sharpe:+.2f} vs base)")
        print(f"    Max DD: {m['max_dd']:.1%} ({diff_dd:+.1%} vs base)")
        print(f"    Trades: {m['trades'].get('n_trades', 0)} ({diff_trades:+d} vs base)")

    # Recommendation
    print(f"\n{'-' * W}")
    print("INTERPRETATION")
    print(f"{'-' * W}")

    # Find best Sharpe
    best_sharpe_idx = max(range(len(metrics_list)), key=lambda i: metrics_list[i]['sharpe'])
    best_cagr_idx = max(range(len(metrics_list)), key=lambda i: metrics_list[i]['cagr'])

    print(f"\n  Best Sharpe Ratio: {metrics_list[best_sharpe_idx]['label']}")
    print(f"  Best CAGR: {metrics_list[best_cagr_idx]['label']}")

    # Check if RSI filter helps
    if best_sharpe_idx == 0 and best_cagr_idx == 0:
        print(f"\n  CONCLUSION: RSI filter does NOT improve the strategy.")
        print(f"  The SMA200 filter already captures trend effectively.")
    elif best_sharpe_idx > 0 or best_cagr_idx > 0:
        print(f"\n  CONCLUSION: RSI filter shows marginal benefit.")
        print(f"  Consider implementing {metrics_list[best_sharpe_idx]['label']} for better risk-adjusted returns.")

    print(f"\n{'=' * W}")


# ======================================================================
# MAIN
# ======================================================================

def main():
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    print("=" * 80)
    print("RSI FILTER TEST - 80-Delta Call Strategy")
    print("=" * 80)
    print(f"\nTesting RSI filters to avoid overbought entries")
    print(f"RSI Period: {RSI_PERIOD}")
    print()

    client = ThetaDataClient()
    if not client.connect():
        print("\nERROR: Cannot connect to Theta Terminal.")
        return

    print("Connected to Theta Terminal.\n")

    spy_by_date, trading_dates, vix_data, sma200, rsi_data, monthly_exps = load_all_data(client)

    # Test scenarios
    scenarios = [
        (None, "A: No RSI filter (base)"),
        (70, "B: RSI < 70"),
        (80, "C: RSI < 80"),
    ]

    results = []

    for rsi_max, label in scenarios:
        snaps, trades = run_simulation(
            client, spy_by_date, trading_dates, vix_data, sma200,
            rsi_data, monthly_exps, rsi_max=rsi_max, label=label
        )

        if snaps:
            m = compute_metrics(snaps, trades, label)
            results.append(m)

    if len(results) >= 2:
        print_comparison(results)

    client.close()


if __name__ == "__main__":
    main()
