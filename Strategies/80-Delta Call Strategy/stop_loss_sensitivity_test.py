"""
Stop-Loss Sensitivity Test for SPY 80-Delta Call Strategy
==========================================================
Tests the impact of adding stop-losses at 25%, 50%, and 75% levels
compared to the baseline (no stop-loss).

Usage:
    python stop_loss_sensitivity_test.py
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
from backtest.black_scholes import find_strike_for_delta

log = logging.getLogger("sl_test")

# ======================================================================
# PARAMETERS (same as spy_deployment_sim.py)
# ======================================================================

CAPITAL = 100_000
DELTA = 0.80
DTE_TARGET = 120
DTE_MIN = 90
DTE_MAX = 150
MH = 60                # max hold in trading days
PT = 0.50              # +50% profit target
RATE = 0.04

DATA_START = "2014-01-01"
DATA_END = "2026-01-31"
SIM_START = "2015-03-01"

# Stop-loss levels to test (None = no stop loss)
STOP_LOSS_LEVELS = [None, 0.25, 0.50, 0.75]


# ======================================================================
# HELPERS
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

    print("Loading SPY expirations...")
    all_exps = client.get_expirations("SPY")
    monthly_exps = [(e, datetime.strptime(e, "%Y-%m-%d").date())
                    for e in all_exps if is_monthly_opex(e)]
    monthly_exps.sort(key=lambda x: x[1])

    print(f"  SPY bars: {len(spy_bars)}")
    print(f"  VIX days: {len(vix_data)}")
    print(f"  Monthly expirations: {len(monthly_exps)}")

    return spy_by_date, trading_dates, vix_data, sma200, monthly_exps


# ======================================================================
# SIMULATION WITH STOP-LOSS
# ======================================================================

def run_simulation_with_sl(client, spy_by_date, trading_dates, vix_data, sma200,
                           monthly_exps, stop_loss_pct=None):
    """
    Run simulation with optional stop-loss.

    stop_loss_pct: None for no stop-loss, or decimal (e.g., 0.25 for 25% SL)
    """
    available_cash = float(CAPITAL)
    pending_cash = 0.0
    positions = []

    contract_eod = {}
    strikes_cache = {}

    daily_snapshots = []
    trade_log = []
    exit_reasons = defaultdict(int)

    start_idx = next((i for i, d in enumerate(trading_dates) if d >= SIM_START), 0)

    sl_label = f"{stop_loss_pct:.0%} SL" if stop_loss_pct else "No SL"

    for day_idx in range(start_idx, len(trading_dates)):
        today = trading_dates[day_idx]
        bar = spy_by_date[today]
        spot = bar["close"]
        sma_val = sma200.get(today)
        above_sma = (spot > sma_val) if sma_val else False
        vix = vix_data.get(today, 20.0)

        # Settle yesterday's exit proceeds
        available_cash += pending_cash
        pending_cash = 0.0

        # Process exits
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

            # Check profit target
            if pnl_pct >= PT:
                exit_reason = "PT"
            # Check stop-loss
            elif stop_loss_pct is not None and pnl_pct <= -stop_loss_pct:
                exit_reason = "SL"
            # Check max hold
            elif pos["days_held"] >= MH:
                exit_reason = "MH"

            if exit_reason:
                proceeds = bid * 100
                pending_cash += proceeds
                exit_reasons[exit_reason] += 1
                trade_log.append({
                    "entry_date": pos["entry_date"],
                    "exit_date": today,
                    "expiration": pos["expiration"],
                    "strike": pos["strike"],
                    "entry_price": pos["entry_price"],
                    "exit_price": bid,
                    "pnl_pct": pnl_pct,
                    "pnl_dollar": proceeds - pos["contract_cost"],
                    "days_held": pos["days_held"],
                    "exit_reason": exit_reason,
                    "contract_cost": pos["contract_cost"],
                })
            else:
                still_open.append(pos)
        positions = still_open

        # Entry: buy 1 contract if above SMA200
        if above_sma and sma_val is not None:
            best_exp, dte_cal = find_best_expiration(today, monthly_exps)
            if best_exp:
                t_years = dte_cal / 365.0
                iv_est = max(0.08, min(0.90, vix / 100.0))
                bs_strike = find_strike_for_delta(spot, t_years, RATE, iv_est, DELTA, "C")

                if bs_strike:
                    if best_exp not in strikes_cache:
                        strikes_cache[best_exp] = client.get_strikes("SPY", best_exp)
                    strikes = strikes_cache[best_exp]

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
                            if available_cash >= contract_cost:
                                available_cash -= contract_cost
                                positions.append({
                                    "entry_date": today,
                                    "expiration": best_exp,
                                    "strike": real_strike,
                                    "entry_price": ask,
                                    "contract_cost": contract_cost,
                                    "days_held": 0,
                                })

        # Mark to market
        positions_value = 0.0
        for pos in positions:
            ckey = (pos["expiration"], pos["strike"])
            eod = contract_eod.get(ckey, {}).get(today)
            bid, ask = get_bid_ask(eod)
            if bid and ask:
                mid = (bid + ask) / 2.0
            else:
                mid = max(0, spot - pos["strike"])
            positions_value += mid * 100

        portfolio_value = available_cash + pending_cash + positions_value

        daily_snapshots.append({
            "date": today,
            "portfolio_value": portfolio_value,
            "spy_close": spot,
        })

    return daily_snapshots, trade_log, dict(exit_reasons)


# ======================================================================
# METRICS
# ======================================================================

def compute_metrics(snapshots, trade_log, exit_reasons, sl_label):
    """Compute performance metrics."""

    df = pd.DataFrame(snapshots)

    start_val = df["portfolio_value"].iloc[0]
    end_val = df["portfolio_value"].iloc[-1]

    n_days = len(df)
    years = n_days / 252.0

    # CAGR
    cagr = (end_val / start_val) ** (1 / years) - 1 if years > 0 else 0

    # Total return
    total_return = end_val / start_val - 1

    # Daily returns
    df["daily_return"] = df["portfolio_value"].pct_change()
    daily_returns = df["daily_return"].dropna()

    # Volatility (annualized)
    vol = daily_returns.std() * np.sqrt(252)

    # Sharpe ratio (assuming 4% risk-free)
    excess_return = cagr - 0.04
    sharpe = excess_return / vol if vol > 0 else 0

    # Sortino ratio
    downside = daily_returns[daily_returns < 0]
    downside_std = downside.std() * np.sqrt(252) if len(downside) > 0 else 0
    sortino = excess_return / downside_std if downside_std > 0 else 0

    # Max drawdown
    cummax = df["portfolio_value"].cummax()
    drawdown = df["portfolio_value"] / cummax - 1
    max_dd = drawdown.min()

    # Calmar ratio
    calmar = cagr / abs(max_dd) if max_dd < 0 else 0

    # Trade statistics
    n_trades = len(trade_log)
    wins = sum(1 for t in trade_log if t["pnl_pct"] > 0)
    losses = n_trades - wins
    win_rate = wins / n_trades if n_trades > 0 else 0

    avg_win = np.mean([t["pnl_pct"] for t in trade_log if t["pnl_pct"] > 0]) if wins > 0 else 0
    avg_loss = np.mean([t["pnl_pct"] for t in trade_log if t["pnl_pct"] <= 0]) if losses > 0 else 0

    total_pnl = sum(t["pnl_dollar"] for t in trade_log)
    avg_pnl = total_pnl / n_trades if n_trades > 0 else 0

    # Profit factor
    gross_profit = sum(t["pnl_dollar"] for t in trade_log if t["pnl_dollar"] > 0)
    gross_loss = abs(sum(t["pnl_dollar"] for t in trade_log if t["pnl_dollar"] < 0))
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else float('inf')

    return {
        "label": sl_label,
        "start_value": start_val,
        "end_value": end_val,
        "total_return": total_return * 100,
        "cagr": cagr * 100,
        "volatility": vol * 100,
        "sharpe": sharpe,
        "sortino": sortino,
        "max_dd": max_dd * 100,
        "calmar": calmar,
        "n_trades": n_trades,
        "wins": wins,
        "losses": losses,
        "win_rate": win_rate * 100,
        "avg_win": avg_win * 100,
        "avg_loss": avg_loss * 100,
        "total_pnl": total_pnl,
        "avg_pnl": avg_pnl,
        "profit_factor": profit_factor,
        "exit_reasons": exit_reasons,
    }


# ======================================================================
# MAIN
# ======================================================================

def main():
    logging.basicConfig(level=logging.WARNING)

    print("=" * 80)
    print("STOP-LOSS SENSITIVITY TEST")
    print("SPY 80-Delta Call Strategy")
    print("=" * 80)
    print()

    client = ThetaDataClient()
    if not client.connect():
        print("ERROR: Cannot connect to Theta Terminal")
        return

    print("Connected to Theta Terminal\n")

    # Load data once
    spy_by_date, trading_dates, vix_data, sma200, monthly_exps = load_all_data(client)
    print()

    # Run simulations for each stop-loss level
    results = []

    for sl in STOP_LOSS_LEVELS:
        sl_label = f"{sl:.0%} Stop-Loss" if sl else "No Stop-Loss (Baseline)"
        print(f"Running: {sl_label}...")

        snapshots, trade_log, exit_reasons = run_simulation_with_sl(
            client, spy_by_date, trading_dates, vix_data, sma200, monthly_exps,
            stop_loss_pct=sl
        )

        metrics = compute_metrics(snapshots, trade_log, exit_reasons, sl_label)
        results.append(metrics)

        print(f"  Trades: {metrics['n_trades']}, Win Rate: {metrics['win_rate']:.0f}%, "
              f"CAGR: {metrics['cagr']:+.1f}%, Max DD: {metrics['max_dd']:.1f}%")

    client.close()

    # Print comparison
    print()
    print("=" * 80)
    print("RESULTS COMPARISON")
    print("=" * 80)
    print()

    # Summary table
    headers = ["Metric", "No SL", "25% SL", "50% SL", "75% SL"]
    print(f"{'Metric':<22} {'No SL':>14} {'25% SL':>14} {'50% SL':>14} {'75% SL':>14}")
    print("-" * 80)

    metrics_to_show = [
        ("End Value", "end_value", "${:,.0f}"),
        ("Total Return", "total_return", "{:+.1f}%"),
        ("CAGR", "cagr", "{:+.1f}%"),
        ("Volatility", "volatility", "{:.1f}%"),
        ("Sharpe Ratio", "sharpe", "{:.2f}"),
        ("Sortino Ratio", "sortino", "{:.2f}"),
        ("Max Drawdown", "max_dd", "{:.1f}%"),
        ("Calmar Ratio", "calmar", "{:.2f}"),
        ("Total Trades", "n_trades", "{:,}"),
        ("Win Rate", "win_rate", "{:.0f}%"),
        ("Avg Win", "avg_win", "{:+.1f}%"),
        ("Avg Loss", "avg_loss", "{:.1f}%"),
        ("Profit Factor", "profit_factor", "{:.2f}"),
        ("Avg P&L/Trade", "avg_pnl", "${:+,.0f}"),
    ]

    for label, key, fmt in metrics_to_show:
        row = f"{label:<22}"
        for r in results:
            val = r[key]
            if key == "profit_factor" and val > 100:
                row += f"{'Inf':>14}"
            else:
                row += f"{fmt.format(val):>14}"
        print(row)

    print()

    # Exit reasons breakdown
    print("-" * 80)
    print("EXIT REASONS")
    print("-" * 80)
    print(f"{'Reason':<22} {'No SL':>14} {'25% SL':>14} {'50% SL':>14} {'75% SL':>14}")
    print("-" * 80)

    for reason in ["PT", "MH", "SL"]:
        row = f"{reason:<22}"
        for r in results:
            count = r["exit_reasons"].get(reason, 0)
            pct = count / r["n_trades"] * 100 if r["n_trades"] > 0 else 0
            row += f"{count:>8} ({pct:>4.0f}%)"
        print(row)

    print()

    # Analysis
    print("=" * 80)
    print("ANALYSIS")
    print("=" * 80)
    print()

    baseline = results[0]

    print("Compared to No Stop-Loss baseline:")
    print()

    for r in results[1:]:
        cagr_diff = r["cagr"] - baseline["cagr"]
        sharpe_diff = r["sharpe"] - baseline["sharpe"]
        dd_diff = r["max_dd"] - baseline["max_dd"]  # Less negative is better
        wr_diff = r["win_rate"] - baseline["win_rate"]

        print(f"  {r['label']}:")
        print(f"    CAGR:      {cagr_diff:+.1f}% vs baseline")
        print(f"    Sharpe:    {sharpe_diff:+.2f} vs baseline")
        print(f"    Max DD:    {dd_diff:+.1f}% vs baseline (positive = less severe)")
        print(f"    Win Rate:  {wr_diff:+.0f}% vs baseline")

        # Overall assessment
        if sharpe_diff > 0.05:
            verdict = "IMPROVES risk-adjusted returns"
        elif sharpe_diff < -0.05:
            verdict = "HURTS risk-adjusted returns"
        else:
            verdict = "NEUTRAL impact"
        print(f"    Verdict:   {verdict}")
        print()

    # Best configuration
    best = max(results, key=lambda x: x["sharpe"])
    print(f"Best risk-adjusted performance: {best['label']}")
    print(f"  Sharpe: {best['sharpe']:.2f}, CAGR: {best['cagr']:+.1f}%, Max DD: {best['max_dd']:.1f}%")
    print()

    # Recommendation
    print("=" * 80)
    print("RECOMMENDATION")
    print("=" * 80)
    print()

    if best["label"] == "No Stop-Loss (Baseline)":
        print("The backtest confirms: NO STOP-LOSS produces the best results.")
        print("Adding a stop-loss reduces returns without meaningfully improving risk metrics.")
    else:
        sl_pct = best["label"].split("%")[0]
        print(f"A {sl_pct}% stop-loss improves risk-adjusted returns.")
        print("Consider adding this to the strategy rules.")

    print()


if __name__ == "__main__":
    main()
