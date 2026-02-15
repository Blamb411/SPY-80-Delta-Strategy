"""
80-Delta Strategy: With vs Without SMA Filter
==============================================
Test the 80-delta call strategy with and without the SMA200 filter
to determine the filter's contribution to returns and risk.

This uses synthetic pricing (Black-Scholes) for speed since we're comparing
relative performance, not absolute accuracy.
"""

import os
import math
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import yfinance as yf
from datetime import datetime, timedelta
from scipy.stats import norm

import functools
print = functools.partial(print, flush=True)

_this_dir = os.path.dirname(os.path.abspath(__file__))

# ======================================================================
# PARAMETERS
# ======================================================================

INITIAL_CAPITAL = 100_000
TARGET_DELTA = 0.80
DTE_TARGET = 120
DTE_MIN = 90
DTE_MAX = 150
MH = 60                    # Max hold in trading days
PT = 0.50                  # +50% profit target
RATE = 0.04
SMA_EXIT_THRESHOLD = 0.02

DATA_START = "2009-01-01"
DATA_END = "2026-01-31"
SIM_START = "2009-06-25"   # Start from UPRO inception for comparison

# ======================================================================
# BLACK-SCHOLES
# ======================================================================

def bs_call_price(S, K, T, r, sigma):
    """Black-Scholes call price."""
    if T <= 0:
        return max(0, S - K)
    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    return S * norm.cdf(d1) - K * math.exp(-r * T) * norm.cdf(d2)

def bs_delta(S, K, T, r, sigma):
    """Black-Scholes call delta."""
    if T <= 0:
        return 1.0 if S > K else 0.0
    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    return norm.cdf(d1)

def find_strike_for_delta(S, T, r, sigma, target_delta):
    """Find strike that gives target delta."""
    if T <= 0:
        return S
    low, high = S * 0.5, S * 1.5
    for _ in range(50):
        mid = (low + high) / 2
        delta = bs_delta(S, mid, T, r, sigma)
        if delta > target_delta:
            low = mid
        else:
            high = mid
    return (low + high) / 2

# ======================================================================
# DATA LOADING
# ======================================================================

def load_data():
    """Load SPY and VIX data."""
    print("Loading SPY data...")
    spy = yf.download("SPY", start=DATA_START, end=DATA_END, progress=False)
    spy = spy.reset_index()
    if isinstance(spy.columns, pd.MultiIndex):
        spy.columns = [c[0] if isinstance(c, tuple) else c for c in spy.columns]

    spy_by_date = {}
    for _, row in spy.iterrows():
        date_str = row["Date"].strftime("%Y-%m-%d")
        close = row.get("Close") or row.get("Adj Close")
        spy_by_date[date_str] = float(close)

    print("Loading VIX data...")
    vix = yf.download("^VIX", start=DATA_START, end=DATA_END, progress=False)
    vix = vix.reset_index()
    if isinstance(vix.columns, pd.MultiIndex):
        vix.columns = [c[0] if isinstance(c, tuple) else c for c in vix.columns]

    vix_by_date = {}
    for _, row in vix.iterrows():
        date_str = row["Date"].strftime("%Y-%m-%d")
        close = row.get("Close") or row.get("Adj Close") or 20.0
        vix_by_date[date_str] = float(close)

    trading_dates = sorted(spy_by_date.keys())
    print(f"  SPY: {len(trading_dates)} days")
    print(f"  VIX: {len(vix_by_date)} days")

    return spy_by_date, vix_by_date, trading_dates

def compute_sma200(spy_by_date, trading_dates):
    """Calculate SMA200."""
    sma200 = {}
    for i in range(199, len(trading_dates)):
        window = [spy_by_date[trading_dates[j]] for j in range(i - 199, i + 1)]
        sma200[trading_dates[i]] = sum(window) / 200.0
    return sma200

# ======================================================================
# BACKTEST
# ======================================================================

def run_backtest(spy_by_date, vix_by_date, trading_dates, sma200, use_sma_filter, label=""):
    """
    Run 80-delta call strategy backtest.

    use_sma_filter: If True, only enter above SMA and exit on SMA breach.
                    If False, always allow entries (no SMA filter).
    """
    print(f"\nRunning {label}...")

    # Filter to simulation period
    sim_dates = [d for d in trading_dates if d >= SIM_START]
    if use_sma_filter:
        sim_dates = [d for d in sim_dates if d in sma200]

    cash = float(INITIAL_CAPITAL)
    positions = []
    daily_values = []
    trades = []

    # Find monthly expirations (3rd Friday of month)
    def is_monthly_opex(date_str):
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        if dt.weekday() != 4:
            return False
        return 15 <= dt.day <= 21

    all_dates = sorted(spy_by_date.keys())
    future_dates = [d for d in all_dates if d >= SIM_START]
    monthly_exps = [(d, datetime.strptime(d, "%Y-%m-%d").date()) for d in future_dates if is_monthly_opex(d)]

    def find_best_exp(entry_date_str):
        entry_dt = datetime.strptime(entry_date_str, "%Y-%m-%d").date()
        best_exp, best_dte = None, 0
        best_diff = 9999
        for exp_str, exp_dt in monthly_exps:
            dte = (exp_dt - entry_dt).days
            if dte < DTE_MIN or dte > DTE_MAX:
                continue
            diff = abs(dte - DTE_TARGET)
            if diff < best_diff:
                best_diff = diff
                best_exp = exp_str
                best_dte = dte
        return best_exp, best_dte

    for day_idx, today in enumerate(sim_dates):
        spot = spy_by_date[today]
        vix = vix_by_date.get(today, 20.0)
        iv = max(0.10, min(0.80, vix / 100.0))

        sma_val = sma200.get(today) if use_sma_filter else None

        # Check SMA condition
        if use_sma_filter and sma_val:
            above_sma = spot > sma_val
            pct_below_sma = (sma_val - spot) / sma_val if sma_val > 0 else 0
        else:
            above_sma = True  # No filter = always "above"
            pct_below_sma = 0

        # Process exits
        new_positions = []
        for pos in positions:
            dte = (datetime.strptime(pos["expiration"], "%Y-%m-%d") - datetime.strptime(today, "%Y-%m-%d")).days
            t = max(0.001, dte / 365.0)

            # Current option value
            current_price = bs_call_price(spot, pos["strike"], t, RATE, iv)

            # Exit conditions
            exit_reason = None
            pnl_pct = (current_price - pos["entry_price"]) / pos["entry_price"]

            if pnl_pct >= PT:
                exit_reason = "PT"
            elif pos["days_held"] >= MH:
                exit_reason = "MH"
            elif use_sma_filter and pct_below_sma >= SMA_EXIT_THRESHOLD:
                exit_reason = "SMA"

            if exit_reason:
                exit_value = current_price * pos["contracts"] * 100
                cash += exit_value
                trades.append({
                    "entry_date": pos["entry_date"],
                    "exit_date": today,
                    "exit_reason": exit_reason,
                    "pnl_pct": pnl_pct,
                    "days_held": pos["days_held"]
                })
            else:
                pos["days_held"] += 1
                pos["current_price"] = current_price
                new_positions.append(pos)

        positions = new_positions

        # Entry logic
        can_enter = (above_sma or not use_sma_filter) and cash > 1000

        if can_enter:
            best_exp, dte = find_best_exp(today)
            if best_exp and dte > 0:
                t = dte / 365.0

                # Find strike for target delta
                strike = find_strike_for_delta(spot, t, RATE, iv, TARGET_DELTA)

                # Calculate option price
                option_price = bs_call_price(spot, strike, t, RATE, iv)

                if option_price > 0.5:  # Min premium $0.50
                    # Size: use all available cash
                    contracts = int(cash / (option_price * 100))
                    if contracts > 0:
                        cost = contracts * option_price * 100
                        cash -= cost

                        positions.append({
                            "entry_date": today,
                            "expiration": best_exp,
                            "strike": strike,
                            "entry_price": option_price,
                            "contracts": contracts,
                            "days_held": 0,
                            "current_price": option_price
                        })

        # Calculate portfolio value
        options_value = sum(p["current_price"] * p["contracts"] * 100 for p in positions)
        portfolio_value = cash + options_value

        daily_values.append({
            "date": today,
            "portfolio_value": portfolio_value,
            "cash": cash,
            "options_value": options_value,
            "num_positions": len(positions),
            "spot": spot
        })

        if (day_idx + 1) % 500 == 0:
            print(f"    [{label}] Day {day_idx + 1}/{len(sim_dates)}: ${portfolio_value:,.0f}")

    df = pd.DataFrame(daily_values)

    # Calculate metrics
    years = len(df) / 252
    end_value = df["portfolio_value"].iloc[-1]
    cagr = (end_value / INITIAL_CAPITAL) ** (1/years) - 1

    daily_returns = df["portfolio_value"].pct_change().dropna()
    sharpe = (daily_returns.mean() / daily_returns.std()) * np.sqrt(252) if daily_returns.std() > 0 else 0

    downside = daily_returns[daily_returns < 0]
    sortino = (daily_returns.mean() / downside.std()) * np.sqrt(252) if len(downside) > 0 and downside.std() > 0 else 0

    cummax = df["portfolio_value"].cummax()
    drawdown = df["portfolio_value"] / cummax - 1
    max_dd = drawdown.min()

    # Trade stats
    trades_df = pd.DataFrame(trades) if trades else pd.DataFrame()
    win_rate = (trades_df["pnl_pct"] > 0).mean() if len(trades_df) > 0 else 0
    avg_pnl = trades_df["pnl_pct"].mean() if len(trades_df) > 0 else 0

    print(f"  {label}: ${end_value:,.0f} | CAGR: {cagr:+.1%} | Sharpe: {sharpe:.2f} | Sortino: {sortino:.2f} | Max DD: {max_dd:.1%}")
    print(f"    Trades: {len(trades)} | Win Rate: {win_rate:.1%} | Avg P/L: {avg_pnl:+.1%}")

    return {
        "name": label,
        "daily_values": df,
        "trades": trades_df,
        "end_value": end_value,
        "cagr": cagr,
        "sharpe": sharpe,
        "sortino": sortino,
        "max_dd": max_dd,
        "win_rate": win_rate,
        "num_trades": len(trades)
    }

# ======================================================================
# MAIN
# ======================================================================

def main():
    print("=" * 80)
    print("80-Delta Strategy: With vs Without SMA Filter")
    print("=" * 80)
    print(f"Period: {SIM_START} to {DATA_END}")
    print(f"Initial Capital: ${INITIAL_CAPITAL:,}")
    print()

    spy_by_date, vix_by_date, trading_dates = load_data()
    sma200 = compute_sma200(spy_by_date, trading_dates)

    results = []

    # Run with SMA filter
    result_sma = run_backtest(spy_by_date, vix_by_date, trading_dates, sma200,
                              use_sma_filter=True, label="80-Delta+SMA")
    results.append(result_sma)

    # Run without SMA filter
    result_no_sma = run_backtest(spy_by_date, vix_by_date, trading_dates, sma200,
                                 use_sma_filter=False, label="80-Delta NoSMA")
    results.append(result_no_sma)

    # Load benchmarks for comparison
    print("\n" + "-" * 70)
    print("Loading benchmarks...")

    # UPRO B&H
    upro = yf.download("UPRO", start=SIM_START, end=DATA_END, progress=False)
    upro = upro.reset_index()
    if isinstance(upro.columns, pd.MultiIndex):
        upro.columns = [c[0] if isinstance(c, tuple) else c for c in upro.columns]

    upro_prices = {}
    for _, row in upro.iterrows():
        date_str = row["Date"].strftime("%Y-%m-%d")
        upro_prices[date_str] = float(row.get("Close") or row.get("Adj Close"))

    upro_dates = sorted(upro_prices.keys())
    shares = INITIAL_CAPITAL / upro_prices[upro_dates[0]]
    upro_values = [{"date": d, "portfolio_value": shares * upro_prices[d]} for d in upro_dates]
    upro_df = pd.DataFrame(upro_values)

    years = len(upro_df) / 252
    upro_end = upro_df["portfolio_value"].iloc[-1]
    upro_rets = upro_df["portfolio_value"].pct_change().dropna()
    upro_result = {
        "name": "UPRO B&H",
        "daily_values": upro_df,
        "end_value": upro_end,
        "cagr": (upro_end / INITIAL_CAPITAL) ** (1/years) - 1,
        "sharpe": (upro_rets.mean() / upro_rets.std()) * np.sqrt(252),
        "sortino": (upro_rets.mean() / upro_rets[upro_rets < 0].std()) * np.sqrt(252),
        "max_dd": (upro_df["portfolio_value"] / upro_df["portfolio_value"].cummax() - 1).min()
    }
    print(f"  UPRO B&H: ${upro_result['end_value']:,.0f} | CAGR: {upro_result['cagr']:+.1%} | Max DD: {upro_result['max_dd']:.1%}")
    results.append(upro_result)

    # Summary
    print("\n" + "=" * 90)
    print("SUMMARY: 80-DELTA WITH vs WITHOUT SMA FILTER")
    print("=" * 90)
    print(f"\n{'Strategy':<20} {'End Value':>14} {'CAGR':>10} {'Sharpe':>10} {'Sortino':>10} {'Max DD':>10}")
    print("-" * 90)

    for r in sorted(results, key=lambda x: x["end_value"], reverse=True):
        print(f"{r['name']:<20} ${r['end_value']:>12,.0f} {r['cagr']:>+9.1%} {r['sharpe']:>10.2f} {r['sortino']:>10.2f} {r['max_dd']:>9.1%}")

    print("-" * 90)

    # Calculate SMA effect
    sma_effect_cagr = result_sma["cagr"] - result_no_sma["cagr"]
    sma_effect_dd = result_sma["max_dd"] - result_no_sma["max_dd"]

    print("\n" + "=" * 70)
    print("SMA FILTER EFFECT ON 80-DELTA STRATEGY")
    print("=" * 70)
    print(f"\n  Without SMA: {result_no_sma['cagr']:+.1%} CAGR, {result_no_sma['max_dd']:.1%} max DD")
    print(f"  With SMA:    {result_sma['cagr']:+.1%} CAGR, {result_sma['max_dd']:.1%} max DD")
    print(f"\n  SMA Effect:  {sma_effect_cagr:+.1%} CAGR, {sma_effect_dd:+.1%} max DD")

    if sma_effect_cagr < 0:
        print(f"\n  CONCLUSION: SMA filter REDUCES returns by {abs(sma_effect_cagr):.1%}")
    else:
        print(f"\n  CONCLUSION: SMA filter INCREASES returns by {sma_effect_cagr:.1%}")

    if sma_effect_dd > 0:
        print(f"              SMA filter REDUCES drawdown by {abs(sma_effect_dd):.1%}")
    else:
        print(f"              SMA filter INCREASES drawdown by {abs(sma_effect_dd):.1%}")

    # Create chart
    print("\n" + "-" * 70)
    print("Creating comparison chart...")

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))

    colors = {"80-Delta+SMA": "green", "80-Delta NoSMA": "blue", "UPRO B&H": "red"}

    # Plot 1: Portfolio values
    ax1 = axes[0, 0]
    for r in results:
        df = r["daily_values"]
        ax1.semilogy(pd.to_datetime(df["date"]), df["portfolio_value"],
                    label=r["name"], color=colors.get(r["name"], "gray"), linewidth=2)
    ax1.set_title("80-Delta: With vs Without SMA Filter", fontweight="bold")
    ax1.set_ylabel("Portfolio Value ($)")
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    # Plot 2: Drawdowns
    ax2 = axes[0, 1]
    for r in results:
        df = r["daily_values"]
        cummax = df["portfolio_value"].cummax()
        dd = (df["portfolio_value"] / cummax - 1) * 100
        ax2.plot(pd.to_datetime(df["date"]), dd, label=r["name"], color=colors.get(r["name"], "gray"))
    ax2.set_title("Drawdown Comparison", fontweight="bold")
    ax2.set_ylabel("Drawdown (%)")
    ax2.legend()
    ax2.grid(True, alpha=0.3)

    # Plot 3: Bar comparison
    ax3 = axes[1, 0]
    strategies = [r["name"] for r in results]
    cagrs = [r["cagr"] * 100 for r in results]
    bar_colors = [colors.get(s, "gray") for s in strategies]
    bars = ax3.bar(strategies, cagrs, color=bar_colors)
    ax3.set_title("CAGR Comparison", fontweight="bold")
    ax3.set_ylabel("CAGR (%)")
    for bar, cagr in zip(bars, cagrs):
        ax3.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                f'{cagr:.1f}%', ha='center', fontsize=10)

    # Plot 4: Summary table
    ax4 = axes[1, 1]
    ax4.axis("off")

    table_data = []
    for r in results:
        table_data.append([
            r["name"],
            f"${r['end_value']:,.0f}",
            f"{r['cagr']*100:+.1f}%",
            f"{r['sharpe']:.2f}",
            f"{r['sortino']:.2f}",
            f"{r['max_dd']*100:.1f}%"
        ])

    # Add SMA effect row
    table_data.append([
        "SMA Effect",
        "",
        f"{sma_effect_cagr*100:+.1f}%",
        f"{result_sma['sharpe'] - result_no_sma['sharpe']:+.2f}",
        f"{result_sma['sortino'] - result_no_sma['sortino']:+.2f}",
        f"{sma_effect_dd*100:+.1f}%"
    ])

    table = ax4.table(
        cellText=table_data,
        colLabels=["Strategy", "End Value", "CAGR", "Sharpe", "Sortino", "Max DD"],
        loc="center",
        cellLoc="center"
    )
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1.2, 1.8)

    plt.tight_layout()
    output_path = os.path.join(_this_dir, "delta_80_sma_comparison.png")
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    print(f"Chart saved to: {output_path}")

    return results


if __name__ == "__main__":
    main()
