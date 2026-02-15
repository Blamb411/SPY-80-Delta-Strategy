"""
SMA Filter Strategy Comparison
==============================
Compare alternative strategies to the 80-Delta call strategy:
1. SPY + SMA200 filter (cash when below SMA)
2. SSO (2x) + SMA200 filter
3. UPRO (3x) + SMA200 filter
4. Collar strategy on SPY (protective put + covered call)

All strategies use the same SMA200 filter logic for fair comparison.
"""

import os
import pandas as pd
import numpy as np
import yfinance as yf
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from scipy.stats import norm
import math

# Force unbuffered output
import functools
print = functools.partial(print, flush=True)

_this_dir = os.path.dirname(os.path.abspath(__file__))

# ======================================================================
# PARAMETERS
# ======================================================================

INITIAL_CAPITAL = 100_000
SMA_PERIOD = 200
SMA_EXIT_THRESHOLD = 0.02  # Exit when 2% below SMA
RISK_FREE_RATE = 0.04

# Collar parameters
COLLAR_PUT_DELTA = 0.30    # Buy 30-delta put (OTM protection)
COLLAR_CALL_DELTA = 0.30   # Sell 30-delta call (OTM, caps upside)
COLLAR_DTE = 30            # Monthly collars

# Date range - start from UPRO inception for fair comparison
DATA_START = "2009-06-25"
DATA_END = "2026-01-31"

# ======================================================================
# BLACK-SCHOLES HELPERS
# ======================================================================

def bs_call_price(S, K, T, r, sigma):
    """Black-Scholes call price."""
    if T <= 0:
        return max(0, S - K)
    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    return S * norm.cdf(d1) - K * math.exp(-r * T) * norm.cdf(d2)

def bs_put_price(S, K, T, r, sigma):
    """Black-Scholes put price."""
    if T <= 0:
        return max(0, K - S)
    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    return K * math.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)

def find_strike_for_delta(S, T, r, sigma, target_delta, right="C"):
    """Find strike that gives target delta."""
    if T <= 0:
        return S

    # Binary search for strike
    if right == "C":
        low, high = S * 0.5, S * 1.5
        for _ in range(50):
            mid = (low + high) / 2
            d1 = (math.log(S / mid) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
            delta = norm.cdf(d1)
            if delta > target_delta:
                low = mid
            else:
                high = mid
    else:  # Put
        low, high = S * 0.5, S * 1.5
        for _ in range(50):
            mid = (low + high) / 2
            d1 = (math.log(S / mid) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
            delta = norm.cdf(d1) - 1  # Put delta is negative
            if delta < -target_delta:
                high = mid
            else:
                low = mid

    return (low + high) / 2

# ======================================================================
# DATA LOADING
# ======================================================================

def load_etf_data(ticker, start_date, end_date):
    """Load ETF price data from Yahoo Finance."""
    print(f"Loading {ticker} data...")
    data = yf.download(ticker, start=start_date, end=end_date, progress=False)
    data = data.reset_index()

    # Handle multi-level columns
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = [c[0] if isinstance(c, tuple) else c for c in data.columns]

    # Find close column
    close_col = None
    for col in data.columns:
        col_str = str(col)
        if 'Close' in col_str or 'close' in col_str:
            close_col = col
            break

    if close_col is None:
        print(f"  Warning: Could not find close column for {ticker}")
        return None

    # Get date column
    date_col = None
    for col in data.columns:
        col_str = str(col)
        if 'Date' in col_str or 'date' in col_str:
            date_col = col
            break

    result = {}
    for _, row in data.iterrows():
        date_str = row[date_col].strftime("%Y-%m-%d")
        result[date_str] = float(row[close_col])

    print(f"  {ticker}: {len(result)} days loaded")
    return result

def load_vix_data(start_date, end_date):
    """Load VIX data for implied volatility estimation."""
    print("Loading VIX data...")
    data = yf.download("^VIX", start=start_date, end=end_date, progress=False)
    data = data.reset_index()

    if isinstance(data.columns, pd.MultiIndex):
        data.columns = [c[0] if isinstance(c, tuple) else c for c in data.columns]

    result = {}
    for _, row in data.iterrows():
        date_str = row["Date"].strftime("%Y-%m-%d") if hasattr(row["Date"], "strftime") else str(row["Date"])[:10]
        close = row.get("Close") or row.get("Adj Close") or 20.0
        result[date_str] = float(close)

    print(f"  VIX: {len(result)} days loaded")
    return result

# ======================================================================
# STRATEGY IMPLEMENTATIONS
# ======================================================================

def calculate_sma(prices_by_date, trading_dates, period=200):
    """Calculate SMA for all dates."""
    sma = {}
    for i in range(period - 1, len(trading_dates)):
        window = [prices_by_date[trading_dates[j]] for j in range(i - period + 1, i + 1)]
        sma[trading_dates[i]] = sum(window) / period
    return sma

def run_sma_filter_strategy(prices_by_date, trading_dates, sma, initial_capital=INITIAL_CAPITAL, leverage=1.0, name="Strategy", spy_prices=None):
    """
    Run SMA filter strategy:
    - Invested when SPY price > SPY SMA (use SPY for signal, trade the given ETF)
    - Cash when SPY price < SPY SMA - threshold

    spy_prices: If provided, use SPY prices for SMA comparison (for SSO/UPRO)
    """
    print(f"\nRunning {name}...")

    # Use SPY prices for signal if provided, otherwise use the ETF prices
    signal_prices = spy_prices if spy_prices else prices_by_date

    cash = initial_capital
    shares = 0
    invested = False

    daily_values = []

    for date in trading_dates:
        if date not in sma:
            continue
        if date not in signal_prices:
            continue

        price = prices_by_date[date]
        signal_price = signal_prices[date]  # SPY price for signal
        sma_val = sma[date]

        # Check position status based on SPY vs SPY SMA
        pct_below_sma = (sma_val - signal_price) / sma_val if sma_val > 0 else 0

        if not invested and signal_price > sma_val:
            # Enter: buy shares
            shares = (cash * leverage) / price
            cash = cash * (1 - leverage)  # For leverage > 1, this goes negative (margin)
            invested = True
        elif invested and pct_below_sma >= SMA_EXIT_THRESHOLD:
            # Exit: sell shares
            cash = cash + shares * price
            shares = 0
            invested = False

        # Calculate portfolio value
        portfolio_value = cash + shares * price
        daily_values.append({
            "date": date,
            "portfolio_value": portfolio_value,
            "invested": invested,
            "price": price,
            "sma": sma_val
        })

    df = pd.DataFrame(daily_values)

    # Calculate metrics
    years = len(df) / 252
    end_value = df["portfolio_value"].iloc[-1]
    cagr = (end_value / initial_capital) ** (1/years) - 1

    daily_returns = df["portfolio_value"].pct_change().dropna()
    sharpe = (daily_returns.mean() / daily_returns.std()) * np.sqrt(252) if daily_returns.std() > 0 else 0

    downside = daily_returns[daily_returns < 0]
    sortino = (daily_returns.mean() / downside.std()) * np.sqrt(252) if len(downside) > 0 and downside.std() > 0 else 0

    cummax = df["portfolio_value"].cummax()
    drawdown = df["portfolio_value"] / cummax - 1
    max_dd = drawdown.min()

    print(f"  {name}: ${end_value:,.0f} | CAGR: {cagr:+.1%} | Sharpe: {sharpe:.2f} | Sortino: {sortino:.2f} | Max DD: {max_dd:.1%}")

    return {
        "name": name,
        "daily_values": df,
        "end_value": end_value,
        "cagr": cagr,
        "sharpe": sharpe,
        "sortino": sortino,
        "max_dd": max_dd
    }

def run_collar_strategy(spy_prices, trading_dates, sma, vix_data, initial_capital=INITIAL_CAPITAL):
    """
    Run collar strategy:
    - Always hold SPY shares
    - When above SMA: no collar (full upside)
    - When below SMA: buy protective put + sell covered call (limited downside & upside)

    Collar is rolled monthly.
    """
    print("\nRunning Collar Strategy...")

    cash = 0
    shares = initial_capital / spy_prices[trading_dates[0]]

    collar_active = False
    collar_put_strike = 0
    collar_call_strike = 0
    collar_expiry = None
    collar_put_premium = 0
    collar_call_premium = 0

    daily_values = []

    for i, date in enumerate(trading_dates):
        if date not in sma:
            continue

        price = spy_prices[date]
        sma_val = sma[date]
        vix = vix_data.get(date, 20.0)
        iv = max(0.10, min(0.80, vix / 100.0))

        pct_below_sma = (sma_val - price) / sma_val if sma_val > 0 else 0

        # Check if collar needs to be rolled or initiated
        collar_expired = collar_expiry and date >= collar_expiry

        if price > sma_val:
            # Above SMA: remove collar if active
            if collar_active:
                # Close collar positions (simplified: assume at intrinsic value)
                put_value = max(0, collar_put_strike - price)
                call_value = max(0, price - collar_call_strike)
                cash += put_value * shares / 100  # Put we own
                cash -= call_value * shares / 100  # Call we owe
                collar_active = False
                collar_expiry = None

        elif pct_below_sma >= SMA_EXIT_THRESHOLD:
            # Below SMA: initiate or roll collar
            if not collar_active or collar_expired:
                # Close old collar if expired
                if collar_expired and collar_active:
                    put_value = max(0, collar_put_strike - price)
                    call_value = max(0, price - collar_call_strike)
                    cash += put_value * shares / 100
                    cash -= call_value * shares / 100

                # Initiate new collar
                T = COLLAR_DTE / 365.0

                # Find strikes for target deltas
                put_strike = find_strike_for_delta(price, T, RISK_FREE_RATE, iv, COLLAR_PUT_DELTA, "P")
                call_strike = find_strike_for_delta(price, T, RISK_FREE_RATE, iv, COLLAR_CALL_DELTA, "C")

                # Calculate premiums
                put_premium = bs_put_price(price, put_strike, T, RISK_FREE_RATE, iv)
                call_premium = bs_call_price(price, call_strike, T, RISK_FREE_RATE, iv)

                # Net cost of collar (usually near zero for equal deltas)
                net_cost = (put_premium - call_premium) * shares / 100
                cash -= net_cost

                collar_put_strike = put_strike
                collar_call_strike = call_strike
                collar_put_premium = put_premium
                collar_call_premium = call_premium
                collar_expiry = (datetime.strptime(date, "%Y-%m-%d") + timedelta(days=COLLAR_DTE)).strftime("%Y-%m-%d")
                collar_active = True

        # Calculate portfolio value
        stock_value = shares * price

        if collar_active:
            # Mark collar to market (simplified)
            days_to_expiry = max(1, (datetime.strptime(collar_expiry, "%Y-%m-%d") - datetime.strptime(date, "%Y-%m-%d")).days)
            T = days_to_expiry / 365.0
            put_value = bs_put_price(price, collar_put_strike, T, RISK_FREE_RATE, iv)
            call_value = bs_call_price(price, collar_call_strike, T, RISK_FREE_RATE, iv)
            collar_mtm = (put_value - call_value) * shares / 100
        else:
            collar_mtm = 0

        portfolio_value = stock_value + cash + collar_mtm

        daily_values.append({
            "date": date,
            "portfolio_value": portfolio_value,
            "collar_active": collar_active,
            "price": price,
            "sma": sma_val
        })

    df = pd.DataFrame(daily_values)

    # Calculate metrics
    years = len(df) / 252
    end_value = df["portfolio_value"].iloc[-1]
    cagr = (end_value / initial_capital) ** (1/years) - 1

    daily_returns = df["portfolio_value"].pct_change().dropna()
    sharpe = (daily_returns.mean() / daily_returns.std()) * np.sqrt(252) if daily_returns.std() > 0 else 0

    downside = daily_returns[daily_returns < 0]
    sortino = (daily_returns.mean() / downside.std()) * np.sqrt(252) if len(downside) > 0 and downside.std() > 0 else 0

    cummax = df["portfolio_value"].cummax()
    drawdown = df["portfolio_value"] / cummax - 1
    max_dd = drawdown.min()

    print(f"  Collar: ${end_value:,.0f} | CAGR: {cagr:+.1%} | Sharpe: {sharpe:.2f} | Sortino: {sortino:.2f} | Max DD: {max_dd:.1%}")

    return {
        "name": "Collar",
        "daily_values": df,
        "end_value": end_value,
        "cagr": cagr,
        "sharpe": sharpe,
        "sortino": sortino,
        "max_dd": max_dd
    }

# ======================================================================
# MAIN
# ======================================================================

def main():
    print("=" * 80)
    print("SMA Filter Strategy Comparison")
    print("=" * 80)
    print(f"Period: {DATA_START} to {DATA_END}")
    print(f"Initial Capital: ${INITIAL_CAPITAL:,}")
    print(f"SMA Period: {SMA_PERIOD} days")
    print(f"Exit Threshold: {SMA_EXIT_THRESHOLD:.0%} below SMA")
    print()

    # Load data
    spy_prices = load_etf_data("SPY", DATA_START, DATA_END)
    sso_prices = load_etf_data("SSO", DATA_START, DATA_END)
    upro_prices = load_etf_data("UPRO", DATA_START, DATA_END)
    vix_data = load_vix_data(DATA_START, DATA_END)

    if not spy_prices:
        print("ERROR: Could not load SPY data")
        return

    # Get common trading dates
    trading_dates = sorted(spy_prices.keys())

    # Calculate SMA on SPY (used for all strategies)
    spy_sma = calculate_sma(spy_prices, trading_dates, SMA_PERIOD)

    # Filter to dates where SMA exists
    trading_dates = [d for d in trading_dates if d in spy_sma]

    print(f"\nBacktest period: {trading_dates[0]} to {trading_dates[-1]}")
    print(f"Trading days: {len(trading_dates)}")

    # Run strategies
    results = []

    # 1. SPY Buy & Hold (no filter, baseline)
    spy_bh = run_sma_filter_strategy(
        spy_prices, trading_dates,
        {d: 0 for d in trading_dates},  # SMA=0 means always below, never invest
        name="SPY B&H"
    )
    # Actually for B&H, we need to always be invested
    print("\nRunning SPY Buy & Hold (no filter)...")
    shares = INITIAL_CAPITAL / spy_prices[trading_dates[0]]
    bh_values = [{"date": d, "portfolio_value": shares * spy_prices[d]} for d in trading_dates]
    bh_df = pd.DataFrame(bh_values)
    years = len(bh_df) / 252
    end_val = bh_df["portfolio_value"].iloc[-1]
    daily_rets = bh_df["portfolio_value"].pct_change().dropna()
    spy_bh = {
        "name": "SPY B&H",
        "daily_values": bh_df,
        "end_value": end_val,
        "cagr": (end_val / INITIAL_CAPITAL) ** (1/years) - 1,
        "sharpe": (daily_rets.mean() / daily_rets.std()) * np.sqrt(252),
        "sortino": (daily_rets.mean() / daily_rets[daily_rets < 0].std()) * np.sqrt(252),
        "max_dd": (bh_df["portfolio_value"] / bh_df["portfolio_value"].cummax() - 1).min()
    }
    print(f"  SPY B&H: ${spy_bh['end_value']:,.0f} | CAGR: {spy_bh['cagr']:+.1%} | Sharpe: {spy_bh['sharpe']:.2f} | Max DD: {spy_bh['max_dd']:.1%}")
    results.append(spy_bh)

    # 2. SPY + SMA Filter
    spy_sma_result = run_sma_filter_strategy(spy_prices, trading_dates, spy_sma, name="SPY+SMA")
    results.append(spy_sma_result)

    # 3. SSO (2x) + SMA Filter (use SPY for signal, trade SSO)
    if sso_prices:
        sso_dates = [d for d in trading_dates if d in sso_prices]
        sso_sma_result = run_sma_filter_strategy(sso_prices, sso_dates, spy_sma, name="SSO+SMA", spy_prices=spy_prices)
        results.append(sso_sma_result)

    # 4. UPRO (3x) + SMA Filter (use SPY for signal, trade UPRO)
    if upro_prices:
        upro_dates = [d for d in trading_dates if d in upro_prices]
        upro_sma_result = run_sma_filter_strategy(upro_prices, upro_dates, spy_sma, name="UPRO+SMA", spy_prices=spy_prices)
        results.append(upro_sma_result)

    # 5. Collar Strategy
    collar_result = run_collar_strategy(spy_prices, trading_dates, spy_sma, vix_data)
    results.append(collar_result)

    # Load 80-Delta results for comparison
    delta80_path = os.path.join(_this_dir, "daily_values_80-Delta.csv")
    if os.path.exists(delta80_path):
        print("\nLoading 80-Delta results for comparison...")
        delta80_df = pd.read_csv(delta80_path)
        delta80_df["date"] = pd.to_datetime(delta80_df["date"])

        # Filter to same period
        start_dt = datetime.strptime(DATA_START, "%Y-%m-%d")
        delta80_df = delta80_df[delta80_df["date"] >= start_dt].copy()

        if len(delta80_df) > 0:
            # Rebase to $100K
            delta80_df["portfolio_value"] = delta80_df["portfolio_value"] / delta80_df["portfolio_value"].iloc[0] * INITIAL_CAPITAL

            years = len(delta80_df) / 252
            end_val = delta80_df["portfolio_value"].iloc[-1]
            daily_rets = delta80_df["portfolio_value"].pct_change().dropna()

            delta80_result = {
                "name": "80-Delta",
                "daily_values": delta80_df,
                "end_value": end_val,
                "cagr": (end_val / INITIAL_CAPITAL) ** (1/years) - 1,
                "sharpe": (daily_rets.mean() / daily_rets.std()) * np.sqrt(252),
                "sortino": (daily_rets.mean() / daily_rets[daily_rets < 0].std()) * np.sqrt(252),
                "max_dd": (delta80_df["portfolio_value"] / delta80_df["portfolio_value"].cummax() - 1).min()
            }
            print(f"  80-Delta: ${delta80_result['end_value']:,.0f} | CAGR: {delta80_result['cagr']:+.1%} | Sharpe: {delta80_result['sharpe']:.2f} | Max DD: {delta80_result['max_dd']:.1%}")
            results.append(delta80_result)

    # Print summary table
    print("\n" + "=" * 90)
    print("SUMMARY: Strategy Comparison (Starting June 2009)")
    print("=" * 90)
    print(f"{'Strategy':<15} {'End Value':>14} {'CAGR':>10} {'Sharpe':>10} {'Sortino':>10} {'Max DD':>10}")
    print("-" * 90)

    for r in sorted(results, key=lambda x: x["end_value"], reverse=True):
        print(f"{r['name']:<15} ${r['end_value']:>12,.0f} {r['cagr']:>+9.1%} {r['sharpe']:>10.2f} {r['sortino']:>10.2f} {r['max_dd']:>9.1%}")

    print("-" * 90)

    # Create comparison chart
    print("\nCreating comparison chart...")

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))

    # Plot 1: Portfolio values (log scale)
    ax1 = axes[0, 0]
    colors = {
        "SPY B&H": "blue",
        "SPY+SMA": "green",
        "SSO+SMA": "orange",
        "UPRO+SMA": "red",
        "Collar": "purple",
        "80-Delta": "brown"
    }

    for r in results:
        df = r["daily_values"]
        color = colors.get(r["name"], "gray")
        linewidth = 2.5 if r["name"] == "80-Delta" else 1.5
        ax1.semilogy(pd.to_datetime(df["date"]), df["portfolio_value"],
                     label=r["name"], color=color, linewidth=linewidth)

    ax1.set_title("Portfolio Value Comparison (Log Scale)", fontweight="bold")
    ax1.set_ylabel("Portfolio Value ($)")
    ax1.legend(loc="upper left")
    ax1.grid(True, alpha=0.3)

    # Plot 2: Drawdowns
    ax2 = axes[0, 1]
    for r in results:
        df = r["daily_values"]
        cummax = df["portfolio_value"].cummax()
        dd = (df["portfolio_value"] / cummax - 1) * 100
        color = colors.get(r["name"], "gray")
        ax2.plot(pd.to_datetime(df["date"]), dd, label=r["name"], color=color, alpha=0.7)

    ax2.set_title("Drawdown Comparison", fontweight="bold")
    ax2.set_ylabel("Drawdown (%)")
    ax2.legend(loc="lower left")
    ax2.grid(True, alpha=0.3)

    # Plot 3: Risk/Return scatter
    ax3 = axes[1, 0]
    for r in results:
        color = colors.get(r["name"], "gray")
        marker = "*" if r["name"] == "80-Delta" else "o"
        size = 200 if r["name"] == "80-Delta" else 100
        ax3.scatter(abs(r["max_dd"]) * 100, r["cagr"] * 100,
                   s=size, color=color, marker=marker, label=r["name"])

    ax3.set_title("Risk vs Return", fontweight="bold")
    ax3.set_xlabel("Max Drawdown (%)")
    ax3.set_ylabel("CAGR (%)")
    ax3.legend(loc="upper left")
    ax3.grid(True, alpha=0.3)

    # Plot 4: Summary table
    ax4 = axes[1, 1]
    ax4.axis("off")

    table_data = []
    for r in sorted(results, key=lambda x: x["end_value"], reverse=True):
        table_data.append([
            r["name"],
            f"${r['end_value']:,.0f}",
            f"{r['cagr']:+.1%}",
            f"{r['sharpe']:.2f}",
            f"{r['sortino']:.2f}",
            f"{r['max_dd']:.1%}"
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
    output_path = os.path.join(_this_dir, "sma_filter_comparison.png")
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    print(f"Chart saved to: {output_path}")

    # Key insights
    print("\n" + "=" * 70)
    print("KEY INSIGHTS")
    print("=" * 70)

    spy_sma_r = next((r for r in results if r["name"] == "SPY+SMA"), None)
    delta80_r = next((r for r in results if r["name"] == "80-Delta"), None)
    upro_sma_r = next((r for r in results if r["name"] == "UPRO+SMA"), None)
    collar_r = next((r for r in results if r["name"] == "Collar"), None)

    if spy_sma_r and delta80_r:
        print(f"\n1. SMA FILTER VALUE:")
        print(f"   SPY+SMA achieves {spy_sma_r['cagr']:+.1%} CAGR vs 80-Delta's {delta80_r['cagr']:+.1%}")
        print(f"   The options overlay adds {delta80_r['cagr'] - spy_sma_r['cagr']:+.1%} CAGR")

    if upro_sma_r and delta80_r:
        print(f"\n2. LEVERAGE COMPARISON:")
        print(f"   UPRO+SMA: {upro_sma_r['cagr']:+.1%} CAGR, {upro_sma_r['max_dd']:.1%} max DD")
        print(f"   80-Delta: {delta80_r['cagr']:+.1%} CAGR, {delta80_r['max_dd']:.1%} max DD")
        if abs(upro_sma_r['max_dd']) > abs(delta80_r['max_dd']):
            print(f"   80-Delta has {abs(upro_sma_r['max_dd']) - abs(delta80_r['max_dd']):.1%} LESS drawdown")

    if collar_r:
        print(f"\n3. COLLAR STRATEGY:")
        print(f"   Collar: {collar_r['cagr']:+.1%} CAGR, {collar_r['max_dd']:.1%} max DD")
        print(f"   Collar caps both upside AND downside - best for uncertainty")

    return results


if __name__ == "__main__":
    main()
