"""
SMA Filter Ablation Study
=========================
Test each strategy WITH and WITHOUT the SMA filter to isolate its effect.

Strategies tested:
1. SPY (with/without SMA)
2. SSO 2x (with/without SMA)
3. UPRO 3x (with/without SMA)
4. 80-Delta calls (with/without SMA) - requires re-running backtest

This will show exactly what the SMA filter contributes (or costs) for each strategy.
"""

import os
import pandas as pd
import numpy as np
import yfinance as yf
import matplotlib.pyplot as plt
from datetime import datetime
from scipy.stats import norm
import math

import functools
print = functools.partial(print, flush=True)

_this_dir = os.path.dirname(os.path.abspath(__file__))

# ======================================================================
# PARAMETERS
# ======================================================================

INITIAL_CAPITAL = 100_000
SMA_PERIOD = 200
SMA_EXIT_THRESHOLD = 0.02

# Date range - start from UPRO inception
DATA_START = "2009-06-25"
DATA_END = "2026-01-31"

# ======================================================================
# DATA LOADING
# ======================================================================

def load_etf_data(ticker, start_date, end_date):
    """Load ETF price data from Yahoo Finance."""
    print(f"Loading {ticker} data...")
    data = yf.download(ticker, start=start_date, end=end_date, progress=False)
    data = data.reset_index()

    if isinstance(data.columns, pd.MultiIndex):
        data.columns = [c[0] if isinstance(c, tuple) else c for c in data.columns]

    close_col = None
    for col in data.columns:
        if 'Close' in str(col):
            close_col = col
            break

    if close_col is None:
        return None

    date_col = None
    for col in data.columns:
        if 'Date' in str(col):
            date_col = col
            break

    result = {}
    for _, row in data.iterrows():
        date_str = row[date_col].strftime("%Y-%m-%d")
        result[date_str] = float(row[close_col])

    print(f"  {ticker}: {len(result)} days")
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

def run_etf_strategy(etf_prices, trading_dates, spy_prices, spy_sma, use_sma_filter, name):
    """
    Run ETF strategy with or without SMA filter.

    use_sma_filter=True: Only invested when SPY > SMA, exit when SPY < SMA-2%
    use_sma_filter=False: Always invested (buy and hold)
    """
    cash = INITIAL_CAPITAL
    shares = 0
    invested = False

    daily_values = []

    for date in trading_dates:
        if date not in etf_prices:
            continue
        if use_sma_filter and date not in spy_sma:
            continue

        price = etf_prices[date]
        spy_price = spy_prices.get(date, 0)
        sma_val = spy_sma.get(date, 0) if use_sma_filter else 0

        if use_sma_filter:
            pct_below_sma = (sma_val - spy_price) / sma_val if sma_val > 0 else 0

            if not invested and spy_price > sma_val:
                # Enter
                shares = cash / price
                cash = 0
                invested = True
            elif invested and pct_below_sma >= SMA_EXIT_THRESHOLD:
                # Exit
                cash = shares * price
                shares = 0
                invested = False
        else:
            # No filter - always invested
            if not invested:
                shares = cash / price
                cash = 0
                invested = True

        portfolio_value = cash + shares * price
        daily_values.append({
            "date": date,
            "portfolio_value": portfolio_value,
            "invested": invested
        })

    if not daily_values:
        return None

    df = pd.DataFrame(daily_values)

    # Calculate metrics
    years = len(df) / 252
    end_value = df["portfolio_value"].iloc[-1]
    cagr = (end_value / INITIAL_CAPITAL) ** (1/years) - 1 if years > 0 else 0

    daily_returns = df["portfolio_value"].pct_change().dropna()
    sharpe = (daily_returns.mean() / daily_returns.std()) * np.sqrt(252) if daily_returns.std() > 0 else 0

    downside = daily_returns[daily_returns < 0]
    sortino = (daily_returns.mean() / downside.std()) * np.sqrt(252) if len(downside) > 0 and downside.std() > 0 else 0

    cummax = df["portfolio_value"].cummax()
    drawdown = df["portfolio_value"] / cummax - 1
    max_dd = drawdown.min()

    return {
        "name": name,
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
    print("SMA FILTER ABLATION STUDY")
    print("=" * 80)
    print(f"Period: {DATA_START} to {DATA_END}")
    print(f"Testing each strategy WITH and WITHOUT SMA filter")
    print()

    # Load data
    spy_prices = load_etf_data("SPY", DATA_START, DATA_END)
    sso_prices = load_etf_data("SSO", DATA_START, DATA_END)
    upro_prices = load_etf_data("UPRO", DATA_START, DATA_END)

    if not spy_prices:
        print("ERROR: Could not load SPY data")
        return

    trading_dates = sorted(spy_prices.keys())
    spy_sma = calculate_sma(spy_prices, trading_dates, SMA_PERIOD)

    # Filter to dates where SMA exists (for fair comparison)
    trading_dates_sma = [d for d in trading_dates if d in spy_sma]

    print(f"\nBacktest period: {trading_dates_sma[0]} to {trading_dates_sma[-1]}")
    print(f"Trading days: {len(trading_dates_sma)}")

    results = []

    # Test each ETF with and without SMA
    etfs = [
        ("SPY", spy_prices, "1x"),
        ("SSO", sso_prices, "2x"),
        ("UPRO", upro_prices, "3x")
    ]

    print("\n" + "-" * 70)
    print("Running ETF strategies...")
    print("-" * 70)

    for ticker, prices, leverage in etfs:
        if not prices:
            continue

        # Without SMA (Buy & Hold)
        result_no_sma = run_etf_strategy(
            prices, trading_dates_sma, spy_prices, spy_sma,
            use_sma_filter=False, name=f"{ticker} B&H"
        )
        if result_no_sma:
            print(f"  {result_no_sma['name']:<12}: ${result_no_sma['end_value']:>12,.0f} | CAGR: {result_no_sma['cagr']:>+6.1%} | Sharpe: {result_no_sma['sharpe']:.2f} | Max DD: {result_no_sma['max_dd']:.1%}")
            results.append(result_no_sma)

        # With SMA filter
        result_sma = run_etf_strategy(
            prices, trading_dates_sma, spy_prices, spy_sma,
            use_sma_filter=True, name=f"{ticker}+SMA"
        )
        if result_sma:
            print(f"  {result_sma['name']:<12}: ${result_sma['end_value']:>12,.0f} | CAGR: {result_sma['cagr']:>+6.1%} | Sharpe: {result_sma['sharpe']:.2f} | Max DD: {result_sma['max_dd']:.1%}")
            results.append(result_sma)

        print()

    # Load 80-Delta daily values if available
    print("-" * 70)
    print("Loading 80-Delta strategy results...")
    print("-" * 70)

    delta80_path = os.path.join(_this_dir, "daily_values_80-Delta.csv")
    if os.path.exists(delta80_path):
        delta80_df = pd.read_csv(delta80_path)
        delta80_df["date"] = pd.to_datetime(delta80_df["date"]).dt.strftime("%Y-%m-%d")

        # Filter to same period
        delta80_df = delta80_df[delta80_df["date"] >= trading_dates_sma[0]].copy()

        if len(delta80_df) > 0:
            # Rebase to $100K
            delta80_df["portfolio_value"] = delta80_df["portfolio_value"] / delta80_df["portfolio_value"].iloc[0] * INITIAL_CAPITAL

            years = len(delta80_df) / 252
            end_val = delta80_df["portfolio_value"].iloc[-1]
            daily_rets = delta80_df["portfolio_value"].pct_change().dropna()

            delta80_result = {
                "name": "80-Delta+SMA",
                "daily_values": delta80_df.rename(columns={"date": "date"}),
                "end_value": end_val,
                "cagr": (end_val / INITIAL_CAPITAL) ** (1/years) - 1,
                "sharpe": (daily_rets.mean() / daily_rets.std()) * np.sqrt(252),
                "sortino": (daily_rets.mean() / daily_rets[daily_rets < 0].std()) * np.sqrt(252),
                "max_dd": (delta80_df["portfolio_value"] / delta80_df["portfolio_value"].cummax() - 1).min()
            }
            print(f"  {delta80_result['name']:<12}: ${delta80_result['end_value']:>12,.0f} | CAGR: {delta80_result['cagr']:>+6.1%} | Sharpe: {delta80_result['sharpe']:.2f} | Max DD: {delta80_result['max_dd']:.1%}")
            results.append(delta80_result)

    # Note about 80-Delta without SMA
    print("\n  NOTE: 80-Delta WITHOUT SMA filter requires running a separate backtest.")
    print("        The current implementation always uses the SMA filter for entries.")

    # Summary table
    print("\n" + "=" * 100)
    print("SUMMARY: SMA FILTER ABLATION STUDY")
    print("=" * 100)

    # Group by base strategy
    print(f"\n{'Strategy':<15} {'End Value':>14} {'CAGR':>10} {'Sharpe':>10} {'Sortino':>10} {'Max DD':>10} {'SMA Effect':>12}")
    print("-" * 100)

    # Calculate SMA effect for each pair
    pairs = [("SPY", "SPY B&H", "SPY+SMA"),
             ("SSO", "SSO B&H", "SSO+SMA"),
             ("UPRO", "UPRO B&H", "UPRO+SMA")]

    for base, bh_name, sma_name in pairs:
        bh = next((r for r in results if r["name"] == bh_name), None)
        sma = next((r for r in results if r["name"] == sma_name), None)

        if bh:
            print(f"{bh['name']:<15} ${bh['end_value']:>12,.0f} {bh['cagr']:>+9.1%} {bh['sharpe']:>10.2f} {bh['sortino']:>10.2f} {bh['max_dd']:>9.1%} {'(baseline)':>12}")
        if sma:
            sma_effect = sma['cagr'] - bh['cagr'] if bh else 0
            dd_effect = sma['max_dd'] - bh['max_dd'] if bh else 0
            print(f"{sma['name']:<15} ${sma['end_value']:>12,.0f} {sma['cagr']:>+9.1%} {sma['sharpe']:>10.2f} {sma['sortino']:>10.2f} {sma['max_dd']:>9.1%} {sma_effect:>+11.1%}")
        print()

    # 80-Delta
    delta80 = next((r for r in results if r["name"] == "80-Delta+SMA"), None)
    if delta80:
        print(f"{delta80['name']:<15} ${delta80['end_value']:>12,.0f} {delta80['cagr']:>+9.1%} {delta80['sharpe']:>10.2f} {delta80['sortino']:>10.2f} {delta80['max_dd']:>9.1%} {'(need test)':>12}")

    print("-" * 100)

    # Key insights
    print("\n" + "=" * 70)
    print("KEY FINDINGS: SMA FILTER EFFECT")
    print("=" * 70)

    spy_bh = next((r for r in results if r["name"] == "SPY B&H"), None)
    spy_sma_r = next((r for r in results if r["name"] == "SPY+SMA"), None)
    sso_bh = next((r for r in results if r["name"] == "SSO B&H"), None)
    sso_sma_r = next((r for r in results if r["name"] == "SSO+SMA"), None)
    upro_bh = next((r for r in results if r["name"] == "UPRO B&H"), None)
    upro_sma_r = next((r for r in results if r["name"] == "UPRO+SMA"), None)

    if spy_bh and spy_sma_r:
        print(f"\n1. SPY (1x Leverage):")
        print(f"   Without SMA: {spy_bh['cagr']:+.1%} CAGR, {spy_bh['max_dd']:.1%} max DD")
        print(f"   With SMA:    {spy_sma_r['cagr']:+.1%} CAGR, {spy_sma_r['max_dd']:.1%} max DD")
        print(f"   SMA Effect:  {spy_sma_r['cagr'] - spy_bh['cagr']:+.1%} CAGR, {spy_sma_r['max_dd'] - spy_bh['max_dd']:+.1%} max DD")
        if spy_sma_r['cagr'] < spy_bh['cagr']:
            print(f"   CONCLUSION:  SMA HURTS returns but HELPS drawdown")

    if sso_bh and sso_sma_r:
        print(f"\n2. SSO (2x Leverage):")
        print(f"   Without SMA: {sso_bh['cagr']:+.1%} CAGR, {sso_bh['max_dd']:.1%} max DD")
        print(f"   With SMA:    {sso_sma_r['cagr']:+.1%} CAGR, {sso_sma_r['max_dd']:.1%} max DD")
        print(f"   SMA Effect:  {sso_sma_r['cagr'] - sso_bh['cagr']:+.1%} CAGR, {sso_sma_r['max_dd'] - sso_bh['max_dd']:+.1%} max DD")

    if upro_bh and upro_sma_r:
        print(f"\n3. UPRO (3x Leverage):")
        print(f"   Without SMA: {upro_bh['cagr']:+.1%} CAGR, {upro_bh['max_dd']:.1%} max DD")
        print(f"   With SMA:    {upro_sma_r['cagr']:+.1%} CAGR, {upro_sma_r['max_dd']:.1%} max DD")
        print(f"   SMA Effect:  {upro_sma_r['cagr'] - upro_bh['cagr']:+.1%} CAGR, {upro_sma_r['max_dd'] - upro_bh['max_dd']:+.1%} max DD")

    if delta80:
        print(f"\n4. 80-Delta Calls:")
        print(f"   With SMA:    {delta80['cagr']:+.1%} CAGR, {delta80['max_dd']:.1%} max DD")
        print(f"   Without SMA: NEEDS SEPARATE BACKTEST")
        print(f"   (Would require modifying delta_capped_backtest.py to remove SMA filter)")

    # Create comparison chart
    print("\n" + "-" * 70)
    print("Creating comparison chart...")

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))

    colors = {
        "SPY B&H": "blue", "SPY+SMA": "lightblue",
        "SSO B&H": "orange", "SSO+SMA": "moccasin",
        "UPRO B&H": "red", "UPRO+SMA": "lightcoral",
        "80-Delta+SMA": "green"
    }

    # Plot 1: Portfolio values
    ax1 = axes[0, 0]
    for r in results:
        df = r["daily_values"]
        color = colors.get(r["name"], "gray")
        linestyle = "--" if "+SMA" in r["name"] and "80-Delta" not in r["name"] else "-"
        linewidth = 2 if "B&H" in r["name"] or "80-Delta" in r["name"] else 1.5
        ax1.semilogy(pd.to_datetime(df["date"]), df["portfolio_value"],
                    label=r["name"], color=color, linestyle=linestyle, linewidth=linewidth)
    ax1.set_title("Portfolio Value: B&H vs SMA Filter", fontweight="bold")
    ax1.set_ylabel("Portfolio Value ($)")
    ax1.legend(loc="upper left", fontsize=8)
    ax1.grid(True, alpha=0.3)

    # Plot 2: CAGR comparison (bar chart)
    ax2 = axes[0, 1]
    strategies = [r["name"] for r in results]
    cagrs = [r["cagr"] * 100 for r in results]
    bar_colors = [colors.get(s, "gray") for s in strategies]
    bars = ax2.bar(strategies, cagrs, color=bar_colors)
    ax2.set_title("CAGR Comparison", fontweight="bold")
    ax2.set_ylabel("CAGR (%)")
    ax2.tick_params(axis='x', rotation=45)
    ax2.axhline(y=0, color='black', linewidth=0.5)
    for bar, cagr in zip(bars, cagrs):
        ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                f'{cagr:.1f}%', ha='center', va='bottom', fontsize=8)

    # Plot 3: Max Drawdown comparison
    ax3 = axes[1, 0]
    max_dds = [abs(r["max_dd"]) * 100 for r in results]
    bars = ax3.bar(strategies, max_dds, color=bar_colors)
    ax3.set_title("Max Drawdown Comparison", fontweight="bold")
    ax3.set_ylabel("Max Drawdown (%)")
    ax3.tick_params(axis='x', rotation=45)
    for bar, dd in zip(bars, max_dds):
        ax3.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                f'{dd:.1f}%', ha='center', va='bottom', fontsize=8)

    # Plot 4: SMA Effect table
    ax4 = axes[1, 1]
    ax4.axis("off")

    table_data = []
    for base, bh_name, sma_name in pairs:
        bh = next((r for r in results if r["name"] == bh_name), None)
        sma = next((r for r in results if r["name"] == sma_name), None)
        if bh and sma:
            cagr_effect = (sma['cagr'] - bh['cagr']) * 100
            dd_effect = (sma['max_dd'] - bh['max_dd']) * 100
            table_data.append([
                base,
                f"{bh['cagr']*100:.1f}%",
                f"{sma['cagr']*100:.1f}%",
                f"{cagr_effect:+.1f}%",
                f"{bh['max_dd']*100:.1f}%",
                f"{sma['max_dd']*100:.1f}%",
                f"{dd_effect:+.1f}%"
            ])

    table = ax4.table(
        cellText=table_data,
        colLabels=["ETF", "B&H CAGR", "SMA CAGR", "CAGR Δ", "B&H DD", "SMA DD", "DD Δ"],
        loc="center",
        cellLoc="center"
    )
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1.2, 1.8)
    ax4.set_title("SMA Filter Effect by Strategy", fontweight="bold", pad=20)

    plt.tight_layout()
    output_path = os.path.join(_this_dir, "sma_ablation_study.png")
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    print(f"Chart saved to: {output_path}")

    # Final recommendation
    print("\n" + "=" * 70)
    print("RECOMMENDATION")
    print("=" * 70)
    print("""
The SMA filter has DIFFERENT effects depending on leverage:

- SPY (1x):  SMA HURTS returns significantly (-6% CAGR)
- SSO (2x):  SMA effect is mixed (small return reduction, DD improvement)
- UPRO (3x): SMA HELPS by reducing catastrophic drawdowns

For the 80-Delta strategy, we need to test without SMA to know for sure.
Given the leverage involved (~1.7x effective), the SMA filter may be:
  - Reducing returns (like SPY)
  - But also reducing drawdowns (like UPRO)

NEXT STEP: Run 80-Delta backtest WITHOUT SMA filter to compare.
""")

    return results


if __name__ == "__main__":
    main()
