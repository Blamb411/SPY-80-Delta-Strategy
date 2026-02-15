"""
Leverage Analysis: What SPY Leverage Matches 80-Delta Returns?
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os

# Force unbuffered output
import functools
print = functools.partial(print, flush=True)

_this_dir = os.path.dirname(os.path.abspath(__file__))

def main():
    print("=" * 80)
    print("Leverage Analysis: What SPY Leverage Matches 80-Delta Returns?")
    print("=" * 80)

    # Load SPY and 80-Delta daily values
    spy_df = pd.read_csv(os.path.join(_this_dir, "daily_values_SPY_B&H.csv"))
    delta80_df = pd.read_csv(os.path.join(_this_dir, "daily_values_80-Delta.csv"))

    spy_df["date"] = pd.to_datetime(spy_df["date"])
    delta80_df["date"] = pd.to_datetime(delta80_df["date"])

    # Filter to 2009+ for fair comparison
    start_date = "2009-06-25"
    spy_df = spy_df[spy_df["date"] >= start_date].copy()
    delta80_df = delta80_df[delta80_df["date"] >= start_date].copy()

    # Rebase both to $100K
    spy_df["portfolio_value"] = spy_df["portfolio_value"] / spy_df["portfolio_value"].iloc[0] * 100000
    delta80_df["portfolio_value"] = delta80_df["portfolio_value"] / delta80_df["portfolio_value"].iloc[0] * 100000

    # Calculate SPY daily returns
    spy_df["daily_return"] = spy_df["portfolio_value"].pct_change().fillna(0)

    # Target: 80-Delta ending value
    target_end_value = delta80_df["portfolio_value"].iloc[-1]
    print(f"\n80-Delta ending value: ${target_end_value:,.0f}")
    print(f"SPY B&H ending value: ${spy_df['portfolio_value'].iloc[-1]:,.0f}")

    # Test different leverage levels
    print("\n" + "-" * 70)
    print("Testing leverage levels to match 80-Delta returns...")
    print("-" * 70)

    leverage_results = []

    for leverage in [1.0, 1.25, 1.5, 1.6, 1.65, 1.7, 1.75, 1.8, 1.85, 1.9, 2.0, 2.5, 3.0]:
        # Simulate leveraged SPY returns (daily rebalancing like a leveraged ETF)
        leveraged_values = [100000]
        for ret in spy_df["daily_return"].iloc[1:]:
            new_val = leveraged_values[-1] * (1 + leverage * ret)
            leveraged_values.append(max(0, new_val))

        spy_df[f"lev_{leverage}"] = leveraged_values

        end_val = leveraged_values[-1]

        # Calculate metrics
        years = len(spy_df) / 252
        cagr = (end_val / 100000) ** (1/years) - 1 if end_val > 0 else -1

        daily_rets = pd.Series(leveraged_values).pct_change().dropna()
        sharpe = (daily_rets.mean() / daily_rets.std()) * np.sqrt(252) if daily_rets.std() > 0 else 0

        downside = daily_rets[daily_rets < 0]
        sortino = (daily_rets.mean() / downside.std()) * np.sqrt(252) if len(downside) > 0 and downside.std() > 0 else 0

        cummax = pd.Series(leveraged_values).cummax()
        drawdown = pd.Series(leveraged_values) / cummax - 1
        max_dd = drawdown.min()

        leverage_results.append({
            "leverage": leverage,
            "end_val": end_val,
            "cagr": cagr,
            "sharpe": sharpe,
            "sortino": sortino,
            "max_dd": max_dd,
        })

        print(f"  {leverage:.2f}x SPY: ${end_val:>12,.0f} | CAGR: {cagr:>+6.1%} | Sharpe: {sharpe:.2f} | Max DD: {max_dd:.1%}")

    # Binary search for exact leverage
    print("\n" + "=" * 70)
    print("FINDING EXACT LEVERAGE TO MATCH 80-DELTA")
    print("=" * 70)

    low, high = 1.0, 3.0
    target = target_end_value

    for _ in range(50):
        mid = (low + high) / 2
        leveraged_values = [100000]
        for ret in spy_df["daily_return"].iloc[1:]:
            new_val = leveraged_values[-1] * (1 + mid * ret)
            leveraged_values.append(max(0, new_val))

        if leveraged_values[-1] < target:
            low = mid
        else:
            high = mid

    exact_leverage = (low + high) / 2

    # Calculate final metrics for exact leverage
    leveraged_values = [100000]
    for ret in spy_df["daily_return"].iloc[1:]:
        new_val = leveraged_values[-1] * (1 + exact_leverage * ret)
        leveraged_values.append(max(0, new_val))

    spy_df["lev_exact"] = leveraged_values

    years = len(spy_df) / 252
    lev_end_val = leveraged_values[-1]
    lev_cagr = (lev_end_val / 100000) ** (1/years) - 1

    daily_rets = pd.Series(leveraged_values).pct_change().dropna()
    lev_sharpe = (daily_rets.mean() / daily_rets.std()) * np.sqrt(252)

    downside = daily_rets[daily_rets < 0]
    lev_sortino = (daily_rets.mean() / downside.std()) * np.sqrt(252)

    cummax = pd.Series(leveraged_values).cummax()
    drawdown = pd.Series(leveraged_values) / cummax - 1
    lev_max_dd = drawdown.min()

    # Get 80-Delta metrics
    d80_daily_rets = delta80_df["portfolio_value"].pct_change().dropna()
    d80_sharpe = (d80_daily_rets.mean() / d80_daily_rets.std()) * np.sqrt(252)
    d80_downside = d80_daily_rets[d80_daily_rets < 0]
    d80_sortino = (d80_daily_rets.mean() / d80_downside.std()) * np.sqrt(252)
    d80_cummax = delta80_df["portfolio_value"].cummax()
    d80_drawdown = delta80_df["portfolio_value"] / d80_cummax - 1
    d80_max_dd = d80_drawdown.min()
    d80_cagr = (target_end_value / 100000) ** (1/years) - 1

    print(f"\nTo match 80-Delta returns, you need: {exact_leverage:.2f}x LEVERAGE on SPY")
    print()
    print(f"{'Metric':<20} {'80-Delta':>15} {f'{exact_leverage:.2f}x SPY':>15} {'Difference':>15}")
    print("-" * 65)
    print(f"{'End Value':<20} ${target_end_value:>13,.0f} ${lev_end_val:>13,.0f} ${lev_end_val - target_end_value:>+13,.0f}")
    print(f"{'CAGR':<20} {d80_cagr:>+14.1%} {lev_cagr:>+14.1%} {lev_cagr - d80_cagr:>+14.1%}")
    print(f"{'Sharpe':<20} {d80_sharpe:>15.2f} {lev_sharpe:>15.2f} {lev_sharpe - d80_sharpe:>+15.2f}")
    print(f"{'Sortino':<20} {d80_sortino:>15.2f} {lev_sortino:>15.2f} {lev_sortino - d80_sortino:>+15.2f}")
    print(f"{'Max Drawdown':<20} {d80_max_dd:>14.1%} {lev_max_dd:>14.1%} {lev_max_dd - d80_max_dd:>+14.1%}")
    print()

    # Key insight
    dd_diff = abs(lev_max_dd) - abs(d80_max_dd)
    print("=" * 70)
    print("KEY INSIGHT")
    print("=" * 70)
    print(f"\n  To match 80-Delta's {d80_cagr:+.1%} CAGR, you need {exact_leverage:.2f}x leveraged SPY.")
    print(f"\n  BUT: {exact_leverage:.2f}x SPY has a {lev_max_dd:.1%} max drawdown")
    print(f"        80-Delta has only a {d80_max_dd:.1%} max drawdown")
    print(f"\n  DIFFERENCE: {dd_diff:.1f}% MORE DRAWDOWN with leveraged SPY!")
    print(f"\n  At the low point, $100K in {exact_leverage:.2f}x SPY dropped to ${100000 * (1 + lev_max_dd):,.0f}")
    print(f"  At the low point, $100K in 80-Delta dropped to ${100000 * (1 + d80_max_dd):,.0f}")
    print()

    # Create comparison chart
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))

    # Plot 1: Portfolio values comparison
    ax1 = axes[0, 0]
    ax1.semilogy(spy_df["date"], spy_df["portfolio_value"], label="SPY B&H (1x)", linewidth=1.5, color="blue")
    ax1.semilogy(spy_df["date"], spy_df["lev_exact"], label=f"{exact_leverage:.2f}x SPY", linewidth=2, color="red", linestyle="--")
    ax1.semilogy(delta80_df["date"], delta80_df["portfolio_value"], label="80-Delta", linewidth=2.5, color="green")
    ax1.set_title("Portfolio Value Comparison (Log Scale)", fontweight="bold")
    ax1.set_ylabel("Portfolio Value ($)")
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    # Plot 2: Drawdown comparison
    ax2 = axes[0, 1]
    lev_dd = pd.Series(leveraged_values) / pd.Series(leveraged_values).cummax() - 1
    d80_dd = delta80_df["portfolio_value"] / delta80_df["portfolio_value"].cummax() - 1

    ax2.fill_between(spy_df["date"], lev_dd * 100, 0, alpha=0.5, color="red", label=f"{exact_leverage:.2f}x SPY")
    ax2.fill_between(delta80_df["date"], d80_dd.values * 100, 0, alpha=0.5, color="green", label="80-Delta")
    ax2.set_title("Drawdown Comparison", fontweight="bold")
    ax2.set_ylabel("Drawdown (%)")
    ax2.legend()
    ax2.grid(True, alpha=0.3)

    # Plot 3: Leverage ladder
    ax3 = axes[1, 0]
    leverages = [r["leverage"] for r in leverage_results]
    end_vals = [r["end_val"] for r in leverage_results]

    ax3.bar([str(l) + "x" for l in leverages], end_vals, color="steelblue", alpha=0.7)
    ax3.axhline(y=target_end_value, color="green", linestyle="--", linewidth=2, label="80-Delta Target")
    ax3.set_title("Ending Value by SPY Leverage", fontweight="bold")
    ax3.set_ylabel("Ending Value ($)")
    ax3.set_xlabel("Leverage")
    ax3.legend()
    ax3.tick_params(axis="x", rotation=45)

    # Plot 4: Risk/Return tradeoff
    ax4 = axes[1, 1]
    for r in leverage_results:
        ax4.scatter(abs(r["max_dd"]) * 100, r["cagr"] * 100, s=100, alpha=0.7)
        ax4.annotate(f"{r['leverage']}x", (abs(r["max_dd"]) * 100, r["cagr"] * 100),
                    textcoords="offset points", xytext=(5, 5), fontsize=8)
    ax4.scatter(abs(d80_max_dd) * 100, d80_cagr * 100, s=200, color="green", marker="*", label="80-Delta", zorder=5)
    ax4.set_title("Risk vs Return: Leveraged SPY vs 80-Delta", fontweight="bold")
    ax4.set_xlabel("Max Drawdown (%)")
    ax4.set_ylabel("CAGR (%)")
    ax4.legend(loc="upper left", fontsize=10)
    ax4.grid(True, alpha=0.3)

    plt.tight_layout()
    output_path = os.path.join(_this_dir, "leverage_analysis.png")
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    print(f"Chart saved to: {output_path}")

    # Return key values for report
    return {
        "exact_leverage": exact_leverage,
        "d80_cagr": d80_cagr,
        "d80_max_dd": d80_max_dd,
        "d80_sharpe": d80_sharpe,
        "d80_sortino": d80_sortino,
        "lev_cagr": lev_cagr,
        "lev_max_dd": lev_max_dd,
        "lev_sharpe": lev_sharpe,
        "lev_sortino": lev_sortino,
    }


if __name__ == "__main__":
    main()
