"""
Market Environment Analysis
============================
Compare strategy returns across different market conditions:
- Monthly returns for each strategy
- Rolling 12-month returns
- Performance in up/down/flat markets

Output:
- Monthly returns table
- Rolling 12-month returns table
- Summary by market environment
"""

import os
import pandas as pd
import numpy as np

# Force unbuffered output
import functools
print = functools.partial(print, flush=True)

_this_dir = os.path.dirname(os.path.abspath(__file__))

def load_daily_values():
    """Load daily portfolio values for each strategy."""
    strategies = {
        "SPY B&H": "daily_values_SPY_B&H.csv",
        "SSO B&H": "daily_values_SSO_B&H.csv",
        "UPRO B&H": "daily_values_UPRO_B&H.csv",
        "70-Delta": "daily_values_70-Delta.csv",
        "80-Delta": "daily_values_80-Delta.csv",
        "90-Delta": "daily_values_90-Delta.csv",
    }

    dfs = {}
    for name, filename in strategies.items():
        filepath = os.path.join(_this_dir, filename)
        if os.path.exists(filepath):
            df = pd.read_csv(filepath)
            df["date"] = pd.to_datetime(df["date"])
            df = df.set_index("date")
            dfs[name] = df["portfolio_value"]
        else:
            print(f"Warning: {filename} not found")

    return pd.DataFrame(dfs)


def calculate_monthly_returns(daily_values):
    """Calculate monthly returns for each strategy."""
    # Resample to month-end values
    monthly_values = daily_values.resample("ME").last()

    # Calculate monthly returns
    monthly_returns = monthly_values.pct_change()

    return monthly_returns


def calculate_rolling_12m_returns(daily_values):
    """Calculate rolling 12-month returns (end of each month)."""
    # Resample to month-end values
    monthly_values = daily_values.resample("ME").last()

    # Rolling 12-month return
    rolling_12m = monthly_values.pct_change(periods=12)

    return rolling_12m


def classify_market_environment(spy_return):
    """Classify market environment based on SPY monthly return."""
    if spy_return > 0.02:  # Up more than 2%
        return "Up (>2%)"
    elif spy_return < -0.02:  # Down more than 2%
        return "Down (<-2%)"
    else:  # Between -2% and +2%
        return "Flat (±2%)"


def main():
    print("=" * 100)
    print("MARKET ENVIRONMENT ANALYSIS")
    print("Comparing Strategy Returns Across Different Market Conditions")
    print("=" * 100)

    # Load data
    print("\nLoading daily values...")
    daily_values = load_daily_values()
    print(f"Loaded {len(daily_values.columns)} strategies from {daily_values.index.min().date()} to {daily_values.index.max().date()}")

    # Calculate monthly returns
    print("\nCalculating monthly returns...")
    monthly_returns = calculate_monthly_returns(daily_values)
    monthly_returns = monthly_returns.dropna()

    # Calculate rolling 12-month returns
    print("Calculating rolling 12-month returns...")
    rolling_12m = calculate_rolling_12m_returns(daily_values)
    rolling_12m = rolling_12m.dropna()

    # Classify market environments
    monthly_returns["Market"] = monthly_returns["SPY B&H"].apply(classify_market_environment)

    # ======================================================================
    # SECTION 1: Monthly Returns Summary Statistics
    # ======================================================================
    print("\n" + "=" * 100)
    print("SECTION 1: MONTHLY RETURNS SUMMARY")
    print("=" * 100)

    strategies = ["SPY B&H", "SSO B&H", "UPRO B&H", "70-Delta", "80-Delta", "90-Delta"]

    print(f"\n{'Strategy':<15} {'Mean':>10} {'Median':>10} {'Std Dev':>10} {'Min':>10} {'Max':>10} {'Months':>8}")
    print("-" * 75)
    for strat in strategies:
        if strat in monthly_returns.columns:
            s = monthly_returns[strat]
            print(f"{strat:<15} {s.mean():>+9.2%} {s.median():>+9.2%} {s.std():>9.2%} {s.min():>+9.2%} {s.max():>+9.2%} {len(s):>8}")

    # ======================================================================
    # SECTION 2: Performance by Market Environment
    # ======================================================================
    print("\n" + "=" * 100)
    print("SECTION 2: PERFORMANCE BY MARKET ENVIRONMENT")
    print("=" * 100)

    environments = ["Up (>2%)", "Flat (±2%)", "Down (<-2%)"]

    for env in environments:
        env_data = monthly_returns[monthly_returns["Market"] == env]
        n_months = len(env_data)
        pct = n_months / len(monthly_returns) * 100

        print(f"\n{env} Markets ({n_months} months, {pct:.1f}% of total)")
        print("-" * 90)
        print(f"{'Strategy':<15} {'Mean':>10} {'Median':>10} {'Win Rate':>10} {'Best':>10} {'Worst':>10}")
        print("-" * 90)

        for strat in strategies:
            if strat in env_data.columns:
                s = env_data[strat]
                win_rate = (s > 0).sum() / len(s) * 100 if len(s) > 0 else 0
                print(f"{strat:<15} {s.mean():>+9.2%} {s.median():>+9.2%} {win_rate:>9.1f}% {s.max():>+9.2%} {s.min():>+9.2%}")

    # ======================================================================
    # SECTION 3: Relative Performance vs SPY
    # ======================================================================
    print("\n" + "=" * 100)
    print("SECTION 3: RELATIVE PERFORMANCE VS SPY (Alpha)")
    print("=" * 100)

    print(f"\nAverage Monthly Alpha (Strategy Return - SPY Return)")
    print("-" * 80)
    print(f"{'Strategy':<15} {'Up Markets':>15} {'Flat Markets':>15} {'Down Markets':>15} {'All Markets':>15}")
    print("-" * 80)

    for strat in strategies:
        if strat == "SPY B&H" or strat not in monthly_returns.columns:
            continue

        alphas = {}
        for env in environments:
            env_data = monthly_returns[monthly_returns["Market"] == env]
            alpha = (env_data[strat] - env_data["SPY B&H"]).mean()
            alphas[env] = alpha

        all_alpha = (monthly_returns[strat] - monthly_returns["SPY B&H"]).mean()

        print(f"{strat:<15} {alphas['Up (>2%)']:>+14.2%} {alphas['Flat (±2%)']:>+14.2%} {alphas['Down (<-2%)']:>+14.2%} {all_alpha:>+14.2%}")

    # ======================================================================
    # SECTION 4: Rolling 12-Month Returns
    # ======================================================================
    print("\n" + "=" * 100)
    print("SECTION 4: ROLLING 12-MONTH RETURNS")
    print("=" * 100)

    print(f"\n{'Strategy':<15} {'Mean':>10} {'Median':>10} {'Std Dev':>10} {'Min':>12} {'Max':>12} {'% Positive':>12}")
    print("-" * 85)
    for strat in strategies:
        if strat in rolling_12m.columns:
            s = rolling_12m[strat]
            pct_pos = (s > 0).sum() / len(s) * 100
            print(f"{strat:<15} {s.mean():>+9.2%} {s.median():>+9.2%} {s.std():>9.2%} {s.min():>+11.2%} {s.max():>+11.2%} {pct_pos:>11.1f}%")

    # ======================================================================
    # SECTION 5: Worst 12-Month Periods
    # ======================================================================
    print("\n" + "=" * 100)
    print("SECTION 5: WORST ROLLING 12-MONTH PERIODS")
    print("=" * 100)

    print("\nWorst 12-month return for each strategy:")
    print("-" * 60)
    for strat in strategies:
        if strat in rolling_12m.columns:
            worst_idx = rolling_12m[strat].idxmin()
            worst_val = rolling_12m[strat].min()
            start_date = worst_idx - pd.DateOffset(months=12)
            print(f"{strat:<15} {worst_val:>+9.2%}  (12 months ending {worst_idx.strftime('%Y-%m')})")

    # ======================================================================
    # SECTION 6: Best 12-Month Periods
    # ======================================================================
    print("\n" + "=" * 100)
    print("SECTION 6: BEST ROLLING 12-MONTH PERIODS")
    print("=" * 100)

    print("\nBest 12-month return for each strategy:")
    print("-" * 60)
    for strat in strategies:
        if strat in rolling_12m.columns:
            best_idx = rolling_12m[strat].idxmax()
            best_val = rolling_12m[strat].max()
            print(f"{strat:<15} {best_val:>+9.2%}  (12 months ending {best_idx.strftime('%Y-%m')})")

    # ======================================================================
    # SECTION 7: Correlation During Different Markets
    # ======================================================================
    print("\n" + "=" * 100)
    print("SECTION 7: CORRELATION WITH SPY BY MARKET ENVIRONMENT")
    print("=" * 100)

    for env in environments:
        env_data = monthly_returns[monthly_returns["Market"] == env]
        print(f"\n{env} Markets - Correlation with SPY:")
        print("-" * 50)
        for strat in strategies:
            if strat == "SPY B&H" or strat not in env_data.columns:
                continue
            corr = env_data[strat].corr(env_data["SPY B&H"])
            print(f"  {strat:<15} {corr:>6.2f}")

    # ======================================================================
    # SECTION 8: Monthly Returns Detail (Last 24 Months)
    # ======================================================================
    print("\n" + "=" * 100)
    print("SECTION 8: MONTHLY RETURNS - LAST 24 MONTHS")
    print("=" * 100)

    recent = monthly_returns.tail(24).copy()
    recent = recent.drop(columns=["Market"])

    print(f"\n{'Month':<12}", end="")
    for strat in strategies:
        if strat in recent.columns:
            print(f"{strat:>12}", end="")
    print()
    print("-" * (12 + 12 * len([s for s in strategies if s in recent.columns])))

    for idx, row in recent.iterrows():
        print(f"{idx.strftime('%Y-%m'):<12}", end="")
        for strat in strategies:
            if strat in recent.columns:
                val = row[strat]
                print(f"{val:>+11.1%}", end=" ")
        print()

    # ======================================================================
    # SECTION 9: Rolling 12-Month Returns Detail (Last 24 Months)
    # ======================================================================
    print("\n" + "=" * 100)
    print("SECTION 9: ROLLING 12-MONTH RETURNS - LAST 24 MONTHS")
    print("=" * 100)

    recent_rolling = rolling_12m.tail(24).copy()

    print(f"\n{'Month':<12}", end="")
    for strat in strategies:
        if strat in recent_rolling.columns:
            print(f"{strat:>12}", end="")
    print()
    print("-" * (12 + 12 * len([s for s in strategies if s in recent_rolling.columns])))

    for idx, row in recent_rolling.iterrows():
        print(f"{idx.strftime('%Y-%m'):<12}", end="")
        for strat in strategies:
            if strat in recent_rolling.columns:
                val = row[strat]
                print(f"{val:>+11.1%}", end=" ")
        print()

    # ======================================================================
    # SECTION 10: Key Insights Summary
    # ======================================================================
    print("\n" + "=" * 100)
    print("SECTION 10: KEY INSIGHTS SUMMARY")
    print("=" * 100)

    # Calculate key comparisons
    up_data = monthly_returns[monthly_returns["Market"] == "Up (>2%)"]
    down_data = monthly_returns[monthly_returns["Market"] == "Down (<-2%)"]

    print("\n1. LEVERAGE CAPTURE RATIO (Strategy gain in up markets / SPY gain)")
    print("-" * 60)
    for strat in ["SSO B&H", "UPRO B&H", "70-Delta", "80-Delta", "90-Delta"]:
        if strat in up_data.columns:
            ratio = up_data[strat].mean() / up_data["SPY B&H"].mean()
            print(f"   {strat:<15} {ratio:.2f}x SPY upside capture")

    print("\n2. DOWNSIDE RATIO (Strategy loss in down markets / SPY loss)")
    print("-" * 60)
    for strat in ["SSO B&H", "UPRO B&H", "70-Delta", "80-Delta", "90-Delta"]:
        if strat in down_data.columns:
            ratio = down_data[strat].mean() / down_data["SPY B&H"].mean()
            print(f"   {strat:<15} {ratio:.2f}x SPY downside exposure")

    print("\n3. UP/DOWN CAPTURE RATIO (Upside capture / Downside exposure)")
    print("   (Higher is better - means more upside capture per unit of downside)")
    print("-" * 60)
    for strat in ["SSO B&H", "UPRO B&H", "70-Delta", "80-Delta", "90-Delta"]:
        if strat in up_data.columns and strat in down_data.columns:
            up_ratio = up_data[strat].mean() / up_data["SPY B&H"].mean()
            down_ratio = down_data[strat].mean() / down_data["SPY B&H"].mean()
            capture_ratio = up_ratio / down_ratio if down_ratio != 0 else 0
            print(f"   {strat:<15} {capture_ratio:.2f}")

    # Save outputs
    print("\n" + "=" * 100)
    print("Saving data to CSV files...")

    monthly_returns.to_csv(os.path.join(_this_dir, "monthly_returns_all_strategies.csv"))
    rolling_12m.to_csv(os.path.join(_this_dir, "rolling_12m_returns_all_strategies.csv"))

    print("  - monthly_returns_all_strategies.csv")
    print("  - rolling_12m_returns_all_strategies.csv")
    print("\nDone!")


if __name__ == "__main__":
    main()
