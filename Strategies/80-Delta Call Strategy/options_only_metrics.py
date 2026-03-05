"""
True options-only portfolio: $100K in 80-delta calls, no shares, no delta cap.

Overrides delta_capped_backtest.SHARES to a huge number so the delta cap
never binds, runs a single backtest (no covered calls), then computes
risk metrics on the options-only equity curve.

Usage:
    python -u options_only_metrics.py > options_only_output.txt 2>&1
"""
import os
import sys
import logging

import numpy as np
import pandas as pd

_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_dir = os.path.dirname(os.path.dirname(_this_dir))
sys.path.insert(0, _project_dir)

# Override SHARES before the simulation function reads it.
# This removes the delta cap (set to 999,999 so it never binds).
import delta_capped_backtest as dcb
dcb.SHARES = 999_999

from delta_capped_backtest import (
    load_all_data, run_delta_capped_simulation, compute_metrics,
    ThetaDataClient, OPTIONS_CASH_ALLOCATION, SIM_START, DATA_END,
)


def options_only_risk_metrics(snapshots_df):
    """Compute full risk metrics for the options-only equity curve."""
    df = snapshots_df.copy()

    # Options-only equity = cash + positions - any CC liability
    df["options_equity"] = df["options_cash"] + df["options_value"] - df["cc_liability"]

    equity = df["options_equity"].values
    dates = df["date"].values

    n_days = len(equity)
    years = n_days / 252.0

    start_val = equity[0]
    end_val = equity[-1]
    total_return = end_val / start_val - 1
    cagr = (end_val / start_val) ** (1 / years) - 1 if years > 0 else 0

    daily_ret = np.diff(equity) / equity[:-1]
    daily_mean = np.mean(daily_ret)
    daily_std = np.std(daily_ret)
    sharpe = (daily_mean / daily_std) * np.sqrt(252) if daily_std > 0 else 0

    neg_rets = daily_ret[daily_ret < 0]
    ds_std = np.std(neg_rets) if len(neg_rets) > 0 else 0
    sortino = (daily_mean / ds_std) * np.sqrt(252) if ds_std > 0 else 0

    cummax = np.maximum.accumulate(equity)
    drawdown = equity / cummax - 1
    max_dd = drawdown.min()
    calmar = cagr / abs(max_dd) if max_dd != 0 else 0

    # Worst 12-month rolling return
    if n_days >= 252:
        rolling_12m = np.array([equity[i] / equity[i - 252] - 1 for i in range(252, n_days)])
        worst_12m = rolling_12m.min()
    else:
        worst_12m = total_return

    # Time underwater (max consecutive days below prior peak)
    underwater_days = 0
    current_streak = 0
    for i in range(len(equity)):
        if equity[i] < cummax[i]:
            current_streak += 1
            underwater_days = max(underwater_days, current_streak)
        else:
            current_streak = 0

    # Year-by-year returns
    dates_pd = pd.to_datetime(dates)
    yearly = {}
    for year in sorted(set(d.year for d in dates_pd)):
        mask = np.array([d.year == year for d in dates_pd])
        yr_eq = equity[mask]
        if len(yr_eq) > 1:
            yearly[year] = yr_eq[-1] / yr_eq[0] - 1

    return {
        "start_val": start_val,
        "end_val": end_val,
        "total_return": total_return,
        "cagr": cagr,
        "sharpe": sharpe,
        "sortino": sortino,
        "max_dd": max_dd,
        "calmar": calmar,
        "worst_12m": worst_12m,
        "max_underwater_days": underwater_days,
        "n_days": n_days,
        "years": years,
        "yearly": yearly,
        "equity_series": equity,
        "dates": dates,
    }


def main():
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(message)s")

    W = 80
    print("=" * W)
    print("TRUE OPTIONS-ONLY PORTFOLIO: $100K, NO SHARES, NO DELTA CAP")
    print("=" * W)
    print(f"\n  Starting capital:  ${OPTIONS_CASH_ALLOCATION:,}")
    print(f"  SPY shares:        0 (options only)")
    print(f"  Delta cap:         None (uncapped)")
    print(f"  Covered calls:     No (no shares to cover)")
    print(f"  Strategy:          80-delta calls, SMA200 filter, +50% PT, 60-day MH")
    print(f"  Period:            {SIM_START} to {DATA_END}")

    client = ThetaDataClient()
    if not client.connect():
        print("\nERROR: Cannot connect to Theta Terminal.")
        return

    print("\nConnected to Theta Terminal. Loading data...")
    spy_by_date, trading_dates, vix_data, sma200, monthly_exps, trailing_12m_returns, rolling_volatility = load_all_data(client)

    # Single run: no covered calls, delta cap effectively disabled
    print("\nRunning backtest: 80-delta calls, no cap, no shares...")
    snaps, trades, cc_trades = run_delta_capped_simulation(
        client, spy_by_date, trading_dates, vix_data, sma200, monthly_exps,
        trailing_12m_returns=trailing_12m_returns,
        rolling_volatility=rolling_volatility,
        force_exit_below_sma=True,
        sell_covered_calls=False,
        label="Options-Only (no cap)",
    )

    client.close()

    if not snaps:
        print("\nInsufficient data.")
        return

    # Combined metrics (includes the phantom shares — we'll ignore those)
    m = compute_metrics(snaps, trades, cc_trades, "Options-Only (no cap)")

    # True options-only metrics
    opt = options_only_risk_metrics(m["snapshots_df"])

    # Trade stats
    tdf = m.get("trade_df", pd.DataFrame())

    print(f"\n{'=' * W}")
    print("OPTIONS-ONLY RESULTS")
    print(f"{'=' * W}")

    rows = [
        ("Starting Value", f"${opt['start_val']:>12,.0f}"),
        ("Ending Value", f"${opt['end_val']:>12,.0f}"),
        ("Total Return", f"{opt['total_return']:>+12.1%}"),
        ("CAGR", f"{opt['cagr']:>+12.1%}"),
        ("Sharpe", f"{opt['sharpe']:>12.2f}"),
        ("Sortino", f"{opt['sortino']:>12.2f}"),
        ("Max Drawdown", f"{opt['max_dd']:>12.1%}"),
        ("Calmar", f"{opt['calmar']:>12.2f}"),
        ("Worst 12-Month", f"{opt['worst_12m']:>12.1%}"),
        ("Max Underwater (days)", f"{opt['max_underwater_days']:>12}"),
    ]
    for name, val in rows:
        print(f"  {name:<30} {val}")

    # Trade stats
    if len(tdf) > 0:
        wins = tdf[tdf["pnl_pct"] > 0]
        losses = tdf[tdf["pnl_pct"] <= 0]
        print(f"\n  {'--- Trade Statistics ---':^50}")
        print(f"  {'Total Trades':<30} {len(tdf):>12}")
        print(f"  {'Win Rate':<30} {len(wins)/len(tdf):>12.1%}")
        print(f"  {'Mean Return':<30} {tdf['pnl_pct'].mean():>+12.1%}")
        print(f"  {'Total P&L':<30} ${tdf['pnl_dollar'].sum():>+11,.0f}")
        print(f"  {'Avg Days Held':<30} {tdf['days_held'].mean():>12.0f}")

    # Year-by-year
    print(f"\n  {'--- Year-by-Year ---':^50}")
    print(f"  {'Year':<8} {'Options Return':>15} {'SPY Return':>15}")
    print(f"  {'-' * 40}")
    df = m["snapshots_df"]
    df_dates = pd.to_datetime(df["date"])
    for year, ret in sorted(opt["yearly"].items()):
        # SPY return for same year
        mask = df_dates.dt.year == year
        spy_yr = df.loc[mask, "spy_close"]
        spy_ret = spy_yr.iloc[-1] / spy_yr.iloc[0] - 1 if len(spy_yr) > 1 else 0
        print(f"  {year:<8} {ret:>+14.1%} {spy_ret:>+14.1%}")

    # Comparison
    print(f"\n{'=' * W}")
    print("COMPARISON vs SPY B&H")
    print(f"{'=' * W}")
    print(f"  {'Metric':<25} {'Options-Only':>18} {'SPY B&H':>18}")
    print(f"  {'-' * 63}")
    print(f"  {'CAGR':<25} {opt['cagr']:>+17.1%} {m['spy_cagr']:>+17.1%}")
    print(f"  {'Sharpe':<25} {opt['sharpe']:>18.2f} {m['spy_sharpe']:>18.2f}")
    print(f"  {'Max DD':<25} {opt['max_dd']:>17.1%} {m['spy_dd']:>17.1%}")

    print(f"\n{'=' * W}")


if __name__ == "__main__":
    main()
