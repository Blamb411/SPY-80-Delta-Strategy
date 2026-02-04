"""
Delta Sensitivity Charts
========================
Generates comparison charts showing combined portfolio performance
across different option deltas [0.60, 0.70, 0.80, 0.90, 1.00]
for both SPY and QQQ strategies.

Each figure (one per ticker) has 2 subplots:
  - Portfolio value curves per delta + B&H reference
  - Drawdown curves per delta

Also prints a summary comparison table of all metrics by delta.

Reuses data-loading and simulation functions from spy_combined_analysis
and qqq_combined_analysis via monkey-patching the DELTA constant.

Requires:
  - Theta Terminal v3 running locally
  - thetadata_cache.db (auto-populated)

Usage:
    python delta_sensitivity_charts.py
"""

import os
import sys
import logging

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as mticker

_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_dir = os.path.dirname(os.path.dirname(_this_dir))
sys.path.insert(0, _project_dir)
sys.path.insert(0, _this_dir)

from backtest.thetadata_client import ThetaDataClient

import spy_combined_analysis as spy_mod
import qqq_combined_analysis as qqq_mod

# ======================================================================
# CONFIG
# ======================================================================

DELTAS = [0.60, 0.70, 0.80, 0.90, 1.00]

# Color palette: one per delta, distinguishable
DELTA_COLORS = {
    0.60: "#2ca02c",   # green
    0.70: "#ff7f0e",   # orange
    0.80: "#1f77b4",   # blue (baseline)
    0.90: "#d62728",   # red
    1.00: "#9467bd",   # purple
}

C_BH = "#7f7f7f"       # gray for B&H reference


def _fmt_dollars(x, _):
    if x >= 1_000_000:
        return f"${x / 1_000_000:.1f}M"
    if x >= 1_000:
        return f"${x / 1_000:.0f}K"
    return f"${x:.0f}"


def compute_stats(daily_values, label=""):
    """Compute key portfolio metrics from a daily value series."""
    pv = np.array(daily_values, dtype=float)
    n = len(pv)
    years = n / 252.0
    rets = np.diff(pv) / pv[:-1]
    rets = rets[np.isfinite(rets)]

    cagr = (pv[-1] / pv[0]) ** (1 / years) - 1 if years > 0 and pv[0] > 0 else 0
    total_ret = pv[-1] / pv[0] - 1 if pv[0] > 0 else 0

    mean_r = rets.mean()
    std_r = rets.std()
    sharpe = (mean_r / std_r) * np.sqrt(252) if std_r > 0 else 0

    down = rets[rets < 0]
    ds_std = down.std() if len(down) > 0 else 0
    sortino = (mean_r / ds_std) * np.sqrt(252) if ds_std > 0 else 0

    cummax = np.maximum.accumulate(pv)
    dd = pv / cummax - 1
    max_dd = dd.min()
    calmar = cagr / abs(max_dd) if max_dd < 0 else 0
    vol = std_r * np.sqrt(252)

    return {
        "label": label, "start": pv[0], "end": pv[-1],
        "total_ret": total_ret, "cagr": cagr,
        "sharpe": sharpe, "sortino": sortino,
        "max_dd": max_dd, "calmar": calmar, "vol": vol,
    }


# ======================================================================
# SPY DELTA SENSITIVITY
# ======================================================================

def run_spy_sensitivity(client, bil_rets):
    """Run SPY combined portfolio at each delta. Returns dict of results."""

    print("\n" + "=" * 70)
    print("SPY DELTA SENSITIVITY")
    print("=" * 70)

    # Load data once
    spy_by_date, trading_dates, vix_data, sma200, monthly_exps = spy_mod.load_all_data(client)

    # Compute CC contracts (delta-independent)
    sim_start_idx = next((i for i, d in enumerate(trading_dates)
                          if d >= spy_mod.SIM_START), 0)
    sim_start_date = trading_dates[sim_start_idx]
    start_price = spy_by_date[sim_start_date]["close"]
    n_shares = int(spy_mod.ALLOC // start_price)
    n_cc_contracts = n_shares // 100

    # Run CC overlay once (uses CC_DELTA=0.30, not DELTA)
    print("\n  Running CC overlay (delta-independent)...")
    cc_values, cc_trade_log = spy_mod.run_cc_overlay(
        client, spy_by_date, trading_dates, vix_data, sma200, monthly_exps,
        n_cc_contracts, spy_mod.SIM_START,
    )

    results = {}
    original_delta = spy_mod.DELTA

    for delta in DELTAS:
        print(f"\n{'~' * 60}")
        print(f"  SPY: Running delta = {delta:.2f}")
        print(f"{'~' * 60}")

        # Monkey-patch DELTA
        spy_mod.DELTA = delta

        # Run options simulation
        snapshots, trade_log = spy_mod.run_options_sim(
            client, spy_by_date, trading_dates, vix_data, sma200, monthly_exps,
        )

        # Build combined portfolio
        port = spy_mod.build_portfolios(
            snapshots, bil_rets, spy_by_date, cc_values, spy_mod.SIM_START,
        )

        # Stats
        s_combined = compute_stats(port["combined_w_cc"],
                                   f"Delta {delta:.2f}")
        s_bh = compute_stats(port["spy_bh"], "SPY B&H")

        results[delta] = {
            "port": port,
            "stats": s_combined,
            "bh_stats": s_bh,
            "snapshots": snapshots,
            "trade_log": trade_log,
        }

    # Restore original DELTA
    spy_mod.DELTA = original_delta

    return results


# ======================================================================
# QQQ DELTA SENSITIVITY
# ======================================================================

def run_qqq_sensitivity(client, bil_rets):
    """Run QQQ combined portfolio at each delta. Returns dict of results."""

    print("\n" + "=" * 70)
    print("QQQ DELTA SENSITIVITY")
    print("=" * 70)

    # Load data once
    qqq_by_date, spy_by_date, trading_dates, vix_data, sma200, monthly_exps = \
        qqq_mod.load_all_data(client)

    # Compute CC contracts (delta-independent)
    sim_start_idx = next((i for i, d in enumerate(trading_dates)
                          if d >= qqq_mod.SIM_START), 0)
    sim_start_date = trading_dates[sim_start_idx]
    start_price = qqq_by_date[sim_start_date]["close"]
    n_shares = int(qqq_mod.ALLOC // start_price)
    n_cc_contracts = n_shares // 100

    # Run CC overlay once
    print("\n  Running CC overlay (delta-independent)...")
    cc_values, cc_trade_log = qqq_mod.run_cc_overlay(
        client, qqq_by_date, trading_dates, vix_data, sma200, monthly_exps,
        n_cc_contracts, qqq_mod.SIM_START,
    )

    results = {}
    original_delta = qqq_mod.DELTA

    for delta in DELTAS:
        print(f"\n{'~' * 60}")
        print(f"  QQQ: Running delta = {delta:.2f}")
        print(f"{'~' * 60}")

        # Monkey-patch DELTA
        qqq_mod.DELTA = delta

        # Run options simulation
        snapshots, trade_log = qqq_mod.run_options_sim(
            client, qqq_by_date, trading_dates, vix_data, sma200, monthly_exps,
        )

        # Build combined portfolio
        port = qqq_mod.build_portfolios(
            snapshots, bil_rets, qqq_by_date, spy_by_date, cc_values,
        )

        # Stats
        s_combined = compute_stats(port["combined_w_cc"],
                                   f"Delta {delta:.2f}")
        s_bh = compute_stats(port["qqq_bh"], "QQQ B&H")

        results[delta] = {
            "port": port,
            "stats": s_combined,
            "bh_stats": s_bh,
            "snapshots": snapshots,
            "trade_log": trade_log,
        }

    # Restore original DELTA
    qqq_mod.DELTA = original_delta

    return results


# ======================================================================
# CHART GENERATION
# ======================================================================

def make_sensitivity_figure(results, ticker, bh_key, filename):
    """
    Generate a 2-subplot figure showing portfolio value and drawdowns
    across all tested deltas.

    Parameters
    ----------
    results : dict   {delta: {"port": {...}, "stats": {...}, ...}}
    ticker  : str    "SPY" or "QQQ"
    bh_key  : str    "spy_bh" or "qqq_bh" -- key in port dict for B&H series
    filename: str    output PNG path
    """

    # Get dates from the first delta's results
    first_delta = DELTAS[0]
    dates_str = results[first_delta]["port"]["dates"]
    dates = pd.to_datetime(dates_str)
    dt_vals = dates.values
    n_days = len(dates)
    years = n_days / 252.0

    # B&H series (same for all deltas)
    bh = results[first_delta]["port"][bh_key]
    bh_end = bh[-1]
    bh_cagr = (bh_end / bh[0]) ** (1 / years) - 1 if years > 0 else 0

    # ---- Figure ----
    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(16, 11),
        gridspec_kw={"height_ratios": [3, 1.5], "hspace": 0.18},
    )

    fig.suptitle(
        f"{ticker} Combined Portfolio: Delta Sensitivity Analysis\n"
        f"($1M {ticker} B&H + $1M Options + BIL + CC, "
        f"{dates_str[0][:4]}\u2013{dates_str[-1][:4]})",
        fontsize=14, fontweight="bold", y=0.98,
    )

    # ---- Subplot 1: Portfolio Value ----
    # B&H reference
    ax1.plot(dt_vals, bh, color=C_BH, linewidth=1.4, linestyle="--",
             label=f"{ticker} B&H ($1M, ${bh_end/1e6:.1f}M, {bh_cagr:+.1%} CAGR)")

    for delta in DELTAS:
        port = results[delta]["port"]
        pv = port["combined_w_cc"]
        stats = results[delta]["stats"]
        c = DELTA_COLORS[delta]
        lw = 2.0 if delta == 0.80 else 1.4
        style = "-" if delta == 0.80 else "-"

        label = (f"Delta {delta:.2f}  "
                 f"(${pv[-1]/1e6:.1f}M, {stats['cagr']:+.1%}, "
                 f"Sh={stats['sharpe']:.2f})")
        ax1.plot(dt_vals, pv, color=c, linewidth=lw, linestyle=style,
                 label=label)

    ax1.set_ylabel("Portfolio Value ($2M start)", fontsize=11)
    ax1.yaxis.set_major_formatter(mticker.FuncFormatter(_fmt_dollars))
    ax1.legend(loc="upper left", fontsize=8.5, framealpha=0.9)
    ax1.grid(True, alpha=0.25)
    ax1.set_xlim(dt_vals[0], dt_vals[-1])

    # ---- Subplot 2: Drawdown ----
    for delta in DELTAS:
        pv = results[delta]["port"]["combined_w_cc"]
        dd = pv / np.maximum.accumulate(pv) - 1
        c = DELTA_COLORS[delta]
        lw = 1.6 if delta == 0.80 else 1.1
        ax2.plot(dt_vals, dd, color=c, linewidth=lw,
                 label=f"Delta {delta:.2f} (max {dd.min():.1%})")

    # B&H drawdown
    dd_bh = bh / np.maximum.accumulate(bh) - 1
    ax2.plot(dt_vals, dd_bh, color=C_BH, linewidth=1.0, linestyle="--",
             label=f"{ticker} B&H ({dd_bh.min():.1%})")

    ax2.set_ylabel("Drawdown", fontsize=11)
    ax2.set_xlabel("Date", fontsize=11)
    ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0%}"))
    ax2.legend(loc="lower left", fontsize=8, ncol=3, framealpha=0.9)
    ax2.grid(True, alpha=0.25)
    ax2.set_xlim(dt_vals[0], dt_vals[-1])

    # Format x-axes
    for ax in [ax1, ax2]:
        ax.xaxis.set_major_locator(mdates.YearLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
        ax.tick_params(axis="x", labelsize=9)

    plt.savefig(filename, dpi=150, bbox_inches="tight")
    print(f"\n  Saved: {filename}")
    plt.close(fig)


def print_comparison_table(results, ticker, bh_key):
    """Print a formatted metrics comparison table across deltas."""
    W = 110
    print(f"\n{'=' * W}")
    print(f"{ticker} DELTA SENSITIVITY -- COMBINED PORTFOLIO METRICS")
    print(f"{'=' * W}")
    print(f"  (Combined = $1M {ticker} B&H + $1M options + BIL + covered calls)")
    print()

    # Header
    print(f"  {'Delta':>6} {'End Value':>14} {'Total Ret':>10} {'CAGR':>8} "
          f"{'Sharpe':>8} {'Sortino':>8} {'Max DD':>8} {'Calmar':>8} {'Vol':>8}")
    print(f"  {'-' * (W - 4)}")

    # B&H reference
    bh_stats = results[DELTAS[0]]["bh_stats"]
    print(f"  {'B&H':>6} ${bh_stats['end']:>12,.0f} {bh_stats['total_ret']:>+9.1%} "
          f"{bh_stats['cagr']:>+7.1%} {bh_stats['sharpe']:>8.2f} "
          f"{bh_stats['sortino']:>8.2f} {bh_stats['max_dd']:>7.1%} "
          f"{bh_stats['calmar']:>8.2f} {bh_stats['vol']:>7.1%}")
    print(f"  {'-' * (W - 4)}")

    # Each delta
    best_sharpe = max(results[d]["stats"]["sharpe"] for d in DELTAS)
    best_sortino = max(results[d]["stats"]["sortino"] for d in DELTAS)
    best_calmar = max(results[d]["stats"]["calmar"] for d in DELTAS)

    for delta in DELTAS:
        s = results[delta]["stats"]
        marker = " <-- baseline" if delta == 0.80 else ""

        # Mark the best values
        sh_mark = " *" if abs(s["sharpe"] - best_sharpe) < 0.005 else ""
        so_mark = " *" if abs(s["sortino"] - best_sortino) < 0.005 else ""
        ca_mark = " *" if abs(s["calmar"] - best_calmar) < 0.005 else ""

        print(f"  {delta:>6.2f} ${s['end']:>12,.0f} {s['total_ret']:>+9.1%} "
              f"{s['cagr']:>+7.1%} {s['sharpe']:>8.2f}{sh_mark:<2} "
              f"{s['sortino']:>8.2f}{so_mark:<2} {s['max_dd']:>7.1%} "
              f"{s['calmar']:>8.2f}{ca_mark:<2} {s['vol']:>7.1%}{marker}")

    print(f"  {'-' * (W - 4)}")
    print(f"  (* = best in column)")

    # Trade summary
    print(f"\n  Trade Counts by Delta:")
    print(f"  {'Delta':>6} {'Total':>8} {'PT':>6} {'MH':>6} {'SMA':>6} "
          f"{'Win Rate':>9} {'Avg Days':>9}")
    print(f"  {'-' * 55}")
    for delta in DELTAS:
        tl = results[delta]["trade_log"]
        if not tl:
            continue
        tdf = pd.DataFrame(tl)
        wins = tdf[tdf["pnl_pct"] > 0]
        pt = len(tdf[tdf["exit_reason"] == "PT"])
        mh = len(tdf[tdf["exit_reason"] == "MH"])
        sma = len(tdf[tdf["exit_reason"] == "SMA"])
        wr = len(wins) / len(tdf) if len(tdf) > 0 else 0
        avg_d = tdf["days_held"].mean()
        print(f"  {delta:>6.2f} {len(tdf):>8} {pt:>6} {mh:>6} {sma:>6} "
              f"{wr:>8.0%} {avg_d:>9.0f}")

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

    W = 70
    print("=" * W)
    print("Delta Sensitivity Analysis")
    print(f"  Deltas: {DELTAS}")
    print(f"  Combined: $1M B&H + $1M options + BIL + CC")
    print("=" * W)

    # Fetch BIL returns once
    print("\nLoading BIL returns...")
    bil_rets = spy_mod.fetch_etf_returns("BIL", spy_mod.DATA_START, spy_mod.DATA_END)
    if not bil_rets:
        print("ERROR: Could not fetch BIL data.")
        return

    # Connect to ThetaData
    client = ThetaDataClient()
    if not client.connect():
        print("\nERROR: Cannot connect to Theta Terminal.")
        return
    print("\nConnected to Theta Terminal.")

    # SPY sensitivity
    spy_results = run_spy_sensitivity(client, bil_rets)
    print_comparison_table(spy_results, "SPY", "spy_bh")
    make_sensitivity_figure(
        spy_results, "SPY", "spy_bh",
        os.path.join(_this_dir, "spy_delta_sensitivity.png"),
    )

    # QQQ sensitivity
    qqq_results = run_qqq_sensitivity(client, bil_rets)
    print_comparison_table(qqq_results, "QQQ", "qqq_bh")
    make_sensitivity_figure(
        qqq_results, "QQQ", "qqq_bh",
        os.path.join(_this_dir, "qqq_delta_sensitivity.png"),
    )

    # Cross-ticker summary
    W2 = 110
    print(f"\n\n{'=' * W2}")
    print("CROSS-TICKER SUMMARY: BEST DELTA BY METRIC")
    print(f"{'=' * W2}")

    for ticker, res in [("SPY", spy_results), ("QQQ", qqq_results)]:
        print(f"\n  {ticker}:")
        best_sh_d = max(DELTAS, key=lambda d: res[d]["stats"]["sharpe"])
        best_so_d = max(DELTAS, key=lambda d: res[d]["stats"]["sortino"])
        best_ca_d = max(DELTAS, key=lambda d: res[d]["stats"]["calmar"])
        best_cagr_d = max(DELTAS, key=lambda d: res[d]["stats"]["cagr"])
        least_dd_d = max(DELTAS, key=lambda d: res[d]["stats"]["max_dd"])  # least negative

        print(f"    Best Sharpe:  delta={best_sh_d:.2f} ({res[best_sh_d]['stats']['sharpe']:.2f})")
        print(f"    Best Sortino: delta={best_so_d:.2f} ({res[best_so_d]['stats']['sortino']:.2f})")
        print(f"    Best Calmar:  delta={best_ca_d:.2f} ({res[best_ca_d]['stats']['calmar']:.2f})")
        print(f"    Best CAGR:    delta={best_cagr_d:.2f} ({res[best_cagr_d]['stats']['cagr']:+.1%})")
        print(f"    Least DD:     delta={least_dd_d:.2f} ({res[least_dd_d]['stats']['max_dd']:.1%})")

    print(f"\n{'=' * W2}")

    client.close()
    print("\nDone!")


if __name__ == "__main__":
    main()
