"""
Strategy Performance Charts
============================
Generates two figures comparing 80-delta call strategies vs SPY B&H:
  1. SPY strategy figure
  2. QQQ strategy figure

Each figure has 3 stacked subplots:
  - Portfolio value ($1M start): Config A, Config B, SPY B&H
  - Drawdown curves
  - Underlying price vs its own SMA200 (shaded when below)

Requires Theta Terminal running (data is cached after first run).

Usage:
    python strategy_charts.py
"""

import os
import sys
import logging
from datetime import datetime

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

import spy_deployment_sim as spy_sim
import qqq_deployment_sim as qqq_sim

DISPLAY_CAPITAL = 1_000_000
SCALE = DISPLAY_CAPITAL / 100_000  # sims run at $100K

# Colors
C_A = "#1f77b4"       # blue  -- Config A
C_B = "#d62728"       # red   -- Config B
C_SPY_BH = "#7f7f7f"  # gray  -- SPY B&H
C_QQQ_BH = "#2ca02c"  # green -- QQQ B&H
C_SMA = "#ff7f0e"     # orange -- SMA200 line
C_BELOW = "#d62728"   # red   -- below-SMA shading


def _cagr(start, end, years):
    if years <= 0 or start <= 0:
        return 0
    return (end / start) ** (1 / years) - 1


def _fmt_dollars(x, _):
    if x >= 1_000_000:
        return f"${x / 1_000_000:.1f}M"
    if x >= 1_000:
        return f"${x / 1_000:.0f}K"
    return f"${x:.0f}"


def make_figure(snaps_a, snaps_b, ticker_by_date, sma200,
                spy_by_date, ticker, filename, qqq_by_date=None):
    """
    Build a 3-subplot figure for one ticker's strategy results.

    Parameters
    ----------
    snaps_a, snaps_b : list[dict]   daily snapshots from run_simulation
    ticker_by_date   : dict         primary underlying bar data (keyed by date str)
    sma200           : dict         SMA200 values (keyed by date str)
    spy_by_date      : dict         SPY bar data (for B&H benchmark)
    ticker           : str          "SPY" or "QQQ"
    filename         : str          output PNG path
    qqq_by_date      : dict|None    if plotting QQQ figure, pass QQQ bars for QQQ B&H line
    """

    df_a = pd.DataFrame(snaps_a)
    df_b = pd.DataFrame(snaps_b)

    dates_str = df_a["date"].tolist()
    dates = pd.to_datetime(df_a["date"])
    n_days = len(dates)
    years = n_days / 252.0

    # --- Portfolio values scaled to $1M ---
    pv_a = df_a["portfolio_value"].values * SCALE
    pv_b = df_b["portfolio_value"].values * SCALE

    # --- SPY B&H equity curve ---
    spy_prices = np.array([spy_by_date.get(d, {}).get("close", np.nan)
                           for d in dates_str], dtype=float)
    # forward-fill any NaNs
    mask = np.isnan(spy_prices)
    if mask.any():
        idx = np.where(~mask, np.arange(len(spy_prices)), 0)
        np.maximum.accumulate(idx, out=idx)
        spy_prices = spy_prices[idx]
    spy_bh = DISPLAY_CAPITAL * spy_prices / spy_prices[0]

    # --- QQQ B&H (only on QQQ figure) ---
    qqq_bh = None
    if qqq_by_date is not None:
        qqq_prices = np.array([qqq_by_date.get(d, {}).get("close", np.nan)
                               for d in dates_str], dtype=float)
        mask = np.isnan(qqq_prices)
        if mask.any():
            idx = np.where(~mask, np.arange(len(qqq_prices)), 0)
            np.maximum.accumulate(idx, out=idx)
            qqq_prices = qqq_prices[idx]
        qqq_bh = DISPLAY_CAPITAL * qqq_prices / qqq_prices[0]

    # --- Drawdowns ---
    dd_a = pv_a / np.maximum.accumulate(pv_a) - 1
    dd_b = pv_b / np.maximum.accumulate(pv_b) - 1
    dd_spy = spy_bh / np.maximum.accumulate(spy_bh) - 1
    dd_qqq = None
    if qqq_bh is not None:
        dd_qqq = qqq_bh / np.maximum.accumulate(qqq_bh) - 1

    # --- Underlying price + SMA ---
    under_prices = np.array([ticker_by_date[d]["close"] for d in dates_str])
    sma_vals = np.array([sma200.get(d, np.nan) for d in dates_str])

    # --- Legend labels with stats ---
    cagr_a = _cagr(DISPLAY_CAPITAL, pv_a[-1], years)
    cagr_b = _cagr(DISPLAY_CAPITAL, pv_b[-1], years)
    cagr_spy = _cagr(DISPLAY_CAPITAL, spy_bh[-1], years)

    lbl_a = f"Config A: Entry-only  (${pv_a[-1]/1e6:.1f}M, {cagr_a:+.1%} CAGR)"
    lbl_b = f"Config B: Thresh-exit (${pv_b[-1]/1e6:.1f}M, {cagr_b:+.1%} CAGR)"
    lbl_spy = f"SPY Buy & Hold (${spy_bh[-1]/1e6:.1f}M, {cagr_spy:+.1%} CAGR)"
    lbl_qqq = ""
    if qqq_bh is not None:
        cagr_qqq = _cagr(DISPLAY_CAPITAL, qqq_bh[-1], years)
        lbl_qqq = f"QQQ Buy & Hold (${qqq_bh[-1]/1e6:.1f}M, {cagr_qqq:+.1%} CAGR)"

    # ======================= FIGURE =======================
    fig, (ax1, ax2, ax3) = plt.subplots(
        3, 1, figsize=(16, 13),
        gridspec_kw={"height_ratios": [3, 1.5, 1.5], "hspace": 0.22},
    )

    fig.suptitle(
        f"{ticker} 80-Delta Call Strategy vs SPY Buy & Hold  "
        f"({dates_str[0][:4]}\u2013{dates_str[-1][:4]},  $1M start)",
        fontsize=15, fontweight="bold", y=0.98,
    )

    dt_vals = dates.values

    # ---------- Subplot 1: Portfolio Value ----------
    ax1.plot(dt_vals, pv_a, color=C_A, linewidth=1.6, label=lbl_a)
    ax1.plot(dt_vals, pv_b, color=C_B, linewidth=1.6, label=lbl_b)
    ax1.plot(dt_vals, spy_bh, color=C_SPY_BH, linewidth=1.4,
             linestyle="--", label=lbl_spy)
    if qqq_bh is not None:
        ax1.plot(dt_vals, qqq_bh, color=C_QQQ_BH, linewidth=1.4,
                 linestyle="--", label=lbl_qqq)

    ax1.set_ylabel("Portfolio Value", fontsize=11)
    ax1.yaxis.set_major_formatter(mticker.FuncFormatter(_fmt_dollars))
    ax1.legend(loc="upper left", fontsize=9, framealpha=0.9)
    ax1.grid(True, alpha=0.25)
    ax1.set_xlim(dt_vals[0], dt_vals[-1])

    # ---------- Subplot 2: Drawdown ----------
    ax2.fill_between(dt_vals, dd_a, 0, color=C_A, alpha=0.25, label="Config A")
    ax2.fill_between(dt_vals, dd_b, 0, color=C_B, alpha=0.20, label="Config B")
    ax2.plot(dt_vals, dd_spy, color=C_SPY_BH, linewidth=1.0,
             linestyle="--", label="SPY B&H")
    if dd_qqq is not None:
        ax2.plot(dt_vals, dd_qqq, color=C_QQQ_BH, linewidth=1.0,
                 linestyle="--", label="QQQ B&H")

    ax2.set_ylabel("Drawdown", fontsize=11)
    ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0%}"))
    ax2.legend(loc="lower left", fontsize=8, ncol=4, framealpha=0.9)
    ax2.grid(True, alpha=0.25)
    ax2.set_xlim(dt_vals[0], dt_vals[-1])

    # ---------- Subplot 3: Underlying Price vs SMA200 ----------
    ax3.plot(dt_vals, under_prices, color=C_A, linewidth=1.2, label=f"{ticker} Close")
    ax3.plot(dt_vals, sma_vals, color=C_SMA, linewidth=1.2,
             linestyle="--", label=f"{ticker} SMA200")
    below = under_prices < sma_vals
    ax3.fill_between(dt_vals, under_prices, sma_vals,
                     where=below, alpha=0.20, color=C_BELOW,
                     label="Below SMA200 (no entries)")
    ax3.set_ylabel(f"{ticker} Price", fontsize=11)
    ax3.set_xlabel("Date", fontsize=11)
    ax3.legend(loc="upper left", fontsize=9, framealpha=0.9)
    ax3.grid(True, alpha=0.25)
    ax3.set_xlim(dt_vals[0], dt_vals[-1])

    # Format all x-axes
    for ax in [ax1, ax2, ax3]:
        ax.xaxis.set_major_locator(mdates.YearLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
        ax.tick_params(axis="x", labelsize=9)

    plt.savefig(filename, dpi=150, bbox_inches="tight")
    print(f"  Saved: {filename}")
    plt.close(fig)


# ======================================================================
# MAIN
# ======================================================================

def main():
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    print("=" * 70)
    print("Strategy Performance Charts  ($1M start)")
    print("=" * 70)

    client = ThetaDataClient()
    if not client.connect():
        print("\nERROR: Cannot connect to Theta Terminal.")
        return

    print("Connected to Theta Terminal.\n")

    # ------------------------------------------------------------------
    # SPY simulations
    # ------------------------------------------------------------------
    print("-" * 50)
    print("Running SPY simulations...")
    print("-" * 50)
    spy_data = spy_sim.load_all_data(client)
    spy_by_date, spy_dates, spy_vix, spy_sma200, spy_exps = spy_data

    spy_snaps_a, _ = spy_sim.run_simulation(
        client, spy_by_date, spy_dates, spy_vix, spy_sma200, spy_exps,
        force_exit_below_sma=False, label="A: Entry-only SMA filter",
    )
    spy_snaps_b, _ = spy_sim.run_simulation(
        client, spy_by_date, spy_dates, spy_vix, spy_sma200, spy_exps,
        force_exit_below_sma=True, label="B: Thresh-exit (>2% below SMA)",
    )

    # ------------------------------------------------------------------
    # QQQ simulations
    # ------------------------------------------------------------------
    print("\n" + "-" * 50)
    print("Running QQQ simulations...")
    print("-" * 50)
    qqq_data = qqq_sim.load_all_data(client)
    qqq_by_date, qqq_spy_by_date, qqq_dates, qqq_vix, qqq_sma200, qqq_exps = qqq_data

    qqq_snaps_a, _ = qqq_sim.run_simulation(
        client, qqq_by_date, qqq_dates, qqq_vix, qqq_sma200, qqq_exps,
        force_exit_below_sma=False, label="A: Entry-only SMA filter",
    )
    qqq_snaps_b, _ = qqq_sim.run_simulation(
        client, qqq_by_date, qqq_dates, qqq_vix, qqq_sma200, qqq_exps,
        force_exit_below_sma=True, label="B: Thresh-exit (>2% below SMA)",
    )

    # ------------------------------------------------------------------
    # Generate charts
    # ------------------------------------------------------------------
    print("\n" + "-" * 50)
    print("Generating charts...")
    print("-" * 50)

    out_dir = _this_dir

    make_figure(
        spy_snaps_a, spy_snaps_b,
        ticker_by_date=spy_by_date,
        sma200=spy_sma200,
        spy_by_date=spy_by_date,
        ticker="SPY",
        filename=os.path.join(out_dir, "spy_strategy_vs_bh.png"),
    )

    make_figure(
        qqq_snaps_a, qqq_snaps_b,
        ticker_by_date=qqq_by_date,
        sma200=qqq_sma200,
        spy_by_date=qqq_spy_by_date,
        ticker="QQQ",
        filename=os.path.join(out_dir, "qqq_strategy_vs_bh.png"),
        qqq_by_date=qqq_by_date,
    )

    client.close()
    print("\nDone!")


if __name__ == "__main__":
    main()
