"""
Report Generator
=================
Console and CSV output for backtest results.
Mirrors the QC algorithm output format with additional breakdowns.

Reports generated:
    1. Results by strategy type (PUT vs CALL)
    2. Results by ticker (all 12)
    3. Top 10 combinations by total P&L
    4. Best combination per ticker
    5. Overall summary
    6. CSV export of all trades
"""

import csv
import json
import math
import logging
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from collections import defaultdict

from shadow_tracker import ShadowTracker, ShadowPosition

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------
# Metrics helpers (standalone, no dependency on backtest_report.py types)
# -----------------------------------------------------------------------

def _calculate_drawdown(pnls: List[float]) -> Tuple[float, float]:
    """Max drawdown from a series of P&Ls. Returns (dollars, pct)."""
    if not pnls:
        return 0.0, 0.0

    cumulative = []
    running = 0.0
    for p in pnls:
        running += p
        cumulative.append(running)

    peak = cumulative[0]
    max_dd = 0.0
    max_dd_pct = 0.0

    for val in cumulative:
        if val > peak:
            peak = val
        dd = peak - val
        if dd > max_dd:
            max_dd = dd
            if peak > 0:
                max_dd_pct = dd / peak

    return max_dd, max_dd_pct


def _calculate_sharpe(pnls: List[float]) -> Optional[float]:
    if len(pnls) < 10:
        return None
    mean = sum(pnls) / len(pnls)
    var = sum((p - mean) ** 2 for p in pnls) / (len(pnls) - 1)
    std = math.sqrt(var)
    if std == 0:
        return None
    return (mean / std) * math.sqrt(12)  # ~12 trades/year with 30 DTE


def _median(values: List[float]) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    n = len(s)
    if n % 2 == 0:
        return (s[n // 2 - 1] + s[n // 2]) / 2
    return s[n // 2]


# -----------------------------------------------------------------------
# Grouping functions
# -----------------------------------------------------------------------

def _group_positions(
    positions: List[ShadowPosition],
    key_func,
) -> Dict[str, List[ShadowPosition]]:
    groups: Dict[str, List[ShadowPosition]] = defaultdict(list)
    for p in positions:
        k = key_func(p)
        groups[k].append(p)
    return dict(groups)


def _combo_key(p: ShadowPosition) -> str:
    """Key that identifies a unique parameter combination."""
    return (
        f"{p.strategy_type}_{int(p.width_pct*100)}pct_"
        f"TP{int(p.tp_pct*100)}_SL{int(p.sl_mult*100)}"
    )


def _metrics_for_group(positions: List[ShadowPosition]) -> Dict:
    """Compute summary metrics for a group of closed positions."""
    pnls = [p.pnl for p in positions if p.pnl is not None]
    if not pnls:
        return {
            "count": 0, "wins": 0, "losses": 0, "win_rate": 0,
            "total_pnl": 0, "avg_pnl": 0, "median_pnl": 0,
            "avg_win": 0, "avg_loss": 0, "max_drawdown": 0,
            "sharpe": None,
        }

    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]

    return {
        "count": len(pnls),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": len(wins) / len(pnls),
        "total_pnl": sum(pnls),
        "avg_pnl": sum(pnls) / len(pnls),
        "median_pnl": _median(pnls),
        "avg_win": sum(wins) / len(wins) if wins else 0,
        "avg_loss": sum(losses) / len(losses) if losses else 0,
        "max_drawdown": _calculate_drawdown(pnls)[0],
        "sharpe": _calculate_sharpe(pnls),
    }


def _exit_reason_counts(positions: List[ShadowPosition]) -> Dict[str, int]:
    counts: Dict[str, int] = defaultdict(int)
    for p in positions:
        if p.exit_reason:
            counts[p.exit_reason] += 1
    return dict(counts)


# -----------------------------------------------------------------------
# Console report
# -----------------------------------------------------------------------

def print_report(tracker: ShadowTracker) -> None:
    """Print full backtest report to console."""
    closed = tracker.closed_positions
    if not closed:
        print("\nNo closed positions to report.")
        return

    print("\n" + "=" * 80)
    print("MASSIVE API LOCAL BACKTESTER — RESULTS")
    print("=" * 80)
    print(f"Generated: {datetime.utcnow().isoformat()}Z")
    print()

    # --- Overall summary ---
    overall = _metrics_for_group(closed)
    exit_reasons = _exit_reason_counts(closed)

    print("-" * 60)
    print("OVERALL SUMMARY")
    print("-" * 60)
    print(f"  Total trades:      {overall['count']}")
    print(f"  Wins / Losses:     {overall['wins']} / {overall['losses']}")
    print(f"  Win Rate:          {overall['win_rate']:.1%}")
    print(f"  Total P&L:         ${overall['total_pnl']:,.2f}")
    print(f"  Avg P&L/trade:     ${overall['avg_pnl']:,.2f}")
    print(f"  Median P&L:        ${overall['median_pnl']:,.2f}")
    print(f"  Avg Win:           ${overall['avg_win']:,.2f}")
    print(f"  Avg Loss:          ${overall['avg_loss']:,.2f}")
    print(f"  Max Drawdown:      ${overall['max_drawdown']:,.2f}")
    if overall['sharpe'] is not None:
        print(f"  Sharpe Ratio:      {overall['sharpe']:.2f}")
    print(f"\n  Exit Reasons:")
    for reason, count in sorted(exit_reasons.items()):
        print(f"    {reason:20s}: {count:5d}")

    # --- By strategy type ---
    print()
    print("-" * 60)
    print("RESULTS BY STRATEGY TYPE")
    print("-" * 60)
    by_strat = _group_positions(closed, lambda p: p.strategy_type)
    print(f"  {'Strategy':<10} {'Count':>7} {'Win%':>7} {'Total P&L':>12} "
          f"{'Avg P&L':>10} {'Avg Win':>10} {'Avg Loss':>10}")
    print(f"  {'-'*66}")
    for strat in ["PUT", "CALL"]:
        if strat in by_strat:
            m = _metrics_for_group(by_strat[strat])
            print(f"  {strat:<10} {m['count']:>7} {m['win_rate']:>6.1%} "
                  f"${m['total_pnl']:>11,.2f} ${m['avg_pnl']:>9,.2f} "
                  f"${m['avg_win']:>9,.2f} ${m['avg_loss']:>9,.2f}")

    # --- By ticker ---
    print()
    print("-" * 60)
    print("RESULTS BY TICKER")
    print("-" * 60)
    by_ticker = _group_positions(closed, lambda p: p.underlying)
    print(f"  {'Ticker':<8} {'Count':>7} {'Win%':>7} {'Total P&L':>12} "
          f"{'Avg P&L':>10} {'Max DD':>10}")
    print(f"  {'-'*56}")
    for ticker in sorted(by_ticker.keys()):
        m = _metrics_for_group(by_ticker[ticker])
        print(f"  {ticker:<8} {m['count']:>7} {m['win_rate']:>6.1%} "
              f"${m['total_pnl']:>11,.2f} ${m['avg_pnl']:>9,.2f} "
              f"${m['max_drawdown']:>9,.2f}")

    # --- Top 10 combinations by total P&L ---
    print()
    print("-" * 60)
    print("TOP 10 COMBINATIONS BY TOTAL P&L")
    print("-" * 60)
    by_combo = _group_positions(closed, _combo_key)
    combo_metrics = []
    for combo_name, positions in by_combo.items():
        m = _metrics_for_group(positions)
        m["combo"] = combo_name
        combo_metrics.append(m)

    combo_metrics.sort(key=lambda x: x["total_pnl"], reverse=True)

    print(f"  {'Combination':<30} {'Count':>7} {'Win%':>7} "
          f"{'Total P&L':>12} {'Avg P&L':>10}")
    print(f"  {'-'*68}")
    for m in combo_metrics[:10]:
        print(f"  {m['combo']:<30} {m['count']:>7} {m['win_rate']:>6.1%} "
              f"${m['total_pnl']:>11,.2f} ${m['avg_pnl']:>9,.2f}")

    # --- Best combination per ticker ---
    print()
    print("-" * 60)
    print("BEST COMBINATION PER TICKER")
    print("-" * 60)
    print(f"  {'Ticker':<8} {'Best Combo':<30} {'Count':>6} "
          f"{'Win%':>6} {'Total P&L':>12}")
    print(f"  {'-'*64}")
    for ticker in sorted(by_ticker.keys()):
        ticker_positions = by_ticker[ticker]
        ticker_combos = _group_positions(ticker_positions, _combo_key)
        best_combo = None
        best_pnl = float("-inf")
        for combo_name, combo_positions in ticker_combos.items():
            total = sum(p.pnl for p in combo_positions if p.pnl)
            if total > best_pnl:
                best_pnl = total
                best_combo = combo_name
        if best_combo:
            m = _metrics_for_group(ticker_combos[best_combo])
            print(f"  {ticker:<8} {best_combo:<30} {m['count']:>6} "
                  f"{m['win_rate']:>5.1%} ${m['total_pnl']:>11,.2f}")

    # --- Pricing regime analysis ---
    print()
    print("-" * 60)
    print("PRICING REGIME ANALYSIS")
    print("-" * 60)
    real_quote = [p for p in closed if p.has_real_quote]
    synthetic = [p for p in closed if not p.has_real_quote]
    if real_quote:
        m = _metrics_for_group(real_quote)
        print(f"  Real bid/ask (2022+):  {m['count']:>6} trades, "
              f"${m['total_pnl']:>10,.2f} P&L, {m['win_rate']:.1%} win rate")
    if synthetic:
        m = _metrics_for_group(synthetic)
        print(f"  Synthetic (pre-2022):  {m['count']:>6} trades, "
              f"${m['total_pnl']:>10,.2f} P&L, {m['win_rate']:.1%} win rate")

    print()
    print("=" * 80)


# -----------------------------------------------------------------------
# CSV export
# -----------------------------------------------------------------------

def export_trades_csv(tracker: ShadowTracker, output_path: str) -> None:
    """Export all closed trades to CSV."""
    closed = tracker.closed_positions
    if not closed:
        logger.warning("No trades to export.")
        return

    fieldnames = [
        "combo_id", "underlying", "strategy_type", "width_pct",
        "tp_pct", "sl_mult",
        "entry_date", "expiration_date", "exit_date", "dte",
        "short_strike", "long_strike", "spot_at_entry",
        "credit", "max_loss", "exit_cost", "pnl",
        "exit_reason", "iv_at_entry", "iv_rank_at_entry",
        "has_real_quote", "short_ticker", "long_ticker",
    ]

    rows = []
    for p in closed:
        rows.append({
            "combo_id": p.combo_id,
            "underlying": p.underlying,
            "strategy_type": p.strategy_type,
            "width_pct": p.width_pct,
            "tp_pct": p.tp_pct,
            "sl_mult": p.sl_mult,
            "entry_date": p.entry_date,
            "expiration_date": p.expiration_date,
            "exit_date": p.exit_date,
            "dte": p.dte,
            "short_strike": p.short_strike,
            "long_strike": p.long_strike,
            "spot_at_entry": p.spot_at_entry,
            "credit": p.credit,
            "max_loss": p.max_loss,
            "exit_cost": p.exit_cost,
            "pnl": p.pnl,
            "exit_reason": p.exit_reason,
            "iv_at_entry": p.iv_at_entry,
            "iv_rank_at_entry": p.iv_rank_at_entry,
            "has_real_quote": p.has_real_quote,
            "short_ticker": p.short_ticker,
            "long_ticker": p.long_ticker,
        })

    rows.sort(key=lambda r: (r["entry_date"], r["combo_id"]))

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nExported {len(rows)} trades to {output_path}")


# -----------------------------------------------------------------------
# JSON summary export
# -----------------------------------------------------------------------

def export_summary_json(tracker: ShadowTracker, output_path: str) -> None:
    """Export summary statistics to JSON."""
    closed = tracker.closed_positions
    overall = _metrics_for_group(closed)

    by_strat = _group_positions(closed, lambda p: p.strategy_type)
    by_ticker = _group_positions(closed, lambda p: p.underlying)
    by_combo = _group_positions(closed, _combo_key)

    output = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "overall": overall,
        "by_strategy": {
            k: _metrics_for_group(v) for k, v in by_strat.items()
        },
        "by_ticker": {
            k: _metrics_for_group(v) for k, v in by_ticker.items()
        },
        "by_combination": {
            k: _metrics_for_group(v) for k, v in by_combo.items()
        },
        "exit_reasons": _exit_reason_counts(closed),
    }

    # Sharpe can't be serialised if None — already handled
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2, default=str)

    print(f"Exported summary to {output_path}")
