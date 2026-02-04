"""
Backtest Reporting Module
=========================
Generates comprehensive reports comparing put credit spread
and iron condor backtest results.

Metrics:
- Win rate, P&L, expectancy
- Max drawdown, Sharpe ratio
- POP calibration (theoretical vs realized)
- Performance by IV bucket/tier
"""

from __future__ import annotations

import json
import csv
import math
import logging
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional, List, Dict, Any
from pathlib import Path

from .put_spread_backtest import PutSpreadBacktestResult, PutSpreadTrade
from .condor_backtest import CondorBacktestResult, CondorTrade

logger = logging.getLogger(__name__)


# =============================================================================
# DATA STRUCTURES
# =============================================================================

@dataclass
class StrategyMetrics:
    """Calculated metrics for a strategy across all symbols."""
    strategy_name: str

    # Trade counts
    total_symbols: int
    symbols_with_trades: int
    total_trades: int
    winning_trades: int
    losing_trades: int

    # P&L
    total_pnl: float
    avg_pnl_per_trade: float
    median_pnl: float
    std_pnl: float

    # Rates
    win_rate: float

    # Expectancy
    avg_win: float
    avg_loss: float
    expectancy: float  # (win% * avg_win) - (loss% * avg_loss)
    profit_factor: float  # total_wins / total_losses

    # Risk metrics
    max_drawdown: float
    max_drawdown_pct: float
    sharpe_ratio: Optional[float]

    # POP calibration
    avg_theoretical_pop: float
    realized_pop: float
    pop_difference: float  # realized - theoretical

    # Additional details
    details: Dict[str, Any]


@dataclass
class ComparisonReport:
    """Comparison report between put spreads and condors."""
    generated_at: str

    put_spread_metrics: Optional[StrategyMetrics]
    condor_metrics: Optional[StrategyMetrics]

    # Head-to-head
    better_strategy: str
    better_by_pnl: float
    better_by_winrate: float

    summary: str


# =============================================================================
# METRICS CALCULATION
# =============================================================================

def calculate_drawdown(pnls: List[float]) -> tuple:
    """
    Calculate max drawdown from a series of trade P&Ls.

    Returns:
        Tuple of (max_drawdown_dollars, max_drawdown_pct)
    """
    if not pnls:
        return 0.0, 0.0

    cumulative = []
    running_sum = 0.0
    for pnl in pnls:
        running_sum += pnl
        cumulative.append(running_sum)

    peak = cumulative[0]
    max_dd = 0.0
    max_dd_pct = 0.0

    for value in cumulative:
        if value > peak:
            peak = value
        dd = peak - value
        if dd > max_dd:
            max_dd = dd
            if peak > 0:
                max_dd_pct = dd / peak

    return max_dd, max_dd_pct


def calculate_sharpe(pnls: List[float], risk_free_rate: float = 0.0) -> Optional[float]:
    """
    Calculate Sharpe ratio from trade P&Ls.

    Assumes each trade is ~30 days, so annualization factor is sqrt(12).
    """
    if len(pnls) < 10:
        return None

    mean_pnl = sum(pnls) / len(pnls)

    if len(pnls) < 2:
        return None

    variance = sum((p - mean_pnl) ** 2 for p in pnls) / (len(pnls) - 1)
    std_pnl = math.sqrt(variance)

    if std_pnl == 0:
        return None

    # Annualize (assuming ~12 trades per year with 30 DTE)
    sharpe = (mean_pnl - risk_free_rate) / std_pnl * math.sqrt(12)

    return sharpe


def calculate_median(values: List[float]) -> float:
    """Calculate median of a list."""
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    if n % 2 == 0:
        return (sorted_vals[n//2 - 1] + sorted_vals[n//2]) / 2
    return sorted_vals[n//2]


def calculate_put_spread_metrics(
    results: Dict[str, PutSpreadBacktestResult]
) -> Optional[StrategyMetrics]:
    """Calculate aggregate metrics for put spread backtest results."""

    if not results:
        return None

    # Collect all trades
    all_trades: List[PutSpreadTrade] = []
    for result in results.values():
        all_trades.extend(result.trades)

    if not all_trades:
        return StrategyMetrics(
            strategy_name="Put Credit Spread",
            total_symbols=len(results),
            symbols_with_trades=0,
            total_trades=0,
            winning_trades=0,
            losing_trades=0,
            total_pnl=0.0,
            avg_pnl_per_trade=0.0,
            median_pnl=0.0,
            std_pnl=0.0,
            win_rate=0.0,
            avg_win=0.0,
            avg_loss=0.0,
            expectancy=0.0,
            profit_factor=0.0,
            max_drawdown=0.0,
            max_drawdown_pct=0.0,
            sharpe_ratio=None,
            avg_theoretical_pop=0.0,
            realized_pop=0.0,
            pop_difference=0.0,
            details={},
        )

    # Sort by date for drawdown calculation
    all_trades.sort(key=lambda t: t.entry_date)

    pnls = [t.pnl for t in all_trades]
    winning = [t for t in all_trades if t.won]
    losing = [t for t in all_trades if not t.won]

    total_pnl = sum(pnls)
    avg_pnl = total_pnl / len(pnls)
    median_pnl = calculate_median(pnls)

    # Standard deviation
    variance = sum((p - avg_pnl) ** 2 for p in pnls) / max(1, len(pnls) - 1)
    std_pnl = math.sqrt(variance)

    win_rate = len(winning) / len(all_trades) if all_trades else 0

    avg_win = sum(t.pnl for t in winning) / len(winning) if winning else 0
    avg_loss = sum(t.pnl for t in losing) / len(losing) if losing else 0

    # Expectancy
    loss_rate = 1 - win_rate
    expectancy = (win_rate * avg_win) + (loss_rate * avg_loss)

    # Profit factor
    total_wins = sum(t.pnl for t in winning)
    total_losses = abs(sum(t.pnl for t in losing))
    profit_factor = total_wins / total_losses if total_losses > 0 else float('inf')

    # Drawdown
    max_dd, max_dd_pct = calculate_drawdown(pnls)

    # Sharpe
    sharpe = calculate_sharpe(pnls)

    # POP calibration
    avg_theoretical_pop = sum(t.theoretical_pop for t in all_trades) / len(all_trades)
    realized_pop = win_rate

    # IV bucket analysis
    iv_bucket_stats = {}
    for bucket in ['low', 'medium', 'high']:
        bucket_trades = [t for t in all_trades
                        if (bucket == 'low' and t.iv_rank < 0.4) or
                           (bucket == 'medium' and 0.4 <= t.iv_rank < 0.6) or
                           (bucket == 'high' and t.iv_rank >= 0.6)]
        if bucket_trades:
            iv_bucket_stats[bucket] = {
                'count': len(bucket_trades),
                'pnl': sum(t.pnl for t in bucket_trades),
                'win_rate': sum(1 for t in bucket_trades if t.won) / len(bucket_trades),
            }

    return StrategyMetrics(
        strategy_name="Put Credit Spread",
        total_symbols=len(results),
        symbols_with_trades=sum(1 for r in results.values() if r.total_trades > 0),
        total_trades=len(all_trades),
        winning_trades=len(winning),
        losing_trades=len(losing),
        total_pnl=total_pnl,
        avg_pnl_per_trade=avg_pnl,
        median_pnl=median_pnl,
        std_pnl=std_pnl,
        win_rate=win_rate,
        avg_win=avg_win,
        avg_loss=avg_loss,
        expectancy=expectancy,
        profit_factor=profit_factor,
        max_drawdown=max_dd,
        max_drawdown_pct=max_dd_pct,
        sharpe_ratio=sharpe,
        avg_theoretical_pop=avg_theoretical_pop,
        realized_pop=realized_pop,
        pop_difference=realized_pop - avg_theoretical_pop,
        details={'iv_bucket_stats': iv_bucket_stats},
    )


def calculate_condor_metrics(
    results: Dict[str, CondorBacktestResult]
) -> Optional[StrategyMetrics]:
    """Calculate aggregate metrics for iron condor backtest results."""

    if not results:
        return None

    all_trades: List[CondorTrade] = []
    for result in results.values():
        all_trades.extend(result.trades)

    if not all_trades:
        return StrategyMetrics(
            strategy_name="Iron Condor",
            total_symbols=len(results),
            symbols_with_trades=0,
            total_trades=0,
            winning_trades=0,
            losing_trades=0,
            total_pnl=0.0,
            avg_pnl_per_trade=0.0,
            median_pnl=0.0,
            std_pnl=0.0,
            win_rate=0.0,
            avg_win=0.0,
            avg_loss=0.0,
            expectancy=0.0,
            profit_factor=0.0,
            max_drawdown=0.0,
            max_drawdown_pct=0.0,
            sharpe_ratio=None,
            avg_theoretical_pop=0.0,
            realized_pop=0.0,
            pop_difference=0.0,
            details={},
        )

    all_trades.sort(key=lambda t: t.entry_date)

    pnls = [t.pnl for t in all_trades]
    winning = [t for t in all_trades if t.won]
    losing = [t for t in all_trades if not t.won]

    total_pnl = sum(pnls)
    avg_pnl = total_pnl / len(pnls)
    median_pnl = calculate_median(pnls)

    variance = sum((p - avg_pnl) ** 2 for p in pnls) / max(1, len(pnls) - 1)
    std_pnl = math.sqrt(variance)

    win_rate = len(winning) / len(all_trades) if all_trades else 0

    avg_win = sum(t.pnl for t in winning) / len(winning) if winning else 0
    avg_loss = sum(t.pnl for t in losing) / len(losing) if losing else 0

    loss_rate = 1 - win_rate
    expectancy = (win_rate * avg_win) + (loss_rate * avg_loss)

    total_wins = sum(t.pnl for t in winning)
    total_losses = abs(sum(t.pnl for t in losing))
    profit_factor = total_wins / total_losses if total_losses > 0 else float('inf')

    max_dd, max_dd_pct = calculate_drawdown(pnls)
    sharpe = calculate_sharpe(pnls)

    avg_theoretical_pop = sum(t.theoretical_pop for t in all_trades) / len(all_trades)
    realized_pop = win_rate

    # Breach analysis
    put_breaches = sum(1 for t in all_trades if t.side_breached == 'put')
    call_breaches = sum(1 for t in all_trades if t.side_breached == 'call')

    # IV tier analysis
    iv_tier_stats = {}
    for tier in ['medium', 'high', 'very_high']:
        tier_trades = [t for t in all_trades if t.iv_tier == tier]
        if tier_trades:
            iv_tier_stats[tier] = {
                'count': len(tier_trades),
                'pnl': sum(t.pnl for t in tier_trades),
                'win_rate': sum(1 for t in tier_trades if t.won) / len(tier_trades),
            }

    return StrategyMetrics(
        strategy_name="Iron Condor",
        total_symbols=len(results),
        symbols_with_trades=sum(1 for r in results.values() if r.total_trades > 0),
        total_trades=len(all_trades),
        winning_trades=len(winning),
        losing_trades=len(losing),
        total_pnl=total_pnl,
        avg_pnl_per_trade=avg_pnl,
        median_pnl=median_pnl,
        std_pnl=std_pnl,
        win_rate=win_rate,
        avg_win=avg_win,
        avg_loss=avg_loss,
        expectancy=expectancy,
        profit_factor=profit_factor,
        max_drawdown=max_dd,
        max_drawdown_pct=max_dd_pct,
        sharpe_ratio=sharpe,
        avg_theoretical_pop=avg_theoretical_pop,
        realized_pop=realized_pop,
        pop_difference=realized_pop - avg_theoretical_pop,
        details={
            'put_breaches': put_breaches,
            'call_breaches': call_breaches,
            'breach_ratio': put_breaches / call_breaches if call_breaches > 0 else float('inf'),
            'iv_tier_stats': iv_tier_stats,
        },
    )


# =============================================================================
# REPORT GENERATION
# =============================================================================

def generate_comparison_report(
    put_spread_results: Optional[Dict[str, PutSpreadBacktestResult]],
    condor_results: Optional[Dict[str, CondorBacktestResult]],
) -> ComparisonReport:
    """Generate comparison report between strategies."""

    ps_metrics = calculate_put_spread_metrics(put_spread_results) if put_spread_results else None
    ic_metrics = calculate_condor_metrics(condor_results) if condor_results else None

    # Determine better strategy
    better = "Neither"
    pnl_diff = 0.0
    winrate_diff = 0.0

    if ps_metrics and ic_metrics:
        if ps_metrics.total_pnl > ic_metrics.total_pnl:
            better = "Put Credit Spread"
            pnl_diff = ps_metrics.total_pnl - ic_metrics.total_pnl
        elif ic_metrics.total_pnl > ps_metrics.total_pnl:
            better = "Iron Condor"
            pnl_diff = ic_metrics.total_pnl - ps_metrics.total_pnl

        winrate_diff = abs(ps_metrics.win_rate - ic_metrics.win_rate)
    elif ps_metrics:
        better = "Put Credit Spread"
        pnl_diff = ps_metrics.total_pnl
    elif ic_metrics:
        better = "Iron Condor"
        pnl_diff = ic_metrics.total_pnl

    # Generate summary
    summary_parts = []

    if ps_metrics and ps_metrics.total_trades > 0:
        summary_parts.append(
            f"Put Spreads: {ps_metrics.total_trades} trades, "
            f"{ps_metrics.win_rate:.1%} win rate, "
            f"${ps_metrics.total_pnl:,.0f} total P&L, "
            f"${ps_metrics.avg_pnl_per_trade:.0f}/trade"
        )

    if ic_metrics and ic_metrics.total_trades > 0:
        summary_parts.append(
            f"Iron Condors: {ic_metrics.total_trades} trades, "
            f"{ic_metrics.win_rate:.1%} win rate, "
            f"${ic_metrics.total_pnl:,.0f} total P&L, "
            f"${ic_metrics.avg_pnl_per_trade:.0f}/trade"
        )

    if better != "Neither":
        summary_parts.append(f"Better strategy: {better} by ${pnl_diff:,.0f}")

    return ComparisonReport(
        generated_at=datetime.utcnow().isoformat() + 'Z',
        put_spread_metrics=ps_metrics,
        condor_metrics=ic_metrics,
        better_strategy=better,
        better_by_pnl=pnl_diff,
        better_by_winrate=winrate_diff,
        summary="\n".join(summary_parts),
    )


def print_console_report(report: ComparisonReport):
    """Print formatted report to console."""

    print("\n" + "=" * 80)
    print("OPTIONS BACKTESTING REPORT")
    print("=" * 80)
    print(f"Generated: {report.generated_at}")
    print()

    # Put Spread Section
    if report.put_spread_metrics:
        m = report.put_spread_metrics
        print("-" * 40)
        print("PUT CREDIT SPREADS")
        print("-" * 40)
        print(f"  Symbols tested:    {m.total_symbols}")
        print(f"  Symbols w/ trades: {m.symbols_with_trades}")
        print(f"  Total trades:      {m.total_trades}")
        print()
        print(f"  Win Rate:          {m.win_rate:.1%}")
        print(f"  Total P&L:         ${m.total_pnl:,.2f}")
        print(f"  Avg P&L/trade:     ${m.avg_pnl_per_trade:.2f}")
        print(f"  Median P&L:        ${m.median_pnl:.2f}")
        print(f"  Std Dev:           ${m.std_pnl:.2f}")
        print()
        print(f"  Avg Win:           ${m.avg_win:.2f}")
        print(f"  Avg Loss:          ${m.avg_loss:.2f}")
        print(f"  Expectancy:        ${m.expectancy:.2f}")
        print(f"  Profit Factor:     {m.profit_factor:.2f}")
        print()
        print(f"  Max Drawdown:      ${m.max_drawdown:.2f} ({m.max_drawdown_pct:.1%})")
        print(f"  Sharpe Ratio:      {m.sharpe_ratio:.2f}" if m.sharpe_ratio else "  Sharpe Ratio:      N/A")
        print()
        print(f"  Theoretical POP:   {m.avg_theoretical_pop:.1%}")
        print(f"  Realized POP:      {m.realized_pop:.1%}")
        print(f"  POP Difference:    {m.pop_difference:+.1%}")

        if 'iv_bucket_stats' in m.details:
            print("\n  By IV Bucket:")
            for bucket, stats in m.details['iv_bucket_stats'].items():
                print(f"    {bucket.upper():8s}: {stats['count']:3d} trades, "
                      f"${stats['pnl']:8,.0f} P&L, {stats['win_rate']:.1%} win")

    # Iron Condor Section
    if report.condor_metrics:
        m = report.condor_metrics
        print()
        print("-" * 40)
        print("IRON CONDORS")
        print("-" * 40)
        print(f"  Symbols tested:    {m.total_symbols}")
        print(f"  Symbols w/ trades: {m.symbols_with_trades}")
        print(f"  Total trades:      {m.total_trades}")
        print()
        print(f"  Win Rate:          {m.win_rate:.1%}")
        print(f"  Total P&L:         ${m.total_pnl:,.2f}")
        print(f"  Avg P&L/trade:     ${m.avg_pnl_per_trade:.2f}")
        print(f"  Median P&L:        ${m.median_pnl:.2f}")
        print(f"  Std Dev:           ${m.std_pnl:.2f}")
        print()
        print(f"  Avg Win:           ${m.avg_win:.2f}")
        print(f"  Avg Loss:          ${m.avg_loss:.2f}")
        print(f"  Expectancy:        ${m.expectancy:.2f}")
        print(f"  Profit Factor:     {m.profit_factor:.2f}")
        print()
        print(f"  Max Drawdown:      ${m.max_drawdown:.2f} ({m.max_drawdown_pct:.1%})")
        print(f"  Sharpe Ratio:      {m.sharpe_ratio:.2f}" if m.sharpe_ratio else "  Sharpe Ratio:      N/A")
        print()
        print(f"  Theoretical POP:   {m.avg_theoretical_pop:.1%}")
        print(f"  Realized POP:      {m.realized_pop:.1%}")
        print(f"  POP Difference:    {m.pop_difference:+.1%}")

        if 'put_breaches' in m.details:
            print(f"\n  Breach Analysis:")
            print(f"    Put breaches:    {m.details['put_breaches']}")
            print(f"    Call breaches:   {m.details['call_breaches']}")

        if 'iv_tier_stats' in m.details:
            print("\n  By IV Tier:")
            for tier, stats in m.details['iv_tier_stats'].items():
                print(f"    {tier.upper():10s}: {stats['count']:3d} trades, "
                      f"${stats['pnl']:8,.0f} P&L, {stats['win_rate']:.1%} win")

    # Comparison
    print()
    print("=" * 40)
    print("COMPARISON")
    print("=" * 40)
    print(f"  Better Strategy:   {report.better_strategy}")
    print(f"  P&L Advantage:     ${report.better_by_pnl:,.2f}")

    if report.put_spread_metrics and report.condor_metrics:
        ps = report.put_spread_metrics
        ic = report.condor_metrics
        print()
        print("  Head-to-Head:")
        print(f"    {'Metric':<20} {'Put Spread':>15} {'Condor':>15}")
        print(f"    {'-'*50}")
        print(f"    {'Win Rate':<20} {ps.win_rate:>14.1%} {ic.win_rate:>14.1%}")
        print(f"    {'Total P&L':<20} ${ps.total_pnl:>13,.0f} ${ic.total_pnl:>13,.0f}")
        print(f"    {'Avg P&L/Trade':<20} ${ps.avg_pnl_per_trade:>13,.0f} ${ic.avg_pnl_per_trade:>13,.0f}")
        print(f"    {'Expectancy':<20} ${ps.expectancy:>13,.0f} ${ic.expectancy:>13,.0f}")
        print(f"    {'Profit Factor':<20} {ps.profit_factor:>14.2f} {ic.profit_factor:>14.2f}")
        print(f"    {'Max Drawdown':<20} ${ps.max_drawdown:>13,.0f} ${ic.max_drawdown:>13,.0f}")

    print()
    print("=" * 80)


def export_trades_csv(
    put_spread_results: Optional[Dict[str, PutSpreadBacktestResult]],
    condor_results: Optional[Dict[str, CondorBacktestResult]],
    output_path: str,
):
    """Export all trades to CSV file."""

    rows = []

    # Put spread trades
    if put_spread_results:
        for symbol, result in put_spread_results.items():
            for t in result.trades:
                rows.append({
                    'strategy': 'put_spread',
                    'symbol': t.symbol,
                    'entry_date': t.entry_date,
                    'expiration_date': t.expiration_date,
                    'spot_price': t.spot_price,
                    'short_strike': t.short_strike,
                    'long_strike': t.long_strike,
                    'short_call_strike': '',
                    'long_call_strike': '',
                    'credit': t.credit,
                    'max_loss': t.max_loss,
                    'theoretical_pop': t.theoretical_pop,
                    'iv_rank': t.iv_rank,
                    'iv_at_entry': t.iv_at_entry,
                    'exit_price': t.exit_price,
                    'pnl': t.pnl,
                    'won': t.won,
                    'reason': t.reason,
                })

    # Condor trades
    if condor_results:
        for symbol, result in condor_results.items():
            for t in result.trades:
                rows.append({
                    'strategy': 'iron_condor',
                    'symbol': t.symbol,
                    'entry_date': t.entry_date,
                    'expiration_date': t.expiration_date,
                    'spot_price': t.spot_price,
                    'short_strike': t.short_put_strike,
                    'long_strike': t.long_put_strike,
                    'short_call_strike': t.short_call_strike,
                    'long_call_strike': t.long_call_strike,
                    'credit': t.credit,
                    'max_loss': t.max_loss,
                    'theoretical_pop': t.theoretical_pop,
                    'iv_rank': t.iv_rank,
                    'iv_at_entry': t.iv_at_entry,
                    'exit_price': t.exit_price,
                    'pnl': t.pnl,
                    'won': t.won,
                    'reason': t.reason,
                })

    if not rows:
        logger.warning("No trades to export")
        return

    # Sort by date
    rows.sort(key=lambda r: r['entry_date'])

    # Write CSV
    fieldnames = list(rows[0].keys())
    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    logger.info(f"Exported {len(rows)} trades to {output_path}")


def export_summary_json(
    report: ComparisonReport,
    output_path: str,
):
    """Export summary statistics to JSON file."""

    def metrics_to_dict(m: Optional[StrategyMetrics]) -> Optional[dict]:
        if m is None:
            return None
        return {
            'strategy_name': m.strategy_name,
            'total_symbols': m.total_symbols,
            'symbols_with_trades': m.symbols_with_trades,
            'total_trades': m.total_trades,
            'winning_trades': m.winning_trades,
            'losing_trades': m.losing_trades,
            'total_pnl': m.total_pnl,
            'avg_pnl_per_trade': m.avg_pnl_per_trade,
            'median_pnl': m.median_pnl,
            'std_pnl': m.std_pnl,
            'win_rate': m.win_rate,
            'avg_win': m.avg_win,
            'avg_loss': m.avg_loss,
            'expectancy': m.expectancy,
            'profit_factor': m.profit_factor if m.profit_factor != float('inf') else None,
            'max_drawdown': m.max_drawdown,
            'max_drawdown_pct': m.max_drawdown_pct,
            'sharpe_ratio': m.sharpe_ratio,
            'avg_theoretical_pop': m.avg_theoretical_pop,
            'realized_pop': m.realized_pop,
            'pop_difference': m.pop_difference,
            'details': m.details,
        }

    output = {
        'generated_at': report.generated_at,
        'put_spread_metrics': metrics_to_dict(report.put_spread_metrics),
        'condor_metrics': metrics_to_dict(report.condor_metrics),
        'comparison': {
            'better_strategy': report.better_strategy,
            'better_by_pnl': report.better_by_pnl,
            'better_by_winrate': report.better_by_winrate,
        },
        'summary': report.summary,
    }

    with open(output_path, 'w') as f:
        json.dump(output, f, indent=2)

    logger.info(f"Exported summary to {output_path}")


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":
    print("Backtest Report Module - Use via run_backtest.py")
