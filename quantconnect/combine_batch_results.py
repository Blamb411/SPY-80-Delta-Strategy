"""
Combine QC Batch Results & Compare with Massive API Backtester
===============================================================

Usage:
  1. Run multi_ticker_batch_test.py on QC for batches 1-4
  2. Copy the CSV data from each run's debug log into separate files:
     batch1_trades.csv, batch2_trades.csv, batch3_trades.csv, batch4_trades.csv
  3. Run this script to combine and compare

Or: paste all 4 CSV blocks into a single file (qc_all_trades.csv) and
this script will handle it.

Comparison notes (QC vs Massive API backtester):
  - QC uses $5/$10 fixed spread widths; Massive uses 5%/10% percentage widths
  - QC has real-time Greeks/IV from OptionChain; Massive uses HV-based IV Rank proxy
  - QC IV Rank: min/max of trailing IV from ATM put; Massive: percentile of 20-day HV
  - Both use bid/ask pricing for entries and exits
  - QC has deeper option chain data (resolution-level quotes)
  - Massive has synthetic pricing pre-2022; QC uses QuantConnect's data
"""

import csv
import os
import math
from collections import defaultdict

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
MASSIVE_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), "Massive backtesting")


def load_csv(filepath):
    """Load trade CSV file."""
    if not os.path.exists(filepath):
        return []
    with open(filepath) as f:
        return list(csv.DictReader(f))


def load_qc_trades():
    """Load QC trades from batch files or combined file."""
    # Try combined file first
    combined = os.path.join(SCRIPT_DIR, "qc_all_trades.csv")
    if os.path.exists(combined):
        trades = load_csv(combined)
        if trades:
            print(f"Loaded {len(trades)} QC trades from qc_all_trades.csv")
            return trades

    # Try individual batch files
    all_trades = []
    for batch in range(1, 5):
        filepath = os.path.join(SCRIPT_DIR, f"batch{batch}_trades.csv")
        if os.path.exists(filepath):
            batch_trades = load_csv(filepath)
            all_trades.extend(batch_trades)
            print(f"  Batch {batch}: {len(batch_trades)} trades from {filepath}")

    if all_trades:
        print(f"Loaded {len(all_trades)} QC trades total from batch files")
    else:
        print("No QC trade files found. Expected:")
        print("  qc_all_trades.csv OR batch1_trades.csv through batch4_trades.csv")
        print("  in the quantconnect/ directory")
    return all_trades


def load_massive_trades():
    """Load Massive API backtester trades (most recent CSV)."""
    import glob
    pattern = os.path.join(MASSIVE_DIR, "trades_*.csv")
    files = sorted(glob.glob(pattern))
    if not files:
        print("No Massive API trade files found in Massive backtesting/")
        return []

    # Use most recent
    filepath = files[-1]
    trades = load_csv(filepath)
    print(f"Loaded {len(trades)} Massive trades from {os.path.basename(filepath)}")
    return trades


def calc_stats(trades, pnl_field="exit_pnl"):
    """Calculate summary statistics for a list of trade dicts."""
    if not trades:
        return None

    pnls = [float(t[pnl_field]) for t in trades]
    n = len(pnls)
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    total = sum(pnls)

    # Max drawdown
    cumulative = 0
    peak = 0
    max_dd = 0
    for p in pnls:
        cumulative += p
        if cumulative > peak:
            peak = cumulative
        dd = peak - cumulative
        if dd > max_dd:
            max_dd = dd

    # Daily Sharpe
    date_field = "exit_date"
    daily_pnl = defaultdict(float)
    for t in trades:
        d = t.get(date_field, t.get("entry_date", "unknown"))
        daily_pnl[str(d)] += float(t[pnl_field])

    daily_returns = list(daily_pnl.values())
    if len(daily_returns) > 1:
        mu = sum(daily_returns) / len(daily_returns)
        std = math.sqrt(sum((r - mu)**2 for r in daily_returns) / (len(daily_returns) - 1))
        sharpe = (mu / std) * math.sqrt(252) if std > 0 else 0
    else:
        sharpe = 0

    return {
        'count': n,
        'wins': len(wins),
        'losses': len(losses),
        'win_rate': len(wins) / n,
        'total_pnl': total,
        'avg_pnl': total / n,
        'avg_win': sum(wins) / len(wins) if wins else 0,
        'avg_loss': sum(losses) / len(losses) if losses else 0,
        'max_dd': max_dd,
        'sharpe': sharpe,
    }


def print_stats(label, stats):
    """Print formatted stats."""
    if stats is None:
        print(f"  {label}: no data")
        return
    print(f"  {label}:")
    print(f"    Trades: {stats['count']:>6}   Win Rate: {stats['win_rate']:.1%}")
    print(f"    Total P&L: ${stats['total_pnl']:>12,.0f}   Avg P&L: ${stats['avg_pnl']:>8,.2f}")
    print(f"    Avg Win:   ${stats['avg_win']:>8,.2f}   Avg Loss: ${stats['avg_loss']:>8,.2f}")
    print(f"    Max DD:    ${stats['max_dd']:>8,.0f}   Sharpe:   {stats['sharpe']:>8.4f}")


def get_strategy_type(trade, source):
    """Normalize strategy type field across sources."""
    if source == "qc":
        return trade.get("strategy", "").upper()
    else:  # massive
        return trade.get("strategy_type", "").upper()


def get_ticker(trade, source):
    """Normalize ticker field across sources."""
    if source == "qc":
        return trade.get("ticker", "")
    else:
        return trade.get("underlying", "")


def get_combo_key(trade, source):
    """Build a normalized combo key for comparison."""
    strat = get_strategy_type(trade, source)
    width = trade.get("width", trade.get("spread_width", ""))
    tp = trade.get("tp_pct", trade.get("take_profit_pct", ""))
    sl = trade.get("sl_mult", trade.get("stop_loss_mult", ""))
    return f"{strat}_{width}_{tp}_{sl}"


def compare_reports(qc_trades, massive_trades):
    """Generate comparison report."""
    # Determine pnl field for massive trades
    massive_pnl = "pnl" if massive_trades and "pnl" in massive_trades[0] else "exit_pnl"

    print()
    print("=" * 90)
    print("COMPARISON: QuantConnect vs Massive API Backtester")
    print("=" * 90)

    # Determine overlapping date range
    qc_dates = set()
    massive_dates = set()
    for t in qc_trades:
        d = str(t.get("entry_date", ""))[:10]
        if d:
            qc_dates.add(d)
    for t in massive_trades:
        d = str(t.get("entry_date", ""))[:10]
        if d:
            massive_dates.add(d)

    if qc_dates and massive_dates:
        qc_range = f"{min(qc_dates)} to {max(qc_dates)}"
        massive_range = f"{min(massive_dates)} to {max(massive_dates)}"
        overlap = qc_dates & massive_dates
        print(f"  QC date range:      {qc_range}")
        print(f"  Massive date range: {massive_range}")
        print(f"  Overlapping entry dates: {len(overlap)}")
    print()

    # Overall comparison
    print("-" * 90)
    print("OVERALL SUMMARY")
    print("-" * 90)
    qc_stats = calc_stats(qc_trades, "exit_pnl")
    massive_stats = calc_stats(massive_trades, massive_pnl)
    print_stats("QuantConnect", qc_stats)
    print_stats("Massive API", massive_stats)
    print()

    # By strategy type
    print("-" * 90)
    print("BY STRATEGY TYPE")
    print("-" * 90)
    for strat in ["PUT", "CALL"]:
        print(f"\n  --- {strat} Spreads ---")
        qc_strat = [t for t in qc_trades if get_strategy_type(t, "qc") == strat]
        massive_strat = [t for t in massive_trades if get_strategy_type(t, "massive") == strat]
        print_stats("  QC", calc_stats(qc_strat, "exit_pnl") if qc_strat else None)
        print_stats("  Massive", calc_stats(massive_strat, massive_pnl) if massive_strat else None)
    print()

    # By ticker
    all_tickers = sorted(set(
        [get_ticker(t, "qc") for t in qc_trades] +
        [get_ticker(t, "massive") for t in massive_trades]
    ))
    all_tickers = [t for t in all_tickers if t]  # filter empty

    print("-" * 90)
    print("BY TICKER")
    print("-" * 90)
    print(f"  {'Ticker':<8} {'QC Trades':>10} {'QC Win%':>8} {'QC P&L':>12} "
          f"{'Mass Trades':>12} {'Mass Win%':>10} {'Mass P&L':>12} {'Delta':>10}")
    print(f"  {'-'*84}")

    for ticker in all_tickers:
        qc_ticker = [t for t in qc_trades if get_ticker(t, "qc") == ticker]
        massive_ticker = [t for t in massive_trades if get_ticker(t, "massive") == ticker]

        qc_s = calc_stats(qc_ticker, "exit_pnl") if qc_ticker else None
        m_s = calc_stats(massive_ticker, massive_pnl) if massive_ticker else None

        qc_count = qc_s['count'] if qc_s else 0
        qc_wr = f"{qc_s['win_rate']:.1%}" if qc_s else "N/A"
        qc_pnl = qc_s['total_pnl'] if qc_s else 0
        m_count = m_s['count'] if m_s else 0
        m_wr = f"{m_s['win_rate']:.1%}" if m_s else "N/A"
        m_pnl = m_s['total_pnl'] if m_s else 0

        delta = qc_pnl - m_pnl if qc_s and m_s else 0

        print(f"  {ticker:<8} {qc_count:>10} {qc_wr:>8} ${qc_pnl:>10,.0f} "
              f"{m_count:>12} {m_wr:>10} ${m_pnl:>10,.0f} ${delta:>8,.0f}")

    print()

    # Key differences callout
    print("-" * 90)
    print("KEY DIFFERENCES TO NOTE")
    print("-" * 90)
    print("  1. Spread Widths: QC uses $5/$10 fixed; Massive uses 5%/10% of underlying")
    print("     - For SPY at $450, Massive 5% = $22.50 vs QC $5")
    print("     - This means Massive spreads are MUCH wider and collect more premium")
    print("  2. IV Source: QC uses real-time implied vol from option chain;")
    print("     Massive uses historical volatility as proxy for IV Rank")
    print("  3. Pricing: QC uses QuantConnect's option data feed;")
    print("     Massive uses Polygon/Massive API daily open-close data")
    print("  4. Pre-2022 Data: Massive uses synthetic 5% bid-ask spread;")
    print("     QC uses QuantConnect's historical option data")
    print("  5. Strike Selection: Both target 0.25 delta with 5% OTM fallback,")
    print("     but delta computation differs (QC real Greeks vs Massive BS model)")
    print()


def main():
    print("=" * 90)
    print("QC BATCH RESULTS COMBINER & COMPARISON TOOL")
    print("=" * 90)
    print()

    qc_trades = load_qc_trades()
    massive_trades = load_massive_trades()

    if qc_trades:
        print()
        print("=" * 90)
        print("QUANTCONNECT RESULTS (all batches combined)")
        print("=" * 90)
        print()

        # Overall
        print_stats("ALL TRADES", calc_stats(qc_trades, "exit_pnl"))
        print()

        # By strategy
        for strat in ["PUT", "CALL"]:
            subset = [t for t in qc_trades if get_strategy_type(t, "qc") == strat]
            if subset:
                print_stats(f"{strat} Spreads", calc_stats(subset, "exit_pnl"))
        print()

        # By ticker
        tickers = sorted(set(get_ticker(t, "qc") for t in qc_trades))
        print(f"  {'Ticker':<8} {'Trades':>8} {'Win%':>8} {'Total P&L':>12} {'Avg P&L':>10}")
        print(f"  {'-'*50}")
        for ticker in tickers:
            if not ticker:
                continue
            subset = [t for t in qc_trades if get_ticker(t, "qc") == ticker]
            s = calc_stats(subset, "exit_pnl")
            print(f"  {ticker:<8} {s['count']:>8} {s['win_rate']:>7.1%} ${s['total_pnl']:>10,.0f} ${s['avg_pnl']:>8,.2f}")
        print()

        # Top 10 combos
        combo_pnls = defaultdict(list)
        for t in qc_trades:
            key = get_combo_key(t, "qc")
            combo_pnls[key].append(float(t["exit_pnl"]))

        combo_ranked = sorted(combo_pnls.items(), key=lambda x: sum(x[1]), reverse=True)
        print("  Top 10 Combinations:")
        for i, (key, pnls) in enumerate(combo_ranked[:10], 1):
            total = sum(pnls)
            wins = len([p for p in pnls if p > 0])
            wr = wins / len(pnls) if pnls else 0
            print(f"    {i:>2}. {key:<30} {len(pnls):>5} trades  "
                  f"Win {wr:.1%}  Total ${total:>10,.0f}")
        print()

        # Exit reasons
        reasons = defaultdict(int)
        for t in qc_trades:
            reasons[t.get("exit_reason", "unknown")] += 1
        print(f"  Exit Reasons: {dict(sorted(reasons.items()))}")
        print()

    if qc_trades and massive_trades:
        compare_reports(qc_trades, massive_trades)
    elif not qc_trades:
        print("\nNo QC trades loaded. Run the batch algorithm on QuantConnect first.")
        print("Copy CSV data from debug logs into batch1_trades.csv through batch4_trades.csv")
    elif not massive_trades:
        print("\nNo Massive trades loaded. Run the Massive API backtester first.")


if __name__ == "__main__":
    main()
