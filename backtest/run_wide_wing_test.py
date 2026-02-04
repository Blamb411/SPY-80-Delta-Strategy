#!/usr/bin/env python3
"""Quick test of wider wing widths: 5%, 7%, 9% (with 3% as baseline)."""

import os
import sys

_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_this_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)
if _this_dir not in sys.path:
    sys.path.insert(0, _this_dir)

from backtest.put_spread_thetadata import run_backtest, compute_risk_metrics, print_results

START_YEAR = 2012
END_YEAR = 2025


def main():
    wing_widths = [0.03, 0.05, 0.07, 0.09]
    results = []

    for wwp in wing_widths:
        label = f"{wwp*100:.0f}% wing"
        print(f"\n--- Running: {label}, SMA=200, SL=3.0x ---")
        trades, sk_sma, sk_iv, sk_oi, sk_cw, sk_data = run_backtest(
            START_YEAR, END_YEAR,
            stop_loss_mult=3.0,
            sma_period=200,
            wing_width_pct=wwp,
        )
        n = len(trades)
        if n == 0:
            results.append({"label": label, "n": 0})
            continue

        wins = sum(1 for t in trades if t["won"])
        total_pnl = sum(t["pnl"] for t in trades)
        avg_pnl = total_pnl / n
        avg_credit = sum(t["credit"] for t in trades) / n
        avg_max_loss = sum(t["max_loss"] for t in trades) / n
        avg_width = sum(t["put_width"] for t in trades) / n
        stops = sum(1 for t in trades if t["exit_reason"] == "stop_loss")
        breaches = sum(1 for t in trades if t["side_breached"] == "put")
        take_profits = sum(1 for t in trades if t["exit_reason"] == "take_profit")
        expirations = sum(1 for t in trades
                         if t["exit_reason"] in ("expiration", "expiration_fallback"))
        m = compute_risk_metrics(trades)

        # Capital efficiency: total P&L / avg max loss
        cap_eff = total_pnl / avg_max_loss if avg_max_loss > 0 else 0

        results.append({
            "label": label, "n": n, "wins": wins, "wr": wins / n,
            "pnl": total_pnl, "avg": avg_pnl,
            "cred": avg_credit, "ml": avg_max_loss, "width": avg_width,
            "stops": stops, "breaches": breaches,
            "tp": take_profits, "exp": expirations,
            "cap_eff": cap_eff,
            "metrics": m,
        })

    # --- Table 1: Performance ---
    print()
    print("=" * 110)
    print(f"WING WIDTH COMPARISON  |  SPY Put Spreads  |  {START_YEAR}-{END_YEAR}"
          f"  |  SMA=200  SL=3.0x")
    print("=" * 110)
    print(f"{'Config':<10} | {'Trd':>4} | {'Win%':>6} | {'Tot P&L':>10}"
          f" | {'Avg P&L':>9} | {'AvgCred':>8} | {'AvgMaxL':>8}"
          f" | {'AvgWid':>7} | {'TP':>4} | {'SL':>4} | {'Exp':>4} | {'PutBr':>5}")
    print("-" * 110)
    for r in results:
        if r["n"] == 0:
            print(f"{r['label']:<10} |  N/A |")
            continue
        print(f"{r['label']:<10} | {r['n']:>4} | {r['wr']*100:>5.1f}%"
              f" | ${r['pnl']:>+9,.0f} | ${r['avg']:>+8.2f}"
              f" | ${r['cred']:>7.3f} | ${r['ml']:>7.0f}"
              f" | ${r['width']:>6.1f} | {r['tp']:>4} | {r['stops']:>4}"
              f" | {r['exp']:>4} | {r['breaches']:>5}")
    print("=" * 110)

    # --- Table 2: Risk Metrics ---
    print()
    print("=" * 110)
    print("RISK-ADJUSTED METRICS  (returns = P&L / max_loss per trade)")
    print("=" * 110)
    print(f"{'Config':<10} | {'Sharpe':>7} | {'ShpAnn':>7} | {'PSR':>7}"
          f" | {'Sortino':>8} | {'SrtAnn':>7}"
          f" | {'MeanRet':>8} | {'StdRet':>8}"
          f" | {'Skew':>7} | {'ExKurt':>7} | {'CapEff':>7}")
    print("-" * 110)
    for r in results:
        if r["n"] == 0 or r.get("metrics") is None:
            print(f"{r['label']:<10} |    N/A |")
            continue
        m = r["metrics"]
        print(f"{r['label']:<10} | {m['sharpe']:>7.3f} | {m['sharpe_annual']:>7.3f}"
              f" | {m['psr']*100:>6.1f}% | {m['sortino']:>8.3f}"
              f" | {m['sortino_annual']:>7.3f}"
              f" | {m['mean_return']:>8.4f} | {m['std_return']:>8.4f}"
              f" | {m['skewness']:>7.3f} | {m['kurtosis_excess']:>7.3f}"
              f" | {r['cap_eff']:>6.2f}x")
    print("=" * 110)

    # --- Detailed output for 7% and 9% ---
    for wwp in [0.07, 0.09]:
        trades, sk_sma, sk_iv, sk_oi, sk_cw, sk_data = run_backtest(
            START_YEAR, END_YEAR,
            stop_loss_mult=3.0,
            sma_period=200,
            wing_width_pct=wwp,
        )
        print_results(trades, sk_sma, sk_iv, sk_oi, sk_data)


if __name__ == "__main__":
    main()
