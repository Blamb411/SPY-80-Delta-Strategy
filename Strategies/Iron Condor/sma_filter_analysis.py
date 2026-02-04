"""
SMA Filter Impact Analysis
============================
Retroactively applies a 200-day SMA filter to the existing trade data
to measure how much it would have helped.

For PUT spreads: only enter when spot > 200 SMA (uptrend)
For CALL spreads: only enter when spot < 200 SMA (downtrend)
  -- OR keep all CALL entries regardless (test both)
"""

import csv
import math
from collections import defaultdict
import cache_db

# Load trades
with open("trades_20260129_081907.csv") as f:
    trades = list(csv.DictReader(f))

print(f"Loaded {len(trades)} trades")
print()

# Compute 200-day SMA for each ticker on each entry date
# We already have underlying bars in the cache
def get_sma200(ticker, date_str):
    closes = cache_db.get_all_closes(ticker, date_str)
    if len(closes) < 200:
        return None
    return sum(closes[-200:]) / 200

# Cache SMA values
sma_cache = {}
for t in trades:
    key = (t["underlying"], t["entry_date"])
    if key not in sma_cache:
        sma_cache[key] = get_sma200(t["underlying"], t["entry_date"])

# Tag each trade with SMA status
for t in trades:
    spot = float(t["spot_at_entry"])
    sma = sma_cache.get((t["underlying"], t["entry_date"]))
    t["sma200"] = sma
    t["above_sma"] = sma is not None and spot > sma
    t["below_sma"] = sma is not None and spot < sma
    t["pnl_f"] = float(t["pnl"])

def report(label, subset):
    if not subset:
        print(f"  {label}: no trades")
        return
    pnls = [float(t["pnl"]) for t in subset]
    n = len(pnls)
    wins = sum(1 for p in pnls if p > 0)
    total = sum(pnls)
    avg = total / n
    losses = [p for p in pnls if p <= 0]
    win_list = [p for p in pnls if p > 0]
    avg_win = sum(win_list) / len(win_list) if win_list else 0
    avg_loss = sum(losses) / len(losses) if losses else 0

    # Daily Sharpe
    daily_pnl = defaultdict(float)
    for t in subset:
        daily_pnl[t["exit_date"]] += float(t["pnl"])
    daily_returns = list(daily_pnl.values())
    if len(daily_returns) > 1:
        mu = sum(daily_returns) / len(daily_returns)
        std = math.sqrt(sum((r - mu)**2 for r in daily_returns) / (len(daily_returns) - 1))
        sharpe = (mu / std) * math.sqrt(252) if std > 0 else 0
    else:
        sharpe = 0

    print(f"  {label}:")
    print(f"    Trades: {n:>6}  Win%: {wins/n:.1%}  Total P&L: ${total:>10,.0f}  "
          f"Avg: ${avg:>7,.0f}  AvgW: ${avg_win:>7,.0f}  AvgL: ${avg_loss:>7,.0f}  "
          f"Sharpe: {sharpe:>6.2f}")

# ============================================================
print("=" * 90)
print("SCENARIO 1: NO FILTER (current results)")
print("=" * 90)
puts = [t for t in trades if t["strategy_type"] == "PUT"]
calls = [t for t in trades if t["strategy_type"] == "CALL"]
report("ALL trades", trades)
report("PUT spreads", puts)
report("CALL spreads", calls)
print()

# ============================================================
print("=" * 90)
print("SCENARIO 2: 200 SMA FILTER — puts only above SMA, calls unfiltered")
print("=" * 90)
filtered_puts = [t for t in puts if t["above_sma"]]
filtered = filtered_puts + calls
report("ALL trades", filtered)
report("PUT (above SMA only)", filtered_puts)
report("CALL (unfiltered)", calls)
print()
removed_puts = [t for t in puts if not t["above_sma"]]
report("PUT trades REMOVED by filter", removed_puts)
print()

# ============================================================
print("=" * 90)
print("SCENARIO 3: 200 SMA FILTER — puts above SMA, calls below SMA")
print("=" * 90)
filtered_calls = [t for t in calls if t["below_sma"]]
filtered_both = filtered_puts + filtered_calls
report("ALL trades", filtered_both)
report("PUT (above SMA only)", filtered_puts)
report("CALL (below SMA only)", filtered_calls)
print()
removed_calls = [t for t in calls if not t["below_sma"]]
report("CALL trades REMOVED by filter", removed_calls)
print()

# ============================================================
print("=" * 90)
print("SCENARIO 4: 200 SMA FILTER — puts only above SMA, NO call spreads")
print("=" * 90)
report("PUT (above SMA only)", filtered_puts)
print()

# ============================================================
print("=" * 90)
print("BREAKDOWN: PUT spreads above vs below SMA")
print("=" * 90)
report("PUT above 200 SMA (uptrend)", [t for t in puts if t["above_sma"]])
report("PUT below 200 SMA (downtrend)", [t for t in puts if t["below_sma"]])
print()

print("=" * 90)
print("BREAKDOWN: CALL spreads above vs below SMA")
print("=" * 90)
report("CALL above 200 SMA", [t for t in calls if t["above_sma"]])
report("CALL below 200 SMA", [t for t in calls if t["below_sma"]])
print()

# ============================================================
# Per-ticker SMA impact
# ============================================================
print("=" * 90)
print("PER-TICKER: PUT spread P&L above vs below 200 SMA")
print("=" * 90)
tickers_in_data = sorted(set(t["underlying"] for t in puts))
print(f"  {'Ticker':<8} {'Above SMA':>12} {'Below SMA':>12} {'Delta':>12} {'Above Win%':>12} {'Below Win%':>12}")
print(f"  {'-'*70}")
for ticker in tickers_in_data:
    above = [t for t in puts if t["underlying"] == ticker and t["above_sma"]]
    below = [t for t in puts if t["underlying"] == ticker and t["below_sma"]]
    pnl_above = sum(float(t["pnl"]) for t in above) if above else 0
    pnl_below = sum(float(t["pnl"]) for t in below) if below else 0
    wr_above = sum(1 for t in above if float(t["pnl"]) > 0) / len(above) if above else 0
    wr_below = sum(1 for t in below if float(t["pnl"]) > 0) / len(below) if below else 0
    print(f"  {ticker:<8} ${pnl_above:>10,.0f} ${pnl_below:>10,.0f} ${pnl_above-pnl_below:>10,.0f} "
          f"{wr_above:>11.1%} {wr_below:>11.1%}")

# Summary
above_total = sum(float(t["pnl"]) for t in puts if t["above_sma"])
below_total = sum(float(t["pnl"]) for t in puts if t["below_sma"])
print(f"  {'TOTAL':<8} ${above_total:>10,.0f} ${below_total:>10,.0f} ${above_total-below_total:>10,.0f}")
print()
print(f"  PUT losses avoided by SMA filter: ${abs(below_total):,.0f}")
