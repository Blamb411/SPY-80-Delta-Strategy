# ThetaData Iron Condor Backtester - Session Log

## Last Session: January 30, 2026

### What Was Built
Two new files implementing the ThetaData-powered iron condor backtester:

1. **`backtest/thetadata_client.py`** - ThetaData v3 REST API client with SQLite caching
   - Connects to Theta Terminal on localhost:25503
   - All API responses cached in `backtest/thetadata_cache.db` (zero API calls on re-runs)
   - Yahoo Finance helpers for VIX and SPY daily data
   - v3 API endpoints (migrated from v2 during development)

2. **`backtest/condor_thetadata.py`** - Iron condor backtester (2012-2026)
   - Uses real ThetaData bid/ask quotes from June 2012 onward
   - Black-Scholes synthetic fallback for Jan-May 2012
   - Same strategy logic as `condor_real_data.py` (IV rank, delta targeting, 50% TP / 75% SL)
   - Data quality filters: OTM validation, $5 min wing width, 60% credit/wing cap
   - CLI: `--year`, `--start/--end`, `--synthetic-only`, `--export-csv`

3. **`backtest/compare_results.py`** - Side-by-side Polygon vs ThetaData comparison

### Completed Runs & Results

| Run | Trades | Win Rate | Total P&L | File |
|-----|--------|----------|-----------|------|
| Full 2012-2026 (unfiltered) | 163 | 44.8% | $6,984 | `results/full_backtest.csv` |
| Full 2012-2026 (filtered) | 60 | 60.0% | $-3,537 | `results/full_backtest_filtered.csv` |
| ThetaData 2020-2025 only | 65 | 66.2% | $-19 | `results/theta_2020_2025.csv` |
| Polygon 2020-2025 (condor_real_data.py) | 81 | 76.5% | $-2,818 | (printed to console) |

### Key Findings from Comparison (2020-2025)

1. **Breach direction flips**: Real quotes show 14 call breaches vs 1 put (Polygon: 7 call, 13 put). Volatility skew makes puts cheaper to close.
2. **IV tier reversal**: Medium IV profitable (+$33/trade) with real data; very high IV loses (-$215/trade). Polygon showed the opposite.
3. **Exit profile**: Real spreads make TP harder (53.8% vs 71.6%) and SL easier to hit (33.8% vs 13.6%).
4. **Both agree**: Strategy is roughly flat over 2020-2025.

### Bugs Fixed During Session
- v3 API migration (v2 endpoints return HTTP 410)
- HTTP 472 "no data" handled as empty result
- Entry-date fallback widened to 5 days
- OTM strike validation added (early years had ITM call legs)
- $5 min wing width (was $10, too aggressive for pre-2021 SPY prices)
- 60% credit/wing ratio cap
- Entry interval advancement on failed trades

### Cache State
- `backtest/thetadata_cache.db` contains all fetched ThetaData responses
- Re-running any backtest makes zero API calls (fully cached)
- Cache covers SPY options from 2012-2026

---

## Where to Pick Up Next

### Immediate Options
1. **Strategy refinement** - The comparison revealed that medium IV is the sweet spot and very high IV should potentially be avoided or use tighter parameters. Could test modified thresholds.
2. **Call-side adjustment** - 14 call breaches vs 1 put suggests the call wing needs wider strikes or the strategy should use asymmetric deltas.
3. **Multi-ticker expansion** - Currently SPY only. Could add QQQ, IWM, etc. (ThetaData has data for all).
4. **Live trading integration** - Wire findings into the actual trading bot.
5. **Full 2012-2026 analysis** - The filtered run (60 trades) is clean but thin. Could relax filters slightly to get more trades while keeping data quality.

### Prerequisites
- Theta Terminal must be running for any new API calls: `java -jar C:\ThetaTerminal\ThetaTerminal.jar` (creds in `C:\ThetaTerminal\creds.txt`)
- For cached re-runs, Theta Terminal is NOT needed

### Quick Test Commands
```bash
# Re-run filtered 2012-2026 (uses cache, no Theta Terminal needed)
python backtest/condor_thetadata.py

# Single year smoke test
python backtest/condor_thetadata.py --year 2024

# Synthetic only (no ThetaData needed at all)
python backtest/condor_thetadata.py --synthetic-only

# Show comparison
python backtest/compare_results.py
```
