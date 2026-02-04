# Session Notes — January 30, 2026

## What We Built Today (Jan 30)

### 1. Real-Data Iron Condor Backtest (condor_real_data.py)
- **New file**: `Massive backtesting/condor_real_data.py` (~430 lines, self-contained)
- Validates the BS-theoretical condor backtest with real historical option prices from Polygon/Massive API
- Uses SPY as underlying (most liquid, $1 strike increments, MWF expirations)
- VIX as IV proxy (VIX = SPY 30-day ATM IV, no need to derive IV from option prices)
- **Architecture**: BS delta calc for target strikes -> find real Polygon contracts -> fetch real option bars -> price from real closes with 1% synthetic spread
- **Key question**: Is the "profitable at VIX>=25" finding real or a BS artifact?
- Entry parameters match existing condor_backtest.py: IV rank thresholds (0.30/0.50/0.70), delta (20/25/30), 3% wing width, 5-day entry interval, 25-45 DTE
- Exit rules: TP 50% of credit, SL 75% of max loss, intrinsic settlement at expiration
- Full reporting: by IV tier, by VIX bucket, monthly timeline, first 10 trades detail
- **Usage**: `python condor_real_data.py` (full 2020-2025) or `python condor_real_data.py --year 2024` (smoke test)

### 2. Real-Data Condor Backtest Results
- **2024 smoke test**: 11 trades, 90.9% win rate, +$819 P&L — looked great
- **2020-2025 full run**: 81 trades, 76.5% win rate, −$3,662 P&L — losses in bear markets
- **2016-2025 extended run**: 121 trades, 80.2% win rate, −$1,236 P&L
- **VIX>=25 IS genuinely profitable** with real data: High (25-30) +$40/trade, Very High (30+) +$49/trade
- VIX<25 is consistently negative — the strategy should ONLY be used in high-vol regimes
- **By IV tier**: Very High (30Δ) +$41/trade, High (25Δ) +$31/trade, Medium (20Δ) −$39/trade
- A filtered strategy (VIX>=25 AND IV rank>=0.50) would have been profitable over 10 years

### 3. Alpha Picks Candidates — Days as Strong Buy Analysis
- Updated `alpha_picks_candidates.csv` and `.xlsx` with actual Strong Buy duration data
- **3 stocks DISQUALIFIED** on 1/30/2026: VIAV, CENX, SANM downgraded to Hold
- **Only 3 candidates meet 75+ day threshold**: CSTM (206d), CLS (82d), GM (101d)
- COHR is close at 73 days (since 11/20/2025) — will qualify by ~Feb 3
- Most top-ranked candidates (FLNC, LASR, LITE, CIEN) are 54-62 days — too recent
- **Actionable picks for Feb 1**: CSTM, CLS, GM (all qualify); watch COHR

### 4. ThetaData Terminal — Successfully Connected
- Fixed config.toml (log_directory was Linux path `/tmp`, changed to `C:/ThetaTerminal/logs`)
- Terminal running on port 25503, v3 API working
- **Real bid/ask spread validation for SPY condor legs**:
  - Short Put (~25Δ): 2.3% spread (vs our 1% assumption — understated)
  - Long Put (wing): 3.0% spread (vs 1% — understated)
  - Short Call (~25Δ): 0.6% spread (vs 1% — OK)
  - Long Call (wing): 0.7% spread (vs 1% — OK)
  - Deep OTM Put: 3.9% spread (worst case)
- **Implication**: Put-side spreads are 2-4x wider than assumed; entry credits are overstated
- Free tier covers 2023-06 to present; Value ($32/mo) extends to 2022, Standard ($64/mo) to 2018

### 5. IBKR Paper Trading — Script Built & Tested
- **New file**: `backtest/ibkr_condor_paper.py` (~550 lines)
- Connects to TWS paper trading account (DUA976236, port 7497)
- Same entry/exit logic as the real-data backtest: IV rank thresholds, delta selection, TP 50%, SL 75%
- SQLite database (`condor_paper.db`) tracks positions and daily P&L logs
- **Test script** (`backtest/ibkr_condor_test.py`): Successfully quoted a full SPY condor
  - LP=605 SP=660 SC=730 LC=745 — all 4 legs qualified and priced
  - Real spreads: LP 1.5%, SP 0.9%, SC 4.7%, LC 9.5%
- **Dry-run test**: Full pipeline validated end-to-end against live TWS
  - VIX: 17.6, IV Rank: 0.11 — correctly skipped entry (low vol = unprofitable)
  - Force-entry dry run: LP=650 SP=670 | SC=725 LC=745, credit $2.75/sh ($275/ct), max loss $1,725
  - Expiration: 2026-02-27 (28 DTE)
- **Bug fix**: Switched from `reqTickers` (snapshot) to `reqMktData` (streaming) to avoid "competing live session" error 10197 when TWS Desktop is open
- **Usage**:
  - `python ibkr_condor_paper.py` — Daily run (monitor + check entry signals)
  - `python ibkr_condor_paper.py --dry-run` — Preview without trading
  - `python ibkr_condor_paper.py --force-entry` — Force entry for testing
  - `python ibkr_condor_paper.py --status` / `--history` — View positions/trades
  - `python ibkr_condor_paper.py --close-all` — Emergency close all
- **Current state**: Script ready for live paper trading. Will auto-enter when VIX/IV rank rises above 0.30
- Note: Paper account also has 1x NVDA 200C 3/20/26 (cost $719.85, current value $956.06)

### 6. LSEG Scoring Status Check
- Last run (Jan 29): 3 Buy, 304 Hold out of 340 stocks
- Root cause unchanged: LSEG returned data for only ~80 of 340 stocks
- ~260 stocks defaulted to composite score 2.500 (Hold) with N/A grades
- RIC conversion bug still pending fix for US stocks (B, GM, W, CLS, NEM, GILD, etc.)
- Priority: Fix LSEG data coverage before re-running scoring

---

# Session Notes — January 29, 2026 (Previous Session)

## What We Built Today

### 1. IV/Premium Correlation Analysis (options_on_picks.py)
- Added `print_iv_analysis()` function to options_on_picks.py
- Splits 77 historical 30D ATM call trades into terciles by option premium (as % of stock price)
- Computes Pearson correlation between premium% and 14-day return
- **Key Finding**: Correlation = -0.210 (slight negative)
  - Low premium (1.2%-4.4%): Avg +43.7%, 60% win rate
  - Mid premium (4.5%-6.3%): Avg +26.4%, 52% win rate
  - High premium (6.5%-15.9%): Avg +15.1%, 48% win rate
- **Implication**: Favor lower-IV candidates when pre-positioning for Alpha Picks
- Note: The "IV proxy" in the code understates true IV by ~2.5x. Proper ATM IV uses Brenner-Subrahmanyam: `IV = premium_pct / (40 * sqrt(DTE/365))`

### 2. Previously Built This Session (from prior context window)
- **alpha_picks_analysis.py** — Historical profile of 90 Alpha Picks (momentum, sector, market cap, announcement pops)
- **alpha_picks_predictor.py** — Scores ~340 SA Strong Buy stocks to predict next pick
  - Factors: momentum (40pts), sector (15pts), market cap (15pts), price range (5pts), recency penalty
  - Includes option pricing section (ATM 30d call price, premium%, IV proxy)
- **options_on_picks.py modifications**:
  - Stop-loss changed from 50% to 75% (75% rarely triggers, doesn't hurt returns)
  - Added --otm flag (tested 5% and 10% OTM — ATM is best risk-adjusted)
  - Added --dte-only flag for filtering to single DTE target
  - Bug fixes: stock price upper bound, dedup, split detection
- **Sector mapping fix** in alpha_picks_analysis.py — SIC-to-GICS mapping restructured so specific SIC ranges come before broad ranges

## Key Results Summary

### Best Options Strategy on Alpha Picks
- **30D ATM Calls, 14-day hold**: Avg +28.0%, Median +8.0%, 53% win rate (77 trades)
- 75% stop-loss: Neutral (only 9% trigger rate, avg barely changes)
- 5% OTM: Higher avg (+52.2%) but lower win rate (44%) and much worse median (-13%)
- 10% OTM: Avg +41.4%, 39% win rate, median -28.6%
- **ATM remains the best risk-adjusted choice**

### Predictor Top Candidates (for Feb 1 announcement)
- Top 10: LASR, FLNC, VIAV, CENX, AMKR, LITE, CIEN, COHR, CSTM, TPC
- Sector distribution: IT 8, Industrials 5, Health Care 3
- User needs to manually check Strong Buy duration on SA (75+ days required)

### ProQuant
- Poor fit for options strategy (14% win rate, mostly small/foreign stocks)
- Focus exclusively on Alpha Picks

## Quant Scoring Model Results (run_scoring.py — Jan 29, 2026)

### Run: 340 SA Strong Buy stocks, all factors
- **Result: 3 Buy, 304 Hold, 0 Strong Buy, 0 Sell**
- **Root cause: LSEG returned data for only ~80 of 340 stocks**
- ~260 stocks defaulted to composite score 2.500 (Hold) with N/A grades
- 29 circuit breakers triggered (notable: NVDA D+ momentum, MSFT D- momentum)

### Stocks That Scored Buy
1. NUTX 4.200 — A- Val, B+ Gro, A- Pro, B+ Mom, B+ EPS
2. MU 4.088 — B+ Val, A- Gro, B+ Pro, A Mom, B EPS
3. NESR 4.025 — B Val, B Gro, A- Pro, A- Mom, B+ EPS

### Two Types of "0 Metrics" Failures
1. **Foreign ADRs/OTC** (expected, no fix): GMTLF, ARREF, LUNMF, SBSW, CGAU, KBCSY, BYDDY, etc.
2. **US stocks with RIC conversion bug** (fixable): B, GM, W, CLS, NEM, GILD, FCX, ALL, CVX, CMI, CIEN, CDE, KGC, TSM, DELL, etc.
   - These are major US stocks that LSEG covers — the LSEG client isn't resolving their RICs correctly
   - Fixing this could bring ~50+ more stocks into proper scoring

### Predictor Candidates That DID Get Scored
- VIAV: 3.312 (C+ Val, B Gro, B- Pro, B- Mom, B+ EPS)
- LITE: 3.288 (C- Val, A- Gro, C Pro, A- Mom, B- EPS)
- CENX: 3.275 (B- Val, C+ Gro, B+ Pro, B Mom, B- EPS)
- AMKR: 3.037 (B Val, C Gro, C+ Pro, B- Mom, B EPS)
- LASR: 2.675 (C- Val, C+ Gro, D Pro, B+ Mom, B- EPS)
- FLNC: 2.712 (D+ Val, C- Gro, D+ Pro, A Mom, B- EPS)
- CIEN: 2.500 (no LSEG data — RIC bug)
- COHR: 2.837 (only 1 metric returned)

### Key Takeaway
The model CAN differentiate stocks when it has data (NUTX, MU, NESR scored well with good reason), but the LSEG data coverage must be fixed before the model is useful at scale. Priority #1 tomorrow.

## What to Focus On Tomorrow

### Priority 1: Fix LSEG Data Coverage (Critical)
- ~260 of 340 stocks got 0 metrics from LSEG — model is useless without data
- Root cause: LSEG RIC conversion failing for many US stocks (B, GM, W, CLS, NEM, GILD, etc.)
- Need to examine lseg_client.py ticker-to-RIC mapping logic
- Common issues: single-letter tickers (B, W), tickers that differ from RIC (CLS→CLS.N), exchange suffix (.N vs .O)
- Also had ReadTimeout errors for some symbols — may need retry logic or longer timeout
- After fix, re-run scoring to see how many stocks get proper ratings
- Goal: get 150+ stocks with full fundamental data (currently ~80)

### Priority 2: Refine the Predictor
- Integrate IV/premium as a scoring factor (favor low-premium candidates)
- Consider adding the quant model's composite score as a predictor input
- Strong Buy duration: user will look up on SA for top candidates
- Cross-reference predictor top picks with quant model Strong Buys

### Priority 3: Pre-Position for Feb 1 Announcement
- Feb 1 is the next Alpha Picks announcement date
- Review final candidate list with both predictor + quant model signals
- Consider buying ATM 30d calls on top 5-10 candidates
- Budget: ~$500-1000 per position (based on historical cost per contract)

### Priority 4: Quant Model Improvements
- LSEG NYSE RIC conversion fix (still pending from prior session)
- EPS Revisions factor (Phase 2 — needs LSEG I/B/E/S access)
- Backtest validator: compare model ratings vs 90 known Alpha Picks historically
- Weight optimization grid search

### Lower Priority
- Analyze whether announcement day pop size correlates with any predictor factors
- Test whether combining predictor score + low IV produces a better filter
- Consider adding earnings date proximity as a factor (earnings near pick date = higher IV = worse options returns)
- Expand universe beyond SA Strong Buys to full market

## File Inventory

### Scripts
- `options_on_picks.py` — Options backtest on Alpha Picks (ATM/OTM, multiple DTEs, stop-loss, IV analysis)
- `alpha_picks_analysis.py` — Historical Alpha Picks profiling (momentum, sector, mcap, timing)
- `alpha_picks_predictor.py` — Predict next Alpha Pick from SA Strong Buy universe
- `quant_model/run_scoring.py` — Run the multi-factor quant scoring model
- `quant_model/config.py` — Model configuration (weights, thresholds, API paths)
- `Massive backtesting/condor_real_data.py` — Real-data iron condor backtest (Polygon API, 2016-2025)
- `backtest/ibkr_condor_paper.py` — IBKR paper trading for SPY iron condors (live)
- `backtest/ibkr_condor_test.py` — Quick SPY condor quote test on IBKR paper

### Data Files
- `ProQuant History 1_29_2026.xlsx` — Source data (90 Alpha Picks, ProQuant history)
- `sa_strong_buys.csv` (in quant_model/data/) — ~340 current SA Strong Buy stocks
- `options_picks_cache.db` — Cached options contracts and bars from Polygon
- `alpha_picks_analysis.db` — Cached ticker details and daily prices
- `price_cache.db` — Shared price cache
- `quant_model/quant_scoring.db` — Quant model scores and fundamentals
- `backtest/condor_paper.db` — Paper trading positions and daily logs (created on first run)
