# Claude Session Summary: SPY 80-Delta Call Strategy Project

**Date:** February 15, 2026
**Session:** Comprehensive strategy research, backtesting, and article preparation
**Model Used:** Claude Opus 4.5

---

## Project Overview

This project develops and validates a systematic options strategy that uses deep in-the-money (80-delta) SPY call options with a 200-day SMA trend filter to achieve leverage-like returns with reduced drawdowns compared to leveraged ETFs.

---

## Key Findings Summary

### 1. Strategy Performance (20-Year Backtest: 2005-2025)

| Strategy | End Value ($100K) | CAGR | Sharpe | Max DD |
|----------|-------------------|------|--------|--------|
| SPY B&H | $847,940 | +10.7% | 0.63 | -55.2% |
| SSO (2x ETF) | $1,620,722 | +15.3% | 0.56 | -84.7% |
| UPRO (3x ETF) | $10,539,342 | +32.5% | 0.81 | -76.8% |
| **80-Delta Calls** | **$3,045,440** | **+17.7%** | **0.66** | **-49.3%** |
| 70-Delta Calls | $2,294,605 | +16.1% | 0.67 | -43.2% |

### 2. Critical Discovery: Up/Down Capture Ratio

| Strategy | Upside Capture | Downside Exposure | Capture Ratio |
|----------|----------------|-------------------|---------------|
| 70-Delta | 1.64x SPY | 1.38x SPY | **1.19** (best) |
| 80-Delta | 1.87x SPY | 1.66x SPY | **1.12** |
| SSO | 1.96x SPY | 2.08x SPY | 0.94 |
| UPRO | 2.95x SPY | 3.16x SPY | 0.93 |

**Key Insight:** Delta strategies have capture ratios >1.0 (gain more per unit of downside); leveraged ETFs have ratios <1.0.

### 3. SMA Filter Effect

- **For stocks/ETFs:** SMA filter HURTS returns by 6-13% CAGR
- **For options:** SMA filter HELPS returns by +16.5% CAGR
- **Reason:** Options expire worthless in downtrends; stocks can wait for recovery

### 4. Market Environment Analysis (199 months)

- Delta strategies show near-zero correlation with SPY in down markets
- UPRO has negative mean return in flat markets (volatility decay)
- The alpha comes from losing less in down markets, not gaining more in up markets

---

## File Locations

### Primary Research Documents

| File | Location | Description |
|------|----------|-------------|
| **Main Research Report** | `Valuation-and-Predictive-Factors/research/spy_80delta_strategy_research_report.md` | Comprehensive technical report with all findings |
| **Strategy Explanation** | `Strategies/80-Delta Call Strategy/STRATEGY_EXPLANATION.md` | Complete implementation guide (977 lines) |
| **Seeking Alpha Draft** | `Valuation-and-Predictive-Factors/research/seeking_alpha_article_draft.md` | ~3,000 word article ready for submission |

### Core Backtest Scripts

| File | Location | Description |
|------|----------|-------------|
| `delta_capped_backtest.py` | `Strategies/80-Delta Call Strategy/` | Main SPY strategy backtest engine |
| `qqq_delta_capped_backtest.py` | `Strategies/80-Delta Call Strategy/` | QQQ version of the strategy |
| `delta_comparison_analysis.py` | `Strategies/80-Delta Call Strategy/` | Compares all delta levels (50-95) |
| `leverage_analysis.py` | `Strategies/80-Delta Call Strategy/` | Finds equivalent SPY leverage |
| `market_environment_analysis.py` | `Strategies/80-Delta Call Strategy/` | Monthly/rolling returns by market condition |

### SMA and Filter Analysis Scripts

| File | Location | Description |
|------|----------|-------------|
| `sma_filter_comparison.py` | `Strategies/80-Delta Call Strategy/` | Compares SPY/SSO/UPRO with/without SMA |
| `sma_ablation_study.py` | `Strategies/80-Delta Call Strategy/` | Tests SMA effect on ETFs |
| `delta_80_no_sma_test.py` | `Strategies/80-Delta Call Strategy/` | Tests 80-Delta with/without SMA |
| `sma_period_comparison.py` | `Strategies/80-Delta Call Strategy/` | Compares SMA50/100/150/200 |

### Rejected Enhancement Tests

| File | Location | Description |
|------|----------|-------------|
| `rsi_filter_test.py` | `Strategies/80-Delta Call Strategy/` | RSI filter (rejected) |
| `puts_below_sma_test.py` | `Strategies/80-Delta Call Strategy/` | Puts in downtrends (rejected) |
| `tail_hedge_test.py` | `Strategies/80-Delta Call Strategy/` | 5-delta put hedging (rejected) |
| `weekly_vs_monthly_test.py` | `Strategies/80-Delta Call Strategy/` | Weekly options (rejected - liquidity) |

### Monitoring and Execution

| File | Location | Description |
|------|----------|-------------|
| `monitor_positions.py` | `Strategies/80-Delta Call Strategy/` | Track current positions with IBKR |
| `ibkr_option_quotes.py` | `Strategies/80-Delta Call Strategy/` | Fetch live quotes from IBKR |

### Data Files

| File | Location | Description |
|------|----------|-------------|
| `delta_comparison_results.csv` | `Strategies/80-Delta Call Strategy/` | Full backtest results all deltas |
| `daily_values_*.csv` | `Strategies/80-Delta Call Strategy/` | Daily portfolio values per strategy |
| `monthly_returns_all_strategies.csv` | `Strategies/80-Delta Call Strategy/` | Monthly returns (gitignored) |
| `rolling_12m_returns_all_strategies.csv` | `Strategies/80-Delta Call Strategy/` | Rolling 12-month returns (gitignored) |

### Charts

| File | Location | Description |
|------|----------|-------------|
| `delta_comparison_chart.png` | `Strategies/80-Delta Call Strategy/` | All delta levels equity curves |
| `comparison_from_2009.png` | `Strategies/80-Delta Call Strategy/` | Fair comparison from UPRO inception |
| `leverage_analysis.png` | `Strategies/80-Delta Call Strategy/` | Leverage equivalence chart |
| `sma_ablation_study.png` | `Strategies/80-Delta Call Strategy/` | SMA effect visualization |
| `delta_80_sma_comparison.png` | `Strategies/80-Delta Call Strategy/` | With/without SMA comparison |
| `sma_filter_comparison.png` | `Strategies/80-Delta Call Strategy/` | ETF SMA comparison |

### Infrastructure

| File | Location | Description |
|------|----------|-------------|
| `thetadata_client.py` | `backtest/` | ThetaData API client for historical options |
| `black_scholes.py` | `backtest/` | Options pricing and Greeks |
| `fred_client.py` | `backtest/` | FRED economic data client |

---

## GitHub Repository

**URL:** https://github.com/Blamb411/SPY-80-Delta-Strategy

**Latest commits:**
1. Add Seeking Alpha article draft
2. Add market environment analysis comparing strategies
3. Add academic citations for SMA200 trend filter
4. Improve research report clarity and accuracy
5. Add SMA filter ablation study findings

---

## Strategy Rules Summary

### Entry Rules
1. SPY > SMA200 (trend filter)
2. Monthly expiration ~120 DTE
3. Strike with 70-80 delta
4. Respect delta cap (options delta ≤ share count)

### Exit Rules
1. 50% profit target
2. 60-day maximum hold
3. Exit all if SPY falls 2%+ below SMA200

### Portfolio Structure
- 86% SPY shares (~$600K for 1,000 shares)
- 14% options cash (~$100K)
- Combined portfolio dampens options volatility

---

## Open Items / Potential Future Work

### 1. Seeking Alpha Article
- **Status:** Draft complete (`seeking_alpha_article_draft.md`)
- **Action:** Review, finalize, submit to Seeking Alpha
- **Note:** May need to adjust formatting for SA's editor

### 2. QQQ Strategy Analysis
- **Status:** Basic backtest exists (`qqq_delta_capped_backtest.py`)
- **Potential:** Could add QQQ results to research report
- **Finding from STRATEGY_EXPLANATION.md:** QQQ viable but higher concentration risk

### 3. Live Trading Implementation
- **Status:** Monitoring scripts exist (`monitor_positions.py`)
- **IBKR Integration:** Basic connection working
- **Potential:** Could build automated execution system

### 4. Combined Portfolio Analysis
- **Status:** Discussed but not fully formalized
- **Potential:** Create explicit combined portfolio backtest showing the 86/14 split results

### 5. Out-of-Sample Testing
- **Status:** Not done
- **Potential:** Run strategy on data not used for parameter selection

### 6. Real-Time Alerts
- **Status:** Basic daily check exists
- **Potential:** Build SMS/email alerts for SMA breaches

---

## Key Parameters (For Reference)

```
DELTA_TARGET = 0.80 (range: 0.70-0.80)
DTE_TARGET = 120 days
DTE_MIN = 90 days
DTE_MAX = 150 days
MAX_HOLD = 60 trading days
PROFIT_TARGET = 0.50 (50%)
SMA_PERIOD = 200 days
SMA_EXIT_THRESHOLD = 0.02 (2% below SMA)
RISK_FREE_RATE = 0.04
```

---

## Academic References Added to Report

1. **Faber (2007)** — "A Quantitative Approach to Tactical Asset Allocation," *Journal of Wealth Management*
2. **Moskowitz, Ooi & Pedersen (2012)** — "Time Series Momentum," *Journal of Financial Economics*
3. **Hurst, Ooi & Pedersen (2017)** — "A Century of Evidence on Trend-Following Investing," AQR Capital
4. **Clare et al. (2016)** — "Trend Following, Risk Parity and Momentum," *International Review of Financial Analysis*

---

## Bugs Fixed This Session

1. **Delta calculation:** Changed from `math.erf()` approximation to exact `scipy.stats.norm.cdf()` in `delta_capped_backtest.py`

2. **SSO/UPRO comparison:** Fixed signal comparison to use SPY prices (not SSO/UPRO prices) for SMA calculation in `sma_filter_comparison.py`

---

## Notes for Next Session

1. **Claude Version:** User wants to upgrade to Claude Opus 4.6 (released Feb 5, 2026). May need to run `claude update` and `claude --model claude-opus-4-6`.

2. **Research Report Location:** The main comprehensive report is at:
   `Valuation-and-Predictive-Factors/research/spy_80delta_strategy_research_report.md`

3. **Two Research Documents Exist:**
   - `spy_80delta_strategy_research_report.md` — Technical research with delta comparison, leverage analysis, SMA ablation
   - `STRATEGY_EXPLANATION.md` — Implementation guide with rejected enhancements, parameter testing
   - Recommendation was to keep them separate (different purposes)

4. **Seeking Alpha Article:** Ready for review at:
   `Valuation-and-Predictive-Factors/research/seeking_alpha_article_draft.md`

5. **Key insight to remember:** The strategy outperforms by **losing less in down markets** (capture ratio >1.0), not by gaining more in up markets.

---

## Commands to Resume Work

```bash
# Navigate to project
cd "C:\Users\Admin\OneDrive\Desktop\Investment Trading Programs\Claude Options Trading Project"

# Check git status
git status

# Run market environment analysis
python "Strategies/80-Delta Call Strategy/market_environment_analysis.py"

# Run main backtest
python "Strategies/80-Delta Call Strategy/delta_capped_backtest.py"

# View research report
cat "Valuation-and-Predictive-Factors/research/spy_80delta_strategy_research_report.md"
```

---

*End of Session Summary*
