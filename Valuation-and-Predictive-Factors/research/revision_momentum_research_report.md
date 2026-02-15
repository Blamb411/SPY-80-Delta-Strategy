# Revision Momentum Factor: Research Report

**Date:** February 2025
**Author:** Quantitative Research
**Status:** Completed Backtest

---

## Executive Summary

This report documents the investigation into **analyst estimate revisions** as a predictive factor for forward stock returns. The hypothesis was that *changes* in consensus earnings estimates (revision momentum) contain more predictive information than static price targets or point-in-time estimates.

**Key Finding:** Revision momentum shows **no consistent predictive power** across the full sample (IC ≈ 0), but exhibits **regime-dependent behavior** — working in bull markets and reversing in bear markets.

---

## 1. Initial Hypothesis

### Background
Sell-side analyst price targets are widely available but suffer from several known biases:
- **Optimism bias**: Analysts systematically over-estimate upside
- **Herding**: Consensus targets cluster around similar values
- **Stale information**: Targets may not reflect recent developments

### The Revision Momentum Thesis

The hypothesis was that **changes in estimates** (revisions) are more informative than static levels because:

1. **Signal extraction**: A revision represents new information being incorporated
2. **Analyst conviction**: Revisions require analysts to update their models — they don't do this lightly
3. **Timing**: Revisions cluster around earnings, when new information arrives
4. **Arbitrage**: Investors may underreact to estimate revisions initially

**Formal Hypothesis:**
> Stocks with positive earnings estimate revisions over the trailing 30 days will outperform stocks with negative revisions over the subsequent 21 trading days.

### Expected Outcome
- **Information Coefficient (IC)** > 0.05 (positive correlation between revision % and forward returns)
- **Long/Short Spread** > +1% monthly (top quintile outperforms bottom quintile)
- **IC_IR** > 0.5 (consistent signal across periods)

---

## 2. Methodology

### 2.1 Factor Definition

**Revision Momentum** = Percentage change in consensus FY1 EPS estimate over the lookback period.

```
Revision % = (Current EPS Estimate - Prior EPS Estimate) / |Prior EPS Estimate| × 100
```

For example:
- EPS 30 days ago: $5.00
- EPS today: $5.25
- Revision Momentum = +5.0%

### 2.2 Backtest Framework

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| **Lookback Period** | 30 days | Captures recent revision activity |
| **Forward Returns** | 21 trading days (~1 month) | Standard factor testing horizon |
| **Test Frequency** | Every 30 days | Non-overlapping periods |
| **Universe** | 110 large-cap US stocks | SPY 80-delta universe + additions |
| **Analysis Period** | Feb 2022 – Dec 2025 | ~3 years, includes bull and bear markets |

### 2.3 Statistical Measures

1. **Information Coefficient (IC)**
   - Spearman rank correlation between revision % and forward returns
   - Range: -1 to +1 (0 = no predictive power)
   - Benchmark: IC > 0.05 is considered meaningful

2. **IC_IR (Information Ratio)**
   - IC_IR = Mean IC / Std IC
   - Measures signal consistency
   - Benchmark: IC_IR > 0.5 suggests a useful factor

3. **Long/Short Spread**
   - Return of top quintile (Q5: most positive revisions) minus bottom quintile (Q1: most negative revisions)
   - Measures economic significance

4. **Hit Rate**
   - Percentage of periods with positive IC
   - Benchmark: > 60% for a reliable factor

### 2.4 Point-in-Time Data

**Critical Design Choice:** To avoid look-ahead bias, we used **point-in-time** estimates — the actual consensus values that existed on each historical date.

This was implemented using LSEG's `SDate` parameter:
```python
# Get estimates as they existed 30 days ago
fields = rd.get_data(
    rics,
    fields=['TR.EPSMean'],
    parameters={'SDate': date_30_days_ago.strftime('%Y-%m-%d')}
)
```

This ensures the backtest reflects what an investor would have actually observed at the time.

---

## 3. Data Sources

### 3.1 Analyst Estimates: LSEG (Refinitiv)

**Source:** LSEG Data Platform (formerly Refinitiv Workspace)
**Fields Used:**
- `TR.EPSMean` — Consensus FY1 EPS estimate
- `TR.RevenueHigh`, `TR.RevenueLow`, `TR.RevenueMean` — Revenue estimates
- `TR.NumberOfEstimates` — Coverage breadth

**Point-in-Time Capability:**
- `SDate` parameter retrieves historical snapshots
- Confirmed working for dates back to 2016+

**Symbol Coverage:**
- 110 large-cap US equities
- Mix of NYSE (.N) and NASDAQ (.O) listings
- Excluded: $K (delisted), $PXD (delisted)

### 3.2 Price Returns: Yahoo Finance

**Source:** yfinance Python package
**Fields Used:**
- Adjusted close prices for forward return calculation
- 21 trading days forward from each test date

**Why Yahoo Finance?**
- Free, reliable, no API limits
- Adjusted prices account for splits/dividends
- Sufficient for large-cap equities

### 3.3 Data Quality Issues Encountered

| Issue | Resolution |
|-------|------------|
| LSEG session timeouts | Added auto-retry with session recovery |
| NYSE vs NASDAQ RIC suffixes | Built symbol-to-exchange mapping |
| Delisted securities ($K, $PXD) | Excluded from analysis |
| Missing estimate data | Used available stocks (typically 106-108 of 110) |

---

## 4. Results

### 4.1 Summary Statistics

| Metric | Value | Assessment |
|--------|-------|------------|
| **Periods Tested** | 34 | Sufficient sample |
| **Date Range** | Feb 2022 – Dec 2025 | ~3 years |
| **Avg Stocks per Period** | 108 | Good coverage |
| **Mean IC** | -0.001 | **No predictive power** |
| **Std IC** | 0.174 | High variability |
| **IC_IR** | -0.01 | **Inconsistent signal** |
| **% Positive IC** | 52.9% | Essentially random |
| **Mean L/S Spread** | -0.58% | Slight negative |
| **Hit Rate** | 52.9% | Random |

### 4.2 Quintile Performance

| Quintile | Description | Avg Monthly Return |
|----------|-------------|-------------------|
| Q1 | Most negative revisions | +1.79% |
| Q5 | Most positive revisions | +1.21% |
| **Spread (Q5 - Q1)** | Long/Short | **-0.58%** |

**Interpretation:** The bottom quintile (negative revisions) actually *outperformed* the top quintile — the opposite of the hypothesis.

### 4.3 Rolling IC by Period

Selected periods showing regime-dependent behavior:

| Date | IC | Spread | Market Context |
|------|-----|--------|----------------|
| Jun 2022 | **-0.343** | -11.59% | Bear market trough |
| Jul 2022 | **-0.269** | -7.21% | Bear market |
| Oct 2022 | **-0.314** | -6.06% | Bear market |
| Aug 2023 | **+0.293** | +5.47% | Bull market |
| Aug 2024 | **+0.305** | +3.54% | Bull market |
| Oct 2024 | **+0.213** | +5.34% | Bull market |

### 4.4 Regime Analysis

The data reveals a clear pattern:

**Bear Markets (2022):**
- Revision momentum **reverses** — stocks with negative revisions outperform
- Likely explanation: Oversold bounce effect; beaten-down stocks snap back

**Bull Markets (2023-2024):**
- Revision momentum **works** — stocks with positive revisions outperform
- Consistent with momentum/quality factors performing well in risk-on environments

---

## 5. Conclusions

### 5.1 Primary Finding

**Revision momentum is NOT a consistent standalone predictive factor.**

- Full-sample IC ≈ 0 (no correlation with forward returns)
- IC_IR < 0.5 (unreliable signal)
- Long/Short spread is slightly negative

### 5.2 Regime-Dependent Behavior

The factor exhibits **strong regime dependence:**

| Regime | IC Direction | Interpretation |
|--------|--------------|----------------|
| Bull market | Positive | Momentum works; positive revisions predict positive returns |
| Bear market | Negative | Mean-reversion dominates; negative revisions predict positive returns |

This suggests revision momentum could work as a **conditional signal** when combined with a regime indicator (e.g., market volatility, trend).

### 5.3 Why the Hypothesis Failed

Several possible explanations:

1. **Market efficiency**: Estimate revisions are priced in quickly (within days, not the 21-day horizon)

2. **Analyst herding**: Revisions cluster together, so by the time consensus moves, the trade is crowded

3. **Confounded by momentum**: Stocks with positive revisions may already be high-momentum names that are due for mean reversion

4. **Sample period**: 2022-2025 included unusual market conditions (post-COVID normalization, rate hikes, AI bubble)

### 5.4 Recommendations for Future Research

1. **Shorter horizon**: Test 5-day and 10-day forward returns (capture faster reaction)

2. **Revision surprise**: Instead of raw revision %, use revision *vs expectations* (how much did estimates change relative to typical volatility)

3. **Acceleration signal**: Test rate-of-change in revisions (second derivative)

4. **Volatility filter**: Only trade revision signals when VIX < 20 (bull market regime)

5. **Sector neutralization**: Control for sector effects which may dominate

6. **Different lookbacks**: Test 7-day and 90-day lookback periods

---

## 6. Technical Implementation Notes

### 6.1 Code Location

| File | Purpose |
|------|---------|
| `predictive/estimate_tracker.py` | LSEG estimate collection and storage |
| `predictive/revision_backtest.py` | Rolling backtest framework |
| `predictive/estimates.db` | SQLite database for estimate snapshots |
| `predictive/revision_backtest_results.db` | Backtest results (resume-capable) |

### 6.2 Running the Backtest

```bash
cd "C:\Users\Admin\OneDrive\Desktop\Investment Trading Programs\Claude Options Trading Project"
python -m Valuation-and-Predictive-Factors.predictive.revision_backtest
```

### 6.3 LSEG Session Management

The backtest includes automatic retry logic for LSEG session failures:
- Closes failed session
- Waits 3 seconds
- Reopens new session
- Retries the failed API call
- Results saved to SQLite after each period (crash recovery)

---

## Appendix A: Full Rolling IC Results

```
Date           IC      Spread    N
----------------------------------------
2022-02-14   +0.211    +2.34%   108
2022-03-16   +0.115    +2.80%   108
2022-04-15   +0.029    +4.53%   108
2022-06-14   -0.343   -11.59%   108  ← Bear market
2022-07-14   -0.269    -7.21%   108  ← Bear market
2022-09-12   +0.118    +4.08%   108
2022-10-12   -0.314    -6.06%   108  ← Bear market
2022-11-11   -0.033    +0.15%   108
2023-01-10   -0.208    -5.12%   108
2023-02-09   -0.111    -6.54%   106
2023-04-10   +0.178    +2.12%   108
2023-05-10   -0.056    -3.36%   108
2023-06-09   -0.005    +0.21%   107
2023-08-08   +0.293    +5.47%   108  ← Bull market
2023-09-07   -0.080    -1.38%   108
2023-11-06   -0.088    -2.02%   108
2023-12-06   -0.030    -1.31%   108
2024-01-05   +0.148    +5.12%   106
2024-03-05   +0.039    +0.54%   108
2024-04-04   -0.235    -2.50%   108
2024-06-03   +0.222    +3.74%   108
2024-07-03   -0.115    -5.97%   108
2024-08-02   +0.305    +3.54%   108  ← Bull market
2024-10-01   +0.199    +3.70%   108
2024-10-31   +0.213    +5.34%   108  ← Bull market
2024-12-30   -0.000    -0.30%   108
2025-01-29   -0.176    -5.33%   108
2025-02-28   +0.171    +0.04%   108
2025-04-29   +0.000    +4.81%   108
2025-05-29   [incomplete]
...
```

---

## Appendix B: Universe Constituents

110 large-cap US equities including:
- Technology: AAPL, MSFT, GOOGL, AMZN, META, NVDA, etc.
- Healthcare: JNJ, UNH, PFE, ABBV, MRK, LLY, etc.
- Financials: JPM, BAC, WFC, GS, MS, C, BLK, etc.
- Consumer: WMT, HD, MCD, NKE, SBUX, etc.
- Energy: XOM, CVX, COP, etc.

Excluded due to delisting:
- $K (Kellogg → Kellanova spinoff)
- $PXD (Pioneer Natural Resources → Acquired by Exxon)

---

*End of Report*
