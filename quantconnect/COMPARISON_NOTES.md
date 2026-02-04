# QuantConnect vs Black-Scholes Backtest Comparison

## Purpose

Compare the put credit spread strategy results using:
1. **Black-Scholes theoretical pricing** (your original backtest)
2. **Real historical bid/ask data** (QuantConnect)

---

## Original Black-Scholes Backtest Results

**Source:** `backtest/results/BACKTESTING_SUMMARY.json`

### Period & Scope
- Trading Period: Nov 2023 - Dec 2025 (~2 years)
- Symbols: 191 stocks from Magic Formula screening
- Pricing: Black-Scholes with volatility skew
- Execution: Conservative (sell at bid, buy at ask)

### Put Credit Spread Results by Bid/Ask Spread Assumption

| Bid/Ask Spread | Trades | Win Rate | Total P&L | Avg P&L | Status |
|----------------|--------|----------|-----------|---------|--------|
| 1% | 6,008 | 85.0% | +$106,071 | +$17.65 | **PROFITABLE** |
| 2% | 6,008 | 84.6% | +$75,384 | +$12.55 | **PROFITABLE** |
| 3% | 6,008 | 84.2% | +$37,327 | +$6.21 | MARGINAL |
| 5% | 6,008 | 83.5% | -$39,369 | -$6.55 | LOSING |

### Key Metrics (1% Spread)
- Max Drawdown: 9.8%
- Annualized ROI: 6.7%
- Average Win: $150.85
- Average Loss: -$800.52

---

## QuantConnect Simple Test Results (2023)

**Source:** Your backtest run on 2026-01-28

### Period & Scope
- Period: Jan 2023 - Dec 2023 (1 year)
- Symbol: SPY only
- Pricing: Real OPRA bid/ask data
- Execution: Market orders at bid/ask

### Results

| Metric | Value |
|--------|-------|
| Starting Capital | $50,000 |
| Final Value | $67,583.50 |
| Return | 35.17% |
| CAGR | 35.4% |
| Sharpe Ratio | 1.5 |
| Max Drawdown | 15.1% |
| Spreads Entered | 36 |

### Issues Noted
- Several "Insufficient buying power" errors (position sizing)
- Some spreads were too narrow ($2-3 width)
- No take-profit/stop-loss management in simple test

---

## Full Strategy Test (To Run)

**File:** `put_spread_full_strategy.py`

### Configuration (Matching Original Backtest)
- Period: 2020-2024 (5 years)
- Symbol: SPY (most liquid, tightest spreads)
- 200-day SMA filter
- RSI < 75 filter
- IV Rank > 30% filter
- 25-delta short put (or 5% OTM fallback)
- 5% spread width
- 50% take profit
- 200% credit stop loss
- Max 3 concurrent positions
- 7-day minimum between entries

---

## Expected Comparison Insights

### What Real Data Will Reveal

1. **Actual Bid/Ask Spreads**
   - B-S assumed 1-5% fixed spreads
   - SPY actual spreads are typically 0.5-2%
   - Less liquid names have 5-15% spreads

2. **Greeks Accuracy**
   - B-S calculates theoretical delta
   - Real delta from market makers may differ

3. **Fill Quality**
   - B-S assumes bid/ask fill
   - Real fills may be better (mid) or worse (slippage)

4. **IV Behavior**
   - B-S uses single ATM IV
   - Real skew affects OTM put pricing

### Hypotheses

1. **SPY results should be BETTER than B-S at 1%** because:
   - SPY spreads are tighter than 1%
   - High liquidity means good fills

2. **Win rate should be similar** (80-85%) because:
   - Strategy logic is identical
   - Market conditions are the same

3. **Drawdown patterns should match** because:
   - Same underlying price movements
   - Same entry/exit timing

---

## How to Run the Full Comparison

### Step 1: Run Full Strategy in QuantConnect
1. Copy `put_spread_full_strategy.py` to QuantConnect
2. Run backtest (will take ~10-20 minutes for 5 years)
3. Download results

### Step 2: Compare Key Metrics

| Metric | B-S (1% spread) | QuantConnect | Difference |
|--------|-----------------|--------------|------------|
| Win Rate | 85% | ? | |
| Avg P&L/Trade | $17.65 | ? | |
| Max Drawdown | 9.8% | ? | |
| Sharpe Ratio | - | ? | |

### Step 3: Analyze Differences

If QuantConnect results are:
- **Better**: B-S was too conservative, strategy is robust
- **Similar**: B-S assumptions were accurate
- **Worse**: B-S was too optimistic, need to adjust assumptions

---

## Limitations of Comparison

1. **Different Symbol Universe**
   - B-S: 191 stocks
   - QC: SPY only (for now)
   - Solution: Add more symbols to QC test

2. **Different Periods**
   - B-S: 2023-2025
   - QC Full: 2020-2024
   - Solution: Run QC for matching period

3. **Position Sizing**
   - B-S: Unlimited concurrent trades
   - QC: Limited by margin
   - This is more realistic

4. **Fees**
   - B-S: Not modeled
   - QC: IB fees included (~$1/contract)

---

## Next Steps After Comparison

1. If results validate strategy:
   - Run paper trading for 3-6 months
   - Start with SPY/QQQ only
   - Track actual fills vs theoretical

2. If results diverge significantly:
   - Identify root cause (spreads, fills, IV model)
   - Adjust B-S assumptions
   - Re-run original backtest with corrections
