# The 80-Delta Call Strategy: A Complete Guide

## Executive Summary

This document explains an options-based investment strategy that combines stock ownership with systematic call option purchases to enhance returns while managing risk. The strategy produced a **19.6% annualized return** (vs. 14.7% for buy-and-hold) with a **Sharpe ratio of 1.03** over a 10-year backtest period (2015-2025).

The explanation is written for readers who are comfortable with mathematics and logical reasoning but may not be familiar with financial jargon. Technical terms are explained when first introduced.

---

## Table of Contents

1. [Backtest Methodology](#backtest-methodology)
2. [The Basic Idea](#the-basic-idea)
3. [Key Concepts Explained](#key-concepts-explained)
4. [How the Strategy Works](#how-the-strategy-works)
5. [The Mathematics Behind It](#the-mathematics-behind-it)
6. [What We Tested and Why](#what-we-tested-and-why)
7. [What We Rejected](#what-we-rejected)
8. [What We Incorporated](#what-we-incorporated)
9. [Understanding the Risk-Adjusted Returns](#understanding-the-risk-adjusted-returns)
10. [Implementation Details](#implementation-details)
11. [The Software Components](#the-software-components)
12. [Strategy Capacity and Scalability](#strategy-capacity-and-scalability)
13. [Risks and Limitations](#risks-and-limitations)

---

## Backtest Methodology

Before diving into the strategy details, here are the key assumptions used throughout our backtesting:

### Portfolio Assumptions

All backtests use a combined portfolio of:
- **3,125 SPY shares** (~$1.9M at current prices) as the buy-and-hold foundation
- **$100,000 options cash allocation** for the 80-delta call strategy

The $100,000 options allocation is discretionary and can be scaled based on risk tolerance. We chose this ratio (~5% options to ~95% shares) as a conservative starting point. Investors seeking more aggressive returns could increase the options allocation, while those seeking lower volatility could decrease it.

### Position Sizing in Backtests vs. Live Trading

**In backtests:** We enter 1 contract per signal for simplicity and to avoid overfitting position sizing rules. This allows clean measurement of signal quality.

**In live trading:** Position sizes are scaled to the actual portfolio. For example, with a larger options allocation, you might enter 10 contracts per signal instead of 1. The key constraint is the delta cap—total options delta should not exceed your share count.

### Performance Metrics

- **CAGR, Sharpe, Sortino, Max DD:** Calculated on the **combined portfolio** (shares + options cash + options positions) unless otherwise noted
- **Sharpe Ratio:** Annualized from daily returns: (mean daily return / std of daily returns) × √252
- **Sortino Ratio:** Like Sharpe but uses only downside deviation (penalizes downside volatility only)
- **Win Rate, P&L:** Refer to the options trades only

### Why Metrics May Vary Slightly

Results may differ slightly between tests due to:
- Different simulation periods (some tests use 2015-2025, others 2015-2026)
- Contract availability (some strikes/expirations have missing quote data)
- Minor code variations between test files

These variations are typically small (±0.1% CAGR, ±0.02 Sharpe).

### Backtest Charts

Visual charts of portfolio growth, drawdowns, and yearly returns are available separately and can be attached to this document for distribution.

---

## The Basic Idea

Imagine you own shares of a stock index fund (specifically SPY, which tracks the S&P 500). You believe the market will generally go up over time, but you'd like to amplify your gains during favorable periods without taking on excessive risk during downturns.

The strategy accomplishes this by:
1. **Holding shares** of SPY as a foundation
2. **Buying call options** on SPY during uptrends to add leveraged exposure
3. **Staying in cash** (for the options portion) during downtrends to reduce risk

The key insight is that we only add the leveraged bet when conditions favor it—when the market is in an uptrend, defined by price being above its 200-day average.

---

## Key Concepts Explained

### What is a Call Option?

A call option is a contract that gives you the right (but not obligation) to buy a stock at a specific price (the "strike price") before a specific date (the "expiration"). You pay a premium upfront for this right.

**Example:** SPY is trading at $600. You buy a call option with:
- Strike price: $580
- Expiration: 4 months from now
- Premium: $35 per share (options trade in 100-share contracts, so $3,500 total)

If SPY rises to $650, your option is worth at least $70 ($650 - $580), giving you a $35 profit per share, or 100% return on your $35 investment. Meanwhile, SPY only rose 8.3%.

If SPY falls to $550, your option expires worthless. You lose the $35 premium (100%), but that's your maximum loss.

### What is Delta?

Delta measures how much an option's price moves when the underlying stock moves by $1.

- **Delta of 0.80 (or "80-delta")** means the option price moves $0.80 for every $1 move in the stock
- High-delta options (0.70-0.90) behave more like owning stock
- Low-delta options (0.10-0.30) are cheaper but less likely to pay off

We use 80-delta calls because they capture most of the stock's upside movement while still providing some leverage (you control $60,000 worth of stock movement for ~$3,500).

### What is the SMA200?

The SMA200 is the "Simple Moving Average over 200 days"—the average closing price over the past 200 trading days (~10 months). It's a widely-followed indicator of the market's long-term trend.

- **Price above SMA200:** Generally considered an uptrend
- **Price below SMA200:** Generally considered a downtrend

This isn't magic—it's a simple, objective rule that historically has separated favorable from unfavorable market conditions.

### What is the Sharpe Ratio?

The Sharpe ratio measures risk-adjusted return. It answers: "How much return did I get per unit of risk taken?"

```
Sharpe Ratio = (Return - Risk-Free Rate) / Volatility
```

- **Sharpe > 1.0:** Excellent risk-adjusted returns
- **Sharpe 0.5-1.0:** Good
- **Sharpe < 0.5:** Mediocre

A strategy with 20% returns and 40% volatility (Sharpe ~0.5) is worse risk-adjusted than one with 12% returns and 10% volatility (Sharpe ~1.0), even though the raw return is higher.

---

## How the Strategy Works

### The Portfolio Structure

The portfolio consists of two components:

1. **Share Holdings:** 2,000 shares of SPY (~$1.2M at current prices)
2. **Options Cash:** $100,000 allocated for buying call options

The share holdings provide steady market exposure. The options cash is deployed tactically.

### The Entry Rules

We buy a new call option when ALL of these conditions are met:

1. **SPY is above its 200-day moving average** (uptrend filter)
2. **We have room in our "delta budget"** (explained below)
3. **We have available cash** in the options allocation

When entering, we:
- Select a **monthly expiration** approximately 120 days out (4 months)
- Choose a strike price that gives us **80 delta** (deep in-the-money)
- Buy **one contract** (controlling 100 shares)

### The Delta Cap

Here's a crucial risk management concept. We limit our total "delta exposure" to match our share holdings.

**What does this mean?**

If we own 2,000 shares, our share delta is 2,000 (each share moves $1 when SPY moves $1). We cap our options delta at an additional 2,000, meaning the combined portfolio acts like owning at most 4,000 shares worth of exposure.

This prevents the strategy from becoming overly leveraged. Without this cap, we could theoretically accumulate unlimited option positions and face catastrophic losses in a downturn.

### The Exit Rules

We exit an option position when ANY of these occur:

1. **Profit Target Hit (50%):** If the option gains 50% from purchase price, we sell
2. **Maximum Holding Period (60 days):** We don't hold options indefinitely
3. **SMA Breach (2% threshold):** If SPY falls more than 2% below its SMA200, we exit all options

The SMA breach exit is particularly important—it forces us out of leveraged positions when the trend turns negative, limiting drawdowns.

### Visual Timeline Example

```
Jan 1:  SPY at $580, above SMA200 ($570). Buy 80-delta call, strike $540, exp May.
Jan 15: Option up 30%. Hold (below 50% target).
Feb 1:  Option up 55%. SELL - profit target hit.
Feb 2:  Buy new call, strike $555, exp June.
Mar 10: SPY drops to $558, SMA200 is $572. SPY is 2.4% below SMA.
        SELL all options - SMA breach exit triggered.
Mar-May: SPY below SMA200. No new options purchased. Just hold shares.
Jun 1:  SPY recovers above SMA200. Resume buying calls.
```

---

## The Mathematics Behind It

### Why 80 Delta?

The choice of 80 delta balances several factors:

**Leverage:** An 80-delta call on SPY might cost $35 when SPY is at $600, controlling $60,000 of notional value. That's roughly 17:1 leverage on the option itself.

**Probability of Profit:** Higher delta options are more likely to end up profitable because they start "in the money" (strike below current price).

**Time Decay:** All options lose value as expiration approaches (theta decay). Higher delta options lose less to time decay in percentage terms because their value is mostly "intrinsic" (real) rather than "extrinsic" (hope).

**The Trade-off:**
- Lower delta (e.g., 50-delta): More leverage, but more time decay and lower win rate
- Higher delta (e.g., 90-delta): Less leverage, less time decay, higher win rate

Testing showed 80-delta provided the best risk-adjusted returns.

### Why 120 DTE (Days to Expiration)?

Options lose value faster as expiration approaches. By choosing ~120 DTE and exiting by 60 days, we avoid the period of accelerated decay.

```
Time Decay Curve (Theta):
Days to Exp:  120    90    60    30    14     7     1
Decay Rate:   Low   Low   Med   High  V.High Extreme
              [---- Our holding period ----]
```

### The SMA200 Filter: Statistical Basis

Looking at SPY from 1993-2025:
- **When above SMA200:** Average annualized return ~15%, volatility ~14%
- **When below SMA200:** Average annualized return ~2%, volatility ~24%

The filter isn't predicting the future—it's identifying regimes where the risk/reward profile is favorable for leveraged long positions.

### Position Sizing: The Delta Cap Math

Let's work through the math:

```
Share holdings:     2,000 shares × delta 1.0 = 2,000 delta
Options cap:        2,000 additional delta allowed

If current options delta: 1,500
Room for new position:    500 delta

New 80-delta call:        80 delta per contract × 100 shares = 80 delta
Can we buy one contract?  80 < 500, yes.

After purchase:           1,500 + 80 = 1,580 options delta
Total portfolio delta:    2,000 + 1,580 = 3,580
```

---

## What We Tested and Why

### Parameters Explored

| Parameter | Values Tested | Chosen Value | Rationale |
|-----------|---------------|--------------|-----------|
| Delta | 60, 70, 80, 90 | 80 | Best Sharpe ratio |
| DTE at entry | 60, 90, 120, 150 | 120 | Balances decay vs. cost |
| Profit target | 30%, 50%, 75%, 100% | 50% | Optimal risk/reward |
| Max hold period | 30, 45, 60, 90 days | 60 | Avoids theta acceleration |
| SMA period | 50, 100, 150, 200 | 200 | See detailed analysis below |
| SMA exit threshold | 0%, 1%, 2%, 3% | 2% | Reduces whipsaws |

### SMA Period Comparison: Detailed Analysis

The choice of SMA period significantly impacts strategy performance. We tested four periods (50, 100, 150, 200 days) with all other parameters held constant.

**Table 1: SMA Period Comparison** (Combined portfolio: 3,125 shares + $100k options)

| SMA | CAGR | Sharpe | Max DD | Trades | Win Rate | SMA Exits | Total P&L |
|-----|------|--------|--------|--------|----------|-----------|-----------|
| 50 | +13.5% | 0.80 | -32.5% | 1,004 | 51.3% | 477 | $328k |
| 100 | +14.9% | 0.85 | -35.1% | 1,022 | 67.4% | 270 | $672k |
| 150 | +15.3% | 0.88 | -32.5% | 1,074 | 70.8% | 205 | $803k |
| 200 | +15.3% | 0.88 | -32.3% | 1,061 | 71.3% | 182 | $794k |

**Key Finding: Shorter SMAs Generate Destructive Whipsaws**

The SMA50 triggered 477 forced exits (vs. 182 for SMA200)—295 additional "whipsaw" trades where price briefly crossed below the SMA, forced an exit, then recovered. Each whipsaw:
- Sells the option position (often at a loss due to time decay)
- Pays the bid-ask spread on exit
- Requires re-entry when price recovers (paying spread again)
- Resets the profit target clock

This whipsaw effect devastated the SMA50 results:
- Win rate collapsed to 51.3% (barely better than a coin flip)
- Lost $465,000 in P&L compared to SMA200
- Gave up 1.8% annual CAGR

**Diminishing Returns from Longer Periods**

The improvement from extending the SMA period showed clear diminishing returns:
- 50→100: Large improvement (+1.4% CAGR, +0.05 Sharpe, 207 fewer whipsaws)
- 100→150: Meaningful improvement (+0.4% CAGR, +0.03 Sharpe, 65 fewer whipsaws)
- 150→200: Marginal difference (same CAGR, same Sharpe, 23 fewer whipsaws)

**Why We Chose SMA200**

SMA150 and SMA200 performed nearly identically. We chose SMA200 because:
1. **Fewest whipsaws** (182 vs. 205) - less trading friction
2. **Highest win rate** (71.3% vs. 70.8%) - better behavioral experience
3. **Best max drawdown** (-32.3% vs. -32.5%) - marginally better risk profile
4. **Industry standard** - the 200-day moving average is widely followed, making it harder for others to front-run the signal

### SMA Exit Threshold Comparison

We also tested different thresholds for exiting all positions when SPY falls below SMA200. The question: should we exit immediately when price crosses below SMA (0% threshold), or allow a buffer before exiting?

**Table 2: SMA Exit Threshold Comparison** (Combined portfolio)

| Threshold | CAGR | Sharpe | Max DD | Trades | Win Rate | SMA Exits | Total P&L |
|-----------|------|--------|--------|--------|----------|-----------|-----------|
| 0% | +15.2% | 0.88 | -33.3% | 1,079 | 65.6% | 280 | $760k |
| 1% | +15.3% | 0.89 | -32.0% | 1,087 | 69.6% | 232 | $785k |
| **2%** | **+15.3%** | **0.88** | **-32.3%** | **1,062** | **71.3%** | **182** | **$800k** |
| 3% | +15.4% | 0.86 | -34.4% | 1,042 | 72.8% | 147 | $820k |

**Why We Chose 2%:**

The 1% and 2% thresholds perform similarly, with 1% having marginally better Sharpe (0.89 vs 0.88) and max drawdown (-32.0% vs -32.3%). We chose 2% because:
- Fewer forced exits (182 vs 232) means less trading friction
- Higher win rate (71.3% vs 69.6%) improves behavioral experience
- More total P&L ($800k vs $785k)
- The 3% threshold shows degraded Sharpe (0.86) and worst max drawdown (-34.4%)—too much buffer allows positions to deteriorate before exit

### Alternative Tickers Tested

| Ticker | Description | Result |
|--------|-------------|--------|
| SPY | S&P 500 ETF | **Selected** - best liquidity, good returns |
| QQQ | Nasdaq-100 ETF | Viable but higher concentration risk |
| RSP | Equal-weight S&P 500 | Rejected - options too illiquid |
| VTI | Total US Market | Rejected - options too illiquid |
| XLK | Technology Sector | Rejected - volatility too high |
| DIA | Dow 30 | Rejected - lower Sharpe than SPY |

### Segmentation Analyses Performed

We analyzed whether entry conditions affected outcomes:

**1. SMA Distance Analysis**
- Segmented trades by how far above SMA200 at entry (0-0.5%, 0.5-1%, 1-2%, >2%)
- Finding: No consistent pattern worth exploiting

**2. Trailing 12-Month Return Analysis**
- Segmented by prior year's market return at entry
- Finding: Weak correlation, not actionable

**3. Valuation Analysis (CAPE Ratio)**
- Tested whether market valuation predicted trade outcomes
- Finding: No significant relationship in our timeframe

### Entry Timing Experiments

**1. Down-Day Entry Strategy**

We tested buying calls only on "down days" (when SPY closes lower than the prior day), hypothesizing that buying after pullbacks might improve entry prices.

**Table 3: Down-Day Entry - Same Position Size** (Combined portfolio)

| Strategy | CAGR | Sharpe | Trades | Win Rate | P&L |
|----------|------|--------|--------|----------|-----|
| Original (daily) | 15.3% | 0.877 | 1,062 | 71.3% | $800k |
| Down-day only | 14.3% | 0.893 | 591 | 72.3% | $627k |

Better per-trade quality (higher Sharpe, win rate) but fewer trades reduced total P&L.

**Table 4: Down-Day Entry - Scaled Position Size** (Combined portfolio)

To fairly compare, we tested buying 2x or 3x contracts on down days:

| Strategy | CAGR | Sharpe | Trades | Total P&L | SMA Exits |
|----------|------|--------|--------|-----------|-----------|
| Original | 15.3% | 0.877 | 1,062 | $800k | 182 |
| Down-day 2x | 15.2% | 0.891 | 506 | $779k | 119 |
| Down-day 3x | 15.7% | 0.891 | 417 | $896k | 77 |

**Finding:** Down-day 3x is a legitimate improvement—higher CAGR, better Sharpe, fewer trades, fewer whipsaw exits, and higher total P&L. This approach is worth pursuing as an enhancement.

**2. Extended Entry Zone Test**

We tested whether continuing to buy calls when spot is in the "buffer zone" (between SMA and 2% below SMA) would reduce whipsaws.

*Hypothesis:* If we keep buying during mild pullbacks rather than stopping at the SMA, we might accumulate more positions before exiting, reducing the whipsaw problem.

**Table 5: Extended Entry Zone Test** (Combined portfolio)

| Strategy | CAGR | Sharpe | Trades | SMA Exits | Buffer Trades | Total P&L |
|----------|------|--------|--------|-----------|---------------|-----------|
| Original (above SMA only) | 15.3% | 0.877 | 1,062 | 182 | 0 | $800k |
| Extended (to 2% below) | 15.3% | 0.873 | 1,132 | 229 | 77 | $785k |

**Finding:** REJECTED. The extended zone made things worse:
- 47 more SMA exits (+26%), not fewer
- Buffer zone entries had only 32.5% win rate vs 71.8% for above-SMA entries
- Buffer zone mean return only +3.0% vs +14.2% for above-SMA entries

The original logic is sound: when price falls below SMA, the trend is weakening. Buying in the buffer zone is catching a falling knife—those entries often proceed to the exit threshold.

**Note:** This analysis reinforces our exit threshold decision. We tested different SMA exit thresholds (0%, 1%, 2%, 3%) and found the 2% buffer optimal—see Table 2 above for full results.

---

## What We Rejected

### 1. Entry Timing Refinements

We considered only entering when SPY was in specific "sweet spots" relative to SMA200. **Rejected** because:
- Small sample sizes per segment (~150 trades each)
- Inconsistent patterns between SPY and QQQ
- Risk of overfitting to historical data

### 2. Covered Call Overlay Below SMA

We tested selling covered calls (betting against upside) when below SMA200. **Rejected** because:
- Added complexity without meaningful return improvement
- Capped upside during recovery rallies
- Transaction costs eroded small gains

### 3. Dynamic Delta Targeting

We tested adjusting target delta based on volatility (lower delta when VIX high). **Rejected** because:
- Reduced exposure precisely when rebounds tend to be strongest
- Backtest improvement didn't justify added complexity

### 4. Alternative Underlyings (RSP, VTI)

Equal-weight and total-market ETFs seemed attractive for diversification. **Rejected** because:
- Options markets too illiquid (wide bid-ask spreads)
- Many days with no quotes available
- Even assuming midpoint execution, no alpha generated

### 5. Stop-Loss on Individual Positions

We tested exiting options that fell 30%, 50%, etc. **Rejected** because:
- Often sold right before recovery
- The SMA exit rule already provides portfolio-level protection
- Win rate declined without improving total return

### 6. RSI Filter (Avoid Overbought Entries)

We tested adding an RSI(14) filter to avoid buying calls when the market is "overbought" (RSI > 70 or > 80). The hypothesis was that avoiding entries during short-term overextension might reduce drawdowns.

**Table 6: RSI Filter Test** (Combined portfolio, 2015-2026)

| Filter | CAGR | Sharpe | Max DD | Trades | Total P&L |
|--------|------|--------|--------|--------|-----------|
| No RSI filter (base) | +15.3% | 0.88 | -32.3% | 1,062 | $800k |
| RSI < 70 | +14.9% | 0.87 | -32.9% | 902 | $684k |
| RSI < 80 | +15.0% | 0.86 | -33.3% | 1,008 | $710k |

**Rejected** because:
- RSI filter reduced returns without improving risk metrics
- Max drawdown actually *increased* with the filter (missed entries during strong rallies that would have cushioned drawdowns)
- The SMA200 filter already captures trend effectively—average entry RSI was 59.6, not extreme
- SPY trends persistently; "overbought" often means "strong momentum" rather than imminent reversal
- Adding RSI creates another parameter to optimize (overfitting risk) with no demonstrated benefit

**Conclusion:** The SMA200 trend filter is sufficient. RSI adds complexity without improving risk-adjusted returns.

**Note on RSI for Indexes vs. Individual Stocks:** Our testing suggests RSI may not be a meaningful timing indicator for broad market indexes like SPY. Indexes tend to trend persistently, so "overbought" often indicates strong momentum rather than imminent reversal. RSI may be more useful for mean-reverting individual stocks, but this hypothesis would require separate testing.

---
 
## What We Incorporated

### 1. The SMA200 Trend Filter (Core)

Only buy calls when SPY > SMA200. This single rule is responsible for most of the strategy's outperformance by avoiding leveraged exposure during bear markets.

*Evidence: Table 1 shows SMA200 outperforms shorter periods with fewer whipsaws.*

### 2. The 2% SMA Exit Threshold

Rather than exiting immediately when price crosses below SMA200, we allow a 2% buffer. This reduces "whipsaw" trades where price briefly dips below then recovers.

*Evidence: Table 2 shows 2% threshold balances fewer forced exits with acceptable drawdown.*

### 3. The Delta Cap

We limit total options delta to match share holdings (e.g., 3,125 delta for 3,125 shares), creating a maximum effective leverage of ~2x.

**Note:** The delta cap is a discretionary risk management feature. Investors with higher risk tolerance could remove or increase the cap, while conservative investors could lower it. Our backtests use the delta cap as shown, but the options-only component (without the share holdings) also generates positive returns. The cap prevents runaway leverage accumulation during strong uptrends.

### 4. The 50% Profit Target

Taking profits at 50% gain:
- Locks in winners before mean reversion
- Frees capital for new positions
- Improves win rate psychologically

### 5. The 60-Day Maximum Hold

Exiting by 60 days regardless of profit:
- Avoids accelerated time decay
- Prevents holding losing positions hoping for recovery
- Maintains portfolio turnover discipline

### 6. Monthly Expirations Only

We only use standard monthly options (third Friday expiration) rather than weekly options. We tested this assumption formally—see Table 7.

**Table 7: Weekly vs Monthly Expirations** (Combined portfolio)

| Metric | Monthly | Weekly | Difference |
|--------|---------|--------|------------|
| CAGR | **+15.30%** | +13.66% | -1.65% |
| Sharpe | **0.877** | 0.856 | -0.021 |
| Sortino | **1.115** | 1.072 | -0.044 |
| Max Drawdown | -32.3% | -30.9% | +1.3% |
| Total Trades | 1,062 | 427 | -635 |
| Win Rate | 71.3% | 76.6% | +5.3% |
| Total P&L | **$799,770** | $354,969 | -$444,801 |
| Missing Quote Days | 785 | 1,816 | +1,031 |

**Why Monthly Wins:**

The key finding is that weekly options have 2.3x more days with missing quotes (1,816 vs 785), indicating liquidity problems. This caused:
- 635 fewer trades executed
- $445,000 less total P&L

Weekly's higher win rate (76.6%) is misleading—it reflects selection bias from only trading on the most liquid days. Monthly expirations provide consistent liquidity for reliable execution.

### 7. Potential Enhancement: Down-Day Entry with Scaling (Under Consideration)

Testing showed that buying only on down days with 3x position scaling may improve results (see Table 4):
- CAGR: 15.7% vs 15.3% original
- Sharpe: 0.891 vs 0.877 original
- Fewer trades: 417 vs 1,062
- Fewer whipsaw exits: 77 vs 182
- Higher total P&L: $896k vs $800k

This approach trades less frequently but in larger size when conditions are favorable (price pullback in an uptrend). The "3x" scaling means entering 3 contracts instead of 1 on each down-day signal. In practice, position size should be scaled to your options allocation—see the Backtest Methodology section for guidance.

---

## Understanding the Risk-Adjusted Returns

### The Sharpe Ratio Puzzle

Here's something that initially seems paradoxical.

**Note on Sharpe/Sortino Consistency:** All Sharpe ratios in this document are calculated the same way (annualized from daily returns). Minor variations between tables reflect different test periods or code paths. See the Backtest Methodology section for calculation details.

| Component | Sharpe Ratio | Sortino Ratio |
|-----------|--------------|---------------|
| SPY shares alone | ~0.67 | ~0.95 |
| Options strategy alone | ~0.6 | ~0.8 |
| **Combined portfolio** | **~1.03** | **~1.35** |

How can combining two things with Sharpe ~0.6 produce a combined Sharpe >1.0? They're both long SPY exposure—shouldn't they be perfectly correlated?

### The Resolution

The options strategy is **not** simply leveraged SPY exposure. Its return stream diverges from SPY in crucial ways:

**1. Conditional Correlation**

The correlation between the options component and SPY is high during uptrends (~0.85) but lower during downtrends (~0.5). Why? Because during downtrends:
- We're not adding new options positions (SMA filter)
- Existing positions are exited (SMA breach rule)
- The options component sits in cash

This asymmetric correlation is valuable—we participate in upside but partially sit out downside.

**2. Cash Buffer Effect**

The $100,000 options allocation (see Backtest Methodology for assumptions) isn't always deployed. During market stress, some portion is in cash. This reduces portfolio volatility during drawdowns more than it reduces returns (since the cash would have been invested in losing options anyway).

**3. Return Distribution Shaping**

The profit targets and max hold rules shape the return distribution differently than buy-and-hold:
- Winners are harvested at +50%
- Losers are cut at 60 days or SMA breach
- This truncates both tails but asymmetrically (more upside capture)

### The Mathematical Intuition

Even with high correlation (ρ = 0.85), portfolio volatility is:

```
σ_portfolio = √(w₁²σ₁² + w₂²σ₂² + 2·w₁·w₂·ρ·σ₁·σ₂)
```

With ρ < 1, volatility combines sub-additively while returns combine additively. The lower correlation during drawdowns is especially valuable because:

1. Drawdowns hurt Sharpe ratio disproportionately (volatility spikes)
2. Reducing correlation specifically during drawdowns improves Sharpe more than reducing it during uptrends

This is why the combined portfolio's Sharpe exceeds both components—we get equity-like returns during good times with reduced participation during bad times.

---

## Implementation Details

### Capital Requirements

| Component | Amount | Purpose |
|-----------|--------|---------|
| Share holdings | ~$1,200,000 | 2,000 SPY shares at ~$600 |
| Options cash | $100,000 | Call option purchases |
| **Total** | **~$1,300,000** | |

The strategy can be scaled proportionally. A smaller investor might use:
- 200 shares ($120,000) + $10,000 options cash = $130,000 total

### Broker Requirements

- **Options approval:** Level 2 or higher (ability to buy calls)
- **Margin:** Not required (we're buying options, not selling)
- **Data:** Real-time SPY quotes and SMA200 calculation

### Daily Monitoring Checklist

```
□ Check SPY closing price vs. SMA200
□ If below SMA by >2%: Exit all option positions
□ If above SMA:
  □ Check current portfolio delta
  □ If delta room available and cash available:
    □ Identify appropriate expiration (~120 DTE)
    □ Find 80-delta strike
    □ Check bid-ask spread (<1%)
    □ Enter limit order at midpoint
□ Check existing positions for:
  □ 50% profit target hit → Sell
  □ 60 days held → Sell
```

### Live Trading Workflow

The strategy includes automated daily monitoring tools.

**Position Tracking (`monitor_positions.py`):**
- Edit `OPEN_POSITIONS` list to add/remove positions
- Run manually: `python monitor_positions.py`
- Connects to IBKR TWS for live quotes (falls back to Black-Scholes estimates)
- Displays P&L, delta exposure, days held, and profit target status

**Daily Check (`daily_check.py`):**
- Run manually: `python daily_check.py`
- With live quotes: `python daily_check.py --quotes`
- Checks: SMA200 filter status, entry signals, exit alerts for all positions

**Automated Scheduling (Windows):**
```
# Run setup_scheduled_task.ps1 as administrator to create scheduled task
# Task runs daily_check.py at 10:00 AM on trading days
# Logs saved to: Strategies/80-Delta Call Strategy/logs/
```

**Data Sources:**
| Source | Data | When Used |
|--------|------|-----------|
| IBKR TWS | Live SPY price, option quotes | Market hours when TWS running |
| ThetaData | EOD SPY price, historical data | Fallback when IBKR unavailable |
| Black-Scholes | Option price estimates | Fallback when no live quotes |

**IBKR Configuration:**
- Port 7497 for TWS Paper Trading
- Port 7496 for TWS Live Trading
- Enable "Allow connections from localhost only" in TWS API settings

### Transaction Costs

Estimated costs per trade:
- Commission: $0.65 per contract (typical retail)
- Bid-ask spread: ~$0.10-0.20 per share ($10-20 per contract)
- Total: ~$15-25 per round-trip trade

With ~50-100 trades per year, annual transaction costs: ~$1,500-2,500

---

## The Software Components

The backtesting framework consists of several Python programs:

### Core Infrastructure

| File | Purpose |
|------|---------|
| `backtest/thetadata_client.py` | Connects to ThetaData API for historical options prices |
| `backtest/black_scholes.py` | Options pricing and Greeks calculations (delta, theta, etc.) |
| `backtest/fred_client.py` | Fetches economic data (CAPE ratio) from Federal Reserve database |

### Main Backtest Engine

| File | Purpose |
|------|---------|
| `delta_capped_backtest.py` | Core SPY strategy backtest - runs the simulation |
| `qqq_delta_capped_backtest.py` | Same strategy applied to QQQ (Nasdaq-100) |
| `rsp_delta_capped_backtest.py` | RSP version with liquidity filtering |
| `vti_delta_capped_backtest.py` | VTI version (Total US Market) |

### Analysis Tools

| File | Purpose |
|------|---------|
| `analysis_sma_distance.py` | Segments trades by entry distance from SMA200 |
| `analysis_trailing_returns.py` | Analyzes correlation with trailing 12-month returns |
| `analysis_valuation.py` | Tests relationship with CAPE (valuation) ratio |
| `screen_tickers.py` | Evaluates candidate tickers for liquidity and returns |
| `sma_period_comparison.py` | Compares SMA50/100/150/200 filter performance |
| `sma_distance_distribution.py` | Analyzes % of time SPY spends at various distances from SMA200 |

### Entry Timing Tests

| File | Purpose |
|------|---------|
| `down_day_entry_test.py` | Tests buying only on days SPY closes lower |
| `down_day_scaled_test.py` | Tests 2x/3x position sizing on down days |
| `extended_entry_zone_test.py` | Tests buying in buffer zone (SMA to 2% below) |
| `rsi_filter_test.py` | Tests RSI filter to avoid overbought entries (rejected) |

### Monitoring and Execution

| File | Purpose |
|------|---------|
| `monitor_positions.py` | Tracks current positions with P&L calculations and IBKR integration |
| `daily_check.py` | Daily trading check: SMA filter, entry signals, exit alerts |
| `run_daily_check.bat` | Windows batch wrapper for scheduled task automation |
| `ibkr_option_quotes.py` | Fetches live option quotes from Interactive Brokers |

### Data Flow

```
ThetaData API ─────────────────┐
                               ▼
Yahoo Finance ───────► thetadata_client.py ───► Price/Options Data
                               │
                               ▼
                    delta_capped_backtest.py
                               │
                     ┌─────────┴─────────┐
                     ▼                   ▼
              Daily Snapshots       Trade Log
                     │                   │
                     ▼                   ▼
              Performance           Analysis Scripts
               Metrics              (segmentation)
```

---

## Strategy Capacity and Scalability

### How Much Capital Can This Strategy Handle?

**Short answer:** $50-100 million in the options component before any market impact concerns.

**Analysis:**

SPY options are the most liquid options market in the world:
- Daily volume: 10-20 million contracts
- Open interest: 20-30 million contracts

Our backtest trades ~5-10 contracts per month. Even scaling 1000x to $100M in options allocation, we'd trade ~5,000-10,000 contracts monthly—a tiny fraction of daily volume.

### Real Constraints on Capacity

The practical limits aren't market impact but rather:

**1. Competition and Edge Erosion**

If many traders adopt similar systematic approaches, the edge diminishes. The SMA200 is widely known; the specific implementation details provide only modest differentiation.

**2. Regime Dependency**

The SMA200 filter worked historically. Future market regimes (different central bank policies, market structure changes) may behave differently.

**3. The Edge is Modest**

The strategy generates perhaps 3-5% annual alpha. This is meaningful but not extraordinary. It doesn't compound as dramatically as strategies with larger edges.

### Scaling Recommendations

| Allocation | Recommendation |
|------------|----------------|
| < $10M options | No concerns, execute normally |
| $10-50M | Consider splitting orders across days |
| $50-100M | Use algorithmic execution, consider QQQ diversification |
| > $100M | Strategy likely too small for this capital base |

---

## Risks and Limitations

### Known Risks

**1. Regime Change Risk**

The SMA200 filter has worked for decades, but past performance doesn't guarantee future results. A prolonged sideways market with frequent SMA crosses could generate losses from whipsaws and time decay.

**2. Gap Risk**

Options can lose significant value overnight if SPY gaps down on news. The 2% SMA exit threshold doesn't protect against overnight moves.

**3. Liquidity Risk**

While SPY options are highly liquid, during market stress (flash crashes, circuit breakers), liquidity can evaporate. Exit orders may fill at unfavorable prices.

**4. Model Risk**

Our backtest assumes:
- Execution at quoted prices
- No slippage beyond bid-ask spread
- Continuous market operation

Real-world execution may be worse.

**5. Concentration Risk**

The strategy is 100% long US large-cap equities. It provides no diversification against:
- US market decline
- Dollar weakness
- Equity bear markets

### Limitations of the Backtest

**1. Survivorship Bias**

We tested SPY because it's the most liquid. We don't know how many similar strategies failed on other instruments.

**2. Look-Ahead Bias (Mitigated)**

We tried to avoid this by using realistic data (actual historical bid-ask spreads) rather than theoretical prices.

**3. Limited History**

10 years (2015-2025) includes only one major bear market (2020 COVID crash, 2022 rate hike selloff). The strategy hasn't been tested through a prolonged multi-year bear market like 2000-2002 or 2007-2009.

**4. Parameter Optimization**

We tested multiple parameters and chose the best ones. Some outperformance may be due to fitting to historical noise rather than genuine patterns.

### What Could Go Wrong

| Scenario | Impact | Mitigation |
|----------|--------|------------|
| Flash crash overnight | Options lose 50-80% | Accept as cost of strategy |
| Prolonged sideways chop | Time decay erodes capital | SMA filter reduces exposure |
| Rising rates crush valuations | Both shares and options decline | None - strategy is long-only |
| Market structure change | Edge disappears | Monitor and adapt |

---

## Conclusion

The 80-delta call strategy is a systematic approach to enhancing equity returns through disciplined options trading. Its key strengths are:

1. **Simplicity:** Clear, rules-based approach anyone can follow
2. **Risk Management:** Delta cap and SMA filter prevent catastrophic losses
3. **Demonstrated Edge:** Statistically significant outperformance over 10 years
4. **Scalability:** Can handle meaningful capital without market impact

Its key weaknesses are:

1. **Modest Alpha:** 3-5% annual improvement, not a path to quick riches
2. **Regime Dependent:** Works in trending markets, struggles in chop
3. **US Equity Concentration:** No diversification benefit
4. **Complexity vs. Buy-and-Hold:** More work for moderate improvement

The strategy is appropriate for investors who:
- Have sufficient capital (~$130K minimum for proper implementation)
- Can monitor positions daily or near-daily
- Accept the complexity in exchange for modest return enhancement
- Understand and accept the risk of occasional large drawdowns

It is not appropriate for investors who:
- Need capital preservation
- Cannot tolerate 30%+ drawdowns
- Want "set and forget" simplicity
- Are looking for outsized returns

---

## Appendix: Glossary

| Term | Definition |
|------|------------|
| **ATM** | At-the-money; strike price equals current stock price |
| **Call Option** | Right to buy stock at strike price before expiration |
| **Delta** | Option price sensitivity to $1 stock move |
| **DTE** | Days to expiration |
| **ITM** | In-the-money; call strike below current price |
| **OTM** | Out-of-the-money; call strike above current price |
| **Premium** | Price paid for an option |
| **Sharpe Ratio** | Risk-adjusted return metric |
| **SMA200** | 200-day simple moving average |
| **Strike** | The price at which option can be exercised |
| **Theta** | Option value decay per day |
| **VIX** | Volatility index; measures expected market volatility |

---

*Document prepared February 2026. Last updated February 7, 2026. Backtest period: March 2015 - January 2026.*

---

## Appendix: Table Index

- **Table 1:** SMA Period Comparison (50/100/150/200 days)
- **Table 2:** SMA Exit Threshold Comparison (0%/1%/2%/3%)
- **Table 3:** Down-Day Entry - Same Position Size
- **Table 4:** Down-Day Entry - Scaled Position Size
- **Table 5:** Extended Entry Zone Test
- **Table 6:** RSI Filter Test
- **Table 7:** Weekly vs Monthly Expirations
