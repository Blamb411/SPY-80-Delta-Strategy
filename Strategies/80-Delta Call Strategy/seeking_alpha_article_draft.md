# Why I Use 80-Delta Calls Instead of UPRO: A 20-Year Analysis

**Summary:**
- UPRO turned $100K into $10.5M over 20 years — but required holding through a -77% drawdown
- The 80-Delta call strategy achieved $3M with only -49% max drawdown and a superior "capture ratio"
- Analysis of 199 months reveals delta strategies gain 1.12x upside per unit of downside; UPRO gains only 0.93x
- The strategy outperforms by losing less in down markets, not by gaining more in up markets
- Best suited for investors who want leverage-like returns without leverage-like drawdowns

---

## Introduction

Let me start with an uncomfortable truth: if you had the discipline to buy UPRO at its inception in 2009 and hold through every crash, panic, and 77% drawdown, you would have crushed almost every other strategy. $100,000 would have become $10.5 million.

But here's the problem — you almost certainly wouldn't have held.

During the 2008-2009 financial crisis, a 3x leveraged position would have dropped from $100,000 to roughly $23,000. During COVID, you'd have watched three-quarters of your portfolio evaporate in weeks. Academic research consistently shows that most investors capitulate near market bottoms. The "paper returns" of leveraged ETFs rarely match the realized returns of actual investors.

So I went looking for an alternative. Something that could capture most of the leverage benefit with drawdowns I could actually survive.

After analyzing 20 years of data and testing dozens of variations, I found it: deep in-the-money call options with a simple trend filter.

This article presents the complete analysis — including the honest admission that UPRO beats my strategy on raw returns. But I'll show you why the **up/down capture ratio** matters more than total return, and why the 80-Delta approach might be the better choice for most investors.

---

## The Honest Comparison

Before diving into the strategy, let's establish what we're comparing. All strategies start with **$100,000** — this is an apples-to-apples comparison.

| Strategy | Effective Leverage | End Value | CAGR | Sharpe | Max Drawdown |
|----------|-------------------|-----------|------|--------|--------------|
| SPY Buy-and-Hold | 1.0x | $847,940 | +10.7% | 0.63 | -55.2% |
| SSO (2x Leveraged) | 2.0x | $1,620,722 | +15.3% | 0.56 | -84.7% |
| UPRO (3x Leveraged) | 3.0x | $10,539,342 | +32.5% | 0.81 | -76.8% |
| **80-Delta Calls** | **~1.7x equiv.** | **$3,045,440** | **+17.7%** | **0.66** | **-49.3%** |
| 70-Delta Calls | ~1.5x equiv. | $2,294,605 | +16.1% | 0.67 | -43.2% |

*Backtest period: January 2005 - January 2026 (20 years). UPRO results start from inception (June 2009); pre-2009 data uses synthetic 3x daily leveraged SPY returns.*

Let me be clear: **UPRO wins on absolute returns.** $10.5 million vs. $3 million isn't close.

But look at the max drawdown column. UPRO's -76.8% means watching $100,000 become $23,200 at the worst point. The 80-Delta strategy's -49.3% means $100,000 becomes $50,700.

Which are you more likely to hold through?

**Important note on the options results:** The numbers shown are for the **options-only** component. In practice, I recommend pairing this with share holdings (roughly 86% shares, 14% options) to dampen volatility. The combined portfolio has much lower drawdowns (~32%) while still capturing most of the alpha.

---

## What Is an 80-Delta Call?

For readers less familiar with options: delta measures how much an option's price moves when the underlying stock moves $1.

- An **80-delta call** moves about $0.80 for every $1 move in the stock
- These are "deep in-the-money" options — the strike price is well below the current stock price
- They provide leverage (you control $60,000 of SPY exposure for about $3,500) while limiting your maximum loss to the premium paid

The strategy is simple:

1. **Only buy calls when SPY is above its 200-day moving average** (trend filter)
2. **Target 70-80 delta** with ~120 days to expiration
3. **Exit at 50% profit** or after 60 days maximum hold
4. **Exit all positions if SPY falls 2% below the 200-day SMA**

That's it. No complex indicators, no earnings plays, no market timing beyond the SMA filter.

---

## The Key Insight: Up/Down Capture Ratio

After running the backtest, I wanted to understand *why* the 80-Delta strategy produced better risk-adjusted returns despite lower absolute returns than UPRO.

The answer emerged when I segmented 199 months of data by market environment.

### The Capture Ratio

I calculated two metrics for each strategy:
- **Upside Capture:** How much the strategy gains relative to SPY in up months (>2%)
- **Downside Exposure:** How much the strategy loses relative to SPY in down months (<-2%)

Then I divided them to get the **capture ratio** — a measure of how much upside you get per unit of downside:

| Strategy | Upside Capture | Downside Exposure | **Capture Ratio** |
|----------|----------------|-------------------|-------------------|
| 70-Delta | 1.64x SPY | 1.38x SPY | **1.19** |
| 80-Delta | 1.87x SPY | 1.66x SPY | **1.12** |
| 90-Delta | 2.11x SPY | 1.93x SPY | **1.10** |
| SSO (2x) | 1.96x SPY | 2.08x SPY | **0.94** |
| UPRO (3x) | 2.95x SPY | 3.16x SPY | **0.93** |

This is the most important table in this article.

**Delta strategies have capture ratios above 1.0.** They gain more upside per unit of downside than the underlying index.

**Leveraged ETFs have capture ratios below 1.0.** They actually lose *relatively more* than they gain.

How is this possible? UPRO captures 2.95x the upside — that's nearly 3x leverage working perfectly. But it also captures 3.16x the downside — worse than 3x. The asymmetry works against you.

The 80-Delta strategy captures 1.87x the upside while only suffering 1.66x the downside. The asymmetry works *for* you.

---

## Performance Across Market Environments

Let's break down exactly where these differences come from. I classified each of the 199 months into three categories:

- **Up markets:** SPY gained more than 2%
- **Flat markets:** SPY moved between -2% and +2%
- **Down markets:** SPY lost more than 2%

Here's what I found:

| Strategy | Up Markets (>2%) | Flat Markets (±2%) | Down Markets (<-2%) |
|----------|------------------|--------------------|--------------------|
| | *Mean Return / Win Rate* | *Mean Return / Win Rate* | *Mean Return / Win Rate* |
| SPY B&H | +4.48% / 100% | +0.25% / 65% | -5.06% / 0% |
| SSO (2x) | +8.78% / 100% | +0.12% / 56% | -10.53% / 0% |
| UPRO (3x) | +13.21% / 100% | **-0.14%** / 52% | -15.99% / 0% |
| 70-Delta | +7.36% / 86% | -0.44% / 39% | -6.98% / 3% |
| 80-Delta | +8.36% / 87% | -0.31% / 42% | -8.40% / 3% |

**Market Distribution:** Up markets occurred 47.7% of months, flat markets 33.2%, down markets 19.1%.

### Finding #1: UPRO Bleeds in Flat Markets

Look at the flat markets column. UPRO has a **negative mean return** (-0.14%) with only a 51.5% win rate — barely better than a coin flip.

This is volatility decay in action. The daily rebalancing mechanism that makes leveraged ETFs work also erodes value during sideways markets. Even when SPY goes nowhere, UPRO loses money.

The 80-Delta strategy also struggles in flat markets (-0.31% mean) due to theta decay — options lose value over time. But at least both strategies share this weakness.

### Finding #2: Delta Strategies Lose Less in Down Markets

Here's where the alpha comes from.

In down months, UPRO loses 15.99% on average — about 3.16x SPY's 5.06% loss. That's *worse* than 3x leverage.

The 80-Delta strategy loses only 8.40% — about 1.66x SPY. That's significantly less than you'd expect from a strategy with 1.7x leverage-equivalent returns.

Why? The SMA trend filter. When SPY falls below its 200-day moving average, the strategy moves to cash. You don't hold options through the worst of the decline.

### Finding #3: Correlation Breaks When It Matters Most

Perhaps the most surprising finding was the correlation analysis:

| Strategy | Up Markets | Flat Markets | Down Markets |
|----------|------------|--------------|--------------|
| SSO | 1.00 | 0.99 | 0.99 |
| UPRO | 1.00 | 0.97 | 0.97 |
| 70-Delta | 0.10 | 0.70 | **-0.11** |
| 80-Delta | 0.12 | 0.72 | **-0.05** |

Leveraged ETFs maintain near-perfect correlation with SPY in all environments — including crashes. When SPY drops, they drop proportionally more. No diversification benefit.

The delta strategies show **near-zero correlation** with SPY during down months. The SMA filter moves the portfolio to cash during downtrends, breaking the correlation precisely when diversification matters most.

This is why combining the options strategy with share holdings improves the combined portfolio's Sharpe ratio — you get equity-like returns during uptrends with reduced correlation during downturns.

---

## The Fundamental Insight

Let me summarize what the market environment analysis reveals:

**The 80-Delta strategy doesn't outperform by gaining more in up markets.** In fact, UPRO captures significantly more upside (+13.21% vs +8.36% in up months).

**It outperforms on a risk-adjusted basis by losing less in down markets.** The 8.40% average loss vs UPRO's 15.99% is where the capture ratio advantage comes from.

This has profound implications for real-world investing:

1. **Behavioral survivability:** A -49% drawdown is painful but survivable. A -77% drawdown triggers capitulation for most investors.

2. **Compounding math:** Losses hurt more than gains help. A 50% loss requires a 100% gain to recover. A 77% loss requires a 335% gain.

3. **Sleep at night factor:** I can hold through a strategy where my worst 12-month period is -37.8%. I'm not sure I could hold through -56.8% (UPRO's worst 12-month return).

---

## Why the SMA Filter Is Essential — And Why It Only Works for Options

Here's a counter-intuitive finding that initially confused me:

When I tested adding the SMA200 filter to buy-and-hold stock strategies, it **hurt** returns:

| ETF | B&H CAGR | +SMA Filter | CAGR Lost |
|-----|----------|-------------|-----------|
| SPY | +13.8% | +7.8% | **-6.0%** |
| SSO | +22.1% | +11.7% | **-10.4%** |
| UPRO | +28.0% | +14.7% | **-13.3%** |

The SMA filter costs you 6-13% CAGR on stocks/ETFs because you miss the sharp rallies that occur during recoveries. Markets tend to snap back quickly, and by the time price crosses above the SMA, you've missed a significant portion of the rebound.

But for the 80-Delta options strategy, the SMA filter **helps** returns:

| Strategy | Without SMA | With SMA | CAGR Gained |
|----------|-------------|----------|-------------|
| 80-Delta Options | +15.7% | +32.2% | **+16.5%** |

How can this be?

**Options have expiration dates. Stocks don't.**

When you hold stocks through a downturn, you suffer temporary losses but can wait indefinitely for recovery. Missing the recovery rallies hurts.

When you hold options through a downturn, they **expire worthless**. There's no recovery. The capital is gone.

The SMA filter saves the options strategy from buying calls during downtrends — calls that would almost certainly expire worthless. That preserved capital then compounds into future winners.

This is supported by academic research. Faber (2007) showed the 10-month moving average (approximately 200 days) reduced S&P 500 max drawdown from -83% to -50% over a century of data. Moskowitz, Ooi & Pedersen (2012) in the *Journal of Financial Economics* demonstrated that trend-following signals work across asset classes. The SMA isn't some mystical indicator — it's a simple, robust way to identify trend direction.

---

## How to Implement the Strategy

For investors interested in implementing this approach, here are the specific rules:

### Entry Rules

1. **SPY must be above its 200-day simple moving average** — this is non-negotiable
2. **Select monthly expiration approximately 120 days out** — avoid weekly options (liquidity issues)
3. **Choose a strike price with 70-80 delta** — deep in-the-money
4. **Respect your delta cap** — I limit total options delta to match my share holdings to prevent over-leveraging

### Exit Rules

1. **50% profit target** — take gains and redeploy capital
2. **60-day maximum hold** — avoids theta decay acceleration in final month
3. **Exit all positions if SPY falls 2%+ below SMA200** — this is the risk management rule that makes everything work

### Capital Requirements

The strategy can be scaled to different portfolio sizes:

| Portfolio Size | Shares | Options Allocation |
|----------------|--------|-------------------|
| $70,000 (minimum) | 100 SPY shares | $10,000 |
| $350,000 | 500 SPY shares | $50,000 |
| $700,000 | 1,000 SPY shares | $100,000 |

The 86% shares / 14% options split is my recommended ratio. The share holdings provide stability while the options overlay generates alpha.

### Broker Requirements

- Level 2 options approval (ability to buy calls)
- No margin required — you're buying options, not selling them
- Real-time quotes for SPY and options chains

---

## Rolling 12-Month Returns: Setting Realistic Expectations

Before implementing any strategy, you should understand the range of outcomes you might experience:

| Strategy | Mean | Worst 12-Mo | Best 12-Mo | % Positive |
|----------|------|-------------|------------|------------|
| SPY B&H | +14.7% | -18.2% | +56.2% | 90.4% |
| SSO | +25.7% | -39.0% | +130.5% | 83.5% |
| UPRO | +36.7% | -56.8% | +227.2% | 77.7% |
| 80-Delta | +30.0% | -37.8% | +211.9% | 75.5% |

The 80-Delta strategy has a positive 12-month return 75.5% of the time — lower than SPY's 90.4% but with a much higher mean return (+30.0% vs +14.7%).

The worst 12-month period for 80-Delta was -37.8% (ending May 2012). For UPRO, it was -56.8% (ending December 2022).

Which would you rather explain to your spouse?

---

## Who Should Use Which Strategy

Let me be direct about who should consider each approach:

| If You... | Consider... |
|-----------|-------------|
| Have iron discipline, 20+ year horizon, can genuinely hold through -77% drawdowns | UPRO — highest absolute returns |
| Want leverage-like returns but can't stomach 75%+ drawdowns | **80-Delta** — best leverage-to-drawdown ratio |
| Prioritize risk-adjusted returns above all else | 70-Delta — highest Sharpe (0.67), lowest drawdown (-43.2%) |
| Want simplicity, no monitoring, no stress | SPY Buy-and-Hold — proven, simple, effective |

Be honest with yourself about which category you fall into. Most people overestimate their ability to hold through severe drawdowns.

---

## But What About Timing UPRO?

Since UPRO's raw returns are so compelling, I spent considerable effort testing whether we could time entries and exits to reduce the drawdowns without giving up too much return. I tested five approaches:

| Timing Strategy | End Value | CAGR | Max DD | Verdict |
|---|---|---|---|---|
| UPRO Buy & Hold | $10,539,348 | +32.5% | -76.8% | Highest returns, brutal drawdowns |
| **UPRO + 25% Trailing Stop** | **$8,328,520** | **+30.6%** | **-46.1%** | **Best risk-adjusted** |
| UPRO + HFEA (55/45 with TMF) | $3,265,259 | +23.4% | -70.6% | Bond correlation broke in 2022 |
| UPRO + VIX<30 Filter | $3,834,597 | +24.6% | -72.7% | Threshold too generous |
| UPRO + Dual Momentum | $962,908 | +14.6% | -50.7% | Too many whipsaws |

The **drawdown exit strategy** stands out: exit UPRO when it drops 25% from its peak, re-enter when it makes a new all-time high or after 40 trading days. This retains 79% of UPRO's returns while cutting the max drawdown from -76.8% to -46.1%. Only 16 trades in 16 years.

Interestingly, this brings UPRO's drawdown (-46.1%) close to the 80-Delta strategy's drawdown (-49.3%), while delivering higher absolute returns. The tradeoff: the drawdown exit requires you to sell at a loss and trust the system to get you back in — behaviorally difficult. The 80-Delta strategy avoids this by never entering during downtrends in the first place.

### What We Tested and Rejected

Several potential enhancements to the 80-Delta strategy itself were investigated:

| Enhancement | Result | Why Rejected |
|---|---|---|
| **VIX<12 entry filter** (skip entries when IV is abnormally low) | Marginal Sharpe improvement | Reduced CAGR; low-VIX environments are often strong trending markets where the strategy performs well |
| **EMA200 instead of SMA200** | Underperformed | EMA's faster responsiveness caused more whipsaw exits; SMA200's lag is actually beneficial |
| **Rolling winners** (instead of taking 50% profit, roll to new contract) | +0.2% CAGR | Increased complexity and transaction costs for marginal gain |
| **Stacking multiple filters** (VIX + roll + EMA) | Diminishing returns | Over-optimizes to historical data; increases curve-fitting risk |

The core 80-Delta + SMA200 strategy remains the recommended approach. Its simplicity is a feature, not a bug.

---

## Risks and Limitations

No strategy is without risks. Here's what you should understand:

**Gap Risk:** Options can lose significant value overnight if SPY gaps down on unexpected news. The SMA exit rule doesn't protect against overnight moves.

**Flat Market Drag:** Both the 80-Delta strategy and leveraged ETFs underperform in flat markets. If we enter a prolonged sideways period, expect frustration.

**Modest Alpha:** The strategy generates approximately 5% annual alpha over SPY. This is meaningful over decades but not a path to quick riches. If you're looking for 50%+ annual returns, this isn't it.

**Daily Monitoring Required:** You need to check positions daily and be ready to exit when the SMA rule triggers. This isn't a set-and-forget approach.

**Model Risk:** Past performance doesn't guarantee future results. The SMA filter has worked for decades, but market structure changes could reduce its effectiveness.

---

## Conclusion

The 80-Delta call strategy isn't about beating UPRO on raw returns. UPRO wins that contest decisively — $10.5 million vs. $3 million over 20 years.

But raw returns don't matter if you can't hold the position.

What the 80-Delta strategy offers is a **capture ratio above 1.0** — gaining 1.87x SPY's upside while suffering only 1.66x the downside. It achieves this through the SMA trend filter, which moves to cash during downtrends and breaks correlation precisely when diversification matters most.

The strategy outperforms on a risk-adjusted basis not by making more in up markets, but by **losing less in down markets**. And those reduced losses compound over time into meaningful outperformance.

For investors who:
- Want leverage-like returns
- Can't hold through 77% drawdowns
- Are willing to monitor positions daily
- Have $70K+ to implement properly

...the 80-Delta call strategy deserves serious consideration.

The spreadsheet returns of UPRO are seductive. But the returns you can actually capture depend on the drawdowns you can actually survive.

---

*Disclosure: I am long SPY shares and SPY call options as described in this article. This is not investment advice. Options involve risk and are not suitable for all investors. Past performance does not guarantee future results. Do your own research before implementing any strategy.*

---

**Supporting Data:**
- Backtest period: January 2005 - January 2026 (20 years)
- Data source: ThetaData historical options prices (2015-2026), Black-Scholes synthetic pricing (2005-2014)
- SMA calculation: 200-day simple moving average of SPY closing prices
- All returns assume reinvestment and account for bid-ask spreads
- All backtest numbers independently verified February 2026 — all 7 delta-level results reproduced to within $1-2 of rounding
- UPRO timing strategies tested February 2026 using actual UPRO price data from yfinance

**Academic References:**
- Faber, M. (2007). "A Quantitative Approach to Tactical Asset Allocation." *Journal of Wealth Management*
- Moskowitz, T., Ooi, Y.H., & Pedersen, L.H. (2012). "Time Series Momentum." *Journal of Financial Economics*
- Hurst, B., Ooi, Y.H., & Pedersen, L.H. (2017). "A Century of Evidence on Trend-Following Investing." AQR Capital Management
- Antonacci, G. (2014). "Dual Momentum Investing." McGraw-Hill Education
