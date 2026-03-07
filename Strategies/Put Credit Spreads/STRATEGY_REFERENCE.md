# Put Credit Spread Strategy Reference

## Overview

This strategy sells put credit spreads on SPY and QQQ to capture the Variance Risk Premium — the well-documented tendency of implied volatility to overstate realized volatility. It uses a 200-day SMA trend filter to avoid selling puts in bear markets and a flat 0.20 delta to maintain consistent probability of profit across all volatility regimes.

The strategy was developed and backtested over the period 2012-2025 using ThetaData historical options quotes with synthetic (Black-Scholes) fallback pricing.

---

## Strategy Parameters (Recommended Configuration)

| Parameter | Value | Description |
|-----------|-------|-------------|
| Tickers | SPY, QQQ | Underlying ETFs |
| Delta | 0.20 (flat) | Short put delta, fixed across all IV tiers |
| Wing width | sigma = 0.75 | Long put placed at 0.75x the expected 1-sigma move below the short put |
| DTE target | 30 days | Target days to expiration (acceptable range: 25-45) |
| SMA period | 200 days | Only enter when underlying > its 200-day SMA |
| IV rank floor | 15% | Only enter when VIX IV rank >= 15% (percentile of trailing 252-day range) |
| Stop loss | 3.0x credit | Close position when loss reaches 3x the credit received |
| Take profit | 50% of credit | Close position when it can be bought back for <= 50% of the credit received |
| Entry interval | 5 trading days | Minimum spacing between new positions |

---

## How It Works

### 1. Entry Filters

Each trading day, the strategy checks three conditions before entering a new trade:

**a) 200-Day SMA Filter**
The underlying's current price must be above its 200-day simple moving average. This keeps the strategy out of sustained downtrends where selling puts is most dangerous. The SMA is computed from the underlying's own price history (SPY uses SPY's SMA, QQQ uses QQQ's SMA).

**b) IV Rank >= 15%**
The VIX must be at or above the 15th percentile of its trailing 252-day (1-year) range. IV rank is calculated as:

    IV Rank = (VIX_today - VIX_252day_low) / (VIX_252day_high - VIX_252day_low)

This ensures there is adequate premium to sell. VIX is used as the volatility signal for SPY; VXN (Nasdaq-100 Volatility Index) is used for QQQ.

**c) Entry Interval**
At least 5 trading days must have elapsed since the last position was opened. This prevents overconcentration of risk around a single market event. Multiple positions can be open simultaneously (overlapping 30-day trades), but new entries are spaced apart.

### 2. Trade Construction

**Expiration selection:**
The strategy selects the expiration closest to 30 DTE within an acceptable range of 25-45 days. Both weekly and monthly expirations are eligible; no monthly-only filter is applied.

**Short put selection:**
The short put strike is determined by solving the Black-Scholes equation for a put with delta equal to 0.20, using the current spot price, the volatility index (VIX for SPY, VXN for QQQ) as the implied volatility input, and the target DTE. The resulting theoretical strike is rounded to the nearest integer and then snapped to the closest listed strike available for the selected expiration. A delta of 0.20 means roughly a 20% probability of finishing in the money. As volatility rises, this strike moves further from the current price in dollar terms, automatically adjusting for market conditions.

**Long put selection (wing width):**
The long put is placed below the short put at a distance determined by the expected move:

    expected_move = spot x (VIX / 100) x sqrt(DTE / 365)
    wing_width = expected_move x 0.75

For example, with SPY at $500 and VIX at 20:

    expected_move = 500 x 0.20 x 0.287 = $28.70
    wing_width = $28.70 x 0.75 = $21.50

The long put would be placed ~$21.50 below the short put. This scaling means wider wings in volatile markets (larger expected moves) and narrower wings in calm markets. The long put typically ends up at approximately delta 0.04-0.06.

**Credit and max loss:**

    Credit received = short put premium - long put premium
    Max loss = wing_width - credit (per share, x100 for per contract)

### 3. Exit Rules

Positions are monitored daily and closed when any of these conditions are met:

**Take profit (50% of credit):**
If the spread can be bought back for 50% or less of the original credit received, close it. This captures the bulk of the time decay without waiting for full expiration, freeing capital for the next trade.

**Stop loss (3x credit):**
If the unrealized loss reaches 3x the credit received, close the position. For example, if you collected $0.50/share ($50/contract), the stop triggers at a $1.50/share ($150/contract) loss. This limits the damage from any single trade.

**Expiration:**
If neither take profit nor stop loss triggers, the spread settles at expiration based on where the underlying closes relative to the strikes.

### 4. What the Strategy is Doing Economically

Options implied volatility consistently overstates actual realized volatility. This means put sellers are systematically overcompensated for the risk they take. The strategy harvests this "variance risk premium" by:

- Selling puts at a probability level (delta 0.20) where the premium collected exceeds the expected loss over many trades
- Using the SMA filter to avoid the regime (bear markets) where the variance risk premium temporarily reverses
- Using vol-scaled wings to keep the risk proportional to market conditions
- Taking profit early (50%) to reduce exposure to late-cycle reversals
- Using a defined stop loss (3x) to cap losses on any single trade

---

## Backtest Results (2012-2025)

Results below reflect the corrected backtester (March 2026): no future-quote lookahead, cooldown only on actual trades, VXN for QQQ.

### SPY (entry interval = 5 days)
| Metric | Value |
|--------|-------|
| Total trades | 190 |
| Win rate | 84.7% |
| Total P&L | +$3,468 |
| Avg P&L/trade | +$18.25 |
| Stop losses triggered | 29 |
| Annualized Sharpe | 1.018 |
| Annualized Sortino | 1.285 |
| PSR | 99.4% |
| Trades per year | 13.7 |

### SPY by Entry Interval

| Metric | Interval=5 | Interval=3 | Interval=1 |
|--------|-----------|-----------|-----------|
| Total trades | 190 | 243 | 509 |
| Win rate | 84.7% | 86.0% | 89.2% |
| Total P&L | +$3,468 | +$4,187 | +$9,633 |
| Avg P&L/trade | +$18.25 | +$17.23 | +$18.93 |
| Annualized Sharpe | 1.018 | 1.184 | 1.374 |
| Annualized Sortino | 1.285 | 1.486 | 1.564 |
| Trades per year | 13.7 | 17.5 | 36.7 |

Shorter intervals increase trade count and total P&L with improving Sharpe ratios, at the cost of more overlapping positions and higher capital requirements.

### QQQ (VXN, entry interval = 5 days)
| Metric | Value |
|--------|-------|
| Total trades | 293 |
| Win rate | 81.9% |
| Total P&L | +$4,257 |
| Avg P&L/trade | +$14.53 |
| Stop losses triggered | 53 |
| Annualized Sharpe | 0.460 |
| Annualized Sortino | 0.515 |
| PSR | 92.5% |
| Trades per year | 21.1 |

### Combined Portfolio (SPY + QQQ, interval=5)
| Metric | Value |
|--------|-------|
| Total trades | 483 |
| Win rate | 83.0% |
| Total P&L | +$7,725 |
| Avg P&L/trade | +$15.99 |
| Trades per year | 34.8 |

### Capital Context
- Average max loss per SPY contract: ~$1,279
- Average credit per SPY trade: ~$0.52/share ($52/contract)
- Average credit-to-width ratio: ~3.2% (SPY), ~4.8% (QQQ)

### Key Risk Metrics
- Worst single year (QQQ): 2022, -$1,729 (3 trades, all stopped out in bear market)
- SPY worst years: 2014 (-$26), 2018 (-$62) — small losses relative to total P&L
- SPY best years: 2020 (+$1,014), 2021 (+$812), 2025 (+$689)
- SPY "Very Low VIX" bucket (VIX 0-15): 37 trades, -$162 — the IV rank filter catches most but not all low-premium entries

### SPY Performance by VIX at Entry
| VIX Bucket | Trades | Win Rate | Total P&L | Avg P&L |
|------------|--------|----------|-----------|---------|
| Very Low (0-15) | 37 | 70.3% | -$162 | -$4.38 |
| Low (15-20) | 84 | 79.8% | +$417 | +$4.96 |
| Medium (20-25) | 43 | 97.7% | +$1,959 | +$45.56 |
| High (25-30) | 20 | 100.0% | +$789 | +$39.45 |
| Very High (30+) | 6 | 100.0% | +$465 | +$77.50 |

---

## Important Caveats

1. **Gap risk**: The stop loss assumes you can exit at 3x credit. In a gap-down or liquidity crisis, actual losses could exceed the stop level.

2. **Sample size in tails**: 29 stop losses on SPY over 13 years is a small sample. The strategy has not been tested through a 2008-style event with live options data.

3. **Execution costs**: The backtest uses historical bid/ask quotes where available but does not model slippage, partial fills, or commission costs.

4. **QQQ is weaker**: Sharpe of 0.460 with 18% stop-loss rate. QQQ adds diversification but also adds volatility. A SPY-only approach is defensible.

5. **Data skip rate**: 702 of 2,760 evaluated days (25%) were skipped due to missing ThetaData quotes. These are not random — they tend to cluster around holidays and low-liquidity periods, which could introduce survivorship-like bias.

---

## Program Index

### Core Files (Required to Run the Strategy Backtest)

| File | Purpose |
|------|---------|
| `backtest/put_spread_thetadata.py` | Main backtester. Contains all strategy logic: entry filters, trade construction, daily monitoring, exit rules, result reporting. Run directly for single-ticker backtests. |
| `backtest/thetadata_client.py` | ThetaData API client with SQLite caching. Fetches historical option quotes, expirations, strikes, and underlying bars. Also fetches VIX history and ticker-specific price bars via yfinance. |
| `backtest/black_scholes.py` | Black-Scholes pricing engine. Provides theoretical option pricing as fallback when historical quotes are unavailable. Includes spread pricing, delta calculation, strike finding, and SMA computation. |

### Comparison and Analysis Scripts

| File | Purpose |
|------|---------|
| `backtest/run_flat_delta_comparison.py` | Compares flat delta 0.20 vs tier-based delta across SPY and QQQ, with and without IV rank ceiling. This produced the final recommended configuration. |
| `backtest/run_iv_volwing_comparison.py` | Tests IV rank thresholds (10-30%) and vol-scaled wing widths (sigma 0.50-1.25). Established IV>=15% and sigma=0.75 as optimal. |
| `backtest/run_ticker_vol_tiers.py` | Tests IV rank floors and ceilings per ticker (SPY, QQQ, IWM). Established that IV ceiling is unnecessary with flat delta. |
| `backtest/run_multi_ticker_comparison.py` | Compares strategy across SPY, QQQ, IWM, DIA. Established SPY as strongest ticker, QQQ as viable second. |
| `backtest/run_put_spread_comparison.py` | Early comparison of SMA periods (off/100/150/200) and stop-loss multipliers (1.5x-3.0x). Established SMA=200 and SL=3.0x. |
| `backtest/run_wing_width_comparison.py` | Tests percentage-based wing widths (3-7%). Preceded the vol-scaled wing approach. |
| `backtest/run_cw_ratio_comparison.py` | Tests minimum credit-to-width ratio filters. Found no benefit to filtering. |
| `backtest/portfolio_simulation.py` | Simulates $10K portfolio growth from 2012-2025 with equity curve, drawdown tracking, and SPY buy-and-hold comparison. |
| `backtest/capital_efficiency.py` | Analyzes returns on deployed capital (not total account). Shows daily capital at risk, utilization rate, and overlay strategy returns. |
| `backtest/analyze_put_spread.py` | General analysis of put spread backtest results with additional statistical breakdowns. |

### How to Run

**Prerequisites:**
- Python 3.8+
- ThetaData Terminal running on localhost (port 25510 or auto-detected)
- Required packages: `requests`, `numpy`, `scipy`, `yfinance`

**Single ticker backtest with recommended parameters:**
```
cd backtest
python put_spread_thetadata.py --start 2012 --end 2025 --sma-period 200 --stop-loss-mult 3.0 --iv-rank-low 0.15 --flat-delta 0.20 --wing-sigma 0.75 --root SPY
```

**Full flat delta comparison (SPY + QQQ):**
```
cd backtest
python run_flat_delta_comparison.py
```

**Portfolio simulation ($10K starting capital):**
```
cd backtest
python portfolio_simulation.py
```

**CLI arguments for put_spread_thetadata.py:**

| Argument | Default | Description |
|----------|---------|-------------|
| `--year` | — | Single year to test |
| `--start` | 2012 | Start year |
| `--end` | 2025 | End year |
| `--root` | SPY | Ticker symbol |
| `--sma-period` | 200 | SMA period (0 = disabled) |
| `--stop-loss-mult` | 2.0 | Stop loss as multiple of credit |
| `--iv-rank-low` | 0.30 | Minimum IV rank to enter |
| `--flat-delta` | 0.0 | Fixed delta (0 = use tier-based) |
| `--wing-sigma` | 0.0 | Vol-scaled wing multiplier (0 = use percentage) |
| `--synthetic-only` | false | Use only Black-Scholes pricing |
| `--entry-interval` | 5 | Min trading days between entries |
| `--export-csv` | — | Export trades to CSV file |

Note: The code defaults (`DEFAULT_IV_RANK_LOW=0.30`, `DEFAULT_STOP_LOSS_MULT=2.0`, etc.) reflect the original conservative settings. The recommended configuration discovered through testing uses the CLI overrides shown above (`--iv-rank-low 0.15`, `--stop-loss-mult 3.0`, `--flat-delta 0.20`, `--wing-sigma 0.75`).

---

## Parameter Evolution (How We Got Here)

| Parameter | Original | Tested Range | Final | Why |
|-----------|----------|-------------|-------|-----|
| IV rank floor | 30% | 10-30% | 15% | 30% produced only 49 trades. 15% gave 160+ trades with best Sharpe. 10% collapsed (too many marginal entries). |
| SMA period | 200 | off, 100, 150, 200 | 200 | Kept strategy out of 2022 bear market. 200 gave best risk-adjusted returns. |
| Stop loss | 2.0x credit | 1.5x-3.0x | 3.0x | 2.0x triggered too often on normal fluctuations. 3.0x let winners recover while still capping catastrophic losses. |
| Wing width | 3% of spot | 3-7% fixed, sigma 0.5-1.25 | sigma 0.75 | Vol-scaled wings adapt to market conditions. 0.75 gave best Sortino (8.384). Higher sigma increased max loss without proportional benefit. |
| Delta | Tier-based (0.20/0.25/0.30) | Tier-based vs flat 0.20 | Flat 0.20 | Tier-based escalated delta in high IV, causing stop losses. Flat 0.20 maintained consistent distance from spot. Fewer stops, higher Sharpe. |
| IV rank ceiling | none | 40-70%, none | none | Originally added to cap risk in very high IV. With flat delta, the high-IV problem is solved — ceiling just removes profitable trades. |
| Tickers | SPY only | SPY, QQQ, IWM, DIA | SPY + QQQ | IWM marginal (Sharpe 0.427). DIA break-even. SPY dominant. QQQ adds diversification. |

---

## Files Not Required for This Strategy

The project contains many other files from earlier work on iron condors, Seeking Alpha stock picking, QuantConnect ports, and alternative data sources. These are unrelated to the put credit spread strategy documented here:

- `options_scanner.py`, `yield_hunter.py` — IB-connected live scanners (iron condor and yield focus)
- `backtest/condor_*.py` — Iron condor backtests (predecessor strategy)
- `Massive backtesting/` — Alternative backtesting framework using Polygon/Massive API
- `quantconnect/` — QuantConnect cloud backtest ports
- `Seeking Alpha Backtests/` — Stock picking model (unrelated to options)
