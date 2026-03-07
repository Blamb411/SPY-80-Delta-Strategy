# spy-80-delta

Options strategy backtesting and analysis for SPY and QQQ.

## Strategies

### Put Credit Spreads (PCS)
Sells put credit spreads to capture the variance risk premium. Uses a 200-day SMA trend filter, flat 0.20 delta, vol-scaled wings, and IV rank floor. Backtested 2012-2025 using ThetaData historical quotes.

### 80-Delta LEAPS Calls
Deep in-the-money SPY LEAPS calls as a leveraged equity substitute with defined risk.

## Directory Structure

| Directory | Contents |
|-----------|----------|
| `Strategies/Put Credit Spreads/` | Canonical PCS backtester, strategy docs, and analysis scripts |
| `backtest/` | Shared backtest infrastructure: ThetaData client, Black-Scholes engine, comparison runners |
| `Analysis/` | Exploratory analysis notebooks and scripts |
| `Valuation-and-Predictive-Factors/` | Factor-based valuation research |
| `quantconnect/` | QuantConnect cloud backtest ports (historical) |

## Key Files

- `Strategies/Put Credit Spreads/put_spread_thetadata.py` — main PCS backtester
- `Strategies/Put Credit Spreads/STRATEGY_REFERENCE.md` — full strategy documentation
- `backtest/thetadata_client.py` — ThetaData v3 API client with SQLite caching
- `backtest/black_scholes.py` — Black-Scholes pricing engine and utilities
- `options_scanner.py` — IB-connected live options scanner (iron condors)
- `yield_hunter.py` — IB-connected live yield scanner (put spreads)

## Prerequisites

- Python 3.8+
- ThetaData Terminal running on localhost (port 25510 or auto-detected)
- Required packages: `requests`, `numpy`, `scipy`, `yfinance`

## Quick Start

```bash
# Run PCS backtest with recommended parameters (SPY, 2012-2025)
cd "Strategies/Put Credit Spreads"
python put_spread_thetadata.py --start 2012 --end 2025 \
    --sma-period 200 --stop-loss-mult 3.0 --iv-rank-low 0.15 \
    --flat-delta 0.20 --wing-sigma 0.75 --root SPY

# QQQ backtest (uses VXN instead of VIX)
python put_spread_thetadata.py --start 2012 --end 2025 \
    --sma-period 200 --stop-loss-mult 3.0 --iv-rank-low 0.15 \
    --flat-delta 0.20 --wing-sigma 0.75 --root QQQ

# Adjust entry spacing (default 5 trading days)
python put_spread_thetadata.py --entry-interval 1 --root SPY

# Synthetic-only mode (no ThetaData needed)
python put_spread_thetadata.py --synthetic-only
```

See [STRATEGY_REFERENCE.md](Strategies/Put%20Credit%20Spreads/STRATEGY_REFERENCE.md) for full documentation.
