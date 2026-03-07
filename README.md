# spy-80-delta

Options strategy backtesting and analysis for SPY and QQQ.

## Strategies

### Put Credit Spreads (PCS) — MOVED
The PCS backtester has been consolidated into the **[put-credit-spreads](https://github.com/Blamb411/Put-Credit-Spreads)** repo. The copies in this repo (`backtest/put_spread_thetadata.py` and `Strategies/Put Credit Spreads/put_spread_thetadata.py`) are thin shims that re-export from `put-credit-spreads` for backward compatibility.

**Run PCS backtests from the canonical repo:**
```bash
cd C:/Users/Admin/Trading/repos/put-credit-spreads
python put_spread_thetadata.py --start 2012 --end 2025 \
    --sma-period 200 --stop-loss-mult 3.0 --iv-rank-low 15 \
    --flat-delta 0.20 --wing-sigma 0.75 --root SPY
```

### 80-Delta LEAPS Calls
Deep in-the-money SPY LEAPS calls as a leveraged equity substitute with defined risk.

## Directory Structure

| Directory | Contents |
|-----------|----------|
| `Strategies/80-Delta Call Strategy/` | 80-delta LEAPS strategy, analysis scripts, position tracker |
| `Strategies/Put Credit Spreads/` | PCS docs and comparison scripts (backtester is a shim → `put-credit-spreads`) |
| `backtest/` | Shared infrastructure: ThetaData client, Black-Scholes engine, comparison runners |
| `Analysis/` | Exploratory analysis notebooks and scripts |
| `quantconnect/` | QuantConnect cloud backtest ports (historical) |

## Key Files

- `backtest/thetadata_client.py` — ThetaData v3 API client with SQLite caching
- `backtest/black_scholes.py` — Black-Scholes pricing engine and utilities
- `Strategies/Put Credit Spreads/STRATEGY_REFERENCE.md` — PCS strategy documentation
- `options_scanner.py` — IB-connected live options scanner (iron condors)
- `yield_hunter.py` — IB-connected live yield scanner (put spreads)

## Prerequisites

- Python 3.8+
- ThetaData Terminal running on localhost (port 25510 or auto-detected)
- Required packages: `requests`, `numpy`, `scipy`, `yfinance`

See [STRATEGY_REFERENCE.md](Strategies/Put%20Credit%20Spreads/STRATEGY_REFERENCE.md) for full PCS documentation.
