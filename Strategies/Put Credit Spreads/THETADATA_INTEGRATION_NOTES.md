# ThetaData Integration Notes

**Last Updated:** March 7, 2026
**Status:** Complete — ThetaData v3 integration is fully operational.

---

## Overview

The backtester uses **ThetaData v3** historical bid/ask quotes for realistic option pricing from June 2012 onward. For January–May 2012 (before ThetaData coverage begins), Black-Scholes synthetic pricing is used as a fallback.

## Architecture

- **API:** ThetaData v3 REST API via Theta Terminal on localhost (port 25510 or auto-detected)
- **Client:** `backtest/thetadata_client.py` handles all API calls, caching, and data normalization
- **Cache:** All fetched data is cached in SQLite (`thetadata_cache.db`, ~2 GB) to avoid redundant API calls
- **Fallback:** `backtest/black_scholes.py` provides theoretical pricing for dates without ThetaData coverage

## Data Sources

| Date Range | Source | Notes |
|------------|--------|-------|
| Jan–May 2012 | Black-Scholes synthetic | 5% synthetic bid/ask spread applied |
| Jun 2012 onward | ThetaData historical quotes | Real bid/ask used for entry/exit pricing |

## Volatility Indices

- **SPY:** VIX (^VIX) for IV rank and strike solving
- **QQQ:** VXN (^VXN) for IV rank and strike solving, with VIX fallback
- Both fetched via Yahoo Finance through `fetch_volatility_index()`

## Setup

1. ThetaData account (Value tier or above for full historical coverage)
2. Java 21+ installed
3. Theta Terminal v3 running: `java -jar ThetaTerminalv3.jar`
4. Credentials in `C:\ThetaTerminal\creds.txt`
