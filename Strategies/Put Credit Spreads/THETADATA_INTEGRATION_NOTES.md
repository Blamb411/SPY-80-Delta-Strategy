# ThetaData Integration Notes

**Last Updated:** January 27, 2026
**Status:** Setup in progress - waiting on ThetaData account access

---

## Goal

Replace Black-Scholes theoretical pricing in backtests with **actual historical bid/ask quotes** from ThetaData for more accurate results.

---

## ThetaData Setup (Updated URLs)

The old thetadata.net URLs are broken. Use these instead:

| Resource | URL |
|----------|-----|
| Main Site | https://beta.thetadata.us/ |
| Pricing | https://beta.thetadata.us/pricing |
| Documentation | https://docs.thetadata.us |
| Terminal Download | https://download-unstable.thetadata.us/ThetaTerminalv3.jar |
| Discord Support | https://discord.thetadata.us |

### Pricing Tiers

| Tier | Price | Historical Data |
|------|-------|-----------------|
| Free | $0 | Limited |
| Value | $32/mo | 4 years |
| Standard | $64/mo | 8 years |
| Pro | $128/mo | 12 years |

---

## Setup Steps

1. **Create account** at https://beta.thetadata.us/pricing (Free tier)
2. **Install Java 21+** - check with `java -version`
3. **Download** ThetaTerminalv3.jar to `C:\ThetaTerminal\`
4. **Create** `creds.txt` in same folder with email on line 1, password on line 2
5. **Run** `java -jar ThetaTerminalv3.jar`

---

## Current Blocker

- Account login/password reset not working (possible server issue)
- Try again tomorrow or contact support via Discord

---

## Existing Code

The project already has a ThetaData test client:
- `backtest/thetadata_test.py` - Basic client for v2 API

**May need updates for v3 API** - we'll test once terminal is running.

---

## Next Steps (When Resuming)

1. Get ThetaData account working (try password reset again or contact support)
2. Install Java 21+ if needed
3. Download and run Theta Terminal v3
4. Test connectivity with existing script (update if needed for v3)
5. Build data fetcher for backtesting integration
6. Create ThetaData-powered backtest module

---

## Integration Plan

Once connected, we'll need to:

1. **Update `thetadata_test.py`** for v3 API (if endpoints changed)
2. **Create `thetadata_fetcher.py`** - fetch historical option chains for backtest dates
3. **Create `thetadata_backtest.py`** - backtest using real bid/ask data instead of Black-Scholes
4. **Cache data** to avoid repeated API calls (similar to existing IBKR cache)

---

## Free Tier Limitations

- Rate limited (exact limit TBD for v3)
- Historical data coverage may be limited
- EOD data only (no intraday)

For serious backtesting, Value tier ($32/mo) gives 4 years of data with unlimited requests.
