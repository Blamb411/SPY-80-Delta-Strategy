"""
Massive API Verification Script
Tests connection and data availability for options backtesting.
"""
import os
from massive import RESTClient

# Use environment variable if set, otherwise fall back
KEY_FILE = os.path.join(os.path.dirname(__file__), "Massive backtesting", "api_key.txt")
with open(KEY_FILE) as f:
    API_KEY = f.read().strip()

client = RESTClient(api_key=API_KEY)

print("=" * 70)
print("MASSIVE API VERIFICATION")
print("=" * 70)

# Test 1: Pull recent SPY price data
print("\n--- Test 1: SPY Daily Bars (last 5 trading days) ---")
try:
    aggs = list(client.list_aggs(
        ticker="SPY",
        multiplier=1,
        timespan="day",
        from_="2026-01-20",
        to="2026-01-27",
        limit=10
    ))
    for a in aggs:
        print(f"  {a.timestamp} | O:{a.open:.2f} H:{a.high:.2f} L:{a.low:.2f} C:{a.close:.2f} V:{a.volume:,.0f}")
    print(f"  -> Got {len(aggs)} bars. OK")
except Exception as e:
    print(f"  FAILED: {e}")

# Test 2: Pull SPY options chain snapshot (current)
print("\n--- Test 2: SPY Options Chain Snapshot (current, 1 strike) ---")
try:
    chain = list(client.list_snapshot_options_chain(
        "SPY",
        params={
            "strike_price": 600,
            "expiration_date.gte": "2026-02-01",
            "expiration_date.lte": "2026-03-01",
            "contract_type": "put",
        },
    ))
    for o in chain[:3]:
        det = o.details
        greeks = o.greeks
        quote = o.last_quote
        print(f"  Contract: {det.ticker}")
        print(f"    Strike: {det.strike_price}, Exp: {det.expiration_date}, Type: {det.contract_type}")
        if greeks:
            print(f"    Delta: {greeks.delta:.4f}, Gamma: {greeks.gamma:.4f}, "
                  f"Theta: {greeks.theta:.4f}, Vega: {greeks.vega:.4f}, IV: {greeks.implied_volatility:.4f}")
        if quote:
            print(f"    Bid: {quote.bid:.2f}, Ask: {quote.ask:.2f}, Midpoint: {quote.midpoint:.2f}")
        print()
    print(f"  -> Got {len(chain)} contracts. OK")
except Exception as e:
    print(f"  FAILED: {e}")

# Test 3: Historical options data availability (check 2020)
print("\n--- Test 3: Historical Options Contracts (SPY, Jan 2020) ---")
try:
    contracts = list(client.list_options_contracts(
        underlying_ticker="SPY",
        expiration_date="2020-02-21",
        contract_type="put",
        limit=10,
    ))
    for c in contracts[:5]:
        print(f"  {c.ticker} | Strike: {c.strike_price} | Exp: {c.expiration_date}")
    print(f"  -> Got {len(contracts)} contracts for Feb 2020 expiry. OK")
except Exception as e:
    print(f"  FAILED: {e}")

# Test 4: Historical options quotes for a specific contract
print("\n--- Test 4: Historical Options Quotes (SPY put, 2020) ---")
try:
    # First get a contract ticker for SPY Feb 2020
    contracts = list(client.list_options_contracts(
        underlying_ticker="SPY",
        expiration_date="2020-02-21",
        contract_type="put",
        strike_price=310,
        limit=1,
    ))
    if contracts:
        opt_ticker = contracts[0].ticker
        print(f"  Contract: {opt_ticker}")

        # Get daily bars for this option
        opt_aggs = list(client.list_aggs(
            ticker=opt_ticker,
            multiplier=1,
            timespan="day",
            from_="2020-01-21",
            to="2020-02-21",
            limit=50
        ))
        for a in opt_aggs[:5]:
            print(f"    {a.timestamp} | O:{a.open:.2f} H:{a.high:.2f} L:{a.low:.2f} C:{a.close:.2f} V:{a.volume:,.0f}")
        print(f"  -> Got {len(opt_aggs)} daily bars. OK")
    else:
        print("  No contracts found")
except Exception as e:
    print(f"  FAILED: {e}")

# Test 5: Check all 12 tickers have options
print("\n--- Test 5: Options Available for All 12 ETFs ---")
tickers = ["SPY", "QQQ", "IWM", "DIA", "XLF", "XLE", "XLK", "XLV", "GLD", "SLV", "TLT", "HYG"]
for t in tickers:
    try:
        contracts = list(client.list_options_contracts(
            underlying_ticker=t,
            expiration_date_gte="2026-02-01",
            expiration_date_lte="2026-03-01",
            contract_type="put",
            limit=3,
        ))
        print(f"  {t:<5} -> {len(contracts)} contracts found. OK")
    except Exception as e:
        print(f"  {t:<5} -> FAILED: {e}")

print("\n" + "=" * 70)
print("VERIFICATION COMPLETE")
print("=" * 70)
