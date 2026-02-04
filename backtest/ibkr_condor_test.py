#!/usr/bin/env python3
"""Quick test: quote an SPY iron condor on IBKR paper trading."""

from ib_insync import IB, Stock, Option
from datetime import datetime, timedelta

ib = IB()
ib.connect("127.0.0.1", 7497, clientId=99, timeout=10)

spy = Stock("SPY", "ARCA", "USD")
ib.qualifyContracts(spy)
md = ib.reqMktData(spy, "", False, False)
ib.sleep(2)
spot = md.marketPrice()
print(f"SPY: ${spot:.2f}")

# Get full chain
chains = ib.reqSecDefOptParams("SPY", "", "STK", spy.conId)
chain = [c for c in chains if c.exchange == "SMART" and len(c.strikes) > 100][0]

exps = sorted(chain.expirations)
now = datetime.now()
valid = [(e, (datetime.strptime(e, "%Y%m%d") - now).days)
         for e in exps if 25 <= (datetime.strptime(e, "%Y%m%d") - now).days <= 45]
best_exp, best_dte = min(valid, key=lambda x: abs(x[1] - 30))

# Pick condor strikes on $5 boundaries (guaranteed to exist)
sp_strike = round(spot * 0.95 / 5) * 5
lp_strike = sp_strike - 15
sc_strike = round(spot * 1.05 / 5) * 5
lc_strike = sc_strike + 15

print(f"Exp: {best_exp} ({best_dte} DTE)")
print(f"Condor: LP={lp_strike} SP={sp_strike} | SC={sc_strike} LC={lc_strike}")

legs_info = [
    ("Long Put",   lp_strike, "P"),
    ("Short Put",  sp_strike, "P"),
    ("Short Call", sc_strike, "C"),
    ("Long Call",  lc_strike, "C"),
]

opt_contracts = [Option("SPY", best_exp, s, r, "SMART") for _, s, r in legs_info]
ib.qualifyContracts(*opt_contracts)
qualified = sum(1 for c in opt_contracts if c.conId > 0)
print(f"Qualified: {qualified}/4")

for (name, _, _), c in zip(legs_info, opt_contracts):
    if c.conId == 0:
        print(f"  FAILED: {name} strike={c.strike}")

tickers = ib.reqTickers(*opt_contracts)
ib.sleep(4)

header = f"{'Leg':<12} {'Strike':>7} {'Bid':>8} {'Ask':>8} {'Mid':>8} {'Sprd':>6} {'Sprd%':>7}"
print(f"\n{header}")
print("-" * 58)

short_bids = 0
long_asks = 0

for (name, _, _), tick in zip(legs_info, tickers):
    bid = tick.bid if tick.bid and tick.bid > 0 else 0
    ask = tick.ask if tick.ask and tick.ask > 0 else 0
    mid = (bid + ask) / 2 if bid and ask else 0
    spread = ask - bid
    sprd_pct = (spread / mid * 100) if mid > 0 else 0
    print(f"{name:<12} {tick.contract.strike:>7.0f} {bid:>8.2f} {ask:>8.2f} {mid:>8.2f} {spread:>5.2f}  {sprd_pct:>5.1f}%")
    if "Short" in name:
        short_bids += bid
    else:
        long_asks += ask

credit = short_bids - long_asks
put_width = sp_strike - lp_strike
call_width = lc_strike - sc_strike
max_width = max(put_width, call_width)
max_loss = (max_width - credit) * 100

print(f"\n  === IRON CONDOR SUMMARY ===")
print(f"  Entry credit:    ${credit:.2f}/share  (${credit * 100:.0f}/contract)")
print(f"  Put width:       ${put_width:.0f}")
print(f"  Call width:      ${call_width:.0f}")
print(f"  Max loss:        ${max_loss:.0f}/contract")
print(f"  Risk/reward:     {max_loss / (credit * 100):.1f}:1")
print(f"  TP (50%):        close at ${credit * 0.50:.2f}/sh")
print(f"  SL (75%):        close at ${max_loss * 0.75:.0f} loss")
print()
print("  IBKR paper trading is READY for live condor testing.")

ib.cancelMktData(spy)
for c in opt_contracts:
    if c.conId > 0:
        ib.cancelMktData(c)
ib.disconnect()
