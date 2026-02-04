"""Compare condor_real_data.py (Polygon) vs condor_thetadata.py (ThetaData) for 2020-2025."""

W = 78

print("=" * W)
print("SIDE-BY-SIDE COMPARISON: condor_real_data.py vs condor_thetadata.py")
print("Period: 2020-2025  |  Ticker: SPY only")
print("=" * W)

print()
print("DATA SOURCES:")
print("  condor_real_data.py  : Polygon/Massive API option closes + 1% synth spread")
print("  condor_thetadata.py  : ThetaData v3 EOD bid/ask (real market quotes)")
print()

print("-" * W)
print(f"{'Metric':<30} | {'Polygon (real_data)':>22} | {'ThetaData':>18}")
print("-" * W)

rows = [
    ("Total trades",             "81",             "65"),
    ("Winners",                  "62 (76.5%)",     "43 (66.2%)"),
    ("Losers",                   "19",             "22"),
    ("Total P&L",                "$-2,818",        "$-19"),
    ("Avg P&L/trade",            "$-34.80",        "$-0.29"),
    ("Avg credit/share",         "$2.99",          "$4.27"),
    ("Avg max loss/contract",    "$1,023",         "$810"),
    ("", "", ""),
    ("Exit: take profit",        "58 (71.6%)",     "35 (53.8%)"),
    ("Exit: stop loss",          "11 (13.6%)",     "22 (33.8%)"),
    ("Exit: expiration",         "12 (14.8%)",     "8 (12.3%)"),
    ("", "", ""),
    ("Put breaches",             "13",             "1"),
    ("Call breaches",            "7",              "14"),
]

for label, polygon, theta in rows:
    if not label:
        print()
        continue
    print(f"  {label:<28} | {polygon:>22} | {theta:>18}")

print()
print("-" * W)
print("BY IV TIER")
print("-" * W)
print(f"  {'Tier':<12} | {'Poly #':>6} {'Poly P&L':>10} {'Poly/t':>8}"
      f" | {'Theta #':>7} {'Theta P&L':>10} {'Theta/t':>8}")
print("-" * W)

tiers = [
    ("medium",    43, -3539, -82,    43, +1416, +33),
    ("high",      21,  -578, -28,    13,  +502, +39),
    ("very_high", 17, +1298, +76,     9, -1937, -215),
]
for tier, pn, pp, pa, tn, tp, ta in tiers:
    print(f"  {tier:<12} | {pn:>5}t ${pp:>+8,} ${pa:>+6}/t"
          f" | {tn:>6}t ${tp:>+8,} ${ta:>+6}/t")

print()
print("-" * W)
print("YEAR-OVER-YEAR")
print("-" * W)
print(f"  {'Year':<6} | {'Poly #':>6} {'Poly WR':>8} {'Poly P&L':>10}"
      f" | {'Theta #':>7} {'Theta WR':>9} {'Theta P&L':>10}")
print("-" * W)

yoy = [
    (2020,  15, "53.3%",   +134,    13, "53.8%",     +63),
    (2021,   3, "66.7%",   +367,     2, "50.0%",    +299),
    (2022,  38, "73.7%",   -877,    33, "69.7%",    +645),
    (2023,   5, "60.0%",   -237,     3, "33.3%",  -1052),
    (2024,  11, "81.8%",  +2275,     8, "75.0%",   -478),
    (2025,   9, "55.6%",  -4212,     6, "83.3%",   +504),
]
for yr, pn, pwr, pp, tn, twr, tp in yoy:
    print(f"  {yr:<6} | {pn:>5}t {pwr:>8} ${pp:>+8,}"
          f" | {tn:>6}t {twr:>9} ${tp:>+8,}")

poly_total = sum(pp for _, _, _, pp, _, _, _ in yoy)
theta_total = sum(tp for _, _, _, _, _, _, tp in yoy)
print("-" * W)
print(f"  {'TOTAL':<6} | {'81':>5}t {'76.5%':>8} ${poly_total:>+8,}"
      f" | {'65':>6}t {'66.2%':>9} ${theta_total:>+8,}")

print()
print("=" * W)
print("KEY DIFFERENCES")
print("=" * W)

print("""
1. TRADE COUNT (81 vs 65)
   Polygon has more trades because it uses option close prices (more
   contracts have close data than bid/ask EOD data). ThetaData quality
   filters also reject entries with credit > 60% of wing width.

2. CREDIT SIZE ($2.99 vs $4.27/share)
   Polygon applies a fixed 1% synthetic bid-ask spread to close prices.
   ThetaData uses real EOD bid/ask. The higher ThetaData credit means
   real spreads are sometimes wider, but you collect more premium.

3. BREACH DIRECTION (put-heavy vs call-heavy)
   Polygon: 13 put breaches, 7 call
   ThetaData: 1 put breach, 14 call
   This is the most significant difference. Real bid/ask quotes show
   the call side stops out far more often than synthetic pricing suggests.
   Polygon's synthetic spread is symmetric, but real markets price puts
   with higher IV (skew), making the put side cheaper to close.

4. EXIT PROFILE (TP-heavy vs balanced)
   Polygon: 71.6% TP, 13.6% SL -- very skewed toward take-profit
   ThetaData: 53.8% TP, 33.8% SL -- more stop-losses with real quotes
   Real bid-ask spreads make TP harder to reach and SL easier to hit.

5. IV TIER REVERSAL
   Polygon: medium tier LOSES, very_high tier WINS
   ThetaData: medium tier WINS, very_high tier LOSES
   With real quotes, selling premium in moderate IV works (+$33/trade),
   but chasing high IV premium backfires (-$215/trade) because real
   spreads widen dramatically in high-vol environments.

6. OVERALL P&L ($-2,818 vs $-19)
   ThetaData is nearly breakeven despite more stop-losses, because
   larger real credits offset the wider spreads. Both backtests agree
   the strategy is roughly flat to slightly negative over 2020-2025.
""")
