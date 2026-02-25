# SPY 80-Delta Call Strategy - Trade Log

## Strategy Rules
| Parameter | Value |
|-----------|-------|
| Entry Signal | SPY > 200-day SMA |
| Target Delta | 70-80 delta |
| Target DTE | ~120 days (90-150 range) |
| Profit Target | **+50%** |
| Max Hold | **60 trading days** |
| Stop Loss | None (per backtest) |
| SMA Exit | Optional: exit if SPY >2% below SMA200 |

---

## Open Positions

### Trade #1
| Field | Value |
|-------|-------|
| **Account** | IRA |
| **Entry Date** | 2026-02-03 |
| **Symbol** | SPY |
| **Strike** | $660 |
| **Expiration** | 2026-06-18 |
| **Type** | CALL |
| **Quantity** | 10 contracts |
| **Entry Price** | $51.60 |
| **Total Cost** | $51,600 |
| **Delta at Entry** | ~0.74 (73-delta) |
| **SPY at Entry** | ~$687 |
| **SPY SMA200** | ~$640 |
| | |
| **Profit Target** | $77.40 (+50%) |
| **Target Value** | $77,400 |
| **Max Hold Date** | ~2026-04-28 (60 trading days) |
| | |
| **Status** | OPEN |

### Trade #2
| Field | Value |
|-------|-------|
| **Account** | IRA |
| **Entry Date** | 2026-02-04 |
| **Symbol** | SPY |
| **Strike** | $650 |
| **Expiration** | 2026-05-29 |
| **Type** | CALL |
| **Quantity** | 10 contracts |
| **Entry Price** | $55.41 |
| **Total Cost** | $55,410 |
| **Delta at Entry** | ~0.80 (80-delta) |
| **SPY at Entry** | ~$685 |
| **SPY SMA200** | ~$640 |
| | |
| **Profit Target** | $83.12 (+50%) |
| **Target Value** | $83,115 |
| **Max Hold Date** | ~2026-04-29 (60 trading days) |
| | |
| **Status** | OPEN |

### Trade #3
| Field | Value |
|-------|-------|
| **Account** | IRA |
| **Entry Date** | 2026-02-06 |
| **Symbol** | SPY |
| **Strike** | $655 |
| **Expiration** | 2026-05-15 |
| **Type** | CALL |
| **Quantity** | 10 contracts |
| **Entry Price** | $49.70 |
| **Total Cost** | $49,700 |
| **Delta at Entry** | ~0.76 (76-delta) |
| **SPY at Entry** | ~$684 |
| **SPY SMA200** | ~$640 |
| | |
| **Profit Target** | $74.55 (+50%) |
| **Target Value** | $74,550 |
| **Max Hold Date** | ~2026-05-01 (60 trading days) |
| | |
| **Status** | OPEN |

### Trade #4
| Field | Value |
|-------|-------|
| **Account** | IRA |
| **Entry Date** | 2026-02-19 |
| **Symbol** | SPY |
| **Strike** | $655 |
| **Expiration** | 2026-06-18 |
| **Type** | CALL |
| **Quantity** | 10 contracts |
| **Entry Price** | $51.76 |
| **Total Cost** | $51,760 |
| **Delta at Entry** | ~0.78 (78-delta) |
| **SPY at Entry** | ~$688 |
| **SPY SMA200** | ~$641 |
| | |
| **Profit Target** | $77.64 (+50%) |
| **Target Value** | $77,640 |
| **Max Hold Date** | ~2026-05-14 (60 trading days) |
| | |
| **Status** | OPEN |

---

## Closed Positions

(None yet)

---

## Performance Summary

| Metric | Value |
|--------|-------|
| Total Trades | 4 |
| Open Trades | 4 |
| Closed Trades | 0 |
| Win Rate | - |
| Total P&L | - |

---

## Daily Notes

### 2026-02-03
- Entered first trade: 10x SPY $660C Jun 2026
- Entry at $51.60 (mid-point fill)
- SPY +7.3% above SMA200 - strong entry signal
- Delta ~0.74 (between 70 and 80 delta targets)
- Profit target: $77.40 (+50%)

### 2026-02-04
- Entered second trade: 10x SPY $650C May 2026
- Entry at $55.41 — down day entry, deeper ITM strike
- Delta ~0.80 at entry
- Profit target: $83.12 (+50%)

### 2026-02-06
- Entered third trade: 10x SPY $655C May 2026
- Entry at $49.70 — 76-delta
- Shorter DTE (98 days), tighter expiry
- Profit target: $74.55 (+50%)

### 2026-02-19
- Entered fourth trade: 10x SPY $655C Jun 2026
- Entry at $51.76 — 78-delta
- Same strike as Trade #3 but Jun expiry (119 DTE)
- Profit target: $77.64 (+50%)
- Total open: 40 contracts across 4 trades
