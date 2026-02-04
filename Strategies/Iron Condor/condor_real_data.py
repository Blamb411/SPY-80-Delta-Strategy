#!/usr/bin/env python3
"""
Real-Data Iron Condor Backtest for SPY
=======================================
Validates the BS-theoretical condor backtest with real historical option
prices from Massive (Polygon) API.  Uses VIX as IV proxy for SPY.

Usage:
    python condor_real_data.py                # full backtest 2020-2025
    python condor_real_data.py --year 2024    # single-year smoke test
"""

import sys
import os
import math
import time
import logging
import argparse
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

# ---------------------------------------------------------------------------
# Path setup — let us import from backtest/ and the local Massive modules
# ---------------------------------------------------------------------------
_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(os.path.dirname(_this_dir))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)
if _this_dir not in sys.path:
    sys.path.insert(0, _this_dir)

import config
import cache_db
import data_fetcher
from backtest.black_scholes import find_strike_for_delta, calculate_iv_rank

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("condor_real")

# ---------------------------------------------------------------------------
# Constants (matching condor_backtest.py)
# ---------------------------------------------------------------------------
IV_RANK_LOW = 0.30
IV_RANK_MED = 0.50
IV_RANK_HIGH = 0.70

DELTA_BY_IV_TIER = {
    "low": None,
    "medium": 0.20,
    "high": 0.25,
    "very_high": 0.30,
}

WING_WIDTH_PCT = 0.03          # 3 % of spot
ENTRY_INTERVAL = 5             # every 5 trading days
DTE_TARGET = 30
DTE_MIN = 25
DTE_MAX = 45
RISK_FREE_RATE = 0.05
TAKE_PROFIT_PCT = 0.50         # close when position value <= 50% of credit
STOP_LOSS_PCT = 0.75           # close when loss >= 75% of max loss
SYNTHETIC_SPREAD_PCT = 0.01    # 1% for SPY (very liquid)

VIX_BUCKETS = [
    ("Very Low",  0, 15),
    ("Low",      15, 20),
    ("Medium",   20, 25),
    ("High",     25, 30),
    ("Very High",30, 100),
]


# ===================================================================
# Step 1 — Data preparation
# ===================================================================

def fetch_vix_history(start: str = "2019-01-01",
                      end: str = "2026-01-31") -> Dict[str, float]:
    """Download VIX daily closes from Yahoo Finance."""
    import yfinance as yf
    import pandas as pd

    log.info("Fetching VIX history from Yahoo Finance ...")
    vix = yf.download("^VIX", start=start, end=end, progress=False)
    if vix.empty:
        log.error("No VIX data returned")
        return {}
    if isinstance(vix.columns, pd.MultiIndex):
        vix.columns = vix.columns.get_level_values(0)
    out: Dict[str, float] = {}
    for idx, row in vix.iterrows():
        out[idx.strftime("%Y-%m-%d")] = float(row["Close"])
    log.info("  VIX: %d days, range %.1f – %.1f",
             len(out), min(out.values()), max(out.values()))
    return out


def ensure_spy_bars(start: str, end: str) -> None:
    """Make sure SPY daily bars are cached."""
    cache_db.init_db()
    data_fetcher.fetch_underlying_bars("SPY", start, end)


def get_spy_bars(start: str, end: str) -> List[Dict]:
    """Load cached SPY bars as list of dicts."""
    return cache_db.get_underlying_bars("SPY", start, end)


# ===================================================================
# Step 2 — Contract discovery
# ===================================================================

def _best_expiration(contracts: List[Dict], entry_date: str) -> Optional[str]:
    """Pick the expiration closest to DTE_TARGET among available contracts."""
    dt_entry = datetime.strptime(entry_date, "%Y-%m-%d").date()
    exps = set(c["expiration_date"] for c in contracts)
    best_exp, best_diff = None, 9999
    for e in exps:
        dte = (datetime.strptime(e, "%Y-%m-%d").date() - dt_entry).days
        diff = abs(dte - DTE_TARGET)
        if diff < best_diff:
            best_diff = diff
            best_exp = e
    return best_exp


def _nearest_contract(contracts: List[Dict], target_strike: float,
                      expiration: str) -> Optional[Dict]:
    """Find the contract nearest to *target_strike* at *expiration*."""
    filtered = [c for c in contracts if c["expiration_date"] == expiration]
    if not filtered:
        return None
    return min(filtered, key=lambda c: abs(c["strike_price"] - target_strike))


def find_condor_contracts(
    entry_date: str,
    spot: float,
    short_put_strike: float,
    long_put_strike: float,
    short_call_strike: float,
    long_call_strike: float,
) -> Optional[Dict]:
    """Discover real Polygon contracts for the four condor legs.

    Returns dict  {leg_name: {option_ticker, strike_price, expiration_date}}
    or None if any leg cannot be found.
    """
    puts = data_fetcher.fetch_options_contracts("SPY", entry_date, "put")
    calls = data_fetcher.fetch_options_contracts("SPY", entry_date, "call")

    if not puts or not calls:
        return None

    # Pick a common expiration (closest to DTE_TARGET) from puts
    exp = _best_expiration(puts, entry_date)
    if exp is None:
        return None

    # Verify calls have the same expiration
    call_exps = set(c["expiration_date"] for c in calls)
    if exp not in call_exps:
        # Try to find the closest matching call expiration
        exp_c = _best_expiration(calls, entry_date)
        if exp_c is None:
            return None
        # Use whichever expiration is present in both
        put_exps = set(c["expiration_date"] for c in puts)
        common = put_exps & call_exps
        if not common:
            return None
        dt_entry = datetime.strptime(entry_date, "%Y-%m-%d").date()
        exp = min(common,
                  key=lambda e: abs((datetime.strptime(e, "%Y-%m-%d").date()
                                     - dt_entry).days - DTE_TARGET))

    lp = _nearest_contract(puts,  long_put_strike,  exp)
    sp = _nearest_contract(puts,  short_put_strike, exp)
    sc = _nearest_contract(calls, short_call_strike, exp)
    lc = _nearest_contract(calls, long_call_strike,  exp)

    if any(x is None for x in [lp, sp, sc, lc]):
        return None

    # Validate structure: LP < SP < SC < LC
    if not (lp["strike_price"] < sp["strike_price"]
            < sc["strike_price"] < lc["strike_price"]):
        return None

    return {
        "long_put":   lp,
        "short_put":  sp,
        "short_call": sc,
        "long_call":  lc,
        "expiration": exp,
    }


# ===================================================================
# Step 3 — Position pricing with real data
# ===================================================================

def _fetch_leg_bars(option_ticker: str, start: str, end: str) -> None:
    """Ensure daily bars are cached for an option leg."""
    data_fetcher.fetch_option_bars(option_ticker, start, end)


def _get_close(option_ticker: str, bar_date: str) -> Optional[float]:
    """Get the close price for a leg on a given date."""
    bar = cache_db.get_option_bar(option_ticker, bar_date)
    if bar and bar["close"] and bar["close"] > 0:
        return bar["close"]
    return None


def price_condor_entry(contracts: Dict, entry_date: str
                       ) -> Optional[Dict]:
    """Price the condor entry from real option closes with synthetic spread.

    Returns dict with credit, max_loss, individual leg prices, or None.
    """
    exp = contracts["expiration"]

    tickers = {
        leg: contracts[leg]["option_ticker"]
        for leg in ("long_put", "short_put", "short_call", "long_call")
    }

    # Fetch bars for all four legs (entry_date through expiration)
    for tk in tickers.values():
        _fetch_leg_bars(tk, entry_date, exp)

    # Read close prices on entry date
    closes = {}
    for leg, tk in tickers.items():
        c = _get_close(tk, entry_date)
        if c is None:
            # Try next trading day as fallback
            dt = datetime.strptime(entry_date, "%Y-%m-%d").date()
            for offset in range(1, 4):
                alt = (dt + timedelta(days=offset)).isoformat()
                c = _get_close(tk, alt)
                if c is not None:
                    break
        if c is None:
            return None
        closes[leg] = c

    # Synthetic bid/ask (1% spread for SPY)
    half = SYNTHETIC_SPREAD_PCT / 2
    sp_bid = closes["short_put"]  * (1 - half)
    sp_ask = closes["short_put"]  * (1 + half)
    sc_bid = closes["short_call"] * (1 - half)
    sc_ask = closes["short_call"] * (1 + half)
    lp_bid = closes["long_put"]   * (1 - half)
    lp_ask = closes["long_put"]   * (1 + half)
    lc_bid = closes["long_call"]  * (1 - half)
    lc_ask = closes["long_call"]  * (1 + half)

    # Credit = sell shorts at bid, buy longs at ask
    credit = (sp_bid + sc_bid) - (lp_ask + lc_ask)
    if credit <= 0:
        return None

    put_width  = (contracts["short_put"]["strike_price"]
                  - contracts["long_put"]["strike_price"])
    call_width = (contracts["long_call"]["strike_price"]
                  - contracts["short_call"]["strike_price"])
    max_width  = max(put_width, call_width)
    max_loss   = (max_width - credit) * 100   # per contract

    return {
        "credit": round(credit, 4),
        "max_loss": round(max_loss, 2),
        "put_width": put_width,
        "call_width": call_width,
        "closes": closes,
        "tickers": tickers,
    }


def price_condor_on_date(tickers: Dict[str, str], price_date: str
                         ) -> Optional[float]:
    """Re-price the condor from real option closes on *price_date*.

    Returns the cost to close (debit) or None if data is missing.
    """
    closes = {}
    for leg, tk in tickers.items():
        c = _get_close(tk, price_date)
        if c is None:
            return None
        closes[leg] = c

    half = SYNTHETIC_SPREAD_PCT / 2
    # Cost to close = buy shorts at ask, sell longs at bid
    close_debit = ((closes["short_put"] * (1 + half)
                    + closes["short_call"] * (1 + half))
                   - (closes["long_put"] * (1 - half)
                      + closes["long_call"] * (1 - half)))
    return max(0.0, close_debit)


def intrinsic_settlement(spot: float, contracts: Dict) -> float:
    """Compute settlement value at expiration from intrinsic values."""
    sp_k = contracts["short_put"]["strike_price"]
    lp_k = contracts["long_put"]["strike_price"]
    sc_k = contracts["short_call"]["strike_price"]
    lc_k = contracts["long_call"]["strike_price"]

    # Put side intrinsic
    sp_intrinsic = max(0, sp_k - spot)
    lp_intrinsic = max(0, lp_k - spot)
    # Call side intrinsic
    sc_intrinsic = max(0, spot - sc_k)
    lc_intrinsic = max(0, spot - lc_k)

    # Position value at expiration (what it costs to settle the shorts
    # minus what you recover from the longs)
    settlement = (sp_intrinsic + sc_intrinsic) - (lp_intrinsic + lc_intrinsic)
    return max(0.0, settlement)


# ===================================================================
# Step 4 — Trade simulation
# ===================================================================

def select_delta_tier(iv_rank: float) -> Tuple[Optional[float], str]:
    """Select short delta and tier name from IV rank."""
    if iv_rank < IV_RANK_LOW:
        return None, "low"
    elif iv_rank < IV_RANK_MED:
        return DELTA_BY_IV_TIER["medium"], "medium"
    elif iv_rank < IV_RANK_HIGH:
        return DELTA_BY_IV_TIER["high"], "high"
    else:
        return DELTA_BY_IV_TIER["very_high"], "very_high"


def compute_vix_iv_rank(vix_today: float,
                        vix_history: Dict[str, float],
                        as_of: str,
                        lookback: int = 252) -> Optional[float]:
    """IV Rank using VIX history (252-day range)."""
    sorted_dates = sorted(d for d in vix_history if d <= as_of)
    if len(sorted_dates) < 20:
        return None
    trail = [vix_history[d] for d in sorted_dates[-lookback:]]
    return calculate_iv_rank(vix_today, trail, lookback)


def simulate_real_condor(
    entry_date: str,
    spot: float,
    vix: float,
    vix_history: Dict[str, float],
    spy_bars: List[Dict],
    bar_idx_map: Dict[str, int],
) -> Optional[Dict]:
    """Simulate a single real-data iron condor trade.

    Returns a result dict or None if the trade cannot be constructed.
    """
    # 1. IV rank from VIX history
    iv_rank = compute_vix_iv_rank(vix, vix_history, entry_date)
    if iv_rank is None:
        return None

    # 2. Delta selection
    short_delta, iv_tier = select_delta_tier(iv_rank)
    if short_delta is None:
        return None   # IV too low

    # 3. BS target strikes using VIX as IV
    vix_decimal = vix / 100.0     # VIX 25 -> 0.25
    dte_years = DTE_TARGET / 365.0

    sp_raw = find_strike_for_delta(spot, dte_years, RISK_FREE_RATE,
                                   vix_decimal, -short_delta, "P")
    sc_raw = find_strike_for_delta(spot, dte_years, RISK_FREE_RATE,
                                   vix_decimal, short_delta, "C")
    if sp_raw is None or sc_raw is None:
        return None

    # Round to $1 (SPY strike increments)
    sp_strike = round(sp_raw)
    sc_strike = round(sc_raw)

    # Wing width
    wing = max(round(spot * WING_WIDTH_PCT), 1)
    lp_strike = sp_strike - wing
    lc_strike = sc_strike + wing

    if lp_strike >= sp_strike or lc_strike <= sc_strike:
        return None

    # 4. Find real contracts
    contracts = find_condor_contracts(
        entry_date, spot, sp_strike, lp_strike, sc_strike, lc_strike)
    if contracts is None:
        return None

    # 5. Price entry
    pricing = price_condor_entry(contracts, entry_date)
    if pricing is None:
        return None

    credit = pricing["credit"]
    max_loss = pricing["max_loss"]
    tickers = pricing["tickers"]
    exp_date = contracts["expiration"]

    # Exit thresholds
    tp_target = credit * TAKE_PROFIT_PCT           # close when cost <= this
    sl_loss   = max_loss * STOP_LOSS_PCT            # max acceptable loss

    # 6. Walk each trading day from entry+1 to expiration
    entry_idx = bar_idx_map.get(entry_date)
    if entry_idx is None:
        return None

    exit_date = None
    exit_reason = None
    exit_pnl = None
    side_breached = None

    exp_dt = datetime.strptime(exp_date, "%Y-%m-%d").date()

    for i in range(entry_idx + 1, len(spy_bars)):
        bar = spy_bars[i]
        d = bar["bar_date"]
        d_dt = datetime.strptime(d, "%Y-%m-%d").date()

        if d_dt > exp_dt:
            break   # past expiration — handle below

        current_spot = bar["close"]

        # On expiration day, always settle at intrinsic
        if d_dt == exp_dt:
            settle_cost = intrinsic_settlement(current_spot, contracts)
            pnl = (credit - settle_cost) * 100
            exit_date = d
            exit_reason = "expiration"
            exit_pnl = pnl
            if current_spot < contracts["short_put"]["strike_price"]:
                side_breached = "put"
            elif current_spot > contracts["short_call"]["strike_price"]:
                side_breached = "call"
            break

        # Intra-life: re-price from real option closes
        close_cost = price_condor_on_date(tickers, d)
        if close_cost is None:
            continue   # no data this day, keep walking

        pnl = (credit - close_cost) * 100

        # Take profit
        if close_cost <= tp_target:
            exit_date = d
            exit_reason = "take_profit"
            exit_pnl = pnl
            break

        # Stop loss
        if pnl <= -sl_loss:
            exit_date = d
            exit_reason = "stop_loss"
            exit_pnl = pnl
            if current_spot < contracts["short_put"]["strike_price"]:
                side_breached = "put"
            elif current_spot > contracts["short_call"]["strike_price"]:
                side_breached = "call"
            break

    # If we never hit an exit or expiration bar (data gap), settle
    # at intrinsic using last available SPY close before expiration.
    if exit_date is None:
        # Find last bar on or before expiration
        for i in range(min(entry_idx + DTE_MAX + 5, len(spy_bars) - 1),
                       entry_idx, -1):
            bar = spy_bars[i]
            d_dt = datetime.strptime(bar["bar_date"], "%Y-%m-%d").date()
            if d_dt <= exp_dt:
                settle_cost = intrinsic_settlement(bar["close"], contracts)
                exit_pnl = (credit - settle_cost) * 100
                exit_date = bar["bar_date"]
                exit_reason = "expiration_fallback"
                if bar["close"] < contracts["short_put"]["strike_price"]:
                    side_breached = "put"
                elif bar["close"] > contracts["short_call"]["strike_price"]:
                    side_breached = "call"
                break

    if exit_pnl is None:
        return None

    return {
        "entry_date": entry_date,
        "expiration": exp_date,
        "exit_date": exit_date,
        "spot": spot,
        "vix": vix,
        "iv_rank": round(iv_rank, 4),
        "iv_tier": iv_tier,
        "short_delta": short_delta,
        "long_put":   contracts["long_put"]["strike_price"],
        "short_put":  contracts["short_put"]["strike_price"],
        "short_call": contracts["short_call"]["strike_price"],
        "long_call":  contracts["long_call"]["strike_price"],
        "credit": credit,
        "max_loss": max_loss,
        "pnl": round(exit_pnl, 2),
        "won": exit_pnl > 0,
        "exit_reason": exit_reason,
        "side_breached": side_breached,
        "put_width": pricing["put_width"],
        "call_width": pricing["call_width"],
    }


# ===================================================================
# Step 5 — Main backtest loop
# ===================================================================

def run_real_condor_backtest(
    start: str = "2020-01-01",
    end: str = "2025-12-31",
) -> List[Dict]:
    """Run the full real-data condor backtest on SPY."""

    log.info("=" * 70)
    log.info("REAL-DATA IRON CONDOR BACKTEST  —  SPY  %s to %s", start, end)
    log.info("=" * 70)

    # --- data prep ---
    # Need extra lookback for VIX IV rank (252 trading days ~ 1 year)
    lookback_start = (datetime.strptime(start, "%Y-%m-%d").date()
                      - timedelta(days=400)).isoformat()

    vix_history = fetch_vix_history(lookback_start, end)
    if not vix_history:
        log.error("Cannot proceed without VIX data")
        return []

    ensure_spy_bars(lookback_start, end)
    spy_bars = get_spy_bars(start, end)
    if not spy_bars:
        log.error("No SPY bars available")
        return []

    bar_idx_map = {b["bar_date"]: i for i, b in enumerate(spy_bars)}
    log.info("SPY bars loaded: %d trading days", len(spy_bars))

    # --- walk dates ---
    trades: List[Dict] = []
    skipped_iv = 0
    skipped_data = 0
    last_entry_idx = -ENTRY_INTERVAL

    for idx in range(len(spy_bars)):
        if idx - last_entry_idx < ENTRY_INTERVAL:
            continue

        bar = spy_bars[idx]
        entry_date = bar["bar_date"]
        spot = bar["close"]

        vix = vix_history.get(entry_date)
        if vix is None:
            # Try adjacent days
            dt = datetime.strptime(entry_date, "%Y-%m-%d").date()
            for offset in (-1, 1, -2, 2):
                alt = (dt + timedelta(days=offset)).isoformat()
                vix = vix_history.get(alt)
                if vix is not None:
                    break
        if vix is None:
            continue

        # Quick IV rank pre-check
        iv_rank = compute_vix_iv_rank(vix, vix_history, entry_date)
        if iv_rank is not None and iv_rank < IV_RANK_LOW:
            skipped_iv += 1
            last_entry_idx = idx   # still counts toward spacing
            continue

        # Attempt trade
        result = simulate_real_condor(
            entry_date, spot, vix, vix_history, spy_bars, bar_idx_map)

        if result is None:
            skipped_data += 1
            continue

        trades.append(result)
        last_entry_idx = idx

        if len(trades) % 10 == 0:
            log.info("  ... %d trades so far (entry %s)", len(trades), entry_date)

    log.info("Backtest complete: %d trades  (%d skipped IV low, %d skipped data)",
             len(trades), skipped_iv, skipped_data)
    return trades


# ===================================================================
# Step 6 — Results & comparison reporting
# ===================================================================

def get_vix_bucket(vix: float) -> str:
    for name, lo, hi in VIX_BUCKETS:
        if lo <= vix < hi:
            return name
    return "Unknown"


def print_results(trades: List[Dict]) -> None:
    if not trades:
        print("\nNo trades to report.")
        return

    wins = [t for t in trades if t["won"]]
    losses = [t for t in trades if not t["won"]]
    total_pnl = sum(t["pnl"] for t in trades)
    avg_pnl = total_pnl / len(trades)
    win_rate = len(wins) / len(trades)
    avg_credit = sum(t["credit"] for t in trades) / len(trades)
    avg_max_loss = sum(t["max_loss"] for t in trades) / len(trades)

    print()
    print("=" * 70)
    print("REAL-DATA IRON CONDOR RESULTS  —  SPY")
    print("=" * 70)
    print(f"  Total trades:    {len(trades)}")
    print(f"  Winners:         {len(wins)}  ({win_rate:.1%})")
    print(f"  Losers:          {len(losses)}")
    print(f"  Total P&L:       ${total_pnl:>+,.2f}")
    print(f"  Avg P&L/trade:   ${avg_pnl:>+,.2f}")
    print(f"  Avg credit:      ${avg_credit:.4f} /share")
    print(f"  Avg max loss:    ${avg_max_loss:,.2f} /contract")

    # Exit reasons
    reasons = defaultdict(int)
    for t in trades:
        reasons[t["exit_reason"]] += 1
    print(f"\n  Exit reasons:")
    for r, n in sorted(reasons.items(), key=lambda x: -x[1]):
        print(f"    {r:<22} {n:>4}  ({n/len(trades):.1%})")

    # Side breached
    put_breaches = sum(1 for t in trades if t["side_breached"] == "put")
    call_breaches = sum(1 for t in trades if t["side_breached"] == "call")
    print(f"\n  Put breaches:    {put_breaches}")
    print(f"  Call breaches:   {call_breaches}")

    # --- By IV tier ---
    print()
    print("-" * 70)
    print("PERFORMANCE BY IV TIER")
    print("-" * 70)
    print(f"{'Tier':<12} | {'Trades':>7} | {'Win Rate':>9} | {'Total P&L':>12} | {'Avg P&L':>10}")
    print("-" * 70)
    for tier in ("medium", "high", "very_high"):
        tier_t = [t for t in trades if t["iv_tier"] == tier]
        if not tier_t:
            print(f"{tier:<12} |     N/A |")
            continue
        tw = sum(1 for t in tier_t if t["won"])
        tp = sum(t["pnl"] for t in tier_t)
        print(f"{tier:<12} | {len(tier_t):>7} | {tw/len(tier_t):>8.1%} | "
              f"${tp:>+10,.2f} | ${tp/len(tier_t):>+9.2f}")

    # --- By VIX bucket ---
    print()
    print("-" * 70)
    print("PERFORMANCE BY VIX AT ENTRY")
    print("-" * 70)
    print(f"{'VIX Bucket':<12} | {'Range':^9} | {'Trades':>7} | {'Win Rate':>9} | "
          f"{'Total P&L':>12} | {'Avg P&L':>10}")
    print("-" * 70)

    total_vix_trades = 0
    total_vix_pnl = 0
    total_vix_wins = 0

    for bname, blo, bhi in VIX_BUCKETS:
        bt = [t for t in trades if blo <= t["vix"] < bhi]
        if not bt:
            rng = f"{blo}-{bhi}" if bhi < 100 else f"{blo}+"
            print(f"{bname:<12} | {rng:^9} |     N/A |")
            continue
        bw = sum(1 for t in bt if t["won"])
        bp = sum(t["pnl"] for t in bt)
        rng = f"{blo}-{bhi}" if bhi < 100 else f"{blo}+"
        print(f"{bname:<12} | {rng:^9} | {len(bt):>7} | {bw/len(bt):>8.1%} | "
              f"${bp:>+10,.2f} | ${bp/len(bt):>+9.2f}")
        total_vix_trades += len(bt)
        total_vix_pnl += bp
        total_vix_wins += bw

    print("-" * 70)
    if total_vix_trades > 0:
        print(f"{'TOTAL':<12} | {'':^9} | {total_vix_trades:>7} | "
              f"{total_vix_wins/total_vix_trades:>8.1%} | "
              f"${total_vix_pnl:>+10,.2f} | ${total_vix_pnl/total_vix_trades:>+9.2f}")

    # --- Monthly P&L timeline ---
    print()
    print("-" * 70)
    print("MONTHLY P&L TIMELINE")
    print("-" * 70)
    monthly: Dict[str, float] = defaultdict(float)
    monthly_n: Dict[str, int] = defaultdict(int)
    for t in trades:
        mo = t["entry_date"][:7]  # YYYY-MM
        monthly[mo] += t["pnl"]
        monthly_n[mo] += 1
    for mo in sorted(monthly):
        bar_len = int(abs(monthly[mo]) / 50)
        bar_char = "+" if monthly[mo] >= 0 else "-"
        bar = bar_char * min(bar_len, 40)
        print(f"  {mo}  {monthly_n[mo]:>3} trades  ${monthly[mo]:>+9.2f}  {bar}")

    # --- Sample trades ---
    print()
    print("-" * 70)
    print("FIRST 10 TRADES (detail)")
    print("-" * 70)
    for i, t in enumerate(trades[:10]):
        print(f"\n  Trade #{i+1}:")
        print(f"    Entry:     {t['entry_date']}   SPY ${t['spot']:.2f}   VIX {t['vix']:.1f}")
        print(f"    IV Rank:   {t['iv_rank']:.2f}  ({t['iv_tier']})  delta={t['short_delta']}")
        print(f"    Strikes:   LP {t['long_put']}  SP {t['short_put']}  "
              f"SC {t['short_call']}  LC {t['long_call']}")
        print(f"    Credit:    ${t['credit']:.4f}/sh   Max loss: ${t['max_loss']:.2f}/ct")
        print(f"    Exit:      {t['exit_date']}  reason={t['exit_reason']}")
        print(f"    P&L:       ${t['pnl']:>+.2f}  {'WIN' if t['won'] else 'LOSS'}"
              + (f"  ({t['side_breached']} breached)" if t['side_breached'] else ""))


# ===================================================================
# CLI entry point
# ===================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Real-data iron condor backtest for SPY")
    parser.add_argument("--year", type=int, default=None,
                        help="Run for a single year (e.g. 2024)")
    parser.add_argument("--start", type=str, default=None,
                        help="Start date YYYY-MM-DD")
    parser.add_argument("--end", type=str, default=None,
                        help="End date YYYY-MM-DD")
    args = parser.parse_args()

    if args.year:
        start = f"{args.year}-01-01"
        end   = f"{args.year}-12-31"
    elif args.start and args.end:
        start = args.start
        end   = args.end
    else:
        start = "2020-01-01"
        end   = "2025-12-31"

    t0 = time.time()
    trades = run_real_condor_backtest(start, end)
    elapsed = time.time() - t0

    print_results(trades)

    print(f"\nCompleted in {elapsed:.1f}s  ({len(trades)} trades)")


if __name__ == "__main__":
    main()
