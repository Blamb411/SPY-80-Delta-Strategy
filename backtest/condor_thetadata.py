#!/usr/bin/env python3
"""
ThetaData Iron Condor Backtester (2012-2026)
=============================================
Uses real historical bid/ask quotes from ThetaData for option pricing,
with Black-Scholes synthetic fallback for Jan-May 2012 when ThetaData
coverage is unavailable.

Replaces the Massive/Polygon-based condor_real_data.py with ThetaData
as the data source, extending coverage back to June 2012.

Usage:
    python condor_thetadata.py                         # full 2012-2026
    python condor_thetadata.py --year 2024             # single year smoke test
    python condor_thetadata.py --start 2020 --end 2025 # date range
    python condor_thetadata.py --synthetic-only         # B-S only (no ThetaData)
    python condor_thetadata.py --call-delta-offset -0.05  # asymmetric call wing
    python condor_thetadata.py --export-csv trades.csv  # export results
"""

import os
import sys
import math
import time
import csv
import logging
import argparse
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_this_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)
if _this_dir not in sys.path:
    sys.path.insert(0, _this_dir)

from backtest.black_scholes import (
    find_strike_for_delta,
    calculate_iv_rank,
    calculate_condor_price_realistic,
    price_condor_to_close,
    black_scholes_price,
)
from backtest.thetadata_client import ThetaDataClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("condor_theta")

# ---------------------------------------------------------------------------
# Strategy constants (match condor_real_data.py exactly)
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

WING_WIDTH_PCT = 0.03          # 3% of spot
MIN_WING_WIDTH = 5.0           # minimum wing width in dollars
MAX_CREDIT_RATIO = 0.60        # reject if credit > 60% of wing width
ENTRY_INTERVAL = 5             # every 5 trading days
DTE_TARGET = 30
DTE_MIN = 25
DTE_MAX = 45
RISK_FREE_RATE = 0.05
TAKE_PROFIT_PCT = 0.50         # close when position value <= 50% of credit
STOP_LOSS_PCT = 0.75           # close when loss >= 75% of max loss
SYNTHETIC_SPREAD_PCT = 0.05    # 5% synthetic spread for Jan-May 2012

# ThetaData coverage starts June 2012
THETADATA_START = "2012-06-01"

VIX_BUCKETS = [
    ("Very Low",  0, 15),
    ("Low",      15, 20),
    ("Medium",   20, 25),
    ("High",     25, 30),
    ("Very High", 30, 100),
]


# ===================================================================
# Step 1 -- IV Rank and Delta Selection
# ===================================================================

def compute_vix_iv_rank(vix_today: float, vix_history: Dict[str, float],
                        as_of: str, lookback: int = 252) -> Optional[float]:
    """IV Rank using VIX history (252-day range)."""
    sorted_dates = sorted(d for d in vix_history if d <= as_of)
    if len(sorted_dates) < 20:
        return None
    trail = [vix_history[d] for d in sorted_dates[-lookback:]]
    return calculate_iv_rank(vix_today, trail, lookback)


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


# ===================================================================
# Step 2 -- Strike Construction
# ===================================================================

def build_condor_strikes(spot: float, vix: float,
                         put_delta: float,
                         call_delta: float = None) -> Optional[Dict[str, float]]:
    """
    Build 4 target strikes using B-S delta targeting.

    Args:
        spot: Current underlying price
        vix: VIX level (percentage, e.g. 20 for 20%)
        put_delta: Delta for put-side short strike (e.g. 0.20)
        call_delta: Delta for call-side short strike. If None, uses put_delta.

    Returns dict with keys: long_put, short_put, short_call, long_call
    or None if delta solve fails.
    """
    if call_delta is None:
        call_delta = put_delta

    vix_decimal = vix / 100.0
    dte_years = DTE_TARGET / 365.0

    sp_raw = find_strike_for_delta(spot, dte_years, RISK_FREE_RATE,
                                   vix_decimal, -put_delta, "P")
    sc_raw = find_strike_for_delta(spot, dte_years, RISK_FREE_RATE,
                                   vix_decimal, call_delta, "C")

    if sp_raw is None or sc_raw is None:
        return None

    # Round to $1 increments (SPY)
    sp_strike = round(sp_raw)
    sc_strike = round(sc_raw)

    # Validate shorts are OTM
    if sp_strike >= spot or sc_strike <= spot:
        return None

    # Wing width with minimum floor
    wing = max(round(spot * WING_WIDTH_PCT), MIN_WING_WIDTH)
    lp_strike = sp_strike - wing
    lc_strike = sc_strike + wing

    if lp_strike >= sp_strike or lc_strike <= sc_strike:
        return None

    return {
        "long_put": float(lp_strike),
        "short_put": float(sp_strike),
        "short_call": float(sc_strike),
        "long_call": float(lc_strike),
    }


def validate_and_snap_strikes(client: ThetaDataClient, targets: Dict[str, float],
                               root: str, expiration: str,
                               spot: float = None) -> Optional[Dict[str, float]]:
    """
    Snap 4 target strikes to nearest real ThetaData strikes.
    Validates LP < SP < SC < LC after snapping, and that shorts are OTM.

    Returns snapped strikes dict or None if validation fails.
    """
    snapped = {}
    for leg in ("long_put", "short_put", "short_call", "long_call"):
        s = client.snap_strike(root, expiration, targets[leg])
        if s is None:
            return None
        snapped[leg] = s

    # Validate structure
    if not (snapped["long_put"] < snapped["short_put"]
            < snapped["short_call"] < snapped["long_call"]):
        return None

    # Validate shorts are OTM
    if spot is not None:
        if snapped["short_put"] >= spot or snapped["short_call"] <= spot:
            return None

    # Validate minimum wing width after snapping
    put_width = snapped["short_put"] - snapped["long_put"]
    call_width = snapped["long_call"] - snapped["short_call"]
    if put_width < MIN_WING_WIDTH or call_width < MIN_WING_WIDTH:
        return None

    return snapped


# ===================================================================
# Step 3 -- ThetaData Pricing
# ===================================================================

def price_condor_entry_thetadata(client: ThetaDataClient, root: str,
                                  expiration: str, strikes: Dict[str, float],
                                  entry_date: str) -> Optional[Dict]:
    """
    Price condor entry using real ThetaData bid/ask quotes.

    Pre-fetches all quotes through expiration for later daily management.
    Returns dict with credit, max_loss, or None if data missing.
    """
    legs = {
        "long_put":   ("P", strikes["long_put"]),
        "short_put":  ("P", strikes["short_put"]),
        "short_call": ("C", strikes["short_call"]),
        "long_call":  ("C", strikes["long_call"]),
    }

    # Pre-fetch EOD data for all 4 legs (entry through expiration)
    prefetched = {}
    for leg_name, (right, strike) in legs.items():
        eod_data = client.prefetch_option_life(root, expiration, strike, right, entry_date)
        prefetched[leg_name] = eod_data

    # Read entry-date bid/ask for each leg
    # If exact entry date not available, use earliest available date from prefetch
    entry_quotes = {}
    for leg_name, (right, strike) in legs.items():
        q = client.get_bid_ask(root, expiration, strike, right, entry_date)
        if q is None or q["bid"] <= 0 or q["ask"] <= 0:
            # Try next few trading days as fallback
            dt = datetime.strptime(entry_date, "%Y-%m-%d").date()
            found = False
            for offset in range(1, 6):
                alt = (dt + timedelta(days=offset)).isoformat()
                q = client.get_bid_ask(root, expiration, strike, right, alt)
                if q and q["bid"] > 0 and q["ask"] > 0:
                    found = True
                    break
            # If still not found, use earliest row from prefetch
            if not found and prefetched[leg_name]:
                for row in prefetched[leg_name]:
                    bid = row.get("bid", 0)
                    ask = row.get("ask", 0)
                    if bid > 0 and ask > 0:
                        q = {"bid": bid, "ask": ask, "quote_date": row["bar_date"]}
                        found = True
                        break
            if not found:
                return None
        entry_quotes[leg_name] = q

    # Credit = sell shorts at bid, buy longs at ask
    credit = (entry_quotes["short_put"]["bid"] + entry_quotes["short_call"]["bid"]
              - entry_quotes["long_put"]["ask"] - entry_quotes["long_call"]["ask"])

    if credit <= 0:
        return None

    put_width = strikes["short_put"] - strikes["long_put"]
    call_width = strikes["long_call"] - strikes["short_call"]
    max_width = max(put_width, call_width)

    # Reject if credit exceeds max ratio of wing width
    # (indicates ITM legs or stale/crossed quotes)
    if max_width > 0 and credit / max_width > MAX_CREDIT_RATIO:
        return None

    max_loss = (max_width - credit) * 100  # per contract

    return {
        "credit": round(credit, 4),
        "max_loss": round(max_loss, 2),
        "put_width": put_width,
        "call_width": call_width,
        "data_source": "thetadata",
        "entry_quotes": entry_quotes,
    }


# ===================================================================
# Step 4 -- Synthetic (B-S) Pricing (Jan-May 2012 fallback)
# ===================================================================

def price_condor_entry_synthetic(spot: float, strikes: Dict[str, float],
                                  vix: float, dte: int) -> Optional[Dict]:
    """
    Price condor entry using Black-Scholes with synthetic bid/ask spread.
    Used for dates before ThetaData coverage (Jan-May 2012).
    """
    vix_decimal = vix / 100.0
    t_years = dte / 365.0

    result = calculate_condor_price_realistic(
        spot,
        strikes["long_put"],
        strikes["short_put"],
        strikes["short_call"],
        strikes["long_call"],
        t_years,
        RISK_FREE_RATE,
        vix_decimal,
        bid_ask_spread_pct=SYNTHETIC_SPREAD_PCT,
        use_skew=True,
    )

    if result is None or result["open_credit"] <= 0:
        return None

    # Reject if credit exceeds max ratio of wing width
    max_width = max(result["put_width"], result["call_width"])
    if max_width > 0 and result["open_credit"] / max_width > MAX_CREDIT_RATIO:
        return None

    return {
        "credit": round(result["open_credit"], 4),
        "max_loss": round(result["max_loss"], 2),
        "put_width": result["put_width"],
        "call_width": result["call_width"],
        "data_source": "synthetic",
    }


# ===================================================================
# Step 5 -- Daily Re-pricing
# ===================================================================

def price_condor_on_date_thetadata(client: ThetaDataClient, root: str,
                                    expiration: str, strikes: Dict[str, float],
                                    price_date: str) -> Optional[float]:
    """
    Re-price condor on a given date using cached ThetaData quotes.

    Returns cost to close (debit) or None if data missing.
    All data should already be cached from prefetch_option_life.
    """
    legs = {
        "long_put":   ("P", strikes["long_put"]),
        "short_put":  ("P", strikes["short_put"]),
        "short_call": ("C", strikes["short_call"]),
        "long_call":  ("C", strikes["long_call"]),
    }

    quotes = {}
    for leg_name, (right, strike) in legs.items():
        q = client.get_bid_ask(root, expiration, strike, right, price_date)
        if q is None or (q["bid"] <= 0 and q["ask"] <= 0):
            return None
        quotes[leg_name] = q

    # Cost to close = buy shorts at ask, sell longs at bid
    close_debit = (quotes["short_put"]["ask"] + quotes["short_call"]["ask"]
                   - quotes["long_put"]["bid"] - quotes["long_call"]["bid"])

    return max(0.0, close_debit)


def price_condor_on_date_synthetic(spot: float, strikes: Dict[str, float],
                                    vix: float, dte: int) -> Optional[float]:
    """
    Re-price condor on a given date using Black-Scholes.
    Used for synthetic path (Jan-May 2012).
    """
    if dte <= 0:
        return None

    vix_decimal = vix / 100.0
    t_years = dte / 365.0

    close_cost = price_condor_to_close(
        spot,
        strikes["long_put"],
        strikes["short_put"],
        strikes["short_call"],
        strikes["long_call"],
        t_years,
        RISK_FREE_RATE,
        vix_decimal,
        bid_ask_spread_pct=SYNTHETIC_SPREAD_PCT,
        use_skew=True,
    )

    return close_cost


# ===================================================================
# Step 6 -- Intrinsic Settlement
# ===================================================================

def intrinsic_settlement(spot: float, strikes: Dict[str, float]) -> float:
    """Compute settlement value at expiration from intrinsic values."""
    sp_intrinsic = max(0, strikes["short_put"] - spot)
    lp_intrinsic = max(0, strikes["long_put"] - spot)
    sc_intrinsic = max(0, spot - strikes["short_call"])
    lc_intrinsic = max(0, spot - strikes["long_call"])

    settlement = (sp_intrinsic + sc_intrinsic) - (lp_intrinsic + lc_intrinsic)
    return max(0.0, settlement)


# ===================================================================
# Step 7 -- Trade Simulation
# ===================================================================

def simulate_condor_trade(
    client: ThetaDataClient,
    entry_date: str,
    spot: float,
    vix: float,
    vix_history: Dict[str, float],
    spy_bars: List[Dict],
    bar_idx_map: Dict[str, int],
    use_synthetic: bool = False,
    call_delta_offset: float = 0.0,
) -> Optional[Dict]:
    """
    Simulate a single iron condor trade.

    Dispatches to ThetaData or synthetic path based on date and flags.

    Args:
        call_delta_offset: Offset applied to call delta (e.g. -0.05 pushes
            calls further OTM). Default 0.0 preserves symmetric behavior.

    Returns a result dict or None if the trade cannot be constructed.
    """
    root = "SPY"

    # 1. IV rank from VIX history
    iv_rank = compute_vix_iv_rank(vix, vix_history, entry_date)
    if iv_rank is None:
        return None

    # 2. Delta selection
    short_delta, iv_tier = select_delta_tier(iv_rank)
    if short_delta is None:
        return None  # IV too low

    # 2b. Asymmetric call delta
    put_delta = short_delta
    call_delta = short_delta + call_delta_offset
    if call_delta <= 0:
        return None  # offset too aggressive

    # 3. Build target strikes via B-S delta
    targets = build_condor_strikes(spot, vix, put_delta, call_delta)
    if targets is None:
        return None

    # 4. Determine data path
    is_synthetic = use_synthetic or entry_date < THETADATA_START

    if is_synthetic:
        # Synthetic path: use B-S strikes directly (rounded to $1)
        strikes = targets
        dte = DTE_TARGET
        expiration_date = (datetime.strptime(entry_date, "%Y-%m-%d").date()
                          + timedelta(days=dte)).isoformat()

        pricing = price_condor_entry_synthetic(spot, strikes, vix, dte)
        if pricing is None:
            return None

    else:
        # ThetaData path: snap to real strikes and use real quotes
        expiration = client.find_nearest_expiration(root, entry_date,
                                                     DTE_TARGET, DTE_MIN, DTE_MAX)
        if expiration is None:
            return None

        strikes = validate_and_snap_strikes(client, targets, root, expiration, spot)
        if strikes is None:
            return None

        expiration_date = expiration

        pricing = price_condor_entry_thetadata(client, root, expiration,
                                                strikes, entry_date)
        if pricing is None:
            return None

    credit = pricing["credit"]
    max_loss = pricing["max_loss"]
    data_source = pricing["data_source"]

    # Exit thresholds
    tp_target = credit * TAKE_PROFIT_PCT
    sl_loss = max_loss * STOP_LOSS_PCT

    # 5. Walk each trading day from entry+1 to expiration
    entry_idx = bar_idx_map.get(entry_date)
    if entry_idx is None:
        return None

    exit_date = None
    exit_reason = None
    exit_pnl = None
    side_breached = None

    exp_dt = datetime.strptime(expiration_date, "%Y-%m-%d").date()

    for i in range(entry_idx + 1, len(spy_bars)):
        bar = spy_bars[i]
        d = bar["bar_date"]
        d_dt = datetime.strptime(d, "%Y-%m-%d").date()

        if d_dt > exp_dt:
            break

        current_spot = bar["close"]

        # On expiration day: intrinsic settlement
        if d_dt == exp_dt:
            settle_cost = intrinsic_settlement(current_spot, strikes)
            pnl = (credit - settle_cost) * 100
            exit_date = d
            exit_reason = "expiration"
            exit_pnl = pnl
            if current_spot < strikes["short_put"]:
                side_breached = "put"
            elif current_spot > strikes["short_call"]:
                side_breached = "call"
            break

        # Daily re-pricing
        if is_synthetic:
            days_left = (exp_dt - d_dt).days
            vix_today = vix_history.get(d)
            if vix_today is None:
                continue
            close_cost = price_condor_on_date_synthetic(
                current_spot, strikes, vix_today, days_left)
        else:
            close_cost = price_condor_on_date_thetadata(
                client, root, expiration_date, strikes, d)

        if close_cost is None:
            continue

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
            if current_spot < strikes["short_put"]:
                side_breached = "put"
            elif current_spot > strikes["short_call"]:
                side_breached = "call"
            break

    # Fallback: settle at intrinsic using last bar before expiration
    if exit_date is None:
        for i in range(min(entry_idx + DTE_MAX + 10, len(spy_bars) - 1),
                       entry_idx, -1):
            bar = spy_bars[i]
            d_dt = datetime.strptime(bar["bar_date"], "%Y-%m-%d").date()
            if d_dt <= exp_dt:
                settle_cost = intrinsic_settlement(bar["close"], strikes)
                exit_pnl = (credit - settle_cost) * 100
                exit_date = bar["bar_date"]
                exit_reason = "expiration_fallback"
                if bar["close"] < strikes["short_put"]:
                    side_breached = "put"
                elif bar["close"] > strikes["short_call"]:
                    side_breached = "call"
                break

    if exit_pnl is None:
        return None

    dte_actual = (exp_dt - datetime.strptime(entry_date, "%Y-%m-%d").date()).days

    return {
        "entry_date": entry_date,
        "expiration": expiration_date,
        "exit_date": exit_date,
        "spot": spot,
        "vix": vix,
        "iv_rank": round(iv_rank, 4),
        "iv_tier": iv_tier,
        "short_delta": short_delta,
        "put_delta": put_delta,
        "call_delta": round(call_delta, 4),
        "call_delta_offset": call_delta_offset,
        "long_put": strikes["long_put"],
        "short_put": strikes["short_put"],
        "short_call": strikes["short_call"],
        "long_call": strikes["long_call"],
        "credit": credit,
        "max_loss": max_loss,
        "pnl": round(exit_pnl, 2),
        "won": exit_pnl > 0,
        "exit_reason": exit_reason,
        "side_breached": side_breached,
        "put_width": pricing["put_width"],
        "call_width": pricing["call_width"],
        "data_source": data_source,
        "dte": dte_actual,
    }


# ===================================================================
# Step 8 -- Main Backtest Loop
# ===================================================================

def run_backtest(start_year: int = 2012, end_year: int = 2026,
                 synthetic_only: bool = False,
                 call_delta_offset: float = 0.0) -> List[Dict]:
    """
    Run the full ThetaData iron condor backtest.

    Args:
        start_year: First year to backtest
        end_year: Last year to backtest
        synthetic_only: If True, use B-S for all dates (no ThetaData needed)
        call_delta_offset: Offset applied to call delta (e.g. -0.05 pushes
            calls further OTM). Default 0.0 preserves symmetric behavior.
    """
    start = f"{start_year}-01-01"
    end = f"{end_year}-12-31"

    log.info("=" * 70)
    log.info("THETADATA IRON CONDOR BACKTEST  --  SPY  %s to %s", start, end)
    if synthetic_only:
        log.info("  MODE: Synthetic only (Black-Scholes)")
    else:
        log.info("  MODE: ThetaData real quotes + synthetic fallback")
    if call_delta_offset != 0.0:
        log.info("  CALL DELTA OFFSET: %+.2f (asymmetric wings)", call_delta_offset)
    log.info("=" * 70)

    # Initialize client
    client = ThetaDataClient()

    # Check ThetaData connection (not needed for synthetic-only)
    if not synthetic_only:
        if not client.connect():
            log.warning("Theta Terminal not available. Falling back to synthetic-only mode.")
            synthetic_only = True

    # Phase 0: One-time data fetch
    lookback_start = (datetime.strptime(start, "%Y-%m-%d").date()
                      - timedelta(days=400)).isoformat()

    vix_history = client.fetch_vix_history(lookback_start, end)
    if not vix_history:
        log.error("Cannot proceed without VIX data")
        return []

    spy_bars = client.fetch_spy_bars(start, end)
    if not spy_bars:
        log.error("No SPY bars available")
        return []

    # Also load the lookback period bars for any overlap
    lookback_bars = client.fetch_spy_bars(lookback_start, end)

    bar_idx_map = {b["bar_date"]: i for i, b in enumerate(spy_bars)}
    log.info("SPY bars: %d trading days (%s to %s)",
             len(spy_bars), spy_bars[0]["bar_date"], spy_bars[-1]["bar_date"])

    # Phase 1: Walk dates
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

        # Get VIX for this date
        vix = vix_history.get(entry_date)
        if vix is None:
            dt = datetime.strptime(entry_date, "%Y-%m-%d").date()
            for offset in (-1, 1, -2, 2):
                alt = (dt + timedelta(days=offset)).isoformat()
                vix = vix_history.get(alt)
                if vix is not None:
                    break
        if vix is None:
            continue

        # IV rank pre-check
        iv_rank = compute_vix_iv_rank(vix, vix_history, entry_date)
        if iv_rank is not None and iv_rank < IV_RANK_LOW:
            skipped_iv += 1
            last_entry_idx = idx
            continue

        # Attempt trade
        result = simulate_condor_trade(
            client, entry_date, spot, vix, vix_history,
            spy_bars, bar_idx_map, use_synthetic=synthetic_only,
            call_delta_offset=call_delta_offset,
        )

        if result is None:
            skipped_data += 1
            last_entry_idx = idx  # still consume the 5-day cooldown
            continue

        trades.append(result)
        last_entry_idx = idx

        if len(trades) % 20 == 0:
            log.info("  ... %d trades so far (entry %s, source=%s)",
                     len(trades), entry_date, result["data_source"])

    log.info("Backtest complete: %d trades  (%d skipped IV low, %d skipped data)",
             len(trades), skipped_iv, skipped_data)

    client.close()
    return trades


# ===================================================================
# Step 9 -- Reporting
# ===================================================================

def get_vix_bucket(vix: float) -> str:
    for name, lo, hi in VIX_BUCKETS:
        if lo <= vix < hi:
            return name
    return "Unknown"


def print_results(trades: List[Dict]) -> None:
    """Print comprehensive backtest results."""
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

    # Detect asymmetric delta from trade data
    sample_offset = trades[0].get("call_delta_offset", 0.0)
    is_asymmetric = sample_offset != 0.0

    print()
    print("=" * 70)
    print("THETADATA IRON CONDOR RESULTS  --  SPY")
    if is_asymmetric:
        print(f"  Call Delta Offset: {sample_offset:+.2f} (asymmetric wings)")
    print("=" * 70)
    print(f"  Period:          {trades[0]['entry_date']} to {trades[-1]['entry_date']}")
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

    # --- Data source breakdown ---
    print()
    print("-" * 70)
    print("DATA SOURCE BREAKDOWN")
    print("-" * 70)
    sources = defaultdict(int)
    for t in trades:
        sources[t["data_source"]] += 1
    for src, n in sorted(sources.items()):
        src_trades = [t for t in trades if t["data_source"] == src]
        src_pnl = sum(t["pnl"] for t in src_trades)
        src_wins = sum(1 for t in src_trades if t["won"])
        src_wr = src_wins / n if n > 0 else 0
        print(f"  {src:<15} {n:>4} trades   Win rate: {src_wr:.1%}   "
              f"P&L: ${src_pnl:>+,.2f}   Avg: ${src_pnl/n:>+,.2f}")

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

    # --- Year-over-year summary ---
    print()
    print("-" * 70)
    print("YEAR-OVER-YEAR SUMMARY")
    print("-" * 70)
    print(f"{'Year':<6} | {'Trades':>7} | {'Win Rate':>9} | {'Total P&L':>12} | "
          f"{'Avg P&L':>10} | {'Source':>12}")
    print("-" * 70)

    years = sorted(set(t["entry_date"][:4] for t in trades))
    for yr in years:
        yr_trades = [t for t in trades if t["entry_date"][:4] == yr]
        yr_wins = sum(1 for t in yr_trades if t["won"])
        yr_pnl = sum(t["pnl"] for t in yr_trades)
        yr_sources = set(t["data_source"] for t in yr_trades)
        src_str = "/".join(sorted(yr_sources))
        print(f"{yr:<6} | {len(yr_trades):>7} | {yr_wins/len(yr_trades):>8.1%} | "
              f"${yr_pnl:>+10,.2f} | ${yr_pnl/len(yr_trades):>+9.2f} | {src_str:>12}")

    # --- Monthly P&L timeline ---
    print()
    print("-" * 70)
    print("MONTHLY P&L TIMELINE")
    print("-" * 70)
    monthly: Dict[str, float] = defaultdict(float)
    monthly_n: Dict[str, int] = defaultdict(int)
    for t in trades:
        mo = t["entry_date"][:7]
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
        if is_asymmetric:
            print(f"    IV Rank:   {t['iv_rank']:.2f}  ({t['iv_tier']})  "
                  f"put_delta={t.get('put_delta', t['short_delta'])}  "
                  f"call_delta={t.get('call_delta', t['short_delta'])}")
        else:
            print(f"    IV Rank:   {t['iv_rank']:.2f}  ({t['iv_tier']})  delta={t['short_delta']}")
        print(f"    Strikes:   LP {t['long_put']}  SP {t['short_put']}  "
              f"SC {t['short_call']}  LC {t['long_call']}")
        print(f"    Credit:    ${t['credit']:.4f}/sh   Max loss: ${t['max_loss']:.2f}/ct")
        print(f"    Exp:       {t['expiration']}  DTE={t['dte']}")
        print(f"    Exit:      {t['exit_date']}  reason={t['exit_reason']}")
        print(f"    P&L:       ${t['pnl']:>+.2f}  {'WIN' if t['won'] else 'LOSS'}"
              + (f"  ({t['side_breached']} breached)" if t['side_breached'] else ""))
        print(f"    Source:    {t['data_source']}")


def export_csv(trades: List[Dict], filepath: str) -> None:
    """Export trades to CSV file."""
    if not trades:
        print("No trades to export.")
        return

    fieldnames = [
        "entry_date", "expiration", "exit_date", "spot", "vix",
        "iv_rank", "iv_tier", "short_delta",
        "put_delta", "call_delta", "call_delta_offset",
        "long_put", "short_put", "short_call", "long_call",
        "credit", "max_loss", "pnl", "won", "exit_reason",
        "side_breached", "put_width", "call_width", "data_source", "dte",
    ]

    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for t in trades:
            writer.writerow({k: t.get(k, "") for k in fieldnames})

    print(f"\nExported {len(trades)} trades to {filepath}")


# ===================================================================
# CLI entry point
# ===================================================================

def main():
    parser = argparse.ArgumentParser(
        description="ThetaData iron condor backtest for SPY (2012-2026)")
    parser.add_argument("--year", type=int, default=None,
                        help="Run for a single year (e.g. 2024)")
    parser.add_argument("--start", type=int, default=None,
                        help="Start year (e.g. 2020)")
    parser.add_argument("--end", type=int, default=None,
                        help="End year (e.g. 2025)")
    parser.add_argument("--synthetic-only", action="store_true",
                        help="Use Black-Scholes only (no ThetaData needed)")
    parser.add_argument("--call-delta-offset", type=float, default=0.0,
                        metavar="OFFSET",
                        help="Offset applied to call delta (e.g. -0.05 pushes "
                             "calls further OTM). Default 0 = symmetric.")
    parser.add_argument("--export-csv", type=str, default=None,
                        metavar="FILE",
                        help="Export trades to CSV file")
    args = parser.parse_args()

    if args.year:
        start_year = args.year
        end_year = args.year
    elif args.start and args.end:
        start_year = args.start
        end_year = args.end
    else:
        start_year = 2012
        end_year = 2026

    t0 = time.time()
    trades = run_backtest(start_year, end_year,
                          synthetic_only=args.synthetic_only,
                          call_delta_offset=args.call_delta_offset)
    elapsed = time.time() - t0

    print_results(trades)

    if args.export_csv:
        export_csv(trades, args.export_csv)

    print(f"\nCompleted in {elapsed:.1f}s  ({len(trades)} trades)")


if __name__ == "__main__":
    main()
