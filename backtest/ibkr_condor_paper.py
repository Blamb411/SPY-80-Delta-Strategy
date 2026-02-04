#!/usr/bin/env python3
"""
IBKR Paper Trading — SPY Iron Condor
======================================
Connects to TWS paper trading account and manages iron condor positions
using the same entry/exit logic validated in the real-data backtest.

Usage:
    python ibkr_condor_paper.py                # Check signals & manage positions
    python ibkr_condor_paper.py --status       # Show open positions only
    python ibkr_condor_paper.py --history      # Show trade history
    python ibkr_condor_paper.py --force-entry  # Enter regardless of signal (testing)
    python ibkr_condor_paper.py --dry-run      # Check everything but don't submit orders
    python ibkr_condor_paper.py --close-all    # Close all open positions at market

Run daily during market hours. Safe to run multiple times per day —
it will not double-enter or re-close already-closed positions.
"""

import os
import sys
import math
import time
import sqlite3
import argparse
import logging
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple

from ib_insync import IB, Stock, Option, Contract, ComboLeg, Order, TagValue

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_this_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from backtest.black_scholes import find_strike_for_delta, calculate_iv_rank

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("condor_paper")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
IB_HOST = "127.0.0.1"
IB_PORT = 7497          # TWS paper trading
IB_CLIENT_ID = 77       # unique to this script

DB_PATH = os.path.join(_this_dir, "condor_paper.db")

# Strategy parameters — match the backtest
IV_RANK_LOW = 0.30
IV_RANK_MED = 0.50
IV_RANK_HIGH = 0.70

DELTA_BY_IV_TIER = {
    "low": None,
    "medium": 0.20,
    "high": 0.25,
    "very_high": 0.30,
}

WING_WIDTH_PCT = 0.03
DTE_TARGET = 30
DTE_MIN = 25
DTE_MAX = 45
RISK_FREE_RATE = 0.05
TAKE_PROFIT_PCT = 0.50
STOP_LOSS_PCT = 0.75
MIN_DAYS_BETWEEN_ENTRIES = 5
NUM_CONTRACTS = 1        # paper trade 1 contract at a time
MAX_OPEN_POSITIONS = 3   # don't stack too many

IV_RANK_LOOKBACK = 252


# ===================================================================
# Database — position and trade tracking
# ===================================================================

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entry_date TEXT NOT NULL,
            expiration TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'open',
            spot_at_entry REAL,
            vix_at_entry REAL,
            iv_rank REAL,
            iv_tier TEXT,
            short_delta REAL,
            lp_strike REAL, sp_strike REAL,
            sc_strike REAL, lc_strike REAL,
            lp_conid INTEGER, sp_conid INTEGER,
            sc_conid INTEGER, lc_conid INTEGER,
            entry_credit REAL,
            max_loss REAL,
            num_contracts INTEGER DEFAULT 1,
            tp_target REAL,
            sl_trigger REAL,
            exit_date TEXT,
            exit_reason TEXT,
            exit_debit REAL,
            pnl REAL,
            entry_order_id INTEGER,
            exit_order_id INTEGER,
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            log_date TEXT NOT NULL,
            position_id INTEGER,
            spot REAL,
            vix REAL,
            lp_mid REAL, sp_mid REAL, sc_mid REAL, lc_mid REAL,
            position_value REAL,
            unrealized_pnl REAL,
            action TEXT,
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()


def get_open_positions() -> List[Dict]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM positions WHERE status='open' ORDER BY entry_date"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_all_positions() -> List[Dict]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM positions ORDER BY entry_date DESC"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_last_entry_date() -> Optional[str]:
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute(
        "SELECT MAX(entry_date) FROM positions"
    ).fetchone()
    conn.close()
    return row[0] if row and row[0] else None


def save_position(pos: Dict) -> int:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.execute("""
        INSERT INTO positions (
            entry_date, expiration, status, spot_at_entry, vix_at_entry,
            iv_rank, iv_tier, short_delta,
            lp_strike, sp_strike, sc_strike, lc_strike,
            lp_conid, sp_conid, sc_conid, lc_conid,
            entry_credit, max_loss, num_contracts,
            tp_target, sl_trigger, entry_order_id, notes
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        pos["entry_date"], pos["expiration"], "open",
        pos["spot"], pos["vix"], pos["iv_rank"], pos["iv_tier"],
        pos["short_delta"],
        pos["lp_strike"], pos["sp_strike"],
        pos["sc_strike"], pos["lc_strike"],
        pos["lp_conid"], pos["sp_conid"],
        pos["sc_conid"], pos["lc_conid"],
        pos["entry_credit"], pos["max_loss"], pos["num_contracts"],
        pos["tp_target"], pos["sl_trigger"],
        pos.get("entry_order_id"), pos.get("notes", ""),
    ))
    conn.commit()
    pos_id = cur.lastrowid
    conn.close()
    return pos_id


def close_position(pos_id: int, exit_date: str, exit_reason: str,
                   exit_debit: float, pnl: float,
                   exit_order_id: Optional[int] = None):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        UPDATE positions SET
            status='closed', exit_date=?, exit_reason=?,
            exit_debit=?, pnl=?, exit_order_id=?
        WHERE id=?
    """, (exit_date, exit_reason, exit_debit, pnl, exit_order_id, pos_id))
    conn.commit()
    conn.close()


def log_daily(log_date: str, position_id: Optional[int], spot: float,
              vix: float, mids: Optional[Dict] = None,
              pos_value: Optional[float] = None,
              unrealized: Optional[float] = None,
              action: str = "", notes: str = ""):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        INSERT INTO daily_log (
            log_date, position_id, spot, vix,
            lp_mid, sp_mid, sc_mid, lc_mid,
            position_value, unrealized_pnl, action, notes
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        log_date, position_id, spot, vix,
        mids.get("lp") if mids else None,
        mids.get("sp") if mids else None,
        mids.get("sc") if mids else None,
        mids.get("lc") if mids else None,
        pos_value, unrealized, action, notes,
    ))
    conn.commit()
    conn.close()


# ===================================================================
# VIX / IV Rank
# ===================================================================

def get_current_vix() -> Optional[float]:
    """Fetch current VIX from Yahoo Finance."""
    import yfinance as yf
    import pandas as pd

    try:
        end = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        start = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
        vix = yf.download("^VIX", start=start, end=end, progress=False)
        if vix.empty:
            return None
        if isinstance(vix.columns, pd.MultiIndex):
            vix.columns = vix.columns.get_level_values(0)
        return float(vix["Close"].iloc[-1])
    except Exception as e:
        log.error("Failed to fetch VIX: %s", e)
        return None


def get_vix_iv_rank() -> Tuple[Optional[float], Optional[float]]:
    """Get current VIX and its IV rank (252-day percentile)."""
    import yfinance as yf
    import pandas as pd

    try:
        end = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        start = (datetime.now() - timedelta(days=400)).strftime("%Y-%m-%d")
        vix_df = yf.download("^VIX", start=start, end=end, progress=False)
        if vix_df.empty:
            return None, None
        if isinstance(vix_df.columns, pd.MultiIndex):
            vix_df.columns = vix_df.columns.get_level_values(0)

        closes = vix_df["Close"].tolist()
        if len(closes) < 20:
            return None, None

        current_vix = float(closes[-1])
        history = [float(v) for v in closes[-IV_RANK_LOOKBACK:]]
        iv_rank = calculate_iv_rank(current_vix, history, IV_RANK_LOOKBACK)
        return current_vix, iv_rank
    except Exception as e:
        log.error("Failed to compute VIX IV rank: %s", e)
        return None, None


def select_delta_tier(iv_rank: float) -> Tuple[Optional[float], str]:
    if iv_rank < IV_RANK_LOW:
        return None, "low"
    elif iv_rank < IV_RANK_MED:
        return DELTA_BY_IV_TIER["medium"], "medium"
    elif iv_rank < IV_RANK_HIGH:
        return DELTA_BY_IV_TIER["high"], "high"
    else:
        return DELTA_BY_IV_TIER["very_high"], "very_high"


# ===================================================================
# IBKR helpers
# ===================================================================

def connect_ibkr() -> IB:
    ib = IB()
    ib.connect(IB_HOST, IB_PORT, clientId=IB_CLIENT_ID, timeout=15)
    if not ib.isConnected():
        raise ConnectionError("Failed to connect to TWS")
    accts = ib.managedAccounts()
    log.info("Connected to IBKR — account %s", accts[0] if accts else "?")
    return ib


def get_spy_spot(ib: IB) -> float:
    spy = Stock("SPY", "ARCA", "USD")
    ib.qualifyContracts(spy)
    md = ib.reqMktData(spy, "", False, False)
    ib.sleep(2)
    spot = md.marketPrice()
    ib.cancelMktData(spy)
    if not spot or math.isnan(spot):
        raise ValueError("Cannot get SPY price")
    return spot


def get_option_chain(ib: IB, spy_conid: int) -> Dict:
    chains = ib.reqSecDefOptParams("SPY", "", "STK", spy_conid)
    # Pick the SMART chain with full strikes
    full_chains = [c for c in chains if c.exchange == "SMART"
                   and len(c.strikes) > 100]
    if not full_chains:
        raise ValueError("No full option chain found")
    return full_chains[0]


def find_expiration(chain, dte_target=DTE_TARGET,
                    dte_min=DTE_MIN, dte_max=DTE_MAX) -> str:
    now = datetime.now()
    valid = []
    for e in chain.expirations:
        dte = (datetime.strptime(e, "%Y%m%d") - now).days
        if dte_min <= dte <= dte_max:
            valid.append((e, dte))
    if not valid:
        raise ValueError(f"No expiration found in {dte_min}-{dte_max} DTE range")
    return min(valid, key=lambda x: abs(x[1] - dte_target))[0]


def snap_strike(target: float, strikes: List[float]) -> float:
    """Snap a target strike to the nearest available strike on $5 boundaries."""
    # Filter to $5-increment strikes (guaranteed to exist for far OTM)
    fives = [s for s in strikes if s % 5 == 0]
    if not fives:
        fives = strikes
    return min(fives, key=lambda s: abs(s - target))


def build_condor_contracts(
    ib: IB, spot: float, short_delta: float, chain, expiration: str,
) -> Optional[Dict]:
    """Build and qualify the 4 condor leg contracts."""
    vix_decimal = get_current_vix()
    if vix_decimal is None:
        return None
    vix_iv = vix_decimal / 100.0
    dte_years = DTE_TARGET / 365.0

    # BS target strikes
    sp_raw = find_strike_for_delta(
        spot, dte_years, RISK_FREE_RATE, vix_iv, -short_delta, "P")
    sc_raw = find_strike_for_delta(
        spot, dte_years, RISK_FREE_RATE, vix_iv, short_delta, "C")
    if sp_raw is None or sc_raw is None:
        log.error("BS delta calc failed")
        return None

    strikes = sorted(chain.strikes)
    wing = round(spot * WING_WIDTH_PCT / 5) * 5  # round wing to $5

    sp_strike = snap_strike(sp_raw, strikes)
    lp_strike = snap_strike(sp_strike - wing, strikes)
    sc_strike = snap_strike(sc_raw, strikes)
    lc_strike = snap_strike(sc_strike + wing, strikes)

    if not (lp_strike < sp_strike < sc_strike < lc_strike):
        log.error("Invalid strike ordering: LP=%s SP=%s SC=%s LC=%s",
                  lp_strike, sp_strike, sc_strike, lc_strike)
        return None

    log.info("Strikes: LP=%.0f SP=%.0f | SC=%.0f LC=%.0f (exp %s)",
             lp_strike, sp_strike, sc_strike, lc_strike, expiration)

    legs = {
        "lp": Option("SPY", expiration, lp_strike, "P", "SMART"),
        "sp": Option("SPY", expiration, sp_strike, "P", "SMART"),
        "sc": Option("SPY", expiration, sc_strike, "C", "SMART"),
        "lc": Option("SPY", expiration, lc_strike, "C", "SMART"),
    }

    contracts = list(legs.values())
    ib.qualifyContracts(*contracts)
    for name, c in legs.items():
        if c.conId == 0:
            log.error("Failed to qualify %s (strike=%.0f)", name, c.strike)
            return None

    return {
        "legs": legs,
        "lp_strike": lp_strike, "sp_strike": sp_strike,
        "sc_strike": sc_strike, "lc_strike": lc_strike,
    }


def quote_legs(ib: IB, legs: Dict) -> Optional[Dict]:
    """Get live bid/ask for all 4 legs. Returns mids + credit."""
    contracts = [legs["lp"], legs["sp"], legs["sc"], legs["lc"]]
    names = ["lp", "sp", "sc", "lc"]

    # Use reqMktData (streaming) instead of reqTickers to avoid
    # "competing live session" snapshot errors on paper accounts
    md_list = []
    for c in contracts:
        md = ib.reqMktData(c, "", False, False)
        md_list.append(md)
    ib.sleep(5)

    result = {}
    missing = []
    for name, md in zip(names, md_list):
        bid = md.bid if md.bid and not math.isnan(md.bid) and md.bid > 0 else 0
        ask = md.ask if md.ask and not math.isnan(md.ask) and md.ask > 0 else 0
        mid = (bid + ask) / 2 if bid and ask else 0
        result[name] = {"bid": bid, "ask": ask, "mid": mid}
        if bid == 0 or ask == 0:
            missing.append(name)
            log.warning("No quote for %s (strike=%.0f)", name,
                        md.contract.strike)

    # Cancel market data subscriptions
    for c in contracts:
        try:
            ib.cancelMktData(c)
        except Exception:
            pass

    if missing:
        log.warning("Missing quotes for: %s. Retrying with snapshot...", missing)
        ib.sleep(2)
        for name in missing:
            idx = names.index(name)
            try:
                tick = ib.reqTickers(contracts[idx])
                ib.sleep(3)
                if tick:
                    t = tick[0] if isinstance(tick, list) else tick
                    bid = t.bid if t.bid and not math.isnan(t.bid) and t.bid > 0 else 0
                    ask = t.ask if t.ask and not math.isnan(t.ask) and t.ask > 0 else 0
                    if bid > 0 and ask > 0:
                        result[name] = {"bid": bid, "ask": ask,
                                        "mid": (bid + ask) / 2}
                        log.info("Retry got quote for %s: bid=%.2f ask=%.2f",
                                 name, bid, ask)
            except Exception as e:
                log.warning("Retry failed for %s: %s", name, e)

    # Credit = sell shorts at bid, buy longs at ask
    credit = (result["sp"]["bid"] + result["sc"]["bid"]
              - result["lp"]["ask"] - result["lc"]["ask"])

    result["credit"] = credit
    result["mids"] = {
        "lp": result["lp"]["mid"], "sp": result["sp"]["mid"],
        "sc": result["sc"]["mid"], "lc": result["lc"]["mid"],
    }
    return result


def submit_condor_entry(ib: IB, legs: Dict, credit: float,
                        num_contracts: int = 1) -> Optional[int]:
    """Submit an iron condor as a BAG (combo) order.

    Returns the order ID or None on failure.
    """
    spy = Stock("SPY", "ARCA", "USD")
    ib.qualifyContracts(spy)

    combo = Contract()
    combo.symbol = "SPY"
    combo.secType = "BAG"
    combo.currency = "USD"
    combo.exchange = "SMART"

    combo.comboLegs = [
        ComboLeg(conId=legs["lp"].conId, ratio=1, action="BUY",
                 exchange="SMART"),
        ComboLeg(conId=legs["sp"].conId, ratio=1, action="SELL",
                 exchange="SMART"),
        ComboLeg(conId=legs["sc"].conId, ratio=1, action="SELL",
                 exchange="SMART"),
        ComboLeg(conId=legs["lc"].conId, ratio=1, action="BUY",
                 exchange="SMART"),
    ]

    # Limit order at the net credit (negative = credit received)
    order = Order()
    order.action = "SELL"  # selling the condor = receiving credit
    order.orderType = "LMT"
    order.totalQuantity = num_contracts
    order.lmtPrice = round(credit, 2)  # positive credit value
    order.transmit = True
    order.tif = "DAY"
    order.smartComboRoutingParams = [TagValue("NonGuaranteed", "1")]

    trade = ib.placeOrder(combo, order)
    log.info("Submitted condor entry order #%s for %d contracts at $%.2f credit",
             trade.order.orderId, num_contracts, credit)

    # Wait for fill (up to 30 seconds)
    for _ in range(30):
        ib.sleep(1)
        if trade.orderStatus.status == "Filled":
            log.info("ORDER FILLED at $%.2f", trade.orderStatus.avgFillPrice)
            return trade.order.orderId
        elif trade.orderStatus.status in ("Cancelled", "ApiCancelled"):
            log.error("Order cancelled: %s", trade.orderStatus.status)
            return None

    # Not filled yet — leave it working
    log.info("Order still working (status: %s). Will check on next run.",
             trade.orderStatus.status)
    return trade.order.orderId


def submit_condor_close(ib: IB, pos: Dict) -> Optional[int]:
    """Submit a closing order for an open condor position."""
    combo = Contract()
    combo.symbol = "SPY"
    combo.secType = "BAG"
    combo.currency = "USD"
    combo.exchange = "SMART"

    # Reverse the legs: buy back shorts, sell longs
    combo.comboLegs = [
        ComboLeg(conId=pos["lp_conid"], ratio=1, action="SELL",
                 exchange="SMART"),
        ComboLeg(conId=pos["sp_conid"], ratio=1, action="BUY",
                 exchange="SMART"),
        ComboLeg(conId=pos["sc_conid"], ratio=1, action="BUY",
                 exchange="SMART"),
        ComboLeg(conId=pos["lc_conid"], ratio=1, action="SELL",
                 exchange="SMART"),
    ]

    order = Order()
    order.action = "BUY"  # buying back = closing
    order.orderType = "MKT"
    order.totalQuantity = pos["num_contracts"]
    order.transmit = True
    order.tif = "DAY"
    order.smartComboRoutingParams = [TagValue("NonGuaranteed", "1")]

    trade = ib.placeOrder(combo, order)
    log.info("Submitted condor CLOSE order #%s", trade.order.orderId)

    for _ in range(30):
        ib.sleep(1)
        if trade.orderStatus.status == "Filled":
            log.info("CLOSE FILLED at $%.2f", trade.orderStatus.avgFillPrice)
            return trade.order.orderId
        elif trade.orderStatus.status in ("Cancelled", "ApiCancelled"):
            log.error("Close order cancelled")
            return None

    log.info("Close order still working (status: %s)",
             trade.orderStatus.status)
    return trade.order.orderId


# ===================================================================
# Core logic
# ===================================================================

def check_entry_signal(dry_run: bool = False, force: bool = False) -> bool:
    """Check if we should enter a new condor today. Returns True if entered."""
    today = datetime.now().strftime("%Y-%m-%d")

    # Check spacing
    last_entry = get_last_entry_date()
    if last_entry and not force:
        days_since = (datetime.strptime(today, "%Y-%m-%d")
                      - datetime.strptime(last_entry, "%Y-%m-%d")).days
        if days_since < MIN_DAYS_BETWEEN_ENTRIES:
            log.info("Skipping: only %d days since last entry (min %d)",
                     days_since, MIN_DAYS_BETWEEN_ENTRIES)
            return False

    # Check max open positions
    open_pos = get_open_positions()
    if len(open_pos) >= MAX_OPEN_POSITIONS and not force:
        log.info("Skipping: %d open positions (max %d)",
                 len(open_pos), MAX_OPEN_POSITIONS)
        return False

    # Get VIX and IV rank
    vix, iv_rank = get_vix_iv_rank()
    if vix is None or iv_rank is None:
        log.error("Cannot compute VIX/IV rank")
        return False

    log.info("VIX: %.1f  IV Rank: %.2f", vix, iv_rank)

    # Select delta
    short_delta, iv_tier = select_delta_tier(iv_rank)
    if short_delta is None and not force:
        log.info("Skipping: IV rank %.2f too low (tier: %s)", iv_rank, iv_tier)
        log_daily(today, None, 0, vix, action="skip",
                  notes=f"IV rank {iv_rank:.2f} < {IV_RANK_LOW}")
        return False

    if force and short_delta is None:
        short_delta = 0.20  # default for forced entry
        iv_tier = "forced"

    log.info("Signal: iv_tier=%s, delta=%.2f", iv_tier, short_delta)

    # Connect to IBKR
    ib = connect_ibkr()
    try:
        spot = get_spy_spot(ib)
        log.info("SPY spot: $%.2f", spot)

        spy = Stock("SPY", "ARCA", "USD")
        ib.qualifyContracts(spy)
        chain = get_option_chain(ib, spy.conId)
        expiration = find_expiration(chain)

        # Build condor
        condor = build_condor_contracts(ib, spot, short_delta, chain, expiration)
        if condor is None:
            log.error("Failed to build condor")
            return False

        # Quote it
        quotes = quote_legs(ib, condor["legs"])
        if quotes is None or quotes["credit"] <= 0:
            log.error("No valid quotes or negative credit (%.2f)",
                      quotes["credit"] if quotes else 0)
            return False

        credit = quotes["credit"]
        put_width = condor["sp_strike"] - condor["lp_strike"]
        call_width = condor["lc_strike"] - condor["sc_strike"]
        max_width = max(put_width, call_width)
        max_loss = (max_width - credit) * 100 * NUM_CONTRACTS
        tp_target = credit * TAKE_PROFIT_PCT
        sl_trigger = max_loss * STOP_LOSS_PCT

        exp_str = datetime.strptime(expiration, "%Y%m%d").strftime("%Y-%m-%d")

        log.info("Credit: $%.2f/sh ($%.0f/ct)  Max loss: $%.0f  "
                 "TP: $%.2f  SL: $%.0f",
                 credit, credit * 100, max_loss, tp_target, sl_trigger)

        if dry_run:
            log.info("DRY RUN — would enter condor. Not submitting order.")
            print(f"\n  DRY RUN — Condor signal detected:")
            print(f"  SPY ${spot:.2f}  VIX {vix:.1f}  IV Rank {iv_rank:.2f}")
            print(f"  LP={condor['lp_strike']:.0f} SP={condor['sp_strike']:.0f} "
                  f"| SC={condor['sc_strike']:.0f} LC={condor['lc_strike']:.0f}")
            print(f"  Credit: ${credit:.2f}/sh (${credit*100:.0f}/ct)  "
                  f"Max loss: ${max_loss:.0f}")
            print(f"  Exp: {exp_str}  TP at ${tp_target:.2f}  SL at ${sl_trigger:.0f} loss")
            return False

        # Submit order
        order_id = submit_condor_entry(
            ib, condor["legs"], credit, NUM_CONTRACTS)

        # Save position
        pos_id = save_position({
            "entry_date": today,
            "expiration": exp_str,
            "spot": spot,
            "vix": vix,
            "iv_rank": iv_rank,
            "iv_tier": iv_tier,
            "short_delta": short_delta,
            "lp_strike": condor["lp_strike"],
            "sp_strike": condor["sp_strike"],
            "sc_strike": condor["sc_strike"],
            "lc_strike": condor["lc_strike"],
            "lp_conid": condor["legs"]["lp"].conId,
            "sp_conid": condor["legs"]["sp"].conId,
            "sc_conid": condor["legs"]["sc"].conId,
            "lc_conid": condor["legs"]["lc"].conId,
            "entry_credit": credit,
            "max_loss": max_loss,
            "num_contracts": NUM_CONTRACTS,
            "tp_target": tp_target,
            "sl_trigger": sl_trigger,
            "entry_order_id": order_id,
            "notes": f"VIX={vix:.1f} IVR={iv_rank:.2f} tier={iv_tier}",
        })

        log_daily(today, pos_id, spot, vix, mids=quotes["mids"],
                  action="entry",
                  notes=f"credit=${credit:.2f} order={order_id}")

        log.info("Position #%d saved", pos_id)
        return True

    finally:
        ib.disconnect()


def monitor_positions(dry_run: bool = False):
    """Check open positions for TP/SL/expiration exits."""
    open_pos = get_open_positions()
    if not open_pos:
        log.info("No open positions to monitor")
        return

    today = datetime.now().strftime("%Y-%m-%d")

    vix = get_current_vix() or 0

    ib = connect_ibkr()
    try:
        spot = get_spy_spot(ib)
        log.info("SPY: $%.2f  VIX: %.1f  Open positions: %d",
                 spot, vix, len(open_pos))

        for pos in open_pos:
            log.info("--- Position #%d (entered %s, exp %s) ---",
                     pos["id"], pos["entry_date"], pos["expiration"])

            # Check expiration
            exp_dt = datetime.strptime(pos["expiration"], "%Y-%m-%d").date()
            today_dt = datetime.strptime(today, "%Y-%m-%d").date()

            if today_dt >= exp_dt:
                # Settle at intrinsic
                sp_intrinsic = max(0, pos["sp_strike"] - spot)
                lp_intrinsic = max(0, pos["lp_strike"] - spot)
                sc_intrinsic = max(0, spot - pos["sc_strike"])
                lc_intrinsic = max(0, spot - pos["lc_strike"])
                settle = ((sp_intrinsic + sc_intrinsic)
                          - (lp_intrinsic + lc_intrinsic))
                pnl = (pos["entry_credit"] - settle) * 100 * pos["num_contracts"]

                log.info("EXPIRATION: settle=$%.2f  P&L=$%.2f", settle, pnl)

                if not dry_run:
                    close_position(pos["id"], today, "expiration", settle, pnl)
                    log_daily(today, pos["id"], spot, vix,
                              pos_value=settle, unrealized_pnl=pnl,
                              action="expiration",
                              notes=f"settled at ${settle:.2f}")
                continue

            # Re-quote the legs
            legs = {
                "lp": Option(conId=pos["lp_conid"], exchange="SMART"),
                "sp": Option(conId=pos["sp_conid"], exchange="SMART"),
                "sc": Option(conId=pos["sc_conid"], exchange="SMART"),
                "lc": Option(conId=pos["lc_conid"], exchange="SMART"),
            }
            contracts = list(legs.values())
            ib.qualifyContracts(*contracts)

            md_list = []
            for c in contracts:
                md = ib.reqMktData(c, "", False, False)
                md_list.append(md)
            ib.sleep(5)

            mids = {}
            for name, md in zip(["lp", "sp", "sc", "lc"], md_list):
                bid = md.bid if md.bid and not math.isnan(md.bid) and md.bid > 0 else 0
                ask = md.ask if md.ask and not math.isnan(md.ask) and md.ask > 0 else 0
                mids[name] = (bid + ask) / 2 if bid and ask else 0

            for c in contracts:
                try:
                    ib.cancelMktData(c)
                except Exception:
                    pass

            # Cost to close = buy shorts at ask, sell longs at bid
            close_cost = 0
            for name, md in zip(["lp", "sp", "sc", "lc"], md_list):
                bid = md.bid if md.bid and not math.isnan(md.bid) and md.bid > 0 else 0
                ask = md.ask if md.ask and not math.isnan(md.ask) and md.ask > 0 else 0
                if "s" == name[0]:  # short legs: buy at ask
                    close_cost += ask
                else:  # long legs: sell at bid
                    close_cost -= bid

            pnl = (pos["entry_credit"] - close_cost) * 100 * pos["num_contracts"]

            log.info("  Position value: $%.2f  Close cost: $%.2f  "
                     "Unrealized P&L: $%.2f",
                     close_cost, close_cost, pnl)

            # Check take profit
            if close_cost <= pos["tp_target"] and close_cost >= 0:
                log.info("  *** TAKE PROFIT triggered (close_cost $%.2f <= "
                         "target $%.2f)", close_cost, pos["tp_target"])
                if not dry_run:
                    order_id = submit_condor_close(ib, pos)
                    close_position(pos["id"], today, "take_profit",
                                   close_cost, pnl, order_id)
                    log_daily(today, pos["id"], spot, vix, mids=mids,
                              pos_value=close_cost, unrealized_pnl=pnl,
                              action="take_profit",
                              notes=f"closed at ${close_cost:.2f}")
                else:
                    log.info("  DRY RUN — would close for take profit")
                continue

            # Check stop loss
            if pnl <= -pos["sl_trigger"]:
                log.info("  *** STOP LOSS triggered (loss $%.2f >= "
                         "trigger $%.2f)", abs(pnl), pos["sl_trigger"])
                if not dry_run:
                    order_id = submit_condor_close(ib, pos)
                    close_position(pos["id"], today, "stop_loss",
                                   close_cost, pnl, order_id)
                    log_daily(today, pos["id"], spot, vix, mids=mids,
                              pos_value=close_cost, unrealized_pnl=pnl,
                              action="stop_loss",
                              notes=f"loss ${abs(pnl):.2f}")
                else:
                    log.info("  DRY RUN — would close for stop loss")
                continue

            # No exit — log and continue
            log_daily(today, pos["id"], spot, vix, mids=mids,
                      pos_value=close_cost, unrealized_pnl=pnl,
                      action="hold",
                      notes=f"P&L=${pnl:.2f}")
            log.info("  Holding (P&L: $%.2f)", pnl)

    finally:
        ib.disconnect()


def close_all_positions():
    """Emergency close all open positions at market."""
    open_pos = get_open_positions()
    if not open_pos:
        print("No open positions.")
        return

    today = datetime.now().strftime("%Y-%m-%d")
    ib = connect_ibkr()
    try:
        spot = get_spy_spot(ib)
        vix = get_current_vix() or 0

        for pos in open_pos:
            log.info("Closing position #%d...", pos["id"])
            order_id = submit_condor_close(ib, pos)

            # Estimate P&L from entry credit (rough, market order)
            close_position(pos["id"], today, "manual_close",
                           0, 0, order_id)
            log_daily(today, pos["id"], spot, vix,
                      action="manual_close",
                      notes="closed all positions")
            log.info("Position #%d closed", pos["id"])
    finally:
        ib.disconnect()


# ===================================================================
# Display functions
# ===================================================================

def print_status():
    """Print current open positions."""
    open_pos = get_open_positions()
    print(f"\n{'='*70}")
    print(f" CONDOR PAPER TRADER — Open Positions")
    print(f"{'='*70}")

    if not open_pos:
        print("\n  No open positions.\n")
        return

    for pos in open_pos:
        dte = (datetime.strptime(pos["expiration"], "%Y-%m-%d")
               - datetime.now()).days
        print(f"\n  Position #{pos['id']}")
        print(f"    Entered:    {pos['entry_date']}  "
              f"SPY ${pos['spot_at_entry']:.2f}  "
              f"VIX {pos['vix_at_entry']:.1f}")
        print(f"    Strikes:    LP {pos['lp_strike']:.0f}  "
              f"SP {pos['sp_strike']:.0f}  |  "
              f"SC {pos['sc_strike']:.0f}  LC {pos['lc_strike']:.0f}")
        print(f"    Expiration: {pos['expiration']} ({dte} DTE)")
        print(f"    Credit:     ${pos['entry_credit']:.2f}/sh  "
              f"Max loss: ${pos['max_loss']:.0f}")
        print(f"    TP target:  ${pos['tp_target']:.2f}  "
              f"SL trigger: ${pos['sl_trigger']:.0f}")
        print(f"    Tier:       {pos['iv_tier']} "
              f"(delta={pos['short_delta']:.2f})")
    print()


def print_history():
    """Print all trade history."""
    positions = get_all_positions()
    print(f"\n{'='*70}")
    print(f" CONDOR PAPER TRADER — Trade History")
    print(f"{'='*70}")

    if not positions:
        print("\n  No trades yet.\n")
        return

    closed = [p for p in positions if p["status"] == "closed"]
    open_p = [p for p in positions if p["status"] == "open"]

    print(f"\n  Total trades: {len(positions)}  "
          f"(Open: {len(open_p)}, Closed: {len(closed)})")

    if closed:
        wins = sum(1 for p in closed if p["pnl"] and p["pnl"] > 0)
        total_pnl = sum(p["pnl"] for p in closed if p["pnl"])
        print(f"  Win rate:     {wins}/{len(closed)} "
              f"({wins/len(closed)*100:.0f}%)")
        print(f"  Total P&L:    ${total_pnl:+,.2f}")
        print(f"  Avg P&L:      ${total_pnl/len(closed):+,.2f}")

    print(f"\n  {'#':<4} {'Entry':<12} {'Exit':<12} {'Reason':<14} "
          f"{'Credit':>7} {'P&L':>10} {'VIX':>5} {'Tier':<10}")
    print(f"  {'-'*78}")

    for p in positions:
        exit_dt = p["exit_date"] or "open"
        reason = p["exit_reason"] or "—"
        pnl_str = f"${p['pnl']:+,.2f}" if p["pnl"] is not None else "—"
        print(f"  {p['id']:<4} {p['entry_date']:<12} {exit_dt:<12} "
              f"{reason:<14} ${p['entry_credit']:>6.2f} {pnl_str:>10} "
              f"{p['vix_at_entry']:>5.1f} {p['iv_tier']:<10}")
    print()


# ===================================================================
# Main
# ===================================================================

def main():
    parser = argparse.ArgumentParser(
        description="IBKR Paper Trading — SPY Iron Condor")
    parser.add_argument("--status", action="store_true",
                        help="Show open positions only")
    parser.add_argument("--history", action="store_true",
                        help="Show full trade history")
    parser.add_argument("--dry-run", action="store_true",
                        help="Check signals but don't submit orders")
    parser.add_argument("--force-entry", action="store_true",
                        help="Enter regardless of signal (testing)")
    parser.add_argument("--close-all", action="store_true",
                        help="Close all open positions at market")
    parser.add_argument("--monitor-only", action="store_true",
                        help="Only monitor existing positions, skip entry check")
    args = parser.parse_args()

    init_db()

    if args.status:
        print_status()
        return

    if args.history:
        print_history()
        return

    if args.close_all:
        close_all_positions()
        return

    print(f"\n{'='*70}")
    print(f" CONDOR PAPER TRADER — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*70}")

    # 1. Monitor existing positions first
    monitor_positions(dry_run=args.dry_run)

    # 2. Check for new entry signal
    if not args.monitor_only:
        print()
        check_entry_signal(dry_run=args.dry_run, force=args.force_entry)

    # 3. Show status
    print_status()


if __name__ == "__main__":
    main()
