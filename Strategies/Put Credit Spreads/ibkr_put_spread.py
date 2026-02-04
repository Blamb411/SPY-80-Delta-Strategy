#!/usr/bin/env python3
"""
IBKR Put Credit Spread — Live Scanner & Executor
===================================================
Scans for put credit spread entry signals on SPY/QQQ and manages
open positions through IBKR TWS (paper or live).

Strategy: Sell put credit spreads capturing the Variance Risk Premium.
  - Flat delta 0.20 short put
  - Vol-scaled wing width (0.75 sigma)
  - 200-day SMA trend filter
  - IV rank >= 15% to enter
  - Take profit at 50% of credit
  - Stop loss at 3x credit
  - 30 DTE target, 5-day entry spacing

Usage:
    python ibkr_put_spread.py                  # Scan for signals & manage positions
    python ibkr_put_spread.py --status         # Show open positions
    python ibkr_put_spread.py --history        # Show trade history
    python ibkr_put_spread.py --dry-run        # Check everything, don't submit orders
    python ibkr_put_spread.py --force-entry    # Enter regardless of signal (testing)
    python ibkr_put_spread.py --close-all      # Close all open positions at market
    python ibkr_put_spread.py --ticker QQQ     # Use QQQ instead of SPY
    python ibkr_put_spread.py --paper          # Use paper trading port (7497)
    python ibkr_put_spread.py --live           # Use live trading port (7496)

Run during market hours. Safe to run multiple times per day.
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
_project_root = os.path.dirname(os.path.dirname(_this_dir))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from backtest.black_scholes import find_strike_for_delta, calculate_iv_rank

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("put_spread")

# ---------------------------------------------------------------------------
# Configuration — recommended parameters from STRATEGY_REFERENCE.md
# ---------------------------------------------------------------------------
IB_HOST = "127.0.0.1"
IB_PORT_PAPER = 7497
IB_PORT_LIVE = 7496
IB_CLIENT_ID = 88           # unique to this script (condor uses 77)

DB_PATH = os.path.join(_this_dir, "put_spread_paper.db")

# Strategy parameters
SHORT_DELTA = 0.20           # flat delta for short put
WING_SIGMA = 0.75            # wing width = sigma * expected_move
IV_RANK_FLOOR = 0.15         # minimum IV rank to enter
DTE_TARGET = 30
DTE_MIN = 25
DTE_MAX = 45
SMA_PERIOD = 200             # trend filter
RISK_FREE_RATE = 0.05
TAKE_PROFIT_PCT = 0.50       # close when spread <= 50% of credit
STOP_LOSS_MULT = 3.0         # close when loss >= 3x credit
MIN_DAYS_BETWEEN_ENTRIES = 5
NUM_CONTRACTS = 1
MAX_OPEN_POSITIONS = 3
IV_RANK_LOOKBACK = 252
MIN_WING_WIDTH = 5.0         # minimum wing in dollars


# ===================================================================
# Database — position and trade tracking
# ===================================================================

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL DEFAULT 'SPY',
            entry_date TEXT NOT NULL,
            expiration TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'open',
            spot_at_entry REAL,
            vix_at_entry REAL,
            iv_rank REAL,
            sma_value REAL,
            short_delta REAL,
            sp_strike REAL, lp_strike REAL,
            sp_conid INTEGER, lp_conid INTEGER,
            entry_credit REAL,
            wing_width REAL,
            max_loss REAL,
            num_contracts INTEGER DEFAULT 1,
            tp_target REAL,
            sl_trigger_debit REAL,
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
            ticker TEXT,
            spot REAL,
            vix REAL,
            sp_mid REAL, lp_mid REAL,
            spread_value REAL,
            unrealized_pnl REAL,
            action TEXT,
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()


def get_open_positions(ticker: str = None) -> List[Dict]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    if ticker:
        rows = conn.execute(
            "SELECT * FROM positions WHERE status='open' AND ticker=? ORDER BY entry_date",
            (ticker,)
        ).fetchall()
    else:
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


def get_last_entry_date(ticker: str) -> Optional[str]:
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute(
        "SELECT MAX(entry_date) FROM positions WHERE ticker=?", (ticker,)
    ).fetchone()
    conn.close()
    return row[0] if row and row[0] else None


def save_position(pos: Dict) -> int:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.execute("""
        INSERT INTO positions (
            ticker, entry_date, expiration, status, spot_at_entry, vix_at_entry,
            iv_rank, sma_value, short_delta,
            sp_strike, lp_strike,
            sp_conid, lp_conid,
            entry_credit, wing_width, max_loss, num_contracts,
            tp_target, sl_trigger_debit, entry_order_id, notes
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        pos["ticker"], pos["entry_date"], pos["expiration"], "open",
        pos["spot"], pos["vix"], pos["iv_rank"], pos["sma_value"],
        pos["short_delta"],
        pos["sp_strike"], pos["lp_strike"],
        pos["sp_conid"], pos["lp_conid"],
        pos["entry_credit"], pos["wing_width"], pos["max_loss"],
        pos["num_contracts"],
        pos["tp_target"], pos["sl_trigger_debit"],
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


def log_daily(log_date: str, position_id: Optional[int], ticker: str,
              spot: float, vix: float,
              sp_mid: float = 0, lp_mid: float = 0,
              spread_value: float = 0, unrealized: float = 0,
              action: str = "", notes: str = ""):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        INSERT INTO daily_log (
            log_date, position_id, ticker, spot, vix,
            sp_mid, lp_mid, spread_value,
            unrealized_pnl, action, notes
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, (log_date, position_id, ticker, spot, vix,
          sp_mid, lp_mid, spread_value,
          unrealized, action, notes))
    conn.commit()
    conn.close()


# ===================================================================
# Market data — VIX, IV Rank, SMA
# ===================================================================

def get_vix_data() -> Tuple[Optional[float], Optional[float]]:
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
        log.error("Failed to compute VIX/IV rank: %s", e)
        return None, None


def get_sma(ticker: str, period: int = SMA_PERIOD) -> Tuple[Optional[float], Optional[float]]:
    """
    Get current price and SMA for a ticker.
    Returns (current_close, sma_value) or (None, None).
    """
    import yfinance as yf
    import pandas as pd

    try:
        end = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        start = (datetime.now() - timedelta(days=period * 2)).strftime("%Y-%m-%d")
        df = yf.download(ticker, start=start, end=end, progress=False)
        if df.empty:
            return None, None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        closes = df["Close"].tolist()
        if len(closes) < period:
            log.warning("Only %d bars for %s SMA (need %d)", len(closes), ticker, period)
            return None, None

        current = float(closes[-1])
        sma = sum(float(c) for c in closes[-period:]) / period
        return current, sma
    except Exception as e:
        log.error("Failed to compute SMA for %s: %s", ticker, e)
        return None, None


# ===================================================================
# IBKR helpers
# ===================================================================

def connect_ibkr(port: int) -> IB:
    ib = IB()
    ib.connect(IB_HOST, port, clientId=IB_CLIENT_ID, timeout=15)
    if not ib.isConnected():
        raise ConnectionError("Failed to connect to TWS")
    accts = ib.managedAccounts()
    mode = "PAPER" if port == IB_PORT_PAPER else "LIVE"
    log.info("Connected to IBKR [%s] — account %s", mode, accts[0] if accts else "?")
    return ib


def get_spot(ib: IB, ticker: str) -> float:
    exchange = "ARCA" if ticker in ("SPY", "QQQ", "IWM", "DIA") else "SMART"
    stock = Stock(ticker, exchange, "USD")
    ib.qualifyContracts(stock)
    md = ib.reqMktData(stock, "", False, False)
    ib.sleep(2)
    spot = md.marketPrice()
    ib.cancelMktData(stock)
    if not spot or math.isnan(spot):
        raise ValueError(f"Cannot get {ticker} price")
    return spot


def get_option_chain(ib: IB, ticker: str) -> Tuple[Dict, int]:
    exchange = "ARCA" if ticker in ("SPY", "QQQ", "IWM", "DIA") else "SMART"
    stock = Stock(ticker, exchange, "USD")
    ib.qualifyContracts(stock)
    chains = ib.reqSecDefOptParams(ticker, "", "STK", stock.conId)
    full_chains = [c for c in chains if c.exchange == "SMART"
                   and len(c.strikes) > 50]
    if not full_chains:
        raise ValueError(f"No full option chain found for {ticker}")
    return full_chains[0], stock.conId


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
    """Snap to nearest available strike."""
    return min(strikes, key=lambda s: abs(s - target))


def build_spread_contracts(
    ib: IB, ticker: str, spot: float, vix: float,
    chain, expiration: str,
) -> Optional[Dict]:
    """Build and qualify the 2 put spread leg contracts."""
    vix_iv = vix / 100.0
    dte_years = DTE_TARGET / 365.0

    # Short put at target delta
    sp_raw = find_strike_for_delta(
        spot, dte_years, RISK_FREE_RATE, vix_iv, -SHORT_DELTA, "P")
    if sp_raw is None:
        log.error("Black-Scholes delta calc failed for short put")
        return None

    # Vol-scaled wing width
    expected_move = spot * vix_iv * math.sqrt(dte_years)
    wing = expected_move * WING_SIGMA
    wing = max(wing, MIN_WING_WIDTH)

    strikes = sorted(chain.strikes)

    sp_strike = snap_strike(sp_raw, strikes)
    lp_strike = snap_strike(sp_strike - wing, strikes)

    # Validate structure
    if not (lp_strike < sp_strike < spot):
        log.error("Invalid strike ordering: LP=%.0f SP=%.0f Spot=%.2f",
                  lp_strike, sp_strike, spot)
        return None

    actual_wing = sp_strike - lp_strike
    if actual_wing < MIN_WING_WIDTH:
        log.error("Wing too narrow: $%.0f (min $%.0f)", actual_wing, MIN_WING_WIDTH)
        return None

    log.info("Strikes: SP=%.0f LP=%.0f (wing=$%.0f, exp %s)",
             sp_strike, lp_strike, actual_wing, expiration)

    legs = {
        "sp": Option(ticker, expiration, sp_strike, "P", "SMART"),
        "lp": Option(ticker, expiration, lp_strike, "P", "SMART"),
    }

    contracts = list(legs.values())
    ib.qualifyContracts(*contracts)
    for name, c in legs.items():
        if c.conId == 0:
            log.error("Failed to qualify %s (strike=%.0f)", name, c.strike)
            return None

    return {
        "legs": legs,
        "sp_strike": sp_strike,
        "lp_strike": lp_strike,
        "wing_width": actual_wing,
    }


def quote_spread(ib: IB, legs: Dict) -> Optional[Dict]:
    """Get live bid/ask for both legs. Returns mids + credit."""
    contracts = [legs["sp"], legs["lp"]]
    names = ["sp", "lp"]

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
            log.warning("No quote for %s (strike=%.0f)", name, md.contract.strike)

    # Cancel subscriptions
    for c in contracts:
        try:
            ib.cancelMktData(c)
        except Exception:
            pass

    # Retry missing with snapshot
    if missing:
        log.warning("Missing quotes for: %s. Retrying...", missing)
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
                        result[name] = {"bid": bid, "ask": ask, "mid": (bid + ask) / 2}
                        log.info("Retry got quote for %s: bid=%.2f ask=%.2f", name, bid, ask)
            except Exception as e:
                log.warning("Retry failed for %s: %s", name, e)

    # Credit = sell short put at bid, buy long put at ask
    credit = result["sp"]["bid"] - result["lp"]["ask"]

    result["credit"] = credit
    result["mids"] = {"sp": result["sp"]["mid"], "lp": result["lp"]["mid"]}
    return result


def submit_spread_entry(ib: IB, ticker: str, legs: Dict, credit: float,
                        num_contracts: int = 1) -> Optional[int]:
    """Submit put credit spread as a BAG (combo) limit order. Returns order ID."""
    combo = Contract()
    combo.symbol = ticker
    combo.secType = "BAG"
    combo.currency = "USD"
    combo.exchange = "SMART"

    combo.comboLegs = [
        ComboLeg(conId=legs["sp"].conId, ratio=1, action="SELL",
                 exchange="SMART"),
        ComboLeg(conId=legs["lp"].conId, ratio=1, action="BUY",
                 exchange="SMART"),
    ]

    order = Order()
    order.action = "SELL"       # selling the spread = receiving credit
    order.orderType = "LMT"
    order.totalQuantity = num_contracts
    order.lmtPrice = round(credit, 2)
    order.transmit = True
    order.tif = "DAY"
    order.smartComboRoutingParams = [TagValue("NonGuaranteed", "1")]

    trade = ib.placeOrder(combo, order)
    log.info("Submitted spread entry order #%s for %d contracts at $%.2f credit",
             trade.order.orderId, num_contracts, credit)

    # Wait for fill (up to 60 seconds)
    for _ in range(60):
        ib.sleep(1)
        if trade.orderStatus.status == "Filled":
            log.info("ORDER FILLED at $%.2f", trade.orderStatus.avgFillPrice)
            return trade.order.orderId
        elif trade.orderStatus.status in ("Cancelled", "ApiCancelled"):
            log.error("Order cancelled: %s", trade.orderStatus.status)
            return None

    log.info("Order still working (status: %s). Will check on next run.",
             trade.orderStatus.status)
    return trade.order.orderId


def submit_spread_close(ib: IB, pos: Dict) -> Optional[int]:
    """Submit closing order for an open put spread position."""
    combo = Contract()
    combo.symbol = pos["ticker"]
    combo.secType = "BAG"
    combo.currency = "USD"
    combo.exchange = "SMART"

    # Reverse: buy back short put, sell long put
    combo.comboLegs = [
        ComboLeg(conId=pos["sp_conid"], ratio=1, action="BUY",
                 exchange="SMART"),
        ComboLeg(conId=pos["lp_conid"], ratio=1, action="SELL",
                 exchange="SMART"),
    ]

    order = Order()
    order.action = "BUY"        # buying back = closing
    order.orderType = "MKT"
    order.totalQuantity = pos["num_contracts"]
    order.transmit = True
    order.tif = "DAY"
    order.smartComboRoutingParams = [TagValue("NonGuaranteed", "1")]

    trade = ib.placeOrder(combo, order)
    log.info("Submitted spread CLOSE order #%s", trade.order.orderId)

    for _ in range(30):
        ib.sleep(1)
        if trade.orderStatus.status == "Filled":
            log.info("CLOSE FILLED at $%.2f", trade.orderStatus.avgFillPrice)
            return trade.order.orderId
        elif trade.orderStatus.status in ("Cancelled", "ApiCancelled"):
            log.error("Close order cancelled")
            return None

    log.info("Close order still working (status: %s)", trade.orderStatus.status)
    return trade.order.orderId


# ===================================================================
# Core logic
# ===================================================================

def check_entry_signal(ticker: str, ib_port: int,
                       dry_run: bool = False, force: bool = False) -> bool:
    """Check if we should enter a new put spread today. Returns True if entered."""
    today = datetime.now().strftime("%Y-%m-%d")

    # Check spacing
    last_entry = get_last_entry_date(ticker)
    if last_entry and not force:
        days_since = (datetime.strptime(today, "%Y-%m-%d")
                      - datetime.strptime(last_entry, "%Y-%m-%d")).days
        if days_since < MIN_DAYS_BETWEEN_ENTRIES:
            log.info("[%s] Skipping: only %d days since last entry (min %d)",
                     ticker, days_since, MIN_DAYS_BETWEEN_ENTRIES)
            return False

    # Check max open positions
    open_pos = get_open_positions(ticker)
    if len(open_pos) >= MAX_OPEN_POSITIONS and not force:
        log.info("[%s] Skipping: %d open positions (max %d)",
                 ticker, len(open_pos), MAX_OPEN_POSITIONS)
        return False

    # Get VIX and IV rank
    vix, iv_rank = get_vix_data()
    if vix is None or iv_rank is None:
        log.error("Cannot compute VIX/IV rank")
        return False
    log.info("VIX: %.1f  IV Rank: %.1f%%", vix, iv_rank * 100)

    # IV rank filter
    if iv_rank < IV_RANK_FLOOR and not force:
        log.info("[%s] Skipping: IV rank %.1f%% < floor %.1f%%",
                 ticker, iv_rank * 100, IV_RANK_FLOOR * 100)
        log_daily(today, None, ticker, 0, vix, action="skip",
                  notes=f"IVR {iv_rank:.1%} < {IV_RANK_FLOOR:.0%}")
        return False

    # SMA filter
    current_price, sma_value = get_sma(ticker, SMA_PERIOD)
    if current_price is None or sma_value is None:
        log.error("[%s] Cannot compute SMA", ticker)
        return False
    log.info("[%s] Price: $%.2f  SMA(%d): $%.2f",
             ticker, current_price, SMA_PERIOD, sma_value)

    if current_price <= sma_value and not force:
        log.info("[%s] Skipping: price $%.2f <= SMA $%.2f (bearish)",
                 ticker, current_price, sma_value)
        log_daily(today, None, ticker, current_price, vix, action="skip",
                  notes=f"Below SMA: ${current_price:.2f} <= ${sma_value:.2f}")
        return False

    log.info("[%s] SIGNAL: IVR=%.1f%%, price above SMA, entering...",
             ticker, iv_rank * 100)

    # Connect to IBKR
    ib = connect_ibkr(ib_port)
    try:
        spot = get_spot(ib, ticker)
        log.info("[%s] Live spot: $%.2f", ticker, spot)

        chain, _ = get_option_chain(ib, ticker)
        expiration = find_expiration(chain)

        # Build spread
        spread = build_spread_contracts(ib, ticker, spot, vix, chain, expiration)
        if spread is None:
            log.error("[%s] Failed to build spread", ticker)
            return False

        # Quote it
        quotes = quote_spread(ib, spread["legs"])
        if quotes is None or quotes["credit"] <= 0:
            log.error("[%s] No valid quotes or negative credit (%.2f)",
                      ticker, quotes["credit"] if quotes else 0)
            return False

        credit = quotes["credit"]
        wing = spread["wing_width"]
        max_loss = (wing - credit) * 100 * NUM_CONTRACTS
        tp_target = credit * TAKE_PROFIT_PCT
        sl_trigger_debit = credit * (1 + STOP_LOSS_MULT)

        exp_str = datetime.strptime(expiration, "%Y%m%d").strftime("%Y-%m-%d")

        log.info("[%s] Credit: $%.2f/sh ($%.0f/ct)  Wing: $%.0f  "
                 "Max loss: $%.0f",
                 ticker, credit, credit * 100, wing, max_loss)
        log.info("[%s] TP when spread <= $%.2f  SL when spread >= $%.2f",
                 ticker, tp_target, sl_trigger_debit)

        if dry_run:
            log.info("DRY RUN — would enter. Not submitting order.")
            print(f"\n  DRY RUN — Put Spread signal detected:")
            print(f"  {ticker} ${spot:.2f}  VIX {vix:.1f}  IVR {iv_rank:.1%}"
                  f"  SMA({SMA_PERIOD}) ${sma_value:.2f}")
            print(f"  SP={spread['sp_strike']:.0f}  LP={spread['lp_strike']:.0f}"
                  f"  Wing=${wing:.0f}")
            print(f"  Credit: ${credit:.2f}/sh (${credit*100:.0f}/ct)"
                  f"  Max loss: ${max_loss:.0f}")
            print(f"  Exp: {exp_str}  TP <= ${tp_target:.2f}"
                  f"  SL >= ${sl_trigger_debit:.2f}")
            return False

        # Submit order
        order_id = submit_spread_entry(
            ib, ticker, spread["legs"], credit, NUM_CONTRACTS)

        # Save position
        pos_id = save_position({
            "ticker": ticker,
            "entry_date": today,
            "expiration": exp_str,
            "spot": spot,
            "vix": vix,
            "iv_rank": iv_rank,
            "sma_value": sma_value,
            "short_delta": SHORT_DELTA,
            "sp_strike": spread["sp_strike"],
            "lp_strike": spread["lp_strike"],
            "sp_conid": spread["legs"]["sp"].conId,
            "lp_conid": spread["legs"]["lp"].conId,
            "entry_credit": credit,
            "wing_width": wing,
            "max_loss": max_loss,
            "num_contracts": NUM_CONTRACTS,
            "tp_target": tp_target,
            "sl_trigger_debit": sl_trigger_debit,
            "entry_order_id": order_id,
            "notes": f"VIX={vix:.1f} IVR={iv_rank:.1%} SMA={sma_value:.2f}",
        })

        log_daily(today, pos_id, ticker, spot, vix,
                  sp_mid=quotes["mids"]["sp"], lp_mid=quotes["mids"]["lp"],
                  spread_value=credit, action="entry",
                  notes=f"credit=${credit:.2f} order={order_id}")

        log.info("[%s] Position #%d saved", ticker, pos_id)
        return True

    finally:
        ib.disconnect()


def monitor_positions(ib_port: int, dry_run: bool = False):
    """Check open positions for TP/SL/expiration exits."""
    open_pos = get_open_positions()
    if not open_pos:
        log.info("No open positions to monitor")
        return

    today = datetime.now().strftime("%Y-%m-%d")
    vix, _ = get_vix_data()
    vix = vix or 0

    ib = connect_ibkr(ib_port)
    try:
        for pos in open_pos:
            ticker = pos["ticker"]
            spot = get_spot(ib, ticker)
            log.info("--- Position #%d [%s] (entered %s, exp %s) ---",
                     pos["id"], ticker, pos["entry_date"], pos["expiration"])

            # Check expiration
            exp_dt = datetime.strptime(pos["expiration"], "%Y-%m-%d").date()
            today_dt = datetime.strptime(today, "%Y-%m-%d").date()

            if today_dt >= exp_dt:
                # Settle at intrinsic
                sp_intrinsic = max(0, pos["sp_strike"] - spot)
                lp_intrinsic = max(0, pos["lp_strike"] - spot)
                settle = sp_intrinsic - lp_intrinsic
                pnl = (pos["entry_credit"] - settle) * 100 * pos["num_contracts"]

                log.info("  EXPIRATION: settle=$%.2f  P&L=$%.2f", settle, pnl)
                if not dry_run:
                    close_position(pos["id"], today, "expiration", settle, pnl)
                    log_daily(today, pos["id"], ticker, spot, vix,
                              spread_value=settle, unrealized_pnl=pnl,
                              action="expiration",
                              notes=f"settled ${settle:.2f}")
                continue

            # Re-quote the legs
            legs = {
                "sp": Option(conId=pos["sp_conid"], exchange="SMART"),
                "lp": Option(conId=pos["lp_conid"], exchange="SMART"),
            }
            contracts = list(legs.values())
            ib.qualifyContracts(*contracts)

            md_list = []
            for c in contracts:
                md = ib.reqMktData(c, "", False, False)
                md_list.append(md)
            ib.sleep(5)

            mids = {}
            bids_asks = {}
            for name, md in zip(["sp", "lp"], md_list):
                bid = md.bid if md.bid and not math.isnan(md.bid) and md.bid > 0 else 0
                ask = md.ask if md.ask and not math.isnan(md.ask) and md.ask > 0 else 0
                mids[name] = (bid + ask) / 2 if bid and ask else 0
                bids_asks[name] = {"bid": bid, "ask": ask}

            for c in contracts:
                try:
                    ib.cancelMktData(c)
                except Exception:
                    pass

            # Cost to close = buy back short put at ask, sell long put at bid
            close_debit = bids_asks["sp"]["ask"] - bids_asks["lp"]["bid"]
            # Mid-based spread value for display
            spread_mid = mids["sp"] - mids["lp"]

            pnl = (pos["entry_credit"] - close_debit) * 100 * pos["num_contracts"]

            log.info("  Spread mid: $%.2f  Close debit: $%.2f  P&L: $%.2f",
                     spread_mid, close_debit, pnl)

            # Take profit: spread value <= TP target (can buy back cheaply)
            if spread_mid <= pos["tp_target"] and spread_mid >= 0:
                log.info("  *** TAKE PROFIT (spread $%.2f <= target $%.2f)",
                         spread_mid, pos["tp_target"])
                if not dry_run:
                    order_id = submit_spread_close(ib, pos)
                    close_position(pos["id"], today, "take_profit",
                                   close_debit, pnl, order_id)
                    log_daily(today, pos["id"], ticker, spot, vix,
                              sp_mid=mids["sp"], lp_mid=mids["lp"],
                              spread_value=spread_mid, unrealized_pnl=pnl,
                              action="take_profit",
                              notes=f"closed at ${close_debit:.2f}")
                else:
                    log.info("  DRY RUN — would close for take profit")
                continue

            # Stop loss: spread value >= SL trigger debit
            if spread_mid >= pos["sl_trigger_debit"]:
                log.info("  *** STOP LOSS (spread $%.2f >= trigger $%.2f)",
                         spread_mid, pos["sl_trigger_debit"])
                if not dry_run:
                    order_id = submit_spread_close(ib, pos)
                    close_position(pos["id"], today, "stop_loss",
                                   close_debit, pnl, order_id)
                    log_daily(today, pos["id"], ticker, spot, vix,
                              sp_mid=mids["sp"], lp_mid=mids["lp"],
                              spread_value=spread_mid, unrealized_pnl=pnl,
                              action="stop_loss",
                              notes=f"loss ${abs(pnl):.2f}")
                else:
                    log.info("  DRY RUN — would close for stop loss")
                continue

            # No exit — hold
            log_daily(today, pos["id"], ticker, spot, vix,
                      sp_mid=mids["sp"], lp_mid=mids["lp"],
                      spread_value=spread_mid, unrealized_pnl=pnl,
                      action="hold", notes=f"P&L=${pnl:.2f}")
            log.info("  Holding (P&L: $%.2f)", pnl)

    finally:
        ib.disconnect()


def close_all_positions(ib_port: int):
    """Emergency close all open positions at market."""
    open_pos = get_open_positions()
    if not open_pos:
        print("No open positions.")
        return

    today = datetime.now().strftime("%Y-%m-%d")
    ib = connect_ibkr(ib_port)
    try:
        for pos in open_pos:
            log.info("Closing position #%d [%s]...", pos["id"], pos["ticker"])
            order_id = submit_spread_close(ib, pos)
            close_position(pos["id"], today, "manual_close", 0, 0, order_id)
            log.info("Position #%d closed", pos["id"])
    finally:
        ib.disconnect()


# ===================================================================
# Display
# ===================================================================

def print_status():
    open_pos = get_open_positions()
    print(f"\n{'='*70}")
    print(f" PUT SPREAD TRADER — Open Positions")
    print(f"{'='*70}")

    if not open_pos:
        print("\n  No open positions.\n")
        return

    for pos in open_pos:
        dte = (datetime.strptime(pos["expiration"], "%Y-%m-%d")
               - datetime.now()).days
        print(f"\n  Position #{pos['id']} [{pos['ticker']}]")
        print(f"    Entered:    {pos['entry_date']}  "
              f"Spot ${pos['spot_at_entry']:.2f}  "
              f"VIX {pos['vix_at_entry']:.1f}  "
              f"IVR {pos['iv_rank']:.1%}")
        print(f"    Strikes:    SP {pos['sp_strike']:.0f}  "
              f"LP {pos['lp_strike']:.0f}  "
              f"(wing ${pos['wing_width']:.0f})")
        print(f"    Expiration: {pos['expiration']} ({dte} DTE)")
        print(f"    Credit:     ${pos['entry_credit']:.2f}/sh  "
              f"Max loss: ${pos['max_loss']:.0f}")
        print(f"    TP target:  spread <= ${pos['tp_target']:.2f}  "
              f"SL trigger: spread >= ${pos['sl_trigger_debit']:.2f}")
    print()


def print_history():
    positions = get_all_positions()
    print(f"\n{'='*70}")
    print(f" PUT SPREAD TRADER — Trade History")
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

    print(f"\n  {'#':<4} {'Tkr':<5} {'Entry':<12} {'Exit':<12} {'Reason':<14} "
          f"{'SP':>5} {'LP':>5} {'Credit':>7} {'P&L':>10} {'VIX':>5}")
    print(f"  {'-'*85}")

    for p in positions:
        exit_dt = p["exit_date"] or "open"
        reason = p["exit_reason"] or "-"
        pnl_str = f"${p['pnl']:+,.2f}" if p["pnl"] is not None else "-"
        print(f"  {p['id']:<4} {p['ticker']:<5} {p['entry_date']:<12} "
              f"{exit_dt:<12} {reason:<14} "
              f"{p['sp_strike']:>5.0f} {p['lp_strike']:>5.0f} "
              f"${p['entry_credit']:>6.2f} {pnl_str:>10} "
              f"{p['vix_at_entry']:>5.1f}")
    print()


def print_scan_summary(ticker: str, spot: float, vix: float, iv_rank: float,
                       sma_value: float, signal: bool):
    """Print a summary of the current scan."""
    print(f"\n{'='*70}")
    print(f" PUT SPREAD SCANNER — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*70}")
    print(f"  Ticker:    {ticker}")
    print(f"  Spot:      ${spot:.2f}")
    print(f"  VIX:       {vix:.1f}")
    print(f"  IV Rank:   {iv_rank:.1%}  "
          f"({'PASS' if iv_rank >= IV_RANK_FLOOR else 'FAIL'}"
          f" >= {IV_RANK_FLOOR:.0%})")
    above = spot > sma_value if sma_value else False
    print(f"  SMA({SMA_PERIOD}): ${sma_value:.2f}  "
          f"({'PASS above' if above else 'FAIL below'})")
    print(f"  Signal:    {'YES' if signal else 'NO'}")
    print(f"\n  Strategy: delta={SHORT_DELTA}, wing_sigma={WING_SIGMA}, "
          f"SL={STOP_LOSS_MULT}x, TP={TAKE_PROFIT_PCT:.0%}")
    print()


# ===================================================================
# Main
# ===================================================================

def main():
    parser = argparse.ArgumentParser(
        description="IBKR Put Credit Spread — Live Scanner & Executor")
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
                        help="Only monitor existing positions")
    parser.add_argument("--ticker", default="SPY",
                        help="Ticker to scan (default: SPY)")
    parser.add_argument("--paper", action="store_true", default=True,
                        help="Use paper trading port 7497 (default)")
    parser.add_argument("--live", action="store_true",
                        help="Use live trading port 7496")
    args = parser.parse_args()

    ib_port = IB_PORT_LIVE if args.live else IB_PORT_PAPER
    ticker = args.ticker.upper()

    init_db()

    if args.status:
        print_status()
        return

    if args.history:
        print_history()
        return

    if args.close_all:
        close_all_positions(ib_port)
        return

    mode_str = "LIVE" if args.live else "PAPER"
    print(f"\n{'='*70}")
    print(f" PUT SPREAD TRADER [{mode_str}] — "
          f"{datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*70}")

    # 1. Monitor existing positions
    monitor_positions(ib_port, dry_run=args.dry_run)

    # 2. Check for new entry signal
    if not args.monitor_only:
        print()
        check_entry_signal(ticker, ib_port,
                           dry_run=args.dry_run, force=args.force_entry)

    # 3. Show status
    print_status()


if __name__ == "__main__":
    main()
