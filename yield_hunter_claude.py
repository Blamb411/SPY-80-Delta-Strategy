from __future__ import annotations

import math
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Optional, Dict, List, Tuple, Any

import numpy as np
import pandas as pd
from ib_insync import IB, Stock, Option, Ticker

# Optional yfinance (earnings + sector). If missing, we skip those enrichments safely.
try:
    import yfinance as yf
except Exception:
    yf = None


# =============================================================================
# CONFIG
# =============================================================================

MAGIC_FORMULA_CSV = "magic_formula_results.csv"
TICKER_COL_CANDIDATES = ["Ticker", "ticker", "Symbol", "symbol", "Stock", "stock", "Instrument"]

# IB connection
IB_HOST = "127.0.0.1"
IB_PORT = 7497            # paper usually 7497; live usually 7496
IB_CLIENT_ID = 22
CONNECT_TIMEOUT_SEC = 15

# Market data type selection
# 1=LIVE, 3=DELAYED, 4=DELAYED_FROZEN
MDT_RTH = 1
MDT_OUTSIDE_RTH = 4

# Strategy definition
DTE_MIN = 30
DTE_MAX = 90
MAX_EXPIRIES_PER_TICKER = 3

TARGET_SHORT_DELTA = -0.25
MAX_DELTA_GAP = 0.12
MAX_SHORT_STRIKES_TO_TRY = 40

# Wing width rule (mid-risk)
WING_WIDTH_PCT = 0.05     # 5% of spot
WING_WIDTH_MIN = 5.0
WING_WIDTH_MAX = 15.0

# Liquidity / quality gates
REQUIRE_BBO = True
MAX_REL_SPREAD_PCT_SHORT = 5.0     # short leg must be <= this
MAX_REL_SPREAD_PCT_LONG = 12.0     # long leg can be wider
MIN_CREDIT = 0.25
MIN_CREDIT_TO_WIDTH = 0.12

TOP_SHORT_CANDS_TO_EVAL = 8
LONG_WING_TRIES = 6

# OI (often missing / unreliable in snapshots; keep off unless you verify it works)
REQUIRE_OI = False
MIN_OI = 50

# Trend/IV/RSI gates
USE_TREND_FILTER = True
SMA_DAYS = 200

USE_RSI_FILTER = True
RSI_PERIOD = 21
RSI_THRESHOLD = 75

USE_IVR_FILTER = True
MIN_IV_RANK = 10

# Earnings handling
EARNINGS_POLICY = "warn"     # "warn" | "skip" | "ignore"
EARNINGS_SOON_DAYS = 10      # hard-skip if earnings is within N days (0 = never)

# Sector diversification
MAX_SECTOR_ALLOC = 2

# pacing
SLEEP_SEC_BETWEEN_TICKERS = 0.15
MAX_TICKERS_PER_RUN: Optional[int] = None

# Logging noise suppression
SUPPRESS_EXPECTED_IB_NOISE = True

# =============================================================================
# DEBUG
# =============================================================================
DEBUG_OPTION_MD = True            # master switch
DEBUG_TICKERS = {"SPY"}           # only print debug for these tickers (edit as needed)
DEBUG_PRINT_ONCE_PER_EXPIRY = True


# =============================================================================
# Reject stats
# =============================================================================

@dataclass
class RejectStats:
    counts: Dict[str, int] = field(default_factory=dict)

    def hit(self, key: str, n: int = 1) -> None:
        key = str(key or "UNKNOWN")
        self.counts[key] = self.counts.get(key, 0) + int(n)

    def merge(self, other: "RejectStats") -> None:
        if other is None:
            return
        for k, v in (other.counts or {}).items():
            self.hit(k, v)

    def summary(self, top: int = 8) -> str:
        if not self.counts:
            return "no details"
        items = sorted(self.counts.items(), key=lambda kv: kv[1], reverse=True)[:max(1, int(top))]
        total = sum(self.counts.values())
        return "total=" + str(total) + " (" + ", ".join(f"{k}={v}" for k, v in items) + ")"


# =============================================================================
# Logging filter for expected IB noise
# =============================================================================

class _SuppressIBNoise(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()

        # "No security definition" (Error 200) expected when probing strikes
        if "Error 200" in msg and "No security definition has been found" in msg:
            return False

        # ib_insync prints "Unknown contract: ..." a lot when probing
        if msg.startswith("Unknown contract:"):
            return False

        # Competing market data session (often happens; not fatal)
        if "Error 10197" in msg:
            return False

        return True


# =============================================================================
# Helpers: math + quotes
# =============================================================================

def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

def pop_above_level_lognormal(spot: float, level: float, iv: float, dte: int, r: float = 0.045) -> Optional[float]:
    """
    Rough risk-neutral-ish probability that S_T > level under lognormal.
    iv is DECIMAL (0.25 for 25%).
    Returns probability in percent (0..100).
    """
    if spot <= 0 or level <= 0 or dte <= 0 or iv <= 0:
        return None
    t = dte / 365.0
    sd = iv * math.sqrt(t)
    if sd <= 0:
        return None
    mu = math.log(spot) + (r - 0.5 * iv * iv) * t
    z = (math.log(level) - mu) / sd
    p = 1.0 - _norm_cdf(z)
    return max(0.0, min(100.0, p * 100.0))

def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def calculate_rsi(prices: List[float], period: int = 14) -> float:
    if len(prices) < period + 1:
        return 50.0
    deltas = np.diff(prices)
    seed = deltas[: period + 1]
    up = seed[seed >= 0].sum() / period
    down = -seed[seed < 0].sum() / period
    if down == 0:
        return 100.0
    rs = up / down
    rsi = np.zeros_like(prices, dtype=float)
    rsi[:period] = 100.0 - 100.0 / (1.0 + rs)
    upv, dnv = up, down
    for i in range(period, len(prices)):
        delta = deltas[i - 1]
        upval = delta if delta > 0 else 0.0
        downval = -delta if delta < 0 else 0.0
        upv = (upv * (period - 1) + upval) / period
        dnv = (dnv * (period - 1) + downval) / period
        if dnv == 0:
            rsi[i] = 100.0
        else:
            rs = upv / dnv
            rsi[i] = 100.0 - 100.0 / (1.0 + rs)
    return float(rsi[-1])

def _is_good_quote(b: Optional[float], a: Optional[float]) -> bool:
    return b is not None and a is not None and b > 0 and a > 0 and a >= b

def _mid(b: Optional[float], a: Optional[float]) -> Optional[float]:
    return (b + a) / 2.0 if _is_good_quote(b, a) else None

def _rel_spread_pct(b: Optional[float], a: Optional[float]) -> Optional[float]:
    m = _mid(b, a)
    if m is None or m <= 0:
        return None
    return ((a - b) / m) * 100.0

def _chunks(seq: List[Any], n: int) -> List[List[Any]]:
    return [seq[i:i+n] for i in range(0, len(seq), n)]

def gate_spread(stats, bid, ask, require_bbo, max_spread, key_prefix):
    if require_bbo and not _is_good_quote(bid, ask):
        stats.hit(f"{key_prefix}_NO_BBO")
        return None, False  # stop

    spr = _rel_spread_pct(bid, ask)
    if spr is None:
        stats.hit(f"{key_prefix}_SPR_NONE")
        return spr, (not require_bbo)  # continue only if not requiring BBO

    if spr > float(max_spread):
        stats.hit(f"{key_prefix}_WIDE")
        return spr, False

    return spr, True


import math
from typing import Optional
from ib_insync import Ticker


def _pick_greeks(t: Ticker):
    """
    IB can populate greeks in different fields depending on data type / timing.

    We score each candidate so we prefer one that actually has usable delta + impliedVol,
    instead of blindly taking the first non-None object.

    Returns:
        A greeks object (modelGreeks / lastGreeks / bidGreeks / askGreeks) or None.
    """
    def _finite(x) -> bool:
        try:
            return x is not None and math.isfinite(float(x))
        except Exception:
            return False

    def _quality_score(g) -> int:
        """
        Lower is better.
        0   = has finite delta AND finite impliedVol > 0
        100 = missing delta
        200 = missing/invalid impliedVol
        """
        if g is None:
            return 10_000

        d = getattr(g, "delta", None)
        iv = getattr(g, "impliedVol", None)

        score = 0
        if not _finite(d):
            score += 100

        if (not _finite(iv)) or float(iv) <= 0:
            score += 200

        return score

    # prefer modelGreeks, then lastGreeks, then bid/ask greeks
    candidates = [
        ("modelGreeks", 0),
        ("lastGreeks", 5),
        ("bidGreeks", 10),
        ("askGreeks", 10),
    ]

    best_g = None
    best_score = 10_000

    for attr, base in candidates:
        g = getattr(t, attr, None)
        if g is None:
            continue
        s = base + _quality_score(g)
        if s < best_score:
            best_score = s
            best_g = g

    return best_g


def _req_tickers_chunked(ib: IB, contracts: List[Any], chunk_size: int = 40) -> List[Ticker]:
    out: List[Ticker] = []
    for ch in _chunks(contracts, chunk_size):
        try:
            out.extend(ib.reqTickers(*ch))
        except Exception:
            continue
    return out


# =============================================================================
# yfinance helpers (cached)
# =============================================================================

_YF_SECTOR_CACHE: Dict[str, str] = {}
_YF_EARNINGS_CACHE: Dict[str, Optional[date]] = {}

def get_sector_yf(ticker: str) -> str:
    if yf is None:
        return "Unknown"
    t = ticker.upper().strip()
    if t in _YF_SECTOR_CACHE:
        return _YF_SECTOR_CACHE[t]
    try:
        info = yf.Ticker(t).info or {}
        sec = info.get("sector") or "Unknown"
    except Exception:
        sec = "Unknown"
    _YF_SECTOR_CACHE[t] = sec
    return sec

def get_next_earnings_date_yf(ticker: str) -> Optional[date]:
    """
    Returns next known earnings date as a date, or None if unavailable.
    Cached per ticker.
    """
    if yf is None:
        return None
    t = ticker.upper().strip()
    if t in _YF_EARNINGS_CACHE:
        return _YF_EARNINGS_CACHE[t]

    out: Optional[date] = None
    try:
        cal = yf.Ticker(t).calendar
        if cal is None:
            out = None
        elif isinstance(cal, pd.DataFrame) and not cal.empty:
            dt_val = cal.iloc[0, 0]
            dt_val = pd.to_datetime(dt_val).to_pydatetime()
            out = dt_val.date()
        elif isinstance(cal, dict):
            v = cal.get("Earnings Date")
            if isinstance(v, (list, tuple)) and v:
                out = pd.to_datetime(v[0]).to_pydatetime().date()
    except Exception:
        out = None

    _YF_EARNINGS_CACHE[t] = out
    return out


# =============================================================================
# Symbol normalization (fix BF-B, BRK.B, etc.)
# =============================================================================

_CLASS_DOT_RE = re.compile(r"^[A-Z]{1,6}\.[A-Z]{1,2}$")
_CLASS_DASH_RE = re.compile(r"^[A-Z]{1,6}-[A-Z]{1,2}$")

def sanitize_raw_ticker(raw: Any) -> Optional[str]:
    """
    Removes exchange suffixes like ".NS" but keeps class tickers like "BRK.B".
    """
    if raw is None:
        return None
    t = str(raw).strip().upper()
    if not t or t == "NAN":
        return None

    # Keep class tickers like BRK.B
    if "." in t and _CLASS_DOT_RE.match(t):
        return t

    # Otherwise strip exchange suffix after dot (e.g., ABCD.NS -> ABCD)
    if "." in t:
        t = t.split(".", 1)[0].strip().upper()

    return t or None

def ib_symbol_variants(ticker: str) -> List[str]:
    """
    Try a few IB-friendly variants:
      BF-B -> BF B / BF.B
      BRK.B -> BRK B / BRK-B
    """
    t = ticker.upper().strip()
    variants = [t]

    if _CLASS_DOT_RE.match(t):
        variants.append(t.replace(".", " "))
        variants.append(t.replace(".", "-"))

    if _CLASS_DASH_RE.match(t):
        variants.append(t.replace("-", " "))
        variants.append(t.replace("-", "."))

    if "-" in t and t.replace("-", " ") not in variants:
        variants.append(t.replace("-", " "))

    out: List[str] = []
    seen = set()
    for v in variants:
        if v and v not in seen:
            out.append(v)
            seen.add(v)
    return out


# =============================================================================
# Options chain helpers
# =============================================================================

@dataclass
class ChainInfo:
    tradingClass: str
    multiplier: str
    expirations: List[str]
    strikes: List[float]

def dte_from_exp(exp_yyyymmdd: str, today: date) -> Optional[int]:
    try:
        exp_d = datetime.strptime(exp_yyyymmdd, "%Y%m%d").date()
    except Exception:
        return None
    return (exp_d - today).days

def choose_expirations(exps: List[str], dte_min: int, dte_max: int, max_n: int, today: date) -> List[Tuple[str, int]]:
    parsed: List[Tuple[str, int]] = []
    for e in exps:
        dte = dte_from_exp(e, today)
        if dte is None:
            continue
        if dte_min <= dte <= dte_max:
            parsed.append((e, dte))
    if not parsed:
        return []

    targets = [30, 60, 90]
    chosen: List[Tuple[str, int]] = []
    used = set()

    for tgt in targets:
        cands = [x for x in parsed if x[0] not in used]
        if not cands:
            break
        best = sorted(cands, key=lambda x: (abs(x[1] - tgt), x[1]))[0]
        chosen.append(best)
        used.add(best[0])
        if len(chosen) >= max_n:
            break

    if len(chosen) < max_n:
        rem = sorted([x for x in parsed if x[0] not in used], key=lambda x: x[1])
        for x in rem:
            chosen.append(x)
            if len(chosen) >= max_n:
                break

    return chosen[:max_n]


# =============================================================================
# Engine
# =============================================================================

class YieldHunterEngine:
    def __init__(self) -> None:
        self.ib = IB()

    def connect(self) -> None:
        if SUPPRESS_EXPECTED_IB_NOISE:
            logging.getLogger("ib_insync.wrapper").addFilter(_SuppressIBNoise())
            logging.getLogger("ib_insync.ib").addFilter(_SuppressIBNoise())

        self.ib.connect(IB_HOST, IB_PORT, clientId=IB_CLIENT_ID, timeout=CONNECT_TIMEOUT_SEC)
        if not self.ib.isConnected():
            raise RuntimeError("IB connect failed.")
        try:
            self.ib.RequestTimeout = 30
        except Exception:
            pass
        print(f"✅ Connected to IBKR {IB_HOST}:{IB_PORT} clientId={IB_CLIENT_ID}")

    def disconnect(self) -> None:
        try:
            self.ib.disconnect()
        except Exception:
            pass

    def set_market_data_type_auto(self) -> None:
        now = datetime.now()
        wd_ok = now.weekday() < 5
        mins = now.hour * 60 + now.minute
        rth = wd_ok and (9 * 60 + 30) <= mins <= (16 * 60)

        mdt = MDT_RTH if rth else MDT_OUTSIDE_RTH
        try:
            self.ib.reqMarketDataType(mdt)
        except Exception:
            pass

        if not rth:
            print("\n" + "!" * 80)
            print("!!! WARNING: OUTSIDE RTH — QUOTES/GREEKS MAY BE STALE OR MISSING !!!")
            print("Run during RTH for trade-quality pricing.")
            print("!" * 80)

    def resolve_stock(self, display_ticker: str) -> Optional[Stock]:
        # Fast path
        s0 = Stock(display_ticker, "SMART", "USD")
        try:
            self.ib.qualifyContracts(s0)
            if int(getattr(s0, "conId", 0) or 0) > 0:
                return s0
        except Exception:
            pass

        # Variants path
        for sym in ib_symbol_variants(display_ticker):
            s = Stock(sym, "SMART", "USD")
            try:
                details = self.ib.reqContractDetails(s)
                if details:
                    c = details[0].contract
                    if getattr(c, "secType", "") == "STK" and getattr(c, "currency", "") == "USD":
                        try:
                            self.ib.qualifyContracts(c)
                        except Exception:
                            pass
                        if int(getattr(c, "conId", 0) or 0) > 0:
                            return c
            except Exception:
                continue

        return None

    def get_market_context(self, display_ticker: str) -> Optional[Dict[str, Any]]:
        underlying = self.resolve_stock(display_ticker)
        if underlying is None:
            return None

        try:
            bars = self.ib.reqHistoricalData(
                underlying, endDateTime="", durationStr="1 Y", barSizeSetting="1 day",
                whatToShow="TRADES", useRTH=True, formatDate=1
            )
        except Exception:
            bars = []

        if not bars or len(bars) < max(SMA_DAYS, RSI_PERIOD) + 5:
            return None

        closes = [float(b.close) for b in bars if b.close is not None and float(b.close) > 0]
        if len(closes) < max(SMA_DAYS, RSI_PERIOD) + 5:
            return None

        spot = closes[-1]
        sma = float(np.mean(closes[-SMA_DAYS:]))
        rsi = float(calculate_rsi(closes, RSI_PERIOD))

        iv_rank = None
        if USE_IVR_FILTER:
            try:
                iv_bars = self.ib.reqHistoricalData(
                    underlying, endDateTime="", durationStr="1 Y", barSizeSetting="1 day",
                    whatToShow="OPTION_IMPLIED_VOLATILITY", useRTH=True, formatDate=1
                )
                if iv_bars and len(iv_bars) >= 20:
                    ivs = [float(b.close) for b in iv_bars if b.close is not None and float(b.close) > 0]
                    if ivs:
                        lo, hi, cur = min(ivs), max(ivs), ivs[-1]
                        iv_rank = 0.0 if hi <= lo else (cur - lo) / (hi - lo) * 100.0
            except Exception:
                iv_rank = None

        return {
            "underlying": underlying,
            "spot": float(spot),
            "sma": float(sma),
            "rsi": float(rsi),
            "iv_rank": iv_rank,
        }

    def get_chain_info(self, underlying: Stock) -> Optional[ChainInfo]:
        try:
            params_list = self.ib.reqSecDefOptParams(underlying.symbol, "", underlying.secType, underlying.conId)
        except Exception:
            return None

        smart = [p for p in params_list if getattr(p, "exchange", "") == "SMART"]
        if not smart:
            return None

        def rank(p):
            tc = str(getattr(p, "tradingClass", "") or "")
            mult = str(getattr(p, "multiplier", "") or "")
            exps = getattr(p, "expirations", []) or []
            strikes = getattr(p, "strikes", []) or []
            return (
                0 if tc == underlying.symbol else 1,
                0 if mult == "100" else 1,
                0 if len(exps) > 0 else 1,
                0 if len(strikes) > 0 else 1,
            )

        p = sorted(smart, key=rank)[0]

        exps = sorted(list(set(getattr(p, "expirations", []) or [])))
        strikes = sorted(list(set(float(s) for s in (getattr(p, "strikes", []) or []) if s is not None)))

        if not exps or not strikes:
            return None

        return ChainInfo(
            tradingClass=str(getattr(p, "tradingClass", "") or underlying.symbol),
            multiplier=str(getattr(p, "multiplier", "") or "100"),
            expirations=exps,
            strikes=strikes,
        )

    def pick_put_credit_spread(
    self,
    *,
    display_ticker: str,
    underlying: Stock,
    spot: float,
    chain: ChainInfo,
    exp: str,
    dte: int,
    stats: Optional[RejectStats] = None,
) -> Optional[Dict[str, Any]]:
    """
    Returns best put credit spread for this expiry, or None.
    Updates stats with reject reasons.
    """
    if stats is None:
        stats = RejectStats()

    # Debug controls (assumes these globals exist in your file)
    debug = bool(DEBUG_OPTION_MD and (display_ticker in DEBUG_TICKERS))
    debug_budget = 3 if bool(DEBUG_PRINT_ONCE_PER_EXPIRY) else 10**9

    def _dbg(msg: str) -> None:
        nonlocal debug_budget
        if not debug:
            return
        if debug_budget <= 0:
            return
        print(msg)
        debug_budget -= 1

    # --- Wing width rule (dollars) ---
    target_width = clamp(float(spot) * float(WING_WIDTH_PCT), float(WING_WIDTH_MIN), float(WING_WIDTH_MAX))

    strikes_all = sorted({float(s) for s in (chain.strikes or [])})
    if not strikes_all:
        stats.hit("NO_STRIKES")
        return None

    # OTM put strikes for short leg
    put_strikes = [k for k in strikes_all if k < float(spot)]
    if not put_strikes:
        stats.hit("NO_PUT_STRIKES")
        return None

    # bounded scan
    put_strikes.sort(reverse=True)
    put_strikes = put_strikes[: int(MAX_SHORT_STRIKES_TO_TRY)]

    tc = chain.tradingClass or display_ticker
    mult = str(chain.multiplier or "100")

    # --- Build + qualify short puts ---
    short_opts: List[Option] = [
        Option(
            symbol=underlying.symbol,
            lastTradeDateOrContractMonth=exp,
            strike=float(k),
            right="P",
            exchange="SMART",
            currency="USD",
            multiplier=mult,
            tradingClass=tc,
        )
        for k in put_strikes
    ]

    for ch in _chunks(short_opts, 30):
        try:
            self.ib.qualifyContracts(*ch)
        except Exception:
            stats.hit("QUALIFY_SHORT_EXC")

    qualified_shorts = [c for c in short_opts if int(getattr(c, "conId", 0) or 0) > 0]
    if not qualified_shorts:
        stats.hit("QUALIFY_NONE")
        return None

    short_tickers = _req_tickers_chunked(self.ib, qualified_shorts, chunk_size=35)

    # ---- DEBUG: short leg snapshot sanity check ----
    if short_tickers:
        tt = short_tickers[0]
        mg0 = getattr(tt, "modelGreeks", None)
        _dbg(
            f"DEBUG SHORT {display_ticker} {exp}: "
            f"mdType={getattr(tt,'marketDataType',None)} "
            f"bid/ask={getattr(tt,'bid',None)}/{getattr(tt,'ask',None)} "
            f"last={getattr(tt,'last',None)} "
            f"delta={getattr(mg0,'delta',None) if mg0 else None} "
            f"iv={getattr(mg0,'impliedVol',None) if mg0 else None}"
        )

    if not short_tickers:
        stats.hit("NO_TICKERS_SHORT")
        return None

    # --- Filter to usable short candidates ---
    short_cands: List[Dict[str, Any]] = []
    for t in short_tickers:
        c = getattr(t, "contract", None)
        if c is None:
            stats.hit("SHORT_NO_CONTRACT")
            continue

        mg = _pick_greeks(t)
        if mg is None:
            stats.hit("NO_GREEKS_SHORT")
            continue

        if debug:
            _dbg(
                f"DEBUG GREEKS_PICK {display_ticker} {exp}: "
                f"has(model/bid/ask/last)="
                f"{bool(getattr(t,'modelGreeks',None))}/"
                f"{bool(getattr(t,'bidGreeks',None))}/"
                f"{bool(getattr(t,'askGreeks',None))}/"
                f"{bool(getattr(t,'lastGreeks',None))} "
                f"picked_delta={getattr(mg,'delta',None)} "
                f"picked_iv={getattr(mg,'impliedVol',None)}"
            )


        delta = getattr(mg, "delta", None)
        iv = getattr(mg, "impliedVol", None)

        if delta is None or iv is None or float(iv) <= 0:
            stats.hit("SHORT_DELTA_IV_MISSING")
            continue
        if float(delta) >= 0:
            stats.hit("SHORT_DELTA_NOT_PUT")
            continue

        bid = getattr(t, "bid", None)
        ask = getattr(t, "ask", None)

        if REQUIRE_BBO and not _is_good_quote(bid, ask):
            stats.hit("SHORT_NO_BBO")
            continue

        spr_pct = _rel_spread_pct(bid, ask)

        # If BBO required: reject missing spread (because it implies missing/invalid quotes)
        # If BBO NOT required: don't reject missing spread; still reject "too wide" if present.
        if spr_pct is None:
            stats.hit("SHORT_SPR_NONE")
            if REQUIRE_BBO:
                continue
        else:
            if float(spr_pct) > float(MAX_REL_SPREAD_PCT_SHORT):
                stats.hit("SHORT_WIDE")
                continue

        if REQUIRE_OI:
            oi = getattr(t, "putOpenInterest", None)
            if oi is not None and MIN_OI is not None and int(oi) < int(MIN_OI):
                stats.hit("OI_LOW")
                continue

        strike = float(getattr(c, "strike", 0.0))
        dgap = abs(float(delta) - float(TARGET_SHORT_DELTA))

        short_cands.append(
            dict(
                ticker=t,
                strike=strike,
                delta=float(delta),
                iv=float(iv),  # decimal
                bid=float(bid) if bid is not None else None,
                ask=float(ask) if ask is not None else None,
                spr_pct=float(spr_pct) if spr_pct is not None else None,
                dgap=float(dgap),
            )
        )

    if not short_cands:
        stats.hit("NO_USABLE_SHORTS")
        return None

    short_cands.sort(key=lambda x: x["dgap"])
    if float(short_cands[0]["dgap"]) > float(MAX_DELTA_GAP):
        stats.hit("DELTA_GAP_TOO_WIDE")
        return None

    short_top = short_cands[: min(int(TOP_SHORT_CANDS_TO_EVAL), len(short_cands))]

    # --- Build/qualify long candidates (multiple per short) ---
    long_opts: List[Option] = []
    long_meta: List[Tuple[int, float, Option]] = []  # (sid, longStrike, opt)

    for sid, s in enumerate(short_top):
        short_strike = float(s["strike"])
        target_long = short_strike - float(target_width)

        below = [k for k in strikes_all if k < short_strike]
        if not below:
            stats.hit("NO_LONG_STRIKES")
            continue

        below.sort(reverse=True)

        start_idx = None
        for j, k in enumerate(below):
            if k <= target_long:
                start_idx = j
                break
        if start_idx is None:
            start_idx = len(below) - 1

        # IMPORTANT: define alt_strikes BEFORE using it (your current code was missing this)
        alt_strikes = below[start_idx : start_idx + int(LONG_WING_TRIES)]
        if not alt_strikes:
            stats.hit("NO_LONG_CANDS")
            continue

        # ---- DEBUG: wing strike selection sanity check ----
        if sid == 0:
            _dbg(
                f"DEBUG WING {display_ticker} {exp}: "
                f"short={short_strike} width={target_width} "
                f"target_long={target_long} alts={alt_strikes[:min(6, len(alt_strikes))]}"
            )

        for alt in alt_strikes:
            opt = Option(
                symbol=underlying.symbol,
                lastTradeDateOrContractMonth=exp,
                strike=float(alt),
                right="P",
                exchange="SMART",
                currency="USD",
                multiplier=mult,
                tradingClass=tc,
            )
            long_opts.append(opt)
            long_meta.append((sid, float(alt), opt))

    if not long_opts:
        stats.hit("NO_LONG_OPTS")
        return None

    for ch in _chunks(long_opts, 30):
        try:
            self.ib.qualifyContracts(*ch)
        except Exception:
            stats.hit("QUALIFY_LONG_EXC")

    # --- group qualified long candidates by sid ---
    long_cands_by_sid: Dict[int, List[Option]] = {}
    fail_ct = 0
    for sid, _alt, opt in long_meta:
        if int(getattr(opt, "conId", 0) or 0) > 0:
            long_cands_by_sid.setdefault(sid, []).append(opt)
        else:
            fail_ct += 1
    if fail_ct:
        stats.hit("QUALIFY_LONG_FAIL", fail_ct)

    all_qual_longs = [opt for opts in long_cands_by_sid.values() for opt in opts]
    if not all_qual_longs:
        stats.hit("NO_QUAL_LONG")
        return None

    # request tickers for ALL qualified long candidates
    long_tickers = _req_tickers_chunked(self.ib, all_qual_longs, chunk_size=35)
    by_conid: Dict[int, Ticker] = {}
    for t in long_tickers:
        c = getattr(t, "contract", None)
        cid = int(getattr(c, "conId", 0) or 0) if c is not None else 0
        if cid:
            by_conid[cid] = t

    # --- Evaluate ALL (short, long) combos and pick best trade ---
    trades: List[Dict[str, Any]] = []

    for sid, s in enumerate(short_top):
        longs_here = long_cands_by_sid.get(sid, [])
        if not longs_here:
            stats.hit("NO_QUAL_LONG_FOR_SHORT")
            continue

        short_bid = s["bid"]
        short_ask = s["ask"]
        short_mid = _mid(short_bid, short_ask)

        for long_c in longs_here:
            t_long = by_conid.get(int(getattr(long_c, "conId", 0) or 0))
            if t_long is None:
                stats.hit("NO_TICKER_LONG")
                continue

            long_bid = getattr(t_long, "bid", None)
            long_ask = getattr(t_long, "ask", None)

            if REQUIRE_BBO and not _is_good_quote(long_bid, long_ask):
                stats.hit("LONG_NO_BBO")
                continue

            long_spr_pct = _rel_spread_pct(long_bid, long_ask)
            if long_spr_pct is None:
                stats.hit("LONG_SPR_NONE")
                if REQUIRE_BBO:
                    continue
            else:
                if float(long_spr_pct) > float(MAX_REL_SPREAD_PCT_LONG):
                    stats.hit("LONG_WIDE")
                    continue

            long_mid = _mid(long_bid, long_ask)

            if short_mid is None or long_mid is None:
                stats.hit("MID_MISSING")
                continue

            # pricing
            credit_cons = None
            if _is_good_quote(short_bid, short_ask) and _is_good_quote(long_bid, long_ask):
                credit_cons = float(short_bid) - float(long_ask)

            credit_mid = float(short_mid) - float(long_mid)
            credit_est = float(credit_cons) if credit_cons is not None else float(credit_mid)

            if credit_est <= 0:
                stats.hit("CREDIT_NONPOS")
                continue
            if credit_est < float(MIN_CREDIT):
                stats.hit("CREDIT_TOO_LOW")
                continue

            short_strike = float(s["strike"])
            long_strike = float(getattr(long_c, "strike", 0.0))
            width = short_strike - long_strike
            if width <= 0:
                stats.hit("WIDTH_BAD")
                continue

            ctw = credit_est / width
            if ctw < float(MIN_CREDIT_TO_WIDTH):
                stats.hit("CREDIT_WIDTH_LOW")
                continue

            max_loss = width - credit_est
            if max_loss <= 0:
                stats.hit("MAXLOSS_NONPOS")
                continue

            breakeven = short_strike - credit_est

            short_iv = float(s["iv"])  # decimal
            pop_be = pop_above_level_lognormal(spot, breakeven, short_iv, dte)
            if pop_be is None:
                stats.hit("POP_NA")
                pop_be = float("nan")

            pop = (pop_be / 100.0) if pop_be == pop_be else None
            ann_ev = None
            if pop is not None:
                ev = (credit_est * pop) - (max_loss * (1.0 - pop))
                ann_ev = (ev / max_loss) * (365.0 / max(dte, 1)) * 100.0

            trades.append(
                {
                    "Ticker": display_ticker,
                    "Spot": round(float(spot), 2),
                    "Expiry": exp,
                    "DTE": int(dte),
                    "LongStrike": float(long_strike),
                    "ShortStrike": float(short_strike),
                    "CreditEst": round(float(credit_est), 2),
                    "CreditMid": round(float(credit_mid), 2),
                    "CreditCons": round(float(credit_cons), 2) if credit_cons is not None else None,
                    "Credit/Width": round(float(ctw), 3),
                    "MaxLoss": round(float(max_loss), 2),
                    "Breakeven": round(float(breakeven), 2),
                    "POP_BE%": round(float(pop_be), 1) if pop_be == pop_be else None,
                    "ShortDelta": round(float(s["delta"]), 3),
                    "ShortIV%": round(float(short_iv) * 100.0, 1),
                    "ShortSpr%": round(float(s["spr_pct"]), 2) if s.get("spr_pct") is not None else None,
                    "LongSpr%": round(float(long_spr_pct), 2) if long_spr_pct is not None else None,
                    "Ann_EV%": round(float(ann_ev), 2) if ann_ev is not None else None,
                }
            )

    if not trades:
        stats.hit("NO_SPREADS_PASS")
        return None

    def _score(tr: Dict[str, Any]) -> float:
        a = tr.get("Ann_EV%")
        if a is not None:
            try:
                return float(a)
            except Exception:
                pass
        return float(tr.get("Credit/Width", 0.0) or 0.0)

    trades.sort(key=_score, reverse=True)
    return trades[0]


# =============================================================================
# MAIN
# =============================================================================

def run() -> None:
    logging.basicConfig(level=logging.WARNING)

    today = date.today()  # ✅ define once, use for earnings window comparisons

    print("🏹 YIELD HUNTER — Put Credit Spread Scanner (Δ≈0.25, 30–90 DTE)")
    print(f"   Wing width rule: clamp({WING_WIDTH_PCT*100:.1f}% of spot, ${WING_WIDTH_MIN:.0f}..${WING_WIDTH_MAX:.0f})")
    print(
        f"   Liquidity: require_bbo={REQUIRE_BBO}, "
        f"shortSpr<={MAX_REL_SPREAD_PCT_SHORT:.1f}%, longSpr<={MAX_REL_SPREAD_PCT_LONG:.1f}%, "
        f"min_credit=${MIN_CREDIT:.2f}, min_credit/width={MIN_CREDIT_TO_WIDTH:.2f}"
    )
    print("")

    df = pd.read_csv(MAGIC_FORMULA_CSV)
    ticker_col = next((c for c in TICKER_COL_CANDIDATES if c in df.columns), None)
    if not ticker_col:
        raise RuntimeError(f"Could not find a ticker column in {MAGIC_FORMULA_CSV}. Tried: {TICKER_COL_CANDIDATES}")

    raw = [sanitize_raw_ticker(x) for x in df[ticker_col].tolist()]
    tickers = [t for t in raw if t]

    if MAX_TICKERS_PER_RUN is not None:
        tickers = tickers[:MAX_TICKERS_PER_RUN]

    print(f"📄 Loaded {len(tickers)} tickers from {MAGIC_FORMULA_CSV} (col={ticker_col})")

    engine = YieldHunterEngine()
    results: List[Dict[str, Any]] = []

    try:
        engine.connect()
        engine.set_market_data_type_auto()

        if DEBUG_DATA:
            5data_health_check(engine, "SPY")


        for i, tkr in enumerate(tickers, start=1):
            print(f"\n[{i}/{len(tickers)}] {tkr}:", end=" ")

            ctx = engine.get_market_context(tkr)
            if not ctx:
                print("Skipped (no market context)")
                engine.ib.sleep(SLEEP_SEC_BETWEEN_TICKERS)
                continue

            underlying: Stock = ctx["underlying"]
            spot = float(ctx["spot"])
            sma = float(ctx["sma"])
            rsi = float(ctx["rsi"])
            iv_rank = ctx["iv_rank"]

            if USE_TREND_FILTER and spot < sma:
                print("Skipped 📉 (below SMA)")
                engine.ib.sleep(SLEEP_SEC_BETWEEN_TICKERS)
                continue

            if USE_RSI_FILTER and rsi > RSI_THRESHOLD:
                print(f"Skipped 🔥 (RSI {rsi:.1f} > {RSI_THRESHOLD})")
                engine.ib.sleep(SLEEP_SEC_BETWEEN_TICKERS)
                continue

            if USE_IVR_FILTER:
                if iv_rank is None:
                    print("Skipped 😴 (IV Rank unavailable)")
                    engine.ib.sleep(SLEEP_SEC_BETWEEN_TICKERS)
                    continue
                if float(iv_rank) < MIN_IV_RANK:
                    print(f"Skipped 😴 (IV Rank {iv_rank:.1f} < {MIN_IV_RANK})")
                    engine.ib.sleep(SLEEP_SEC_BETWEEN_TICKERS)
                    continue

            chain = engine.get_chain_info(underlying)
            if not chain:
                print("Skipped (no SMART option chain)")
                engine.ib.sleep(SLEEP_SEC_BETWEEN_TICKERS)
                continue

            expiries = choose_expirations(chain.expirations, DTE_MIN, DTE_MAX, MAX_EXPIRIES_PER_TICKER, today=today)
            if not expiries:
                print(f"Skipped (no expiries in {DTE_MIN}-{DTE_MAX} DTE)")
                engine.ib.sleep(SLEEP_SEC_BETWEEN_TICKERS)
                continue

            earnings_dt = get_next_earnings_date_yf(tkr) if EARNINGS_POLICY != "ignore" else None

            best_trade: Optional[Dict[str, Any]] = None
            rej = RejectStats()  # ✅ once per ticker

            for exp, dte in expiries:
                earnings_before_exp = False
                days_to_earn = None

                if earnings_dt is not None:
                    try:
                        exp_d = datetime.strptime(exp, "%Y%m%d").date()
                        days_to_earn = (earnings_dt - today).days
                        earnings_before_exp = (today <= earnings_dt <= exp_d)
                    except Exception:
                        earnings_before_exp = False
                        days_to_earn = None

                # Apply earnings policy
                if earnings_before_exp:
                    rej.hit("EARNINGS_BEFORE_EXP")

                    # Hard skip if earnings is soon (works even in "warn" mode)
                    if EARNINGS_SOON_DAYS and days_to_earn is not None and 0 <= days_to_earn <= EARNINGS_SOON_DAYS:
                        continue

                    # Full skip mode: skip any expiry that contains earnings
                    if EARNINGS_POLICY == "skip":
                        continue

                trade = engine.pick_put_credit_spread(
                    display_ticker=tkr,
                    underlying=underlying,
                    spot=spot,
                    chain=chain,
                    exp=exp,
                    dte=dte,
                    stats=rej,
                )
                if not trade:
                    continue

                # Attach earnings info to the trade so it survives as best_trade
                trade["EarningsDate"] = earnings_dt.isoformat() if earnings_dt else None
                trade["EarningsBeforeExpiry"] = earnings_before_exp
                trade["DaysToEarnings"] = days_to_earn

                score = trade.get("Ann_EV%")
                if score is None:
                    score = trade.get("Credit/Width", 0.0)

                if best_trade is None:
                    best_trade = trade
                else:
                    best_score = best_trade.get("Ann_EV%")
                    if best_score is None:
                        best_score = best_trade.get("Credit/Width", 0.0)
                    if (score is not None) and (best_score is None or score > best_score):
                        best_trade = trade



            if not best_trade:
                print(f"Skipped (no trade found; {rej.summary()})")
                engine.ib.sleep(SLEEP_SEC_BETWEEN_TICKERS)
                continue

            best_trade["RSI"] = round(rsi, 1)
            best_trade["SMA"] = round(sma, 2)
            best_trade["IV_Rank"] = round(float(iv_rank), 1) if iv_rank is not None else None
            best_trade["Sector"] = get_sector_yf(tkr)

            if EARNINGS_POLICY == "warn" and best_trade.get("EarningsBeforeExpiry"):
                dte = best_trade.get("DaysToEarnings")
                dte_s = "?" if dte is None else str(dte)
                print(
                    f"⚠️ Earnings before expiry: {best_trade.get('EarningsDate')} "
                    f"({dte_s} days)"
                )

            print(
                f"✅ Found {best_trade['Expiry']} "
                f"{best_trade['LongStrike']:.0f}/{best_trade['ShortStrike']:.0f}P "
                f"credit≈{best_trade['CreditEst']:.2f} "
                f"(AnnEV={best_trade.get('Ann_EV%')})"
            )

            results.append(best_trade)
            engine.ib.sleep(SLEEP_SEC_BETWEEN_TICKERS)

    finally:
        engine.disconnect()

    if not results:
        print("\nNo trades found.")
        return

    df_out = pd.DataFrame(results)

    print("\n⚖️ APPLYING SECTOR DIVERSIFICATION...")
    df_out = df_out.sort_values(by=["Ann_EV%", "Credit/Width"], ascending=False, na_position="last")

    final_rows = []
    sec_counts: Dict[str, int] = {}
    for _, row in df_out.iterrows():
        sec = str(row.get("Sector") or "Unknown")
        n = sec_counts.get(sec, 0)
        if n < MAX_SECTOR_ALLOC:
            final_rows.append(row)
            sec_counts[sec] = n + 1

    final_df = pd.DataFrame(final_rows)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_name = f"yield_hunter_put_credit_{ts}.csv"
    final_df.to_csv(out_name, index=False)

    print(f"\n🏆 Saved: {out_name}")
    cols = [
        "Ticker", "Sector", "Spot", "Expiry", "DTE",
        "LongStrike", "ShortStrike", "CreditEst", "MaxLoss", "Breakeven",
        "POP_BE%", "ShortDelta", "ShortIV%", "ShortSpr%", "LongSpr%", "Ann_EV%"
        "EarningsDate", "DaysToEarnings", "EarningsBeforeExpiry",
    ]
    cols = [c for c in cols if c in final_df.columns]
    print(final_df[cols].head(15))

DEBUG_DATA = True

def data_health_check(engine: YieldHunterEngine, symbol: str = "SPY") -> None:
    print("\n" + "="*80)
    print(f"DATA HEALTH CHECK: {symbol}")
    print("="*80)

    # --- qualify underlying stock without needing engine.resolve_stock ---
    stk = Stock(symbol, "SMART", "USD")
    try:
        engine.ib.qualifyContracts(stk)
    except Exception:
        pass

    # Fallback if qualify didn't populate conId
    if int(getattr(stk, "conId", 0) or 0) <= 0:
        try:
            details = engine.ib.reqContractDetails(stk)
            if details:
                stk = details[0].contract
                try:
                    engine.ib.qualifyContracts(stk)
                except Exception:
                    pass
        except Exception:
            pass

    if int(getattr(stk, "conId", 0) or 0) <= 0:
        print("Could not qualify underlying stock contract (conId still 0).")
        return

    # Underlying quote (snapshot)
    ut = engine.ib.reqTickers(stk)
    if not ut:
        print("No underlying ticker returned.")
        return
    ut0 = ut[0]
    print(
        f"UNDERLYING {symbol}: mdType={getattr(ut0,'marketDataType',None)} "
        f"bid/ask={getattr(ut0,'bid',None)}/{getattr(ut0,'ask',None)} "
        f"last={getattr(ut0,'last',None)}"
    )

    chain = engine.get_chain_info(stk)
    if not chain:
        print("No SMART chain.")
        return

    expiries = choose_expirations(chain.expirations, 30, 90, 1)
    if not expiries:
        print("No expiry 30–90 DTE.")
        return
    exp, dte = expiries[0]

    spot = float(getattr(ut0, "last", None) or 0.0)
    if spot <= 0:
        b = getattr(ut0, "bid", None)
        a = getattr(ut0, "ask", None)
        m = _mid(b, a)
        spot = float(m or 0.0)

    if spot <= 0:
        print("Could not infer spot from last or bid/ask.")
        return

    strikes = sorted([float(x) for x in (chain.strikes or []) if x is not None])
    if not strikes:
        print("Chain has no strikes.")
        return

    near = min(strikes, key=lambda k: abs(k - spot))
    opt = Option(
        symbol=stk.symbol,
        lastTradeDateOrContractMonth=exp,
        strike=float(near),
        right="P",
        exchange="SMART",
        currency="USD",
        multiplier=str(chain.multiplier or "100"),
        tradingClass=str(chain.tradingClass or stk.symbol),
    )

    try:
        engine.ib.qualifyContracts(opt)
    except Exception:
        pass

    ot = engine.ib.reqTickers(opt)
    if not ot:
        print("No option ticker returned.")
        return
    ot0 = ot[0]
    mg = getattr(ot0, "modelGreeks", None)

    print(
        f"OPTION {symbol} {exp} {near}P: mdType={getattr(ot0,'marketDataType',None)} "
        f"bid/ask={getattr(ot0,'bid',None)}/{getattr(ot0,'ask',None)} "
        f"last={getattr(ot0,'last',None)} "
        f"modelGreeks={'YES' if mg else 'NO'} "
        f"delta={getattr(mg,'delta',None) if mg else None} "
        f"iv={getattr(mg,'impliedVol',None) if mg else None}"
    )


if __name__ == "__main__":
    run()
