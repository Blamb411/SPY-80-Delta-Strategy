"""
LSEG Data Library Client — Full Data Pipeline
================================================
Fetches ALL fundamental data from LSEG Workspace:
  - Value metrics (PE, FwdPE, PB, PS, EV/EBITDA, PEG)
  - Profitability metrics (margins, ROE)
  - Growth metrics (computed from estimate changes over time)
  - EPS revision metrics (consensus changes via SDate)
  - Stock info (name, sector, industry, market cap)

Confirmed working fields (lseg-data 2.1.1 / Workspace):
  TR.PE, TR.FwdPE, TR.PriceToBVPerShare, TR.PriceToSalesPerShare,
  TR.EVToEBITDA, TR.PEG, TR.GrossMargin, TR.OperatingMargin,
  TR.NetProfitMargin, TR.F.ReturnAvgTotEqPct, TR.EPSMean (+ SDate),
  TR.EPSActValue, TR.EPSMeanEstimate, TR.EPSStdDev, TR.EPSSmartEst,
  TR.RevenueMean, TR.EBITDAMean, TR.CompanyName, TR.GICSSector,
  TR.GICSIndustry, TR.CompanyMarketCap, TR.PriceClose
"""

import os
import sys
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple

log = logging.getLogger("lseg_client")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
import db_schema


# -----------------------------------------------------------------------
# Field mapping: our metric names -> LSEG TR fields
# -----------------------------------------------------------------------

# Value fields (fetched directly)
VALUE_FIELDS = {
    "pe_ratio":      "TR.PE",
    "forward_pe":    "TR.FwdPE",
    "pb_ratio":      "TR.PriceToBVPerShare",
    "ps_ratio":      "TR.PriceToSalesPerShare",
    "ev_to_ebitda":  "TR.EVToEBITDA",
    "peg_ratio":     "TR.PEG",
}

# Profitability fields (fetched directly)
PROFITABILITY_FIELDS = {
    "gross_margin":  "TR.GrossMargin",
    "ebit_margin":   "TR.OperatingMargin",
    "net_margin":    "TR.NetProfitMargin",
    "roe":           "TR.F.ReturnAvgTotEqPct",
}

# Stock info fields
INFO_FIELDS = {
    "company_name":  "TR.CompanyName",
    "exchange":      "TR.ExchangeName",
    "sector":        "TR.GICSSector",
    "industry":      "TR.GICSIndustry",
    "market_cap":    "TR.CompanyMarketCap",
    "price":         "TR.PriceClose",
}

# Growth is computed from estimate changes (see compute_growth_metrics)
# EPS revisions computed from consensus changes (see compute_revision_metrics)

# -----------------------------------------------------------------------
# RIC overrides for symbols that don't follow standard mapping
# -----------------------------------------------------------------------
# Multi-class shares use lowercase class letter in LSEG RICs
RIC_OVERRIDES = {
    "BRK.B": "BRKb.N",
    "BRK.A": "BRKa.N",
    "CWEN.A": "CWENa.N",
    "MOG.A": "MOGa.N",
    "MOG.B": "MOGb.N",
}


class LSEGClient:
    """Unified LSEG data client for all fundamental, estimate, and info data."""

    def __init__(self, db_path: str = config.DB_PATH):
        self.db_path = db_path
        self._ld = None
        self._session_open = False
        self._ric_cache = {}  # symbol -> resolved RIC
        self._prefetched = set()  # symbols with bulk-prefetched value/profitability
        self._load_ric_cache()

    # -------------------------------------------------------------------
    # Session management
    # -------------------------------------------------------------------

    def _get_lib(self):
        """Lazy-import lseg.data."""
        if self._ld is None:
            try:
                import lseg.data as ld
                self._ld = ld
            except ImportError:
                try:
                    import refinitiv.data as ld
                    self._ld = ld
                except ImportError:
                    return None
        return self._ld

    def is_available(self) -> bool:
        """Check if LSEG library is installed and session can open."""
        ld = self._get_lib()
        if ld is None:
            return False
        if not self._session_open:
            try:
                ld.open_session()
                self._session_open = True
            except Exception as e:
                log.warning("Failed to open LSEG session: %s", e)
                return False
        return True

    def _ensure_session(self):
        if not self._session_open:
            ld = self._get_lib()
            if ld is None:
                raise RuntimeError("lseg.data not installed")
            ld.open_session()
            self._session_open = True

    def close(self):
        if self._session_open and self._ld:
            try:
                self._ld.close_session()
            except Exception:
                pass
            self._session_open = False

    def _recover_session(self):
        """Close and reopen LSEG session after a connection failure."""
        log.warning("Recovering LSEG session...")
        if self._session_open and self._ld:
            try:
                self._ld.close_session()
            except Exception:
                pass
            self._session_open = False
        time.sleep(1)
        self._ensure_session()
        log.info("Session recovered")

    # -------------------------------------------------------------------
    # RIC resolution
    # -------------------------------------------------------------------

    def _load_ric_cache(self):
        """Load previously resolved RICs from database."""
        try:
            conn = db_schema.get_connection(self.db_path)
            rows = conn.execute("SELECT symbol, ric FROM ric_cache").fetchall()
            for row in rows:
                self._ric_cache[row["symbol"]] = row["ric"]
            conn.close()
        except Exception as e:
            log.debug("RIC cache load skipped (table may not exist): %s", e)

    def _save_ric(self, symbol: str, ric: str):
        """Persist a resolved RIC to the database."""
        try:
            conn = db_schema.get_connection(self.db_path)
            conn.execute(
                "INSERT OR REPLACE INTO ric_cache (symbol, ric, resolved_at) "
                "VALUES (?, ?, ?)",
                (symbol, ric, datetime.utcnow().isoformat()),
            )
            conn.commit()
            conn.close()
        except Exception as e:
            log.warning("Failed to save RIC %s -> %s: %s", symbol, ric, e)

    def _to_ric(self, symbol: str) -> str:
        """Convert ticker to RIC using cache, overrides, and default."""
        if symbol in RIC_OVERRIDES:
            return RIC_OVERRIDES[symbol]
        if symbol in self._ric_cache:
            return self._ric_cache[symbol]
        # Default to .O for unresolved symbols without dots
        if "." not in symbol:
            return f"{symbol}.O"
        # Dot-containing symbol without override — pass through
        return symbol

    def _resolve_rics_batch(self, symbols: List[str]) -> None:
        """
        Resolve correct RICs for a batch of symbols using LSEG batch lookup.

        Tests .O (NASDAQ) first, then .N (NYSE) for failures.
        Results are cached in memory and persisted to DB.
        """
        self._ensure_session()

        # Identify symbols that need resolution
        to_resolve = []
        for sym in symbols:
            if sym in RIC_OVERRIDES or sym in self._ric_cache:
                continue
            if "." in sym:
                # Dot-containing without override — skip batch resolution
                continue
            to_resolve.append(sym)

        if not to_resolve:
            return

        print(f"  Resolving RICs for {len(to_resolve)} symbols...")

        # Step 1: Test .O (NASDAQ) candidates in batches
        valid_rics = set()
        rics_o = [f"{sym}.O" for sym in to_resolve]

        for start in range(0, len(rics_o), 80):
            chunk = rics_o[start:start + 80]
            try:
                df = self._fetch_batch(chunk, ["TR.CompanyName"])
                if df is not None and not df.empty:
                    inst_col = df.columns[0]
                    data_col = df.columns[1] if len(df.columns) > 1 else None
                    if data_col:
                        for _, row in df.iterrows():
                            instrument = str(row[inst_col]).strip()
                            val = row[data_col]
                            if val is not None and str(val) not in ("", "nan", "<NA>"):
                                valid_rics.add(instrument)
            except Exception as e:
                print(f"    Batch resolve error (.O): {e}")

        # Step 2: Test .N (NYSE) for symbols not found on NASDAQ
        need_nyse = [sym for sym in to_resolve if f"{sym}.O" not in valid_rics]

        if need_nyse:
            rics_n = [f"{sym}.N" for sym in need_nyse]
            for start in range(0, len(rics_n), 80):
                chunk = rics_n[start:start + 80]
                try:
                    df = self._fetch_batch(chunk, ["TR.CompanyName"])
                    if df is not None and not df.empty:
                        inst_col = df.columns[0]
                        data_col = df.columns[1] if len(df.columns) > 1 else None
                        if data_col:
                            for _, row in df.iterrows():
                                instrument = str(row[inst_col]).strip()
                                val = row[data_col]
                                if val is not None and str(val) not in ("", "nan", "<NA>"):
                                    valid_rics.add(instrument)
                except Exception as e:
                    print(f"    Batch resolve error (.N): {e}")

        # Step 3: Cache results
        resolved_o = 0
        resolved_n = 0
        unresolved = 0

        for sym in to_resolve:
            ric_o = f"{sym}.O"
            ric_n = f"{sym}.N"

            if ric_o in valid_rics:
                self._ric_cache[sym] = ric_o
                self._save_ric(sym, ric_o)
                resolved_o += 1
            elif ric_n in valid_rics:
                self._ric_cache[sym] = ric_n
                self._save_ric(sym, ric_n)
                resolved_n += 1
            else:
                # Unresolved — likely foreign/OTC stock not in LSEG
                self._ric_cache[sym] = ric_o  # default
                unresolved += 1

        print(f"    NASDAQ (.O): {resolved_o}")
        print(f"    NYSE (.N):   {resolved_n}")
        print(f"    Unresolved:  {unresolved}")

    def _clear_stale_fetches(self, symbols: List[str], as_of_date: str) -> int:
        """
        Clear fetch_log entries for symbols that previously returned 0 metrics.
        This allows them to be re-fetched with corrected RICs.
        """
        conn = db_schema.get_connection(self.db_path)
        cleared = 0

        for sym in symbols:
            fetch_key = f"lseg_all:{sym}:{as_of_date}"
            row = conn.execute(
                "SELECT details FROM fetch_log WHERE fetch_key = ?",
                (fetch_key,),
            ).fetchone()

            if row and row["details"]:
                try:
                    details = json.loads(row["details"])
                    if details.get("metrics_found", 0) == 0:
                        conn.execute(
                            "DELETE FROM fetch_log WHERE fetch_key = ?",
                            (fetch_key,),
                        )
                        cleared += 1
                except (json.JSONDecodeError, TypeError):
                    pass

        conn.commit()
        conn.close()
        return cleared

    # -------------------------------------------------------------------
    # Low-level fetch helpers
    # -------------------------------------------------------------------

    def _is_rate_limit_error(self, exc: Exception) -> bool:
        """Check if an exception indicates a rate limit (needs longer backoff)."""
        msg = str(exc).lower()
        return "too many" in msg or "rate limit" in msg or "throttl" in msg

    def _is_session_error(self, exc: Exception) -> bool:
        """Check if an exception indicates a session/connection problem."""
        msg = str(exc).lower()
        return any(kw in msg for kw in (
            "session", "timeout", "connect", "reset", "broken",
            "eof", "refused", "unavailable", "status", "http",
        ))

    def _fetch_single(self, ric: str, field: str,
                      params: Optional[Dict] = None) -> Optional[float]:
        """Fetch a single numeric field value with retry logic."""
        max_retries = 3
        for attempt in range(max_retries):
            self._ensure_session()
            try:
                if params:
                    df = self._ld.get_data(ric, fields=[field], parameters=params)
                else:
                    df = self._ld.get_data(ric, fields=[field])
                if df is not None and not df.empty and len(df.columns) > 1:
                    val = df.iloc[0, 1]
                    if val is not None and str(val) not in ("", "nan", "<NA>"):
                        return float(val)
                return None  # valid "no data" — don't retry
            except (ValueError, TypeError):
                return None  # data conversion issue — don't retry
            except Exception as e:
                if attempt < max_retries - 1:
                    if self._is_rate_limit_error(e):
                        wait = 5 * (2 ** attempt)  # 5s, 10s
                        log.warning("_fetch_single %s/%s rate-limited (attempt %d) — waiting %.0fs",
                                    ric, field, attempt + 1, wait)
                        time.sleep(wait)
                    elif self._is_session_error(e):
                        wait = 0.5 * (2 ** attempt)
                        log.warning("_fetch_single %s/%s attempt %d failed: %s — retrying in %.1fs",
                                    ric, field, attempt + 1, e, wait)
                        time.sleep(wait)
                        self._recover_session()
                    else:
                        log.warning("_fetch_single %s/%s failed: %s", ric, field, e)
                        return None
                else:
                    log.warning("_fetch_single %s/%s failed after %d attempts: %s",
                                ric, field, max_retries, e)
                    return None
        return None

    def _fetch_single_str(self, ric: str, field: str) -> Optional[str]:
        """Fetch a single string field value."""
        self._ensure_session()
        try:
            df = self._ld.get_data(ric, fields=[field])
            if df is not None and not df.empty and len(df.columns) > 1:
                val = df.iloc[0, 1]
                if val is not None and str(val) not in ("", "nan", "<NA>"):
                    return str(val)
        except Exception as e:
            log.warning("_fetch_single_str %s/%s failed: %s", ric, field, e)
        return None

    def _fetch_batch(self, rics: List[str], fields: List[str],
                     params: Optional[Dict] = None):
        """Fetch multiple fields for multiple RICs with retry logic. Returns DataFrame."""
        max_retries = 3
        for attempt in range(max_retries):
            self._ensure_session()
            try:
                if params:
                    df = self._ld.get_data(rics, fields=fields, parameters=params)
                else:
                    df = self._ld.get_data(rics, fields=fields)
                if df is not None and not df.empty:
                    return df
                return None  # valid empty result — don't retry
            except Exception as e:
                if attempt < max_retries - 1:
                    if self._is_rate_limit_error(e):
                        wait = 5 * (2 ** attempt)  # 5s, 10s
                        log.warning("_fetch_batch (%d RICs, %d fields) rate-limited (attempt %d) — waiting %.0fs",
                                    len(rics), len(fields), attempt + 1, wait)
                        time.sleep(wait)
                    elif self._is_session_error(e):
                        wait = 0.5 * (2 ** attempt)
                        log.warning("_fetch_batch (%d RICs, %d fields) attempt %d failed: %s — retrying in %.1fs",
                                    len(rics), len(fields), attempt + 1, e, wait)
                        time.sleep(wait)
                        self._recover_session()
                    else:
                        log.warning("_fetch_batch (%d RICs, %d fields) failed: %s",
                                    len(rics), len(fields), e)
                        return None
                else:
                    log.warning("_fetch_batch (%d RICs, %d fields) failed after %d attempts: %s",
                                len(rics), len(fields), max_retries, e)
                    return None
        return None

    # -------------------------------------------------------------------
    # Cache helpers
    # -------------------------------------------------------------------

    def _is_fetched(self, fetch_key: str) -> bool:
        conn = db_schema.get_connection(self.db_path)
        row = conn.execute(
            "SELECT 1 FROM fetch_log WHERE fetch_key = ?", (fetch_key,)
        ).fetchone()
        conn.close()
        return row is not None

    def _mark_fetched(self, fetch_key: str, details: Optional[Dict] = None):
        conn = db_schema.get_connection(self.db_path)
        conn.execute(
            "INSERT OR REPLACE INTO fetch_log (fetch_key, last_fetched, details) VALUES (?, ?, ?)",
            (fetch_key, datetime.utcnow().isoformat(),
             json.dumps(details) if details else None),
        )
        conn.commit()
        conn.close()

    def _save_metric(self, symbol: str, as_of_date: str, metric_name: str,
                     value: Optional[float], source: str = "lseg",
                     fiscal_period: str = "TTM"):
        if value is None:
            return
        conn = db_schema.get_connection(self.db_path)
        conn.execute(
            """INSERT OR REPLACE INTO fundamental_data
               (symbol, as_of_date, metric_name, fiscal_period,
                metric_value, source, fetched_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (symbol, as_of_date, metric_name, fiscal_period,
             float(value), source, datetime.utcnow().isoformat()),
        )
        conn.commit()
        conn.close()

    # -------------------------------------------------------------------
    # Batch parsing helper
    # -------------------------------------------------------------------

    def _parse_batch_values(self, df, ric: str,
                            field_map: Dict[str, str]) -> Dict[str, Optional[float]]:
        """
        Extract typed metric values from a batch DataFrame for a given RIC.

        Args:
            df: DataFrame returned by _fetch_batch (may contain multiple RICs)
            ric: The RIC to extract values for
            field_map: {metric_name: TR.Field} mapping

        Returns:
            {metric_name: float_value_or_None}
        """
        result = {name: None for name in field_map}
        if df is None or df.empty:
            return result

        # Find row for this RIC
        inst_col = df.columns[0]
        ric_rows = df[df[inst_col].astype(str).str.strip() == ric]
        if ric_rows.empty:
            return result

        row = ric_rows.iloc[0]

        for metric_name, field in field_map.items():
            # Try matching column by field name
            val = None
            for col in df.columns[1:]:
                col_lower = col.lower()
                field_key = field.split(".")[-1].lower()
                if field_key in col_lower:
                    raw = row[col]
                    if raw is not None and str(raw) not in ("", "nan", "<NA>"):
                        try:
                            val = float(raw)
                        except (ValueError, TypeError):
                            pass
                    break

            # Fallback: positional match
            if val is None:
                keys = list(field_map.keys())
                idx = keys.index(metric_name) + 1  # +1 for instrument col
                if idx < len(row):
                    raw = row.iloc[idx]
                    if raw is not None and str(raw) not in ("", "nan", "<NA>"):
                        try:
                            val = float(raw)
                        except (ValueError, TypeError):
                            pass

            result[metric_name] = val

        return result

    # -------------------------------------------------------------------
    # Stock info
    # -------------------------------------------------------------------

    def fetch_stock_info(self, symbol: str, as_of_date: str) -> Dict[str, Optional[str]]:
        """Fetch company name, sector, industry, market cap, price."""
        ric = self._to_ric(symbol)
        info = {}

        # Try batch fetch for efficiency
        fields = list(INFO_FIELDS.values())
        df = self._fetch_batch([ric], fields)

        if df is not None and not df.empty:
            row = df.iloc[0]
            for metric_name, field in INFO_FIELDS.items():
                # Find the column — LSEG returns human-readable column names
                for col in df.columns[1:]:
                    if field.split(".")[-1].lower() in col.lower() or \
                       metric_name.replace("_", " ") in col.lower():
                        val = row[col]
                        if val is not None and str(val) not in ("", "nan", "<NA>"):
                            info[metric_name] = val
                        break
                # Fallback: use column position
                if metric_name not in info:
                    idx = list(INFO_FIELDS.keys()).index(metric_name) + 1
                    if idx < len(row):
                        val = row.iloc[idx]
                        if val is not None and str(val) not in ("", "nan", "<NA>"):
                            info[metric_name] = val
        else:
            # Fetch individually
            for metric_name, field in INFO_FIELDS.items():
                if metric_name in ("market_cap", "price"):
                    info[metric_name] = self._fetch_single(ric, field)
                else:
                    info[metric_name] = self._fetch_single_str(ric, field)

        # Save to stock_universe table
        self._save_stock_info(symbol, as_of_date, info)
        return info

    def _save_stock_info(self, symbol: str, as_of_date: str,
                         info: Dict[str, any]):
        """Save stock info to stock_universe table."""
        market_cap = info.get("market_cap")
        if market_cap is not None:
            try:
                market_cap = float(market_cap)
            except (ValueError, TypeError):
                market_cap = None

        price = info.get("price")
        if price is not None:
            try:
                price = float(price)
            except (ValueError, TypeError):
                price = None

        sector = info.get("sector")
        industry = info.get("industry")
        is_reit = 1 if (sector and "real estate" in str(sector).lower()) or \
                       (industry and "reit" in str(industry).lower()) else 0

        passes = 1
        if market_cap and market_cap < config.MIN_MARKET_CAP:
            passes = 0
        if price and price < config.MIN_PRICE:
            passes = 0
        if is_reit and config.EXCLUDE_REITS:
            passes = 0

        conn = db_schema.get_connection(self.db_path)
        conn.execute(
            """INSERT OR REPLACE INTO stock_universe
               (symbol, as_of_date, company_name, exchange, sector, industry,
                market_cap, share_price, is_reit, passes_filter)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (symbol, as_of_date,
             str(info.get("company_name", "")) or None,
             str(info.get("exchange", "")) or None,
             str(sector) if sector else None,
             str(industry) if industry else None,
             market_cap, price, is_reit, passes),
        )
        conn.commit()
        conn.close()

    # -------------------------------------------------------------------
    # Value metrics
    # -------------------------------------------------------------------

    def fetch_value_metrics(self, symbol: str, as_of_date: str) -> Dict[str, Optional[float]]:
        """Fetch value ratio metrics (batched: 1-2 API calls instead of 12)."""
        # If already prefetched, read from DB
        if hasattr(self, '_prefetched') and symbol in self._prefetched:
            cached = self.get_cached_metrics(symbol, as_of_date)
            return {k: cached.get(k) for k in VALUE_FIELDS}

        ric = self._to_ric(symbol)
        fields_list = list(VALUE_FIELDS.values())

        # Batch call with SDate
        df = self._fetch_batch([ric], fields_list, {"SDate": as_of_date})
        metrics = self._parse_batch_values(df, ric, VALUE_FIELDS)

        # Fallback for missing fields without SDate
        missing = [k for k, v in metrics.items() if v is None]
        if missing:
            missing_fields = {k: VALUE_FIELDS[k] for k in missing}
            df2 = self._fetch_batch([ric], list(missing_fields.values()))
            fallback = self._parse_batch_values(df2, ric, missing_fields)
            for k, v in fallback.items():
                if v is not None:
                    metrics[k] = v

        for metric_name, val in metrics.items():
            self._save_metric(symbol, as_of_date, metric_name, val)

        return metrics

    # -------------------------------------------------------------------
    # Profitability metrics
    # -------------------------------------------------------------------

    def fetch_profitability_metrics(self, symbol: str,
                                    as_of_date: str) -> Dict[str, Optional[float]]:
        """Fetch profitability metrics (batched: 1-2 API calls instead of 8)."""
        # If already prefetched, read from DB
        if hasattr(self, '_prefetched') and symbol in self._prefetched:
            cached = self.get_cached_metrics(symbol, as_of_date)
            return {k: cached.get(k) for k in PROFITABILITY_FIELDS}

        ric = self._to_ric(symbol)
        fields_list = list(PROFITABILITY_FIELDS.values())

        # Batch call with SDate
        df = self._fetch_batch([ric], fields_list, {"SDate": as_of_date})
        metrics = self._parse_batch_values(df, ric, PROFITABILITY_FIELDS)

        # Fallback for missing fields without SDate
        missing = [k for k, v in metrics.items() if v is None]
        if missing:
            missing_fields = {k: PROFITABILITY_FIELDS[k] for k in missing}
            df2 = self._fetch_batch([ric], list(missing_fields.values()))
            fallback = self._parse_batch_values(df2, ric, missing_fields)
            for k, v in fallback.items():
                if v is not None:
                    metrics[k] = v

        for metric_name, val in metrics.items():
            self._save_metric(symbol, as_of_date, metric_name, val)

        return metrics

    # -------------------------------------------------------------------
    # Growth metrics (computed from estimate changes)
    # -------------------------------------------------------------------

    def compute_growth_metrics(self, symbol: str,
                               as_of_date: str) -> Dict[str, Optional[float]]:
        """
        Compute growth metrics from LSEG estimate data (batched: 3 API calls instead of 10).

        Since pre-computed growth fields are not available, we derive growth from:
        1. FY1 vs FY2 EPS consensus -> forward EPS growth
        2. EPSMean now vs 1 year ago -> trailing EPS estimate growth
        3. RevenueMean now vs 1 year ago -> revenue estimate growth
        4. EBITDAMean now vs 1 year ago -> EBITDA estimate growth
        5. FY1 vs FY2 Revenue consensus -> forward revenue growth
        """
        ric = self._to_ric(symbol)
        as_of = datetime.strptime(as_of_date, "%Y-%m-%d")
        one_year_ago = (as_of - timedelta(days=365)).strftime("%Y-%m-%d")

        metrics = {}

        # --- Call 1: FY1 fields at today's SDate (EPS, Revenue, EBITDA) ---
        fy1_fields = {"eps_fy1": "TR.EPSMean", "rev_fy1": "TR.RevenueMean",
                      "ebitda_fy1": "TR.EBITDAMean"}
        df1 = self._fetch_batch([ric], list(fy1_fields.values()),
                                {"Period": "FY1", "SDate": as_of_date})
        fy1_vals = self._parse_batch_values(df1, ric, fy1_fields)
        eps_fy1 = fy1_vals.get("eps_fy1")
        rev_now = fy1_vals.get("rev_fy1")
        ebitda_now = fy1_vals.get("ebitda_fy1")

        # --- Call 2: FY2 fields at today's SDate (EPS, Revenue) ---
        fy2_fields = {"eps_fy2": "TR.EPSMean", "rev_fy2": "TR.RevenueMean"}
        df2 = self._fetch_batch([ric], list(fy2_fields.values()),
                                {"Period": "FY2", "SDate": as_of_date})
        fy2_vals = self._parse_batch_values(df2, ric, fy2_fields)
        eps_fy2 = fy2_vals.get("eps_fy2")
        rev_fy2 = fy2_vals.get("rev_fy2")

        # --- Call 3: FY1 fields at 1 year ago SDate (EPS, Revenue, EBITDA) ---
        hist_fields = {"eps_1y": "TR.EPSMean", "rev_1y": "TR.RevenueMean",
                       "ebitda_1y": "TR.EBITDAMean"}
        df3 = self._fetch_batch([ric], list(hist_fields.values()),
                                {"Period": "FY1", "SDate": one_year_ago})
        hist_vals = self._parse_batch_values(df3, ric, hist_fields)
        eps_1y_ago = hist_vals.get("eps_1y")
        rev_1y_ago = hist_vals.get("rev_1y")
        ebitda_1y_ago = hist_vals.get("ebitda_1y")

        # --- Compute growth metrics ---
        # Forward EPS: FY2 / FY1 - 1
        if eps_fy1 and eps_fy2 and abs(eps_fy1) > 0.001:
            metrics["eps_growth_yoy"] = ((eps_fy2 / eps_fy1) - 1) * 100
        else:
            metrics["eps_growth_yoy"] = None

        # Trailing EPS: now vs 1 year ago
        if eps_fy1 and eps_1y_ago and abs(eps_1y_ago) > 0.001:
            metrics["eps_growth_3y"] = ((eps_fy1 / eps_1y_ago) - 1) * 100
        else:
            metrics["eps_growth_3y"] = None

        # Revenue trailing: now vs 1 year ago
        if rev_now and rev_1y_ago and abs(rev_1y_ago) > 0.001:
            metrics["revenue_growth_yoy"] = ((rev_now / rev_1y_ago) - 1) * 100
        else:
            metrics["revenue_growth_yoy"] = None

        # Revenue forward: FY2 / FY1
        if rev_now and rev_fy2 and abs(rev_now) > 0.001:
            metrics["revenue_growth_3y"] = ((rev_fy2 / rev_now) - 1) * 100
        else:
            metrics["revenue_growth_3y"] = None

        # EBITDA trailing: now vs 1 year ago
        if ebitda_now and ebitda_1y_ago and abs(ebitda_1y_ago) > 0.001:
            metrics["ebitda_growth_yoy"] = ((ebitda_now / ebitda_1y_ago) - 1) * 100
        else:
            metrics["ebitda_growth_yoy"] = None

        # Save all
        for metric_name, val in metrics.items():
            self._save_metric(symbol, as_of_date, metric_name, val)

        return metrics

    # -------------------------------------------------------------------
    # EPS Revision metrics (consensus changes)
    # -------------------------------------------------------------------

    def compute_revision_metrics(self, symbol: str,
                                 as_of_date: str) -> Dict[str, Optional[float]]:
        """
        Compute EPS revision sub-factor values from consensus changes.

        Uses point-in-time consensus:
          eps_estimate_change_7d:  % change in FY1 EPS mean over 7 days
          eps_estimate_change_30d: % change in FY1 EPS mean over 30 days
          eps_estimate_change_90d: % change in FY1 EPS mean over 90 days
          last_earnings_surprise:  (actual - estimate) / |estimate| * 100
          estimate_dispersion:     -StdDev / |Mean| (inverted, higher=better)
        """
        ric = self._to_ric(symbol)
        as_of = datetime.strptime(as_of_date, "%Y-%m-%d")

        metrics = {}

        # Current FY1 EPS mean
        eps_now = self._fetch_single(ric, "TR.EPSMean",
                                     {"Period": "FY1", "SDate": as_of_date})

        # Historical consensus changes
        for metric_name, days_back in [("eps_estimate_change_7d", 7),
                                        ("eps_estimate_change_30d", 30),
                                        ("eps_estimate_change_90d", 90)]:
            past_date = (as_of - timedelta(days=days_back)).strftime("%Y-%m-%d")
            eps_past = self._fetch_single(ric, "TR.EPSMean",
                                          {"Period": "FY1", "SDate": past_date})
            if eps_now is not None and eps_past is not None and abs(eps_past) > 0.001:
                metrics[metric_name] = ((eps_now - eps_past) / abs(eps_past)) * 100
            else:
                metrics[metric_name] = None

        # Earnings surprise
        eps_actual = self._fetch_single(ric, "TR.EPSActValue")
        eps_estimate = self._fetch_single(ric, "TR.EPSMeanEstimate")
        if eps_actual is not None and eps_estimate is not None and abs(eps_estimate) > 0.001:
            metrics["last_earnings_surprise"] = \
                ((eps_actual - eps_estimate) / abs(eps_estimate)) * 100
        else:
            metrics["last_earnings_surprise"] = None

        # Estimate dispersion (inverted — lower spread = more agreement = better)
        eps_std = self._fetch_single(ric, "TR.EPSStdDev", {"Period": "FY1"})
        if eps_std is not None and eps_now is not None and abs(eps_now) > 0.001:
            metrics["estimate_dispersion"] = -(eps_std / abs(eps_now)) * 100
        else:
            metrics["estimate_dispersion"] = None

        for metric_name, val in metrics.items():
            self._save_metric(symbol, as_of_date, metric_name, val,
                              fiscal_period="FY1")

        return metrics

    # -------------------------------------------------------------------
    # Unified fetch: all metrics for a symbol
    # -------------------------------------------------------------------

    def fetch_all_metrics(self, symbol: str, as_of_date: str,
                          force: bool = False) -> Dict[str, Optional[float]]:
        """
        Fetch all scoring metrics for a symbol from LSEG.
        Returns combined dict of all metric values.
        """
        fetch_key = f"lseg_all:{symbol}:{as_of_date}"
        if not force and self._is_fetched(fetch_key):
            return self.get_cached_metrics(symbol, as_of_date)

        print(f"  {symbol}...", end=" ", flush=True)
        all_metrics = {}

        # Stock info (also populates stock_universe table)
        self.fetch_stock_info(symbol, as_of_date)

        # Value
        val_m = self.fetch_value_metrics(symbol, as_of_date)
        all_metrics.update(val_m)

        # Profitability
        prof_m = self.fetch_profitability_metrics(symbol, as_of_date)
        all_metrics.update(prof_m)

        # Growth (computed from estimates)
        grow_m = self.compute_growth_metrics(symbol, as_of_date)
        all_metrics.update(grow_m)

        # EPS Revisions (computed from consensus changes)
        eps_m = self.compute_revision_metrics(symbol, as_of_date)
        all_metrics.update(eps_m)

        count = sum(1 for v in all_metrics.values() if v is not None)
        print(f"{count} metrics")

        self._mark_fetched(fetch_key, {"metrics_found": count})
        return all_metrics

    def get_cached_metrics(self, symbol: str,
                           as_of_date: str) -> Dict[str, Optional[float]]:
        """Get previously cached metrics from DB."""
        conn = db_schema.get_connection(self.db_path)
        rows = conn.execute(
            """SELECT metric_name, metric_value FROM fundamental_data
               WHERE symbol = ? AND as_of_date = ?""",
            (symbol, as_of_date),
        ).fetchall()
        conn.close()
        return {row["metric_name"]: row["metric_value"] for row in rows}

    # -------------------------------------------------------------------
    # Batch operations
    # -------------------------------------------------------------------

    def _bulk_prefetch_fundamentals(self, symbols: List[str],
                                     as_of_date: str) -> int:
        """
        Bulk-fetch value + profitability metrics for all symbols in chunks of 50.
        Stores results directly to fundamental_data table.

        Returns count of symbols with at least one metric found.
        """
        all_fields = {}
        all_fields.update(VALUE_FIELDS)
        all_fields.update(PROFITABILITY_FIELDS)
        field_names = list(all_fields.values())

        log.info("Bulk prefetching value+profitability for %d symbols (%d fields per chunk)...",
                 len(symbols), len(field_names))

        prefetched = 0
        ric_to_sym = {}

        for chunk_start in range(0, len(symbols), 50):
            chunk_syms = symbols[chunk_start:chunk_start + 50]
            chunk_rics = []
            for sym in chunk_syms:
                ric = self._to_ric(sym)
                chunk_rics.append(ric)
                ric_to_sym[ric] = sym

            df = self._fetch_batch(chunk_rics, field_names, {"SDate": as_of_date})

            if df is None or df.empty:
                log.warning("Bulk prefetch chunk %d-%d returned no data",
                            chunk_start, chunk_start + len(chunk_syms))
                continue

            inst_col = df.columns[0]

            for _, row in df.iterrows():
                ric = str(row[inst_col]).strip()
                sym = ric_to_sym.get(ric)
                if not sym:
                    continue

                found = 0
                for metric_name, field in all_fields.items():
                    val = None
                    # Try matching by field name
                    for col in df.columns[1:]:
                        field_key = field.split(".")[-1].lower()
                        if field_key in col.lower():
                            raw = row[col]
                            if raw is not None and str(raw) not in ("", "nan", "<NA>"):
                                try:
                                    val = float(raw)
                                except (ValueError, TypeError):
                                    pass
                            break

                    # Fallback: positional
                    if val is None:
                        keys = list(all_fields.keys())
                        idx = keys.index(metric_name) + 1
                        if idx < len(row):
                            raw = row.iloc[idx]
                            if raw is not None and str(raw) not in ("", "nan", "<NA>"):
                                try:
                                    val = float(raw)
                                except (ValueError, TypeError):
                                    pass

                    if val is not None:
                        self._save_metric(sym, as_of_date, metric_name, val)
                        found += 1

                if found > 0:
                    self._prefetched.add(sym)
                    prefetched += 1

            log.info("  Prefetch chunk %d-%d: %d/%d symbols with data",
                     chunk_start + 1, chunk_start + len(chunk_syms),
                     sum(1 for s in chunk_syms if s in self._prefetched),
                     len(chunk_syms))
            time.sleep(0.3)  # throttle between chunks

        log.info("Bulk prefetch complete: %d/%d symbols", prefetched, len(symbols))
        return prefetched

    def fetch_universe_batch(self, symbols: List[str], as_of_date: str,
                             force: bool = False) -> int:
        """Fetch all metrics for a list of symbols. Returns count fetched."""
        if not self.is_available():
            print("  LSEG not available")
            return 0

        total = len(symbols)
        print(f"  Fetching LSEG data for {total} symbols...")
        t_start = time.time()

        # Step 1: Resolve RICs (test .O vs .N for unknown symbols)
        self._resolve_rics_batch(symbols)

        # Step 2: Clear stale fetch_log entries for previously failed symbols
        if not force:
            cleared = self._clear_stale_fetches(symbols, as_of_date)
            if cleared:
                print(f"  Cleared {cleared} stale cache entries (0-metric symbols)")

        # Step 3: Bulk prefetch value + profitability for all symbols
        self._prefetched.clear()
        self._bulk_prefetch_fundamentals(symbols, as_of_date)
        print(f"  Bulk prefetched value/profitability for {len(self._prefetched)}/{total} symbols")

        # Step 4: Per-symbol fetch (growth, revisions, stock info + any missed value/prof)
        fetched = 0
        errors = 0

        for i, symbol in enumerate(symbols, 1):
            try:
                self.fetch_all_metrics(symbol, as_of_date, force=force)
                fetched += 1
            except Exception as e:
                errors += 1
                log.warning("Error fetching %s: %s", symbol, e)
                print(f"  Error on {symbol}: {e}")

            # Throttle between symbols
            time.sleep(0.3)

            if i % 25 == 0:
                elapsed = time.time() - t_start
                print(f"  Progress: {i}/{total} symbols ({fetched} ok, {errors} errors) "
                      f"[{elapsed:.0f}s elapsed]")

        elapsed = time.time() - t_start
        print(f"  Done: {fetched}/{total} symbols ({errors} errors) in {elapsed:.0f}s")
        return fetched

    def fetch_info_batch(self, symbols: List[str],
                         as_of_date: str) -> int:
        """Fetch just stock info for universe building (fast)."""
        if not self.is_available():
            return 0

        # Try batch fetch first (much faster)
        rics = [self._to_ric(s) for s in symbols]
        fields = list(INFO_FIELDS.values())

        # Process in chunks of 50
        fetched = 0
        for chunk_start in range(0, len(rics), 50):
            chunk_rics = rics[chunk_start:chunk_start + 50]
            chunk_syms = symbols[chunk_start:chunk_start + 50]

            df = self._fetch_batch(chunk_rics, fields)
            if df is not None and not df.empty:
                for idx, row in df.iterrows():
                    sym = chunk_syms[idx] if idx < len(chunk_syms) else None
                    if sym is None:
                        continue

                    info = {}
                    for col_idx, (metric_name, _) in enumerate(INFO_FIELDS.items()):
                        col_pos = col_idx + 1
                        if col_pos < len(row):
                            val = row.iloc[col_pos]
                            if val is not None and str(val) not in ("", "nan", "<NA>"):
                                info[metric_name] = val

                    if info:
                        self._save_stock_info(sym, as_of_date, info)
                        fetched += 1
            else:
                # Fallback to individual fetches
                for sym in chunk_syms:
                    try:
                        self.fetch_stock_info(sym, as_of_date)
                        fetched += 1
                    except Exception as e:
                        log.warning("fetch_stock_info fallback failed for %s: %s", sym, e)

        return fetched
