"""
GuruFocus API Client
=====================
Fetches fundamental data (value, growth, profitability metrics) from
GuruFocus API with SQLite caching to avoid redundant calls.

API Docs: https://www.gurufocus.com/api.php
"""

import os
import sys
import json
import time
import sqlite3
import urllib.request
import urllib.error
from datetime import datetime
from typing import Optional, Dict, List, Any

# Add parent to path for config imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
import db_schema


class GuruFocusClient:
    """Wrapper for GuruFocus API with local caching."""

    def __init__(self, api_key: Optional[str] = None, db_path: str = config.DB_PATH):
        if api_key:
            self.api_key = api_key
        elif os.path.exists(config.GURUFOCUS_API_KEY_FILE):
            self.api_key = config.load_api_key(config.GURUFOCUS_API_KEY_FILE)
        else:
            raise FileNotFoundError(
                f"GuruFocus API key not found at {config.GURUFOCUS_API_KEY_FILE}\n"
                "Create this file with your API key from https://www.gurufocus.com/api.php"
            )
        self.base_url = config.GURUFOCUS_BASE_URL
        self.db_path = db_path
        self.delay = config.GURUFOCUS_DELAY_SECONDS

    def _api_get(self, endpoint: str) -> Optional[Dict]:
        """Make a GET request to GuruFocus API."""
        url = f"{self.base_url}/{self.api_key}/{endpoint}"
        try:
            req = urllib.request.Request(url)
            req.add_header("User-Agent", "QuantScoringModel/1.0")
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode())
            time.sleep(self.delay)
            return data
        except urllib.error.HTTPError as e:
            if e.code == 429:
                print(f"  Rate limited, waiting 60s...")
                time.sleep(60)
                return self._api_get(endpoint)
            print(f"  HTTP Error {e.code} for {endpoint}: {e.reason}")
            return None
        except Exception as e:
            print(f"  Error fetching {endpoint}: {e}")
            return None

    def _is_fetched(self, fetch_key: str) -> bool:
        """Check if data has been fetched already."""
        conn = db_schema.get_connection(self.db_path)
        row = conn.execute(
            "SELECT 1 FROM fetch_log WHERE fetch_key = ?", (fetch_key,)
        ).fetchone()
        conn.close()
        return row is not None

    def _mark_fetched(self, fetch_key: str, details: Optional[Dict] = None) -> None:
        """Record that a fetch was completed."""
        conn = db_schema.get_connection(self.db_path)
        conn.execute(
            "INSERT OR REPLACE INTO fetch_log (fetch_key, last_fetched, details) VALUES (?, ?, ?)",
            (fetch_key, datetime.utcnow().isoformat(),
             json.dumps(details) if details else None),
        )
        conn.commit()
        conn.close()

    def _save_metric(self, symbol: str, as_of_date: str, metric_name: str,
                     value: Optional[float], fiscal_period: str = "TTM") -> None:
        """Save a single metric to fundamental_data."""
        if value is None:
            return
        conn = db_schema.get_connection(self.db_path)
        conn.execute(
            """INSERT OR REPLACE INTO fundamental_data
               (symbol, as_of_date, metric_name, fiscal_period, metric_value, source, fetched_at)
               VALUES (?, ?, ?, ?, ?, 'gurufocus', ?)""",
            (symbol, as_of_date, metric_name, fiscal_period,
             float(value), datetime.utcnow().isoformat()),
        )
        conn.commit()
        conn.close()

    def _extract_value(self, data: Dict, path: str) -> Optional[float]:
        """Extract a numeric value from nested API response using dot-path."""
        keys = path.split(".")
        obj = data
        for key in keys:
            if isinstance(obj, dict) and key in obj:
                obj = obj[key]
            else:
                return None
        try:
            if obj is None or obj == "":
                return None
            return float(obj)
        except (ValueError, TypeError):
            return None

    def fetch_stock_summary(self, symbol: str) -> Optional[Dict]:
        """Fetch stock summary data (profile + key ratios)."""
        return self._api_get(f"stock/{symbol}/summary")

    def fetch_financials(self, symbol: str) -> Optional[Dict]:
        """Fetch financial statement data."""
        return self._api_get(f"stock/{symbol}/financials")

    def fetch_key_ratios(self, symbol: str) -> Optional[Dict]:
        """Fetch key financial ratios."""
        return self._api_get(f"stock/{symbol}/keyratios")

    def fetch_all_metrics(self, symbol: str, as_of_date: str,
                          force: bool = False) -> Dict[str, Optional[float]]:
        """
        Fetch all scoring metrics for a symbol. Returns dict of metric_name -> value.
        Uses cache; set force=True to refetch.
        """
        fetch_key = f"gf_metrics:{symbol}:{as_of_date}"
        if not force and self._is_fetched(fetch_key):
            return self.get_cached_metrics(symbol, as_of_date)

        print(f"  Fetching GuruFocus data for {symbol}...", end=" ", flush=True)

        # Fetch summary which contains most ratios
        summary = self.fetch_stock_summary(symbol)
        if not summary:
            print("no data")
            self._mark_fetched(fetch_key, {"status": "no_data"})
            return {}

        metrics = {}

        # Extract metrics from summary data
        # The exact paths depend on GuruFocus API response structure
        # Summary typically returns: summary, ratios, profitability, growth
        ratio_paths = {
            "pe_ratio":     ["summary.pe", "ratios.PE Ratio"],
            "forward_pe":   ["summary.forwardPE", "ratios.Forward PE Ratio"],
            "pb_ratio":     ["summary.pb", "ratios.PB Ratio"],
            "ps_ratio":     ["summary.ps", "ratios.PS Ratio"],
            "ev_to_ebitda": ["summary.ev2ebitda", "ratios.EV-to-EBITDA"],
            "peg_ratio":    ["summary.peg", "ratios.PEG Ratio"],
        }

        profitability_paths = {
            "gross_margin": ["summary.grossmargin", "profitability.Gross Margin %"],
            "ebit_margin":  ["summary.operatingmargin", "profitability.Operating Margin %"],
            "net_margin":   ["summary.netmargin", "profitability.Net Margin %"],
            "roe":          ["summary.roe", "profitability.ROE %"],
            "roa":          ["summary.roa", "profitability.ROA %"],
            "roic":         ["summary.roic", "profitability.ROIC %"],
        }

        growth_paths = {
            "revenue_growth_yoy": ["summary.revenue_growth", "growth.Revenue Growth (YoY) %"],
            "revenue_growth_3y":  ["growth.3-Year Revenue Growth Rate"],
            "eps_growth_yoy":     ["summary.epsgrowth", "growth.EPS without NRI Growth (YoY) %"],
            "eps_growth_3y":      ["growth.3-Year EPS without NRI Growth Rate"],
            "ebitda_growth_yoy":  ["growth.EBITDA Growth (YoY) %"],
        }

        # Try each path until we find a value
        for metric_name, paths in {**ratio_paths, **profitability_paths, **growth_paths}.items():
            value = None
            for path in paths:
                value = self._extract_value(summary, path)
                if value is not None:
                    break
            metrics[metric_name] = value
            self._save_metric(symbol, as_of_date, metric_name, value)

        # Also extract stock info for universe table
        self._save_stock_info(symbol, as_of_date, summary)

        count = sum(1 for v in metrics.values() if v is not None)
        print(f"{count} metrics")
        self._mark_fetched(fetch_key, {"metrics_found": count})
        return metrics

    def _save_stock_info(self, symbol: str, as_of_date: str, summary: Dict) -> None:
        """Extract and save stock info to stock_universe table."""
        conn = db_schema.get_connection(self.db_path)

        company_name = None
        exchange = None
        sector = None
        industry = None
        market_cap = None
        price = None

        # Try common paths in GuruFocus response
        for name_path in ["summary.company", "general.company"]:
            company_name = company_name or self._extract_str(summary, name_path)
        for exch_path in ["summary.exchange", "general.exchange"]:
            exchange = exchange or self._extract_str(summary, exch_path)
        for sect_path in ["summary.sector", "general.sector"]:
            sector = sector or self._extract_str(summary, sect_path)
        for ind_path in ["summary.industry", "general.industry"]:
            industry = industry or self._extract_str(summary, ind_path)
        for mc_path in ["summary.mktcap", "general.mktcap"]:
            market_cap = market_cap or self._extract_value(summary, mc_path)
        for px_path in ["summary.price", "general.price"]:
            price = price or self._extract_value(summary, px_path)

        # Market cap from GF may be in millions
        if market_cap and market_cap < 100_000:
            market_cap = market_cap * 1_000_000

        is_reit = 1 if (sector and "reit" in sector.lower()) or \
                       (industry and "reit" in industry.lower()) else 0

        passes = 1
        if market_cap and market_cap < config.MIN_MARKET_CAP:
            passes = 0
        if price and price < config.MIN_PRICE:
            passes = 0
        if is_reit and config.EXCLUDE_REITS:
            passes = 0

        conn.execute(
            """INSERT OR REPLACE INTO stock_universe
               (symbol, as_of_date, company_name, exchange, sector, industry,
                market_cap, share_price, is_reit, passes_filter)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (symbol, as_of_date, company_name, exchange, sector, industry,
             market_cap, price, is_reit, passes),
        )
        conn.commit()
        conn.close()

    def _extract_str(self, data: Dict, path: str) -> Optional[str]:
        """Extract a string value from nested dict using dot-path."""
        keys = path.split(".")
        obj = data
        for key in keys:
            if isinstance(obj, dict) and key in obj:
                obj = obj[key]
            else:
                return None
        return str(obj) if obj else None

    def get_cached_metrics(self, symbol: str, as_of_date: str) -> Dict[str, Optional[float]]:
        """Get previously cached metrics from DB."""
        conn = db_schema.get_connection(self.db_path)
        rows = conn.execute(
            """SELECT metric_name, metric_value FROM fundamental_data
               WHERE symbol = ? AND as_of_date = ?""",
            (symbol, as_of_date),
        ).fetchall()
        conn.close()
        return {row["metric_name"]: row["metric_value"] for row in rows}

    def get_cached_stock_info(self, symbol: str, as_of_date: str) -> Optional[Dict]:
        """Get cached stock universe info."""
        conn = db_schema.get_connection(self.db_path)
        row = conn.execute(
            "SELECT * FROM stock_universe WHERE symbol = ? AND as_of_date = ?",
            (symbol, as_of_date),
        ).fetchone()
        conn.close()
        return dict(row) if row else None

    def fetch_universe_batch(self, symbols: List[str], as_of_date: str,
                             force: bool = False) -> int:
        """Fetch metrics for a list of symbols. Returns count fetched."""
        fetched = 0
        total = len(symbols)
        for i, symbol in enumerate(symbols, 1):
            try:
                self.fetch_all_metrics(symbol, as_of_date, force=force)
                fetched += 1
            except Exception as e:
                print(f"  Error on {symbol}: {e}")
            if i % 25 == 0:
                print(f"  Progress: {i}/{total} symbols")
        return fetched
