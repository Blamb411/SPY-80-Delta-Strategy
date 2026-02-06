"""
Damodaran Historical Data Fetcher
=================================
Downloads and caches 10 years of historical P/E and margin data from
Damodaran's NYU Stern archives for CAPE proxy and margin mean-reversion analysis.

Data source: https://pages.stern.nyu.edu/~adamodar/pc/archives/

Usage:
    # Build/update the historical database (run periodically)
    python damodaran_historical.py

    # Use in code
    from damodaran_historical import get_sector_historical_pe, get_sector_historical_margins
"""

import os
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, Optional, Tuple
import warnings

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

_this_dir = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(_this_dir, "historical_data.db")

# Years to fetch (10 years of history)
HISTORICAL_YEARS = list(range(2016, 2026))  # 2016-2025

# Archive URL patterns
PE_ARCHIVE_URL = "https://pages.stern.nyu.edu/~adamodar/pc/archives/pedata{yy:02d}.xls"
MARGIN_ARCHIVE_URL = "https://pages.stern.nyu.edu/~adamodar/pc/archives/margin{yy:02d}.xls"

# Current data URLs (for the most recent year)
PE_CURRENT_URL = "https://pages.stern.nyu.edu/~adamodar/pc/datasets/pedata.xls"
MARGIN_CURRENT_URL = "https://pages.stern.nyu.edu/~adamodar/pc/datasets/margin.xls"

# Import sector mapping from main fetcher
try:
    from damodaran_fetcher import INDUSTRY_TO_SECTOR, _map_sector
except ImportError:
    # Fallback if run standalone - define minimal mapping
    INDUSTRY_TO_SECTOR = {}
    def _map_sector(industry_name):
        return "Other"


def _safe_float(val):
    """Convert value to float, returning NaN for non-numeric values."""
    if val is None:
        return np.nan
    try:
        v = float(val)
        if abs(v) > 1e10:
            return np.nan
        return v
    except (ValueError, TypeError):
        return np.nan


def _init_database():
    """Initialize SQLite database with required tables."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS historical_pe (
            year INTEGER,
            sector TEXT,
            forward_pe REAL,
            trailing_pe REAL,
            peg_ratio REAL,
            expected_growth REAL,
            num_firms INTEGER,
            PRIMARY KEY (year, sector)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS historical_margins (
            year INTEGER,
            sector TEXT,
            gross_margin REAL,
            net_margin REAL,
            operating_margin REAL,
            ebitda_margin REAL,
            num_firms INTEGER,
            PRIMARY KEY (year, sector)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fetch_log (
            year INTEGER,
            data_type TEXT,
            fetch_date TEXT,
            success INTEGER,
            error_msg TEXT,
            PRIMARY KEY (year, data_type)
        )
    """)

    conn.commit()
    conn.close()


def fetch_archived_pe_data(year: int) -> Optional[pd.DataFrame]:
    """
    Fetch P/E data from Damodaran archive for a specific year.

    Args:
        year: Full year (e.g., 2020)

    Returns:
        DataFrame with industry-level P/E data, or None if fetch fails
    """
    yy = year % 100

    # Try archive URL first, then current URL for most recent year
    urls_to_try = [PE_ARCHIVE_URL.format(yy=yy)]
    if year >= 2025:
        urls_to_try.append(PE_CURRENT_URL)

    for url in urls_to_try:
        try:
            try:
                df = pd.read_excel(url, sheet_name="Industry Averages",
                                   header=7, engine="xlrd")
            except Exception:
                df = pd.read_excel(url, sheet_name=1, header=7)

            name_col = df.columns[0]
            df = df.dropna(subset=[name_col])
            df = df[~df[name_col].astype(str).str.contains("Total Market", case=False, na=False)]
            df[name_col] = df[name_col].astype(str).str.strip()

            result = pd.DataFrame()
            result["industry"] = df[name_col]
            result["num_firms"] = df.iloc[:, 1].apply(_safe_float)
            result["trailing_pe"] = df.iloc[:, 4].apply(_safe_float)
            result["forward_pe"] = df.iloc[:, 5].apply(_safe_float)
            result["expected_growth"] = df.iloc[:, -2].apply(_safe_float)
            result["peg_ratio"] = df.iloc[:, -1].apply(_safe_float)

            # Map to sectors
            result["sector"] = result["industry"].apply(_map_sector)

            return result

        except Exception as e:
            continue

    return None


def fetch_archived_margin_data(year: int) -> Optional[pd.DataFrame]:
    """
    Fetch margin data from Damodaran archive for a specific year.

    Args:
        year: Full year (e.g., 2020)

    Returns:
        DataFrame with industry-level margin data, or None if fetch fails
    """
    yy = year % 100

    urls_to_try = [MARGIN_ARCHIVE_URL.format(yy=yy)]
    if year >= 2025:
        urls_to_try.append(MARGIN_CURRENT_URL)

    for url in urls_to_try:
        try:
            try:
                df = pd.read_excel(url, sheet_name="Industry Averages",
                                   header=None, engine="xlrd")
            except Exception:
                df = pd.read_excel(url, sheet_name=1, header=None)

            # Row 8 typically has column names, data starts at row 9
            data_start = 9
            df_data = df.iloc[data_start:].copy()
            df_data = df_data.dropna(subset=[0])
            df_data = df_data[~df_data[0].astype(str).str.contains(
                "Total Market|Variable|Industry Name", case=False, na=False)]

            result = pd.DataFrame()
            result["industry"] = df_data[0].astype(str).str.strip().values
            result["num_firms"] = df_data.iloc[:, 1].apply(_safe_float).values
            result["gross_margin"] = df_data.iloc[:, 2].apply(_safe_float).values
            result["net_margin"] = df_data.iloc[:, 3].apply(_safe_float).values

            # Operating margin position varies; try column 5
            if df_data.shape[1] > 5:
                result["operating_margin"] = df_data.iloc[:, 5].apply(_safe_float).values

            # EBITDA margin typically at column 11
            if df_data.shape[1] > 11:
                result["ebitda_margin"] = df_data.iloc[:, 11].apply(_safe_float).values

            # Map to sectors
            result["sector"] = result["industry"].apply(_map_sector)

            return result

        except Exception as e:
            continue

    return None


def _aggregate_to_sectors(industry_df: pd.DataFrame, value_cols: list) -> pd.DataFrame:
    """Aggregate industry data to sector level using firm-weighted averages."""
    df = industry_df.copy()
    df = df[df["sector"] != "Other"]

    sector_data = []
    for sector in sorted(df["sector"].unique()):
        sector_rows = df[df["sector"] == sector]
        row = {"sector": sector}
        row["num_firms"] = int(sector_rows["num_firms"].sum()) if "num_firms" in sector_rows.columns else 0

        weights = sector_rows["num_firms"].fillna(1) if "num_firms" in sector_rows.columns else pd.Series(1, index=sector_rows.index)
        total_weight = weights.sum()

        for col in value_cols:
            if col not in sector_rows.columns:
                row[col] = np.nan
                continue
            vals = sector_rows[col]
            valid_mask = vals.notna() & (vals.abs() < 1000)
            if valid_mask.any():
                w = weights[valid_mask]
                v = vals[valid_mask]
                if w.sum() > 0:
                    row[col] = (v * w).sum() / w.sum()
                else:
                    row[col] = v.mean()
            else:
                row[col] = np.nan

        sector_data.append(row)

    return pd.DataFrame(sector_data)


def _store_pe_data(year: int, sector_df: pd.DataFrame, conn: sqlite3.Connection):
    """Store sector P/E data in database."""
    cursor = conn.cursor()

    for _, row in sector_df.iterrows():
        cursor.execute("""
            INSERT OR REPLACE INTO historical_pe
            (year, sector, forward_pe, trailing_pe, peg_ratio, expected_growth, num_firms)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            year,
            row["sector"],
            row.get("forward_pe"),
            row.get("trailing_pe"),
            row.get("peg_ratio"),
            row.get("expected_growth"),
            row.get("num_firms", 0)
        ))

    conn.commit()


def _store_margin_data(year: int, sector_df: pd.DataFrame, conn: sqlite3.Connection):
    """Store sector margin data in database."""
    cursor = conn.cursor()

    for _, row in sector_df.iterrows():
        cursor.execute("""
            INSERT OR REPLACE INTO historical_margins
            (year, sector, gross_margin, net_margin, operating_margin, ebitda_margin, num_firms)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            year,
            row["sector"],
            row.get("gross_margin"),
            row.get("net_margin"),
            row.get("operating_margin"),
            row.get("ebitda_margin"),
            row.get("num_firms", 0)
        ))

    conn.commit()


def _log_fetch(year: int, data_type: str, success: bool, error_msg: str = None):
    """Log fetch attempt to database."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        INSERT OR REPLACE INTO fetch_log (year, data_type, fetch_date, success, error_msg)
        VALUES (?, ?, ?, ?, ?)
    """, (year, data_type, datetime.now().isoformat(), 1 if success else 0, error_msg))

    conn.commit()
    conn.close()


def build_historical_database(force_refresh: bool = False) -> Tuple[int, int]:
    """
    Download 2016-2025 data and cache in SQLite.

    Args:
        force_refresh: If True, re-download all data even if cached

    Returns:
        Tuple of (pe_years_fetched, margin_years_fetched)
    """
    _init_database()
    conn = sqlite3.connect(DB_PATH)

    pe_success = 0
    margin_success = 0

    print("Building historical Damodaran database...")
    print(f"Database location: {DB_PATH}")
    print()

    for year in HISTORICAL_YEARS:
        # Check if already fetched (unless force refresh)
        if not force_refresh:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM historical_pe WHERE year = ?", (year,))
            if cursor.fetchone()[0] > 0:
                print(f"  {year}: Already cached (skipping)")
                pe_success += 1
                margin_success += 1
                continue

        print(f"  {year}: Fetching...", end=" ", flush=True)

        # Fetch P/E data
        pe_df = fetch_archived_pe_data(year)
        if pe_df is not None and len(pe_df) > 0:
            sector_pe = _aggregate_to_sectors(pe_df, ["forward_pe", "trailing_pe", "peg_ratio", "expected_growth"])
            _store_pe_data(year, sector_pe, conn)
            _log_fetch(year, "pe", True)
            pe_success += 1
            print("P/E OK", end=" ", flush=True)
        else:
            _log_fetch(year, "pe", False, "Failed to fetch or empty data")
            print("P/E FAILED", end=" ", flush=True)

        # Fetch margin data
        margin_df = fetch_archived_margin_data(year)
        if margin_df is not None and len(margin_df) > 0:
            sector_margin = _aggregate_to_sectors(margin_df, ["gross_margin", "net_margin", "operating_margin", "ebitda_margin"])
            _store_margin_data(year, sector_margin, conn)
            _log_fetch(year, "margin", True)
            margin_success += 1
            print("Margins OK")
        else:
            _log_fetch(year, "margin", False, "Failed to fetch or empty data")
            print("Margins FAILED")

    conn.close()

    print()
    print(f"Summary: P/E data for {pe_success}/{len(HISTORICAL_YEARS)} years, "
          f"Margin data for {margin_success}/{len(HISTORICAL_YEARS)} years")

    return pe_success, margin_success


def get_sector_historical_pe(sector: str) -> Dict:
    """
    Return historical P/E statistics for a sector over 10 years.

    Args:
        sector: GICS sector name (e.g., "Information Technology")

    Returns:
        Dict with keys: mean, std, min, max, count, years (list of annual values)
    """
    if not os.path.exists(DB_PATH):
        return {"mean": np.nan, "std": np.nan, "min": np.nan, "max": np.nan,
                "count": 0, "years": {}}

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT year, forward_pe FROM historical_pe
        WHERE sector = ? AND forward_pe IS NOT NULL
        ORDER BY year
    """, (sector,))

    rows = cursor.fetchall()
    conn.close()

    if not rows:
        return {"mean": np.nan, "std": np.nan, "min": np.nan, "max": np.nan,
                "count": 0, "years": {}}

    years_dict = {row[0]: row[1] for row in rows}
    values = [row[1] for row in rows if row[1] is not None and not np.isnan(row[1])]

    if not values:
        return {"mean": np.nan, "std": np.nan, "min": np.nan, "max": np.nan,
                "count": 0, "years": years_dict}

    return {
        "mean": np.mean(values),
        "std": np.std(values, ddof=1) if len(values) > 1 else 0.0,
        "min": np.min(values),
        "max": np.max(values),
        "count": len(values),
        "years": years_dict
    }


def get_sector_historical_margins(sector: str) -> Dict:
    """
    Return historical margin statistics for a sector over 10 years.

    Args:
        sector: GICS sector name (e.g., "Information Technology")

    Returns:
        Dict with keys for net_margin: mean, std, min, max, count, years
        Note: Values are converted to percentage format (12.6% stored as 12.6)
    """
    if not os.path.exists(DB_PATH):
        return {"mean": np.nan, "std": np.nan, "min": np.nan, "max": np.nan,
                "count": 0, "years": {}}

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT year, net_margin FROM historical_margins
        WHERE sector = ? AND net_margin IS NOT NULL
        ORDER BY year
    """, (sector,))

    rows = cursor.fetchall()
    conn.close()

    if not rows:
        return {"mean": np.nan, "std": np.nan, "min": np.nan, "max": np.nan,
                "count": 0, "years": {}}

    # Convert from decimal to percentage (0.126 -> 12.6)
    years_dict = {row[0]: row[1] * 100 for row in rows}
    values = [row[1] * 100 for row in rows if row[1] is not None and not np.isnan(row[1])]

    if not values:
        return {"mean": np.nan, "std": np.nan, "min": np.nan, "max": np.nan,
                "count": 0, "years": years_dict}

    return {
        "mean": np.mean(values),
        "std": np.std(values, ddof=1) if len(values) > 1 else 0.0,
        "min": np.min(values),
        "max": np.max(values),
        "count": len(values),
        "years": years_dict
    }


def get_all_historical_data() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Get all historical P/E and margin data as DataFrames.

    Returns:
        Tuple of (pe_df, margin_df)
    """
    if not os.path.exists(DB_PATH):
        return pd.DataFrame(), pd.DataFrame()

    conn = sqlite3.connect(DB_PATH)

    pe_df = pd.read_sql_query("SELECT * FROM historical_pe ORDER BY year, sector", conn)
    margin_df = pd.read_sql_query("SELECT * FROM historical_margins ORDER BY year, sector", conn)

    conn.close()

    return pe_df, margin_df


def print_historical_summary():
    """Print a summary of cached historical data."""
    pe_df, margin_df = get_all_historical_data()

    if pe_df.empty:
        print("No historical data cached. Run build_historical_database() first.")
        return

    print("\n" + "=" * 80)
    print("HISTORICAL DATA SUMMARY")
    print("=" * 80)

    print(f"\nP/E Data: {len(pe_df)} records")
    print(f"  Years: {pe_df['year'].min()} - {pe_df['year'].max()}")
    print(f"  Sectors: {pe_df['sector'].nunique()}")

    print(f"\nMargin Data: {len(margin_df)} records")
    print(f"  Years: {margin_df['year'].min()} - {margin_df['year'].max()}")
    print(f"  Sectors: {margin_df['sector'].nunique()}")

    # Sample: 10-year stats for a few sectors
    print("\n" + "-" * 80)
    print("SAMPLE: 10-YEAR FORWARD P/E STATISTICS")
    print("-" * 80)

    sample_sectors = ["Information Technology", "Financials", "Health Care", "Energy"]
    print(f"\n  {'Sector':<25} {'Mean':>8} {'Std':>8} {'Min':>8} {'Max':>8} {'N':>4}")
    print(f"  {'-' * 60}")

    for sector in sample_sectors:
        stats = get_sector_historical_pe(sector)
        if stats["count"] > 0:
            print(f"  {sector:<25} {stats['mean']:>8.1f} {stats['std']:>8.1f} "
                  f"{stats['min']:>8.1f} {stats['max']:>8.1f} {stats['count']:>4}")


if __name__ == "__main__":
    # Build/update the historical database
    pe_years, margin_years = build_historical_database(force_refresh=False)

    # Print summary
    print_historical_summary()
