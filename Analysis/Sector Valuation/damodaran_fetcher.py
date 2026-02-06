"""
Damodaran Sector Data Fetcher
==============================
Downloads and parses Aswath Damodaran's industry datasets from NYU Stern.
Aggregates ~96 industries into 11 GICS sectors for valuation analysis.

Data source: https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datacurrent.html
Updated annually (typically January).

Usage:
    from damodaran_fetcher import fetch_all_damodaran_data
    sectors, industries = fetch_all_damodaran_data()
"""

import pandas as pd
import numpy as np
import warnings

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# ---------------------------------------------------------------------------
# Damodaran dataset URLs (US data)
# ---------------------------------------------------------------------------
DATASETS = {
    "pe": "https://pages.stern.nyu.edu/~adamodar/pc/datasets/pedata.xls",
    "pbv": "https://pages.stern.nyu.edu/~adamodar/pc/datasets/pbvdata.xls",
    "vebitda": "https://pages.stern.nyu.edu/~adamodar/pc/datasets/vebitda.xls",
    "ps": "https://pages.stern.nyu.edu/~adamodar/pc/datasets/psdata.xls",
    "margin": "https://pages.stern.nyu.edu/~adamodar/pc/datasets/margin.xls",
    "roe": "https://pages.stern.nyu.edu/~adamodar/pc/datasets/roe.xls",
    "fundgr": "https://pages.stern.nyu.edu/~adamodar/pc/datasets/fundgr.xls",
}

# ---------------------------------------------------------------------------
# Industry -> GICS Sector Mapping
# ---------------------------------------------------------------------------
# Maps Damodaran's ~96 industry names to 11 GICS sectors.
# Industries not matched fall into "Other".

INDUSTRY_TO_SECTOR = {
    # Information Technology
    "Computer Services": "Information Technology",
    "Electronics (Consumer & Office)": "Information Technology",
    "Electronics (General)": "Information Technology",
    "Information Services": "Information Technology",
    "Semiconductor": "Information Technology",
    "Semiconductor Equip": "Information Technology",
    "Software (Entertainment)": "Information Technology",
    "Software (Internet)": "Information Technology",
    "Software (System & Application)": "Information Technology",
    "Computer/Peripherals": "Information Technology",
    "Electrical Equipment": "Information Technology",

    # Health Care
    "Biotechnology": "Health Care",
    "Healthcare Products": "Health Care",
    "Healthcare Support Services": "Health Care",
    "Healthcare Information and Technology": "Health Care",
    "Hospitals/Healthcare Facilities": "Health Care",
    "Pharma & Drugs": "Health Care",

    # Financials
    "Bank (Money Center)": "Financials",
    "Banks (Regional)": "Financials",
    "Brokerage & Investment Banking": "Financials",
    "Financial Svcs. (Non-bank & Insurance)": "Financials",
    "Insurance (General)": "Financials",
    "Insurance (Life)": "Financials",
    "Insurance (Prop/Cas.)": "Financials",
    "Investments & Asset Management": "Financials",
    "Reinsurance": "Financials",
    "Thrift": "Financials",

    # Consumer Discretionary
    "Advertising": "Consumer Discretionary",
    "Apparel": "Consumer Discretionary",
    "Auto & Truck": "Consumer Discretionary",
    "Auto Parts": "Consumer Discretionary",
    "Cable TV": "Consumer Discretionary",
    "Entertainment": "Consumer Discretionary",
    "Furnishings": "Consumer Discretionary",
    "Homebuilding": "Consumer Discretionary",
    "Hotel/Gaming": "Consumer Discretionary",
    "Household Products": "Consumer Discretionary",
    "Publishing & Newspapers": "Consumer Discretionary",
    "Recreation": "Consumer Discretionary",
    "Restaurant/Dining": "Consumer Discretionary",
    "Retail (Automotive)": "Consumer Discretionary",
    "Retail (Building Supply)": "Consumer Discretionary",
    "Retail (Distributors)": "Consumer Discretionary",
    "Retail (General)": "Consumer Discretionary",
    "Retail (Online)": "Consumer Discretionary",
    "Retail (Special Lines)": "Consumer Discretionary",
    "Shoe": "Consumer Discretionary",

    # Communication Services
    "Broadcasting": "Communication Services",
    "Telecom (Wireless)": "Communication Services",
    "Telecom. Equipment": "Communication Services",
    "Telecom. Services": "Communication Services",

    # Industrials
    "Aerospace/Defense": "Industrials",
    "Air Transport": "Industrials",
    "Building Materials": "Industrials",
    "Business & Consumer Services": "Industrials",
    "Construction Supplies": "Industrials",
    "Diversified": "Industrials",
    "Engineering/Construction": "Industrials",
    "Environmental & Waste Services": "Industrials",
    "Industrial Services": "Industrials",
    "Machinery": "Industrials",
    "Office Equipment & Services": "Industrials",
    "Packaging & Container": "Industrials",
    "Shipbuilding & Marine": "Industrials",
    "Transportation": "Industrials",
    "Transportation (Railroads)": "Industrials",
    "Trucking": "Industrials",

    # Consumer Staples
    "Beverage (Alcoholic)": "Consumer Staples",
    "Beverage (Soft)": "Consumer Staples",
    "Education": "Consumer Staples",
    "Food Processing": "Consumer Staples",
    "Food Wholesalers": "Consumer Staples",
    "Retail (Grocery and Food)": "Consumer Staples",
    "Tobacco": "Consumer Staples",

    # Energy
    "Coal & Related Energy": "Energy",
    "Oil/Gas (Integrated)": "Energy",
    "Oil/Gas (Production and Exploration)": "Energy",
    "Oil/Gas Distribution": "Energy",
    "Oilfield Svcs/Equip.": "Energy",

    # Utilities
    "Power": "Utilities",
    "Utility (General)": "Utilities",
    "Utility (Water)": "Utilities",
    "Green & Renewable Energy": "Utilities",

    # Real Estate
    "R.E.I.T.": "Real Estate",
    "Real Estate (Development)": "Real Estate",
    "Real Estate (General/Diversified)": "Real Estate",
    "Real Estate (Operations & Services)": "Real Estate",

    # Materials
    "Chemical (Basic)": "Materials",
    "Chemical (Diversified)": "Materials",
    "Chemical (Specialty)": "Materials",
    "Metals & Mining": "Materials",
    "Paper/Forest Products": "Materials",
    "Precious Metals": "Materials",
    "Rubber& Tires": "Materials",
    "Steel": "Materials",

    # Often classified differently
    "Drugs (Pharmaceutical)": "Health Care",
    "Farming/Agriculture": "Consumer Staples",
    "Oil/Gas (Midstream)": "Energy",
}


def _safe_float(val):
    """Convert value to float, returning NaN for non-numeric values."""
    if val is None:
        return np.nan
    try:
        v = float(val)
        if abs(v) > 1e10:  # likely garbage
            return np.nan
        return v
    except (ValueError, TypeError):
        return np.nan


def _read_damodaran_sheet(url, header_row=7):
    """
    Read a Damodaran Excel file, using the specified row as header.
    Returns DataFrame with industry data.
    """
    try:
        df = pd.read_excel(url, sheet_name="Industry Averages",
                           header=header_row, engine="xlrd")
    except Exception:
        df = pd.read_excel(url, sheet_name=1, header=header_row)

    # Drop rows where industry name is NaN (footer/spacer rows)
    name_col = df.columns[0]
    df = df.dropna(subset=[name_col])

    # Drop the "Total Market" summary row if present
    df = df[~df[name_col].astype(str).str.contains("Total Market", case=False, na=False)]

    # Strip whitespace from industry names
    df[name_col] = df[name_col].astype(str).str.strip()

    # Drop columns that are entirely NaN (spacer columns)
    df = df.dropna(axis=1, how="all")

    return df


def fetch_pe_data():
    """Fetch P/E, Forward P/E, PEG, and expected growth by industry."""
    url = DATASETS["pe"]
    df = _read_damodaran_sheet(url)

    cols = df.columns.tolist()
    name_col = cols[0]

    # Identify columns by position (Damodaran's layout is consistent)
    # Columns: Industry Name, # firms, % Money Losing, Current PE, Trailing PE,
    #          Forward PE, Agg Mkt Cap/Net Income, Agg (profitable only),
    #          Expected Growth 5yr, PEG
    result = pd.DataFrame()
    result["industry"] = df[name_col]
    result["num_firms"] = df.iloc[:, 1].apply(_safe_float)
    result["pct_money_losing"] = df.iloc[:, 2].apply(_safe_float)
    result["current_pe"] = df.iloc[:, 3].apply(_safe_float)
    result["trailing_pe"] = df.iloc[:, 4].apply(_safe_float)
    result["forward_pe"] = df.iloc[:, 5].apply(_safe_float)

    # Expected growth and PEG are typically the last two columns
    result["expected_growth_5yr"] = df.iloc[:, -2].apply(_safe_float)
    result["peg_ratio"] = df.iloc[:, -1].apply(_safe_float)

    return result


def fetch_pbv_data():
    """Fetch Price/Book, ROE, EV/Invested Capital, ROIC by industry."""
    url = DATASETS["pbv"]
    df = _read_damodaran_sheet(url)

    cols = df.columns.tolist()
    name_col = cols[0]

    result = pd.DataFrame()
    result["industry"] = df[name_col]
    result["pbv"] = df.iloc[:, 2].apply(_safe_float)
    result["roe"] = df.iloc[:, 3].apply(_safe_float)
    result["ev_invested_capital"] = df.iloc[:, 4].apply(_safe_float)
    result["roic"] = df.iloc[:, 5].apply(_safe_float)

    return result


def fetch_vebitda_data():
    """Fetch EV/EBITDA and EV/EBIT by industry."""
    url = DATASETS["vebitda"]
    # vebitda has a two-row header (row 7 = group labels, row 8 = column names)
    # Data starts at row 9. Use row 8 as header.
    try:
        df = pd.read_excel(url, sheet_name="Industry Averages",
                           header=None, engine="xlrd")
    except Exception:
        df = pd.read_excel(url, sheet_name=1, header=None)

    # Row 8 has the actual column names: Industry Name, Number of firms,
    # EV/EBITDAR&D, EV/EBITDA, EV/EBIT, EV/EBIT(1-t) [positive only],
    # then same 4 for "All firms"
    # Use column positions directly since column names repeat
    data_start = 9  # first data row
    df_data = df.iloc[data_start:].copy()
    df_data = df_data.dropna(subset=[0])  # drop rows without industry name
    df_data = df_data[~df_data[0].astype(str).str.contains("Total Market", case=False, na=False)]

    result = pd.DataFrame()
    result["industry"] = df_data[0].astype(str).str.strip().values
    # Column 3 = EV/EBITDA (positive EBITDA firms only)
    result["ev_ebitda"] = df_data.iloc[:, 3].apply(_safe_float).values
    # Column 4 = EV/EBIT (positive EBITDA firms only)
    result["ev_ebit"] = df_data.iloc[:, 4].apply(_safe_float).values

    return result


def fetch_ps_data():
    """Fetch Price/Sales, Net Margin, EV/Sales by industry."""
    url = DATASETS["ps"]
    df = _read_damodaran_sheet(url)

    cols = df.columns.tolist()
    name_col = cols[0]

    result = pd.DataFrame()
    result["industry"] = df[name_col]
    result["price_to_sales"] = df.iloc[:, 2].apply(_safe_float)
    result["net_margin_ps"] = df.iloc[:, 3].apply(_safe_float)
    result["ev_to_sales"] = df.iloc[:, 4].apply(_safe_float)
    result["pretax_operating_margin"] = df.iloc[:, 5].apply(_safe_float)

    return result


def fetch_margin_data():
    """Fetch margin data by industry (gross, operating, net, EBITDA)."""
    url = DATASETS["margin"]
    try:
        df = pd.read_excel(url, sheet_name="Industry Averages",
                           header=None, engine="xlrd")
    except Exception:
        df = pd.read_excel(url, sheet_name=1, header=None)

    # Row 7 = group headers, Row 8 = column names, data starts at row 9
    # Columns (by position):
    #   0: Industry Name, 1: # firms, 2: Gross Margin, 3: Net Margin,
    #   4-10: Various operating margins,
    #   11: EBITDA/Sales (EBITDA margin)
    data_start = 9
    df_data = df.iloc[data_start:].copy()
    df_data = df_data.dropna(subset=[0])
    df_data = df_data[~df_data[0].astype(str).str.contains("Total Market|Variable|Industry Name",
                                                             case=False, na=False)]

    result = pd.DataFrame()
    result["industry"] = df_data[0].astype(str).str.strip().values
    result["gross_margin"] = df_data.iloc[:, 2].apply(_safe_float).values
    result["net_margin"] = df_data.iloc[:, 3].apply(_safe_float).values
    # Column 5 = Pre-tax Unadjusted Operating Margin
    result["operating_margin"] = df_data.iloc[:, 5].apply(_safe_float).values
    # Column 11 = EBITDA/Sales
    if df_data.shape[1] > 11:
        result["ebitda_margin"] = df_data.iloc[:, 11].apply(_safe_float).values

    return result


def fetch_roe_data():
    """Fetch ROE by industry."""
    url = DATASETS["roe"]
    df = _read_damodaran_sheet(url)

    cols = df.columns.tolist()
    name_col = cols[0]

    result = pd.DataFrame()
    result["industry"] = df[name_col]
    result["roe_unadj"] = df.iloc[:, 2].apply(_safe_float)
    result["roe_rd_adj"] = df.iloc[:, 3].apply(_safe_float)

    return result


def fetch_fundgr_data():
    """Fetch fundamental growth rates by industry."""
    url = DATASETS["fundgr"]
    df = _read_damodaran_sheet(url)

    cols = df.columns.tolist()
    name_col = cols[0]

    result = pd.DataFrame()
    result["industry"] = df[name_col]
    result["roe_fundgr"] = df.iloc[:, 2].apply(_safe_float)
    result["retention_ratio"] = df.iloc[:, 3].apply(_safe_float)
    result["fundamental_growth"] = df.iloc[:, 4].apply(_safe_float)

    return result


def _map_sector(industry_name):
    """Map a Damodaran industry name to a GICS sector."""
    # Direct match
    if industry_name in INDUSTRY_TO_SECTOR:
        return INDUSTRY_TO_SECTOR[industry_name]

    # Fuzzy match: try partial matching
    industry_lower = industry_name.lower()

    if "software" in industry_lower or "computer" in industry_lower or "semiconductor" in industry_lower:
        return "Information Technology"
    if "pharma" in industry_lower or "drug" in industry_lower or "biotech" in industry_lower or "health" in industry_lower:
        return "Health Care"
    if "bank" in industry_lower or "insurance" in industry_lower or "financial" in industry_lower or "brokerage" in industry_lower:
        return "Financials"
    if "oil" in industry_lower or "coal" in industry_lower or "oilfield" in industry_lower:
        return "Energy"
    if "utility" in industry_lower or "power" in industry_lower:
        return "Utilities"
    if "real estate" in industry_lower or "reit" in industry_lower or "r.e.i.t" in industry_lower:
        return "Real Estate"
    if "chemical" in industry_lower or "metal" in industry_lower or "steel" in industry_lower or "mining" in industry_lower:
        return "Materials"
    if "retail" in industry_lower or "auto" in industry_lower or "hotel" in industry_lower or "restaurant" in industry_lower:
        return "Consumer Discretionary"
    if "food" in industry_lower or "beverage" in industry_lower or "tobacco" in industry_lower:
        return "Consumer Staples"
    if "telecom" in industry_lower or "broadcast" in industry_lower:
        return "Communication Services"
    if "aerospace" in industry_lower or "transport" in industry_lower or "machinery" in industry_lower or "engineer" in industry_lower:
        return "Industrials"

    return "Other"


def merge_industry_data():
    """
    Fetch all Damodaran datasets and merge into a single industry-level DataFrame.
    Returns DataFrame indexed by industry name.
    """
    print("Fetching Damodaran datasets...")

    print("  P/E ratios...")
    pe = fetch_pe_data()

    print("  Price/Book...")
    pbv = fetch_pbv_data()

    print("  EV/EBITDA...")
    ev = fetch_vebitda_data()

    print("  Price/Sales...")
    ps = fetch_ps_data()

    print("  Margins...")
    margins = fetch_margin_data()

    print("  ROE...")
    roe = fetch_roe_data()

    print("  Fundamental Growth...")
    fundgr = fetch_fundgr_data()

    # Merge all on industry name
    merged = pe.copy()

    for df in [pbv, ev, ps, margins, roe, fundgr]:
        # Avoid duplicate columns
        cols_to_add = [c for c in df.columns if c != "industry" and c not in merged.columns]
        if cols_to_add:
            merged = merged.merge(df[["industry"] + cols_to_add],
                                  on="industry", how="left")

    # Add sector mapping
    merged["sector"] = merged["industry"].apply(_map_sector)

    # Convert percentage columns (Damodaran reports some as decimals, some as percentages)
    pct_cols = ["pct_money_losing", "expected_growth_5yr", "roe", "roic",
                "net_margin_ps", "pretax_operating_margin", "gross_margin",
                "net_margin", "ebitda_margin", "roe_unadj", "roe_rd_adj",
                "retention_ratio", "fundamental_growth"]
    for col in pct_cols:
        if col in merged.columns:
            vals = merged[col]
            # If max value > 1 and not already in percentage form, keep as-is
            # If max value <= 1, convert to percentage
            valid = vals.dropna()
            if len(valid) > 0 and valid.abs().max() <= 1.0:
                merged[col] = merged[col] * 100

    print(f"\n  Merged {len(merged)} industries across {merged['sector'].nunique()} sectors")

    return merged


def aggregate_to_sectors(industry_df):
    """
    Aggregate industry-level data to GICS sectors.
    Uses firm-count weighted averages where possible.
    """
    df = industry_df.copy()
    df = df[df["sector"] != "Other"]

    # Metrics to aggregate with weighted average (weighted by num_firms)
    value_cols = ["current_pe", "trailing_pe", "forward_pe", "peg_ratio",
                  "expected_growth_5yr", "pbv", "roe", "roic",
                  "ev_invested_capital", "ev_ebitda", "ev_ebit",
                  "price_to_sales", "ev_to_sales",
                  "gross_margin", "net_margin", "operating_margin",
                  "net_margin_ps", "pretax_operating_margin", "ebitda_margin",
                  "roe_unadj", "roe_rd_adj", "fundamental_growth"]

    # Filter to columns that exist
    value_cols = [c for c in value_cols if c in df.columns]

    sector_data = []

    for sector in sorted(df["sector"].unique()):
        sector_rows = df[df["sector"] == sector]
        row = {"sector": sector}
        row["num_industries"] = len(sector_rows)
        row["num_firms"] = sector_rows["num_firms"].sum()

        weights = sector_rows["num_firms"].fillna(1)
        total_weight = weights.sum()

        for col in value_cols:
            vals = sector_rows[col]
            valid_mask = vals.notna() & (vals.abs() < 1000)  # exclude extreme outliers
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


def fetch_all_damodaran_data():
    """
    Main entry point: fetch all Damodaran data, return sector and industry DataFrames.

    Returns:
        (sector_df, industry_df) tuple
    """
    industry_df = merge_industry_data()
    sector_df = aggregate_to_sectors(industry_df)
    return sector_df, industry_df


if __name__ == "__main__":
    sector_df, industry_df = fetch_all_damodaran_data()

    print("\n" + "=" * 80)
    print("SECTOR SUMMARY")
    print("=" * 80)

    display_cols = ["sector", "num_firms", "forward_pe", "peg_ratio",
                    "expected_growth_5yr", "pbv", "roe"]
    display_cols = [c for c in display_cols if c in sector_df.columns]

    print(f"\n  {'Sector':<25} {'Firms':>6} {'Fwd PE':>8} {'PEG':>8} "
          f"{'Growth':>8} {'P/B':>8} {'ROE':>8}")
    print(f"  {'-' * 75}")

    for _, row in sector_df.sort_values("forward_pe").iterrows():
        fwd_pe = f"{row['forward_pe']:.1f}" if pd.notna(row.get('forward_pe')) else "N/A"
        peg = f"{row['peg_ratio']:.2f}" if pd.notna(row.get('peg_ratio')) else "N/A"
        growth = f"{row['expected_growth_5yr']:.1f}%" if pd.notna(row.get('expected_growth_5yr')) else "N/A"
        pbv = f"{row['pbv']:.1f}" if pd.notna(row.get('pbv')) else "N/A"
        roe_val = f"{row['roe']:.1f}%" if pd.notna(row.get('roe')) else "N/A"

        print(f"  {row['sector']:<25} {row['num_firms']:>6.0f} {fwd_pe:>8} "
              f"{peg:>8} {growth:>8} {pbv:>8} {roe_val:>8}")

    print("\nDone!")
