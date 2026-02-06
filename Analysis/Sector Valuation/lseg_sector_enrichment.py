"""
LSEG Sector Data Enrichment
=============================
Fetches live fundamental data from LSEG Workspace for major stocks
in each GICS sector, then computes market-cap-weighted sector averages.

Provides real-time Forward P/E, PEG, and other metrics as a complement
to the Damodaran industry averages (which are updated annually).

Requires: LSEG Workspace desktop app running with active session.

Usage:
    from lseg_sector_enrichment import fetch_lseg_sector_data
    lseg_sectors = fetch_lseg_sector_data()
"""

import warnings
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=RuntimeWarning)

# ---------------------------------------------------------------------------
# Major stocks per sector (top holdings of sector ETFs by market cap)
# ---------------------------------------------------------------------------
SECTOR_STOCKS = {
    "Information Technology": [
        "AAPL", "MSFT", "NVDA", "AVGO", "ORCL",
        "CRM", "AMD", "ADBE", "CSCO", "ACN",
    ],
    "Health Care": [
        "UNH", "LLY", "JNJ", "ABBV", "MRK",
        "TMO", "ABT", "AMGN", "PFE", "ISRG",
    ],
    "Financials": [
        "BRK.B", "JPM", "V", "MA", "BAC",
        "GS", "MS", "SPGI", "BLK", "AXP",
    ],
    "Consumer Discretionary": [
        "AMZN", "TSLA", "HD", "MCD", "NKE",
        "LOW", "SBUX", "TJX", "BKNG", "CMG",
    ],
    "Communication Services": [
        "META", "GOOGL", "NFLX", "DIS", "TMUS",
        "CMCSA", "VZ", "T", "EA", "CHTR",
    ],
    "Industrials": [
        "GE", "CAT", "RTX", "UNP", "HON",
        "DE", "BA", "LMT", "UPS", "ADP",
    ],
    "Consumer Staples": [
        "WMT", "PG", "COST", "KO", "PEP",
        "PM", "MDLZ", "CL", "MO", "KHC",
    ],
    "Energy": [
        "XOM", "CVX", "COP", "EOG", "SLB",
        "MPC", "PXD", "PSX", "OXY", "VLO",
    ],
    "Utilities": [
        "NEE", "SO", "DUK", "CEG", "SRE",
        "AEP", "D", "EXC", "XEL", "ED",
    ],
    "Real Estate": [
        "PLD", "AMT", "EQIX", "CCI", "PSA",
        "SPG", "O", "WELL", "DLR", "VICI",
    ],
    "Materials": [
        "LIN", "APD", "SHW", "ECL", "FCX",
        "NEM", "NUE", "VMC", "MLM", "DD",
    ],
}

# LSEG fields to fetch
LSEG_FIELDS = {
    "pe": "TR.PE",
    "forward_pe": "TR.FwdPE",
    "peg": "TR.PEG",
    "pb": "TR.PriceToBVPerShare",
    "ps": "TR.PriceToSalesPerShare",
    "ev_ebitda": "TR.EVToEBITDA",
    "roe": "TR.F.ReturnAvgTotEqPct",
    "market_cap": "TR.CompanyMarketCap",
    "price": "TR.PriceClose",
    "name": "TR.CompanyName",
}


def _get_lseg():
    """Import and return lseg.data library."""
    try:
        import lseg.data as ld
        return ld
    except ImportError:
        try:
            import refinitiv.data as ld
            return ld
        except ImportError:
            return None


def _to_ric(ticker):
    """Convert ticker to LSEG RIC."""
    overrides = {
        "BRK.B": "BRKb.N",
        "BRK.A": "BRKa.N",
    }
    if ticker in overrides:
        return overrides[ticker]
    return f"{ticker}.O"  # Default to NASDAQ


# NYSE-listed stocks need .N suffix
NYSE_STOCKS = {
    "JPM", "BAC", "GS", "MS", "AXP", "BLK",  # Financials
    "XOM", "CVX", "COP", "EOG", "SLB", "MPC", "PSX", "OXY", "VLO", "PXD",  # Energy
    "JNJ", "ABT", "PFE", "TMO", "AMGN",  # Health Care
    "UNH", "LLY", "MRK", "ABBV",  # Health Care
    "HD", "LOW", "NKE", "MCD",  # Consumer Discretionary
    "CAT", "RTX", "UNP", "HON", "DE", "BA", "LMT", "UPS", "GE",  # Industrials
    "WMT", "PG", "KO", "PEP", "PM", "CL", "MO", "KHC",  # Consumer Staples
    "NEE", "SO", "DUK", "AEP", "D", "EXC", "XEL", "ED", "SRE", "CEG",  # Utilities
    "PLD", "AMT", "CCI", "PSA", "SPG", "O", "WELL", "VICI",  # Real Estate
    "LIN", "APD", "SHW", "ECL", "FCX", "NEM", "NUE", "VMC", "MLM", "DD",  # Materials
    "DIS", "CMCSA", "VZ", "T",  # Comm Services
    "V", "MA", "SPGI",  # Financials (NYSE)
}


def fetch_lseg_sector_data():
    """
    Fetch live fundamental data for major stocks in each sector
    and compute market-cap-weighted sector averages.

    Returns:
        DataFrame with sector-level metrics from LSEG live data.
    """
    ld = _get_lseg()
    if ld is None:
        print("  LSEG library not installed")
        return None

    print("Opening LSEG session...")
    try:
        ld.open_session()
    except Exception as e:
        print(f"  Failed to open LSEG session: {e}")
        return None

    print("Connected to LSEG Workspace")

    sector_results = []

    try:
        for sector, tickers in SECTOR_STOCKS.items():
            print(f"\n  Fetching {sector}...")

            # Build RICs using exchange-aware resolution
            rics = []
            for t in tickers:
                if t in {"BRK.B", "BRK.A"}:
                    rics.append(_to_ric(t))
                elif t in NYSE_STOCKS:
                    rics.append(f"{t}.N")
                else:
                    rics.append(f"{t}.O")

            fields = list(LSEG_FIELDS.values())

            try:
                df = ld.get_data(rics, fields=fields)
            except Exception as e:
                print(f"    Error: {e}")
                continue

            if df is None or df.empty:
                print(f"    No data returned")
                continue

            # Parse results
            stocks = []
            for _, row in df.iterrows():
                stock = {}
                for metric, field in LSEG_FIELDS.items():
                    # Find the matching column
                    val = None
                    for col in df.columns:
                        if field.split(".")[-1].lower() in col.lower():
                            raw = row[col]
                            if raw is not None and str(raw) not in ("", "nan", "<NA>"):
                                try:
                                    val = float(raw)
                                except (ValueError, TypeError):
                                    if metric == "name":
                                        val = str(raw)
                            break

                    # Fallback: positional
                    if val is None and metric != "name":
                        idx = list(LSEG_FIELDS.keys()).index(metric) + 1
                        if idx < len(row):
                            raw = row.iloc[idx]
                            if raw is not None and str(raw) not in ("", "nan", "<NA>"):
                                try:
                                    val = float(raw)
                                except (ValueError, TypeError):
                                    pass

                    stock[metric] = val

                if stock.get("market_cap") and stock["market_cap"] > 0:
                    stocks.append(stock)

            if not stocks:
                print(f"    No valid stock data")
                continue

            # Compute market-cap-weighted averages
            total_mcap = sum(s["market_cap"] for s in stocks if s.get("market_cap"))

            sector_row = {"sector": sector, "num_stocks": len(stocks)}

            for metric in ["pe", "forward_pe", "peg", "pb", "ps", "ev_ebitda", "roe"]:
                vals = [(s[metric], s["market_cap"])
                        for s in stocks
                        if s.get(metric) is not None
                        and s.get("market_cap") is not None
                        and 0 < s[metric] < 500]  # filter extreme outliers

                if vals:
                    weighted_sum = sum(v * w for v, w in vals)
                    weight_total = sum(w for _, w in vals)
                    sector_row[f"lseg_{metric}"] = weighted_sum / weight_total
                else:
                    sector_row[f"lseg_{metric}"] = np.nan

            # Count how many stocks had data
            sector_row["stocks_with_pe"] = sum(1 for s in stocks
                                                if s.get("forward_pe") is not None
                                                and 0 < s["forward_pe"] < 500)

            n_valid = sector_row["stocks_with_pe"]
            print(f"    {len(stocks)} stocks, {n_valid} with Forward P/E data")

            sector_results.append(sector_row)

    finally:
        try:
            ld.close_session()
        except Exception:
            pass
        print("\nLSEG session closed")

    if not sector_results:
        return None

    return pd.DataFrame(sector_results)


def print_lseg_comparison(lseg_df, damodaran_df):
    """Print comparison of LSEG live data vs Damodaran."""
    W = 90

    print(f"\n{'=' * W}")
    print("LSEG LIVE DATA vs DAMODARAN (Annual Update)")
    print(f"{'=' * W}")

    merged = damodaran_df.merge(lseg_df, on="sector", how="left")

    print(f"\n  {'Sector':<25} {'Dam Fwd PE':>10} {'LSEG Fwd PE':>12} {'Diff':>7} "
          f"{'Dam PEG':>8} {'LSEG PEG':>9}")
    print(f"  {'-' * 75}")

    for _, row in merged.sort_values("forward_pe").iterrows():
        dam_pe = f"{row['forward_pe']:.1f}" if pd.notna(row.get('forward_pe')) else "N/A"
        lseg_pe = f"{row['lseg_forward_pe']:.1f}" if pd.notna(row.get('lseg_forward_pe')) else "N/A"

        diff = ""
        if pd.notna(row.get('forward_pe')) and pd.notna(row.get('lseg_forward_pe')):
            d = row['lseg_forward_pe'] - row['forward_pe']
            diff = f"{d:+.1f}"

        dam_peg = f"{row['peg_ratio']:.2f}" if pd.notna(row.get('peg_ratio')) else "N/A"
        lseg_peg = f"{row['lseg_peg']:.2f}" if pd.notna(row.get('lseg_peg')) else "N/A"

        print(f"  {row['sector']:<25} {dam_pe:>10} {lseg_pe:>12} {diff:>7} "
              f"{dam_peg:>8} {lseg_peg:>9}")

    # Flag large divergences
    print(f"\n  Large divergences explained:")
    for _, row in merged.iterrows():
        if pd.notna(row.get('forward_pe')) and pd.notna(row.get('lseg_forward_pe')):
            d = row['lseg_forward_pe'] - row['forward_pe']
            if abs(d) > 15:
                sector = row['sector']
                print(f"  - {sector}: LSEG {d:+.0f}x vs Damodaran — "
                      f"large-cap mega stocks dominate mcap-weighted average")

    print(f"\n  Note: Damodaran data updated ~Jan 2026 (all US industries, equal-weighted)")
    print(f"  LSEG data is live (top 10 stocks per sector, market-cap-weighted)")
    print(f"  Key difference: Damodaran equal-weights industries; LSEG weights by market cap")
    print(f"  Mega-cap stocks (TSLA, AMZN, etc.) can dominate LSEG sector averages")


if __name__ == "__main__":
    lseg_df = fetch_lseg_sector_data()
    if lseg_df is not None:
        print("\nLSEG Sector Data:")
        for _, row in lseg_df.iterrows():
            fpe = f"{row['lseg_forward_pe']:.1f}" if pd.notna(row.get('lseg_forward_pe')) else "N/A"
            peg = f"{row['lseg_peg']:.2f}" if pd.notna(row.get('lseg_peg')) else "N/A"
            print(f"  {row['sector']:<25} Fwd PE: {fpe:>7}  PEG: {peg:>7}")
