"""
LSEG Fundamental Field Probe
===============================
Tests whether LSEG Workspace has the ~20 fundamental metrics needed
for Value, Growth, and Profitability factors — potentially replacing
GuruFocus entirely.

Run:  python lseg_fundamental_probe.py
"""

import sys

try:
    import lseg.data as ld
    LIB_NAME = "lseg.data"
except ImportError:
    try:
        import refinitiv.data as ld
        LIB_NAME = "refinitiv.data"
    except ImportError:
        print("No LSEG library found.")
        sys.exit(1)


def try_field(ric, field, params=None):
    """Try fetching a field. Returns (success, value)."""
    try:
        if params:
            df = ld.get_data(ric, fields=[field], parameters=params)
        else:
            df = ld.get_data(ric, fields=[field])
        if df is not None and not df.empty and len(df.columns) > 1:
            val = df.iloc[0, 1]
            if val is not None and str(val) not in ("", "nan", "<NA>"):
                return True, val
    except Exception:
        pass
    return False, None


def section(title):
    print(f"\n{'=' * 70}")
    print(f" {title}")
    print(f"{'=' * 70}")


def main():
    version = getattr(ld, "__version__", "unknown")
    print(f"LSEG Fundamental Probe — {LIB_NAME} {version}")
    print("=" * 70)

    ld.open_session()
    print("[OK] Session opened\n")

    ric = "AAPL.O"

    # ==================================================================
    # 1. VALUE FACTOR — PE, Forward PE, PB, PS, EV/EBITDA, PEG
    # ==================================================================
    section("1. VALUE METRICS")

    value_fields = [
        # Trailing PE
        ("TR.PE", None, "Trailing P/E"),
        ("TR.PERatio", None, "P/E Ratio"),
        ("TR.F.PE", None, "F.PE"),
        ("TR.PriceTurnoverRatio", None, "PriceTurnoverRatio"),
        # Forward PE
        ("TR.ForwardPE", None, "Forward P/E"),
        ("TR.FwdPE", None, "Fwd P/E"),
        ("TR.PE", {"Period": "FY1"}, "PE (FY1)"),
        ("TR.PERatioFY1", None, "PERatioFY1"),
        # PB
        ("TR.PriceToBVPerShare", None, "Price/Book"),
        ("TR.PBRatio", None, "P/B Ratio"),
        ("TR.PriceToBookValue", None, "PriceToBookValue"),
        ("TR.F.PBRatio", None, "F.PBRatio"),
        # PS
        ("TR.PriceToSalesPerShare", None, "Price/Sales"),
        ("TR.PSRatio", None, "P/S Ratio"),
        ("TR.PriceToSales", None, "PriceToSales"),
        ("TR.F.PSRatio", None, "F.PSRatio"),
        # EV/EBITDA
        ("TR.EVToEBITDA", None, "EV/EBITDA"),
        ("TR.EVEBITDA", None, "EVEBITDA"),
        ("TR.F.EVToEBITDA", None, "F.EVToEBITDA"),
        ("TR.EnterpriseValueToEBITDA", None, "EntValueToEBITDA"),
        # PEG
        ("TR.PEG", None, "PEG Ratio"),
        ("TR.PEGRatio", None, "PEGRatio"),
        ("TR.F.PEGRatio", None, "F.PEGRatio"),
    ]

    for field, params, label in value_fields:
        ok, val = try_field(ric, field, params)
        status = f"[PASS] = {val}" if ok else "[    ]"
        p = f" ({', '.join(f'{k}={v}' for k,v in params.items())})" if params else ""
        print(f"  {status:<35} {field}{p:<20} ({label})")

    # ==================================================================
    # 2. PROFITABILITY — Margins, ROE, ROA, ROIC
    # ==================================================================
    section("2. PROFITABILITY METRICS")

    profit_fields = [
        # Gross Margin
        ("TR.GrossMargin", None, "Gross Margin"),
        ("TR.GrossProfitMargin", None, "GrossProfitMargin"),
        ("TR.F.GrossMargin", None, "F.GrossMargin"),
        ("TR.GrossMarginPercent", None, "GrossMargin%"),
        # Operating / EBIT Margin
        ("TR.OperatingMargin", None, "Operating Margin"),
        ("TR.OperMargin", None, "OperMargin"),
        ("TR.EBITMargin", None, "EBIT Margin"),
        ("TR.F.OperMargin", None, "F.OperMargin"),
        # Net Margin
        ("TR.NetProfitMargin", None, "Net Margin"),
        ("TR.NetMargin", None, "NetMargin"),
        ("TR.ProfitMargin", None, "ProfitMargin"),
        ("TR.F.NetMargin", None, "F.NetMargin"),
        # ROE
        ("TR.ROE", None, "ROE"),
        ("TR.ReturnOnEquity", None, "ReturnOnEquity"),
        ("TR.F.ReturnAvgTotEqPct", None, "F.ReturnAvgTotEq%"),
        ("TR.ROEPercent", None, "ROE%"),
        # ROA
        ("TR.ROA", None, "ROA"),
        ("TR.ReturnOnAssets", None, "ReturnOnAssets"),
        ("TR.F.ReturnAvgTotAstPct", None, "F.ReturnAvgTotAst%"),
        ("TR.ROAPercent", None, "ROA%"),
        # ROIC
        ("TR.ROIC", None, "ROIC"),
        ("TR.ReturnOnInvestedCapital", None, "ReturnOnInvCap"),
        ("TR.F.ROIC", None, "F.ROIC"),
        ("TR.ReturnOnCapital", None, "ReturnOnCapital"),
    ]

    for field, params, label in profit_fields:
        ok, val = try_field(ric, field, params)
        status = f"[PASS] = {val}" if ok else "[    ]"
        print(f"  {status:<35} {field:<35} ({label})")

    # ==================================================================
    # 3. GROWTH — Revenue, EPS, EBITDA growth
    # ==================================================================
    section("3. GROWTH METRICS")

    growth_fields = [
        # Revenue Growth YoY
        ("TR.RevenueGrowth", None, "Revenue Growth"),
        ("TR.RevenueGrowthRate", None, "RevGrowthRate"),
        ("TR.RevenueMeanGrowthRate", None, "RevMeanGrowthRate"),
        ("TR.F.SalesGro1Yr", None, "F.SalesGro1Yr"),
        ("TR.RevenueGrowthPct", None, "RevGrowth%"),
        # Revenue Growth 3Y
        ("TR.F.SalesGro3Yr", None, "F.SalesGro3Yr"),
        ("TR.RevenueGrowth3Y", None, "RevGrowth3Y"),
        ("TR.Revenue3YrCAGR", None, "Rev3YrCAGR"),
        # EPS Growth YoY
        ("TR.EPSGrowth", None, "EPS Growth"),
        ("TR.EPSGrowthRate", None, "EPSGrowthRate"),
        ("TR.F.EPSGro1Yr", None, "F.EPSGro1Yr"),
        ("TR.EPSGrowthPct", None, "EPSGrowth%"),
        # EPS Growth 3Y
        ("TR.F.EPSGro3Yr", None, "F.EPSGro3Yr"),
        ("TR.EPSGrowth3Y", None, "EPSGrowth3Y"),
        ("TR.EPS3YrCAGR", None, "EPS3YrCAGR"),
        # EBITDA Growth YoY
        ("TR.EBITDAGrowth", None, "EBITDA Growth"),
        ("TR.EBITDAGrowthRate", None, "EBITDAGrowthRate"),
        ("TR.F.EBITDAGro1Yr", None, "F.EBITDAGro1Yr"),
        ("TR.EBITDAGrowthPct", None, "EBITDAGrowth%"),
    ]

    for field, params, label in growth_fields:
        ok, val = try_field(ric, field, params)
        status = f"[PASS] = {val}" if ok else "[    ]"
        print(f"  {status:<35} {field:<35} ({label})")

    # ==================================================================
    # 4. STOCK INFO — Sector, Industry, Market Cap
    # ==================================================================
    section("4. STOCK INFO (for universe filtering)")

    info_fields = [
        ("TR.CompanyName", None, "Company Name"),
        ("TR.ExchangeName", None, "Exchange"),
        ("TR.TRBCEconomicSector", None, "TRBC Sector"),
        ("TR.GICSSector", None, "GICS Sector"),
        ("TR.TRBCIndustry", None, "TRBC Industry"),
        ("TR.GICSIndustry", None, "GICS Industry"),
        ("TR.CompanyMarketCap", None, "Market Cap"),
        ("TR.MarketCap", None, "MarketCap"),
        ("TR.PriceClose", None, "Price Close"),
        ("TR.TRBCIndustryGroup", None, "TRBC Industry Group"),
        ("TR.GICSSubIndustry", None, "GICS Sub-Industry"),
    ]

    for field, params, label in info_fields:
        ok, val = try_field(ric, field, params)
        status = f"[PASS] = {val}" if ok else "[    ]"
        print(f"  {status:<45} {field:<30} ({label})")

    # ==================================================================
    # 5. POINT-IN-TIME TEST — can we get historical fundamentals?
    # ==================================================================
    section("5. POINT-IN-TIME FUNDAMENTALS (SDate)")

    # Critical for backtesting: can we get PE/margins as of a past date?
    pit_fields = ["TR.PE", "TR.PriceToBVPerShare", "TR.EVToEBITDA",
                  "TR.ROE", "TR.GrossMargin", "TR.CompanyMarketCap"]

    for sdate_label, sdate in [("2025-01-15", "2025-01-15"),
                                ("2024-01-15", "2024-01-15"),
                                ("2023-01-15", "2023-01-15")]:
        print(f"\n  As of {sdate_label}:")
        for field in pit_fields:
            ok, val = try_field(ric, field, {"SDate": sdate})
            status = f"[PASS] = {val}" if ok else "[    ]"
            print(f"    {status:<35} {field}")

    # ==================================================================
    # 6. BATCH TEST — multiple symbols at once
    # ==================================================================
    section("6. BATCH TEST (5 symbols)")

    batch_rics = ["AAPL.O", "MSFT.O", "NVDA.O", "GOOGL.O", "AMZN.O"]
    batch_fields = ["TR.CompanyName", "TR.PE", "TR.PriceToBVPerShare",
                    "TR.ROE", "TR.CompanyMarketCap", "TR.GICSSector"]

    # Try the batch with fields that have passed individually
    # (we won't know which ones until runtime, so try the common ones)
    print(f"  Fetching {len(batch_fields)} fields for {len(batch_rics)} symbols...")
    try:
        df = ld.get_data(batch_rics, fields=batch_fields)
        if df is not None and not df.empty:
            print(f"  [PASS] Shape: {df.shape}")
            print(f"  Columns: {list(df.columns)}")
            # Print with formatting
            for _, row in df.iterrows():
                print(f"    {row.iloc[0]:<10} {row.to_dict()}")
        else:
            print("  [EMPTY]")
    except Exception as e:
        print(f"  [FAIL] {e}")

    # ==================================================================
    # SUMMARY
    # ==================================================================
    section("SUMMARY")
    print("""
  Paste this output into For Claude.txt and I will:

  1. Map working fields to each factor's sub-factors
  2. Rewrite the data pipeline to use LSEG for everything
  3. Remove the GuruFocus dependency entirely
  4. If point-in-time works, backtesting is fully supported
""")

    try:
        ld.close_session()
    except Exception:
        pass


if __name__ == "__main__":
    main()
