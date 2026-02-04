"""
LSEG Field Probe — Find Correct Field Names
==============================================
Probes lseg-data 25.3 for I/B/E/S estimate fields needed by the
EPS Revisions factor in the quant scoring model.

Run:  python lseg_field_probe.py
"""

import sys
import traceback

try:
    import lseg.data as ld
    LIB_NAME = "lseg.data"
except ImportError:
    try:
        import refinitiv.data as ld
        LIB_NAME = "refinitiv.data"
    except ImportError:
        print("Neither lseg.data nor refinitiv.data found.")
        sys.exit(1)


def try_field(ric, field, params=None):
    """Try fetching a field and return (success, value)."""
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


def try_fields_batch(ric, fields, params=None):
    """Try fetching multiple fields at once. Returns {field: value} for successes."""
    try:
        if params:
            df = ld.get_data(ric, fields=fields, parameters=params)
        else:
            df = ld.get_data(ric, fields=fields)
        if df is not None and not df.empty:
            results = {}
            for col in df.columns[1:]:  # skip Instrument column
                val = df.iloc[0][col]
                if val is not None and str(val) not in ("", "nan", "<NA>"):
                    results[col] = val
            return results
    except Exception:
        pass
    return {}


def section(title):
    print(f"\n{'=' * 70}")
    print(f" {title}")
    print(f"{'=' * 70}")


def main():
    version = getattr(ld, "__version__", "unknown")
    print(f"LSEG Field Probe — {LIB_NAME} {version}")
    print("=" * 70)

    ld.open_session()
    print("[OK] Session opened\n")

    ric = "AAPL.O"

    # ==================================================================
    # 1. NUMBER OF ESTIMATES
    # ==================================================================
    section("1. NUMBER OF ESTIMATES")
    candidates = [
        ("TR.NumberOfEstimates", {"Period": "FY1"}),
        ("TR.NumberOfEstimates", None),
        ("TR.EpsNoEst", {"Period": "FY1"}),
        ("TR.EpsNoEst", None),
        ("TR.EPSNumEstimates", {"Period": "FY1"}),
        ("TR.FY1NoEstEPS", None),
        ("TR.NoEstEPS", {"Period": "FY1"}),
        ("TR.EPSFRMeanNumInclEstimates", {"Period": "FY1"}),
        ("TR.F.EpsNoEst", None),
    ]
    for field, params in candidates:
        ok, val = try_field(ric, field, params)
        status = f"[PASS] = {val}" if ok else "[    ]"
        p = f" ({', '.join(f'{k}={v}' for k,v in params.items())})" if params else ""
        print(f"  {status:<30} {field}{p}")

    # ==================================================================
    # 2. UP/DOWN REVISION COUNTS
    # ==================================================================
    section("2. UP/DOWN REVISION COUNTS")

    rev_candidates = [
        # lseg-data style
        ("TR.NumOfUpRevisions", {"Period": "FY1", "RollPeriod": "7D"}),
        ("TR.NumOfDownRevisions", {"Period": "FY1", "RollPeriod": "7D"}),
        ("TR.NumOfUpRevisions", {"Period": "FY1", "RollPeriod": "30D"}),
        ("TR.NumOfDownRevisions", {"Period": "FY1", "RollPeriod": "30D"}),
        ("TR.NumOfUpRevisions", {"Period": "FY1", "RollPeriod": "90D"}),
        ("TR.NumOfDownRevisions", {"Period": "FY1", "RollPeriod": "90D"}),
        # Without RollPeriod
        ("TR.NumOfUpRevisions", {"Period": "FY1"}),
        ("TR.NumOfDownRevisions", {"Period": "FY1"}),
        ("TR.NumOfUpRevisions", None),
        ("TR.NumOfDownRevisions", None),
        # Week/Month/Quarter suffix variants
        ("TR.EpsUpRevisions1Wk", {"Period": "FY1"}),
        ("TR.EpsDownRevisions1Wk", {"Period": "FY1"}),
        ("TR.EpsUpRevisions1Mo", {"Period": "FY1"}),
        ("TR.EpsDownRevisions1Mo", {"Period": "FY1"}),
        ("TR.EpsUpRevisions3Mo", {"Period": "FY1"}),
        ("TR.EpsDownRevisions3Mo", {"Period": "FY1"}),
        ("TR.EpsUpRevisions1Wk", None),
        ("TR.EpsDownRevisions1Wk", None),
        ("TR.EpsUpRevisions1Mo", None),
        ("TR.EpsDownRevisions1Mo", None),
        ("TR.EpsUpRevisions3Mo", None),
        ("TR.EpsDownRevisions3Mo", None),
        # FR prefix
        ("TR.F.EPSUpRev1Mo", None),
        ("TR.F.EPSDownRev1Mo", None),
        ("TR.F.EPSUpRev3Mo", None),
        ("TR.F.EPSDownRev3Mo", None),
        # StarMine / SmartEstimate
        ("TR.EpsRevUpW", {"Period": "FY1"}),
        ("TR.EpsRevDnW", {"Period": "FY1"}),
        ("TR.EpsRevUpM", {"Period": "FY1"}),
        ("TR.EpsRevDnM", {"Period": "FY1"}),
        ("TR.EpsRevUpQ", {"Period": "FY1"}),
        ("TR.EpsRevDnQ", {"Period": "FY1"}),
        ("TR.EpsRevUpW", None),
        ("TR.EpsRevDnW", None),
        ("TR.EpsRevUpM", None),
        ("TR.EpsRevDnM", None),
        # Cased differently
        ("TR.EPSUpRevisions", {"Period": "FY1"}),
        ("TR.EPSDownRevisions", {"Period": "FY1"}),
        ("TR.EPSUpRev", {"Period": "FY1"}),
        ("TR.EPSDownRev", {"Period": "FY1"}),
        # FY1 in field name
        ("TR.FY1EPSUpRevisions", None),
        ("TR.FY1EPSDownRevisions", None),
        ("TR.FY1EpsUpRev1Mo", None),
        ("TR.FY1EpsDownRev1Mo", None),
    ]

    for field, params in rev_candidates:
        ok, val = try_field(ric, field, params)
        status = f"[PASS] = {val}" if ok else "[    ]"
        p = f" ({', '.join(f'{k}={v}' for k,v in params.items())})" if params else ""
        print(f"  {status:<30} {field}{p}")

    # ==================================================================
    # 3. EPS SURPRISE
    # ==================================================================
    section("3. EPS SURPRISE")
    surprise_candidates = [
        ("TR.EPSSurprise", None),
        ("TR.EPSSurprisePct", None),
        ("TR.EpsSurprise", None),
        ("TR.EpsSurprisePct", None),
        ("TR.EPSActualSurprise", None),
        ("TR.EPSSurprisePercent", None),
        ("TR.F.EPSSurprise", None),
        ("TR.F.EPSSurprisePct", None),
        ("TR.EPSActValue", None),
        ("TR.EPSMeanEstimate", None),
    ]
    for field, params in surprise_candidates:
        ok, val = try_field(ric, field, params)
        status = f"[PASS] = {val}" if ok else "[    ]"
        print(f"  {status:<30} {field}")

    # ==================================================================
    # 4. EPS MEAN HISTORY (point-in-time via SDate)
    # ==================================================================
    section("4. EPS MEAN HISTORY (SDate)")
    from datetime import datetime, timedelta
    today = datetime.now()
    sdates = {
        "today":  today.strftime("%Y-%m-%d"),
        "7d ago": (today - timedelta(days=7)).strftime("%Y-%m-%d"),
        "30d ago": (today - timedelta(days=30)).strftime("%Y-%m-%d"),
        "90d ago": (today - timedelta(days=90)).strftime("%Y-%m-%d"),
        "1y ago":  (today - timedelta(days=365)).strftime("%Y-%m-%d"),
    }
    for label, sdate in sdates.items():
        ok, val = try_field(ric, "TR.EPSMean", {"Period": "FY1", "SDate": sdate})
        status = f"[PASS] = {val}" if ok else "[    ]"
        print(f"  {status:<30} TR.EPSMean FY1, SDate={sdate} [{label}]")

    print()
    for label, sdate in sdates.items():
        ok, val = try_field(ric, "TR.EPSMean", {"Period": "FY2", "SDate": sdate})
        status = f"[PASS] = {val}" if ok else "[    ]"
        print(f"  {status:<30} TR.EPSMean FY2, SDate={sdate} [{label}]")

    # ==================================================================
    # 5. BROAD ESTIMATE FIELD SCAN
    # ==================================================================
    section("5. BROAD FIELD SCAN (no params)")
    broad_fields = [
        "TR.EPSMean", "TR.EPSMedian", "TR.EPSHigh", "TR.EPSLow",
        "TR.EPSStdDev", "TR.EPSActValue", "TR.EPSMeanEstimate",
        "TR.RevenueMean", "TR.RevenueMedian", "TR.RevenueHigh",
        "TR.RevenueLow", "TR.RevenueActValue",
        "TR.EBITDAMean", "TR.EBITDAActValue",
        "TR.PriceTargetMean", "TR.PriceTargetMedian",
        "TR.PriceTargetHigh", "TR.PriceTargetLow",
        "TR.NumOfRecommendations", "TR.ConsRecommendation",
        "TR.StarMineSmartEstimate",
        "TR.EPSSmartEst",
    ]
    for field in broad_fields:
        ok, val = try_field(ric, field)
        status = f"[PASS] = {val}" if ok else "[    ]"
        print(f"  {status:<35} {field}")

    section("   BROAD FIELD SCAN (Period=FY1)")
    for field in broad_fields:
        ok, val = try_field(ric, field, {"Period": "FY1"})
        status = f"[PASS] = {val}" if ok else "[    ]"
        print(f"  {status:<35} {field} (FY1)")

    # ==================================================================
    # 6. BATCH TEST — all working fields at once for 5 symbols
    # ==================================================================
    section("6. BATCH MULTI-SYMBOL TEST")

    # Collect fields that passed individually (we know these work)
    known_good = ["TR.EPSMean", "TR.EPSActValue", "TR.EPSMeanEstimate"]
    batch_rics = ["AAPL.O", "MSFT.O", "NVDA.O", "GOOGL.O", "AMZN.O"]

    print(f"  Fetching {len(known_good)} fields for {len(batch_rics)} symbols...")
    try:
        df = ld.get_data(batch_rics, fields=known_good, parameters={"Period": "FY1"})
        if df is not None and not df.empty:
            print(f"  [PASS] Shape: {df.shape}")
            print(f"  Columns: {list(df.columns)}")
            print(df.to_string(index=False))
        else:
            print("  [EMPTY]")
    except Exception as e:
        print(f"  [FAIL] {e}")

    # ==================================================================
    # SUMMARY
    # ==================================================================
    section("RESULTS SUMMARY")

    print("""
  Copy the output above into For Claude.txt and I will:

  1. Identify which fields work for revision counts
  2. Update lseg_client.py to use the correct field names
  3. Enable the 5-factor model if sufficient data is available

  Even if no revision count fields work, we can build the EPS Revisions
  factor from point-in-time EPSMean changes (Approach B):
    - Fetch TR.EPSMean at SDate = today, -7d, -30d, -90d
    - Compute % change in consensus over each window
    - Compute surprise from TR.EPSActValue vs TR.EPSMeanEstimate
""")

    try:
        ld.close_session()
    except Exception:
        pass


if __name__ == "__main__":
    main()
