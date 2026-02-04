"""
LSEG Access Diagnostic
========================
Probes your LSEG Data Library installation and permissions to determine
what data you can access — specifically I/B/E/S EPS estimates for the
quant scoring model.

Run:  python lseg_access_check.py
"""

import sys
import traceback


def section(title):
    print(f"\n{'=' * 60}")
    print(f" {title}")
    print(f"{'=' * 60}")


def check(label, func):
    """Run a check and print pass/fail."""
    try:
        result = func()
        print(f"  [PASS] {label}")
        return result
    except Exception as e:
        print(f"  [FAIL] {label}")
        print(f"         {type(e).__name__}: {e}")
        return None


def main():
    print("LSEG Data Library — Access Diagnostic")
    print("=" * 60)

    # ---------------------------------------------------------------
    # 1. Check if lseg.data is installed
    # ---------------------------------------------------------------
    section("1. INSTALLATION")

    ld = check("Import lseg.data", lambda: __import__("lseg.data"))
    if ld is None:
        print("\n  lseg.data is not installed.")
        print("  Install with:  pip install lseg-data")
        print("  Docs: https://developers.lseg.com/en/api-catalog/refinitiv-data-platform/refinitiv-data-library-for-python")

        # Check for older refinitiv.data package
        rd = check("Import refinitiv.data (legacy)", lambda: __import__("refinitiv.data"))
        if rd:
            print("\n  Found legacy refinitiv.data package.")
            print("  Consider upgrading: pip install lseg-data")

        # Check for eikon
        ek = check("Import eikon (legacy Eikon API)", lambda: __import__("eikon"))
        if ek:
            print("\n  Found legacy eikon package.")

        if not rd and not ek:
            print("\n  No LSEG/Refinitiv packages found.")
            print("  Exiting — install lseg-data first.")
            return

    # ---------------------------------------------------------------
    # 2. Check version and configuration
    # ---------------------------------------------------------------
    section("2. VERSION & CONFIG")

    try:
        import lseg.data as ld
        version = getattr(ld, "__version__", "unknown")
        print(f"  lseg.data version: {version}")
    except ImportError:
        # Try legacy
        try:
            import refinitiv.data as ld
            version = getattr(ld, "__version__", "unknown")
            print(f"  refinitiv.data version: {version}")
        except ImportError:
            try:
                import eikon as ek
                version = getattr(ek, "__version__", "unknown")
                print(f"  eikon version: {version}")
                print("\n  NOTE: eikon uses a different API. The checks below")
                print("  will use the eikon interface instead.")
                run_eikon_checks(ek)
                return
            except ImportError:
                print("  No LSEG library available.")
                return

    # Check for config file
    import os
    config_paths = [
        os.path.expanduser("~/.refinitiv/refinitiv-data.config.json"),
        os.path.expanduser("~/lseg-data.config.json"),
        os.path.expanduser("~/.lseg/lseg-data.config.json"),
        "lseg-data.config.json",
        "refinitiv-data.config.json",
    ]
    found_config = False
    for p in config_paths:
        if os.path.exists(p):
            print(f"  Config found: {p}")
            found_config = True
            # Read and show (redact keys)
            try:
                import json
                with open(p) as f:
                    cfg = json.load(f)
                print(f"  Config keys: {list(cfg.keys())}")
                if "sessions" in cfg:
                    for sess_name, sess_cfg in cfg["sessions"].items():
                        print(f"    Session '{sess_name}': type={sess_cfg.get('type', '?')}")
            except Exception as e:
                print(f"  Could not parse config: {e}")

    if not found_config:
        print("  No config file found in standard locations.")
        print("  Checked:", ", ".join(config_paths))
        print("\n  You may need to create a config file. See:")
        print("  https://developers.lseg.com/en/api-catalog/refinitiv-data-platform/refinitiv-data-library-for-python/quick-start")

    # ---------------------------------------------------------------
    # 3. Try to open a session
    # ---------------------------------------------------------------
    section("3. SESSION")

    session = None
    try:
        ld.open_session()
        print("  [PASS] Session opened successfully")
        session = True
    except Exception as e:
        print(f"  [FAIL] Could not open session: {e}")
        print("\n  Common fixes:")
        print("  - Ensure Workspace/Eikon is running (for Desktop session)")
        print("  - Check your app key in the config file")
        print("  - For Platform session, verify client credentials")
        session = False

    if not session:
        print("\n  Cannot proceed without a session. Fix the session issue above.")
        print("  If you have a Workspace app key, create ~/lseg-data.config.json:")
        print('  {"sessions": {"default": "desktop", "desktop": {"app-key": "YOUR_KEY"}}}')
        return

    # ---------------------------------------------------------------
    # 4. Test basic data access
    # ---------------------------------------------------------------
    section("4. BASIC DATA ACCESS")

    # Test simple price fetch
    check("Fetch AAPL price", lambda: ld.get_data("AAPL.O", fields=["TR.PriceClose"]))

    # Test fundamental data
    result = check("Fetch AAPL PE ratio",
                   lambda: ld.get_data("AAPL.O", fields=["TR.PE"]))
    if result is not None:
        print(f"         Data: {result.to_string() if hasattr(result, 'to_string') else result}")

    # ---------------------------------------------------------------
    # 5. Test I/B/E/S Estimates Access (the key data we need)
    # ---------------------------------------------------------------
    section("5. I/B/E/S ESTIMATES ACCESS")

    # Current consensus EPS
    ibes_fields = {
        "Consensus EPS Mean (FY1)": ["TR.EPSMean", {"Period": "FY1"}],
        "Consensus EPS Mean (FY2)": ["TR.EPSMean", {"Period": "FY2"}],
        "Number of Estimates": ["TR.NumberOfEstimates", {"Period": "FY1"}],
        "# Up Revisions 7D": ["TR.NumOfUpRevisions", {"Period": "FY1", "RollPeriod": "7D"}],
        "# Down Revisions 7D": ["TR.NumOfDownRevisions", {"Period": "FY1", "RollPeriod": "7D"}],
        "# Up Revisions 30D": ["TR.NumOfUpRevisions", {"Period": "FY1", "RollPeriod": "30D"}],
        "# Down Revisions 30D": ["TR.NumOfDownRevisions", {"Period": "FY1", "RollPeriod": "30D"}],
        "# Up Revisions 90D": ["TR.NumOfUpRevisions", {"Period": "FY1", "RollPeriod": "90D"}],
        "# Down Revisions 90D": ["TR.NumOfDownRevisions", {"Period": "FY1", "RollPeriod": "90D"}],
        "EPS Surprise (Last Q)": ["TR.EPSSurprise"],
        "EPS Actual (Last Q)": ["TR.EPSActValue"],
        "EPS Estimate (Last Q)": ["TR.EPSMeanEstimate"],
    }

    accessible_fields = []
    test_ric = "AAPL.O"
    print(f"  Testing I/B/E/S fields for {test_ric}:\n")

    for label, field_spec in ibes_fields.items():
        field = field_spec[0]
        params = field_spec[1] if len(field_spec) > 1 else {}
        try:
            if params:
                df = ld.get_data(test_ric, fields=[field], parameters=params)
            else:
                df = ld.get_data(test_ric, fields=[field])

            if df is not None and not df.empty:
                val = df.iloc[0, -1] if hasattr(df, 'iloc') else df
                print(f"  [PASS] {label:<30} = {val}")
                accessible_fields.append(label)
            else:
                print(f"  [EMPTY] {label:<30} (returned empty)")
        except Exception as e:
            err = str(e)[:60]
            print(f"  [FAIL] {label:<30} — {err}")

    # ---------------------------------------------------------------
    # 6. Test Historical Estimates (point-in-time)
    # ---------------------------------------------------------------
    section("6. HISTORICAL / POINT-IN-TIME ESTIMATES")

    # This is critical for backtesting — we need estimates AS OF a past date
    pit_tests = {
        "EPS Mean as of 2025-01-15": {
            "fields": ["TR.EPSMean"],
            "params": {"Period": "FY1", "SDate": "2025-01-15"},
        },
        "EPS Mean as of 2024-01-15": {
            "fields": ["TR.EPSMean"],
            "params": {"Period": "FY1", "SDate": "2024-01-15"},
        },
        "Revision counts as of 2025-01-15": {
            "fields": ["TR.NumOfUpRevisions", "TR.NumOfDownRevisions"],
            "params": {"Period": "FY1", "RollPeriod": "30D", "SDate": "2025-01-15"},
        },
    }

    for label, spec in pit_tests.items():
        try:
            df = ld.get_data(test_ric, fields=spec["fields"], parameters=spec["params"])
            if df is not None and not df.empty:
                vals = df.iloc[0, 1:].to_dict() if hasattr(df, 'iloc') else df
                print(f"  [PASS] {label}")
                print(f"         {vals}")
                accessible_fields.append(f"PIT: {label}")
            else:
                print(f"  [EMPTY] {label}")
        except Exception as e:
            err = str(e)[:80]
            print(f"  [FAIL] {label} — {err}")

    # ---------------------------------------------------------------
    # 7. Test Batch Fetch (multiple symbols)
    # ---------------------------------------------------------------
    section("7. BATCH FETCH (multiple symbols)")

    batch_rics = ["AAPL.O", "MSFT.O", "NVDA.O", "GOOG.O", "AMZN.O"]
    try:
        df = ld.get_data(
            batch_rics,
            fields=["TR.EPSMean", "TR.NumberOfEstimates", "TR.EPSSurprise"],
            parameters={"Period": "FY1"},
        )
        if df is not None and not df.empty:
            print(f"  [PASS] Batch fetch for {len(batch_rics)} symbols")
            print(f"         Shape: {df.shape}")
            print(f"         Columns: {list(df.columns)}")
            print(df.to_string(index=False))
        else:
            print("  [EMPTY] Batch fetch returned no data")
    except Exception as e:
        print(f"  [FAIL] Batch fetch — {e}")

    # ---------------------------------------------------------------
    # 8. Test Search for Available Content
    # ---------------------------------------------------------------
    section("8. CONTENT DISCOVERY")

    try:
        # Check what content sets are available
        if hasattr(ld, 'discovery'):
            result = ld.discovery.search(query="I/B/E/S", top=5)
            print(f"  [PASS] Discovery search returned results")
            if hasattr(result, 'to_string'):
                print(result.to_string())
        else:
            print("  [SKIP] Discovery API not available in this version")
    except Exception as e:
        print(f"  [FAIL] Discovery — {e}")

    # ---------------------------------------------------------------
    # Summary
    # ---------------------------------------------------------------
    section("SUMMARY")

    print(f"  Accessible I/B/E/S fields: {len(accessible_fields)}/{len(ibes_fields) + 3}")
    if accessible_fields:
        print(f"  Fields available:")
        for f in accessible_fields:
            print(f"    - {f}")

    total_needed = len(ibes_fields)
    if len(accessible_fields) >= total_needed * 0.7:
        print(f"\n  VERDICT: Good I/B/E/S access. You can enable the 5-factor model.")
        print(f"  Next: Update lseg_client.py to use these fields.")
    elif len(accessible_fields) > 0:
        print(f"\n  VERDICT: Partial access. Some I/B/E/S fields available.")
        print(f"  You may need to request additional data permissions.")
    else:
        print(f"\n  VERDICT: No I/B/E/S access detected.")
        print(f"  Contact your LSEG account rep to request I/B/E/S Estimates data.")
        print(f"  The 4-factor model (without EPS Revisions) will still work.")

    # Close session
    try:
        ld.close_session()
    except Exception:
        pass


def run_eikon_checks(ek):
    """Fallback checks using the legacy eikon API."""
    section("EIKON API CHECKS")

    # Check for app key
    print("  Enter your Eikon App Key (or press Enter to skip):")
    app_key = input("  > ").strip()
    if not app_key:
        print("  Skipped. Set app key with: eikon.set_app_key('YOUR_KEY')")
        return

    try:
        ek.set_app_key(app_key)
        print("  [PASS] App key set")
    except Exception as e:
        print(f"  [FAIL] Set app key: {e}")
        return

    # Test data fetch
    test_fields = [
        "TR.EPSMean", "TR.NumberOfEstimates",
        "TR.NumOfUpRevisions", "TR.NumOfDownRevisions",
        "TR.EPSSurprise",
    ]

    try:
        df, err = ek.get_data("AAPL.O", test_fields, {"Period": "FY1"})
        if df is not None:
            print(f"  [PASS] Eikon data fetch")
            print(f"         {df.to_string()}")
        if err:
            print(f"  Errors: {err}")
    except Exception as e:
        print(f"  [FAIL] Eikon data fetch: {e}")


if __name__ == "__main__":
    main()
