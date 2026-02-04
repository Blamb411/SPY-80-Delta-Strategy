"""
Run Scoring — Single Date
===========================
Score the full universe for a single date and display results.

Usage:
    python run_scoring.py                              # Score full universe
    python run_scoring.py --date 2026-01-15            # Score for specific date
    python run_scoring.py --symbols AAPL,MSFT,NVDA     # Score specific stocks
    python run_scoring.py --file strong_buys.csv       # Score tickers from file
    python run_scoring.py --top 20                     # Show top N results
    python run_scoring.py --file tickers.csv --report  # Score file + save report

File formats supported:
    - Plain text: one ticker per line
    - CSV with header: reads "Symbol", "Ticker", or "symbol" column
    - CSV without recognized header: reads first column
    - Lines starting with # are ignored (comments)
"""

import os
import sys
import csv
import argparse
import warnings
from datetime import datetime

warnings.filterwarnings("ignore", message="invalid value encountered in cast")

# Ensure we can import from project root
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
import db_schema
from data.lseg_client import LSEGClient
from data.polygon_client import PolygonClient
from data.universe_builder import build_universe, get_filtered_universe, get_sector_map
from scoring.composite_scorer import score_universe, get_top_n
from analysis.report_generator import generate_scoring_report, save_report


def load_symbols_from_file(filepath: str) -> list:
    """
    Load ticker symbols from a file.

    Supports:
      - .txt: one ticker per line
      - .csv: looks for Symbol/Ticker/symbol/ticker column, falls back to first column
      - Comments (lines starting with #) are skipped
      - Blank lines are skipped
    """
    filepath = os.path.abspath(filepath)
    if not os.path.exists(filepath):
        print(f"  ERROR: File not found: {filepath}")
        sys.exit(1)

    symbols = []
    ext = os.path.splitext(filepath)[1].lower()

    with open(filepath, "r", encoding="utf-8-sig") as f:
        # Sniff for CSV with headers
        sample = f.read(4096)
        f.seek(0)

        # Check if it looks like a CSV with a header
        sniffer = csv.Sniffer()
        try:
            has_header = sniffer.has_header(sample)
        except csv.Error:
            has_header = False

        if ext == ".csv" or has_header or "," in sample.split("\n")[0]:
            # Parse as CSV
            reader = csv.DictReader(f) if has_header else None

            if reader and reader.fieldnames:
                # Find the ticker column
                ticker_col = None
                for candidate in ["Symbol", "Ticker", "symbol", "ticker",
                                  "SYMBOL", "TICKER", "Stock", "stock",
                                  "Stock Symbol", "Sym"]:
                    if candidate in reader.fieldnames:
                        ticker_col = candidate
                        break

                if ticker_col:
                    print(f"  Reading column '{ticker_col}' from {os.path.basename(filepath)}")
                    for row in reader:
                        sym = row[ticker_col].strip()
                        if sym and not sym.startswith("#"):
                            # Clean: remove $ prefix, whitespace, quotes
                            sym = sym.lstrip("$").strip().upper()
                            if sym and sym.isalpha() or "." in sym:
                                symbols.append(sym)
                else:
                    # No recognized header — use first column
                    print(f"  No Symbol/Ticker column found, using first column")
                    f.seek(0)
                    next(f)  # skip header row
                    for line in f:
                        parts = line.strip().split(",")
                        if parts:
                            sym = parts[0].strip().strip('"').lstrip("$").upper()
                            if sym and not sym.startswith("#") and (sym.isalpha() or "." in sym):
                                symbols.append(sym)
            else:
                # No header — first column
                f.seek(0)
                for line in f:
                    parts = line.strip().split(",")
                    if parts:
                        sym = parts[0].strip().strip('"').lstrip("$").upper()
                        if sym and not sym.startswith("#") and (sym.isalpha() or "." in sym):
                            symbols.append(sym)
        else:
            # Plain text, one ticker per line
            for line in f:
                sym = line.strip().lstrip("$").upper()
                if sym and not sym.startswith("#") and (sym.isalpha() or "." in sym):
                    symbols.append(sym)

    # Deduplicate while preserving order
    seen = set()
    unique = []
    for s in symbols:
        if s not in seen:
            seen.add(s)
            unique.append(s)

    return unique


def main():
    parser = argparse.ArgumentParser(description="Run quant scoring model")
    parser.add_argument("--date", default=datetime.now().strftime("%Y-%m-%d"),
                        help="Scoring date (YYYY-MM-DD)")
    parser.add_argument("--symbols", default=None,
                        help="Comma-separated symbols (overrides universe)")
    parser.add_argument("--file", default=None,
                        help="File with ticker symbols (.csv or .txt)")
    parser.add_argument("--top", type=int, default=20,
                        help="Number of top results to display")
    parser.add_argument("--all", action="store_true",
                        help="Show all results, not just top N")
    parser.add_argument("--skip-fetch", action="store_true",
                        help="Skip data fetching, use cached data only")
    parser.add_argument("--report", action="store_true",
                        help="Save report to file")
    args = parser.parse_args()

    score_date = args.date
    print(f"\n{'=' * 70}")
    print(f" QUANT SCORING MODEL — {score_date}")
    print(f"{'=' * 70}")

    # Step 1: Initialize database
    print("\n[1/5] Initializing database...")
    db_schema.init_db()

    # Step 2: Build universe
    print("\n[2/5] Building universe...")
    if args.file:
        symbols = load_symbols_from_file(args.file)
        print(f"  Loaded {len(symbols)} symbols from {os.path.basename(args.file)}")
        if symbols:
            print(f"  First: {symbols[0]}, Last: {symbols[-1]}")
    elif args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(",")]
        print(f"  Using {len(symbols)} specified symbols")
    else:
        symbols = build_universe(score_date)
        print(f"  Universe: {len(symbols)} symbols")

    if not symbols:
        print("  ERROR: No symbols to score.")
        return

    # Step 3: Fetch data (unless --skip-fetch)
    if not args.skip_fetch:
        print("\n[3/5] Fetching data...")

        # LSEG fundamentals + estimates
        lseg = LSEGClient()
        if lseg.is_available():
            print("  Fetching LSEG fundamental + estimate data...")
            lseg.fetch_universe_batch(symbols, score_date)
        else:
            print("  WARNING: LSEG not available (is Workspace running?)")
            print("  Proceeding with cached data only for fundamentals.")

        # Polygon momentum
        print("  Computing momentum from price data...")
        poly = PolygonClient()
        poly.fetch_momentum_batch(symbols, score_date)
    else:
        print("\n[3/5] Skipping data fetch (using cached data)")

    # Step 4: Get sector map and filter universe
    print("\n[4/5] Filtering universe and preparing scores...")
    sector_map = get_sector_map(score_date)
    filtered = get_filtered_universe(score_date)
    if filtered:
        symbols = filtered
    print(f"  Scoring {len(symbols)} stocks")

    # Step 5: Score
    print("\n[5/5] Running scoring pipeline...")
    results = score_universe(symbols, score_date, sector_map)

    # Display results
    show_count = len(results) if args.all else min(args.top, len(results))
    label = "ALL" if args.all else f"TOP {show_count}"

    print(f"\n{'=' * 75}")
    print(f" {label} STOCKS")
    print(f"{'=' * 75}")
    print(f"{'Rank':<5} {'Symbol':<7} {'Score':<7} {'Rating':<12} "
          f"{'Val':>4} {'Gro':>4} {'Pro':>4} {'Mom':>4} {'EPS':>4}")
    print("-" * 75)

    for i, r in enumerate(results[:show_count], 1):
        cb = " *" if r.get("circuit_breaker_hit") else ""
        eps = r.get('eps_revisions_grade', 'N/A')
        print(f"{i:<5} {r['symbol']:<7} {r['composite_score']:<7.3f} "
              f"{r['rating']:<12} "
              f"{r['value_grade']:>4} {r['growth_grade']:>4} "
              f"{r['profitability_grade']:>4} {r['momentum_grade']:>4} "
              f"{eps:>4}{cb}")

    # Rating summary
    print(f"\n--- Rating Summary ---")
    rating_counts = {}
    for r in results:
        rating_counts[r["rating"]] = rating_counts.get(r["rating"], 0) + 1
    for rating in ["Strong Buy", "Buy", "Hold", "Sell", "Strong Sell"]:
        count = rating_counts.get(rating, 0)
        if count > 0:
            pct = count / len(results) * 100
            print(f"  {rating:<12} {count:>4}  ({pct:5.1f}%)")

    # Show Strong Sell candidates too
    short_candidates = [r for r in results if r["rating"] in ("Strong Sell", "Sell")]
    if short_candidates:
        print(f"\n--- Short Candidates ({len(short_candidates)}) ---")
        for r in short_candidates[:10]:
            eps = r.get('eps_revisions_grade', 'N/A')
            print(f"  {r['symbol']:<7} {r['composite_score']:<7.3f} {r['rating']:<12} "
                  f"V:{r['value_grade']} G:{r['growth_grade']} "
                  f"P:{r['profitability_grade']} M:{r['momentum_grade']} E:{eps}")

    # Save report if requested
    if args.report:
        report = generate_scoring_report(score_date)
        path = save_report(report, f"scoring_report_{score_date}.txt")
        print(f"\nReport saved to {path}")

    print(f"\nDone. {len(results)} stocks scored. Results in {config.DB_PATH}")
    return results


if __name__ == "__main__":
    main()
