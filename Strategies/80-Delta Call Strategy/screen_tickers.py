"""
Ticker Screening Tool for Options Strategy
===========================================
Screens potential tickers for the 80-delta call strategy based on:

1. Liquid options (OI > 500 on ATM options, spread < 2%)
2. Steady growth (positive 5-year return, Sharpe > 0.5)
3. Lower volatility (annualized vol < 25%)
4. ETF preferred (less idiosyncratic risk)

Candidate tickers to test:
- RSP (Equal-weight S&P 500)
- IWM (Russell 2000)
- DIA (Dow 30)
- VTI (Total US Market)
- XLK (Tech Sector)
- XLF (Financials)
- XLV (Healthcare)

Usage:
    python screen_tickers.py

Output:
- Table of tickers with liquidity scores
- Recommendation of which to backtest
"""

import os
import sys
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_dir = os.path.dirname(os.path.dirname(_this_dir))
sys.path.insert(0, _project_dir)

from backtest.thetadata_client import ThetaDataClient


# ======================================================================
# PARAMETERS
# ======================================================================

# Candidate tickers to screen
CANDIDATES = [
    "SPY",   # Benchmark
    "QQQ",   # Nasdaq-100
    "RSP",   # Equal-weight S&P 500
    "IWM",   # Russell 2000
    "DIA",   # Dow 30
    "VTI",   # Total US Market
    "XLK",   # Technology Sector
    "XLF",   # Financials Sector
    "XLV",   # Healthcare Sector
    "XLE",   # Energy Sector
    "XLI",   # Industrials Sector
    "XLY",   # Consumer Discretionary
]

# Screening criteria
MIN_OI = 500              # Minimum open interest on ATM options
MAX_SPREAD_PCT = 0.02     # Maximum 2% bid-ask spread
MIN_5Y_RETURN = 0.0       # Positive 5-year return required
MIN_SHARPE = 0.5          # Minimum 5-year Sharpe ratio
MAX_VOLATILITY = 0.25     # Maximum 25% annualized volatility

# Data range
DATA_START = "2019-01-01"  # 5 years of history
DATA_END = "2026-01-31"

# For options liquidity check, use a recent trading date
LIQUIDITY_CHECK_DATE = "2025-01-24"  # A known trading day


# ======================================================================
# SCREENING FUNCTIONS
# ======================================================================

def calculate_metrics(bars):
    """Calculate performance metrics from price bars."""
    if len(bars) < 252:
        return None

    closes = np.array([b["close"] for b in bars])
    dates = [b["bar_date"] for b in bars]

    # 5-year total return
    total_return = (closes[-1] / closes[0]) - 1

    # Annualized return
    years = len(bars) / 252.0
    cagr = (closes[-1] / closes[0]) ** (1 / years) - 1 if years > 0 else 0

    # Daily returns
    daily_returns = np.diff(closes) / closes[:-1]

    # Annualized volatility
    annual_vol = np.std(daily_returns, ddof=1) * np.sqrt(252)

    # Sharpe ratio (assuming 4% risk-free rate)
    excess_return = cagr - 0.04
    sharpe = excess_return / annual_vol if annual_vol > 0 else 0

    # Max drawdown
    peak = np.maximum.accumulate(closes)
    drawdown = (closes - peak) / peak
    max_dd = drawdown.min()

    return {
        "start_date": dates[0],
        "end_date": dates[-1],
        "n_bars": len(bars),
        "years": years,
        "start_price": closes[0],
        "end_price": closes[-1],
        "total_return": total_return,
        "cagr": cagr,
        "volatility": annual_vol,
        "sharpe": sharpe,
        "max_dd": max_dd,
    }


def check_options_liquidity(client, ticker, check_date, current_price=None):
    """
    Check options liquidity for a ticker.

    Returns:
        dict with oi, spread_pct, has_options, score
    """
    result = {
        "has_options": False,
        "n_expirations": 0,
        "atm_oi": 0,
        "spread_pct": 1.0,
        "score": 0,
    }

    # Get expirations
    try:
        expirations = client.get_expirations(ticker)
    except Exception as e:
        print(f"    Error getting expirations for {ticker}: {e}")
        return result

    if not expirations:
        return result

    result["has_options"] = True
    result["n_expirations"] = len(expirations)

    # Find a monthly expiration ~30-60 DTE
    check_dt = datetime.strptime(check_date, "%Y-%m-%d").date()
    target_exp = None

    for exp_str in sorted(expirations):
        exp_dt = datetime.strptime(exp_str, "%Y-%m-%d").date()
        dte = (exp_dt - check_dt).days
        if 30 <= dte <= 60:
            # Check if it's a monthly (3rd Friday)
            if exp_dt.weekday() == 4 and 15 <= exp_dt.day <= 21:
                target_exp = exp_str
                break

    if not target_exp:
        # Fall back to any expiration 30-60 DTE
        for exp_str in sorted(expirations):
            exp_dt = datetime.strptime(exp_str, "%Y-%m-%d").date()
            dte = (exp_dt - check_dt).days
            if 30 <= dte <= 60:
                target_exp = exp_str
                break

    if not target_exp:
        return result

    # Get strikes
    try:
        strikes = client.get_strikes(ticker, target_exp)
    except Exception as e:
        print(f"    Error getting strikes for {ticker} {target_exp}: {e}")
        return result

    if not strikes:
        return result

    # Use provided current price or fetch it
    if current_price is None:
        bars = client.fetch_ticker_bars(ticker, check_date, check_date)
        if not bars:
            return result
        current_price = bars[0]["close"]

    atm_strike = min(strikes, key=lambda s: abs(s - current_price))

    # Get EOD data for ATM call
    try:
        eod_data = client.get_option_eod(
            ticker, target_exp, atm_strike, "C",
            check_date, check_date
        )
    except Exception as e:
        print(f"    Error getting EOD for {ticker}: {e}")
        return result

    if not eod_data:
        return result

    eod = eod_data[0]
    bid = eod.get("bid", 0) or 0
    ask = eod.get("ask", 0) or 0
    oi = eod.get("open_interest", 0) or 0

    result["atm_oi"] = oi

    if bid > 0 and ask > 0:
        result["spread_pct"] = (ask - bid) / bid

    # Calculate liquidity score (0-100)
    oi_score = min(100, (oi / MIN_OI) * 50) if oi > 0 else 0
    spread_score = max(0, 50 - (result["spread_pct"] / MAX_SPREAD_PCT) * 50)
    result["score"] = oi_score + spread_score

    return result


def screen_ticker(client, ticker):
    """Screen a single ticker for all criteria."""
    print(f"\n  Screening {ticker}...")

    result = {
        "ticker": ticker,
        "bars_loaded": 0,
        "total_return": None,
        "cagr": None,
        "volatility": None,
        "sharpe": None,
        "max_dd": None,
        "has_options": False,
        "n_expirations": 0,
        "atm_oi": 0,
        "spread_pct": None,
        "liquidity_score": 0,
        "passes_growth": False,
        "passes_sharpe": False,
        "passes_vol": False,
        "passes_liquidity": False,
        "overall_pass": False,
        "recommendation": "SKIP",
    }

    # Load price history
    bars = client.fetch_ticker_bars(ticker, DATA_START, DATA_END)
    if not bars:
        print(f"    No price data for {ticker}")
        return result

    result["bars_loaded"] = len(bars)

    # Calculate performance metrics
    metrics = calculate_metrics(bars)
    if metrics is None:
        print(f"    Insufficient data for {ticker}")
        return result

    result["total_return"] = metrics["total_return"]
    result["cagr"] = metrics["cagr"]
    result["volatility"] = metrics["volatility"]
    result["sharpe"] = metrics["sharpe"]
    result["max_dd"] = metrics["max_dd"]

    # Check growth criteria
    result["passes_growth"] = metrics["total_return"] >= MIN_5Y_RETURN
    result["passes_sharpe"] = metrics["sharpe"] >= MIN_SHARPE
    result["passes_vol"] = metrics["volatility"] <= MAX_VOLATILITY

    # Use the last available date and price for liquidity check
    last_bar = bars[-1]
    check_date = last_bar["bar_date"]
    current_price = last_bar["close"]

    # Check options liquidity
    liquidity = check_options_liquidity(client, ticker, check_date, current_price)
    result["has_options"] = liquidity["has_options"]
    result["n_expirations"] = liquidity["n_expirations"]
    result["atm_oi"] = liquidity["atm_oi"]
    result["spread_pct"] = liquidity["spread_pct"]
    result["liquidity_score"] = liquidity["score"]

    result["passes_liquidity"] = (
        liquidity["has_options"] and
        liquidity["atm_oi"] >= MIN_OI and
        liquidity["spread_pct"] <= MAX_SPREAD_PCT
    )

    # Overall pass
    result["overall_pass"] = (
        result["passes_growth"] and
        result["passes_sharpe"] and
        result["passes_vol"] and
        result["passes_liquidity"]
    )

    # Recommendation
    if result["overall_pass"]:
        result["recommendation"] = "BACKTEST"
    elif result["passes_growth"] and result["passes_liquidity"]:
        result["recommendation"] = "CONSIDER"
    elif result["has_options"]:
        result["recommendation"] = "MARGINAL"
    else:
        result["recommendation"] = "SKIP"

    print(f"    CAGR: {metrics['cagr']:+.1%}, Vol: {metrics['volatility']:.1%}, "
          f"Sharpe: {metrics['sharpe']:.2f}, OI: {liquidity['atm_oi']}")

    return result


def run_screening():
    """Run the ticker screening."""
    print("=" * 80)
    print("Ticker Screening Tool for 80-Delta Call Strategy")
    print("=" * 80)

    print(f"\nScreening criteria:")
    print(f"  - Positive 5-year return: > {MIN_5Y_RETURN:.0%}")
    print(f"  - Sharpe ratio: > {MIN_SHARPE:.2f}")
    print(f"  - Volatility: < {MAX_VOLATILITY:.0%}")
    print(f"  - ATM options OI: > {MIN_OI}")
    print(f"  - Bid-ask spread: < {MAX_SPREAD_PCT:.0%}")

    client = ThetaDataClient()
    if not client.connect():
        print("\nERROR: Cannot connect to Theta Terminal.")
        print("Note: Options liquidity checks require Theta Terminal.")
        print("Proceeding with price metrics only...")

    print(f"\nScreening {len(CANDIDATES)} tickers...")

    results = []
    for ticker in CANDIDATES:
        result = screen_ticker(client, ticker)
        results.append(result)

    client.close()

    # Create results DataFrame
    df = pd.DataFrame(results)

    # Print summary table
    W = 100
    print(f"\n{'=' * W}")
    print("SCREENING RESULTS")
    print(f"{'=' * W}")

    print(f"\n  {'Ticker':<8} {'CAGR':>8} {'Vol':>8} {'Sharpe':>8} "
          f"{'MaxDD':>8} {'OI':>8} {'Spread':>8} {'Score':>8} {'Rec':>10}")
    print(f"  {'-' * 90}")

    for _, row in df.iterrows():
        cagr_str = f"{row['cagr']:+.1%}" if row['cagr'] is not None else "--"
        vol_str = f"{row['volatility']:.1%}" if row['volatility'] is not None else "--"
        sharpe_str = f"{row['sharpe']:.2f}" if row['sharpe'] is not None else "--"
        dd_str = f"{row['max_dd']:.1%}" if row['max_dd'] is not None else "--"
        oi_str = f"{row['atm_oi']:,.0f}" if row['atm_oi'] else "--"
        spread_str = f"{row['spread_pct']:.1%}" if row['spread_pct'] is not None else "--"
        score_str = f"{row['liquidity_score']:.0f}" if row['liquidity_score'] else "--"

        print(f"  {row['ticker']:<8} {cagr_str:>8} {vol_str:>8} {sharpe_str:>8} "
              f"{dd_str:>8} {oi_str:>8} {spread_str:>8} {score_str:>8} {row['recommendation']:>10}")

    # Print detailed pass/fail
    print(f"\n{'=' * W}")
    print("CRITERIA PASS/FAIL")
    print(f"{'=' * W}")

    print(f"\n  {'Ticker':<8} {'Growth':>10} {'Sharpe':>10} {'Vol':>10} "
          f"{'Liquidity':>10} {'Overall':>10}")
    print(f"  {'-' * 60}")

    for _, row in df.iterrows():
        g = "PASS" if row['passes_growth'] else "FAIL"
        s = "PASS" if row['passes_sharpe'] else "FAIL"
        v = "PASS" if row['passes_vol'] else "FAIL"
        l = "PASS" if row['passes_liquidity'] else "FAIL"
        o = "PASS" if row['overall_pass'] else "FAIL"

        print(f"  {row['ticker']:<8} {g:>10} {s:>10} {v:>10} {l:>10} {o:>10}")

    # Recommendations
    print(f"\n{'=' * W}")
    print("RECOMMENDATIONS")
    print(f"{'=' * W}")

    backtest = df[df["recommendation"] == "BACKTEST"]["ticker"].tolist()
    consider = df[df["recommendation"] == "CONSIDER"]["ticker"].tolist()
    marginal = df[df["recommendation"] == "MARGINAL"]["ticker"].tolist()

    print(f"\n  BACKTEST (all criteria pass):")
    if backtest:
        for t in backtest:
            print(f"    - {t}")
    else:
        print("    None")

    print(f"\n  CONSIDER (growth + liquidity pass):")
    if consider:
        for t in consider:
            row = df[df["ticker"] == t].iloc[0]
            reason = []
            if not row["passes_sharpe"]:
                reason.append(f"Sharpe {row['sharpe']:.2f} < {MIN_SHARPE}")
            if not row["passes_vol"]:
                reason.append(f"Vol {row['volatility']:.1%} > {MAX_VOLATILITY:.0%}")
            print(f"    - {t}: {', '.join(reason)}")
    else:
        print("    None")

    print(f"\n  MARGINAL (has options but other issues):")
    if marginal:
        for t in marginal:
            row = df[df["ticker"] == t].iloc[0]
            issues = []
            if not row["passes_growth"]:
                issues.append(f"negative return ({row['total_return']:+.1%})")
            if not row["passes_liquidity"]:
                if row["atm_oi"] < MIN_OI:
                    issues.append(f"low OI ({row['atm_oi']:.0f})")
                if row["spread_pct"] and row["spread_pct"] > MAX_SPREAD_PCT:
                    issues.append(f"wide spread ({row['spread_pct']:.1%})")
            print(f"    - {t}: {', '.join(issues)}")
    else:
        print("    None")

    # Key insights
    print(f"\n{'=' * W}")
    print("KEY INSIGHTS")
    print(f"{'=' * W}")

    # Best overall
    if not df.empty:
        best_sharpe = df.loc[df["sharpe"].idxmax()] if df["sharpe"].notna().any() else None
        best_liquidity = df.loc[df["liquidity_score"].idxmax()] if df["liquidity_score"].notna().any() else None
        lowest_vol = df.loc[df["volatility"].idxmin()] if df["volatility"].notna().any() else None

        if best_sharpe is not None:
            print(f"\n  Best Sharpe ratio: {best_sharpe['ticker']} ({best_sharpe['sharpe']:.2f})")
        if best_liquidity is not None:
            print(f"  Best liquidity: {best_liquidity['ticker']} (score: {best_liquidity['liquidity_score']:.0f})")
        if lowest_vol is not None:
            print(f"  Lowest volatility: {lowest_vol['ticker']} ({lowest_vol['volatility']:.1%})")

    # Save results
    output_file = os.path.join(_this_dir, "ticker_screening_results.csv")
    df.to_csv(output_file, index=False)
    print(f"\n  Results saved to: {output_file}")

    print(f"\n{'=' * 80}")


if __name__ == "__main__":
    run_screening()
