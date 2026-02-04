"""
IBKR Real-Time Option Quote Checker
====================================
Fetches current bid/ask/OI for SPY options from Interactive Brokers.

Requires TWS or IB Gateway running with API enabled.

Usage:
    python ibkr_option_quotes.py --strike 655 --exp 2026-06-18
    python ibkr_option_quotes.py --delta 80 --dte 120
"""

import argparse
import sys
import time
from datetime import datetime, date, timedelta

try:
    from ib_insync import IB, Stock, Option, util
except ImportError:
    print("ERROR: ib_insync not installed. Run: pip install ib_insync")
    sys.exit(1)


# IBKR Connection settings
IB_HOST = "127.0.0.1"
IB_PORT = 7497  # 7497 for TWS paper, 7496 for TWS live
IB_CLIENT_ID = 99


def connect_ibkr():
    """Connect to TWS/Gateway."""
    ib = IB()
    try:
        ib.connect(IB_HOST, IB_PORT, clientId=IB_CLIENT_ID)
        print(f"Connected to IBKR at {IB_HOST}:{IB_PORT}")
        return ib
    except Exception as e:
        print(f"ERROR: Cannot connect to IBKR: {e}")
        print("\nMake sure TWS or IB Gateway is running with API enabled:")
        print("  - TWS: File > Global Configuration > API > Settings")
        print("  - Enable 'Enable ActiveX and Socket Clients'")
        print("  - Port should be 7497 (paper) or 7496 (live)")
        return None


def get_spy_price(ib):
    """Get current SPY price."""
    spy = Stock("SPY", "SMART", "USD")
    ib.qualifyContracts(spy)

    ticker = ib.reqMktData(spy, '', False, False)
    ib.sleep(2)  # Wait for data

    price = ticker.marketPrice()
    ib.cancelMktData(spy)

    return price if price > 0 else None


def get_option_quote(ib, symbol, expiration, strike, right="C"):
    """
    Get real-time quote for an option.

    Args:
        symbol: Underlying symbol (e.g., "SPY")
        expiration: Expiration date as "YYYYMMDD" or "YYYY-MM-DD"
        strike: Strike price
        right: "C" for call, "P" for put

    Returns:
        dict with bid, ask, last, volume, open_interest
    """
    # Format expiration
    if "-" in str(expiration):
        exp_str = expiration.replace("-", "")
    else:
        exp_str = str(expiration)

    opt = Option(symbol, exp_str, strike, right, "SMART")

    try:
        ib.qualifyContracts(opt)
    except Exception as e:
        return {"error": f"Invalid contract: {e}"}

    # Request market data
    ticker = ib.reqMktData(opt, '', False, False)
    ib.sleep(2)  # Wait for data

    # Get snapshot data
    result = {
        "symbol": symbol,
        "expiration": expiration,
        "strike": strike,
        "right": right,
        "bid": ticker.bid if ticker.bid > 0 else None,
        "ask": ticker.ask if ticker.ask > 0 else None,
        "last": ticker.last if ticker.last > 0 else None,
        "volume": ticker.volume if ticker.volume >= 0 else None,
        "open_interest": None,  # Need fundamental data request for OI
    }

    # Calculate spread
    if result["bid"] and result["ask"]:
        result["spread"] = result["ask"] - result["bid"]
        result["mid"] = (result["bid"] + result["ask"]) / 2
        result["spread_pct"] = result["spread"] / result["mid"] * 100

    ib.cancelMktData(opt)

    return result


def get_option_chain_snapshot(ib, symbol, expiration, strikes):
    """Get quotes for multiple strikes."""
    results = []

    for strike in strikes:
        quote = get_option_quote(ib, symbol, expiration, strike, "C")
        results.append(quote)
        time.sleep(0.5)  # Rate limiting

    return results


def find_strike_for_delta(spot, delta, dte, iv=0.16, rate=0.045):
    """Estimate strike for target delta using simplified Black-Scholes."""
    import math

    t = dte / 365.0
    d1_target = 0.0  # We need to back out K from delta

    # For call: delta ≈ N(d1)
    # Invert to find d1 from delta
    from scipy.stats import norm
    d1 = norm.ppf(delta)

    # d1 = (ln(S/K) + (r + σ²/2)t) / (σ√t)
    # Solve for K:
    # ln(S/K) = d1 * σ√t - (r + σ²/2)t
    # S/K = exp(d1 * σ√t - (r + σ²/2)t)
    # K = S / exp(...)

    sqrt_t = math.sqrt(t)
    exponent = d1 * iv * sqrt_t - (rate + 0.5 * iv * iv) * t
    K = spot / math.exp(exponent)

    return K


def main():
    parser = argparse.ArgumentParser(description="IBKR Option Quote Checker")
    parser.add_argument("--symbol", default="SPY", help="Underlying symbol")
    parser.add_argument("--strike", type=float, help="Strike price")
    parser.add_argument("--exp", help="Expiration (YYYY-MM-DD)")
    parser.add_argument("--delta", type=int, help="Target delta (e.g., 80 for 0.80)")
    parser.add_argument("--dte", type=int, default=120, help="Days to expiration")
    parser.add_argument("--scan", action="store_true", help="Scan multiple strikes")

    args = parser.parse_args()

    print("=" * 70)
    print("IBKR Option Quote Checker")
    print("=" * 70)

    ib = connect_ibkr()
    if not ib:
        return

    try:
        # Get SPY price
        spot = get_spy_price(ib)
        if spot:
            print(f"\n{args.symbol} Current Price: ${spot:.2f}")
        else:
            print(f"\nWARNING: Could not get {args.symbol} price")
            spot = 695  # Fallback

        # Determine expiration
        if args.exp:
            expiration = args.exp
        else:
            # Find expiration ~DTE days out
            target_date = date.today() + timedelta(days=args.dte)
            # Round to 3rd Friday (monthly opex)
            while target_date.weekday() != 4 or not (15 <= target_date.day <= 21):
                target_date += timedelta(days=1)
            expiration = target_date.strftime("%Y-%m-%d")

        dte = (datetime.strptime(expiration, "%Y-%m-%d").date() - date.today()).days
        print(f"Expiration: {expiration} ({dte} DTE)")

        # Determine strikes to check
        if args.strike:
            strikes = [args.strike]
        elif args.delta:
            target_delta = args.delta / 100.0
            est_strike = find_strike_for_delta(spot, target_delta, dte)
            # Round to $5 increments
            center_strike = round(est_strike / 5) * 5
            strikes = [center_strike - 10, center_strike - 5, center_strike,
                      center_strike + 5, center_strike + 10]
            print(f"\nTarget delta: {target_delta:.2f}, estimated strike: ${center_strike}")
        else:
            # Default: check around ATM
            center = round(spot / 5) * 5
            strikes = list(range(int(center - 30), int(center + 35), 5))

        print(f"\nChecking {len(strikes)} strikes...")
        print()

        # Get quotes
        if args.scan or len(strikes) > 1:
            print(f"{'Strike':>8} {'Bid':>8} {'Ask':>8} {'Spread':>8} {'Sprd%':>7} {'Volume':>8}")
            print("-" * 60)

            for strike in strikes:
                quote = get_option_quote(ib, args.symbol, expiration, strike, "C")

                if "error" in quote:
                    print(f"${strike:>7.0f}  {quote['error']}")
                    continue

                bid = quote.get('bid')
                ask = quote.get('ask')
                spread = quote.get('spread')
                spread_pct = quote.get('spread_pct')
                volume = quote.get('volume')

                bid_str = f"${bid:.2f}" if bid else "N/A"
                ask_str = f"${ask:.2f}" if ask else "N/A"
                spread_str = f"${spread:.2f}" if spread else "N/A"
                spct_str = f"{spread_pct:.1f}%" if spread_pct else "N/A"
                vol_str = f"{volume:,}" if volume is not None else "N/A"

                # Liquidity marker
                marker = ""
                if spread_pct and spread_pct <= 1.0:
                    marker = " <-- TIGHT"
                elif spread_pct and spread_pct <= 2.0:
                    marker = " <-- OK"

                print(f"${strike:>7.0f} {bid_str:>8} {ask_str:>8} {spread_str:>8} {spct_str:>7} {vol_str:>8}{marker}")

                time.sleep(0.3)
        else:
            quote = get_option_quote(ib, args.symbol, expiration, strikes[0], "C")
            print(f"\nQuote for {args.symbol} ${strikes[0]} Call exp {expiration}:")
            for k, v in quote.items():
                if v is not None:
                    print(f"  {k}: {v}")

        print()

    finally:
        ib.disconnect()
        print("Disconnected from IBKR")


if __name__ == "__main__":
    main()
