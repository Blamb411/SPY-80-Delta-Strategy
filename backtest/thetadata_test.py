#!/usr/bin/env python3
"""
ThetaData API Test Script
=========================

Tests connectivity and data retrieval from ThetaData's REST API.
Requires the Theta Terminal application to be running locally.

Prerequisites:
1. Create a free account at https://www.thetadata.net/
2. Download and install Theta Terminal from https://www.thetadata.net/terminal
3. Launch Theta Terminal and log in with your credentials
4. Run this script

Free tier provides:
- 1 year of historical EOD data (from 2023-06-01)
- 30 requests per minute rate limit
- Real-time quotes with 15-minute delay
"""

import sys
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, List, Any
import time

# Check for required packages
try:
    import requests
    import pandas as pd
except ImportError as e:
    print("Missing required packages. Install with:")
    print("  pip install requests pandas")
    sys.exit(1)


# ThetaData REST API base URL (Theta Terminal must be running)
BASE_URL = "http://localhost:25510"  # Default port for Theta Terminal v3

# Alternative ports to try if default doesn't work
ALTERNATE_PORTS = [25510, 25503, 25511]


class ThetaDataClient:
    """Simple client for ThetaData REST API."""

    def __init__(self, base_url: str = None):
        self.base_url = base_url
        self.session = requests.Session()

    def find_active_port(self) -> Optional[str]:
        """Find the port where Theta Terminal is running."""
        for port in ALTERNATE_PORTS:
            url = f"http://localhost:{port}"
            try:
                response = self.session.get(f"{url}/v2/system/terminal/status", timeout=2)
                if response.status_code == 200:
                    print(f"Found Theta Terminal on port {port}")
                    return url
            except requests.exceptions.ConnectionError:
                continue
        return None

    def connect(self) -> bool:
        """Test connection to Theta Terminal."""
        if self.base_url is None:
            self.base_url = self.find_active_port()

        if self.base_url is None:
            print("\nERROR: Could not connect to Theta Terminal.")
            print("\nPlease ensure:")
            print("  1. You have downloaded Theta Terminal from https://www.thetadata.net/terminal")
            print("  2. Theta Terminal is running")
            print("  3. You are logged in with your ThetaData credentials")
            return False

        try:
            response = self.session.get(f"{self.base_url}/v2/system/terminal/status", timeout=5)
            if response.status_code == 200:
                print(f"Connected to Theta Terminal at {self.base_url}")
                return True
        except Exception as e:
            print(f"Connection error: {e}")

        return False

    def get_option_expirations(self, root: str) -> Optional[List[str]]:
        """Get available expiration dates for an option root."""
        try:
            response = self.session.get(
                f"{self.base_url}/v2/list/expirations",
                params={"root": root},
                timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                if "response" in data:
                    # Convert YYYYMMDD to YYYY-MM-DD format
                    expirations = []
                    for exp in data["response"]:
                        if len(str(exp)) == 8:
                            exp_str = str(exp)
                            formatted = f"{exp_str[:4]}-{exp_str[4:6]}-{exp_str[6:]}"
                            expirations.append(formatted)
                    return expirations
            else:
                print(f"Error getting expirations: {response.status_code}")
                print(response.text)
        except Exception as e:
            print(f"Error: {e}")
        return None

    def get_option_strikes(self, root: str, expiration: str) -> Optional[List[float]]:
        """Get available strikes for an option root and expiration."""
        # Convert YYYY-MM-DD to YYYYMMDD
        exp_int = int(expiration.replace("-", ""))
        try:
            response = self.session.get(
                f"{self.base_url}/v2/list/strikes",
                params={"root": root, "exp": exp_int},
                timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                if "response" in data:
                    # Strikes are in cents, convert to dollars
                    strikes = [s / 1000 for s in data["response"]]
                    return strikes
            else:
                print(f"Error getting strikes: {response.status_code}")
        except Exception as e:
            print(f"Error: {e}")
        return None

    def get_option_eod(
        self,
        root: str,
        expiration: str,
        strike: float,
        right: str,  # 'C' or 'P'
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        Get end-of-day option data including OHLC and Greeks.

        Returns DataFrame with columns:
        - date, open, high, low, close, volume, open_interest
        - bid, ask (if available)
        """
        # Convert formats
        exp_int = int(expiration.replace("-", ""))
        strike_int = int(strike * 1000)  # Convert to millicents
        start_int = int(start_date.replace("-", ""))
        end_int = int(end_date.replace("-", ""))

        try:
            response = self.session.get(
                f"{self.base_url}/v2/hist/option/eod",
                params={
                    "root": root,
                    "exp": exp_int,
                    "strike": strike_int,
                    "right": right,
                    "start_date": start_int,
                    "end_date": end_int
                },
                timeout=30
            )

            if response.status_code == 200:
                data = response.json()
                if "response" in data and data["response"]:
                    # Parse the response
                    header = data.get("header", {})
                    format_cols = header.get("format", [])

                    rows = data["response"]
                    df = pd.DataFrame(rows, columns=format_cols if format_cols else None)

                    # Rename columns to standard names if needed
                    col_mapping = {
                        "ms_of_day": "time_ms",
                        "open": "open",
                        "high": "high",
                        "low": "low",
                        "close": "close",
                        "volume": "volume",
                        "count": "trade_count",
                        "date": "date"
                    }

                    # Convert date column from YYYYMMDD to datetime
                    if "date" in df.columns:
                        df["date"] = pd.to_datetime(df["date"].astype(str), format="%Y%m%d")

                    return df
            else:
                print(f"Error getting EOD data: {response.status_code}")
                print(response.text[:500])

        except Exception as e:
            print(f"Error fetching EOD data: {e}")

        return None

    def get_option_quote_eod(
        self,
        root: str,
        expiration: str,
        strike: float,
        right: str,
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        Get end-of-day bid/ask quotes for an option.

        This is what we need for validating our backtest assumptions!
        """
        exp_int = int(expiration.replace("-", ""))
        strike_int = int(strike * 1000)
        start_int = int(start_date.replace("-", ""))
        end_int = int(end_date.replace("-", ""))

        try:
            response = self.session.get(
                f"{self.base_url}/v2/hist/option/quote",
                params={
                    "root": root,
                    "exp": exp_int,
                    "strike": strike_int,
                    "right": right,
                    "start_date": start_int,
                    "end_date": end_int,
                    "ivl": 0  # 0 = EOD snapshot
                },
                timeout=30
            )

            if response.status_code == 200:
                data = response.json()
                if "response" in data and data["response"]:
                    header = data.get("header", {})
                    format_cols = header.get("format", [])
                    rows = data["response"]
                    df = pd.DataFrame(rows, columns=format_cols if format_cols else None)

                    if "date" in df.columns:
                        df["date"] = pd.to_datetime(df["date"].astype(str), format="%Y%m%d")

                    return df
            else:
                print(f"Error getting quote data: {response.status_code}")
                print(response.text[:500])

        except Exception as e:
            print(f"Error fetching quote data: {e}")

        return None

    def get_option_greeks_eod(
        self,
        root: str,
        expiration: str,
        strike: float,
        right: str,
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """Get end-of-day Greeks and IV for an option."""
        exp_int = int(expiration.replace("-", ""))
        strike_int = int(strike * 1000)
        start_int = int(start_date.replace("-", ""))
        end_int = int(end_date.replace("-", ""))

        try:
            response = self.session.get(
                f"{self.base_url}/v2/hist/option/greeks",
                params={
                    "root": root,
                    "exp": exp_int,
                    "strike": strike_int,
                    "right": right,
                    "start_date": start_int,
                    "end_date": end_int,
                    "ivl": 0
                },
                timeout=30
            )

            if response.status_code == 200:
                data = response.json()
                if "response" in data and data["response"]:
                    header = data.get("header", {})
                    format_cols = header.get("format", [])
                    rows = data["response"]
                    df = pd.DataFrame(rows, columns=format_cols if format_cols else None)

                    if "date" in df.columns:
                        df["date"] = pd.to_datetime(df["date"].astype(str), format="%Y%m%d")

                    return df
            else:
                print(f"Error getting Greeks: {response.status_code}")

        except Exception as e:
            print(f"Error fetching Greeks: {e}")

        return None


def test_basic_connectivity(client: ThetaDataClient) -> bool:
    """Test basic API connectivity."""
    print("\n" + "=" * 60)
    print("TEST 1: Basic Connectivity")
    print("=" * 60)

    if not client.connect():
        return False

    print("SUCCESS: Connected to Theta Terminal")
    return True


def test_get_expirations(client: ThetaDataClient, symbol: str = "SPY") -> Optional[List[str]]:
    """Test getting option expirations."""
    print("\n" + "=" * 60)
    print(f"TEST 2: Get Option Expirations for {symbol}")
    print("=" * 60)

    expirations = client.get_option_expirations(symbol)

    if expirations is None or len(expirations) == 0:
        print("FAILED: Could not retrieve expirations")
        return None

    print(f"SUCCESS: Found {len(expirations)} expirations")
    print(f"Next 5 expirations: {expirations[:5]}")
    return expirations


def test_get_strikes(client: ThetaDataClient, symbol: str, expiration: str) -> Optional[List[float]]:
    """Test getting option strikes."""
    print("\n" + "=" * 60)
    print(f"TEST 3: Get Strikes for {symbol} {expiration}")
    print("=" * 60)

    strikes = client.get_option_strikes(symbol, expiration)

    if strikes is None or len(strikes) == 0:
        print("FAILED: Could not retrieve strikes")
        return None

    print(f"SUCCESS: Found {len(strikes)} strikes")
    print(f"Strike range: ${min(strikes):.2f} - ${max(strikes):.2f}")

    # Find ATM strikes (assuming SPY is ~500-600 range)
    atm_strikes = [s for s in strikes if 480 <= s <= 520]
    print(f"Near-ATM strikes: {atm_strikes[:10]}")

    return strikes


def test_get_eod_data(
    client: ThetaDataClient,
    symbol: str,
    expiration: str,
    strike: float,
    right: str = "P"
) -> Optional[pd.DataFrame]:
    """Test getting EOD OHLC data."""
    print("\n" + "=" * 60)
    print(f"TEST 4: Get EOD Data for {symbol} {expiration} ${strike} {right}")
    print("=" * 60)

    # Get last 30 days of data
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

    df = client.get_option_eod(symbol, expiration, strike, right, start_date, end_date)

    if df is None or df.empty:
        print("FAILED: Could not retrieve EOD data")
        return None

    print(f"SUCCESS: Retrieved {len(df)} rows")
    print(f"\nColumns: {list(df.columns)}")
    print(f"\nSample data:")
    print(df.head(10).to_string())

    return df


def test_get_quote_data(
    client: ThetaDataClient,
    symbol: str,
    expiration: str,
    strike: float,
    right: str = "P"
) -> Optional[pd.DataFrame]:
    """Test getting EOD bid/ask quote data - THE KEY DATA FOR BACKTEST VALIDATION."""
    print("\n" + "=" * 60)
    print(f"TEST 5: Get BID/ASK Quotes for {symbol} {expiration} ${strike} {right}")
    print("=" * 60)

    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

    df = client.get_option_quote_eod(symbol, expiration, strike, right, start_date, end_date)

    if df is None or df.empty:
        print("FAILED: Could not retrieve quote data")
        print("Note: Quote data may require a paid subscription")
        return None

    print(f"SUCCESS: Retrieved {len(df)} rows")
    print(f"\nColumns: {list(df.columns)}")
    print(f"\nSample data:")
    print(df.head(10).to_string())

    # Calculate bid-ask spread if available
    if "bid" in df.columns and "ask" in df.columns:
        df["spread"] = df["ask"] - df["bid"]
        df["spread_pct"] = (df["spread"] / ((df["bid"] + df["ask"]) / 2)) * 100
        print(f"\nBid-Ask Spread Analysis:")
        print(f"  Average spread: ${df['spread'].mean():.4f}")
        print(f"  Average spread %: {df['spread_pct'].mean():.2f}%")
        print(f"  This compares to our backtest assumption of 1-5%")

    return df


def test_get_greeks(
    client: ThetaDataClient,
    symbol: str,
    expiration: str,
    strike: float,
    right: str = "P"
) -> Optional[pd.DataFrame]:
    """Test getting Greeks and IV data."""
    print("\n" + "=" * 60)
    print(f"TEST 6: Get Greeks/IV for {symbol} {expiration} ${strike} {right}")
    print("=" * 60)

    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

    df = client.get_option_greeks_eod(symbol, expiration, strike, right, start_date, end_date)

    if df is None or df.empty:
        print("FAILED: Could not retrieve Greeks data")
        return None

    print(f"SUCCESS: Retrieved {len(df)} rows")
    print(f"\nColumns: {list(df.columns)}")
    print(f"\nSample data:")
    print(df.head(10).to_string())

    return df


def main():
    print("=" * 60)
    print("ThetaData API Test Script")
    print("=" * 60)
    print()
    print("This script tests connectivity to ThetaData's REST API")
    print("and retrieves sample options data for SPY.")
    print()
    print("Prerequisites:")
    print("  1. ThetaData account (free tier works)")
    print("  2. Theta Terminal running and logged in")
    print()

    # Initialize client
    client = ThetaDataClient()

    # Test 1: Basic connectivity
    if not test_basic_connectivity(client):
        print("\n" + "=" * 60)
        print("SETUP INSTRUCTIONS")
        print("=" * 60)
        print()
        print("1. Go to https://www.thetadata.net/ and create a free account")
        print("2. Download Theta Terminal from https://www.thetadata.net/terminal")
        print("3. Install and launch Theta Terminal")
        print("4. Log in with your ThetaData credentials")
        print("5. Wait for 'Connected' status in Theta Terminal")
        print("6. Re-run this script")
        return

    # Test 2: Get expirations
    expirations = test_get_expirations(client, "SPY")
    if not expirations:
        return

    # Pick an expiration ~30 days out
    target_date = datetime.now() + timedelta(days=30)
    nearest_exp = None
    for exp in expirations:
        exp_date = datetime.strptime(exp, "%Y-%m-%d")
        if exp_date >= target_date:
            nearest_exp = exp
            break

    if not nearest_exp:
        nearest_exp = expirations[0]

    print(f"\nUsing expiration: {nearest_exp}")

    # Test 3: Get strikes
    strikes = test_get_strikes(client, "SPY", nearest_exp)
    if not strikes:
        return

    # Pick an OTM put strike (~25 delta, roughly 5% below ATM)
    # Assuming SPY is around 500
    target_strike = 480  # Adjust based on current SPY price
    nearest_strike = min(strikes, key=lambda x: abs(x - target_strike))
    print(f"\nUsing strike: ${nearest_strike}")

    # Test 4: Get EOD OHLC data
    eod_df = test_get_eod_data(client, "SPY", nearest_exp, nearest_strike, "P")

    # Test 5: Get bid/ask quotes (KEY for backtest validation)
    quote_df = test_get_quote_data(client, "SPY", nearest_exp, nearest_strike, "P")

    # Test 6: Get Greeks
    greeks_df = test_get_greeks(client, "SPY", nearest_exp, nearest_strike, "P")

    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    print()
    print(f"Connectivity:     {'PASS' if client.base_url else 'FAIL'}")
    print(f"Expirations:      {'PASS' if expirations else 'FAIL'}")
    print(f"Strikes:          {'PASS' if strikes else 'FAIL'}")
    print(f"EOD OHLC Data:    {'PASS' if eod_df is not None else 'FAIL'}")
    print(f"Bid/Ask Quotes:   {'PASS' if quote_df is not None else 'FAIL (may need paid tier)'}")
    print(f"Greeks/IV:        {'PASS' if greeks_df is not None else 'FAIL'}")

    print("\n" + "=" * 60)
    print("NEXT STEPS")
    print("=" * 60)
    print()

    if quote_df is not None:
        print("SUCCESS! ThetaData provides the bid/ask quote data needed")
        print("for rigorous backtest validation.")
        print()
        print("To integrate with our backtesting system:")
        print("  1. Fetch historical bid/ask for options in our universe")
        print("  2. Compare actual spreads to our 1-5% assumptions")
        print("  3. Re-run backtests with actual spread data")
    else:
        print("The free tier may not include historical bid/ask quotes.")
        print("Consider upgrading to Value ($25/mo) or Standard ($75/mo)")
        print("tier for full historical quote data access.")

    print()
    print("Data saved to: (none - test only)")
    print("Run with --save flag to export data to CSV")


if __name__ == "__main__":
    main()
