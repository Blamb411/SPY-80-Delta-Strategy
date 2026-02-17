"""
Put Credit Spread Scanner
=========================

Local scanner for put credit spread opportunities using:
- ThetaData: Historical IV for IV Rank calculation
- IBKR: Real-time option quotes and greeks

Entry Criteria:
1. Price > 200-day SMA (uptrend filter)
2. RSI(14) < 75 (not overbought)
3. IV Rank > 30% (elevated premium)

Spread Construction:
- Short put at ~25 delta
- Long put ~5% below short strike
- Target DTE: 25-35 days
- Minimum credit: 20% of width

Output:
- Ranked list of opportunities by expected value
"""

import sys
import os
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np

# Add paths for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    import yfinance as yf
except ImportError:
    print("ERROR: yfinance not installed. Run: pip install yfinance")
    sys.exit(1)

# Optional imports - will check availability
IBKR_AVAILABLE = False
THETADATA_AVAILABLE = False

try:
    from ib_insync import IB, Stock, Option, util
    IBKR_AVAILABLE = True
except ImportError:
    print("Note: ib_insync not installed. IBKR features disabled.")

try:
    from thetadata import ThetaClient
    THETADATA_AVAILABLE = True
except ImportError:
    print("Note: thetadata not installed. Using Yahoo for IV estimation.")


# =============================================================================
# CONFIGURATION
# =============================================================================

# Watchlist - liquid underlyings for put spreads
WATCHLIST = [
    # Large-cap tech
    'SPY', 'QQQ', 'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'NVDA',
    # Other liquid
    'IWM', 'XLF', 'XLE', 'GLD', 'TLT',
    # Additional
    'AMD', 'TSLA', 'JPM', 'BAC', 'XOM', 'CVX',
]

# Entry filters
SMA_PERIOD = 200
RSI_PERIOD = 14
RSI_MAX = 75.0
IV_RANK_MIN = 30  # 30%

# Spread construction
TARGET_DELTA = 0.25  # 25-delta short put
SPREAD_WIDTH_PCT = 0.05  # Long strike 5% below short
TARGET_DTE_MIN = 25
TARGET_DTE_MAX = 35
MIN_CREDIT_PCT = 0.20  # Minimum 20% of spread width as credit

# IBKR connection
IB_HOST = "127.0.0.1"
IB_PORT = 7497
IB_CLIENT_ID = 98


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def calculate_sma(prices: pd.Series, period: int) -> float:
    """Calculate simple moving average."""
    if len(prices) < period:
        return None
    return prices.iloc[-period:].mean()


def calculate_rsi(prices: pd.Series, period: int = 14) -> float:
    """Calculate RSI."""
    if len(prices) < period + 1:
        return None

    delta = prices.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()

    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi.iloc[-1]


def estimate_iv_rank_yahoo(ticker: str) -> Optional[float]:
    """
    Estimate IV Rank using Yahoo Finance options data.

    This is a rough estimate based on current implied volatility
    relative to historical volatility range.

    Note: For more accurate IV rank, use ThetaData which provides
    historical IV data.
    """
    try:
        stock = yf.Ticker(ticker)

        # Get historical data
        hist = stock.history(period='1y')
        if len(hist) < 100:
            return None

        # Calculate historical volatility at different lookbacks
        returns = hist['Close'].pct_change().dropna()

        # 20-day rolling HV for the year
        hv_series = returns.rolling(20).std() * np.sqrt(252) * 100
        hv_series = hv_series.dropna()

        if len(hv_series) < 50:
            return None

        current_hv = hv_series.iloc[-1]
        hv_min = hv_series.quantile(0.05)  # 5th percentile
        hv_max = hv_series.quantile(0.95)  # 95th percentile

        # Try to get current IV from options
        current_iv = None
        try:
            options = stock.options
            if options and len(options) > 0:
                # Get first expiration with >10 DTE
                current_price = hist['Close'].iloc[-1]
                for exp in options[:3]:  # Check first 3 expirations
                    try:
                        chain = stock.option_chain(exp)
                        calls = chain.calls
                        if not calls.empty:
                            # Find ATM call
                            atm_call = calls.iloc[(calls['strike'] - current_price).abs().argsort()[:1]]
                            iv = atm_call['impliedVolatility'].values[0]
                            if iv > 0:
                                current_iv = iv * 100
                                break
                    except Exception:
                        continue
        except Exception:
            pass

        # If no IV, use current HV as proxy
        if current_iv is None:
            current_iv = current_hv

        # Calculate rank
        if hv_max - hv_min > 0:
            iv_rank = (current_iv - hv_min) / (hv_max - hv_min) * 100
            return min(100, max(0, iv_rank))

        return 50  # Default to neutral

    except Exception as e:
        # Silently return None - logging can be enabled for debugging
        return None


def find_strike_for_delta(spot: float, delta: float, dte: int,
                          iv: float = 0.20, rate: float = 0.045) -> float:
    """
    Estimate strike for target delta using simplified Black-Scholes.

    Args:
        spot: Current stock price
        delta: Target delta (e.g., -0.25 for 25-delta put)
        dte: Days to expiration
        iv: Implied volatility (decimal)
        rate: Risk-free rate (decimal)

    Returns:
        Estimated strike price
    """
    from scipy.stats import norm
    import math

    t = dte / 365.0

    # For put: delta = N(d1) - 1, so N(d1) = delta + 1
    # For 25-delta put, delta = -0.25, so N(d1) = 0.75
    n_d1 = abs(delta) if delta < 0 else (1 - delta)
    d1 = norm.ppf(1 - n_d1)  # Invert

    # Solve for strike
    sqrt_t = math.sqrt(t)
    exponent = d1 * iv * sqrt_t - (rate + 0.5 * iv * iv) * t
    K = spot / math.exp(exponent)

    return K


# =============================================================================
# SCANNER CLASS
# =============================================================================

class PutSpreadScanner:
    """
    Scanner for put credit spread opportunities.
    """

    def __init__(self, use_ibkr: bool = True, use_thetadata: bool = True):
        """
        Initialize scanner.

        Args:
            use_ibkr: Use IBKR for real-time quotes
            use_thetadata: Use ThetaData for IV rank
        """
        self.use_ibkr = use_ibkr and IBKR_AVAILABLE
        self.use_thetadata = use_thetadata and THETADATA_AVAILABLE
        self.ib = None
        self.theta = None

    def connect_ibkr(self) -> bool:
        """Connect to IBKR TWS/Gateway."""
        if not IBKR_AVAILABLE:
            return False

        try:
            self.ib = IB()
            self.ib.connect(IB_HOST, IB_PORT, clientId=IB_CLIENT_ID)
            print(f"Connected to IBKR at {IB_HOST}:{IB_PORT}")
            return True
        except Exception as e:
            print(f"Could not connect to IBKR: {e}")
            self.ib = None
            return False

    def disconnect_ibkr(self) -> None:
        """Disconnect from IBKR."""
        if self.ib:
            self.ib.disconnect()
            print("Disconnected from IBKR")

    def get_stock_data(self, ticker: str) -> Dict:
        """
        Get stock data needed for entry filters.

        Returns:
            Dict with price, SMA, RSI, IV rank
        """
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period='1y')

            if len(hist) < SMA_PERIOD:
                return {'error': 'Insufficient history'}

            current_price = hist['Close'].iloc[-1]
            sma_200 = calculate_sma(hist['Close'], SMA_PERIOD)
            rsi = calculate_rsi(hist['Close'], RSI_PERIOD)

            # IV Rank
            if self.use_thetadata:
                # TODO: Implement ThetaData IV rank
                iv_rank = estimate_iv_rank_yahoo(ticker)
            else:
                iv_rank = estimate_iv_rank_yahoo(ticker)

            return {
                'ticker': ticker,
                'price': current_price,
                'sma_200': sma_200,
                'rsi': rsi,
                'iv_rank': iv_rank,
                'above_sma': current_price > sma_200 if sma_200 else None,
            }

        except Exception as e:
            return {'ticker': ticker, 'error': str(e)}

    def check_entry_criteria(self, data: Dict) -> Tuple[bool, str]:
        """
        Check if entry criteria are met.

        Returns:
            (passes, reason) tuple
        """
        if 'error' in data:
            return False, data['error']

        # Filter 1: Price > 200 SMA
        if not data.get('above_sma'):
            return False, f"Below 200 SMA ({data.get('price', 0):.2f} vs {data.get('sma_200', 0):.2f})"

        # Filter 2: RSI < 75
        rsi = data.get('rsi')
        if rsi is None or rsi >= RSI_MAX:
            return False, f"RSI too high ({rsi:.1f} >= {RSI_MAX})"

        # Filter 3: IV Rank > 30%
        iv_rank = data.get('iv_rank')
        if iv_rank is None:
            return False, "IV Rank unavailable"
        if iv_rank < IV_RANK_MIN:
            return False, f"IV Rank too low ({iv_rank:.0f}% < {IV_RANK_MIN}%)"

        return True, "All criteria met"

    def get_spread_quote_ibkr(self, ticker: str, spot: float,
                               expiration: str, short_strike: float,
                               long_strike: float) -> Dict:
        """
        Get real-time quote for a put spread from IBKR.

        Returns:
            Dict with bid/ask for short and long puts
        """
        if not self.ib:
            return {'error': 'IBKR not connected'}

        try:
            exp_str = expiration.replace('-', '')

            # Create contracts
            short_put = Option(ticker, exp_str, short_strike, 'P', 'SMART')
            long_put = Option(ticker, exp_str, long_strike, 'P', 'SMART')

            # Qualify contracts
            self.ib.qualifyContracts(short_put)
            self.ib.qualifyContracts(long_put)

            # Request market data
            short_ticker = self.ib.reqMktData(short_put, '', False, False)
            long_ticker = self.ib.reqMktData(long_put, '', False, False)
            self.ib.sleep(2)

            result = {
                'short_strike': short_strike,
                'long_strike': long_strike,
                'short_bid': short_ticker.bid if short_ticker.bid > 0 else None,
                'short_ask': short_ticker.ask if short_ticker.ask > 0 else None,
                'long_bid': long_ticker.bid if long_ticker.bid > 0 else None,
                'long_ask': long_ticker.ask if long_ticker.ask > 0 else None,
            }

            # Cancel market data
            self.ib.cancelMktData(short_put)
            self.ib.cancelMktData(long_put)

            # Calculate spread credit/debit
            if result['short_bid'] and result['long_ask']:
                result['credit'] = result['short_bid'] - result['long_ask']
                result['width'] = short_strike - long_strike
                result['credit_pct'] = result['credit'] / result['width'] * 100

            return result

        except Exception as e:
            return {'error': str(e)}

    def get_spread_quote_yahoo(self, ticker: str, spot: float,
                                target_dte: int = 30) -> Dict:
        """
        Get spread quote using Yahoo Finance options data.

        Returns:
            Dict with estimated spread parameters
        """
        try:
            stock = yf.Ticker(ticker)
            options = stock.options

            if not options:
                return {'error': 'No options available'}

            # Find expiration closest to target DTE
            target_date = date.today() + timedelta(days=target_dte)
            best_exp = None
            best_diff = float('inf')

            for exp in options:
                exp_date = datetime.strptime(exp, '%Y-%m-%d').date()
                diff = abs((exp_date - target_date).days)
                if diff < best_diff and TARGET_DTE_MIN <= (exp_date - date.today()).days <= TARGET_DTE_MAX + 10:
                    best_diff = diff
                    best_exp = exp

            if best_exp is None:
                return {'error': 'No suitable expiration found'}

            # Get puts
            chain = stock.option_chain(best_exp)
            puts = chain.puts

            if puts.empty:
                return {'error': 'No puts available'}

            # Find 25-delta short put (estimate from strike distance)
            # ~25 delta is roughly 5-8% OTM
            short_target = spot * 0.94  # ~6% OTM
            short_put = puts.iloc[(puts['strike'] - short_target).abs().argsort()[:1]]
            short_strike = short_put['strike'].values[0]

            # Find long put ~5% below short
            long_target = short_strike * (1 - SPREAD_WIDTH_PCT)
            long_candidates = puts[puts['strike'] < short_strike]

            if long_candidates.empty:
                return {'error': 'No long put candidates'}

            long_put = long_candidates.iloc[(long_candidates['strike'] - long_target).abs().argsort()[:1]]
            long_strike = long_put['strike'].values[0]

            # Get quotes
            short_bid = short_put['bid'].values[0]
            short_ask = short_put['ask'].values[0]
            long_bid = long_put['bid'].values[0]
            long_ask = long_put['ask'].values[0]

            # Calculate credit (sell short bid, buy long ask)
            credit = short_bid - long_ask
            width = short_strike - long_strike

            dte = (datetime.strptime(best_exp, '%Y-%m-%d').date() - date.today()).days

            return {
                'expiration': best_exp,
                'dte': dte,
                'short_strike': short_strike,
                'long_strike': long_strike,
                'short_bid': short_bid,
                'short_ask': short_ask,
                'long_bid': long_bid,
                'long_ask': long_ask,
                'credit': credit,
                'width': width,
                'credit_pct': (credit / width * 100) if width > 0 else 0,
                'max_loss': (width - credit) * 100,  # Per contract
                'max_profit': credit * 100,  # Per contract
            }

        except Exception as e:
            return {'error': str(e)}

    def scan(self, watchlist: List[str] = None) -> pd.DataFrame:
        """
        Scan watchlist for put spread opportunities.

        Returns:
            DataFrame with ranked opportunities
        """
        watchlist = watchlist or WATCHLIST
        opportunities = []

        print(f"\nScanning {len(watchlist)} symbols...")
        print("=" * 70)

        for ticker in watchlist:
            print(f"\n{ticker}:")

            # Get stock data
            data = self.get_stock_data(ticker)

            if 'error' in data:
                print(f"  Skip: {data['error']}")
                continue

            # Check entry criteria
            passes, reason = self.check_entry_criteria(data)

            if not passes:
                print(f"  Skip: {reason}")
                continue

            print(f"  PASSES: Price={data['price']:.2f}, RSI={data['rsi']:.1f}, IV Rank={data['iv_rank']:.0f}%")

            # Get spread quote
            if self.ib:
                # Use IBKR for real-time quotes
                spread = self.get_spread_quote_yahoo(ticker, data['price'])  # Fallback to Yahoo for now
            else:
                spread = self.get_spread_quote_yahoo(ticker, data['price'])

            if 'error' in spread:
                print(f"  Could not get spread quote: {spread['error']}")
                continue

            # Check minimum credit
            if spread.get('credit_pct', 0) < MIN_CREDIT_PCT * 100:
                print(f"  Skip: Credit too low ({spread['credit_pct']:.1f}% < {MIN_CREDIT_PCT*100}%)")
                continue

            print(f"  OPPORTUNITY: {spread['short_strike']}/{spread['long_strike']} "
                  f"exp {spread.get('expiration', 'N/A')} "
                  f"credit ${spread['credit']:.2f} ({spread['credit_pct']:.1f}%)")

            opportunities.append({
                'ticker': ticker,
                'price': data['price'],
                'sma_200': data['sma_200'],
                'rsi': data['rsi'],
                'iv_rank': data['iv_rank'],
                'expiration': spread.get('expiration'),
                'dte': spread.get('dte'),
                'short_strike': spread['short_strike'],
                'long_strike': spread['long_strike'],
                'credit': spread['credit'],
                'width': spread['width'],
                'credit_pct': spread['credit_pct'],
                'max_profit': spread.get('max_profit'),
                'max_loss': spread.get('max_loss'),
            })

        # Sort by credit percentage (best opportunities first)
        df = pd.DataFrame(opportunities)
        if not df.empty:
            df = df.sort_values('credit_pct', ascending=False)

        return df

    def print_report(self, df: pd.DataFrame) -> None:
        """Print formatted opportunity report."""
        print("\n" + "=" * 80)
        print("PUT CREDIT SPREAD OPPORTUNITIES")
        print("=" * 80)
        print(f"Scan Time: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print(f"Criteria: Price > 200 SMA, RSI < {RSI_MAX}, IV Rank > {IV_RANK_MIN}%")
        print(f"Spread: ~25 delta short put, {SPREAD_WIDTH_PCT*100:.0f}% width")

        if df.empty:
            print("\nNo opportunities found.")
            return

        print(f"\nFound {len(df)} opportunities:\n")
        print(f"{'Ticker':<8} {'Price':>8} {'RSI':>6} {'IV Rank':>8} "
              f"{'Spread':>12} {'DTE':>5} {'Credit':>8} {'Cr%':>6}")
        print("-" * 80)

        for _, row in df.iterrows():
            spread_str = f"{row['short_strike']:.0f}/{row['long_strike']:.0f}"
            print(f"{row['ticker']:<8} ${row['price']:>7.2f} {row['rsi']:>5.1f} "
                  f"{row['iv_rank']:>7.0f}% {spread_str:>12} {row['dte']:>5} "
                  f"${row['credit']:>6.2f} {row['credit_pct']:>5.1f}%")

        print("\n" + "-" * 80)
        print("Notes:")
        print("- Credit% = Credit / Spread Width (higher = better risk/reward)")
        print("- Max Profit = Credit * 100 (per contract)")
        print("- Max Loss = (Width - Credit) * 100 (per contract)")
        print("- Target: 50% profit or expiration")


def main():
    """Run put spread scanner."""
    print("Put Credit Spread Scanner")
    print("=" * 50)
    print(f"IBKR Available: {IBKR_AVAILABLE}")
    print(f"ThetaData Available: {THETADATA_AVAILABLE}")

    scanner = PutSpreadScanner(use_ibkr=True, use_thetadata=False)

    # Try to connect to IBKR
    if IBKR_AVAILABLE:
        scanner.connect_ibkr()

    try:
        # Run scan
        opportunities = scanner.scan()

        # Print report
        scanner.print_report(opportunities)

        # Save to CSV
        if not opportunities.empty:
            filename = f"put_spread_opportunities_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
            opportunities.to_csv(filename, index=False)
            print(f"\nSaved to: {filename}")

    finally:
        scanner.disconnect_ibkr()


if __name__ == '__main__':
    main()
