"""
SMA-Triggered Put SELLING Strategy
If buying puts at SMA breaks loses 79c on the dollar,
be the seller and collect that premium.

Rigorous backtest: sell SPY puts when Price/SMA < threshold,
close when Price > SMA or at expiration.
Tests naked puts and put credit spreads.
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
import yfinance as yf
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

SEP = "=" * 70

# Load data
spy = yf.download('SPY', start='2010-01-01', end='2026-03-20', progress=False)
if isinstance(spy.columns, pd.MultiIndex):
    spy.columns = spy.columns.get_level_values(0)
close = spy['Close'].dropna()
sma200 = close.rolling(200).mean()
ratio = close / sma200

# Also get VIX for IV-aware premium estimation
vix = yf.download('^VIX', start='2010-01-01', end='2026-03-20', progress=False)
if isinstance(vix.columns, pd.MultiIndex):
    vix.columns = vix.columns.get_level_values(0)
vix_close = vix['Close'].dropna().reindex(close.index, method='ffill')

valid_idx = ratio.dropna().index
print(SEP)
print("SMA-TRIGGERED PUT SELLING STRATEGY")
print(f"SPY data: {len(valid_idx)} trading days (2010-2026)")
print(SEP)


def estimate_put_premium(spot, strike, vix_level, dte_days=63):
    """Estimate put premium using simplified Black-Scholes approximation.
    Uses VIX as IV proxy, scaled for moneyness."""
    from math import log, sqrt, exp
    try:
        iv = max(vix_level / 100, 0.10)
        t = dte_days / 365
        # Simplified BS put: use normal approximation
        d1 = (log(spot / strike) + (0.05 + iv**2 / 2) * t) / (iv * sqrt(t))
        d2 = d1 - iv * sqrt(t)
        # Normal CDF approximation
        def norm_cdf(x):
            return 0.5 * (1 + np.sign(x) * (1 - np.exp(-2 * x**2 / np.pi))**0.5)
        put = strike * exp(-0.05 * t) * norm_cdf(-d2) - spot * norm_cdf(-d1)
        return max(put, 0.01)
    except:
        return spot * 0.02  # fallback


def run_put_selling_backtest(threshold, otm_pct, use_spread=False,
                              spread_width_pct=0.05, starting_capital=100000,
                              risk_per_trade_pct=0.05, max_contracts=10):
    """
    Backtest selling puts when Price/SMA < threshold.

    Parameters:
    - threshold: e.g., 0.98 means sell when price is 2% below SMA
    - otm_pct: how far OTM the short put is (0 = ATM, 0.05 = 5% OTM)
    - use_spread: if True, also buy a put further OTM to cap risk
    - spread_width_pct: width of the spread as % of spot
    - starting_capital: initial account value
    - risk_per_trade_pct: max risk per trade as % of account
    """
    equity = starting_capital
    peak_equity = equity
    max_dd = 0
    trades = []
    equity_curve = []

    in_trade = False
    entry_info = None

    for i, dt in enumerate(valid_idx):
        r = ratio[dt]
        price = float(close[dt])
        v = float(vix_close[dt]) if dt in vix_close.index and not np.isnan(vix_close[dt]) else 20

        # Track equity curve
        equity_curve.append({'date': dt, 'equity': equity})

        if not in_trade and r < threshold:
            # SELL PUT (or put spread)
            short_strike = round(price * (1 - otm_pct), 0)
            premium = estimate_put_premium(price, short_strike, v, 63)

            if use_spread:
                long_strike = round(price * (1 - otm_pct - spread_width_pct), 0)
                long_premium = estimate_put_premium(price, long_strike, v, 63)
                net_credit = premium - long_premium
                max_loss_per_share = (short_strike - long_strike) - net_credit
                max_loss_per_share = max(max_loss_per_share, 0.01)
            else:
                net_credit = premium
                long_strike = 0
                # Naked put max loss = strike - premium (if stock goes to 0)
                # Use a practical max loss estimate of 20% of strike
                max_loss_per_share = short_strike * 0.20

            # Position sizing
            trade_budget = equity * risk_per_trade_pct
            contracts = min(int(trade_budget / (max_loss_per_share * 100)), max_contracts)
            contracts = max(contracts, 1)

            in_trade = True
            entry_info = {
                'entry_date': dt,
                'entry_price': price,
                'entry_vix': v,
                'short_strike': short_strike,
                'long_strike': long_strike,
                'credit': net_credit,
                'contracts': contracts,
                'expiry_idx': i + 63,
                'max_loss': max_loss_per_share * 100 * contracts,
            }

        elif in_trade:
            exit_trade = False
            exit_reason = None

            # Exit conditions
            if r >= 1.00:
                exit_trade = True
                exit_reason = 'sma_recovery'
            elif i >= entry_info['expiry_idx']:
                exit_trade = True
                exit_reason = 'expiration'

            if exit_trade:
                # Calculate P&L
                short_strike = entry_info['short_strike']
                long_strike = entry_info['long_strike']
                credit = entry_info['credit']
                contracts = entry_info['contracts']

                # Intrinsic value at exit
                short_intrinsic = max(0, short_strike - price)
                if use_spread:
                    long_intrinsic = max(0, long_strike - price)
                    spread_value = short_intrinsic - long_intrinsic
                else:
                    spread_value = short_intrinsic

                pnl_per_share = credit - spread_value
                pnl = pnl_per_share * 100 * contracts

                equity += pnl
                peak_equity = max(peak_equity, equity)
                dd = (equity - peak_equity) / peak_equity
                max_dd = min(max_dd, dd)

                trades.append({
                    'entry_date': entry_info['entry_date'],
                    'exit_date': dt,
                    'entry_price': entry_info['entry_price'],
                    'exit_price': price,
                    'vix_at_entry': entry_info['entry_vix'],
                    'short_strike': short_strike,
                    'credit': credit,
                    'contracts': contracts,
                    'pnl': pnl,
                    'exit_reason': exit_reason,
                    'won': pnl > 0,
                    'spy_return': (price - entry_info['entry_price']) / entry_info['entry_price'] * 100,
                })

                in_trade = False

                # Re-enter if still below threshold
                if r < threshold:
                    short_strike = round(price * (1 - otm_pct), 0)
                    premium = estimate_put_premium(price, short_strike, v, 63)
                    if use_spread:
                        long_strike = round(price * (1 - otm_pct - spread_width_pct), 0)
                        long_premium = estimate_put_premium(price, long_strike, v, 63)
                        net_credit = premium - long_premium
                        max_loss_per_share = max((short_strike - long_strike) - net_credit, 0.01)
                    else:
                        net_credit = premium
                        long_strike = 0
                        max_loss_per_share = short_strike * 0.20

                    trade_budget = equity * risk_per_trade_pct
                    contracts = min(int(trade_budget / (max_loss_per_share * 100)), max_contracts)
                    contracts = max(contracts, 1)

                    in_trade = True
                    entry_info = {
                        'entry_date': dt, 'entry_price': price, 'entry_vix': v,
                        'short_strike': short_strike, 'long_strike': long_strike,
                        'credit': net_credit, 'contracts': contracts,
                        'expiry_idx': i + 63,
                        'max_loss': max_loss_per_share * 100 * contracts,
                    }

    # Compute metrics
    if not trades:
        return None

    trades_df = pd.DataFrame(trades)
    years = len(valid_idx) / 252
    total_pnl = trades_df.pnl.sum()
    wins = trades_df.won.sum()
    win_rate = wins / len(trades_df) * 100
    gross_wins = trades_df[trades_df.won].pnl.sum()
    gross_losses = abs(trades_df[~trades_df.won].pnl.sum())
    pf = gross_wins / gross_losses if gross_losses > 0 else 999
    cagr = ((equity / starting_capital) ** (1/years) - 1) * 100
    avg_pnl = total_pnl / len(trades_df)

    # Sharpe from trade returns
    trade_rets = trades_df.pnl / starting_capital  # simplified
    sharpe = trade_rets.mean() / trade_rets.std() * np.sqrt(len(trades_df)/years) if trade_rets.std() > 0 else 0

    return {
        'trades': len(trades_df),
        'win_rate': win_rate,
        'total_pnl': total_pnl,
        'avg_pnl': avg_pnl,
        'pf': pf,
        'cagr': cagr,
        'max_dd': max_dd * 100,
        'sharpe': sharpe,
        'final_equity': equity,
        'trades_df': trades_df,
    }


# ============================================================
# TEST 1: NAKED PUT SELLING
# ============================================================
print(f"\n{SEP}")
print("TEST 1: NAKED PUT SELLING (various thresholds and strikes)")
print(f"Starting capital: $100,000  |  Risk per trade: 5%")
print(SEP)

print(f"\n  {'Config':<30} {'Trades':>6} {'WR':>5} {'P&L':>10} {'PF':>5} "
      f"{'CAGR':>6} {'MaxDD':>6} {'Sharpe':>6} {'Final':>10}")
print(f"  {'-'*86}")

for threshold in [1.00, 0.98, 0.96]:
    for otm in [0.0, 0.02, 0.05]:
        label = f"SMA<{threshold:.2f} {otm*100:.0f}%OTM naked"
        r = run_put_selling_backtest(threshold, otm, use_spread=False)
        if r:
            pf_str = f"{r['pf']:.1f}" if r['pf'] < 100 else "inf"
            print(f"  {label:<30} {r['trades']:>6} {r['win_rate']:>4.0f}% "
                  f"${r['total_pnl']:>+9,.0f} {pf_str:>5} "
                  f"{r['cagr']:>5.1f}% {r['max_dd']:>5.1f}% "
                  f"{r['sharpe']:>5.2f} ${r['final_equity']:>9,.0f}")

# ============================================================
# TEST 2: PUT CREDIT SPREADS (capped risk)
# ============================================================
print(f"\n{SEP}")
print("TEST 2: PUT CREDIT SPREADS (5% wing width, capped risk)")
print(SEP)

print(f"\n  {'Config':<30} {'Trades':>6} {'WR':>5} {'P&L':>10} {'PF':>5} "
      f"{'CAGR':>6} {'MaxDD':>6} {'Sharpe':>6} {'Final':>10}")
print(f"  {'-'*86}")

for threshold in [1.00, 0.98, 0.96]:
    for otm in [0.0, 0.02, 0.05]:
        label = f"SMA<{threshold:.2f} {otm*100:.0f}%OTM spread"
        r = run_put_selling_backtest(threshold, otm, use_spread=True, spread_width_pct=0.05)
        if r:
            pf_str = f"{r['pf']:.1f}" if r['pf'] < 100 else "inf"
            print(f"  {label:<30} {r['trades']:>6} {r['win_rate']:>4.0f}% "
                  f"${r['total_pnl']:>+9,.0f} {pf_str:>5} "
                  f"{r['cagr']:>5.1f}% {r['max_dd']:>5.1f}% "
                  f"{r['sharpe']:>5.2f} ${r['final_equity']:>9,.0f}")

# ============================================================
# TEST 3: BEST CONFIG DETAILED ANALYSIS
# ============================================================
# Run the most promising config with full detail
best = run_put_selling_backtest(0.98, 0.02, use_spread=True, spread_width_pct=0.05)

if best:
    tdf = best['trades_df']
    print(f"\n{SEP}")
    print(f"DETAILED: SMA < 0.98, 2% OTM PUT CREDIT SPREAD (5% wing)")
    print(SEP)

    print(f"\n  {'Entry':<12} {'Exit':<12} {'SPY':>6} {'VIX':>5} {'Strike':>7} "
          f"{'Credit':>7} {'Cts':>4} {'P&L':>10} {'SPY Ret':>8} {'Result':>7}")
    print(f"  {'-'*82}")

    for _, t in tdf.iterrows():
        result = "WIN" if t['won'] else "LOSS"
        print(f"  {t['entry_date'].strftime('%Y-%m-%d'):<12} "
              f"{t['exit_date'].strftime('%Y-%m-%d'):<12} "
              f"${t['entry_price']:>5.0f} {t['vix_at_entry']:>4.0f} "
              f"${t['short_strike']:>6.0f} ${t['credit']:>5.2f} "
              f"{t['contracts']:>4} ${t['pnl']:>+9,.0f} "
              f"{t['spy_return']:>+7.1f}% {result:>7}")

    # Year by year
    tdf['year'] = tdf['entry_date'].dt.year
    print(f"\n  Year-by-Year:")
    print(f"  {'Year':<6} {'Trades':>6} {'WR':>5} {'P&L':>10}")
    print(f"  {'-'*30}")
    for year in sorted(tdf.year.unique()):
        yr = tdf[tdf.year == year]
        print(f"  {year:<6} {len(yr):>6} {yr.won.mean()*100:>4.0f}% ${yr.pnl.sum():>+9,.0f}")

# ============================================================
# COMPARISON TO ALWAYS-ON PCS
# ============================================================
print(f"\n{SEP}")
print("COMPARISON: SMA-TRIGGERED vs ALWAYS-ON PUT SELLING")
print(SEP)
print(f"""
  The SMA-triggered strategy only sells puts when Price/SMA < threshold.
  This means it's only active ~10-15% of the time.

  The always-on PCS strategy (P-01) sells puts continuously.

  Key advantage of SMA-triggered:
  - Higher premium collected (IV is elevated when SMA breaks)
  - Contrarian signal (SMA breaks tend to recover)
  - Only risks capital during specific windows

  Key disadvantage:
  - Very few trades (~1-2/year at 0.98 threshold)
  - Miss the steady premium income from normal-market selling
  - 2020 and 2022 concentrated losses can be severe

  VERDICT: This works best as a SUPPLEMENTAL strategy —
  increase PCS position size when the SMA signal fires
  (contrarian sizing, like the regime indicator research showed),
  rather than a standalone strategy with too few trades.
""")
