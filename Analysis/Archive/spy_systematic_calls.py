"""
SPY Systematic 80-Delta Calls Backtest
=======================================
Tests systematic buying of 80-delta SPY calls with various exit rules
and trend filters over 30+ years of data.

Uses Black-Scholes synthetic pricing with VIX as IV input.
Downloads SPY (1993-present), VIX, and 3-month T-bill rate via yfinance.

Usage:
    python spy_systematic_calls.py
"""

import numpy as np
import pandas as pd
import yfinance as yf
import sys
import os
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from backtest.black_scholes import black_scholes_price, find_strike_for_delta, black_scholes_greeks

# ═══════════════════════════════════════════════════════════════════════════
# PARAMETERS
# ═══════════════════════════════════════════════════════════════════════════

DELTA = 0.80
DTE_ENTRY = 90              # trading days (~4.2 months calendar)
ENTRY_INTERVAL = 20         # trading days between entries (~monthly)
SPREAD_COST = 0.02          # 2% round-trip for SPY options
IV_FLOOR = 0.08
IV_CAP = 0.90
RISK_FREE_DEFAULT = 0.04    # fallback if T-bill data unavailable

RULE_SETS = {
    'A: PT50/NoSL/MH60': {'pt': 0.50, 'sl': None, 'mh': 60},
    'B: PT50/SL50/MH60': {'pt': 0.50, 'sl': 0.50, 'mh': 60},
    'C: PT30/SL50/MH60': {'pt': 0.30, 'sl': 0.50, 'mh': 60},
    'D: NoPT/SL50/MH60': {'pt': None, 'sl': 0.50, 'mh': 60},
}


# ═══════════════════════════════════════════════════════════════════════════
# DATA
# ═══════════════════════════════════════════════════════════════════════════

def download_data():
    """Download SPY, VIX, and risk-free rate. Return merged DataFrame."""

    print("Downloading SPY...")
    spy_raw = yf.download('SPY', start='1993-01-29', end='2026-02-01', auto_adjust=True)
    if isinstance(spy_raw.columns, pd.MultiIndex):
        spy_raw.columns = spy_raw.columns.droplevel(1)
    spy = spy_raw[['Close']].rename(columns={'Close': 'close'}).copy()

    print("Downloading VIX...")
    vix_raw = yf.download('^VIX', start='1993-01-01', end='2026-02-01', auto_adjust=True)
    if isinstance(vix_raw.columns, pd.MultiIndex):
        vix_raw.columns = vix_raw.columns.droplevel(1)
    vix = vix_raw[['Close']].rename(columns={'Close': 'vix'}).copy()

    print("Downloading 3-month T-bill rate...")
    try:
        irx_raw = yf.download('^IRX', start='1993-01-01', end='2026-02-01', auto_adjust=True)
        if isinstance(irx_raw.columns, pd.MultiIndex):
            irx_raw.columns = irx_raw.columns.droplevel(1)
        irx = irx_raw[['Close']].rename(columns={'Close': 'rate'}).copy()
        irx['rate'] = irx['rate'] / 100.0  # convert from percentage points
        has_rate = True
    except Exception:
        irx = pd.DataFrame()
        has_rate = False

    # Merge
    df = spy.copy()
    df = df.join(vix, how='left')
    if has_rate and not irx.empty:
        df = df.join(irx, how='left')

    # Forward-fill gaps
    df['vix'] = df['vix'].ffill().bfill()
    if 'rate' not in df.columns:
        df['rate'] = RISK_FREE_DEFAULT
    else:
        df['rate'] = df['rate'].ffill().bfill().fillna(RISK_FREE_DEFAULT)
        df['rate'] = df['rate'].clip(0.001, 0.15)  # sanity bounds

    # Compute indicators
    df['sma200'] = df['close'].rolling(200).mean()
    df['sma50'] = df['close'].rolling(50).mean()

    # IV from VIX (VIX is quoted in percentage points, e.g. 20 = 20%)
    df['iv'] = (df['vix'] / 100.0).clip(IV_FLOOR, IV_CAP)

    # Need SMA200 to be valid
    df = df.dropna(subset=['sma200']).copy()

    return df


# ═══════════════════════════════════════════════════════════════════════════
# OPTION PRICING
# ═══════════════════════════════════════════════════════════════════════════

def price_call(spot, strike, dte_tdays, iv, rate):
    """Price a call. DTE in trading days, converted via /252."""
    if dte_tdays <= 0:
        return max(0.0, spot - strike)
    t = dte_tdays / 252.0
    p = black_scholes_price(spot, strike, t, rate, iv, 'C')
    return p if p is not None else 0.0


def find_call_strike(spot, iv, dte_tdays, rate, target_delta=0.80):
    """Find strike for target delta. Returns None on failure."""
    t = dte_tdays / 252.0
    try:
        return find_strike_for_delta(spot, t, rate, iv, target_delta, 'C')
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════════════════
# POSITION
# ═══════════════════════════════════════════════════════════════════════════

class Position:
    __slots__ = ['entry_date', 'entry_spot', 'strike', 'entry_cost',
                 'entry_mid', 'iv_entry', 'days_held']

    def __init__(self, entry_date, entry_spot, strike, entry_mid, iv_entry):
        self.entry_date = entry_date
        self.entry_spot = entry_spot
        self.strike = strike
        self.entry_mid = entry_mid
        self.entry_cost = entry_mid * (1 + SPREAD_COST / 2)  # pay above mid
        self.iv_entry = iv_entry
        self.days_held = 0


# ═══════════════════════════════════════════════════════════════════════════
# BACKTEST ENGINE
# ═══════════════════════════════════════════════════════════════════════════

def run_backtest(df, rule_set, filter_mask):
    """
    Run one backtest configuration.

    Returns:
        trades: list of trade dicts
        entries_considered: how many entry points checked
        entries_filtered: how many were filtered out
    """
    pt = rule_set['pt']
    sl = rule_set['sl']
    mh = rule_set['mh']

    dates = df.index.tolist()
    closes = df['close'].values
    ivs = df['iv'].values
    rates = df['rate'].values
    mask = filter_mask.values

    n = len(dates)
    open_positions = []
    trades = []
    entries_considered = 0
    entries_filtered = 0
    max_open = 0

    next_entry_idx = 0

    for i in range(n):
        spot = float(closes[i])
        iv = float(ivs[i])
        rate = float(rates[i])
        date = dates[i]

        # ── Check exits on open positions ──
        still_open = []
        for pos in open_positions:
            pos.days_held += 1
            dte_remaining = DTE_ENTRY - pos.days_held
            mid = price_call(spot, pos.strike, dte_remaining, iv, rate)
            exit_proceeds = mid * (1 - SPREAD_COST / 2)
            pnl_pct = exit_proceeds / pos.entry_cost - 1 if pos.entry_cost > 0 else 0

            exit_reason = None
            if pt is not None and pnl_pct >= pt:
                exit_reason = 'PT'
            elif sl is not None and pnl_pct <= -sl:
                exit_reason = 'SL'
            elif pos.days_held >= mh:
                exit_reason = 'MH'

            if exit_reason:
                trades.append({
                    'entry_date': pos.entry_date,
                    'exit_date': date,
                    'entry_spot': pos.entry_spot,
                    'exit_spot': spot,
                    'stock_return': spot / pos.entry_spot - 1,
                    'entry_cost': pos.entry_cost,
                    'exit_proceeds': exit_proceeds,
                    'pnl_pct': pnl_pct,
                    'days_held': pos.days_held,
                    'exit_reason': exit_reason,
                    'iv_entry': pos.iv_entry,
                    'iv_exit': iv,
                })
            else:
                still_open.append(pos)

        open_positions = still_open
        if len(open_positions) > max_open:
            max_open = len(open_positions)

        # ── Check new entry ──
        if i >= next_entry_idx:
            entries_considered += 1
            if mask[i]:
                strike = find_call_strike(spot, iv, DTE_ENTRY, rate, DELTA)
                if strike is not None:
                    mid = price_call(spot, strike, DTE_ENTRY, iv, rate)
                    if mid > 0.10:
                        pos = Position(date, spot, strike, mid, iv)
                        open_positions.append(pos)
            else:
                entries_filtered += 1

            next_entry_idx = i + ENTRY_INTERVAL

    return trades, entries_considered, entries_filtered, max_open


# ═══════════════════════════════════════════════════════════════════════════
# METRICS
# ═══════════════════════════════════════════════════════════════════════════

def compute_metrics(trades):
    """Compute performance metrics from trade list."""
    if not trades:
        return None

    tdf = pd.DataFrame(trades)
    n = len(tdf)
    returns = tdf['pnl_pct'].values

    mean_ret = returns.mean()
    med_ret = float(np.median(returns))
    win_rate = float((returns > 0).mean())

    wins = returns[returns > 0]
    losses = returns[returns <= 0]
    avg_win = float(wins.mean()) if len(wins) > 0 else 0.0
    avg_loss = float(losses.mean()) if len(losses) > 0 else 0.0

    # Exit reasons
    reasons = tdf['exit_reason'].value_counts().to_dict()

    # Annualized Sharpe (per-trade basis)
    trades_per_year = 252.0 / ENTRY_INTERVAL
    annual_mean = mean_ret * trades_per_year
    annual_std = returns.std() * np.sqrt(trades_per_year)
    sharpe = annual_mean / annual_std if annual_std > 0 else 0.0

    # Max drawdown on cumulative equity (sequential trade order by exit date)
    tdf_sorted = tdf.sort_values('exit_date')
    cum = np.cumprod(1 + tdf_sorted['pnl_pct'].values)
    running_max = np.maximum.accumulate(cum)
    dd = cum / running_max - 1
    max_dd = float(dd.min())

    # Yearly breakdown by entry year
    tdf['entry_year'] = pd.to_datetime(tdf['entry_date']).dt.year
    yearly = tdf.groupby('entry_year').agg(
        mean_ret=('pnl_pct', 'mean'),
        total_ret=('pnl_pct', 'sum'),
        n_trades=('pnl_pct', 'count'),
        win_rate=('pnl_pct', lambda x: (x > 0).mean()),
        stock_mean=('stock_return', 'mean'),
    )

    worst_yr = float(yearly['mean_ret'].min())
    worst_yr_name = int(yearly['mean_ret'].idxmin())
    best_yr = float(yearly['mean_ret'].max())
    best_yr_name = int(yearly['mean_ret'].idxmax())

    # Losing streaks
    is_loss = (tdf_sorted['pnl_pct'].values <= 0).astype(int)
    if is_loss.sum() > 0:
        groups = np.diff(np.concatenate([[0], is_loss, [0]]))
        starts = np.where(groups == 1)[0]
        ends = np.where(groups == -1)[0]
        streaks = ends - starts if len(starts) > 0 else [0]
        max_losing_streak = int(max(streaks)) if len(streaks) > 0 else 0
    else:
        max_losing_streak = 0

    # Stock comparison
    stock_mean = float(tdf['stock_return'].mean())

    # Annual P&L per $1K per trade
    annual_pnl_1k = mean_ret * trades_per_year * 1000

    return {
        'n_trades': n,
        'mean_ret': mean_ret,
        'med_ret': med_ret,
        'win_rate': win_rate,
        'avg_win': avg_win,
        'avg_loss': avg_loss,
        'sharpe': sharpe,
        'max_dd': max_dd,
        'worst_yr': worst_yr,
        'worst_yr_name': worst_yr_name,
        'best_yr': best_yr,
        'best_yr_name': best_yr_name,
        'max_losing_streak': max_losing_streak,
        'reasons': reasons,
        'stock_mean': stock_mean,
        'annual_pnl_1k': annual_pnl_1k,
        'yearly': yearly,
        'annual_mean': annual_mean,
    }


# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════

def main():
    df = download_data()

    start = df.index[0].strftime('%Y-%m-%d')
    end = df.index[-1].strftime('%Y-%m-%d')
    n_years = len(df) / 252.0

    print(f"\nData: {start} to {end} ({len(df)} trading days, ~{n_years:.1f} years)")
    print(f"SPY: ${df['close'].min():.2f} - ${df['close'].max():.2f}")
    print(f"VIX: {df['vix'].min():.1f} - {df['vix'].max():.1f}")
    print(f"Rate: {df['rate'].min():.2%} - {df['rate'].max():.2%}")

    # ── Build filter masks ──
    filters = {
        '1_no_filter':     pd.Series(True, index=df.index),
        '2_sma200':        df['close'] > df['sma200'],
        '3_sma50':         df['close'] > df['sma50'],
        '4_vix<25':        df['vix'] < 25,
        '5_sma200+vix25':  (df['close'] > df['sma200']) & (df['vix'] < 25),
    }

    # Show filter coverage
    print("\nFilter coverage (% of trading days where entry is allowed):")
    for fname, fmask in filters.items():
        pct = fmask.mean()
        print(f"  {fname:<20} {pct:.1%}")

    # ── Run all combinations ──
    results = {}
    total = len(RULE_SETS) * len(filters)
    count = 0

    for rule_name, rule_set in RULE_SETS.items():
        for filter_name, fmask in filters.items():
            count += 1
            print(f"\n[{count}/{total}] {rule_name} | {filter_name}", end='')

            trades, considered, filtered, max_open = run_backtest(df, rule_set, fmask)
            metrics = compute_metrics(trades)

            if metrics:
                metrics['entries_considered'] = considered
                metrics['entries_filtered'] = filtered
                metrics['max_concurrent'] = max_open
                results[(rule_name, filter_name)] = metrics
                print(f"  -> {metrics['n_trades']} trades, "
                      f"Mean={metrics['mean_ret']:+.1%}, "
                      f"Win={metrics['win_rate']:.1%}, "
                      f"Sharpe={metrics['sharpe']:.2f}")
            else:
                print(f"  -> No trades")

    # ══════════════════════════════════════════════════════════════════════
    # OUTPUT
    # ══════════════════════════════════════════════════════════════════════

    W = 140

    print("\n" + "=" * W)
    print("SPY SYSTEMATIC 80-DELTA CALLS BACKTEST")
    print(f"Period: {start} to {end} (~{n_years:.0f} years)")
    print(f"Entry: {DELTA:.0%}-delta call, {DTE_ENTRY} DTE (tdays), "
          f"every {ENTRY_INTERVAL} tdays (~monthly)")
    print(f"Spread: {SPREAD_COST:.0%} round-trip | IV: VIX-based")
    print("=" * W)

    # ── Summary Table ──────────────────────────────────────────────────
    print(f"\n{'Rule Set':<22} {'Filter':<18} {'N':>5} {'Filt%':>6} "
          f"{'Mean':>7} {'Med':>7} {'Win%':>6} {'Sharpe':>7} {'MaxDD':>7} "
          f"{'AvgW':>7} {'AvgL':>7} {'WorstYr':>14} {'$/yr':>8}")
    print("-" * W)

    for (rn, fn), m in sorted(results.items()):
        filt_pct = m['entries_filtered'] / max(m['entries_considered'], 1)
        print(f"{rn:<22} {fn:<18} {m['n_trades']:>5} {filt_pct:>5.0%} "
              f"{m['mean_ret']:>+6.1%} {m['med_ret']:>+6.1%} {m['win_rate']:>5.1%} "
              f"{m['sharpe']:>7.2f} {m['max_dd']:>+6.1%} "
              f"{m['avg_win']:>+6.1%} {m['avg_loss']:>+6.1%} "
              f"{m['worst_yr']:>+6.1%}({m['worst_yr_name']}) "
              f"{m['annual_pnl_1k']:>+7.0f}")

    # ── Filter Impact by Rule Set ──────────────────────────────────────
    print(f"\n{'=' * 90}")
    print("FILTER IMPACT BY RULE SET")
    print(f"{'=' * 90}")

    for rule_name in RULE_SETS:
        sharpe_vals = [results[(rule_name, fn)]['sharpe']
                       for fn in filters if (rule_name, fn) in results]
        best_sharpe = max(sharpe_vals) if sharpe_vals else 0

        print(f"\n  {rule_name}:")
        print(f"    {'Filter':<20} {'N':>6} {'Filt%':>6} {'Mean':>8} "
              f"{'Win%':>7} {'Sharpe':>7} {'MaxDD':>8} {'MaxOpen':>8}")
        print(f"    {'-' * 75}")

        for fn in filters:
            key = (rule_name, fn)
            if key in results:
                m = results[key]
                filt_pct = m['entries_filtered'] / max(m['entries_considered'], 1)
                marker = " ***" if abs(m['sharpe'] - best_sharpe) < 0.001 else ""
                print(f"    {fn:<20} {m['n_trades']:>6} {filt_pct:>5.0%} "
                      f"{m['mean_ret']:>+7.1%} {m['win_rate']:>6.1%} "
                      f"{m['sharpe']:>7.2f} {m['max_dd']:>+7.1%} "
                      f"{m['max_concurrent']:>8}{marker}")

    # ── Best Configuration Detail ──────────────────────────────────────
    best_key = max(results, key=lambda k: results[k]['sharpe'])
    best = results[best_key]

    print(f"\n{'=' * 90}")
    print(f"BEST BY SHARPE: {best_key[0]} | {best_key[1]}")
    print(f"{'=' * 90}")
    print(f"  Trades:            {best['n_trades']}")
    print(f"  Mean per trade:    {best['mean_ret']:+.2%}")
    print(f"  Median per trade:  {best['med_ret']:+.2%}")
    print(f"  Win rate:          {best['win_rate']:.1%}")
    print(f"  Avg win / loss:    {best['avg_win']:+.2%} / {best['avg_loss']:+.2%}")
    print(f"  Sharpe:            {best['sharpe']:.2f}")
    print(f"  Max drawdown:      {best['max_dd']:+.1%}")
    print(f"  Max losing streak: {best['max_losing_streak']} trades")
    print(f"  Max concurrent:    {best['max_concurrent']} positions")
    print(f"  Exit reasons:      {best['reasons']}")
    print(f"  Annual P&L/$1K:    ${best['annual_pnl_1k']:+,.0f}")

    print(f"\n  Year-by-Year:")
    print(f"  {'Year':<6} {'Trades':>7} {'Mean':>9} {'Win%':>7} {'SPY Stock':>10}")
    print(f"  {'-' * 42}")
    for year, row in best['yearly'].iterrows():
        print(f"  {year:<6} {int(row['n_trades']):>7} {row['mean_ret']:>+8.2%} "
              f"{row['win_rate']:>6.1%} {row['stock_mean']:>+9.2%}")

    # ── Decade Analysis ────────────────────────────────────────────────
    print(f"\n{'=' * 90}")
    print(f"DECADE ANALYSIS (best config: {best_key[0]} | {best_key[1]})")
    print(f"{'=' * 90}")

    yearly = best['yearly']
    decades = {
        '1990s': range(1990, 2000),
        '2000s': range(2000, 2010),
        '2010s': range(2010, 2020),
        '2020s': range(2020, 2030),
    }

    for dec_name, dec_range in decades.items():
        dec_data = yearly[yearly.index.isin(dec_range)]
        if len(dec_data) > 0:
            n_trades = int(dec_data['n_trades'].sum())
            mean = dec_data['mean_ret'].mean()  # avg of annual means
            # Weighted mean across all trades in the decade
            total_trades = dec_data['n_trades'].sum()
            weighted_mean = (dec_data['mean_ret'] * dec_data['n_trades']).sum() / total_trades
            win = (dec_data['mean_ret'] > 0).mean()
            print(f"\n  {dec_name}: {len(dec_data)} years, {n_trades} trades")
            print(f"    Weighted mean per trade: {weighted_mean:+.2%}")
            print(f"    Profitable years: {win:.0%}")
            print(f"    Worst year: {dec_data['mean_ret'].min():+.2%} "
                  f"({int(dec_data['mean_ret'].idxmin())})")
            print(f"    Best year:  {dec_data['mean_ret'].max():+.2%} "
                  f"({int(dec_data['mean_ret'].idxmax())})")

    # ── Regime Analysis ────────────────────────────────────────────────
    print(f"\n{'=' * 90}")
    print("REGIME ANALYSIS (best configuration)")
    print(f"{'=' * 90}")

    # Compute annual SPY returns
    df_yr = df.groupby(df.index.year)['close'].agg(['first', 'last'])
    df_yr['spy_ret'] = df_yr['last'] / df_yr['first'] - 1

    bull_years = set(df_yr[df_yr['spy_ret'] > 0.10].index)
    bear_years = set(df_yr[df_yr['spy_ret'] < -0.10].index)
    flat_years = set(df_yr.index) - bull_years - bear_years

    for label, regime_set, desc in [
        ('BULL', bull_years, 'SPY > +10%'),
        ('FLAT', flat_years, 'SPY -10% to +10%'),
        ('BEAR', bear_years, 'SPY < -10%'),
    ]:
        regime_data = yearly[yearly.index.isin(regime_set)]
        if len(regime_data) > 0:
            total_t = int(regime_data['n_trades'].sum())
            weighted = ((regime_data['mean_ret'] * regime_data['n_trades']).sum()
                        / regime_data['n_trades'].sum())
            w_stock = ((regime_data['stock_mean'] * regime_data['n_trades']).sum()
                       / regime_data['n_trades'].sum())
            print(f"\n  {label} ({desc}): {len(regime_data)} years, {total_t} trades")
            print(f"    Options mean: {weighted:+.2%}  |  Stock mean: {w_stock:+.2%}"
                  f"  |  Leverage: {weighted / w_stock:.1f}x" if w_stock != 0
                  else f"    Options mean: {weighted:+.2%}  |  Stock mean: {w_stock:+.2%}")
            for yr, row in regime_data.iterrows():
                spy_r = df_yr.loc[yr, 'spy_ret'] if yr in df_yr.index else 0
                print(f"      {yr}: opts {row['mean_ret']:+.1%}, "
                      f"stock {row['stock_mean']:+.1%}, SPY {spy_r:+.1%}, "
                      f"n={int(row['n_trades'])}, win={row['win_rate']:.0%}")

    # ── SPY Buy-and-Hold Comparison ────────────────────────────────────
    print(f"\n{'=' * 90}")
    print("SPY BUY-AND-HOLD COMPARISON")
    print(f"{'=' * 90}")

    spy_start = float(df['close'].iloc[0])
    spy_end = float(df['close'].iloc[-1])
    spy_total = spy_end / spy_start - 1
    spy_cagr = (spy_end / spy_start) ** (1 / n_years) - 1

    spy_annual_rets = df_yr['spy_ret']
    spy_sharpe = (spy_annual_rets.mean() / spy_annual_rets.std()
                  if spy_annual_rets.std() > 0 else 0)
    spy_worst = spy_annual_rets.min()
    spy_worst_yr = int(spy_annual_rets.idxmin())

    print(f"  SPY total return:    {spy_total:+,.0%}")
    print(f"  SPY CAGR:            {spy_cagr:+.1%}")
    print(f"  SPY worst year:      {spy_worst:+.1%} ({spy_worst_yr})")
    print(f"  SPY Sharpe (annual): {spy_sharpe:.2f}")

    print(f"\n  Options vs stock at same entry points (no filter):")
    print(f"  {'Rule Set':<22} {'Opt Mean':>9} {'Stock Mean':>11} "
          f"{'Leverage':>9} {'Opt Sharpe':>11}")
    print(f"  {'-' * 65}")
    for rn in RULE_SETS:
        key = (rn, '1_no_filter')
        if key in results:
            m = results[key]
            lev = (m['mean_ret'] / m['stock_mean']
                   if m['stock_mean'] != 0 else float('nan'))
            print(f"  {rn:<22} {m['mean_ret']:>+8.2%} {m['stock_mean']:>+10.2%} "
                  f"{lev:>8.1f}x {m['sharpe']:>10.2f}")

    # ── Capital Efficiency ─────────────────────────────────────────────
    print(f"\n{'=' * 90}")
    print("CAPITAL EFFICIENCY (best configuration)")
    print(f"{'=' * 90}")

    print(f"  Max concurrent positions: {best['max_concurrent']}")
    print(f"  If allocating $1,000/trade:")
    print(f"    Max capital at risk: ${best['max_concurrent'] * 1000:,}")
    print(f"    Mean return/trade:   {best['mean_ret']:+.2%} (${best['mean_ret'] * 1000:+,.0f})")
    print(f"    ~{252 / ENTRY_INTERVAL:.0f} trades/year")
    print(f"    Annual P&L:          ${best['annual_pnl_1k']:+,.0f} per $1K/trade")
    ann_on_max_cap = best['annual_pnl_1k'] / (best['max_concurrent'] * 1000)
    print(f"    Return on max capital: {ann_on_max_cap:+.1%}")

    print(f"\n{'=' * 90}")
    print("DONE")
    print(f"{'=' * 90}")


if __name__ == '__main__':
    main()
