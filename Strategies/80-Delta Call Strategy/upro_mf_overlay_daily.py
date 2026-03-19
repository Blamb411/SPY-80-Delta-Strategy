"""UPRO DD25/Cool40 with managed futures overlay — DAILY simulation."""
import sys
sys.stdout.reconfigure(encoding='utf-8')
import yfinance as yf
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

SEP = "=" * 70

print(SEP)
print("UPRO DD25/COOL40 — MANAGED FUTURES OVERLAY (DAILY)")
print(SEP)

# Fetch daily data
print("\nFetching daily data...")
tickers_to_get = {
    'UPRO': ('2009-06-01', '2026-03-19'),
    'DBMF': ('2019-05-01', '2026-03-19'),
    'BIL':  ('2007-01-01', '2026-03-19'),
    'SPY':  ('2007-01-01', '2026-03-19'),
    'TLT':  ('2007-01-01', '2026-03-19'),
    'GLD':  ('2007-01-01', '2026-03-19'),
    'UUP':  ('2007-01-01', '2026-03-19'),
}

closes = {}
for ticker, (start, end) in tickers_to_get.items():
    df = yf.download(ticker, start=start, end=end, progress=False)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    closes[ticker] = df['Close'].dropna()
    print(f"  {ticker}: {len(closes[ticker])} days")

rets = {t: closes[t].pct_change().dropna() for t in closes}

# Build daily synthetic managed futures (pre-DBMF)
print("\nBuilding daily synthetic managed futures...")
assets_df = pd.DataFrame({
    'SPY': closes['SPY'], 'TLT': closes['TLT'],
    'GLD': closes['GLD'], 'UUP': closes['UUP']
}).dropna()
asset_rets = assets_df.pct_change().dropna()

lookback = 252
synth = pd.Series(0.0, index=asset_rets.index)
for i in range(lookback, len(asset_rets)):
    day_ret = 0
    for col in asset_rets.columns:
        trailing = asset_rets[col].iloc[i-lookback:i].sum()
        current = asset_rets[col].iloc[i]
        if trailing > 0:
            day_ret += current / 4
        else:
            day_ret -= current / 4
    synth.iloc[i] = day_ret
synth = synth.iloc[lookback:]

# Combine: DBMF where available, synthetic before
mf_daily = synth.copy()
dbmf_rets = rets['DBMF']
for dt in dbmf_rets.index:
    if dt in mf_daily.index:
        mf_daily[dt] = dbmf_rets[dt]
print(f"  Combined MF: {len(mf_daily)} days")

# Align everything
common = rets['UPRO'].index.intersection(mf_daily.index).intersection(rets['BIL'].index)
upro_r = rets['UPRO'].reindex(common)
mf_r = mf_daily.reindex(common).fillna(0)
bil_r = rets['BIL'].reindex(common).fillna(0)

print(f"  Aligned: {len(common)} days "
      f"({common[0].strftime('%Y-%m-%d')} to {common[-1].strftime('%Y-%m-%d')})")


def run_dd25(upro_rets, cash_rets, cool_days=40):
    """Run DD25/Cool40 on daily data. Returns equity series."""
    equity = 10000.0
    peak = equity
    is_out = False
    cool_ctr = 0
    eq_list = []
    n_exits = 0
    in_days = 0
    out_days = 0

    for dt in upro_rets.index:
        u = float(upro_rets[dt])
        c = float(cash_rets.get(dt, 0))
        if np.isnan(u): u = 0
        if np.isnan(c): c = 0

        if is_out:
            equity *= (1 + c)
            out_days += 1
            cool_ctr += 1
            if cool_ctr >= cool_days:
                is_out = False
                cool_ctr = 0
                peak = equity
        else:
            equity *= (1 + u)
            in_days += 1
            peak = max(peak, equity)
            dd = (equity / peak) - 1
            if dd <= -0.25:
                is_out = True
                cool_ctr = 0
                n_exits += 1

        eq_list.append(equity)

    eq_s = pd.Series(eq_list, index=upro_rets.index)
    return eq_s, n_exits, in_days, out_days


def metrics(eq_series, label, n_exits=None, in_days=None, out_days=None):
    """Compute performance metrics from equity series."""
    years = len(eq_series) / 252
    final = eq_series.iloc[-1]
    cagr = (final / 10000) ** (1/years) - 1
    peak = eq_series.cummax()
    max_dd = ((eq_series / peak) - 1).min()
    daily_r = eq_series.pct_change().dropna()
    sharpe = daily_r.mean() / daily_r.std() * np.sqrt(252) if daily_r.std() > 0 else 0
    sortino_d = daily_r[daily_r < 0].std()
    sortino = daily_r.mean() / sortino_d * np.sqrt(252) if sortino_d > 0 else 0

    return {
        'label': label, 'final': final, 'cagr': cagr * 100,
        'sharpe': sharpe, 'sortino': sortino, 'max_dd': max_dd * 100,
        'n_exits': n_exits, 'in_days': in_days, 'out_days': out_days,
    }


results = []

# A) Baseline: T-bills during out
eq_a, exits_a, in_a, out_a = run_dd25(upro_r, bil_r)
results.append(metrics(eq_a, "DD25/Cool40 (T-bills during OUT)", exits_a, in_a, out_a))

# B) Managed futures during out
eq_b, exits_b, in_b, out_b = run_dd25(upro_r, mf_r)
results.append(metrics(eq_b, "DD25/Cool40 (MF during OUT)", exits_b, in_b, out_b))

# C) Permanent overlays with DD25 on blended portfolio
for mf_pct in [0.10, 0.15, 0.20, 0.25, 0.30]:
    upro_pct = 1.0 - mf_pct
    equity = 10000.0
    peak = equity
    is_out = False
    cool_ctr = 0
    eq_list = []
    n_exits = 0

    for dt in common:
        u = float(upro_r[dt])
        m = float(mf_r[dt])
        b = float(bil_r.get(dt, 0))
        if np.isnan(u): u = 0
        if np.isnan(m): m = 0
        if np.isnan(b): b = 0

        if is_out:
            r = upro_pct * b + mf_pct * m  # cash on UPRO, keep MF
            equity *= (1 + r)
            cool_ctr += 1
            if cool_ctr >= 40:
                is_out = False
                cool_ctr = 0
                peak = equity
        else:
            r = upro_pct * u + mf_pct * m
            equity *= (1 + r)
            peak = max(peak, equity)
            dd = (equity / peak) - 1
            if dd <= -0.25:
                is_out = True
                cool_ctr = 0
                n_exits += 1

        eq_list.append(equity)

    eq_s = pd.Series(eq_list, index=common)
    label = f"{int(upro_pct*100)}% UPRO + {int(mf_pct*100)}% MF (permanent)"
    results.append(metrics(eq_s, label, n_exits))

# D) UPRO Buy & Hold
eq_bh = (1 + upro_r).cumprod() * 10000
results.append(metrics(eq_bh, "UPRO Buy & Hold (no timing)"))

# Print results
years = len(common) / 252
print(f"\n  Period: {years:.1f} years | Starting capital: $10,000")
print(f"\n  {'Config':<42} {'Final':>10} {'CAGR':>7} {'Sharpe':>7} "
      f"{'Sortino':>8} {'MaxDD':>7} {'Exits':>6}")
print(f"  {'-'*90}")
for r in results:
    exits_str = str(r['n_exits']) if r['n_exits'] is not None else "-"
    extra = ""
    if r.get('in_days') and r.get('out_days'):
        pct_in = r['in_days'] / (r['in_days'] + r['out_days']) * 100
        extra = f"  ({pct_in:.0f}% in)"
    print(f"  {r['label']:<42} ${r['final']:>9,.0f} {r['cagr']:>6.1f}% "
          f"{r['sharpe']:>7.2f} {r['sortino']:>8.2f} {r['max_dd']:>6.1f}% "
          f"{exits_str:>6}{extra}")

# Comparison summary
base = results[0]
mf_out = results[1]
print(f"\n  --- COMPARISON: MF during OUT vs T-bills ---")
print(f"  CAGR improvement:    {mf_out['cagr'] - base['cagr']:+.1f}%")
print(f"  Sharpe improvement:  {mf_out['sharpe'] - base['sharpe']:+.2f}")
print(f"  Sortino improvement: {mf_out['sortino'] - base['sortino']:+.2f}")
print(f"  Max DD improvement:  {mf_out['max_dd'] - base['max_dd']:+.1f}pp")
print(f"  Final value:         ${mf_out['final']:,.0f} vs ${base['final']:,.0f} "
      f"(+${mf_out['final'] - base['final']:,.0f})")
