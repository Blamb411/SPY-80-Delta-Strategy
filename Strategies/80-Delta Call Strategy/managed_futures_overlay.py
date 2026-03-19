"""
Managed Futures Overlay on UPRO DD25/Cool40 and 80-Delta
Models permanent allocation and "out period" alternatives.
Uses synthetic managed futures returns for pre-2019 period.
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
import yfinance as yf
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

SEP = "=" * 70

# ============================================================
# 1. BUILD SYNTHETIC MANAGED FUTURES INDEX (2009-2026)
# ============================================================
print(SEP)
print("BUILDING SYNTHETIC MANAGED FUTURES RETURNS")
print(SEP)

# Simple trend-following model on 4 asset classes:
# Equities (SPY), Bonds (TLT), Gold (GLD), Dollar (UUP)
# Rule: for each asset, go long if 12-month return > 0, else short
# Equal-weight across 4 assets

assets = {'SPY': 'Equities', 'TLT': 'Bonds', 'GLD': 'Gold', 'UUP': 'Dollar'}
data = {}
for ticker, label in assets.items():
    df = yf.download(ticker, start='2007-01-01', end='2026-03-19', interval='1mo', progress=False)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    data[ticker] = df['Close'].dropna()
    data[ticker].index = data[ticker].index.to_period('M').to_timestamp()
    print(f"  {ticker} ({label}): {len(data[ticker])} months")

# Also get actual DBMF for validation (2019+)
dbmf = yf.download('DBMF', start='2019-01-01', end='2026-03-19', interval='1mo', progress=False)
if isinstance(dbmf.columns, pd.MultiIndex):
    dbmf.columns = dbmf.columns.get_level_values(0)
dbmf_rets = dbmf['Close'].pct_change().dropna()
dbmf_rets.index = dbmf_rets.index.to_period('M').to_timestamp()
print(f"  DBMF: {len(dbmf_rets)} months (for validation)")

# Get UPRO
upro = yf.download('UPRO', start='2009-06-01', end='2026-03-19', interval='1mo', progress=False)
if isinstance(upro.columns, pd.MultiIndex):
    upro.columns = upro.columns.get_level_values(0)
upro_close = upro['Close'].dropna()
upro_close.index = upro_close.index.to_period('M').to_timestamp()
upro_rets = upro_close.pct_change().dropna()
print(f"  UPRO: {len(upro_rets)} months")

# Build synthetic trend-following returns
# 12-month lookback momentum on each asset
asset_rets = pd.DataFrame({t: data[t].pct_change() for t in assets}).dropna()

synth_mf = pd.Series(0.0, index=asset_rets.index)
for i in range(12, len(asset_rets)):
    month_return = 0
    for ticker in assets:
        # 12-month trailing return
        trailing = (1 + asset_rets[ticker].iloc[i-12:i]).prod() - 1
        # Go long if positive momentum, short if negative
        current_ret = asset_rets[ticker].iloc[i]
        if trailing > 0:
            month_return += current_ret / len(assets)
        else:
            month_return -= current_ret / len(assets)
    synth_mf.iloc[i] = month_return

synth_mf = synth_mf.iloc[12:]  # drop lookback period
print(f"  Synthetic MF: {len(synth_mf)} months")

# Validate against DBMF (overlap period)
overlap = pd.DataFrame({
    'Synthetic': synth_mf,
    'DBMF': dbmf_rets
}).dropna()

if len(overlap) > 12:
    corr = overlap['Synthetic'].corr(overlap['DBMF'])
    print(f"\n  Validation (overlap period {len(overlap)} months):")
    print(f"    Correlation synthetic vs DBMF: {corr:.3f}")
    print(f"    Synthetic ann return: {overlap['Synthetic'].mean()*12*100:.1f}%")
    print(f"    DBMF ann return: {overlap['DBMF'].mean()*12*100:.1f}%")
    print(f"    Synthetic ann vol: {overlap['Synthetic'].std()*np.sqrt(12)*100:.1f}%")
    print(f"    DBMF ann vol: {overlap['DBMF'].std()*np.sqrt(12)*100:.1f}%")

# Use DBMF where available, synthetic before that
mf_returns = synth_mf.copy()
for dt in dbmf_rets.index:
    if dt in mf_returns.index:
        mf_returns[dt] = dbmf_rets[dt]
print(f"  Combined MF series: {len(mf_returns)} months "
      f"({len(dbmf_rets)} DBMF + {len(mf_returns)-len(dbmf_rets)} synthetic)")

# T-bill returns (BIL)
bil = yf.download('BIL', start='2007-01-01', end='2026-03-19', interval='1mo', progress=False)
if isinstance(bil.columns, pd.MultiIndex):
    bil.columns = bil.columns.get_level_values(0)
bil_rets = bil['Close'].pct_change().dropna()
bil_rets.index = bil_rets.index.to_period('M').to_timestamp()

# ============================================================
# 2. UPRO DD25/Cool40 WITH MANAGED FUTURES OVERLAY
# ============================================================
print(f"\n{SEP}")
print("UPRO DD25/COOL40 — MANAGED FUTURES OVERLAY")
print(SEP)

# Implement DD25/Cool40
def run_upro_dd25(upro_rets, cash_rets, cool_days=40):
    """Run DD25/Cool40 and return monthly returns."""
    equity = 1.0
    peak = 1.0
    out = False
    cool_counter = 0
    monthly_rets = []

    for dt, ret in upro_rets.items():
        if out:
            # In cash - use cash returns
            cash_dt = cash_rets.get(dt, 0)
            if pd.isna(cash_dt):
                cash_dt = 0
            equity *= (1 + cash_dt)
            monthly_rets.append(cash_dt)
            cool_counter += 21  # ~21 trading days per month
            if cool_counter >= cool_days:
                out = False
                cool_counter = 0
        else:
            # In UPRO
            equity *= (1 + ret)
            monthly_rets.append(ret)
            peak = max(peak, equity)
            dd = (equity / peak) - 1
            if dd <= -0.25:
                out = True
                cool_counter = 0

    return pd.Series(monthly_rets, index=upro_rets.index)

# Align all series
common_idx = upro_rets.index.intersection(mf_returns.index).intersection(bil_rets.index)
upro_aligned = upro_rets.reindex(common_idx)
mf_aligned = mf_returns.reindex(common_idx)
bil_aligned = bil_rets.reindex(common_idx)

print(f"\nAligned period: {common_idx[0].strftime('%Y-%m')} to {common_idx[-1].strftime('%Y-%m')} "
      f"({len(common_idx)} months)")

def compute_metrics(rets, label):
    cum = (1 + rets).cumprod()
    years = len(rets) / 12
    cagr = (cum.iloc[-1] ** (1/years) - 1) * 100
    sharpe = rets.mean() / rets.std() * np.sqrt(12) if rets.std() > 0 else 0
    peak = cum.cummax()
    dd = ((cum / peak) - 1) * 100
    max_dd = dd.min()
    return {'label': label, 'cagr': cagr, 'sharpe': sharpe, 'max_dd': max_dd,
            'final': cum.iloc[-1]}

# Scenarios
configs = []

# A) Baseline: 100% UPRO DD25/Cool40, T-bills during out
baseline_rets = run_upro_dd25(upro_aligned, bil_aligned)
configs.append(compute_metrics(baseline_rets, "100% UPRO DD25/Cool40 (T-bills)"))

# B) Permanent allocations: X% UPRO + Y% managed futures
for mf_pct in [0.10, 0.15, 0.20, 0.25, 0.30]:
    upro_pct = 1.0 - mf_pct
    blend_rets = upro_pct * upro_aligned + mf_pct * mf_aligned
    # Apply DD25/Cool40 to the UPRO portion only
    # More accurate: track blended portfolio value for DD trigger
    equity = 1.0
    peak = 1.0
    out = False
    cool_counter = 0
    blended = []
    for dt in common_idx:
        if out:
            # Cash portion: T-bills for UPRO allocation, keep MF running
            cash_ret = bil_aligned.get(dt, 0) if not pd.isna(bil_aligned.get(dt, 0)) else 0
            total_ret = upro_pct * cash_ret + mf_pct * mf_aligned.get(dt, 0)
            if pd.isna(total_ret):
                total_ret = 0
            equity *= (1 + total_ret)
            blended.append(total_ret)
            cool_counter += 21
            if cool_counter >= 40:
                out = False
                cool_counter = 0
        else:
            upro_ret = upro_aligned.get(dt, 0)
            mf_ret = mf_aligned.get(dt, 0)
            if pd.isna(upro_ret): upro_ret = 0
            if pd.isna(mf_ret): mf_ret = 0
            total_ret = upro_pct * upro_ret + mf_pct * mf_ret
            equity *= (1 + total_ret)
            blended.append(total_ret)
            peak = max(peak, equity)
            dd = (equity / peak) - 1
            if dd <= -0.25:
                out = True
                cool_counter = 0

    blend_series = pd.Series(blended, index=common_idx)
    label = f"{int(upro_pct*100)}% UPRO + {int(mf_pct*100)}% MF (permanent)"
    configs.append(compute_metrics(blend_series, label))

# C) MF during "out" periods only (instead of T-bills)
equity = 1.0
peak = 1.0
out = False
cool_counter = 0
mf_out_rets = []
for dt in common_idx:
    if out:
        mf_ret = mf_aligned.get(dt, 0)
        if pd.isna(mf_ret): mf_ret = 0
        equity *= (1 + mf_ret)
        mf_out_rets.append(mf_ret)
        cool_counter += 21
        if cool_counter >= 40:
            out = False
            cool_counter = 0
    else:
        upro_ret = upro_aligned.get(dt, 0)
        if pd.isna(upro_ret): upro_ret = 0
        equity *= (1 + upro_ret)
        mf_out_rets.append(upro_ret)
        peak = max(peak, equity)
        dd = (equity / peak) - 1
        if dd <= -0.25:
            out = True
            cool_counter = 0

mf_out_series = pd.Series(mf_out_rets, index=common_idx)
configs.append(compute_metrics(mf_out_series, "UPRO DD25 + MF during OUT periods"))

# D) 100% UPRO buy and hold (no timing)
configs.append(compute_metrics(upro_aligned, "100% UPRO Buy & Hold (no timing)"))

# Print results
print(f"\n  {'Config':<45} {'CAGR':>7} {'Sharpe':>7} {'MaxDD':>7} {'Growth':>8}")
print(f"  {'-'*76}")
for c in configs:
    print(f"  {c['label']:<45} {c['cagr']:>6.1f}% {c['sharpe']:>7.2f} "
          f"{c['max_dd']:>6.1f}% {c['final']:>7.1f}x")

# ============================================================
# 3. 80-DELTA WITH MANAGED FUTURES OVERLAY
# ============================================================
print(f"\n{SEP}")
print("80-DELTA STRATEGY — MANAGED FUTURES OVERLAY CONCEPT")
print(SEP)

# For 80-delta, the overlay is simpler: allocate some of the
# cash margin to managed futures
spy_monthly = yf.download('SPY', start='2009-06-01', end='2026-03-19',
                          interval='1mo', progress=False)
if isinstance(spy_monthly.columns, pd.MultiIndex):
    spy_monthly.columns = spy_monthly.columns.get_level_values(0)
spy_rets = spy_monthly['Close'].pct_change().dropna()
spy_rets.index = spy_rets.index.to_period('M').to_timestamp()
spy_aligned = spy_rets.reindex(common_idx)

# The 80-delta strategy is approximately 1.3x SPY exposure
# (delta ~0.80 with leverage from options)
delta_rets = 1.3 * spy_aligned

print(f"\n  80-delta approximated as 1.3x SPY monthly returns")
print(f"  (Simplified — actual strategy has roll costs, gamma, theta)")

configs_80d = []
configs_80d.append(compute_metrics(delta_rets, "80-Delta (no overlay)"))

for mf_pct in [0.05, 0.10, 0.15, 0.20]:
    delta_pct = 1.0 - mf_pct
    blend = delta_pct * delta_rets + mf_pct * mf_aligned.reindex(common_idx).fillna(0)
    label = f"{int(delta_pct*100)}% 80-Delta + {int(mf_pct*100)}% MF"
    configs_80d.append(compute_metrics(blend, label))

print(f"\n  {'Config':<45} {'CAGR':>7} {'Sharpe':>7} {'MaxDD':>7}")
print(f"  {'-'*62}")
for c in configs_80d:
    print(f"  {c['label']:<45} {c['cagr']:>6.1f}% {c['sharpe']:>7.2f} {c['max_dd']:>6.1f}%")

print(f"\n  Note: 80-delta returns are approximated. Real strategy has")
print(f"  additional alpha (+2.08% Jensen's) and different drawdown profile.")
print(f"  The MF overlay's relative benefit should be similar.")
