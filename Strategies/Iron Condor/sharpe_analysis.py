"""
Sharpe Ratio & Probabilistic Sharpe Ratio Analysis
=====================================================
Computes proper annualized Sharpe from daily P&L time series,
plus PSR (Bailey & Lopez de Prado, 2012).
"""

import csv
import math
from collections import defaultdict
from datetime import datetime

# Load trades
with open("trades_20260129_081907.csv") as f:
    trades = list(csv.DictReader(f))

print(f"Loaded {len(trades)} trades")
print()

# ============================================================
# METHOD 1: Trade-level Sharpe (naive)
# ============================================================
pnls = [float(t["pnl"]) for t in trades]
n = len(pnls)
mean_pnl = sum(pnls) / n
var = sum((p - mean_pnl) ** 2 for p in pnls) / (n - 1)
std_pnl = math.sqrt(var)

naive_sharpe = (mean_pnl / std_pnl) * math.sqrt(12)
print("METHOD 1: Trade-level (naive, sqrt(12) annualization)")
print(f"  Trades:          {n}")
print(f"  Mean P&L/trade:  ${mean_pnl:.2f}")
print(f"  Std P&L/trade:   ${std_pnl:.2f}")
print(f"  Sharpe:          {naive_sharpe:.4f}")
print()

# ============================================================
# METHOD 2: Daily P&L time series Sharpe
# ============================================================
# Attribute each trade's P&L to its EXIT date
daily_pnl = defaultdict(float)
for t in trades:
    daily_pnl[t["exit_date"]] += float(t["pnl"])

# Get all trading dates (union of entry and exit dates)
entry_dates = set(t["entry_date"] for t in trades)
exit_dates = set(t["exit_date"] for t in trades)
all_trading_dates = sorted(entry_dates | exit_dates)

daily_returns = [daily_pnl.get(d, 0.0) for d in all_trading_dates]
n_days = len(daily_returns)

mean_daily = sum(daily_returns) / n_days
var_daily = sum((r - mean_daily) ** 2 for r in daily_returns) / (n_days - 1)
std_daily = math.sqrt(var_daily)

daily_sharpe = (mean_daily / std_daily) * math.sqrt(252)

print("METHOD 2: Daily P&L time series (sqrt(252) annualization)")
print(f"  Trading days:    {n_days}")
print(f"  Days with P&L:   {len(daily_pnl)}")
print(f"  Mean daily P&L:  ${mean_daily:.2f}")
print(f"  Std daily P&L:   ${std_daily:.2f}")
print(f"  Sharpe (ann.):   {daily_sharpe:.4f}")
print()

# ============================================================
# METHOD 3: Weekly P&L time series Sharpe
# ============================================================
weekly_pnl = defaultdict(float)
for d, pnl in daily_pnl.items():
    dt = datetime.strptime(d, "%Y-%m-%d")
    week_key = dt.strftime("%Y-W%W")
    weekly_pnl[week_key] += pnl

# Include zero-pnl weeks
all_weeks = set()
for d in all_trading_dates:
    dt = datetime.strptime(d, "%Y-%m-%d")
    all_weeks.add(dt.strftime("%Y-W%W"))

weekly_returns = [weekly_pnl.get(w, 0.0) for w in sorted(all_weeks)]
n_weeks = len(weekly_returns)
mean_weekly = sum(weekly_returns) / n_weeks
var_weekly = sum((r - mean_weekly) ** 2 for r in weekly_returns) / (n_weeks - 1)
std_weekly = math.sqrt(var_weekly)
weekly_sharpe = (mean_weekly / std_weekly) * math.sqrt(52)

print("METHOD 3: Weekly P&L time series (sqrt(52) annualization)")
print(f"  Weeks:           {n_weeks}")
print(f"  Mean weekly P&L: ${mean_weekly:.2f}")
print(f"  Std weekly P&L:  ${std_weekly:.2f}")
print(f"  Sharpe (ann.):   {weekly_sharpe:.4f}")
print()

# ============================================================
# DISTRIBUTION STATISTICS
# ============================================================
returns = daily_returns
n_obs = len(returns)
mu = sum(returns) / n_obs
sigma = math.sqrt(sum((r - mu) ** 2 for r in returns) / (n_obs - 1))

# Skewness (sample, adjusted)
if sigma > 0 and n_obs > 2:
    m3 = sum((r - mu) ** 3 for r in returns) / n_obs
    skew = (n_obs / ((n_obs - 1) * (n_obs - 2))) * sum(((r - mu) / sigma) ** 3 for r in returns)
else:
    skew = 0

# Excess kurtosis (sample, adjusted)
if sigma > 0 and n_obs > 3:
    raw_kurt = (n_obs * (n_obs + 1)) / ((n_obs - 1) * (n_obs - 2) * (n_obs - 3))
    raw_kurt *= sum(((r - mu) / sigma) ** 4 for r in returns)
    kurt = raw_kurt - (3 * (n_obs - 1) ** 2) / ((n_obs - 2) * (n_obs - 3))
else:
    kurt = 0

print("=" * 60)
print("DISTRIBUTION STATISTICS (daily P&L series)")
print("=" * 60)
print(f"  Observations:    {n_obs}")
print(f"  Mean:            ${mu:.2f}")
print(f"  Std Dev:         ${sigma:.2f}")
print(f"  Skewness:        {skew:.4f}")
print(f"  Excess Kurtosis: {kurt:.4f}")
if skew < 0:
    print(f"  -> Negative skew: left tail is fatter (large losses)")
if kurt > 0:
    print(f"  -> Positive kurtosis: heavier tails than normal")
print()

# ============================================================
# PROBABILISTIC SHARPE RATIO (PSR)
# Bailey & Lopez de Prado (2012)
# ============================================================
#
# PSR(SR*) = Phi( (SR_hat - SR*) * sqrt(n-1)
#                 / sqrt(1 - gamma3*SR_hat + (gamma4-1)/4 * SR_hat^2) )
#
# SR_hat = non-annualized Sharpe (mean/std of the return series)
# SR*    = benchmark Sharpe (also non-annualized)
# gamma3 = skewness
# gamma4 = excess kurtosis
# n      = number of observations

def norm_cdf(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

sr_hat = mu / sigma if sigma > 0 else 0  # non-annualized daily Sharpe
sr_hat_ann = sr_hat * math.sqrt(252)

print("=" * 60)
print("PROBABILISTIC SHARPE RATIO (PSR)")
print("=" * 60)
print(f"  SR_hat (daily):      {sr_hat:.6f}")
print(f"  SR_hat (annualized): {sr_hat_ann:.4f}")
print(f"  n (observations):    {n_obs}")
print(f"  Skewness:            {skew:.4f}")
print(f"  Excess kurtosis:     {kurt:.4f}")
print()

benchmarks_ann = [0.0, 0.5, 1.0, -0.5]
print(f"  {'Benchmark SR*':>15} {'PSR':>10} {'Interpretation'}")
print(f"  {'-'*55}")

for sr_star_ann in sorted(benchmarks_ann):
    sr_star = sr_star_ann / math.sqrt(252)  # convert to daily scale

    denom_sq = 1 - skew * sr_hat + (kurt / 4) * sr_hat ** 2
    if denom_sq > 0:
        denom = math.sqrt(denom_sq)
        z = (sr_hat - sr_star) * math.sqrt(n_obs - 1) / denom
        psr = norm_cdf(z)
    else:
        psr = 0.5

    label = ""
    if sr_star_ann == 0.0:
        label = "<- key: prob of positive SR"
    elif sr_star_ann == 0.5:
        label = "mediocre benchmark"
    elif sr_star_ann == 1.0:
        label = "good benchmark"

    print(f"  {sr_star_ann:>15.1f} {psr:>9.1%}   {label}")

print()

# ============================================================
# MINIMUM TRACK RECORD LENGTH (minTRL)
# Bailey & Lopez de Prado (2012)
# ============================================================
# How many observations needed for PSR(SR*=0) > 95%?
# minTRL = max(1, floor( (z_0.95)^2 * (1 - skew*SR + (kurt/4)*SR^2) / SR^2 ) + 1 )

z_95 = 1.645  # one-sided 95%
if sr_hat != 0:
    min_trl = max(1, math.ceil(
        z_95 ** 2 * (1 - skew * sr_hat + (kurt / 4) * sr_hat ** 2)
        / (sr_hat ** 2)
    ) + 1)
else:
    min_trl = float("inf")

print("=" * 60)
print("MINIMUM TRACK RECORD LENGTH (minTRL)")
print("=" * 60)
print(f"  Observations needed for 95% confidence SR > 0: {min_trl}")
print(f"  Current observations:                          {n_obs}")
if n_obs >= min_trl:
    print(f"  -> Sufficient data (have {n_obs}, need {min_trl})")
else:
    print(f"  -> INSUFFICIENT data (have {n_obs}, need {min_trl})")
print()

# ============================================================
# DEFLATED SHARPE RATIO (DSR) - multiple testing adjustment
# ============================================================
# We tested 16 combinations. DSR adjusts for this.
# SR* for DSR = sqrt(V(SR_hat)) * ((1-gamma) * Z_inv(1 - 1/N) + gamma * Z_inv(1 - 1/(N*e)))
# where N = number of trials, gamma = Euler-Mascheroni constant

num_trials = 16  # number of combinations tested
euler_gamma = 0.5772156649

def norm_ppf(p):
    """Approximate inverse normal CDF (Beasley-Springer-Moro)."""
    if p <= 0:
        return -10
    if p >= 1:
        return 10
    if p == 0.5:
        return 0
    # Use rational approximation
    if p < 0.5:
        t = math.sqrt(-2 * math.log(p))
    else:
        t = math.sqrt(-2 * math.log(1 - p))
    c0, c1, c2 = 2.515517, 0.802853, 0.010328
    d1, d2, d3 = 1.432788, 0.189269, 0.001308
    result = t - (c0 + c1 * t + c2 * t * t) / (1 + d1 * t + d2 * t * t + d3 * t * t * t)
    if p < 0.5:
        return -result
    return result

# Expected max SR under null (all strategies have SR=0)
# E[max(SR)] ~ sqrt(2*ln(N)) - (ln(pi) + ln(ln(N))) / (2*sqrt(2*ln(N)))  for large N
# Or more precisely using order statistics

if num_trials > 1:
    p1 = 1 - 1 / num_trials
    p2 = 1 - 1 / (num_trials * math.e)
    z1 = norm_ppf(p1)
    z2 = norm_ppf(p2)

    # Variance of SR_hat
    var_sr = (1 - skew * sr_hat + (kurt / 4) * sr_hat ** 2) / (n_obs - 1)
    std_sr = math.sqrt(var_sr) if var_sr > 0 else 0

    expected_max_sr = std_sr * ((1 - euler_gamma) * z1 + euler_gamma * z2)
    expected_max_sr_ann = expected_max_sr * math.sqrt(252)

    # DSR = PSR with SR* = expected_max_sr
    if std_sr > 0:
        z_dsr = (sr_hat - expected_max_sr) / std_sr
        dsr = norm_cdf(z_dsr)
    else:
        dsr = 0.5

    print("=" * 60)
    print(f"DEFLATED SHARPE RATIO (DSR) — {num_trials} combinations tested")
    print("=" * 60)
    print(f"  Expected max SR under null (daily):      {expected_max_sr:.6f}")
    print(f"  Expected max SR under null (annualized): {expected_max_sr_ann:.4f}")
    print(f"  Observed SR (daily):                     {sr_hat:.6f}")
    print(f"  Observed SR (annualized):                {sr_hat_ann:.4f}")
    print(f"  DSR (prob true SR > 0 after adjustment): {dsr:.1%}")
    print()
    if dsr < 0.05:
        print(f"  -> After adjusting for testing 16 combos, there is only a")
        print(f"     {dsr:.1%} chance the true SR is positive. Strategy is NOT")
        print(f"     statistically significant.")
    elif dsr < 0.50:
        print(f"  -> Weak evidence. {dsr:.1%} probability true SR > 0 after")
        print(f"     adjusting for multiple testing.")

print()
print("=" * 60)
print("SUMMARY")
print("=" * 60)
print(f"  Annualized Sharpe (daily basis):  {daily_sharpe:.4f}")
print(f"  Annualized Sharpe (weekly basis): {weekly_sharpe:.4f}")
print(f"  PSR (prob SR > 0):                {norm_cdf((sr_hat - 0) * math.sqrt(n_obs-1) / math.sqrt(max(0.001, 1 - skew*sr_hat + (kurt/4)*sr_hat**2))):.1%}")
print(f"  DSR (after 16 combos):            {dsr:.1%}")
