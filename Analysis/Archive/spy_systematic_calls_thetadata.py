"""
SPY Systematic 80-Delta Calls -- ThetaData Validation
=====================================================
Validates the B-S synthetic backtest using real historical bid/ask data
from ThetaData. Compares two DTE/MH configurations to address the
time-value-at-exit problem:

  Config A: DTE ~120 cal days, MH=60 tdays  (match synthetic's time buffer)
  Config B: DTE ~90 cal days,  MH=40 tdays  (earlier exit preserves time value)

Three execution scenarios:
  1. Mid-price: (bid + ask) / 2
  2. Natural (worst fill): buy at ask, sell at bid
  3. 25% slippage: mid +/- 25% of half-spread

Requires:
  - Theta Terminal v3 running locally
  - thetadata_cache.db (auto-populated)

Usage:
    python spy_systematic_calls_thetadata.py
"""

import os
import sys
import math
import logging
from datetime import datetime, date, timedelta
from collections import defaultdict

import numpy as np
import pandas as pd

_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_dir = os.path.dirname(_this_dir)
sys.path.insert(0, _project_dir)

from backtest.thetadata_client import ThetaDataClient
from backtest.black_scholes import black_scholes_price, find_strike_for_delta

log = logging.getLogger("spy_thetadata")

# ═══════════════════════════════════════════════════════════════════════════
# PARAMETERS -- fixed across both configs
# ═══════════════════════════════════════════════════════════════════════════

DELTA = 0.80
ENTRY_INTERVAL = 20         # trading days between entries
PT = 0.50                   # profit target +50%
RATE = 0.04                 # risk-free rate for B-S strike estimation

DATA_START = "2005-01-01"   # go as far back as ThetaData covers
DATA_END = "2026-01-31"

SCENARIOS = ["mid", "natural", "slippage25"]

# Two configurations to compare
CONFIGS = {
    "A_DTE120_MH60": {
        "label": "DTE~120cal / MH=60td",
        "dte_target": 120,
        "dte_min": 90,
        "dte_max": 150,
        "mh": 60,
        "description": "Match synthetic: long DTE -> time value cushion at MH exit (~36 cal days left)",
    },
    "B_DTE90_MH40": {
        "label": "DTE~90cal / MH=40td",
        "dte_target": 90,
        "dte_min": 60,
        "dte_max": 120,
        "mh": 40,
        "description": "Same DTE, earlier exit -> time value preserved (~34 cal days left)",
    },
}


# ═══════════════════════════════════════════════════════════════════════════
# EXECUTION PRICE HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def entry_price_for_scenario(bid, ask, scenario):
    """Price to BUY an option under each scenario."""
    if bid is None or ask is None or bid <= 0 or ask <= 0:
        return None
    mid = (bid + ask) / 2.0
    spread = ask - bid
    if scenario == "mid":
        return mid
    elif scenario == "natural":
        return ask
    elif scenario == "slippage25":
        return mid + 0.25 * spread
    return mid


def exit_price_for_scenario(bid, ask, scenario):
    """Price to SELL an option under each scenario."""
    if bid is None or ask is None or bid <= 0 or ask <= 0:
        return None
    mid = (bid + ask) / 2.0
    spread = ask - bid
    if scenario == "mid":
        return mid
    elif scenario == "natural":
        return bid
    elif scenario == "slippage25":
        return mid - 0.25 * spread
    return mid


def is_monthly_opex(exp_str):
    """Check if an expiration is a standard monthly (3rd Friday of the month)."""
    exp_dt = datetime.strptime(exp_str, "%Y-%m-%d").date()
    if exp_dt.weekday() != 4:  # Not a Friday
        return False
    # 3rd Friday falls on days 15-21
    return 15 <= exp_dt.day <= 21


def find_monthly_expiration(client, root, entry_date, target_dte, dte_min, dte_max):
    """
    Find the nearest MONTHLY (3rd Friday) expiration within DTE range.
    Monthly expirations have much better data coverage in ThetaData.
    """
    expirations = client.get_expirations(root, entry_date)
    if not expirations:
        return None

    entry_dt = datetime.strptime(entry_date, "%Y-%m-%d").date()
    best_exp = None
    best_diff = 9999

    for exp_str in expirations:
        if not is_monthly_opex(exp_str):
            continue
        exp_dt = datetime.strptime(exp_str, "%Y-%m-%d").date()
        dte = (exp_dt - entry_dt).days
        if dte < dte_min or dte > dte_max:
            continue
        diff = abs(dte - target_dte)
        if diff < best_diff:
            best_diff = diff
            best_exp = exp_str

    return best_exp


def get_usable_bid_ask(eod_row, is_spy=True):
    """Extract bid/ask from EOD row, falling back to close if needed."""
    bid = eod_row.get("bid", 0) or 0
    ask = eod_row.get("ask", 0) or 0

    if bid > 0 and ask > 0 and ask >= bid:
        return bid, ask, False

    # Fallback to close with estimated spread
    close = eod_row.get("close", 0) or 0
    if close > 0:
        # SPY options have very tight spreads
        half_spread = 0.002 if is_spy else 0.015
        return close * (1 - half_spread), close * (1 + half_spread), True

    return None, None, True


# ═══════════════════════════════════════════════════════════════════════════
# DATA LOADING
# ═══════════════════════════════════════════════════════════════════════════

def load_market_data(client):
    """Load SPY bars and VIX, compute SMA200."""

    print("Loading SPY bars...")
    spy_bars = client.fetch_spy_bars(DATA_START, DATA_END)
    spy_by_date = {b["bar_date"]: b["close"] for b in spy_bars}
    trading_dates = sorted(spy_by_date.keys())

    print("Loading VIX history...")
    vix_data = client.fetch_vix_history(DATA_START, DATA_END)

    # Compute SMA200
    sma200 = {}
    for i in range(199, len(trading_dates)):
        window = [spy_by_date[trading_dates[j]] for j in range(i - 199, i + 1)]
        sma200[trading_dates[i]] = sum(window) / 200.0

    print(f"  SPY bars: {len(spy_bars)} ({trading_dates[0]} to {trading_dates[-1]})")
    print(f"  VIX days: {len(vix_data)}")
    print(f"  SMA200 from: {sorted(sma200.keys())[0]}")

    return spy_by_date, trading_dates, vix_data, sma200


# ═══════════════════════════════════════════════════════════════════════════
# BACKTEST ENGINE -- parameterized by config
# ═══════════════════════════════════════════════════════════════════════════

def run_backtest(client, spy_by_date, trading_dates, vix_data, sma200, config):
    """
    Fetch option data for all entry points and track trades.
    Returns list of trade dicts (all scenarios, both filter states).

    config dict must contain: dte_target, dte_min, dte_max, mh, label
    """
    dte_target = config["dte_target"]
    dte_min = config["dte_min"]
    dte_max = config["dte_max"]
    mh = config["mh"]
    label = config["label"]

    print(f"\n{'-' * 70}")
    print(f"Config: {label}")
    print(f"  DTE target={dte_target} cal, range=[{dte_min}, {dte_max}], MH={mh} tdays")
    print(f"{'-' * 70}")

    # Identify ALL entry dates (every ENTRY_INTERVAL trading days, after SMA200 valid)
    first_sma_idx = next(i for i, d in enumerate(trading_dates) if d in sma200)
    all_entries = []
    next_entry = first_sma_idx
    for i in range(first_sma_idx, len(trading_dates)):
        if i >= next_entry:
            all_entries.append((i, trading_dates[i]))
            next_entry = i + ENTRY_INTERVAL

    print(f"  Entry dates: {len(all_entries)} "
          f"({all_entries[0][1]} to {all_entries[-1][1]})")

    all_trades = []
    skipped = defaultdict(int)

    for count, (entry_idx, entry_date) in enumerate(all_entries):
        spot = spy_by_date[entry_date]
        vix = vix_data.get(entry_date, 20.0)
        iv_est = max(0.08, min(0.90, vix / 100.0))
        sma_val = sma200.get(entry_date)
        above_sma200 = spot > sma_val if sma_val else False

        # Progress
        if (count + 1) % 50 == 0 or count == 0:
            print(f"  [{count+1}/{len(all_entries)}] {entry_date}  "
                  f"SPY=${spot:.2f}  VIX={vix:.1f}  "
                  f"SMA200={'above' if above_sma200 else 'BELOW'}")

        # Find nearest MONTHLY (3rd Friday) expiration
        expiration = find_monthly_expiration(
            client, "SPY", entry_date,
            target_dte=dte_target, dte_min=dte_min, dte_max=dte_max
        )
        if not expiration:
            skipped["no_expiration"] += 1
            continue

        entry_dt = datetime.strptime(entry_date, "%Y-%m-%d").date()
        exp_dt = datetime.strptime(expiration, "%Y-%m-%d").date()
        dte_cal = (exp_dt - entry_dt).days

        # B-S strike estimate using VIX as IV
        t_years = dte_cal / 365.0
        bs_strike = find_strike_for_delta(spot, t_years, RATE, iv_est, DELTA, 'C')
        if bs_strike is None:
            skipped["bs_strike_fail"] += 1
            continue

        # Snap to real strike
        real_strike = client.snap_strike("SPY", expiration, bs_strike)
        if real_strike is None:
            skipped["no_strikes"] += 1
            continue

        # Prefetch EOD data (one API call covers entry through expiration)
        eod_data = client.prefetch_option_life(
            "SPY", expiration, real_strike, "C", entry_date
        )
        if not eod_data:
            skipped["no_eod"] += 1
            continue

        # Fetch Greeks for delta/IV verification
        greeks_data = client.get_option_greeks(
            "SPY", expiration, real_strike, "C", entry_date, expiration
        )

        eod_by_date = {r["bar_date"]: r for r in eod_data}
        greeks_by_date = {r["greeks_date"]: r for r in greeks_data}

        # Entry EOD -- deep ITM data can start late, search up to 10 days
        entry_eod = eod_by_date.get(entry_date)
        actual_entry_date = entry_date
        if not entry_eod:
            for offset in range(1, 11):
                idx = entry_idx + offset
                if idx < len(trading_dates):
                    d = trading_dates[idx]
                    if d in eod_by_date:
                        entry_eod = eod_by_date[d]
                        actual_entry_date = d
                        break
            if not entry_eod:
                skipped["no_entry_eod"] += 1
                continue

        entry_bid, entry_ask, estimated = get_usable_bid_ask(entry_eod)
        if entry_bid is None:
            skipped["no_entry_bidask"] += 1
            continue

        # Use actual entry date for spot and Greeks
        actual_spot = spy_by_date.get(actual_entry_date, spot)
        entry_greeks = greeks_by_date.get(actual_entry_date, greeks_by_date.get(entry_date))
        actual_delta = entry_greeks.get("delta") if entry_greeks else None
        actual_iv = entry_greeks.get("iv") if entry_greeks else None

        spread_pct = (entry_ask - entry_bid) / ((entry_ask + entry_bid) / 2)
        mid_price = (entry_bid + entry_ask) / 2.0

        # Find actual entry index in trading_dates
        try:
            actual_entry_idx = trading_dates.index(actual_entry_date)
        except ValueError:
            actual_entry_idx = entry_idx

        # Track trade for each scenario
        for scenario in SCENARIOS:
            ep = entry_price_for_scenario(entry_bid, entry_ask, scenario)
            if ep is None or ep <= 0:
                continue

            # Walk through subsequent trading days from actual entry
            days_held = 0
            exit_reason = None
            exit_price = None
            exit_date = None
            exit_spot = None
            exit_bid = None
            exit_ask = None

            for j in range(actual_entry_idx + 1, min(actual_entry_idx + mh + 20, len(trading_dates))):
                d = trading_dates[j]
                days_held += 1

                d_spot = spy_by_date.get(d)
                d_eod = eod_by_date.get(d)

                if d_eod is None:
                    # No option data this day
                    if days_held >= mh:
                        # MH exit with intrinsic value
                        intrinsic = max(0, (d_spot or 0) - real_strike)
                        exit_reason = "MH"
                        exit_price = intrinsic * 0.998  # tiny spread
                        exit_date = d
                        exit_spot = d_spot
                        break
                    continue

                d_bid, d_ask, d_est = get_usable_bid_ask(d_eod)
                if d_bid is None:
                    if days_held >= mh:
                        intrinsic = max(0, (d_spot or 0) - real_strike)
                        exit_reason = "MH"
                        exit_price = intrinsic * 0.998
                        exit_date = d
                        exit_spot = d_spot
                        break
                    continue

                xp = exit_price_for_scenario(d_bid, d_ask, scenario)
                if xp is None:
                    continue

                pnl_pct = xp / ep - 1

                # PT check (priority over MH)
                if pnl_pct >= PT:
                    exit_reason = "PT"
                    exit_price = xp
                    exit_date = d
                    exit_spot = d_spot
                    exit_bid = d_bid
                    exit_ask = d_ask
                    break

                # MH check
                if days_held >= mh:
                    exit_reason = "MH"
                    exit_price = xp
                    exit_date = d
                    exit_spot = d_spot
                    exit_bid = d_bid
                    exit_ask = d_ask
                    break

            if exit_reason is None:
                skipped[f"no_exit_{scenario}"] += 1
                continue

            # Compute remaining calendar DTE at exit
            exit_dt = datetime.strptime(exit_date, "%Y-%m-%d").date()
            remaining_cal_dte = (exp_dt - exit_dt).days

            all_trades.append({
                'config': label,
                'entry_date': actual_entry_date,
                'exit_date': exit_date,
                'entry_spot': actual_spot,
                'exit_spot': exit_spot,
                'stock_return': (exit_spot / actual_spot - 1) if exit_spot else None,
                'expiration': expiration,
                'strike': real_strike,
                'dte_cal': dte_cal,
                'remaining_dte_cal': remaining_cal_dte,
                'actual_delta': actual_delta,
                'actual_iv': actual_iv,
                'entry_bid': entry_bid,
                'entry_ask': entry_ask,
                'entry_mid': mid_price,
                'spread_pct': spread_pct,
                'entry_price': ep,
                'exit_price': exit_price,
                'pnl_pct': exit_price / ep - 1,
                'days_held': days_held,
                'exit_reason': exit_reason,
                'scenario': scenario,
                'above_sma200': above_sma200,
                'bid_ask_estimated': estimated,
                'mh': mh,
            })

    print(f"\n  Total trades recorded: {len(all_trades)}")
    print(f"  Skipped: {dict(skipped)}")

    return all_trades


# ═══════════════════════════════════════════════════════════════════════════
# ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════

def analyze_trades(trade_list, mh_val):
    """Analyze trades for a given filter/scenario combination."""

    if not trade_list:
        return None

    tdf = pd.DataFrame(trade_list)
    n = len(tdf)
    returns = tdf['pnl_pct'].values

    mean_ret = float(returns.mean())
    med_ret = float(np.median(returns))
    win_rate = float((returns > 0).mean())

    wins = returns[returns > 0]
    losses = returns[returns <= 0]
    avg_win = float(wins.mean()) if len(wins) > 0 else 0.0
    avg_loss = float(losses.mean()) if len(losses) > 0 else 0.0

    reasons = tdf['exit_reason'].value_counts().to_dict()

    # Sharpe -- use actual MH for annualization since trades overlap
    trades_per_year = 252.0 / ENTRY_INTERVAL
    annual_mean = mean_ret * trades_per_year
    annual_std = returns.std() * np.sqrt(trades_per_year)
    sharpe = annual_mean / annual_std if annual_std > 0 else 0.0

    # Max drawdown
    tdf_sorted = tdf.sort_values('exit_date')
    cum = np.cumprod(1 + tdf_sorted['pnl_pct'].values)
    running_max = np.maximum.accumulate(cum)
    dd = cum / running_max - 1
    max_dd = float(dd.min())

    # Stock comparison
    stock_rets = tdf['stock_return'].dropna().values
    stock_mean = float(stock_rets.mean()) if len(stock_rets) > 0 else 0.0

    # Remaining DTE at exit
    rem_dtes = tdf['remaining_dte_cal'].values
    avg_rem_dte = float(rem_dtes.mean()) if len(rem_dtes) > 0 else 0.0
    med_rem_dte = float(np.median(rem_dtes)) if len(rem_dtes) > 0 else 0.0

    # MH-exit remaining DTE
    mh_trades = tdf[tdf['exit_reason'] == 'MH']
    mh_rem_dte = float(mh_trades['remaining_dte_cal'].mean()) if len(mh_trades) > 0 else 0.0

    # Yearly breakdown
    tdf['year'] = pd.to_datetime(tdf['entry_date']).dt.year
    yearly = tdf.groupby('year').agg(
        mean_ret=('pnl_pct', 'mean'),
        n_trades=('pnl_pct', 'count'),
        win_rate=('pnl_pct', lambda x: (x > 0).mean()),
        stock_mean=('stock_return', 'mean'),
    )

    # Spread statistics
    spreads = tdf['spread_pct'].values * 100  # to percentage points
    est_count = tdf['bid_ask_estimated'].sum()

    # Days held stats
    days_held = tdf['days_held'].values

    return {
        'n': n,
        'mean_ret': mean_ret,
        'med_ret': med_ret,
        'win_rate': win_rate,
        'avg_win': avg_win,
        'avg_loss': avg_loss,
        'sharpe': sharpe,
        'max_dd': max_dd,
        'reasons': reasons,
        'stock_mean': stock_mean,
        'annual_pnl_1k': mean_ret * trades_per_year * 1000,
        'yearly': yearly,
        'spread_mean': float(spreads.mean()),
        'spread_med': float(np.median(spreads)),
        'spread_p90': float(np.percentile(spreads, 90)),
        'estimated_bidask': int(est_count),
        'avg_rem_dte': avg_rem_dte,
        'med_rem_dte': med_rem_dte,
        'mh_rem_dte': mh_rem_dte,
        'avg_days_held': float(days_held.mean()),
        'med_days_held': float(np.median(days_held)),
    }


# ═══════════════════════════════════════════════════════════════════════════
# RESULTS PRINTING
# ═══════════════════════════════════════════════════════════════════════════

def print_config_results(trades, config_name, config):
    """Print results for one configuration."""
    tdf = pd.DataFrame(trades)
    label = config["label"]
    mh = config["mh"]

    dates = sorted(tdf['entry_date'].unique())
    years = sorted(pd.to_datetime(tdf['entry_date']).dt.year.unique())

    W = 110

    print(f"\n{'=' * W}")
    print(f"CONFIG: {label}")
    print(f"  DTE target={config['dte_target']} cal days, "
          f"range=[{config['dte_min']}, {config['dte_max']}]")
    print(f"  Rules: PT=+{PT:.0%}, no stop-loss, MH={mh} tdays")
    print(f"  Period: {dates[0]} to {dates[-1]} ({len(years)} years)")
    print(f"  Rationale: {config['description']}")
    print(f"{'=' * W}")

    # ── Bid-Ask Spread + Delta + IV Statistics ────────────────────────
    entry_spreads = tdf.drop_duplicates('entry_date')['spread_pct'].values * 100
    est_count = tdf.drop_duplicates('entry_date')['bid_ask_estimated'].sum()
    total_entries = tdf['entry_date'].nunique()

    print(f"\n  Entry Spread: mean={entry_spreads.mean():.2f}%, "
          f"median={np.median(entry_spreads):.2f}%, "
          f"estimated={int(est_count)}/{total_entries}")

    deltas = tdf.drop_duplicates('entry_date')['actual_delta'].dropna().values
    if len(deltas) > 0:
        print(f"  Entry Delta:  mean={deltas.mean():.3f}, "
              f"median={np.median(deltas):.3f} (target={DELTA:.2f})")

    ivs = tdf.drop_duplicates('entry_date')['actual_iv'].dropna().values
    if len(ivs) > 0:
        print(f"  Entry IV:     mean={ivs.mean():.1%}, "
              f"median={np.median(ivs):.1%}")

    # ── DTE at entry and exit ──────────────────────────────────────────
    entry_dtes = tdf.drop_duplicates('entry_date')['dte_cal'].values
    print(f"  Entry DTE:    mean={entry_dtes.mean():.0f} cal, "
          f"median={np.median(entry_dtes):.0f} cal")

    # ── Summary by Scenario × Filter ─────────────────────────────────
    print(f"\n  {'Scenario':<12} {'Filter':<10} {'N':>5} {'Mean':>8} {'Med':>8} "
          f"{'Win%':>7} {'Sharpe':>7} {'MaxDD':>8} {'AvgW':>8} {'AvgL':>8} "
          f"{'PT%':>6} {'RemDTE':>7} {'$/yr':>8}")
    print(f"  {'-' * 108}")

    results = {}

    for scenario in SCENARIOS:
        for filter_name, filter_fn in [
            ("no_filter", lambda r: True),
            ("sma200", lambda r: r['above_sma200']),
        ]:
            subset = tdf[(tdf['scenario'] == scenario)].copy()
            if filter_name == "sma200":
                subset = subset[subset['above_sma200'] == True]

            if len(subset) == 0:
                continue

            m = analyze_trades(subset.to_dict('records'), mh)
            if m is None:
                continue

            results[(scenario, filter_name)] = m

            pt_pct = m['reasons'].get('PT', 0) / m['n'] if m['n'] > 0 else 0

            print(f"  {scenario:<12} {filter_name:<10} {m['n']:>5} "
                  f"{m['mean_ret']:>+7.1%} {m['med_ret']:>+7.1%} "
                  f"{m['win_rate']:>6.1%} {m['sharpe']:>7.2f} "
                  f"{m['max_dd']:>+7.1%} {m['avg_win']:>+7.1%} "
                  f"{m['avg_loss']:>+7.1%} {pt_pct:>5.0%} "
                  f"{m['avg_rem_dte']:>6.0f}d "
                  f"{m['annual_pnl_1k']:>+7.0f}")

    # ── Year-by-Year: Natural + SMA200 ────────────────────────────────
    key = ("natural", "sma200")
    if key in results:
        m = results[key]
        print(f"\n  Year-by-Year (Natural + SMA200):")
        print(f"  {'Year':<6} {'Trades':>7} {'Mean':>9} {'Win%':>7} {'Stock':>9}")
        print(f"  {'-' * 42}")
        for year, row in m['yearly'].iterrows():
            print(f"  {year:<6} {int(row['n_trades']):>7} {row['mean_ret']:>+8.2%} "
                  f"{row['win_rate']:>6.1%} {row['stock_mean']:>+8.2%}")

    return results


def print_comparison(results_a, results_b, config_a, config_b):
    """Print side-by-side comparison of two configurations."""

    W = 110

    print(f"\n{'=' * W}")
    print("SIDE-BY-SIDE COMPARISON: Natural (Worst Fill) + SMA200")
    print(f"{'=' * W}")

    key = ("natural", "sma200")
    a = results_a.get(key)
    b = results_b.get(key)

    if not a or not b:
        print("  Cannot compare -- one or both configs have no trades for natural+sma200")
        return

    label_a = config_a["label"]
    label_b = config_b["label"]

    print(f"\n  {'Metric':<30} {label_a:>20} {label_b:>20} {'Diff':>12}")
    print(f"  {'-' * 85}")

    rows = [
        ("Trades",             'n',            'd'),
        ("Mean return/trade",  'mean_ret',     '%'),
        ("Median return/trade",'med_ret',      '%'),
        ("Win rate",           'win_rate',     '%'),
        ("Avg win",            'avg_win',      '%'),
        ("Avg loss",           'avg_loss',     '%'),
        ("Sharpe",             'sharpe',       'f'),
        ("Max drawdown",       'max_dd',       '%'),
        ("Annual P&L per $1K", 'annual_pnl_1k','$'),
        ("Avg remaining DTE",  'avg_rem_dte',  'd'),
        ("MH-exit remaining DTE",'mh_rem_dte', 'd'),
        ("Avg days held",      'avg_days_held','d'),
        ("Spread mean%",       'spread_mean',  'bp'),
    ]

    for metric_name, key_name, fmt_type in rows:
        va = a.get(key_name, 0)
        vb = b.get(key_name, 0)
        diff = vb - va

        if fmt_type == '%':
            sa = f"{va:+.1%}"
            sb = f"{vb:+.1%}"
            sd = f"{diff:+.1%}"
        elif fmt_type == 'f':
            sa = f"{va:.2f}"
            sb = f"{vb:.2f}"
            sd = f"{diff:+.2f}"
        elif fmt_type == '$':
            sa = f"${va:+,.0f}"
            sb = f"${vb:+,.0f}"
            sd = f"${diff:+,.0f}"
        elif fmt_type == 'd':
            sa = f"{va:.0f}"
            sb = f"{vb:.0f}"
            sd = f"{diff:+.0f}"
        elif fmt_type == 'bp':
            sa = f"{va:.2f}%"
            sb = f"{vb:.2f}%"
            sd = f"{diff:+.2f}%"
        else:
            sa = f"{va}"
            sb = f"{vb}"
            sd = f"{diff}"

        print(f"  {metric_name:<30} {sa:>20} {sb:>20} {sd:>12}")

    # Exit reason breakdown
    print(f"\n  Exit Reasons:")
    for reason in ["PT", "MH"]:
        ra = a['reasons'].get(reason, 0)
        rb = b['reasons'].get(reason, 0)
        pct_a = ra / a['n'] if a['n'] > 0 else 0
        pct_b = rb / b['n'] if b['n'] > 0 else 0
        print(f"    {reason}: A={ra} ({pct_a:.0%}), B={rb} ({pct_b:.0%})")

    # ── Synthetic reference comparison ─────────────────────────────────
    print(f"\n{'=' * W}")
    print("vs SYNTHETIC BACKTEST (Natural + SMA200)")
    print(f"{'=' * W}")

    synth = {
        'mean_ret': 0.1707,
        'med_ret': 0.3421,
        'win_rate': 0.667,
        'sharpe': 1.43,
        'avg_loss': -0.361,
    }

    print(f"\n  {'Metric':<30} {'Synthetic':>14} {label_a:>20} {label_b:>20}")
    print(f"  {'-' * 87}")

    synth_rows = [
        ("Mean return/trade",  'mean_ret',  '%'),
        ("Median return/trade",'med_ret',   '%'),
        ("Win rate",           'win_rate',  '%'),
        ("Avg loss",           'avg_loss',  '%'),
        ("Sharpe",             'sharpe',    'f'),
    ]

    for metric_name, key_name, fmt_type in synth_rows:
        vs = synth.get(key_name, 0)
        va = a.get(key_name, 0)
        vb = b.get(key_name, 0)

        if fmt_type == '%':
            ss = f"{vs:+.1%}"
            sa = f"{va:+.1%}"
            sb = f"{vb:+.1%}"
        elif fmt_type == 'f':
            ss = f"{vs:.2f}"
            sa = f"{va:.2f}"
            sb = f"{vb:.2f}"
        else:
            ss = f"{vs}"
            sa = f"{va}"
            sb = f"{vb}"

        print(f"  {metric_name:<30} {ss:>14} {sa:>20} {sb:>20}")

    print(f"\n  Key question: Which config's avg loss is closer to synthetic's {synth['avg_loss']:+.1%}?")
    print(f"    Config A avg loss: {a['avg_loss']:+.1%}  "
          f"(diff from synthetic: {a['avg_loss'] - synth['avg_loss']:+.1%})")
    print(f"    Config B avg loss: {b['avg_loss']:+.1%}  "
          f"(diff from synthetic: {b['avg_loss'] - synth['avg_loss']:+.1%})")

    # ── Year-by-Year comparison ────────────────────────────────────────
    print(f"\n{'=' * W}")
    print("YEAR-BY-YEAR COMPARISON (Natural + SMA200)")
    print(f"{'=' * W}")

    ya = a['yearly']
    yb = b['yearly']
    all_years = sorted(set(ya.index.tolist() + yb.index.tolist()))

    print(f"\n  {'Year':<6}  {'--- Config A ---':^27}  {'--- Config B ---':^27}")
    print(f"  {'':6}  {'N':>5} {'Mean':>9} {'Win%':>7}  {'N':>5} {'Mean':>9} {'Win%':>7}")
    print(f"  {'-' * 65}")

    for year in all_years:
        if year in ya.index:
            ra = ya.loc[year]
            sa = f"  {int(ra['n_trades']):>5} {ra['mean_ret']:>+8.2%} {ra['win_rate']:>6.1%}"
        else:
            sa = f"  {'--':>5} {'--':>9} {'--':>7}"

        if year in yb.index:
            rb = yb.loc[year]
            sb = f"  {int(rb['n_trades']):>5} {rb['mean_ret']:>+8.2%} {rb['win_rate']:>6.1%}"
        else:
            sb = f"  {'--':>5} {'--':>9} {'--':>7}"

        print(f"  {year:<6}{sa}{sb}")


# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════

def main():
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    print("=" * 80)
    print("SPY Systematic Calls -- ThetaData DTE/MH Comparison")
    print("=" * 80)

    # Connect to ThetaData
    client = ThetaDataClient()
    if not client.connect():
        print("\nERROR: Cannot connect to Theta Terminal.")
        print("Please start Theta Terminal and try again.")
        return

    print("Connected to Theta Terminal.\n")

    # Load market data (shared across both configs)
    spy_by_date, trading_dates, vix_data, sma200 = load_market_data(client)

    # Run both configurations
    config_names = list(CONFIGS.keys())
    all_results = {}

    for config_name in config_names:
        config = CONFIGS[config_name]
        trades = run_backtest(
            client, spy_by_date, trading_dates, vix_data, sma200, config
        )

        if not trades:
            print(f"\n  WARNING: No trades for config {config_name}")
            all_results[config_name] = ([], {})
            continue

        results = print_config_results(trades, config_name, config)
        all_results[config_name] = (trades, results)

    # Side-by-side comparison
    name_a, name_b = config_names[0], config_names[1]
    _, results_a = all_results[name_a]
    _, results_b = all_results[name_b]

    if results_a and results_b:
        print_comparison(results_a, results_b, CONFIGS[name_a], CONFIGS[name_b])

    print(f"\n{'=' * 80}")
    print("DONE")
    print(f"{'=' * 80}")

    client.close()


if __name__ == "__main__":
    main()
