"""
Protective Put Strategy: Buy SPY puts when price approaches SMA200
Backtest for hedging 80-delta LEAPS and UPRO positions.
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
import yfinance as yf
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

SEP = "=" * 70

print(SEP)
print("PROTECTIVE PUT STRATEGY: SPY PUTS WHEN SMA BREAKS DOWN")
print("Hedge for 80-Delta LEAPS and UPRO positions")
print(SEP)

# Get daily SPY data
spy = yf.download('SPY', start='2010-01-01', end='2026-03-20', progress=False)
if isinstance(spy.columns, pd.MultiIndex):
    spy.columns = spy.columns.get_level_values(0)

close = spy['Close'].dropna()

print(f"\nSPY data: {len(close)} days ({close.index[0].strftime('%Y-%m-%d')} to "
      f"{close.index[-1].strftime('%Y-%m-%d')})")

# Compute SMA200
sma200 = close.rolling(200).mean()
ratio = close / sma200  # price / SMA ratio

# Test multiple thresholds
print(f"\n{SEP}")
print("SIGNAL ANALYSIS: HOW OFTEN DOES EACH THRESHOLD TRIGGER?")
print(SEP)

print(f"\n  {'Threshold':<20} {'Days Below':>10} {'% of Time':>9} {'# Episodes':>10} "
      f"{'Avg Duration':>12}")
print(f"  {'-'*63}")

for threshold in [1.00, 0.99, 0.98, 0.97, 0.96, 0.95]:
    below = ratio < threshold
    below_valid = below.dropna()
    days_below = below_valid.sum()
    pct = days_below / len(below_valid) * 100

    # Count episodes (consecutive runs below threshold)
    episodes = 0
    in_episode = False
    durations = []
    current_dur = 0
    for val in below_valid:
        if val and not in_episode:
            episodes += 1
            in_episode = True
            current_dur = 1
        elif val and in_episode:
            current_dur += 1
        elif not val and in_episode:
            in_episode = False
            durations.append(current_dur)
            current_dur = 0
    if in_episode:
        durations.append(current_dur)

    avg_dur = np.mean(durations) if durations else 0
    label = f"Price/SMA < {threshold:.2f}"
    print(f"  {label:<20} {int(days_below):>10} {pct:>8.1f}% {episodes:>10} {avg_dur:>11.0f}d")

# Backtest the protective put strategy
print(f"\n{SEP}")
print("BACKTEST: PROTECTIVE PUTS AT EACH THRESHOLD")
print(SEP)

print(f"""
  Strategy: When SPY price/SMA200 drops below threshold, buy 3-month
  10% OTM SPY puts. Hold until price recovers above SMA200 or puts expire.

  Put cost estimated at 2.5% of notional for 3-month 10% OTM.
  Put payoff = max(0, strike - SPY_at_exit) for intrinsic value.
  Rolls every 3 months if still below threshold.
""")

# Simulate for each threshold
for threshold in [1.00, 0.99, 0.98, 0.97, 0.96, 0.95]:
    # Track when we're in a hedge
    hedged = False
    put_entry_price = 0
    put_strike = 0
    put_expiry_idx = 0
    put_cost = 0.025  # 2.5% of notional per 3 months

    total_premium_spent = 0
    total_put_payoff = 0
    total_spy_loss_while_hedged = 0
    total_spy_gain_while_unhedged = 0
    n_puts_bought = 0
    n_puts_expired_worthless = 0
    n_puts_profitable = 0

    valid_idx = ratio.dropna().index
    for i, dt in enumerate(valid_idx):
        r = ratio[dt]
        price = close[dt]

        if not hedged and r < threshold:
            # Enter hedge: buy 3-month 10% OTM put
            hedged = True
            put_entry_price = price
            put_strike = price * 0.90  # 10% OTM
            put_expiry_idx = i + 63  # ~3 months
            premium = price * put_cost
            total_premium_spent += premium
            n_puts_bought += 1

        elif hedged:
            # Check if we should exit
            exit_hedge = False
            put_value = 0

            if r >= 1.00:
                # Price recovered above SMA — exit hedge
                exit_hedge = True
                put_value = max(0, put_strike - price)
            elif i >= put_expiry_idx:
                # Put expired
                exit_hedge = True
                put_value = max(0, put_strike - price)
                if put_value == 0:
                    n_puts_expired_worthless += 1

            if exit_hedge:
                total_put_payoff += put_value
                if put_value > 0:
                    n_puts_profitable += 1
                spy_change = (price - put_entry_price) / put_entry_price
                total_spy_loss_while_hedged += min(0, spy_change) * put_entry_price
                hedged = False

                # If still below threshold, re-enter immediately
                if r < threshold:
                    hedged = True
                    put_entry_price = price
                    put_strike = price * 0.90
                    put_expiry_idx = i + 63
                    premium = price * put_cost
                    total_premium_spent += premium
                    n_puts_bought += 1

    # Annualize
    years = len(valid_idx) / 252
    annual_cost = total_premium_spent / years
    annual_payoff = total_put_payoff / years

    label = f"Price/SMA < {threshold:.2f}"
    print(f"\n  {label}:")
    print(f"    Puts bought: {n_puts_bought} over {years:.1f} years "
          f"({n_puts_bought/years:.1f}/year)")
    print(f"    Expired worthless: {n_puts_expired_worthless} "
          f"({n_puts_expired_worthless/max(n_puts_bought,1)*100:.0f}%)")
    print(f"    Profitable: {n_puts_profitable} "
          f"({n_puts_profitable/max(n_puts_bought,1)*100:.0f}%)")
    print(f"    Total premium spent: ${total_premium_spent:,.0f}")
    print(f"    Total put payoff:    ${total_put_payoff:,.0f}")
    print(f"    Net cost:            ${total_premium_spent - total_put_payoff:+,.0f}")
    print(f"    Annual cost:         ${annual_cost:,.0f}/year")
    print(f"    Annual payoff:       ${annual_payoff:,.0f}/year")
    print(f"    Net annual:          ${annual_payoff - annual_cost:+,.0f}/year")

# Detailed analysis of the best threshold
print(f"\n{SEP}")
print("DETAILED: WHEN DID THE PUTS PAY OFF?")
print(SEP)

threshold = 0.98  # likely sweet spot
hedged = False
episodes = []
current_episode = None

valid_idx = ratio.dropna().index
for i, dt in enumerate(valid_idx):
    r = ratio[dt]
    price = close[dt]

    if not hedged and r < threshold:
        hedged = True
        current_episode = {
            'entry_date': dt, 'entry_price': price,
            'put_strike': price * 0.90,
            'premium': price * 0.025,
            'expiry_idx': i + 63,
        }
    elif hedged:
        exit_hedge = False
        if r >= 1.00 or i >= current_episode['expiry_idx']:
            exit_hedge = True

        if exit_hedge:
            put_value = max(0, current_episode['put_strike'] - price)
            spy_change = (price - current_episode['entry_price']) / current_episode['entry_price'] * 100
            current_episode['exit_date'] = dt
            current_episode['exit_price'] = price
            current_episode['put_payoff'] = put_value
            current_episode['spy_return'] = spy_change
            current_episode['net'] = put_value - current_episode['premium']
            episodes.append(current_episode)
            hedged = False

            if r < threshold:
                hedged = True
                current_episode = {
                    'entry_date': dt, 'entry_price': price,
                    'put_strike': price * 0.90,
                    'premium': price * 0.025,
                    'expiry_idx': i + 63,
                }

print(f"\n  Threshold: Price/SMA < 0.98")
print(f"  {'Entry':<12} {'Exit':<12} {'SPY Entry':>10} {'SPY Exit':>10} "
      f"{'SPY Ret':>8} {'Premium':>8} {'Payoff':>8} {'Net':>10}")
print(f"  {'-'*82}")

for ep in episodes:
    print(f"  {ep['entry_date'].strftime('%Y-%m-%d'):<12} "
          f"{ep['exit_date'].strftime('%Y-%m-%d'):<12} "
          f"${ep['entry_price']:>8.2f} ${ep['exit_price']:>8.2f} "
          f"{ep['spy_return']:>+7.1f}% "
          f"${ep['premium']:>6.0f} ${ep['put_payoff']:>6.0f} "
          f"${ep['net']:>+8.0f}")

total_net = sum(ep['net'] for ep in episodes)
total_prem = sum(ep['premium'] for ep in episodes)
total_pay = sum(ep['put_payoff'] for ep in episodes)
big_payoffs = [ep for ep in episodes if ep['put_payoff'] > 0]

print(f"\n  Summary (0.98 threshold):")
print(f"    Episodes: {len(episodes)}")
print(f"    Total premium: ${total_prem:,.0f}")
print(f"    Total payoff:  ${total_pay:,.0f}")
print(f"    Net:           ${total_net:+,.0f}")
print(f"    Profitable episodes: {len(big_payoffs)}")

if big_payoffs:
    print(f"\n    Profitable put episodes:")
    for ep in big_payoffs:
        print(f"      {ep['entry_date'].strftime('%Y-%m-%d')}: "
              f"SPY {ep['spy_return']:+.1f}%, "
              f"put paid ${ep['put_payoff']:,.0f} vs ${ep['premium']:,.0f} cost")

# Application to 80-delta and UPRO
print(f"\n{SEP}")
print("APPLICATION TO YOUR PORTFOLIO")
print(SEP)

leaps_notional = 60 * 100 * 656 * 0.80  # ~$3.1M
upro_notional = 1000 * 99 * 3  # ~$297K
total_hedge_notional = leaps_notional + upro_notional

print(f"""
  Positions to hedge:
    80-Delta LEAPS: ~${leaps_notional:,.0f} notional equity exposure
    UPRO (3x):      ~${upro_notional:,.0f} effective equity exposure
    TOTAL:          ~${total_hedge_notional:,.0f}

  At the 0.98 threshold:
    Approximate annual premium cost: {0.025 * 4 * 100:.1f}% of notional
    (buying 3-month puts ~4x/year when signal is active)

    On ${total_hedge_notional:,.0f} notional: ~${total_hedge_notional * 0.025:,.0f} per put purchase
    Signal triggers ~{len(episodes)/16:.1f}x per year on average
    Estimated annual cost: ~${total_hedge_notional * 0.025 * len(episodes)/16:,.0f}

  The put provides protection specifically when your LEAPS and UPRO
  are most vulnerable — when SPY is breaking below its 200-day SMA.
  In normal markets (price above SMA), you pay nothing.
""")
