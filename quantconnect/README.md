# QuantConnect Put Credit Spread Strategy

This folder contains the QuantConnect version of the put credit spread backtest strategy.

## Files

| File | Description |
|------|-------------|
| `put_spread_full_strategy.py` | **Full strategy** - matches original B-S backtest parameters |
| `simple_put_spread_debug.py` | Simple test version with logging |
| `put_credit_spread_strategy.py` | Initial multi-symbol version (needs refinement) |
| `COMPARISON_NOTES.md` | Detailed comparison framework |

## How to Use

### Step 1: Log into QuantConnect

Go to https://www.quantconnect.com and log in to your account.

### Step 2: Create a New Algorithm

1. Click **"Create New Algorithm"** (or go to Algorithm Lab)
2. Select **Python** as the language
3. Delete the default template code

### Step 3: Copy the Code

1. Open `put_credit_spread_strategy.py` from this folder
2. Copy the entire contents
3. Paste into the QuantConnect editor

### Step 4: Configure the Backtest

Adjust these parameters in the code as needed:

```python
# Backtest period
self.set_start_date(2020, 1, 1)
self.set_end_date(2024, 12, 31)
self.set_cash(100000)

# Symbols to trade
SYMBOLS = ["SPY", "QQQ", "IWM", "AAPL", "MSFT"]
```

**For initial testing**, start with just `["SPY"]` to run faster.

### Step 5: Run the Backtest

1. Click the **"Backtest"** button (or press Ctrl+B)
2. Wait for results (options backtests can take several minutes)
3. Review the results in the backtest report

## Strategy Parameters

These match your original `put_spread_backtest.py`:

| Parameter | Value | Description |
|-----------|-------|-------------|
| SMA_PERIOD | 200 | Price must be above 200-day SMA |
| RSI_PERIOD | 14 | RSI calculation period |
| RSI_MAX | 75 | Don't enter if RSI above this |
| IV_RANK_MIN | 0.30 | Minimum 30% IV Rank to enter |
| TARGET_DELTA | 0.25 | Short put at ~25 delta |
| SPREAD_WIDTH_PCT | 0.05 | Long put 5% below short |
| TARGET_DTE | 30 | Target days to expiration |
| TAKE_PROFIT_PCT | 0.50 | Exit at 50% of max profit |
| STOP_LOSS_MULTIPLIER | 2.0 | Exit if loss exceeds 2x credit |
| MIN_DAYS_BETWEEN_ENTRIES | 5 | Spacing between entries |

## Key Differences from Black-Scholes Backtest

| Aspect | Your Backtest | QuantConnect |
|--------|---------------|--------------|
| Option Prices | Black-Scholes theoretical | Actual historical bid/ask |
| Greeks | Calculated | Market-provided |
| Fills | Assumed mid-point | Simulated with spread |
| Data | IBKR cached | AlgoSeek OPRA data |

## Expected Results

When comparing results:

1. **Win rate** may differ due to actual vs theoretical prices
2. **Average P&L** should be more realistic (accounts for real spreads)
3. **Trade count** may vary based on actual option availability

## Troubleshooting

### "No data for symbol"
- The symbol may not have options data for that period
- Try a more liquid symbol like SPY

### Backtest runs slowly
- Options backtests are data-intensive
- Reduce the date range or number of symbols
- Use Daily resolution instead of Minute for faster (less accurate) tests

### No trades triggered
- Check if filters are too strict for the period
- Lower IV_RANK_MIN to 0.20 for testing
- Check the logs for filter failures

## Comparing to Your Original Backtest

After running the QuantConnect backtest, compare:

1. **Total trades** - Are similar numbers of trades triggered?
2. **Win rate** - How does actual data compare to B-S model?
3. **Average credit** - Are real spreads tighter or wider?
4. **P&L distribution** - Similar risk/reward profile?

This comparison will validate whether your Black-Scholes assumptions were realistic.
