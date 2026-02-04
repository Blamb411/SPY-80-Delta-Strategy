"""
SPY 80-Delta Call Strategy - Position Alerts
=============================================
Monitors positions and sends alerts when thresholds are hit.

Alert Conditions:
  - Profit target hit (+50%) - SELL SIGNAL
  - Strong gain (+30%) - Consider taking partial profits
  - Max hold approaching (5 days) - Prepare to exit
  - Significant loss (-20%) - Review position

Can be run:
  - Manually: python position_alerts.py
  - Continuously: python position_alerts.py --watch
  - With Task Scheduler for periodic checks

Usage:
    python position_alerts.py              # Check once
    python position_alerts.py --watch      # Monitor continuously (checks every 5 min)
    python position_alerts.py --test       # Test alert system
"""

import argparse
import math
import time
import json
import os
import sys
from datetime import datetime, date, timedelta
from dataclasses import dataclass
from typing import List, Optional, Dict
from pathlib import Path

# Try to import notification libraries
try:
    from winotify import Notification, audio
    HAS_TOAST = True
except ImportError:
    HAS_TOAST = False

try:
    import winsound
    HAS_SOUND = True
except ImportError:
    HAS_SOUND = False

try:
    from ib_insync import IB, Stock, Option
    HAS_IBKR = True
except ImportError:
    HAS_IBKR = False


# ============================================================================
# CONFIGURATION
# ============================================================================

IB_HOST = "127.0.0.1"
IB_PORT = 7497
IB_CLIENT_ID = 96

# Alert thresholds
ALERT_PROFIT_TARGET = 0.50      # +50% - SELL
ALERT_STRONG_GAIN = 0.30        # +30% - Consider partial
ALERT_APPROACHING_TARGET = 0.40  # +40% - Getting close
ALERT_SIGNIFICANT_LOSS = -0.20  # -20% - Review
ALERT_MAX_HOLD_DAYS = 5         # Days before max hold

# Monitoring settings
CHECK_INTERVAL_SEC = 300  # 5 minutes
MARKET_OPEN_HOUR = 9
MARKET_OPEN_MIN = 30
MARKET_CLOSE_HOUR = 16
MARKET_CLOSE_MIN = 0

# File paths
SCRIPT_DIR = Path(__file__).parent
ALERT_LOG_FILE = SCRIPT_DIR / "alert_log.json"
ALERT_HISTORY_FILE = SCRIPT_DIR / "alert_history.json"


# ============================================================================
# POSITION DATA (Same as monitor_positions.py)
# ============================================================================

@dataclass
class Position:
    account: str
    entry_date: str
    symbol: str
    strike: float
    expiration: str
    right: str
    quantity: int
    entry_price: float
    notes: str = ""

    @property
    def position_id(self) -> str:
        return f"{self.symbol}_{self.strike}_{self.expiration}_{self.account}"

    @property
    def total_cost(self) -> float:
        return self.entry_price * 100 * self.quantity

    @property
    def profit_target_price(self) -> float:
        return self.entry_price * (1 + ALERT_PROFIT_TARGET)

    @property
    def days_held(self) -> int:
        entry = datetime.strptime(self.entry_date, "%Y-%m-%d").date()
        today = date.today()
        days = 0
        current = entry
        while current < today:
            current += timedelta(days=1)
            if current.weekday() < 5:
                days += 1
        return days

    @property
    def days_remaining(self) -> int:
        return max(0, 60 - self.days_held)

    @property
    def dte(self) -> int:
        exp = datetime.strptime(self.expiration, "%Y-%m-%d").date()
        return (exp - date.today()).days


# Open positions - KEEP IN SYNC WITH monitor_positions.py
OPEN_POSITIONS = [
    Position(
        account="IRA",
        entry_date="2026-02-03",
        symbol="SPY",
        strike=660,
        expiration="2026-06-18",
        right="C",
        quantity=10,
        entry_price=51.60,
        notes="First trade - 73 delta at entry"
    ),
]


# ============================================================================
# ALERT TYPES
# ============================================================================

@dataclass
class Alert:
    timestamp: str
    position_id: str
    alert_type: str
    message: str
    current_price: float
    pnl_pct: float
    urgency: str  # "HIGH", "MEDIUM", "LOW"

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "position_id": self.position_id,
            "alert_type": self.alert_type,
            "message": self.message,
            "current_price": self.current_price,
            "pnl_pct": self.pnl_pct,
            "urgency": self.urgency,
        }


# ============================================================================
# NOTIFICATION FUNCTIONS
# ============================================================================

def send_notification(title: str, message: str, urgency: str = "MEDIUM"):
    """Send notification via available methods."""

    timestamp = datetime.now().strftime("%H:%M:%S")

    # Console output (always)
    print()
    print("!" * 70)
    print(f"ALERT [{urgency}] - {timestamp}")
    print(f"  {title}")
    print(f"  {message}")
    print("!" * 70)
    print()

    # Sound alert
    if HAS_SOUND:
        if urgency == "HIGH":
            # Three beeps for high urgency
            for _ in range(3):
                winsound.Beep(1000, 300)
                time.sleep(0.1)
        elif urgency == "MEDIUM":
            # Two beeps
            for _ in range(2):
                winsound.Beep(800, 200)
                time.sleep(0.1)
        else:
            # One beep
            winsound.Beep(600, 150)

    # Windows toast notification
    if HAS_TOAST:
        try:
            toast = Notification(
                app_id="SPY Options Alert",
                title=title,
                msg=message,
            )
            # Set sound based on urgency
            if urgency == "HIGH":
                toast.set_audio(audio.Reminder, loop=False)
            elif urgency == "MEDIUM":
                toast.set_audio(audio.Default, loop=False)

            toast.show()
        except Exception as e:
            pass  # Toast failed, but we already printed to console


def log_alert(alert: Alert):
    """Log alert to file."""

    # Load existing alerts
    alerts = []
    if ALERT_LOG_FILE.exists():
        try:
            with open(ALERT_LOG_FILE, 'r') as f:
                alerts = json.load(f)
        except:
            alerts = []

    # Add new alert
    alerts.append(alert.to_dict())

    # Keep last 100 alerts
    alerts = alerts[-100:]

    # Save
    with open(ALERT_LOG_FILE, 'w') as f:
        json.dump(alerts, f, indent=2)


def get_alert_history() -> Dict[str, str]:
    """Get history of last alert time per position/type."""
    if ALERT_HISTORY_FILE.exists():
        try:
            with open(ALERT_HISTORY_FILE, 'r') as f:
                return json.load(f)
        except:
            return {}
    return {}


def update_alert_history(position_id: str, alert_type: str):
    """Update alert history to prevent repeated alerts."""
    history = get_alert_history()
    key = f"{position_id}_{alert_type}"
    history[key] = datetime.now().isoformat()

    with open(ALERT_HISTORY_FILE, 'w') as f:
        json.dump(history, f, indent=2)


def should_alert(position_id: str, alert_type: str, cooldown_hours: int = 1) -> bool:
    """Check if we should send this alert (not sent recently)."""
    history = get_alert_history()
    key = f"{position_id}_{alert_type}"

    if key not in history:
        return True

    last_alert = datetime.fromisoformat(history[key])
    cooldown = timedelta(hours=cooldown_hours)

    return datetime.now() - last_alert > cooldown


# ============================================================================
# IBKR CONNECTION
# ============================================================================

def connect_ibkr() -> Optional[IB]:
    if not HAS_IBKR:
        return None

    ib = IB()
    try:
        ib.connect(IB_HOST, IB_PORT, clientId=IB_CLIENT_ID, timeout=10)
        return ib
    except:
        return None


def get_spy_price(ib: IB) -> Optional[float]:
    spy = Stock("SPY", "SMART", "USD")
    ib.qualifyContracts(spy)
    ticker = ib.reqMktData(spy, '', False, False)
    ib.sleep(2)
    price = ticker.marketPrice()
    ib.cancelMktData(spy)
    return price if price > 0 else None


def get_option_price(ib: IB, pos: Position) -> Optional[float]:
    exp_str = pos.expiration.replace("-", "")
    opt = Option(pos.symbol, exp_str, pos.strike, pos.right, "SMART")

    try:
        ib.qualifyContracts(opt)
    except:
        return None

    ticker = ib.reqMktData(opt, '', False, False)
    ib.sleep(2)

    mid = None
    if ticker.bid > 0 and ticker.ask > 0:
        mid = (ticker.bid + ticker.ask) / 2
    elif ticker.last > 0:
        mid = ticker.last

    ib.cancelMktData(opt)
    return mid


# ============================================================================
# ALERT CHECKING
# ============================================================================

def check_position_alerts(pos: Position, current_price: float, spot: float) -> List[Alert]:
    """Check a position for alert conditions."""

    alerts = []
    timestamp = datetime.now().isoformat()
    pnl_pct = (current_price / pos.entry_price - 1)

    # 1. PROFIT TARGET HIT (+50%)
    if pnl_pct >= ALERT_PROFIT_TARGET:
        if should_alert(pos.position_id, "PROFIT_TARGET", cooldown_hours=0.5):
            alerts.append(Alert(
                timestamp=timestamp,
                position_id=pos.position_id,
                alert_type="PROFIT_TARGET",
                message=f"SELL NOW! {pos.symbol} ${pos.strike}C hit +{pnl_pct:.0%} "
                        f"(Target: +{ALERT_PROFIT_TARGET:.0%}). "
                        f"Current: ${current_price:.2f}, Entry: ${pos.entry_price:.2f}",
                current_price=current_price,
                pnl_pct=pnl_pct,
                urgency="HIGH"
            ))
            update_alert_history(pos.position_id, "PROFIT_TARGET")

    # 2. APPROACHING TARGET (+40%)
    elif pnl_pct >= ALERT_APPROACHING_TARGET:
        if should_alert(pos.position_id, "APPROACHING_TARGET", cooldown_hours=4):
            alerts.append(Alert(
                timestamp=timestamp,
                position_id=pos.position_id,
                alert_type="APPROACHING_TARGET",
                message=f"{pos.symbol} ${pos.strike}C at +{pnl_pct:.0%}, "
                        f"approaching +50% target. Watch closely!",
                current_price=current_price,
                pnl_pct=pnl_pct,
                urgency="MEDIUM"
            ))
            update_alert_history(pos.position_id, "APPROACHING_TARGET")

    # 3. STRONG GAIN (+30%)
    elif pnl_pct >= ALERT_STRONG_GAIN:
        if should_alert(pos.position_id, "STRONG_GAIN", cooldown_hours=8):
            alerts.append(Alert(
                timestamp=timestamp,
                position_id=pos.position_id,
                alert_type="STRONG_GAIN",
                message=f"{pos.symbol} ${pos.strike}C at +{pnl_pct:.0%}. "
                        f"Consider setting a trailing stop or taking partial profits.",
                current_price=current_price,
                pnl_pct=pnl_pct,
                urgency="LOW"
            ))
            update_alert_history(pos.position_id, "STRONG_GAIN")

    # 4. SIGNIFICANT LOSS (-20%)
    if pnl_pct <= ALERT_SIGNIFICANT_LOSS:
        if should_alert(pos.position_id, "SIGNIFICANT_LOSS", cooldown_hours=24):
            alerts.append(Alert(
                timestamp=timestamp,
                position_id=pos.position_id,
                alert_type="SIGNIFICANT_LOSS",
                message=f"{pos.symbol} ${pos.strike}C down {pnl_pct:.0%}. "
                        f"Review position. No stop-loss per strategy rules.",
                current_price=current_price,
                pnl_pct=pnl_pct,
                urgency="MEDIUM"
            ))
            update_alert_history(pos.position_id, "SIGNIFICANT_LOSS")

    # 5. MAX HOLD APPROACHING
    if pos.days_remaining <= ALERT_MAX_HOLD_DAYS and pos.days_remaining > 0:
        if should_alert(pos.position_id, "MAX_HOLD", cooldown_hours=24):
            alerts.append(Alert(
                timestamp=timestamp,
                position_id=pos.position_id,
                alert_type="MAX_HOLD",
                message=f"{pos.symbol} ${pos.strike}C has {pos.days_remaining} days "
                        f"until 60-day max hold. Current P&L: {pnl_pct:+.0%}. "
                        f"Prepare to exit.",
                current_price=current_price,
                pnl_pct=pnl_pct,
                urgency="MEDIUM"
            ))
            update_alert_history(pos.position_id, "MAX_HOLD")

    # 6. MAX HOLD REACHED
    if pos.days_remaining <= 0:
        if should_alert(pos.position_id, "MAX_HOLD_EXIT", cooldown_hours=1):
            alerts.append(Alert(
                timestamp=timestamp,
                position_id=pos.position_id,
                alert_type="MAX_HOLD_EXIT",
                message=f"SELL NOW! {pos.symbol} ${pos.strike}C reached 60-day max hold. "
                        f"Exit position. Current P&L: {pnl_pct:+.0%}",
                current_price=current_price,
                pnl_pct=pnl_pct,
                urgency="HIGH"
            ))
            update_alert_history(pos.position_id, "MAX_HOLD_EXIT")

    return alerts


def is_market_hours() -> bool:
    """Check if market is currently open."""
    now = datetime.now()

    # Weekend check
    if now.weekday() >= 5:
        return False

    # Time check
    market_open = now.replace(hour=MARKET_OPEN_HOUR, minute=MARKET_OPEN_MIN, second=0)
    market_close = now.replace(hour=MARKET_CLOSE_HOUR, minute=MARKET_CLOSE_MIN, second=0)

    return market_open <= now <= market_close


def run_alert_check(verbose: bool = True) -> List[Alert]:
    """Run one alert check cycle."""

    all_alerts = []

    if not OPEN_POSITIONS:
        if verbose:
            print("No open positions to monitor.")
        return all_alerts

    # Connect to IBKR
    ib = connect_ibkr()
    if not ib:
        if verbose:
            print("Could not connect to IBKR. Skipping this check.")
        return all_alerts

    try:
        # Get SPY price
        spot = get_spy_price(ib)
        if not spot:
            if verbose:
                print("Could not get SPY price.")
            return all_alerts

        if verbose:
            print(f"SPY: ${spot:.2f}")

        # Check each position
        for pos in OPEN_POSITIONS:
            current_price = get_option_price(ib, pos)

            if not current_price:
                if verbose:
                    print(f"Could not get price for {pos.symbol} ${pos.strike}C")
                continue

            pnl_pct = (current_price / pos.entry_price - 1) * 100

            if verbose:
                print(f"{pos.symbol} ${pos.strike}C: ${current_price:.2f} ({pnl_pct:+.1f}%)")

            # Check alerts
            alerts = check_position_alerts(pos, current_price, spot)

            for alert in alerts:
                send_notification(
                    f"SPY Options Alert - {alert.alert_type}",
                    alert.message,
                    alert.urgency
                )
                log_alert(alert)
                all_alerts.append(alert)

    finally:
        ib.disconnect()

    return all_alerts


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="Position Alert System")
    parser.add_argument("--watch", action="store_true",
                        help="Continuously monitor (every 5 min during market hours)")
    parser.add_argument("--test", action="store_true",
                        help="Test alert system")
    parser.add_argument("--interval", type=int, default=CHECK_INTERVAL_SEC,
                        help="Check interval in seconds (default: 300)")
    args = parser.parse_args()

    print()
    print("=" * 70)
    print("SPY 80-DELTA CALL STRATEGY - ALERT SYSTEM")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    print()

    # Test mode
    if args.test:
        print("Testing alert system...")
        send_notification(
            "Test Alert - HIGH",
            "This is a HIGH urgency test alert. Profit target hit!",
            "HIGH"
        )
        time.sleep(2)
        send_notification(
            "Test Alert - MEDIUM",
            "This is a MEDIUM urgency test alert.",
            "MEDIUM"
        )
        time.sleep(2)
        send_notification(
            "Test Alert - LOW",
            "This is a LOW urgency test alert.",
            "LOW"
        )
        print("\nAlert test complete.")
        return

    # Watch mode
    if args.watch:
        print(f"Starting continuous monitoring (every {args.interval} seconds)")
        print("Press Ctrl+C to stop")
        print()

        while True:
            try:
                now = datetime.now()

                if is_market_hours():
                    print(f"\n[{now.strftime('%H:%M:%S')}] Checking positions...")
                    alerts = run_alert_check(verbose=True)

                    if not alerts:
                        print("No alerts triggered.")
                else:
                    print(f"[{now.strftime('%H:%M:%S')}] Market closed. Waiting...")

                time.sleep(args.interval)

            except KeyboardInterrupt:
                print("\nStopping alert monitor.")
                break

    else:
        # Single check
        print("Running single alert check...")
        print()
        alerts = run_alert_check(verbose=True)

        if alerts:
            print(f"\n{len(alerts)} alert(s) triggered.")
        else:
            print("\nNo alerts triggered.")


if __name__ == "__main__":
    main()
