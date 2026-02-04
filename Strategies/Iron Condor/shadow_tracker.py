"""
Shadow Tracker
===============
Manages the 16-combination position tracking per entry signal.

Each entry signal spawns 16 shadow positions:
    2 strategies (PUT, CALL)  x  2 widths (5%, 10%)
    x  2 take-profit levels (50%, 75%)  x  2 stop-loss levels (100%, 200%)

Each shadow position is independently monitored for TP, SL, and expiration.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Dict

import config

logger = logging.getLogger(__name__)


@dataclass
class ShadowPosition:
    """A single shadow (hypothetical) position."""
    # Identity
    combo_id: str           # e.g. "SPY_2023-06-15_PUT_5pct_TP50_SL100"
    underlying: str
    entry_date: str
    expiration_date: str

    # Strategy params
    strategy_type: str      # "PUT" or "CALL"
    width_pct: float        # 0.05 or 0.10
    tp_pct: float           # 0.50 or 0.75
    sl_mult: float          # 1.0 or 2.0

    # Spread details
    short_ticker: str
    long_ticker: str
    short_strike: float
    long_strike: float
    spot_at_entry: float
    credit: float           # per-share credit received
    max_loss: float         # per-contract max loss
    iv_at_entry: Optional[float]
    iv_rank_at_entry: Optional[float]
    has_real_quote: bool

    # Thresholds (computed at entry)
    tp_target: float        # close when cost_to_close <= credit * (1 - tp_pct)
    sl_trigger: float       # close when cost_to_close >= credit + credit * sl_mult

    # State
    status: str = "open"    # "open", "closed"
    exit_date: Optional[str] = None
    exit_reason: Optional[str] = None  # "take_profit", "stop_loss", "expiration"
    exit_cost: Optional[float] = None  # cost to close (per share)
    pnl: Optional[float] = None       # per-contract P&L
    dte: int = 0


def create_combo_id(
    underlying: str,
    entry_date: str,
    strategy_type: str,
    width_pct: float,
    tp_pct: float,
    sl_mult: float,
) -> str:
    width_label = f"{int(width_pct*100)}pct"
    tp_label = f"TP{int(tp_pct*100)}"
    sl_label = f"SL{int(sl_mult*100)}"
    return f"{underlying}_{entry_date}_{strategy_type}_{width_label}_{tp_label}_{sl_label}"


class ShadowTracker:
    """Tracks all shadow positions across all tickers."""

    def __init__(self):
        self.positions: List[ShadowPosition] = []
        self._closed: List[ShadowPosition] = []

    @property
    def open_positions(self) -> List[ShadowPosition]:
        return [p for p in self.positions if p.status == "open"]

    @property
    def closed_positions(self) -> List[ShadowPosition]:
        return self._closed

    @property
    def all_positions(self) -> List[ShadowPosition]:
        return self.positions + self._closed

    def open_count_for_ticker(self, ticker: str) -> int:
        """Count open entry signals (not combos) for a ticker.

        Each entry signal has up to 16 combos. We count unique entry dates.
        """
        entry_dates = set()
        for p in self.open_positions:
            if p.underlying == ticker:
                entry_dates.add(p.entry_date)
        return len(entry_dates)

    def last_entry_date_for_ticker(self, ticker: str) -> Optional[str]:
        """Get the most recent entry date for a ticker."""
        dates = []
        for p in self.positions:
            if p.underlying == ticker:
                dates.append(p.entry_date)
        return max(dates) if dates else None

    def can_enter(self, ticker: str, trade_date: str) -> bool:
        """Check if we can open a new entry signal for this ticker today."""
        # Check concurrent limit
        if self.open_count_for_ticker(ticker) >= config.MAX_CONCURRENT_PER_TICKER:
            return False

        # Check minimum days between entries
        last = self.last_entry_date_for_ticker(ticker)
        if last:
            last_dt = datetime.strptime(last, "%Y-%m-%d").date()
            today = datetime.strptime(trade_date, "%Y-%m-%d").date()
            if (today - last_dt).days < config.MIN_DAYS_BETWEEN_ENTRIES:
                return False

        return True

    def add_position(self, pos: ShadowPosition) -> None:
        """Add a new shadow position."""
        self.positions.append(pos)

    def close_position(
        self,
        pos: ShadowPosition,
        exit_date: str,
        exit_reason: str,
        exit_cost: float,
    ) -> None:
        """Close a shadow position and compute P&L."""
        pos.status = "closed"
        pos.exit_date = exit_date
        pos.exit_reason = exit_reason
        pos.exit_cost = exit_cost

        # P&L = (credit - exit_cost) * 100 per contract
        pos.pnl = round((pos.credit - exit_cost) * 100, 2)

        # Move to closed list
        if pos in self.positions:
            self.positions.remove(pos)
        self._closed.append(pos)

    def check_exit_conditions(
        self,
        pos: ShadowPosition,
        current_date: str,
        cost_to_close: Optional[float],
        spot: float,
    ) -> Optional[Dict]:
        """Check if a position should be exited.

        Returns dict with exit info or None if position stays open.
        """
        # Check expiration
        exp_dt = datetime.strptime(pos.expiration_date, "%Y-%m-%d").date()
        cur_dt = datetime.strptime(current_date, "%Y-%m-%d").date()

        if cur_dt >= exp_dt:
            # Expired — settle at intrinsic
            from spread_builder import intrinsic_value_at_expiration
            settlement = intrinsic_value_at_expiration(
                spot, pos.short_strike, pos.long_strike, pos.strategy_type,
            )
            return {
                "reason": "expiration",
                "exit_cost": settlement,
            }

        if cost_to_close is None:
            return None

        # Take profit: close if cost to close is cheap enough
        # TP target: we keep tp_pct of the credit
        # E.g. credit=1.00, tp_pct=0.50 => close when cost_to_close <= 0.50
        remaining_value = pos.credit * (1 - pos.tp_pct)
        if cost_to_close <= remaining_value:
            return {
                "reason": "take_profit",
                "exit_cost": cost_to_close,
            }

        # Stop loss: close if loss exceeds threshold
        # SL trigger: close when cost_to_close >= credit + credit * sl_mult
        # E.g. credit=1.00, sl_mult=1.0 => close when cost_to_close >= 2.00
        sl_threshold = pos.credit + pos.credit * pos.sl_mult
        if cost_to_close >= sl_threshold:
            return {
                "reason": "stop_loss",
                "exit_cost": cost_to_close,
            }

        return None

    def get_positions_needing_monitoring(self, current_date: str) -> List[ShadowPosition]:
        """Get open positions that need daily price checks."""
        return [p for p in self.open_positions]

    def get_unique_option_tickers_to_monitor(self) -> Dict[str, List[ShadowPosition]]:
        """Group open positions by their option tickers for efficient data fetching.

        Returns dict mapping option_ticker -> list of positions using it.
        """
        ticker_map: Dict[str, List[ShadowPosition]] = {}
        for p in self.open_positions:
            for ot in [p.short_ticker, p.long_ticker]:
                if ot not in ticker_map:
                    ticker_map[ot] = []
                ticker_map[ot].append(p)
        return ticker_map

    def summary(self) -> Dict:
        """Return a summary of all positions."""
        all_pos = self.all_positions
        open_pos = self.open_positions
        closed = self.closed_positions

        wins = [p for p in closed if p.pnl and p.pnl > 0]
        losses = [p for p in closed if p.pnl and p.pnl <= 0]

        return {
            "total_positions": len(all_pos),
            "open": len(open_pos),
            "closed": len(closed),
            "wins": len(wins),
            "losses": len(losses),
            "win_rate": len(wins) / len(closed) if closed else 0,
            "total_pnl": sum(p.pnl for p in closed if p.pnl),
            "avg_pnl": (sum(p.pnl for p in closed if p.pnl) / len(closed)
                        if closed else 0),
        }
