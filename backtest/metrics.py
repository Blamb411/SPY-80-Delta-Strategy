"""
Performance Metrics
===================
Consistent Sharpe, Sortino, CAGR, and other metric calculations.
Centralized to ensure all scripts report metrics the same way.
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass


@dataclass
class PerformanceMetrics:
    """Container for strategy performance metrics."""
    total_return: float
    cagr: float
    sharpe_ratio: float
    sortino_ratio: float
    max_drawdown: float
    max_drawdown_date: Optional[str] = None
    volatility: float = 0.0
    downside_deviation: float = 0.0
    calmar_ratio: float = 0.0
    trading_days: int = 0
    years: float = 0.0

    def to_dict(self) -> Dict:
        """Convert to dictionary for logging."""
        return {
            "total_return": f"{self.total_return:+.1%}",
            "cagr": f"{self.cagr:+.1%}",
            "sharpe_ratio": f"{self.sharpe_ratio:.3f}",
            "sortino_ratio": f"{self.sortino_ratio:.3f}",
            "max_drawdown": f"{self.max_drawdown:.1%}",
            "max_drawdown_date": self.max_drawdown_date,
            "volatility": f"{self.volatility:.1%}",
            "calmar_ratio": f"{self.calmar_ratio:.2f}",
        }

    def print_summary(self, label: str = "Performance"):
        """Print formatted summary."""
        print(f"\n{label}:")
        print(f"  CAGR:           {self.cagr:+.1%}")
        print(f"  Sharpe Ratio:   {self.sharpe_ratio:.3f}")
        print(f"  Sortino Ratio:  {self.sortino_ratio:.3f}")
        print(f"  Max Drawdown:   {self.max_drawdown:.1%}")
        print(f"  Volatility:     {self.volatility:.1%}")
        if self.max_drawdown_date:
            print(f"  Max DD Date:    {self.max_drawdown_date}")


def calculate_returns(values: List[float]) -> np.ndarray:
    """
    Calculate daily returns from a series of portfolio values.

    Args:
        values: List of daily portfolio values

    Returns:
        NumPy array of daily returns (first element is 0)
    """
    values = np.array(values)
    returns = np.zeros(len(values))
    returns[1:] = values[1:] / values[:-1] - 1
    return returns


def calculate_cagr(start_value: float, end_value: float, years: float) -> float:
    """
    Calculate Compound Annual Growth Rate.

    Args:
        start_value: Initial portfolio value
        end_value: Final portfolio value
        years: Number of years

    Returns:
        CAGR as decimal (e.g., 0.15 for 15%)
    """
    if start_value <= 0 or end_value <= 0 or years <= 0:
        return 0.0

    return (end_value / start_value) ** (1 / years) - 1


def calculate_sharpe_ratio(
    returns: np.ndarray,
    risk_free_rate: float = 0.0,
    annualization_factor: float = 252,
) -> float:
    """
    Calculate annualized Sharpe Ratio.

    Formula: (mean_return - rf) / std_return * sqrt(252)

    Args:
        returns: Array of daily returns
        risk_free_rate: Annual risk-free rate (default 0)
        annualization_factor: Trading days per year (default 252)

    Returns:
        Sharpe ratio
    """
    if len(returns) < 2:
        return 0.0

    # Remove any NaN/inf values
    returns = returns[np.isfinite(returns)]
    if len(returns) < 2:
        return 0.0

    mean_return = np.mean(returns)
    std_return = np.std(returns, ddof=1)

    if std_return == 0:
        return 0.0

    # Convert annual risk-free rate to daily
    daily_rf = risk_free_rate / annualization_factor

    sharpe = (mean_return - daily_rf) / std_return * np.sqrt(annualization_factor)
    return sharpe


def calculate_sortino_ratio(
    returns: np.ndarray,
    risk_free_rate: float = 0.0,
    annualization_factor: float = 252,
) -> float:
    """
    Calculate annualized Sortino Ratio.

    Like Sharpe but uses only downside deviation (penalizes downside volatility only).

    Formula: (mean_return - rf) / downside_deviation * sqrt(252)

    Args:
        returns: Array of daily returns
        risk_free_rate: Annual risk-free rate (default 0)
        annualization_factor: Trading days per year (default 252)

    Returns:
        Sortino ratio
    """
    if len(returns) < 2:
        return 0.0

    # Remove any NaN/inf values
    returns = returns[np.isfinite(returns)]
    if len(returns) < 2:
        return 0.0

    mean_return = np.mean(returns)

    # Calculate downside deviation (only negative returns)
    daily_rf = risk_free_rate / annualization_factor
    downside_returns = returns[returns < daily_rf] - daily_rf

    if len(downside_returns) == 0:
        return float('inf') if mean_return > daily_rf else 0.0

    downside_deviation = np.sqrt(np.mean(downside_returns ** 2))

    if downside_deviation == 0:
        return float('inf') if mean_return > daily_rf else 0.0

    sortino = (mean_return - daily_rf) / downside_deviation * np.sqrt(annualization_factor)
    return sortino


def calculate_max_drawdown(values: List[float]) -> Tuple[float, Optional[str], Optional[int]]:
    """
    Calculate maximum drawdown from peak.

    Args:
        values: List of daily portfolio values

    Returns:
        Tuple of (max_drawdown, date_of_max_dd, index_of_max_dd)
        max_drawdown is negative (e.g., -0.32 for -32%)
    """
    if len(values) < 2:
        return 0.0, None, None

    values = np.array(values)
    cummax = np.maximum.accumulate(values)
    drawdown = values / cummax - 1

    max_dd = np.min(drawdown)
    max_dd_idx = np.argmin(drawdown)

    return max_dd, None, max_dd_idx


def calculate_volatility(returns: np.ndarray, annualization_factor: float = 252) -> float:
    """
    Calculate annualized volatility.

    Args:
        returns: Array of daily returns
        annualization_factor: Trading days per year

    Returns:
        Annualized volatility as decimal
    """
    if len(returns) < 2:
        return 0.0

    returns = returns[np.isfinite(returns)]
    if len(returns) < 2:
        return 0.0

    return np.std(returns, ddof=1) * np.sqrt(annualization_factor)


def calculate_calmar_ratio(cagr: float, max_drawdown: float) -> float:
    """
    Calculate Calmar Ratio (CAGR / |Max Drawdown|).

    Args:
        cagr: Compound annual growth rate
        max_drawdown: Maximum drawdown (negative number)

    Returns:
        Calmar ratio
    """
    if max_drawdown >= 0:
        return float('inf') if cagr > 0 else 0.0

    return cagr / abs(max_drawdown)


def calculate_all_metrics(
    values: List[float],
    dates: Optional[List[str]] = None,
    risk_free_rate: float = 0.0,
) -> PerformanceMetrics:
    """
    Calculate all performance metrics from daily portfolio values.

    Args:
        values: List of daily portfolio values
        dates: Optional list of date strings (for max DD date)
        risk_free_rate: Annual risk-free rate

    Returns:
        PerformanceMetrics dataclass with all metrics
    """
    if len(values) < 2:
        return PerformanceMetrics(
            total_return=0.0,
            cagr=0.0,
            sharpe_ratio=0.0,
            sortino_ratio=0.0,
            max_drawdown=0.0,
        )

    values = np.array(values)
    trading_days = len(values)
    years = trading_days / 252.0

    # Basic metrics
    total_return = values[-1] / values[0] - 1
    cagr = calculate_cagr(values[0], values[-1], years)

    # Returns-based metrics
    returns = calculate_returns(values)
    sharpe = calculate_sharpe_ratio(returns, risk_free_rate)
    sortino = calculate_sortino_ratio(returns, risk_free_rate)
    volatility = calculate_volatility(returns)

    # Drawdown
    max_dd, _, max_dd_idx = calculate_max_drawdown(values)
    max_dd_date = dates[max_dd_idx] if dates and max_dd_idx is not None else None

    # Calmar
    calmar = calculate_calmar_ratio(cagr, max_dd)

    # Downside deviation
    daily_rf = risk_free_rate / 252
    downside_returns = returns[returns < daily_rf] - daily_rf
    downside_dev = np.sqrt(np.mean(downside_returns ** 2)) * np.sqrt(252) if len(downside_returns) > 0 else 0

    return PerformanceMetrics(
        total_return=total_return,
        cagr=cagr,
        sharpe_ratio=sharpe,
        sortino_ratio=sortino,
        max_drawdown=max_dd,
        max_drawdown_date=max_dd_date,
        volatility=volatility,
        downside_deviation=downside_dev,
        calmar_ratio=calmar,
        trading_days=trading_days,
        years=years,
    )


@dataclass
class TradeStats:
    """Container for trade statistics."""
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: float
    total_pnl: float
    avg_win: float
    avg_loss: float
    avg_return: float
    median_return: float
    profit_factor: float
    avg_days_held: float

    def print_summary(self, label: str = "Trade Statistics"):
        """Print formatted summary."""
        print(f"\n{label}:")
        print(f"  Total Trades:   {self.total_trades}")
        print(f"  Win Rate:       {self.win_rate:.1%}")
        print(f"  Total P&L:      ${self.total_pnl:,.0f}")
        print(f"  Avg Win:        {self.avg_win:+.1%}")
        print(f"  Avg Loss:       {self.avg_loss:+.1%}")
        print(f"  Profit Factor:  {self.profit_factor:.2f}")
        print(f"  Avg Days Held:  {self.avg_days_held:.0f}")


def calculate_trade_stats(trades: List[Dict]) -> TradeStats:
    """
    Calculate trade statistics from a list of trade records.

    Args:
        trades: List of dicts with 'pnl_pct', 'pnl_dollar', 'days_held'

    Returns:
        TradeStats dataclass
    """
    if not trades:
        return TradeStats(
            total_trades=0,
            winning_trades=0,
            losing_trades=0,
            win_rate=0.0,
            total_pnl=0.0,
            avg_win=0.0,
            avg_loss=0.0,
            avg_return=0.0,
            median_return=0.0,
            profit_factor=0.0,
            avg_days_held=0.0,
        )

    pnl_pcts = [t.get("pnl_pct", 0) for t in trades]
    pnl_dollars = [t.get("pnl_dollar", 0) for t in trades]
    days_held = [t.get("days_held", 0) for t in trades]

    total_trades = len(trades)
    winning_trades = sum(1 for p in pnl_pcts if p > 0)
    losing_trades = sum(1 for p in pnl_pcts if p <= 0)
    win_rate = winning_trades / total_trades if total_trades > 0 else 0

    winning_pcts = [p for p in pnl_pcts if p > 0]
    losing_pcts = [p for p in pnl_pcts if p <= 0]

    avg_win = np.mean(winning_pcts) if winning_pcts else 0
    avg_loss = np.mean(losing_pcts) if losing_pcts else 0
    avg_return = np.mean(pnl_pcts)
    median_return = np.median(pnl_pcts)

    total_gains = sum(d for d in pnl_dollars if d > 0)
    total_losses = abs(sum(d for d in pnl_dollars if d < 0))
    profit_factor = total_gains / total_losses if total_losses > 0 else float('inf')

    return TradeStats(
        total_trades=total_trades,
        winning_trades=winning_trades,
        losing_trades=losing_trades,
        win_rate=win_rate,
        total_pnl=sum(pnl_dollars),
        avg_win=avg_win,
        avg_loss=avg_loss,
        avg_return=avg_return,
        median_return=median_return,
        profit_factor=profit_factor,
        avg_days_held=np.mean(days_held) if days_held else 0,
    )


def print_comparison_table(
    results: List[Dict],
    metrics: List[str] = None,
    label_key: str = "name",
) -> None:
    """
    Print a formatted comparison table of multiple strategies.

    Args:
        results: List of dicts with strategy results
        metrics: List of metric keys to display (default: common set)
        label_key: Key to use for row labels
    """
    if metrics is None:
        metrics = ["cagr", "sharpe", "sortino", "max_dd", "trades", "win_rate", "total_pnl"]

    # Header
    col_width = 12
    header = f"  {'Strategy':<20}"
    for m in metrics:
        header += f" {m:>{col_width}}"
    print(header)
    print("  " + "-" * (20 + len(metrics) * (col_width + 1)))

    # Rows
    for r in results:
        row = f"  {r.get(label_key, 'Unknown'):<20}"
        for m in metrics:
            val = r.get(m, "N/A")
            if isinstance(val, float):
                if "pct" in m or m in ["cagr", "max_dd", "win_rate"]:
                    row += f" {val:>{col_width}.1%}"
                else:
                    row += f" {val:>{col_width}.3f}"
            elif isinstance(val, int):
                row += f" {val:>{col_width},}"
            else:
                row += f" {str(val):>{col_width}}"
        print(row)
