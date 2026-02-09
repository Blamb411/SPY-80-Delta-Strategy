"""
Market Calendar Utilities
=========================
Monthly OpEx detection and trading day utilities.
Centralized to prevent implementation drift across scripts.
"""

from datetime import datetime, date, timedelta
from typing import List, Tuple, Optional
import calendar


def is_monthly_opex(exp_date) -> bool:
    """
    Check if a date is a standard monthly options expiration (3rd Friday).

    Args:
        exp_date: Date string (YYYY-MM-DD) or date object

    Returns:
        True if the date is the third Friday of its month
    """
    if isinstance(exp_date, str):
        exp_dt = datetime.strptime(exp_date, "%Y-%m-%d").date()
    elif isinstance(exp_date, datetime):
        exp_dt = exp_date.date()
    else:
        exp_dt = exp_date

    # Must be a Friday
    if exp_dt.weekday() != 4:
        return False

    # Third Friday falls between 15th and 21st
    return 15 <= exp_dt.day <= 21


def get_third_friday(year: int, month: int) -> date:
    """
    Get the third Friday of a given month.

    Args:
        year: Year (e.g., 2025)
        month: Month (1-12)

    Returns:
        Date of the third Friday
    """
    # Find the first day of the month
    first_day = date(year, month, 1)

    # Find the first Friday
    days_until_friday = (4 - first_day.weekday()) % 7
    first_friday = first_day + timedelta(days=days_until_friday)

    # Third Friday is 14 days later
    third_friday = first_friday + timedelta(days=14)

    return third_friday


def get_monthly_expirations(start_date: str, end_date: str) -> List[Tuple[str, date]]:
    """
    Generate list of monthly option expirations between two dates.

    Args:
        start_date: Start date string (YYYY-MM-DD)
        end_date: End date string (YYYY-MM-DD)

    Returns:
        List of (date_string, date_object) tuples for monthly expirations
    """
    start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()

    expirations = []

    # Start from the month of start_date
    current = date(start_dt.year, start_dt.month, 1)

    while current <= end_dt:
        third_fri = get_third_friday(current.year, current.month)

        if start_dt <= third_fri <= end_dt:
            expirations.append((third_fri.strftime("%Y-%m-%d"), third_fri))

        # Move to next month
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)

    return expirations


def filter_monthly_expirations(expirations: List[str]) -> List[Tuple[str, date]]:
    """
    Filter a list of expiration dates to only include monthly OpEx.

    Args:
        expirations: List of date strings (YYYY-MM-DD)

    Returns:
        List of (date_string, date_object) tuples for monthly expirations only
    """
    monthly = []
    for exp_str in expirations:
        if is_monthly_opex(exp_str):
            exp_dt = datetime.strptime(exp_str, "%Y-%m-%d").date()
            monthly.append((exp_str, exp_dt))

    return sorted(monthly, key=lambda x: x[1])


def find_best_expiration(
    entry_date: str,
    expirations: List[Tuple[str, date]],
    dte_target: int = 120,
    dte_min: int = 90,
    dte_max: int = 150,
) -> Tuple[Optional[str], int]:
    """
    Find the best expiration date for a given entry date.

    Args:
        entry_date: Entry date string (YYYY-MM-DD)
        expirations: List of (date_string, date_object) tuples
        dte_target: Target days to expiration (default 120)
        dte_min: Minimum acceptable DTE (default 90)
        dte_max: Maximum acceptable DTE (default 150)

    Returns:
        Tuple of (best_expiration_string, dte) or (None, 0) if none found
    """
    entry_dt = datetime.strptime(entry_date, "%Y-%m-%d").date()

    best_exp = None
    best_dte = 0
    best_diff = 9999

    for exp_str, exp_dt in expirations:
        dte = (exp_dt - entry_dt).days

        if dte_min <= dte <= dte_max:
            diff = abs(dte - dte_target)
            if diff < best_diff:
                best_diff = diff
                best_exp = exp_str
                best_dte = dte

    return best_exp, best_dte


def calculate_dte(entry_date: str, expiration_date: str) -> int:
    """
    Calculate days to expiration.

    Args:
        entry_date: Entry date string (YYYY-MM-DD)
        expiration_date: Expiration date string (YYYY-MM-DD)

    Returns:
        Days to expiration (integer)
    """
    entry_dt = datetime.strptime(entry_date, "%Y-%m-%d").date()
    exp_dt = datetime.strptime(expiration_date, "%Y-%m-%d").date()
    return (exp_dt - entry_dt).days


def is_trading_day(date_str: str, trading_dates: List[str]) -> bool:
    """
    Check if a date is a trading day.

    Args:
        date_str: Date string (YYYY-MM-DD)
        trading_dates: List of trading day strings

    Returns:
        True if the date is a trading day
    """
    return date_str in trading_dates


def get_next_trading_day(date_str: str, trading_dates: List[str]) -> Optional[str]:
    """
    Get the next trading day after a given date.

    Args:
        date_str: Date string (YYYY-MM-DD)
        trading_dates: Sorted list of trading day strings

    Returns:
        Next trading day string or None if not found
    """
    for td in trading_dates:
        if td > date_str:
            return td
    return None
