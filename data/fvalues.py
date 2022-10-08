"""Module for some globally used default variables and enumerations.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

from enum import IntEnum, Enum

# Enum class for standard data query rows order
class Rows(IntEnum):
    """
        Enum class for the database query result.
    """
    Symbol = 0
    ISIN = 1
    Source = 2
    DateTime = 3
    Timespan = 4
    Open = 5
    High = 6
    Low = 7
    Close = 8
    AdjClose = 9
    Volume = 10
    Dividends = 11
    Transactions = 12
    VWAP = 13

class Timespans(str, Enum):
    """
        Enum class for timespans.
    """
    All = "All"
    Unknown = "Unknown"
    Intraday = "Intraday"
    OneMinute = "OneMinute"
    FiveMinutes = "FiveMinutes"
    TenMinutes = "TenMinutes"
    FifteenMinutes = "FifteenMinutes"
    TwentyMinutes = "TwentyMinutes"
    ThirtyMinutes = "ThirtyMinutes"
    OneHour = "OneHour"
    Day = "Day"
    Week = "Week"
    Month = "Month"
    Year = "Year"
