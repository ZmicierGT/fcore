"""Module for some globally used default variables and enumerations.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

from enum import IntEnum, Enum

def_first_date = -2147483648
def_last_date = 9999999999

# TODO LOW remove plurals here

# Enum class for standard data query rows order
class Quotes(IntEnum):
    """
        Enum class for the database query result.
    """
    DateTime = 0
    Open = 1
    High = 2
    Low = 3
    AdjClose = 4  # TODO MID Change it do just Close
    Volume = 5
    RawClose = 6
    Dividends = 7
    SplitCoefficient = 8
    Transactions = 9

class Timespans(str, Enum):
    """
        Enum class for timespans.
    """
    All = "All"  # Indicates that all data should be polled (regarding the timespan).
    Unknown = "Unknown"
    Tick = "Tick"
    Minute = "Minute"
    TwoMinutes = "2_Minutes"  # YF only
    FiveMinutes = "5_Minutes"  # All except Polygon
    TenMinutes = "10_Minutes"  # Currently not used by any supported data source. Kept for future.
    FifteenMinutes = "15_Minutes"  # All except Polygon
    TwentyMinutes = "20_Minutes"  # Currently not used by any supported data source. Kept for future.
    ThirtyMinutes = "30_Minutes"  # All except Polygon
    Hour = "Hour"  # All
    NinetyMinutes = "90_Minutes"  # YF only
    Day = "Day"
    FiveDays = "5_Days"  # YF only
    Week = "Week"
    Month = "Month"
    Quarter = "Quarter"  # Polygon and YF
    Year = "Year"

class SecType(str, Enum):
    """
        Enum class for security types.
    """
    All = "All"
    Unknown = "Unknown"
    Stock = "Stock"

class Currency(str, Enum):
    """
        Enum class for currencies.
    """
    All = "All"
    Unknown = "Unknown"
    Stock = "USD"

class ReportPeriod(str, Enum):
    """
        Enum class for reports period.
    """
    All = "All"
    Unknown = "Unknown"
    Quarter = "Quarter"
    Year = "Year"

class DbTypes(Enum):
    """
        Database types enum. Currently only SQLite is supported.
    """
    SQLite = "sqlite"
