"""Module for some globally used default variables and enumerations.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""

from enum import IntEnum, Enum

def_first_date = -2147483648  # Earliest supported timestamp
def_last_date = 9999999999  # Latest supported timestamp

# TODO MID Think if these enums are needed if we switch to labelled numpy arrays.
# TODO LOW Switch these enumerations to StrEnum when appropriate versions of Python become more popular.
# TODO LOW remove plurals here
# Enum class for standard data query rows order
class Quotes(str, Enum):
    """
        Enum class for the database query result.
    """
    TimeStamp = 'time_stamp'
    DateTime = 'date_time'
    Open = 'opened'
    High = 'high'
    Low = 'low'
    Close = 'closed'
    Volume = 'volume'
    Transactions = 'transactions'

class StockQuotes(str, Enum):
    """
        Enum class for the database stock quote query result.
    """
    TimeStamp = 'time_stamp'
    DateTime = 'date_time'
    Open = 'opened'
    High = 'high'
    Low = 'low'
    Close = 'closed'
    Volume = 'volume'
    Transactions = 'transactions'
    AdjClose = 'adj_close'
    ExDividends = 'divs_ex'
    PayDividends = 'divs_pay'
    Splits = 'splits'

class Dividends(str, Enum):
    """
        Enumeration for stock dividends data.
    """
    DeclDate = 'declaration_date'
    ExDate = 'ex_date'
    RecordDate = 'record_date'
    PaymentDate = 'payment_date'
    Amount = 'amount'
    Currency = 'currency'
    Source = 'source'

class StockSplits(str, Enum):
    """
        Enumeration for stock splits data.
    """
    Date = 'split_date'
    Ratio = 'split_ratio'
    Source = 'source'

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
    USD = "USD"

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

class Algorithm(IntEnum):
    """Enum with the supported algorithms for scikit-learn."""
    LR = 0
    LDA = 1
    KNC = 2
    GaussianNB = 3
    DTC = 4
    SVC = 5
