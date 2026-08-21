"""
fcore Data API — unified data acquisition, caching and storage layer.
"""

__version__ = "0.1.0"

from data.fdata import FdataError, Subquery, SecData, SecFetcher

from data.stock import StockData, StockFetcher, StockDataEntries

from data.yf import YF, YFDataEntries
from data.fmp import FMP, Exchanges, FMPDataEntries

from data.fvalues import (
    DbTypes,
    Quotes, StockQuotes, Weighted, Algorithm,
    Timespans, SecType, Currency,
    ReportPeriod, Dividends, StockSplits, Sector,
    Timezones,
    def_first_date, def_last_date, trading_days_per_year,
    djia, djia_combined, djia_dict,
    sector_titles, sectors_dict, sectors_combined,
    sw20, ca_big_five, ca_big_six,
)

from data.futils import (
    get_dt, add_column, thread_available,
    show_image, gui_available, update_layout, get_labelled_ndarray,
    Log, lg,
)

__all__ = [
    "FdataError", "Subquery", "SecData", "SecFetcher",
    "StockData", "StockFetcher", "StockDataEntries",
    "YF", "YFDataEntries", "FMP", "FMPDataEntries", "Exchanges",
    "DbTypes",
    "Quotes", "StockQuotes", "Weighted", "Algorithm",
    "Timespans", "SecType", "Currency",
    "ReportPeriod", "Dividends", "StockSplits", "Sector",
    "Timezones",
    "def_first_date", "def_last_date", "trading_days_per_year",
    "djia", "djia_combined", "djia_dict",
    "sector_titles", "sectors_dict", "sectors_combined",
    "sw20", "ca_big_five", "ca_big_six",
    "get_dt", "add_column", "thread_available",
    "show_image", "gui_available", "update_layout", "get_labelled_ndarray",
    "Log", "lg",
]
