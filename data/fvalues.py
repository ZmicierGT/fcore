"""Module for some globally used default variables and enumerations."""
from enum import IntEnum

# There is no native 'StrEnum' in Python prior to 3.10 and workaround like using (str, Enum) as base
# classes does not work correctly on 3.10+ (however, it works correctly on earlier versions of Python).
# That is why we need another workaround to handle correctly both 3.10+ and prior versions.
try:
    from enum import StrEnum
except ImportError:
    from strenum import StrEnum

def_first_date = -2147483648  # Earliest supported timestamp
def_last_date = 9999999999  # Latest supported timestamp

trading_days_per_year = 252

# NOTE: KRFT Historical quotes (delisted) are not provided by any supported data source yet.
# TODO MID Delisted UTX is problematic as well as quotes and surprises may be obtained from FMP but not info or cap.
djia_jun_08_2009 = ['MMM', 'DD', 'MCD', 'AA', 'XOM', 'MRK', 'AXP', 'GE', 'MSFT', 'T', 'HPQ', 'PFE', 'BAC', 'HD', 'PG',\
                    'BA', 'INTC', 'TRV', 'CAT', 'IBM', 'UTX', 'CVX', 'JNJ', 'VZ', 'CSCO', 'JPM', 'WMT', 'KO', 'KRFT', 'DIS']

djia_sep_24_2012 = ['MMM', 'DD', 'MCD', 'AA', 'XOM', 'MRK', 'AXP', 'GE', 'MSFT', 'T', 'HPQ', 'PFE', 'BAC', 'HD', 'PG',\
                    'BA', 'INTC', 'TRV', 'CAT', 'IBM', 'UTX', 'CVX', 'JNJ', 'VZ', 'CSCO', 'JPM', 'WMT', 'KO', 'UNH', 'DIS']

djia_sep_23_2013 = ['MMM', 'DD', 'MCD', 'XOM', 'MRK', 'AXP', 'GE', 'MSFT', 'T', 'PFE', 'HD', 'PG', 'NKE', 'GS', 'V',\
                    'BA', 'INTC', 'TRV', 'CAT', 'IBM', 'UTX', 'CVX', 'JNJ', 'VZ', 'CSCO', 'JPM', 'WMT', 'KO', 'UNH', 'DIS']

djia_mar_19_2015 = ['MMM', 'DD', 'MCD', 'XOM', 'MRK', 'AXP', 'GE', 'MSFT', 'AAPL', 'PFE', 'HD', 'PG', 'NKE', 'GS', 'V',\
                    'BA', 'INTC', 'TRV', 'CAT', 'IBM', 'UTX', 'CVX', 'JNJ', 'VZ', 'CSCO', 'JPM', 'WMT', 'KO', 'UNH', 'DIS']

# Actually there were no changes in tickers - just one company was renamed. So basically index composition is as the previous
djia_sep_01_2017 = ['MMM', 'DD', 'MCD', 'XOM', 'MRK', 'AXP', 'GE', 'MSFT', 'AAPL', 'PFE', 'HD', 'PG', 'NKE', 'GS', 'V',\
                    'BA', 'INTC', 'TRV', 'CAT', 'IBM', 'UTX', 'CVX', 'JNJ', 'VZ', 'CSCO', 'JPM', 'WMT', 'KO', 'UNH', 'DIS']

# Actually there were no changes in tickers - just one company was renamed. So basically index composition is as the previous
djia_feb_01_2018 = ['MMM', 'DD', 'MCD', 'XOM', 'MRK', 'AXP', 'GE', 'MSFT', 'AAPL', 'PFE', 'HD', 'PG', 'NKE', 'GS', 'V',\
                    'BA', 'INTC', 'TRV', 'CAT', 'IBM', 'UTX', 'CVX', 'JNJ', 'VZ', 'CSCO', 'JPM', 'WMT', 'KO', 'UNH', 'DIS']

djia_jun_26_2018 = ['MMM', 'DD', 'MCD', 'XOM', 'MRK', 'AXP', 'WBA', 'MSFT', 'AAPL', 'PFE', 'HD', 'PG', 'NKE', 'GS', 'V',\
                    'BA', 'INTC', 'TRV', 'CAT', 'IBM', 'UTX', 'CVX', 'JNJ', 'VZ', 'CSCO', 'JPM', 'WMT', 'KO', 'UNH', 'DIS']

djia_apr_02_2019 = ['MMM', 'DOW', 'MCD', 'XOM', 'MRK', 'AXP', 'WBA', 'MSFT', 'AAPL', 'PFE', 'HD', 'PG', 'NKE', 'GS', 'V',\
                    'BA', 'INTC', 'TRV', 'CAT', 'IBM', 'UTX', 'CVX', 'JNJ', 'VZ', 'CSCO', 'JPM', 'WMT', 'KO', 'UNH', 'DIS']

djia_apr_06_2020 = ['MMM', 'DOW', 'MCD', 'XOM', 'MRK', 'AXP', 'WBA', 'MSFT', 'AAPL', 'PFE', 'HD', 'PG', 'NKE', 'GS', 'V',\
                    'BA', 'INTC', 'TRV', 'CAT', 'IBM', 'RTX', 'CVX', 'JNJ', 'VZ', 'CSCO', 'JPM', 'WMT', 'KO', 'UNH', 'DIS']

djia_aug_31_2020 = ['MMM', 'AXP', 'AMGN', 'AAPL', 'BA', 'CAT', 'CVX', 'CSCO', 'KO', 'DIS', 'DOW', 'GS', 'HD', 'HON', 'IBM',\
                    'INTC', 'JNJ', 'JPM', 'MCD', 'MRK', 'MSFT', 'NKE', 'PG', 'CRM', 'TRV', 'UNH', 'VZ', 'V', 'WBA', 'WMT']

djia_feb_26_2024 = ['MMM', 'AXP', 'AMGN', 'AAPL', 'BA', 'CAT', 'CVX', 'CSCO', 'KO', 'DIS', 'DOW', 'GS', 'HD', 'HON', 'IBM',\
                    'INTC', 'JNJ', 'JPM', 'MCD', 'MRK', 'MSFT', 'NKE', 'PG', 'CRM', 'TRV', 'UNH', 'VZ', 'V', 'AMZN', 'WMT']

# Note that each time sorting of the resulting list may be different
djia_combined = sorted(list(set(djia_jun_08_2009 + djia_sep_24_2012 + djia_sep_23_2013 + djia_mar_19_2015 + \
                                djia_jun_26_2018 + djia_apr_02_2019 + djia_apr_06_2020 + djia_feb_26_2024)))

djia_combined.remove('UTX')  # No historical quotes for these delisted symbols in the supported data sources
djia_combined.remove('KRFT')

djia_dict = {'2009-06-08': djia_jun_08_2009,
             '2012-09-24': djia_sep_24_2012,
             '2013-09-23': djia_sep_23_2013,
             '2015-03-19': djia_mar_19_2015,
             '2018-06-26': djia_mar_19_2015,
             '2019-04-02': djia_apr_02_2019,
             '2020-04-06': djia_apr_06_2020,
             '2020-08-31': djia_aug_31_2020,
             '2024-02-26': djia_feb_26_2024}

# The current DJIA composition (EDOW for equal weighted ETF for comparison, DIA for Price weighted)
djia = djia_feb_26_2024

sector_titles = ['Technology', 'Financial Services', 'Healthcare', 'Consumer Cyclical', 'Industrials', \
                 'Communication Services', 'Consumer Defensive', 'Energy', 'Basic Materials', 'Real Estate', 'Utilities']

###############################
# Below are sectoral ETFs lists
###############################

# Oldest possible sectoral ETFs list. The earliest date for the whole list is 30 Jun 2000.
# As it contains ETFs from different providers, the approach of management of funds may differ.
sector_etfs_oldest = ['XLK',  # Technology 24 Dec 1998
                      'XLF',  # Financial Services 24 Dec 1998
                      'XLV',  # Health Care 24 Dec 1998
                      'XLY',  # Consumer Discretionary 24 Dec 1998
                      'XLI',  # Industrials 24 Dec 1998
                      'IYZ',  # Communication services 2 Jun 2000
                      'XLP',  # Consumer Staples 24 Dec 1998
                      'XLE',  # Energy 28 Dec 1998
                      'XLB',  # Materials 24 Dec 1998
                      'IYR',  # Real Estate 30 Jun 2000
                      'XLU']  # Utilities 24 Dec 1998

# SPDR Sector ETFs. The earliest date for the whole list is 22 Jun 2018.
sector_etfs_spdr = ['XLK',  # Technology 24 Dec 1998
                    'XLF',  # Financial Services 24 Dec 1998
                    'XLV',  # Health Care 24 Dec 1998
                    'XLY',  # Consumer Discretionary 24 Dec 1998
                    'XLI',  # Industrials 24 Dec 1998
                    'XLC',  # Communication Services 22 Jun 2018
                    'XLP',  # Consumer Staples 24 Dec 1998
                    'XLE',  # Energy 28 Dec 1998
                    'XLB',  # Materials 24 Dec 1998
                    'XLRE', # Real Estate 9 Dec 2015
                    'XLU']  # Utilities 24 Dec 1998

# Vanguard Sector ETFs. The earliest date for the whole list is 1 Oct 2004.
sector_etfs_vg = ['VGT',  # Technology 30 Jan 2004
                  'VFH',  # Financial Services 30 Jan 2004
                  'VHT',  # Health Care 30 Jan 2004
                  'VCR',  # Consumer Discretionary 30 Jan 2004
                  'VIS',  # Industrials 30 Sep 2004
                  'VOX',  # Communication Services 30 Sep 2004
                  'VDC',  # Consumer Staples 30 Jan 2004
                  'VDE',  # Energy 1 Oct 2004
                  'VAW',  # Materials 30 Jan 2004
                  'VNQ',  # Read Estate 1 Oct 2004
                  'VPU']  # Utilities 30 Jan 2004

# Global sectors ETFs (US traded). The earliest date is 16 May 2008
sector_etfs_global = ['IXN',  # Technology 5 Apr 2002
                      'IXG',  # Financial Services 5 Apr 2002
                      'IXJ',  # Health Care 5 Apr 2002
                      'RXI',  # Consumer Discretionary 22 Sep 2006
                      'EXI',  # Industrials 21 Sep 2006
                      'IXP',  # Communication Services 1 Dec 2001
                      'KXI',  # Consumer Staples 22 Sep 2006
                      'IXC',  # Energy 5 Apr 2002
                      'MXI',  # Materials 22 Sep 2006
                      'RWO',  # Read Estate 16 May 2008
                      'JXI']  # Utilities 22 Sep 2006

sectors_1_oct_2004 = ['XLK', 'XLF', 'XLV', 'XLY', 'XLI', 'VOX', 'XLP', 'XLE', 'XLB', 'VNQ', 'XLU']

# Sectors compositions for long-term backtests
sectors_dict = {'2000-06-30': sector_etfs_oldest,
                '2004-10-01': sectors_1_oct_2004,
                '2018-06-22': sector_etfs_spdr}

# TODO MID Create an enum for sectors
sectors_combined = {'XLK': sector_titles[0],
                    'XLF': sector_titles[1],
                    'XLV': sector_titles[2],
                    'XLY': sector_titles[3],
                    'XLI': sector_titles[4],
                    'IYZ': sector_titles[5],
                    'VOX': sector_titles[5],
                    'XLC': sector_titles[5],
                    'XLP': sector_titles[6],
                    'XLE': sector_titles[7],
                    'XLB': sector_titles[8],
                    'IYR': sector_titles[9],
                    'VNQ': sector_titles[9],
                    'XLRE': sector_titles[9],
                    'XLU': sector_titles[10]}

# Timezon abbreviations used in data sources but which may not present on all system (and packages like tzdata).
Timezones = {
    'ACDT': 'Australia/Adelaide',
    'ACST': 'Australia/Darwin',
    'ADT':  'America/Halifax',
    'AEDT': 'Australia/Sydney',
    'AEST': 'Australia/Brisbane',
    'AKDT': 'America/Anchorage',
    'AKST': 'America/Anchorage',
    'AST':  'America/Santo_Domingo',
    'AWST': 'Australia/Perth',
    'BST':  'Europe/London',
    'CAT':  'Africa/Maputo',
    'CDT':  'America/Chicago',
    'CEST': 'Europe/Paris',
    'CET':  'Europe/Paris',
    'CST':  'Asia/Shanghai',
    'EAT':  'Africa/Nairobi',
    'EDT':  'America/New_York',
    'EEST': 'Europe/Athens',
    'EET':  'Europe/Athens',
    'EST':  'America/Jamaica',
    'GMT':  'Africa/Abidjan',
    'HDT':  'America/Adak',
    'HKT':  'Asia/Hong_Kong',
    'HST':  'Pacific/Honolulu',
    'IST':  'Asia/Kolkata',
    'JST':  'Asia/Tokyo',
    'KST':  'Asia/Seoul',
    'MDT':  'America/Denver',
    'MSK':  'Europe/Moscow',
    'MST':  'America/Denver',
    'NDT':  'America/St_Johns',
    'NST':  'America/St_Johns',
    'NZDT': 'Pacific/Auckland',
    'NZST': 'Pacific/Auckland',
    'PDT':  'America/Los_Angeles',
    'PHT':  'Asia/Manila',
    'PKT':  'Asia/Karachi',
    'PST':  'America/Los_Angeles',
    'SAST': 'Africa/Johannesburg',
    'SST':  'Pacific/Pago_Pago',
    'UTC':  'Africa/Abidjan',
    'WAT':  'Africa/Lagos',
    'WEST': 'Europe/Lisbon',
    'WET':  'Europe/Lisbon',
    'WIB':  'Asia/Jakarta',
    'WIT':  'Asia/Jayapura',
    'WITA': 'Asia/Makassar'
}

# Time zones of some popular exchanges
Exchanges = {
    'AMEX':     'America/New_York',
    'ETF':      'America/New_York',  # TODO MID It may be a problem with other regions or other data sources than FMP
    'ASX':      'Australia/Sydney',
    'BSE':      'Asia/Kolkata',
    'EURONEXT': 'Europe/Paris',
    'HKSE':     'Asia/Hong_Kong',
    'JPX':      'Asia/Tokyo',
    'LSE':      'Europe/London',
    'NASDAQ':   'America/New_York',
    'NSE':      'Asia/Kolkata',
    'NYSE':     'America/New_York',
    'OTC':      'America/New_York',
    'PNK':      'America/New_York',
    'SSE':      'Asia/Shanghai',
    'SHH':      'Asia/Shanghai',
    'SHZ':      'Asia/Shanghai',
    'TSX':      'America/Toronto',
    'XETRA':    'Europe/Frankfurt'
}

# TODO LOW remove plurals here
# Enum class for standard data query rows order
class Quotes(StrEnum):
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

class StockQuotes(StrEnum):
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
    AdjOpen = 'adj_open'
    AdjHigh = 'adj_high'
    AdjLow = 'adj_low'
    AdjClose = 'adj_close'
    AdjVolume = 'adj_volume'
    ExDividends = 'divs_ex'
    PayDividends = 'divs_pay'
    Splits = 'splits'

class Dividends(StrEnum):
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

class StockSplits(StrEnum):
    """
        Enumeration for stock splits data.
    """
    Date = 'split_date'
    Ratio = 'split_ratio'
    Source = 'source'

# TODO LOW Think that in DB only Minute, Day and in the future Tick quotes are stored.
# Other timespans are calculated manually based on the minute or EOD data.
class Timespans(StrEnum):
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
    FourHour = "4_Hours"  # FMP only
    Day = "Day"

class SecType(StrEnum):
    """
        Enum class for security types.
    """
    All = "All"
    Unknown = "Unknown"
    Stock = "Stock"
    ETF = "ETF"
    Crypto = "Crypto"

class Currency(StrEnum):
    """
        Enum class for currencies.
    """
    All = "All"
    Unknown = "Unknown"
    USD = "USD"

class ReportPeriod(StrEnum):
    """
        Enum class for reports period.
    """
    All = "All"
    Unknown = "Unknown"
    Quarter = "Quarter"
    Year = "Year"

class DbTypes(StrEnum):
    """
        Database types enum. Currently only SQLite is supported.
    """
    SQLite = "sqlite"

class Algorithm(IntEnum):
    """Enum with some algorithms for scikit-learn."""
    LR = 0
    LDA = 1
    KNC = 2
    GaussianNB = 3
    DTC = 4
    SVC = 5

class Weighted(IntEnum):
    """Enum with the supported portfolio weighting methods."""
    Unweighted = 0  # TODO HIGH Should work correctly if grouping is specified
    Equal = 1
    Price = 2
    Cap = 3
