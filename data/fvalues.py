"""Module for some globally used default variables and enumerations.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
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

# DJIA composition (EDOW for equal weighted ETF for comparison, DIA for Price weighted)
djia = ['MMM', 'AXP', 'AMGN', 'AAPL', 'BA', 'CAT', 'CVX', 'CSCO', 'KO', 'DIS', 'DOW', 'GS', 'HD', 'HON', 'IBM', 'INTC',\
        'JNJ', 'JPM', 'MCD', 'MRK', 'MSFT', 'NKE', 'PG', 'CRM', 'TRV', 'UNH', 'VZ', 'V', 'WBA', 'WMT']

# S&P 500 Composition (RSP for equal weighted ETF for comparison, SPY for Cap)
snp500 = ['A', 'AAL', 'AAPL', 'ABBV', 'ABNB', 'ABT', 'ACGL', 'ACN', 'ADBE', 'ADI', 'ADM', 'ADP', 'ADSK', 'AEE', 'AEP', \
       'AES', 'AFL', 'AIG', 'AIZ', 'AJG', 'AKAM', 'ALB', 'ALGN', 'ALK', 'ALL', 'ALLE', 'AMAT', 'AMCR', 'AMD', 'AME', \
       'AMGN', 'AMP', 'AMT', 'AMZN', 'ANET', 'ANSS', 'AON', 'AOS', 'APA', 'APD', 'APH', 'APTV', 'ARE', 'ATO', 'AVB', \
       'AVGO', 'AVY', 'AWK', 'AXON', 'AXP', 'AZO', 'BA', 'BAC', 'BALL', 'BAX', 'BBWI', 'BBY', 'BDX', 'BEN', 'BF-B', \
       'BG', 'BIIB', 'BIO', 'BK', 'BKNG', 'BKR', 'BLK', 'BMY', 'BR', 'BRK-B', 'BRO', 'BSX', 'BWA', 'BX', 'BXP', 'C', \
       'CAG', 'CAH', 'CARR', 'CAT', 'CB', 'CBOE', 'CBRE', 'CCI', 'CCL', 'CDAY', 'CDNS', 'CDW', 'CE', 'CEG', 'CF', 'CFG', \
       'CHD', 'CHRW', 'CHTR', 'CI', 'CINF', 'CL', 'CLX', 'CMA', 'CMCSA', 'CME', 'CMG', 'CMI', 'CMS', 'CNC', 'CNP', \
       'COF', 'COO', 'COP', 'COR', 'COST', 'CPB', 'CPRT', 'CPT', 'CRL', 'CRM', 'CSCO', 'CSGP', 'CSX', 'CTAS', 'CTLT', \
       'CTRA', 'CTSH', 'CTVA', 'CVS', 'CVX', 'CZR', 'D', 'DAL', 'DD', 'DE', 'DFS', 'DG', 'DGX', 'DHI', 'DHR', 'DIS', \
       'DLR', 'DLTR', 'DOV', 'DOW', 'DPZ', 'DRI', 'DTE', 'DUK', 'DVA', 'DVN', 'DXCM', 'EA', 'EBAY', 'ECL', 'ED', 'EFX', \
       'EG', 'EIX', 'EL', 'ELV', 'EMN', 'EMR', 'ENPH', 'EOG', 'EPAM', 'EQIX', 'EQR', 'EQT', 'ES', 'ESS', 'ETN', 'ETR', \
       'ETSY', 'EVRG', 'EW', 'EXC', 'EXPD', 'EXPE', 'EXR', 'F', 'FANG', 'FAST', 'FCX', 'FDS', 'FDX', 'FE', 'FFIV', 'FI', \
       'FICO', 'FIS', 'FITB', 'FLT', 'FMC', 'FOX', 'FOXA', 'FRT', 'FSLR', 'FTNT', 'FTV', 'GD', 'GE', 'GEHC', 'GEN', \
       'GILD', 'GIS', 'GL', 'GLW', 'GM', 'GNRC', 'GOOG', 'GPC', 'GPN', 'GRMN', 'GS', 'GWW', 'HAL', 'HAS', \
       'HBAN', 'HCA', 'HD', 'HES', 'HIG', 'HII', 'HLT', 'HOLX', 'HON', 'HPE', 'HPQ', 'HRL', 'HSIC', 'HST', 'HSY', \
       'HUBB', 'HUM', 'HWM', 'IBM', 'ICE', 'IDXX', 'IEX', 'IFF', 'ILMN', 'INCY', 'INTC', 'INTU', 'INVH', 'IP', 'IPG', \
       'IQV', 'IR', 'IRM', 'ISRG', 'IT', 'ITW', 'IVZ', 'J', 'JBHT', 'JCI', 'JKHY', 'JNJ', 'JNPR', 'JPM', 'K', 'KDP', \
       'KEY', 'KEYS', 'KHC', 'KIM', 'KLAC', 'KMB', 'KMI', 'KMX', 'KO', 'KR', 'KVUE', 'L', 'LDOS', 'LEN', 'LH', 'LHX', \
       'LIN', 'LKQ', 'LLY', 'LMT', 'LNT', 'LOW', 'LRCX', 'LULU', 'LUV', 'LVS', 'LW', 'LYB', 'LYV', 'MA', 'MAA', 'MAR', \
       'MAS', 'MCD', 'MCHP', 'MCK', 'MCO', 'MDLZ', 'MDT', 'MET', 'META', 'MGM', 'MHK', 'MKC', 'MKTX', 'MLM', 'MMC', \
       'MMM', 'MNST', 'MO', 'MOH', 'MOS', 'MPC', 'MPWR', 'MRK', 'MRNA', 'MRO', 'MS', 'MSCI', 'MSFT', 'MSI', 'MTB', \
       'MTCH', 'MTD', 'MU', 'NCLH', 'NDAQ', 'NDSN', 'NEE', 'NEM', 'NFLX', 'NI', 'NKE', 'NOC', 'NOW', 'NRG', 'NSC', \
       'NTAP', 'NTRS', 'NUE', 'NVDA', 'NVR', 'NWS', 'NWSA', 'NXPI', 'O', 'ODFL', 'OKE', 'OMC', 'ON', 'ORCL', 'ORLY', \
       'OTIS', 'OXY', 'PANW', 'PARA', 'PAYC', 'PAYX', 'PCAR', 'PCG', 'PEAK', 'PEG', 'PEP', 'PFE', 'PFG', 'PG', 'PGR', \
       'PH', 'PHM', 'PKG', 'PLD', 'PM', 'PNC', 'PNR', 'PNW', 'PODD', 'POOL', 'PPG', 'PPL', 'PRU', 'PSA', 'PSX', 'PTC', \
       'PWR', 'PXD', 'PYPL', 'QCOM', 'QRVO', 'RCL', 'REG', 'REGN', 'RF', 'RHI', 'RJF', 'RL', 'RMD', 'ROK', 'ROL', 'ROP', \
       'ROST', 'RSG', 'RTX', 'RVTY', 'SBAC', 'SBUX', 'SCHW', 'SEDG', 'SEE', 'SHW', 'SJM', 'SLB', 'SNA', 'SNPS', 'SO', \
       'SPG', 'SPGI', 'SRE', 'STE', 'STLD', 'STT', 'STX', 'STZ', 'SWK', 'SWKS', 'SYF', 'SYK', 'SYY', 'T', 'TAP', 'TDG', \
       'TDY', 'TECH', 'TEL', 'TER', 'TFC', 'TFX', 'TGT', 'TJX', 'TMO', 'TMUS', 'TPR', 'TRGP', 'TRMB', 'TROW', 'TRV', \
       'TSCO', 'TSLA', 'TSN', 'TT', 'TTWO', 'TXN', 'TXT', 'TYL', 'UAL', 'UDR', 'UHS', 'ULTA', 'UNH', 'UNP', 'UPS', \
       'URI', 'USB', 'V', 'VFC', 'VICI', 'VLO', 'VLTO', 'VMC', 'VRSK', 'VRSN', 'VRTX', 'VTR', 'VTRS', 'VZ', 'WAB', \
       'WAT', 'WBA', 'WBD', 'WDC', 'WEC', 'WELL', 'WFC', 'WHR', 'WM', 'WMB', 'WMT', 'WRB', 'WRK', 'WST', 'WTW', 'WY', \
       'WYNN', 'XEL', 'XOM', 'XRAY', 'XYL', 'YUM', 'ZBH', 'ZBRA', 'ZION', 'ZTS']

# SCHD Composition (Cap)
schd = ['AMGN', 'AVGO', 'VZ', 'KO', 'MRK', 'PEP', 'ABBV', 'CSCO', 'HD', 'PFE', 'TXN', 'CVX', 'UPS', 'LMT', 'BLK', 'ADP',\
        'MO', 'EOG', 'BX', 'ITW', 'MMM', 'USB', 'VLO', 'KMB', 'F', 'PAYX', 'ALL', 'FAST', 'OKE', 'NEM', 'LYB', 'TROW',\
        'CTRA', 'MTB', 'DRI', 'FITB', 'HBAN', 'NTRS', 'RF', 'SNA', 'PKG', 'K', 'TSN', 'AMCR', 'WSO', 'BBY', 'FNF', 'IP',\
        'IPG', 'NRG', 'KEY', 'WSM', 'RHI', 'UNM', 'HRB', 'PARA', 'WHR', 'FAF', 'CMA', 'ZION', 'MSM', 'WU', 'RDN', 'HUN',\
        'OZK', 'SNV', 'AAP', 'JHG', 'LAZ', 'WEN', 'LEG', 'CRI', 'CATY', 'APAM', 'MDC', 'IBOC', 'CVBF', 'BOH', 'OFG',\
        'BANR', 'AGM', 'CBRL', 'CHCO', 'NWBI', 'CNS', 'FCF', 'STC', 'KFRC', 'STBA', 'BKE', 'SRCE', 'RGR',\
        'CWEN-A', 'GES', 'PFC', 'ETD', 'HFWA', 'EBF', 'HAFC', 'CPF']

# RSPT Composition (Equal)
rspt = ['AAPL', 'ACN', 'ADBE', 'ADI', 'ADSK', 'AKAM', 'AMAT', 'AMD', 'ANET', 'ANSS', 'APH', \
        'AVGO', 'CDNS', 'CDW', 'CRM', 'CSCO', 'CTSH', 'ENPH', 'EPAM', 'FFIV', 'FICO', 'FSLR', 'FTNT', \
        'GEN', 'GLW', 'HPE', 'HPQ', 'IBM', 'INTC', 'INTU', 'IT', 'JNPR', 'KEYS', 'KLAC', 'LRCX', 'MCHP', \
        'MPWR', 'MSFT', 'MSI', 'MU', 'NOW', 'NTAP', 'NVDA', 'NXPI', 'ON', 'ORCL', 'PANW', 'PTC', 'QCOM', \
        'QRVO', 'ROP', 'SEDG', 'SNPS', 'STX', 'SWKS', 'TDY', 'TEL', 'TER', 'TRMB', 'TXN', 'TYL', 'VRSN', \
        'WDC', 'ZBRA']

# Nasdaq 100 composition (QQQE for equal weighted ETF for comparison, QQQ for Cap)
nasdaq100 = ['AAPL', 'ABNB', 'ADBE', 'ADI', 'ADP', 'ADSK', 'AEP', 'ALGN', 'AMAT', 'AMD', 'AMGN', 'AMZN', 'ANSS', 'ASML', \
             'ATVI', 'AVGO', 'AZN', 'BIIB', 'BKNG', 'BKR', 'CDNS', 'CEG', 'CHTR', 'CMCSA', 'COST', 'CPRT', 'CRWD', \
             'CSCO', 'CSGP', 'CSX', 'CTAS', 'CTSH', 'DDOG', 'DLTR', 'DXCM', 'EA', 'EBAY', 'ENPH', 'EXC', 'FANG', 'FAST', \
             'FI', 'FTNT', 'GILD', 'GOOG', 'HON', 'IDXX', 'ILMN', 'INTC', 'INTU', 'ISRG', 'JD', 'KDP', 'KHC', \
             'KLAC', 'LCID', 'LRCX', 'LULU', 'MAR', 'MCHP', 'MDLZ', 'MELI', 'META', 'MNST', 'MRNA', 'MRVL', 'MSFT', \
             'MU', 'NFLX', 'NVDA', 'NXPI', 'ODFL', 'ORLY', 'PANW', 'PAYX', 'PCAR', 'PDD', 'PEP', 'PYPL', 'QCOM', 'REGN', \
             'RIVN', 'ROST', 'SBUX', 'SGEN', 'SIRI', 'SNPS', 'TEAM', 'TMUS', 'TSLA', 'TXN', 'VRSK', 'VRTX', 'WBA',\
             'WBD', 'WDAY', 'XEL', 'ZM', 'ZS']

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
    AdjClose = 'adj_close'
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
    Day = "Day"

class SecType(StrEnum):
    """
        Enum class for security types.
    """
    All = "All"
    Unknown = "Unknown"
    Stock = "Stock"
    ETF = "ETF"

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
    """Enum with the supported algorithms for scikit-learn."""
    LR = 0
    LDA = 1
    KNC = 2
    GaussianNB = 3
    DTC = 4
    SVC = 5

class Weighted(IntEnum):
    """Enum with the supported portfolio weighting methods."""
    Equal = 0
    Price = 1
    Cap = 2
    Sector = 3
