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

djia_nov_08_2024 = ['MMM', 'AXP', 'AMGN', 'AAPL', 'BA', 'CAT', 'CVX', 'CSCO', 'KO', 'DIS', 'SHW', 'GS', 'HD', 'HON', 'IBM',\
                    'NVDA', 'JNJ', 'JPM', 'MCD', 'MRK', 'MSFT', 'NKE', 'PG', 'CRM', 'TRV', 'UNH', 'VZ', 'V', 'AMZN', 'WMT']

# Note that each time sorting of the resulting list may be different
djia_combined = sorted(list(set(djia_jun_08_2009 + djia_sep_24_2012 + djia_sep_23_2013 + djia_mar_19_2015 + \
                                djia_jun_26_2018 + djia_apr_02_2019 + djia_apr_06_2020 + djia_feb_26_2024 + \
                                djia_nov_08_2024)))

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
             '2024-02-26': djia_feb_26_2024,
             '2024-11-08': djia_nov_08_2024}

# The current DJIA composition (EDOW for equal weighted ETF for comparison, DIA for Price weighted)
djia = djia_nov_08_2024

sector_titles = ['Technology', 'Financial Services', 'Healthcare', 'Consumer Cyclical', 'Industrials', \
                 'Communication Services', 'Consumer Defensive', 'Energy', 'Basic Materials', 'Real Estate', 'Utilities']

########################################
# SW20 (Swiss Market Index) related data
########################################

# ETFs for comparison:
# XSMI.SW - CHF Dist (1/2008)
# XSMC.SW - CHF Acc (7/2013)
# EWL - USD Dist (Likely MSCI Switzerland based, 3/1996)

# The following tickers are missed from yfinance / FMP data:
# Credit Suisse Group - CSGN.SW / CS
# Syngenta - SYNN.SW / SYT (ADR ticker is taken by other company)
# Actelion - ATLN.SW / ALIOY (No ADR data on FMP)
# Transocean - RIGN.SW / RIG (ADR present on yfinance)
# Synthes - SYST.SW / SYSTY (No ADR data on FMP)
# Nobel Biocare - NOBN.SW / NBHYY (No ADR data on FMP)

# Credit Suisse Group (CSGN.SW) was replaced by Kuehne + Nagel International AG (KNIN.SW)
sw20_06_jun_23 = ['NESN.SW', 'ROG.SW', 'NOVN.SW', 'CFR.SW', 'ZURN.SW', 'UBSG.SW', 'ABBN.SW', 'LONN.SW', 'SIKA.SW', \
                  'ALC.SW', 'GIVN.SW', 'HOLN.SW', 'SCMN.SW', 'PGHN.SW', 'SREN.SW', 'SOON.SW', 'GEBN.SW', 'SLHN.SW', \
                  'LOGN.SW', 'KNIN.SW']

# SGS SA (SGSN.SW) was replaced with Sonova (SOON.SW)
sw20_24_sep_22 = ['NESN.SW', 'ROG.SW', 'NOVN.SW', 'CFR.SW', 'ZURN.SW', 'UBSG.SW', 'ABBN.SW', 'LONN.SW', 'SIKA.SW', \
                  'ALC.SW', 'GIVN.SW', 'HOLN.SW', 'SCMN.SW', 'PGHN.SW', 'SREN.SW', 'SOON.SW', 'GEBN.SW', 'SLHN.SW', \
                  'LOGN.SW', 'CSGN.SW']

# Swatch Group (UHR.SW) was replaced with Logitech (LOGN.SW)
sw20_05_sep_21 = ['NESN.SW', 'ROG.SW', 'NOVN.SW', 'CFR.SW', 'ZURN.SW', 'UBSG.SW', 'ABBN.SW', 'LONN.SW', 'SIKA.SW', \
                  'ALC.SW', 'GIVN.SW', 'HOLN.SW', 'SCMN.SW', 'PGHN.SW', 'SREN.SW', 'SGSN.SW', 'GEBN.SW', 'SLHN.SW', \
                  'LOGN.SW', 'CSGN.SW']

# Skipped (May 2021) as the only change was LafargeHolcim renamed to Holcim.

# Adecco (ADEN.SW) replaced by Partners Group (PGHN.SW)
sw20_03_sep_20 = ['NESN.SW', 'ROG.SW', 'NOVN.SW', 'CFR.SW', 'ZURN.SW', 'UBSG.SW', 'ABBN.SW', 'LONN.SW', 'SIKA.SW', \
                  'ALC.SW', 'GIVN.SW', 'HOLN.SW', 'SCMN.SW', 'PGHN.SW', 'SREN.SW', 'SGSN.SW', 'GEBN.SW', 'SLHN.SW', \
                  'UHR.SW', 'CSGN.SW']

# Julius Bar (BAER.SW) replaced by Alcon (ALC.SW)
sw20_27_mar_19 = ['NESN.SW', 'ROG.SW', 'NOVN.SW', 'CFR.SW', 'ZURN.SW', 'UBSG.SW', 'ABBN.SW', 'LONN.SW', 'SIKA.SW', \
                  'ALC.SW', 'GIVN.SW', 'HOLN.SW', 'SCMN.SW', 'ADEN.SW', 'SREN.SW', 'SGSN.SW', 'GEBN.SW', 'SLHN.SW', \
                  'UHR.SW', 'CSGN.SW']

# Sika AG (SIKA.SW) replaced Syngenta (SYNN.SW) after its purchase by ChemChina
sw20_05_may_17 = ['NESN.SW', 'ROG.SW', 'NOVN.SW', 'CFR.SW', 'ZURN.SW', 'UBSG.SW', 'ABBN.SW', 'LONN.SW', 'SIKA.SW', \
                  'BAER.SW', 'GIVN.SW', 'HOLN.SW', 'SCMN.SW', 'ADEN.SW', 'SREN.SW', 'SGSN.SW', 'GEBN.SW', 'SLHN.SW', \
                  'UHR.SW', 'CSGN.SW']

# Actelion (ATLN.SW) replaced by Lonza Group (LONN.SW) after Actelion's purchase by Johnson & Johnson
sw20_24_apr_17 = ['NESN.SW', 'ROG.SW', 'NOVN.SW', 'CFR.SW', 'ZURN.SW', 'UBSG.SW', 'ABBN.SW', 'LONN.SW', 'BAER.SW', \
                  'SYNN.SW', 'GIVN.SW', 'HOLN.SW', 'SCMN.SW', 'ADEN.SW', 'SREN.SW', 'SGSN.SW', 'GEBN.SW', 'SLHN.SW', \
                  'UHR.SW', 'CSGN.SW']

# Transocean (RIGN.SW) replaced by Swiss Life (SLHN.SW)
sw20_14_jan_16 = ['NESN.SW', 'ROG.SW', 'NOVN.SW', 'CFR.SW', 'ZURN.SW', 'UBSG.SW', 'ABBN.SW', 'BAER.SW', 'SYNN.SW', \
                  'ATLN.SW', 'GIVN.SW', 'HOLN.SW', 'SCMN.SW', 'ADEN.SW', 'SREN.SW', 'SGSN.SW', 'GEBN.SW', 'SLHN.SW', \
                  'UHR.SW', 'CSGN.SW']

# Synthes (SYST.SW) replaced by Geberit (GEBN.SW) due to the merger with Johnson & Johnson
sw20_18_jun_12 = ['NESN.SW', 'ROG.SW', 'NOVN.SW', 'CFR.SW', 'ZURN.SW', 'UBSG.SW', 'ABBN.SW', 'BAER.SW', 'SYNN.SW', \
                  'ATLN.SW', 'GIVN.SW', 'HOLN.SW', 'SCMN.SW', 'ADEN.SW', 'SREN.SW', 'SGSN.SW', 'GEBN.SW', 'UHR.SW', \
                  'CSGN.SW', 'RIGN.SW']

# Lonza Group (LONN.SW) replaced by Givaudan (GIVN.SW)
sw20_16_sep_11 = ['NESN.SW', 'ROG.SW', 'NOVN.SW', 'CFR.SW', 'ZURN.SW', 'UBSG.SW', 'ABBN.SW', 'BAER.SW', 'SYNN.SW', \
                  'ATLN.SW', 'GIVN.SW', 'HOLN.SW', 'SCMN.SW', 'ADEN.SW', 'SREN.SW', 'SGSN.SW', 'UHR.SW', 'CSGN.SW', \
                  'RIGN.SW', 'SYST.SW']

# Swiss Life (SLHN.SW) replaced by Transocean (RIGN.SW)
sw20_21_jun_10 = ['NESN.SW', 'ROG.SW', 'NOVN.SW', 'CFR.SW', 'ZURN.SW', 'UBSG.SW', 'ABBN.SW', 'BAER.SW', 'SYNN.SW', \
                  'ATLN.SW', 'LONN.SW', 'HOLN.SW', 'SCMN.SW', 'ADEN.SW', 'SREN.SW', 'SGSN.SW', 'UHR.SW', 'CSGN.SW', \
                  'SYST.SW', 'RIGN.SW']

# Nobel Biocare (NOBN.SW) and Baloise (BALN.SW) replaced by Lonza Group (LONN.SW) and SGS (SGSN.SW)
sw20_21_sep_09 = ['NESN.SW', 'ROG.SW', 'NOVN.SW', 'CFR.SW', 'ZURN.SW', 'UBSG.SW', 'ABBN.SW', 'BAER.SW', 'SYNN.SW', \
                  'ATLN.SW', 'LONN.SW', 'HOLN.SW', 'SCMN.SW', 'ADEN.SW', 'SREN.SW', 'SGSN.SW', 'UHR.SW', 'SLHN.SW', \
                  'CSGN.SW', 'SYST.SW']

sw20_05_jul_07 = ['NESN.SW', 'ROG.SW', 'NOVN.SW', 'CFR.SW', 'ZURN.SW', 'UBSG.SW', 'ABBN.SW', 'BAER.SW', 'SYNN.SW', \
                  'ATLN.SW', 'HOLN.SW', 'SCMN.SW', 'ADEN.SW', 'SREN.SW', 'BALN.SW', 'UHR.SW', 'SLHN.SW', 'CSGN.SW', \
                  'SYST.SW', 'NOBN.SW']

sw20 = sw20_06_jun_23

sw20_combined = sorted(list(set(sw20_05_jul_07 + sw20_21_sep_09 + sw20_21_jun_10 + sw20_16_sep_11 + \
                                sw20_18_jun_12 + sw20_14_jan_16 + sw20_24_apr_17 + sw20_27_mar_19 + \
                                sw20_03_sep_20 + sw20_05_sep_21 + sw20_24_sep_22 + sw20_06_jun_23)))

sw20_combined_yf = sw20_combined

# No data at yfinance
sw20_combined_yf.remove('ATLN.SW')
sw20_combined_yf.remove('CSGN.SW')
sw20_combined_yf.remove('NOBN.SW')
sw20_combined_yf.remove('RIGN.SW')
sw20_combined_yf.remove('SYNN.SW')
sw20_combined_yf.remove('SYST.SW')

sw20_dict = {'2007-07-05': sw20_05_jul_07,
             '2009-09-21': sw20_21_sep_09,
             '2010-06-21': sw20_21_jun_10,
             '2011-09-16': sw20_16_sep_11,
             '2012-06-18': sw20_18_jun_12,
             '2016-01-14': sw20_14_jan_16,
             '2017-04-24': sw20_24_apr_17,
             '2019-03-27': sw20_27_mar_19,
             '2020-09-20': sw20_03_sep_20,
             '2021-09-05': sw20_05_sep_21,
             '2022-09-24': sw20_24_sep_22,
             '2023-06-06': sw20_06_jun_23}

# Use like return sw20_adrs.get(input, "Not found") to get an entry or
# [sw20_adrs.get(item, item) for item in the_list] to 'convert' the whole list to ADRs
sw20_adrs = {
    'ABBN.SW':  'ABBNY',  # 4/2001
    'ADEN.SW':  'AHEXY',  # 12/2009
    'ALC.SW':   'ALC',    # 4/2019
    'BAER.SW':  'JBAXY',  # 4/2010
    'BALN.SW':  'BLHEY',  # 10/2009 No ADR but in index since 2007
    'CFR.SW':   'CFRUY',  # 12/2009 No ADR but in index since 2007
    'GEBN.SW':  'GBERY',  # 10/2011
    'GIVN.SW':  'GVDNY',  # 12/2008
    'HOLN.SW':  'HCMLY',  # 1/2010
    'KNIN.SW':  'KHNGY',  # 3/2012
    'LOGN.SW':  'LOGI',   # 3/1997
    'LONN.SW':  'LZAGY',  # 1/2010
    'NESN.SW':  'NSRGY',  # 11/1996
    'NOVN.SW':  'NVS',    # 11/1996
    'PGHN.SW':  'PGPHF',  # 1/2015
    'ROG.SW':   'RHHBY',  # 8/2003
    'SCMN.SW':  'SCMWY',  # 10/1998
    'SGSN.SW':  'SGSOY',  # 1/2008
    'SIKA.SW':  'SXYAY',  # 11/2015
    'SLHN.SW':  'SZLMY',  # 5/2009
    'SOON.SW':  'SONVY',  # 4/2011
    'SREN.SW':  'SSREY',  # 5/2011
    'UBSG.SW':  'UBS',    # 5/2000
    'UHR.SW':   'SWGAY',  # 10.2010
    'ZURN.SW':  'ZURVY',  # 6/2003
    'CSGN.SW':  'CS',     # Delisted
    'SYNN.SW':  'SYT',    # Delisted
    'ATLN.SW':  'ALIOY',  # Delisted
    'RIGN.SW':  'RIG',    # Delisted
    'SYST.SW':  'SYSTY',  # Delisted
    'NOBN.SW':  'NBHYY'   # Delisted
}

# Credit Suisse Group (CS) was replaced by Kuehne + Nagel International AG (KHNGY)
sw20_adr_06_jun_23 = [sw20_adrs.get(item, item) for item in sw20_06_jun_23]

# SGS SA (SGSOY) was replaced with Sonova (SONVY)
sw20_adr_24_sep_22 = [sw20_adrs.get(item, item) for item in sw20_24_sep_22]

# Swatch Group (SWGAY) was replaced with Logitech (LOGI)
sw20_adr_05_sep_21 = [sw20_adrs.get(item, item) for item in sw20_05_sep_21]

# Skipped (May 2021) as the only change was LafargeHolcim renamed to Holcim.

# Adecco (AHEXY) replaced by Partners Group (PGPHF)
sw20_adr_03_sep_20 = [sw20_adrs.get(item, item) for item in sw20_03_sep_20]

# Julius Bar (JBAXY) replaced by Alcon (ALC)
sw20_adr_27_mar_19 = [sw20_adrs.get(item, item) for item in sw20_27_mar_19]

# Sika AG (SXYAY) replaced Syngenta (SYT) after its purchase by ChemChina
sw20_adr_05_may_17 = [sw20_adrs.get(item, item) for item in sw20_05_may_17]

# Actelion (ALIOY) replaced by Lonza Group (LZAGY) after Actelion's purchase by Johnson & Johnson
sw20_adr_24_apr_17 = [sw20_adrs.get(item, item) for item in sw20_24_apr_17]

# Transocean (RIG) replaced by Swiss Life (SLHN)
sw20_adr_14_jan_16 = [sw20_adrs.get(item, item) for item in sw20_14_jan_16]

# Synthes (SYSTY) replaced by Geberit (GBERY) due to the merger with Johnson & Johnson
sw20_adr_18_jun_12 = [sw20_adrs.get(item, item) for item in sw20_18_jun_12]

# Lonza Group (LZAGY) replaced by Givaudan (GVDNY)
sw20_adr_16_sep_11 = [sw20_adrs.get(item, item) for item in sw20_16_sep_11]

# Swiss Life (SLHN) replaced by Transocean (RIG)
sw20_adr_21_jun_10 = [sw20_adrs.get(item, item) for item in sw20_21_jun_10]

# Nobel Biocare (NBHYY) and Baloise (BLHEY) replaced by Lonza Group (LZAGY) and SGS (SGSOY)
sw20_adr_21_sep_09 = [sw20_adrs.get(item, item) for item in sw20_21_sep_09]

sw20_adr_05_jul_07 = [sw20_adrs.get(item, item) for item in sw20_05_jul_07]

sw20_adr = sw20_adr_06_jun_23

sw20_adr_combined = sorted(list(set(sw20_adr_05_jul_07 + sw20_adr_21_sep_09 + sw20_adr_21_jun_10 + sw20_adr_16_sep_11 + \
                                    sw20_adr_18_jun_12 + sw20_adr_14_jan_16 + sw20_adr_24_apr_17 + sw20_adr_27_mar_19 + \
                                    sw20_adr_03_sep_20 + sw20_adr_05_sep_21 + sw20_adr_24_sep_22 + sw20_adr_06_jun_23)))

sw20_adr_combined_fmp = sw20_adr_combined

# No data on FMP
sw20_adr_combined_fmp.remove('ALIOY')
sw20_adr_combined_fmp.remove('SYSTY')
sw20_adr_combined_fmp.remove('NBHYY')

# Ticker is taken by another company
sw20_adr_combined_fmp.remove('SYT')

sw20_adr_combined_yf = sw20_adr_combined_fmp

# No data on yahoo finance
sw20_adr_combined_yf.remove('CS')

sw20_adr_dict = {'2007-07-05': sw20_adr_05_jul_07,
                 '2009-09-21': sw20_adr_21_sep_09,
                 '2010-06-21': sw20_adr_21_jun_10,
                 '2011-09-16': sw20_adr_16_sep_11,
                 '2012-06-18': sw20_adr_18_jun_12,
                 '2016-01-14': sw20_adr_14_jan_16,
                 '2017-04-24': sw20_adr_24_apr_17,
                 '2019-03-27': sw20_adr_27_mar_19,
                 '2020-09-20': sw20_adr_03_sep_20,
                 '2021-09-05': sw20_adr_05_sep_21,
                 '2022-09-24': sw20_adr_24_sep_22,
                 '2023-06-06': sw20_adr_06_jun_23}

##############################
# Canadian Big-Five/Six banks:
##############################
ca_big_five = ['RY.TO', 'TD.TO', 'BNS.TO', 'BMO.TO', 'CM.TO']  # 1/1995
ca_big_five_adr = ['RY', 'TD', 'BNS', 'BMO', 'CM']  # 6/2002

ca_big_six = ca_big_five + ['NA.TO']  # 1/1995
ca_big_six_adr = ca_big_five_adr + ['NTIOF']  # 4/2010

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
