"""Minimalistic demonstration of data management.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""

# Edit settings.py to add your API keys for data sources. Free API keys are sufficient for this example.
from data import yf, fmp  # API wrappers for popular data sources (please note that they are unofficial)
from data.fvalues import Timespans
from data.stock import report_year  # Condition to request annual report

from datetime import datetime, timedelta

# Fetch quotes if needed. Otherwise just take them from a database.
# Divs and splits (in any will be fetched as well).
yfi = yf.YF(symbol='SPY', first_date="2017-1-1", last_date="2018-1-1")
yfi.get()

# Contiguous interval of quotes will be preserved in the database.
yfi.first_date = "2020-1-1"
yfi.last_date = last_date="2021-1-1"

yfi.get()

print(f"Total quotes num for 'SPY': {yfi.get_quotes_num()}")

# Get a recent quote

print(f"\nRecent quote data for 'SPY': {yfi.get_recent_data()}")

print("Fetch daily quotes, dividend and split data for 'IBM' from FMP...")

fmpi = fmp.FmpStock(symbol='IBM')
fmpi.get_quotes_only()  # Do not get dividends and splits

print(f"Fetch fundamental data for {'IBM'} from FMP...")

# Fetch fundamental data and add it to DB
fmpi.get_surprises()
fmpi.get_cash_flow()
fmpi.get_balance_sheet()
fmpi.get_income_statement()

print("Get quotes from DB along with some fundamental data")
fmpi.db_connect()
rows = fmpi.get_quotes(queries=[fmp.FmpSubquery('fmp_surprises', 'actualEarning'),
                                fmp.FmpSubquery('fmp_capitalization', 'cap'),
                                fmp.FmpSubquery('fmp_income_statement', 'ebitda', condition=report_year, title='annual_income_statement'),
                                fmp.FmpSubquery('fmp_balance_sheet', 'goodwill', condition=report_year, title='annual_balance_sheet'),
                                fmp.FmpSubquery('fmp_cash_flow', 'netIncome', condition=report_year, title='annual_cashflow')])
fmpi.db_close()

# Print last rows of requested data
print(f"\nThe last row of obtained quotes and fundamental data for 'IBM':\n{rows[-1]}")
