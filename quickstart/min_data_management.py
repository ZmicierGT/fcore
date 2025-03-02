"""Minimalistic demonstration of data management.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""

# Edit settings.py to add your API keys for data sources. Free API keys are sufficient for this example.
from data import yf, fmp  # API wrappers for popular data sources (please note that they are unofficial)
from data.fdata import Subquery
from data.fvalues import Timespans
from data.stock import report_year  # Condition to request annual report

from datetime import datetime, timedelta

# Fetch quotes if needed. Otherwise just take them from a database.
# Divs and splits (in any will be fetched as well).
yfi = yf.YF(symbol='IBM', first_date="2024-1-1", last_date="2024-5-1")
yfi.get()

# Contiguous interval of quotes will be preserved in the database.
yfi.first_date = "2025-1-1"
yfi.last_date = last_date="2025-3-1"

yfi.get()

print(f"Total quotes num for 'SPY': {yfi.get_quotes_num()}")

# Get a recent quote

print(f"\nRecent quote data for 'SPY': {yfi.get_recent_data()}")

print(f"\nFetch fundamental data for {'IBM'} from FMP...")

fmpi = fmp.FmpStock(symbol='IBM', verbosity=True)

fmpi.get_surprises()
fmpi.get_cap()

fmpi.get_income_statement()
fmpi.get_balance_sheet()
fmpi.get_cash_flow()

print("\nGet quotes from DB along with some fundamental data")
fmpi.db_connect()
rows = fmpi.get_quotes(ignore_source=True,
                       queries=[Subquery('fmp_surprises', 'actualEarning'),
                                Subquery('fmp_capitalization', 'cap'),
                                Subquery('fmp_income_statement', 'ebitda', condition=report_year, title='annual_income_statement'),
                                Subquery('fmp_balance_sheet', 'goodwill', condition=report_year, title='annual_balance_sheet'),
                                Subquery('fmp_cash_flow', 'netIncome', condition=report_year, title='annual_cashflow')])
fmpi.db_close()

# Print last rows of requested data
print(f"\nThe last row of obtained quotes and fundamental data for 'IBM':\n{rows[-1]}")
