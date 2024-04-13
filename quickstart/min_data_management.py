"""Minimalistic demonstration of data management.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from termcolor import colored
print(colored("This demo is outdated and needs to be rewritten.", "red"))
# Edit settings.py to add your API keys for data sources. Free API keys are sufficient for this example.
from data import av, fh, yf, polygon  # API wrappers for popular data sources (please note that they are unofficial)
from data.fvalues import Timespans
from data.stock import report_year  # Condition to request annual report.

from datetime import datetime, timedelta

# Fetch quotes if needed. Otherwise just take them from a database.
# Contigious interval of quotes will be preserved in the database.
yf.YF(symbol='SPY', first_date="2017-1-1", last_date="2018-1-1").get()
yf.YF(symbol='SPY', first_date="2020-1-1", last_date="2021-1-1", verbosity=True).get()

# Fetch last week of minute SPY quotes from Polygon
now = datetime.now()
then = datetime.now() - timedelta(days=7)
pvi = polygon.Polygon(symbol='SPY', first_date=then, last_date=now, timespan=Timespans.Minute, verbosity=True)

p_quotes = pvi.get_quotes_only()

print(f"Total quotes num for 'SPY': {len(p_quotes)}")

print("Fetch daily quotes, dividend and split data for 'IBM' from AV/YF...")

avi = av.AVStock(symbol='IBM')
avi.get_quotes_only()  # Do not get dividends and splits

yfi = yf.YF(symbol='IBM')
yfi.get_dividends()
yfi.get_splits()

print(f"Fetch fundamental data for {'IBM'} from AV...")

# Fetch fundamental data and add it to DB
avi.get_earnings()
avi.get_cash_flow()
avi.get_balance_sheet()
avi.get_income_statement()

print("Get quotes from DB along with some fundamental data")
avi.db_connect()
rows = avi.get_quotes(queries=[av.AvSubquery('av_earnings', 'reported_date'),  # It will get both quarterly and annual reports
                               av.AvSubquery('av_earnings', 'reported_eps'),
                               av.AvSubquery('av_cash_flow', 'operating_cashflow', condition=report_year, title='annual_cashflow')])
avi.db_close()

# Print last rows of requested data
print(f"\nThe last row of obtained quotes and fundamental data for 'IBM':\n{rows[-1]}")

# Get the latest quote from Finnhub for AAPL (responce described in fvalues.Quotes)
aapl_data = fh.FHStock(symbol='AAPL').get_recent_data()

print(f"\nRecent quote data for 'AAPL': {aapl_data}")
