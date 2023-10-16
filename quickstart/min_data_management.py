"""Minimalistic demonstration of data management.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
# Edit settings.py to add your API keys for data sources. Free API keys are sufficient for this example.
from data import av, fh, yf, polygon  # API wrappers for popular data sources (please note that they are unofficial)
from data.fvalues import Timespans
from data.fdata import Subquery
from data.stock import report_year  # Condition to request annual report.

from datetime import datetime, timedelta

# Fetch quotes if needed. Otherwise just take them from a database.
yf.YF(symbol='SPY', first_date="2010-1-1", last_date="2012-1-1").fetch_if_none()
yf.YF(symbol='SPY', first_date="2020-1-1", last_date="2022-1-1", verbosity=True).fetch_if_none()

# Fetch last week of minute SPY quotes from Polygon
now = datetime.now()
then = datetime.now() - timedelta(days=7)
pvi = polygon.Polygon(symbol='SPY', first_date=then, last_date=now, timespan=Timespans.Minute)

p_quotes = pvi.fetch_quotes()  # Fetch quotes but do not add them to DB

pvi.db_connect()
before, after = pvi.add_quotes(p_quotes)  # Add quotes to DB
pvi.db_close()

print(f"Total quotes num before and after the operation (it won't increase if quotes already present in DB): {before}, {after}")

symbol = 'IBM'

print(f"Fetch daily quotes, dividend and split data for {symbol} from AV/YF...")

avi = av.AVStock(symbol=symbol)
avi.fetch_if_none()

yfi = yf.YF(symbol=symbol)
yfi.fetch_dividends_if_none(245)
yfi.fetch_splits_if_none(8)

print(f"Fetch fundamental data for {symbol} from AV...")

# Fetch fundamental data and add it to DB
avi.fetch_earnings_if_none(109)
avi.fetch_cash_flow_if_none(25)
avi.fetch_balance_sheet_if_none(25)
avi.fetch_income_statement_if_none(25)

print("Get quotes from DB along with some fundamental data")
avi.db_connect()
rows = avi.get_quotes(queries=[Subquery('earnings', 'reported_date'),  # It will get both quarterly and annual reports
                               Subquery('earnings', 'reported_eps'),
                               Subquery('cash_flow', 'operating_cashflow', condition=report_year, title='annual_cashflow')])
avi.db_close()

# Print last rows of requested data
print(f"\nThe last row of obtained quotes and fundamental data for IBM:\n{rows[-1]}")

# Get the latest quote from Finnhub for AAPL (responce described in fvalues.Quotes)
aapl_data = fh.FHStock(symbol='AAPL').get_recent_data()

print(f"\nRecent quote data for AAPL: {aapl_data}")
