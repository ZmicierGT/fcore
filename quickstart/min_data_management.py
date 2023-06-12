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

# This example checks if there is at least 565 dayly quotes for SPY in the database and if no
# then it fetches it from Yahoo Finance. DB connection will be estables automatically (if needed).
yf.YF(symbol='SPY', first_date="2021-1-2", last_date="2023-4-1").fetch_if_none(565)

# Fetch last week of minute SPY quotes from Polygon
now = datetime.now()
then = datetime.now() - timedelta(days=7)
pvi = polygon.Polygon(symbol='SPY', first_date=then, last_date=now, timespan=Timespans.Minute)

p_quotes = pvi.fetch_quotes()  # Fetch quotes but do not add them to DB

pvi.db_connect()
before, after = pvi.add_quotes(p_quotes)  # Add quotes to DB
pvi.db_close()

print(f"Total quotes num before and after the operation (it won't increase if quotes already present in DB): {before}, {after}")

# Fetch quotes and fundamental data from AlphaVantage. Please note that the free key allows 5 API calls per minute.
avi = av.AVStock(symbol='IBM')
avi.compact = False  # Request all awailable quotes data for IBM

avi.fetch_if_none(15423)  # Fetch quotes and add it to DB

# Fetch fundamental data and add it to DB
avi.fetch_earnings_if_none(109)
avi.fetch_cash_flow_if_none(25)
avi.fetch_balance_sheet_if_none(25)
avi.fetch_income_statement_if_none(25)

# Get quotes from DB along with some fundamental data
avi.db_connect()
rows = avi.get_quotes(columns=['time_stamp'],  # Get time stamp in addition to a formatted data time.
                      queries=[Subquery('earnings', 'reported_date'),  # It will get both quarterly and annual reports
                               Subquery('earnings', 'reported_eps'),
                               Subquery('cash_flow', 'operating_cashflow', condition=report_year, title='annual_cashflow')])
avi.db_close()

# Print last rows of requested data
print(f"\nThe last row of obtained quotes and fundamental data for IBM:\n{dict(rows[-1])}")

# Get the latest quote from Finnhub for AAPL (responce described in fvalues.Quotes)
aapl_data = fh.FHStock(symbol='AAPL').get_recent_data()

print(f"\nRecent quote data for AAPL: {aapl_data}")
