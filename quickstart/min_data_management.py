"""Minimalistic demonstration of data management.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""

from data import yf, fmp
from data.fdata import Subquery
from data.futils import get_dt
from data.fvalues import def_last_date

from datetime import datetime, timedelta
from dateutil import tz

symbol = 'AAPL'

# NOTE: The free FMP plan provides only the most recent capitalization data
# (~3 months), so the date range must overlap the recent period.
first_date = get_dt(datetime.now(tz.UTC)) - timedelta(days=60)
last_date = def_last_date

# Fetch quotes if needed. Otherwise just take them from a database.
# Divs and splits (in any will be fetched as well).
yfi = yf.YF(symbol=symbol, first_date=first_date, last_date=last_date)
yfi.get()

print(f"Total quotes num for '{symbol}': {yfi.get_quotes_num()}")

# Get a recent quote

print(f"\nRecent quote data for '{symbol}': {yfi.get_recent_data()}")

# ============ Multiple data sources with subqueries ============
# Fetch quotes from YF and capitalization data from FMP into the same
# database, then obtain the cap values for each quote via a subquery.

fmpi = fmp.FMP(symbol=symbol, first_date=first_date, last_date=last_date)
fmpi.get_cap()  # No quotes needed to fetch the cap data

rows = yfi.get(queries=[Subquery('fmp_capitalization', 'cap', title='cap')])

for row in rows:
    print(f"{row['date_time']}: close {row['closed']}, cap {row['cap']}")
