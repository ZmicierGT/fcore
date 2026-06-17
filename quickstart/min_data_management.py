"""Minimalistic demonstration of data management.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""

from data import yf

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
