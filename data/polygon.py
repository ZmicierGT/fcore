"""Polygon.IO API wrapper.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""

from datetime import datetime
import pytz

from dateutil.relativedelta import relativedelta

import http.client
import urllib.error
import requests

import json

from data import stock
from data.fdata import FdataError

from data.fvalues import Timespans, SecType, Currency, def_first_date, def_last_date

import settings

class Polygon(stock.StockFetcher):
    """
        Polygon.IO wrapper class.
    """
    def __init__(self, **kwargs):
        """
            Initialize Polygon.IO wrapper class.
        """
        super().__init__(**kwargs)

        # Default values
        self.source_title = "Polygon.io"
        self.year_delta = settings.Polygon.year_delta
        self.api_key = settings.Polygon.api_key

        self.sectype = SecType.Unknown  # Multiple security types may be obtaines by similar Polygon API calls
        self.currency = Currency.Unknown  # Currencies are not supported yet

        if self.api_key is None:
            raise FdataError("API key is needed for this data source. Get your free API key at polygon.io and put it in setting.py")

        # IF first/last datetimes are not provided, use the current datetime as the last and current - year_delta as first
        if self.first_date_ts == def_first_date:
            new_first_date = datetime.now() - relativedelta(years=int(self.year_delta))
            new_first_date = new_first_date.replace(tzinfo=pytz.utc)
            new_first_date = new_first_date.replace(hour=0, minute=0, second=0)
            self.first_date = new_first_date

        if self.last_date_ts == def_last_date:
            new_last_date = datetime.now()
            new_last_date = new_last_date.replace(tzinfo=pytz.utc)
            new_last_date = new_last_date.replace(hour=23, minute=59, second=59)
            self.last_date = new_last_date

    def get_timespan_str(self):
        """
            Get the timespan for queries.

            Raises:
                FdataError: incorrect/unsupported timespan requested.

            Returns:
                str: timespan for Polygon query.
        """
        if self.timespan in [Timespans.Minute,
                             Timespans.Hour,
                             Timespans.Day,
                             Timespans.Week,
                             Timespans.Month,
                             Timespans.Quarter,
                             Timespans.Year]:
            return self.timespan.lower()
        else:
            raise FdataError(f"Requested timespan is not supported by Polygon: {self.timespan}")

    def fetch_quotes(self):
        """
            The method to fetch quotes.

            Returns:
                list: quotes data

            Raises:
                FdataError: Network error happened, no data obtained or can't parse json.
        """
        first_date = self.first_date.date()
        last_date = self.last_date.date()

        url = f"https://api.polygon.io/v2/aggs/ticker/{self.symbol}/range/1/{self.get_timespan_str()}/{first_date}/{last_date}?adjusted=true&sort=asc&limit=50000&apiKey={self.api_key}"

        try:
            response = requests.get(url, timeout=30)
        except (urllib.error.HTTPError, urllib.error.URLError, http.client.HTTPException, json.decoder.JSONDecodeError) as e:
            raise FdataError(f"Can't fetch quotes: {e}") from e
        
        try:
            json_data = json.loads(response.text)
            json_results = json_data['results']
        except (json.JSONDecodeError, KeyError) as e:
            error = e

            try:
                error = json_data['error']
            except (json.JSONDecodeError, KeyError):
                # Not relevant for error reporting
                pass

            raise FdataError(f"Can't parse json or no symbol found. {error}") from e

        if len(json_results) == 0:
            raise FdataError("No data obtained.")

        quotes_data = []

        for quote in json_results:
            # No need in ms
            ts = int(quote['t'] / 1000)
            # Keep all non-intraday timestamps at 23:59:59
            if self.timespan in (Timespans.Day, Timespans.Week, Timespans.Month, Timespans.Year):
                dt = datetime.utcfromtimestamp(ts)
                dt = dt.replace(tzinfo=pytz.utc)
                dt = dt.replace(hour=23, minute=59, second=59)
                ts = int(datetime.timestamp(dt))

            quote_dict = {
                'ts': ts,
                'open': quote['o'],
                'high': quote['h'],
                'low': quote['l'],
                'adj_close': quote['c'],
                'raw_close': 'NULL',
                'volume': quote['v'],
                'divs': 'NULL',
                'transactions': quote['n'],
                'split': 'NULL',
                'sectype': self.sectype.value,
                'currency': self.currency.value
            }

            quotes_data.append(quote_dict)

        return quotes_data

    def fetch_income_statement(self):
        raise FdataError(f"Income statement data is not supported (yet) for the source {type(self).__name__}")

    def fetch_balance_sheet(self):
        raise FdataError(f"Balance sheet data is not supported (yet) for the source {type(self).__name__}")

    def fetch_cash_flow(self):
        raise FdataError(f"Cash flow data is not supported (yet) for the source {type(self).__name__}")

    def fetch_earnings(self):
        raise FdataError(f"Earnings statement data is not supported (yet) for the source {type(self).__name__}")

    def get_recent_data(self, to_cache=False):
        raise FdataError(f"Real time data is not supported (yet) for the source {type(self).__name__}")
