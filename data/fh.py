"""Finnhub API wrapper.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from datetime import datetime
import pytz

import finnhub

from data.fvalues import SecType, Currency
from data import stock
from data.fdata import FdataError
from data.futils import get_dt, get_labelled_ndarray

import settings

class FHStock(stock.StockFetcher):
    """
        Finnhub API wrapper class.
    """
    def __init__(self, **kwargs):
        """Initialize the instance of FHStock class."""
        super().__init__(**kwargs)

        # Default values
        self.source_title = "Finnhub"
        self.api_key = settings.Finnhub.api_key

        self.sectype = SecType.Stock  # TODO LOW Distinguish stock and ETF for FH
        self.currency = Currency.Unknown  # Currencies are not supported yet

    def get_timespan_str(self):
        """
            Get timespan string (like '5min' and so on) to query a particular data source based on the timespan specified
            in the datasource instance.

            Returns:
                str: timespan string.
        """
        raise FdataError("Timespans are not supported yet in Finnhub datasource.")

    def fetch_quotes(self, first_ts=None, last_ts=None):
        """
            The method to fetch quotes.

            Args:
                first_ts(int): overridden first ts to fetch.
                last_ts(int): overridden last ts to fetch.

            Returns:
                list: quotes data

            Raises:
                FdataError: incorrect API key(limit reached), http error happened, invalid timespan or no data obtained.
        """
        raise FdataError(f"Fetching quotes is not supported (yet) for the source {type(self).__name__}")

    def fetch_income_statement(self):
        raise FdataError(f"Income statement data is not supported (yet) for the source {type(self).__name__}")

    def fetch_balance_sheet(self):
        raise FdataError(f"Balance sheet data is not supported (yet) for the source {type(self).__name__}")

    def fetch_cash_flow(self):
        raise FdataError(f"Cash flow data is not supported (yet) for the source {type(self).__name__}")

    def fetch_earnings(self):
        raise FdataError(f"Earnings statement data is not supported (yet) for the source {type(self).__name__}")

    def fetch_dividends(self):
        raise FdataError(f"Dividends data is not supported (yet) for the source {type(self).__name__}")

    def fetch_splits(self):
        raise FdataError(f"Splits statement data is not supported (yet) for the source {type(self).__name__}")

    def get_recent_data(self, to_cache=False):
        """
            Get recent quote.

            Args:
                to_cache(bool): indicates if real time data should be cached in a database.

            Raises:
                FdataErorr: markets are closed or network error has happened.

            Returns:
                list: real time data.
        """
        # Get recent quote

        try:
            finnhub_client = finnhub.Client(api_key=self.api_key)
            quote = finnhub_client.quote(self.symbol)
        except finnhub.FinnhubRequestException as e:
            raise FdataError(f"Can't get quote: {e}") from e

        dt = datetime.fromtimestamp(quote['t'])
        # Always keep datetimes in UTC time zone!
        dt = dt.replace(tzinfo=pytz.utc)
        ts = int(dt.timestamp())

        result = {'time_stamp': ts,
                  'date_time': get_dt(ts).replace(microsecond=0).isoformat(' '),
                  'opened': quote['o'],
                  'high': quote['h'],
                  'low': quote['l'],
                  'closed': quote['c'],
                  'volume': None,
                  'transactions': None,
                  'adj_close': quote['c'],
                  'divs_ex': 0.0,
                  'divs_pay': 0.0,
                  'splits': 1.0
                 }

        result = [result]
        result = get_labelled_ndarray(result)

        return result
