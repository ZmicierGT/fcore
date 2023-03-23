"""Finnhub API wrapper.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""
from datetime import datetime
import pytz

import json

import finnhub

from data.fvalues import SecTypes, Currency
from data import fdata
from data.fdata import FdataError

import settings

# TODO LOW Add unit test for this module

class FHStock(fdata.BaseFetchData):
    """
        Finnhub API wrapper class.
    """
    def __init__(self, **kwargs):
        """Initialize the instance of FHStock class."""
        super().__init__(**kwargs)

        # Default values
        self.source_title = "Finnhub"
        self.api_key = settings.Finnhub.api_key

        self.sectype = SecTypes.Stock  # TODO LOW Distinguish stock and ETF for FH
        self.currency = Currency.Unknown  # Currencies are not supported yet

    def fetch_quotes(self):
        """
            The method to fetch quotes.

            Raises:
                FdataError: incorrect API key(limit reached), http error happened, invalid timespan or no data obtained.

            Returns:
                list: quotes data
        """
        raise FdataError("This method is not yet implemented for FH")

    # TODO MID This method should be abstract in the base class
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

        result = [ts,
                  quote['o'],
                  quote['h'],
                  quote['l'],
                  quote['c'],  # Consider that Close and AdjClose is the same for intraday timespans
                  quote['c'],
                  'NULL',
                  'NULL',
                  'NULL',
                  'NULL']

        return result
