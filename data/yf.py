"""Yahoo Finance wrapper.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

import http.client
import urllib.error
import urllib.request

from enum import IntEnum

from datetime import datetime
import pytz

import yfinance as yf

from data import fdata, futils
from data.fvalues import Timespans, def_first_date, def_last_date
from data.fdata import FdataError

# Provides parameters for the query to Yahoo Finance
class YFQuery(fdata.Query):
    """
        Yahoo Finance query class.
    """
    def __init__(self, **kwargs):
        """
            Initialize Yahoo Finance query class.
        """
        self.timespan = Timespans.Day
        super().__init__(**kwargs)

        # Default values
        self.source_title = "YF"

    def get_timespan(self):
        """
            Get the timespan for the query.

            No need to convert the default timespan to Yahoo Finance timespan because they are the same.
        """
        request_timespan = "1d"

        if self.timespan == Timespans.Week:
            request_timespan = "1w"
        elif self.timespan == Timespans.Month:
            request_timespan = "1mo"
        elif self.timespan == Timespans.Intraday:
            request_timespan = "1m"

        return request_timespan

class YFdiv(IntEnum):
    """
        Enum for YF dividends csv header.
    """
    Date = 0
    Amount = 1

class YF(fdata.BaseFetchData):
    """
        Yahoo Finance wrapper class.
    """
    def __init__(self, query):
        """Initialize the instance of YF class."""
        super().__init__(query)

    def fetch_quotes(self):
        """
            The method to fetch quotes.

            Returns:
                list: quotes data

            Raises:
                FdataError: network error, no data obtained, can't parse json or the date is incorrect.
        """
        if self.query.first_date_ts != def_first_date or self.query.last_date_ts != def_last_date:
            data = yf.Ticker(self.query.symbol).history(interval=self.query.get_timespan(),
                                                        start=self.query.first_date_str,
                                                        end=self.query.last_date_str)
        else:
            data = yf.Ticker(self.query.symbol).history(interval=self.query.get_timespan(), period='max')

        length = len(data)

        if length == 0:
            raise FdataError(f"Can not fetch quotes for {self.query.symbol}. No quotes fetched.")

        # Create a list of dictionaries with quotes
        quotes_data = []

        for ind in range(length):
            dt = data.index[ind]
            dt = dt.replace(tzinfo=pytz.utc)
            ts = int(datetime.timestamp(dt))

            if self.query.get_timespan() in [Timespans.Day, Timespans.Week, Timespans.Month]:
                # Add 23:59:59 to non-intraday quotes
                quote_dict['t'] = ts + 86399

            quote_dict = {
                "v": data['Volume'][ind],
                "o": data['Open'][ind],
                "c": data['Close'][ind],
                "h": data['High'][ind],
                "l": data['Low'][ind],
                "cl": "NULL",
                "n": "NULL",
                "vw": "NULL",
                "d": data['Dividends'][ind],
                "t": ts
            }

            quotes_data.append(quote_dict)

        if len(quotes_data) != length:
            raise FdataError(f"Obtained and parsed data length does not match: {length} != {len(quotes_data)}.")

        return quotes_data
