"""Yahoo Finance wrapper.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

from enum import IntEnum

from datetime import datetime
import pytz

import yfinance as yfin

from data import fdata
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
            data = yfin.Ticker(self.query.symbol).history(interval=self.query.get_timespan(),
                                                        start=self.query.first_date_str,
                                                        end=self.query.last_date_str)
        else:
            data = yfin.Ticker(self.query.symbol).history(interval=self.query.get_timespan(), period='max')

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

    def get_rt_data(self, to_cache=False):
        """
            Get real time data. Used in screening.

            Args:
                to_cache(bool): indicates if real time data should be cached in a database.

            Returns:
                list: real time data.
        """
        data = yfin.download(tickers=self.query.symbol, period='1d', interval='1m')
        row = data.iloc[-1]

        result = [self.query.symbol,
                  None,
                  self.query.source_title,
                  # TODO check if such datetime manipulations may have an impact depending on a locale.
                  str(data.index[-1])[:16],
                  self.query.timespan.value,
                  row['Open'],
                  row['High'],
                  row['Low'],
                  row['Close'],
                  row['Adj Close'],
                  row['Volume'],
                  None,
                  None,
                  None]

        # Todo caching should be implemented

        return result
