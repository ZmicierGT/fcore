"""AlphaVantage API wrapper.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

from datetime import datetime
import pytz

import http.client
import urllib.error
import requests

from data import fdata

from data.fvalues import Timespans
from data.fdata import FdataError

from enum import Enum

from data.futils import get_ts_from_str

import settings

# TODO Add unit test for this module

class AVStock(fdata.BaseFetchData):
    """
        AlphaVantage API wrapper class.
    """
    def __init__(self, **kwargs):
        """Initialize the instance of AV class."""
        super().__init__(**kwargs)

        # Default values
        self.source_title = "AlphaVantage"
        self.api_key = settings.AV.api_key
        self.compact = True  # Indicates if a limited number (100) of quotes should be obtained

        if self.api_key is None:
            raise FdataError("API key is needed for this data source. Get your free API key at alphavantage.co and put it in setting.py")

    def get_timespan(self):
        """
            Get the timespan.

            Converts universal timespan to AlphaVantage timespan.

            Raises:
                FdataError: incorrect/unsupported timespan requested.

            Returns:
                str: timespan for AV query.
        """
        if self.timespan == Timespans.Minute:
            return '1min'
        elif self.timespan == Timespans.FiveMinutes:
            return '5min'
        elif self.timespan == Timespans.FifteenMinutes:
            return '15min'
        elif self.timespan == Timespans.ThirtyMinutes:
            return '30min'
        elif self.timespan == Timespans.Hour:
            return '60min'
        # Other timespans are obtained with different function and there is no timespan parameter.
        else:
            raise FdataError(f"Unsupported timespan: {self.timespan}")

    def fetch_quotes(self):
        """
            The method to fetch quotes.

            Returns:
                list: quotes data

            Raises:
                FdataError: incorrect API key(limit reached), http error happened, invalid timespan or no data obtained.
        """
        # At first, need to set a function depending on a timespan.
        if self.timespan == Timespans.Day:
            output_size = 'compact' if self.compact else 'full'
            json_key = 'Time Series (Daily)'

            url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={self.symbol}&outputsize={output_size}&apikey={self.api_key}'
        elif self.timespan == Timespans.Week:
            json_key = 'Weekly Adjusted Time Series'

            url = f'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY_ADJUSTED&symbol={self.symbol}&apikey={self.api_key}'
        elif self.timespan == Timespans.Month:
            json_key = 'Monthly Adjusted Time Series'

            url = f'https://www.alphavantage.co/query?function=TIME_SERIES_MONTHLY_ADJUSTED&symbol={self.symbol}&apikey={self.api_key}'
        # All intraday timespans
        elif self.timespan in [Timespans.Minute,
                               Timespans.FiveMinutes,
                               Timespans.FifteenMinutes,
                               Timespans.ThirtyMinutes,
                               Timespans.Hour]:
            output_size = 'compact' if self.compact else 'full'
            json_key = f'Time Series ({self.get_timespan()})'

            url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={self.symbol}&interval={self.get_timespan()}&outputsize={output_size}&apikey={self.api_key}'
        else:
            raise FdataError(f"Unsupported timespan: {self.timespan}")

        # Get quotes data
        try:
            response = requests.get(url, timeout=30)
        except (urllib.error.HTTPError, urllib.error.URLError, http.client.HTTPException) as e:
            raise FdataError(f"Can't fetch quotes: {e}") from e

        json_data = response.json()

        dict_results = json_data[json_key]
        datetimes = list(dict_results.keys())

        if len(datetimes) == 0:
            raise FdataError("No data obtained.")

        quotes_data = []

        for dt_str in datetimes:
            try:
                ts = get_ts_from_str(dt_str)
            except ValueError as e:
                raise FdataError(f"Can't parse the datetime {dt_str}: {e}") from e

            # Keep all datetimes UTC adjusted
            dt = datetime.utcfromtimestamp(ts)
            dt = dt.replace(tzinfo=pytz.utc)

            # The current quote to process
            quote = dict_results[dt_str]

            quote_dict = {
                't': None,
                'o': quote['1. open'],
                'h': quote['2. high'],
                'l': quote['3. low'],
                'c': None,
                'cl': None,
                'v': None,
                'd': None,
                'n': None,
                'vw': None
            }

            # Set the entries depending if the quote is intraday
            if self.timespan in (Timespans.Day, Timespans.Week, Timespans.Month):
                quote_dict['c'] = quote['5. adjusted close']
                quote_dict['cl'] = quote['4. close']
                quote_dict['v'] = quote['6. volume']
                quote_dict['d'] = quote['7. dividend amount']

                # Keep all non-intraday timestamps at 23:59:59
                dt = dt.replace(hour=23, minute=59, second=59)
            else:
                quote_dict['c'] = quote['4. close']
                quote_dict['v'] = quote['5. volume']

            # Set the timestamp
            quote_dict['t'] = int(datetime.timestamp(dt))

            quotes_data.append(quote_dict)

        return quotes_data
