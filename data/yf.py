"""Yahoo Finance wrapper.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

import http.client
import urllib.error
import urllib.request

from enum import IntEnum

from data import fdata, futils
from data.fvalues import Timespans
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
        super().__init__(**kwargs)

        # Default values
        self.source_title = "YF"
        self.timespan = Timespans.Day

    def get_timespan(self):
        """
            Get the timespan for the query.

            No need to convert the default timespan to Yahoo Finance timespan because they are the same.
        """
        return self.timespan

class YFcsv(IntEnum):
    """
        Enum for YF quote csv header.
    """
    Date = 0
    Open = 1
    High = 2
    Low = 3
    Close = 4
    AdjClose = 5
    Volume = 6


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
        request_timespan = "1d"

        if self.query.timespan == Timespans.Week:
            request_timespan = "1w"
        elif self.query.timespan == Timespans.Month:
            request_timespan = "1mo"

        quotes_url = f"https://query1.finance.yahoo.com/v7/finance/download/{self.query.symbol}?period1={self.query.first_date_ts * 1000}&period2={self.query.last_date_ts * 1000}&interval={request_timespan}&events=history&includeAdjustedClose=true"

        try:
            quotes_response = urllib.request.urlopen(quotes_url)
        except (urllib.error.HTTPError, urllib.error.URLError, http.client.HTTPException) as e:
            raise FdataError(f"Can't fetch quotes: {e}") from e

        raw_quote_data = quotes_response.read()
        quote_data = raw_quote_data.decode("utf8")

        # Skip the header
        quotes = quote_data.splitlines()[1:]

        # Get dividends data
        divs_url = f"https://query1.finance.yahoo.com/v7/finance/download/{self.query.symbol}?period1={self.query.first_date_ts * 1000}&period2={self.query.last_date_ts * 1000}&interval={request_timespan}&events=div&includeAdjustedClose=true"

        try:
            divs_response = urllib.request.urlopen(divs_url)
        except (urllib.error.HTTPError, urllib.error.URLError, http.client.HTTPException) as e:
            raise FdataError(f"Can't fetch quotes: {e}") from e

        raw_divs_data = divs_response.read()
        divs_data = raw_divs_data.decode("utf8")

        # Skip the header
        divs = divs_data.splitlines()[1:]

        div_dates = []
        div_amounts = []

        for div_values in divs:
            div = div_values.split(',')

            date = div[YFdiv.Date]

            try:
                ts = futils.get_ts_from_str(date)
            except ValueError as e:
                raise FdataError(f"The date {date} is incorrect: {e}") from e

            div_dates.append(ts)
            div_amounts.append(div[YFdiv.Amount])

        quotes_data = []

        # Create a list of dictionaries with quotes
        for quote_values in quotes:
            quote = quote_values.split(',')

            quote_dict = {
                "v": quote[YFcsv.Volume],
                "o": quote[YFcsv.Open],
                "c": quote[YFcsv.Close],
                "h": quote[YFcsv.High],
                "l": quote[YFcsv.Low],
                "cl": "NULL",
                "n": "NULL",
                "vw": "NULL",
                "d": "NULL"
            }

            date = quote[YFcsv.Date]

            try:
                ts = futils.get_ts_from_str(date)
            except ValueError as e:
                raise FdataError(f"The date {date} is incorrect: {e}") from e

            # Add 23:59:59 to non-intraday quotes
            quote_dict['t'] = ts + 86399

            # Check if we have dividends data for this timestamp
            if ts in div_dates:
                index = div_dates.index(ts)
                quote_dict['d'] = div_amounts[index]

            quotes_data.append(quote_dict)

        if len(quotes_data) == 0:
            raise FdataError("No data obtained.")

        return quotes_data
