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

class AVType(str, Enum):
    """
        Enumeration with AlphaVantage functions.
    """
    Intraday = 'TIME_SERIES_INTRADAY'
    IntradayExtended = 'TIME_SERIES_INTRADAY_EXTENDED'
    Daily = 'TIME_SERIES_DAILY'
    Weekly = 'TIME_SERIES_WEEKLY'
    WeeklyAdjusted = 'TIME_SERIES_WEEKLY_ADJUSTED'
    Monthly = 'TIME_SERIES_MONTHLY'
    MontlyAdjusted = 'TIME_SERIES_MONTHLY_ADJUSTED'
    Currency = 'CURRENCY_EXCHANGE_RATE'
    FxIntraday = 'FX_INTRADAY'
    FxDaily = 'FX_DAILY'
    FxWeekly = 'FX_WEEKLY'
    FxMonthly = 'FX_MONTHLY'
    Crypto = 'CURRENCY_EXCHANGE_RATE'
    CryptoIntraday = 'CRYPTO_INTRADAY'
    CryptoDaily = 'DIGITAL_CURRENCY_DAILY'
    CryptoWeekly = 'DIGITAL_CURRENCY_WEEKLY'
    CryptoMonthly = 'DIGITAL_CURRENCY_MONTHLY'

class AVResultsCI(str, Enum):
    """
        Enumeration with AlphaVantage results for crypto intraday.
    """
    Open = '1. open'
    High = '2. high'
    Low = '3. low'
    Close = '4. close'
    Volume = '5. volume'

class AVResultsCD(str, Enum):
    """
        Enumeration with AlphaVantage results for crypto daily.
    """
    Open = '1a. open (USD)'
    High = '2a. high (USD)'
    Low = '3a. low (USD)'
    Close = '4a. close (USD)'
    Volume = '5. volume'

class AV(fdata.BaseFetchData):
    """
        AlphaVantage API wrapper class.
    """
    def __init__(self, **kwargs):
        """Initialize the instance of AV class."""
        super().__init__(**kwargs)

        # Default values
        self.source_title = "AlphaVantage"
        self.api_key = settings.AV.api_key  # Please note that free keys do not support live quotes now.
        self.type = AVType.Daily

        if self.api_key is None:
            raise FdataError("API key is needed for this data source. Get your free API key at alphavantage.co and put it in setting.py")

    def get_timespan(self):
        """
            Get the timespan.

            Converts universal timespan to AlphaVantage timespan.

            Raises:
                FdataError: incorrect timespan.
        """
        if self.timespan in (Timespans.Intraday, Timespans.OneMinute):
            return '1min'
        elif self.timespan in (Timespans.FiveMinutes):
            return '5min'
        elif self.timespan in (Timespans.FifteenMinutes):
            return '15min'
        elif self.timespan in (Timespans.ThirtyMinutes):
            return '30min'
        elif self.timespan in (Timespans.OneHour):
            return '60min'
        else:
            raise FdataError(f"Unsupported timespan: {self.timespan}")

    def fetch_quotes(self):
        """
            The method to fetch quotes.

            Returns:
                list: quotes data

            Raises:
                FdataError: incorrect API key(limit reached), http error happened or no data obtained.
        """
        url = f"https://www.alphavantage.co/query?function={self.type}&symbol={self.symbol}&market=USD&interval={self.get_timespan()}&apikey={self.api_key}"

        AVResults = AVResultsCI

        if self.type == AVType.CryptoDaily:
            AVResults = AVResultsCD

        try:
            response = requests.get(url, timeout=30)
        except (urllib.error.HTTPError, urllib.error.URLError, http.client.HTTPException) as e:
            raise FdataError(f"Can't fetch quotes: {e}") from e

        json_data = response.json()

        try:
            key = list(json_data.keys())[1]
        except IndexError as e:
            raise FdataError(f"Can't get quotes. Maybe API key limit is reached: {e}") from e

        dict_results = json_data[key]

        datetimes = list(dict_results.keys())

        if len(datetimes) == 0:
            raise FdataError("No data obtained.")

        quotes_data = []

        for dt_str in datetimes:
            try:
                ts = get_ts_from_str(dt_str)
            except ValueError as e:
                raise FdataError(f"Can't parse the datetime {dt_str}: {e}") from e

            # Keep all non-intraday timestamps at 23:59:59
            if self.timespan in (Timespans.Day, Timespans.Week, Timespans.Month, Timespans.Year):
                # Keep all datetimes UTC adjusted
                dt = datetime.utcfromtimestamp(ts)
                dt = dt.replace(tzinfo=pytz.utc)
                dt = dt.replace(hour=23, minute=59, second=59)
                ts = int(datetime.timestamp(dt))

            quote = dict_results[dt_str]

            quote_dict = {
                "v": quote[AVResults.Volume],
                "o": quote[AVResults.Open],
                "c": quote[AVResults.Close],
                "h": quote[AVResults.High],
                "l": quote[AVResults.Low],
                "cl": "NULL",
                "n": "NULL",
                "vw": "NULL",
                "d": "NULL",
                "t": ts
            }

            quotes_data.append(quote_dict)

        return quotes_data
