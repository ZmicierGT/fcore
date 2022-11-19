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

from enum import Enum

from data.futils import check_datetime

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

class AVResults(str, Enum):
    """
        Enumeration with AlphaVantage results.
    """
    Open = '1. open'
    High = '2. high'
    Low = '3. low'
    Close = '4. close'
    Volume = '5. volume'

# Provides parameters for the query to Alpha Vantage
class AVQuery(fdata.Query):
    """
        AlphaVantage query class.
    """
    def __init__(self):
        """
            Initialize AlphaVantage query class.
        """
        super().__init__()

        # Default values
        self.source_title = "AlphaVantage"
        self.timespan = Timespans.Day
        self.api_key = "get_your_free_api_key_at_alphavantage.co"
        self.type = AVType.Daily

    def get_timespan(self):
        """
            Get the timespan for the query.

            Converts universal timespan to AlphaVantage timespan.

            Raises:
                AVError: incorrect timespan.
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
            raise AVError(f"Unsupported timespan: {self.timespan}")

class AVError(Exception):
    """
        AlphaVantage exception class.
    """
    pass

class AV(fdata.BaseFetchData):
    """
        AlphaVantage API wrapper class.
    """
    def fetch_quotes(self):
        """
            The method to fetch quotes.

            Returns:
                list: quotes data

            Raises:
                AVError: incorrect API key(limit reached), http error happened or no data obtained.
        """
        if self.query.type not in (AVType.Crypto, AVType.CryptoIntraday, AVType.CryptoDaily, AVType.CryptoWeekly, AVType.CryptoMonthly):
            url = f"https://www.alphavantage.co/query?function={self.query.type}&symbol={self.query.symbol}$interval={self.query.get_timespan()}&apikey={self.query.api_key}"
        else:
            url = f"https://www.alphavantage.co/query?function={self.query.type}&symbol={self.query.symbol}&market=USD&interval={self.query.get_timespan()}&apikey={self.query.api_key}"
        
        try:
            response = requests.get(url)
        except (urllib.error.HTTPError, urllib.error.URLError, http.client.HTTPException) as e:
            raise AVError(f"Can't fetch quotes: {e}") from e

        json_data = response.json()

        try:
            key = list(json_data.keys())[1]
        except IndexError as e:
            raise AVError(f"Can't get quotes. Maybe API key limit is reached: {e}") from e

        dict_results = json_data[key]

        datetimes = list(dict_results.keys())

        if len(datetimes) == 0:
            raise AVError("No data obtained.")

        quotes_data = []

        for dt_str in datetimes:
            ts = check_datetime(dt_str)[1]

            # Keep all non-intraday timestamps at 23:59:59
            if self.query.timespan in (Timespans.Day, Timespans.Week, Timespans.Month, Timespans.Year):
                # Keep all datetimes UTC adjusted
                dt = datetime.utcfromtimestamp(ts)
                dt = dt.replace(tzinfo=pytz.utc)
                dt = dt.replace(hour=23, minute=59, second=59)
                ts = int(datetime.timestamp(dt))
                print("Adjust")

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
