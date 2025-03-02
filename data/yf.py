"""Yahoo Finance wrapper.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from datetime import datetime, timedelta
from dateutil import tz
import calendar

import pandas as pd
import numpy as np

import yfinance as yfin

from data import stock
from data.fvalues import Timespans, SecType, Currency
from data.fdata import FdataError
from data.futils import get_labelled_ndarray, get_dt

import urllib.error
import http.client

class YF(stock.StockFetcher):
    """
        Yahoo Finance wrapper class.
    """
    def __init__(self, **kwargs):
        """
            Initialize Yahoo Finance wrapper class.
        """
        super().__init__(**kwargs)

        # Default values
        self.source_title = "YF"

        self._data = None  # Cached data for splits/divs
        self._data_symbol = self.symbol  # Symbol of cached data

        self._sec_info_supported = True
        self._stock_info_supported = True

    def get_timespan_str(self):
        """
            Get the timespan for queries.

            Raises:
                FdataError: incorrect/unsupported timespan requested.

            Returns:
                str: timespan for YF query.
        """
        if self.timespan == Timespans.Minute:
            return '1m'
        elif self.timespan == Timespans.TwoMinutes:
            return '2m'
        elif self.timespan == Timespans.FiveMinutes:
            return '5m'
        elif self.timespan == Timespans.FifteenMinutes:
            return '15m'
        elif self.timespan == Timespans.ThirtyMinutes:
            return '30m'
        elif self.timespan == Timespans.Hour:
            return '1h'
        elif self.timespan == Timespans.NinetyMinutes:
            return '90m'
        elif self.timespan == Timespans.Day:
            return '1d'
        else:
            raise FdataError(f"Requested timespan is not supported by YF: {self.timespan.value}")

    # TODO MID Think how to handle a situation that YF fetches the current quote even if period is incomplete
    def fetch_quotes(self, first_ts=None, last_ts=None):
        """
            The method to fetch quotes.

            Args:
                first_ts(int): overridden first ts to fetch.
                last_ts(int): overridden last ts to fetch.

            Returns:
                list: quotes data

            Raises:
                FdataError: network error, no data obtained, can't parse json or the date is incorrect.
        """
        # Adjust dates for the exchange time zone for the request
        first_date, last_date = self.get_request_dates(first_ts, last_ts)

        # Dates should differ or no data obtained
        if (last_date - first_date).days == 0:
            first_date = first_date - timedelta(days=1)

        data = yfin.download(self.symbol,
                             interval=self.get_timespan_str(),
                             start=first_date,
                             end=last_date,
                             auto_adjust=False)

        length = len(data)

        if length == 0:
            self.log(f"Can not fetch quotes for {self.symbol}. No quotes fetched.")
            return

        pick_ts = np.vectorize(lambda x: calendar.timegm(get_dt(str(x), self.get_timezone()).utctimetuple()))

        data = data.reset_index()

        if self.is_intraday() is False:
            # TODO LOW For simplicity just set time to 23:59:59 without time zone adjustments.
            # For some markets (non-US) timestamps (which are supposed to be UTC-adjusted) may be incorrect.
            data['ts'] = data['Date'].dt.normalize() + timedelta(hours=23, minutes=59, seconds=59)
            data['ts'] = data['ts'].astype(int).div(10**9).astype(int)  # One more astype to get rid of .0

            # Reverse-adjust the quotes
            splits = self.__fetch_splits()

            for i in range(len(splits)):
                ind = np.searchsorted(data['ts'], [splits['ts'][i] ,], side='right')[0] - 1
                split_ratio = splits['split_ratio'][i]

                data.loc[: ind, 'Open'] = data.loc[:ind, 'Open'][self.symbol] * split_ratio
                data.loc[: ind, 'High'] = data.loc[:ind, 'High'][self.symbol] * split_ratio
                data.loc[: ind, 'Low'] = data.loc[:ind, 'Low'][self.symbol] * split_ratio
                data.loc[: ind, 'Close'] = data.loc[:ind, 'Close'][self.symbol] * split_ratio
                data.loc[: ind, 'Volume'] = round(data.loc[:ind, 'Volume'][self.symbol] / split_ratio)
        else:
            data['ts'] = pick_ts(data['Datetime'])

        # Create a list of dictionaries with quotes
        quotes_data = []

        for ind in range(length):
            quote_dict = {
                'volume': data.iloc[[ind]]['Volume'].values[0][0],
                'open': data.iloc[[ind]]['Open'].values[0][0],
                'close': data.iloc[[ind]]['Close'].values[0][0],
                'high': data.iloc[[ind]]['High'].values[0][0],
                'low': data.iloc[[ind]]['Low'].values[0][0],
                'transactions': 'NULL',
                'ts': data.iloc[[ind]]['ts'].values[0]
            }

            quotes_data.append(quote_dict)

        if len(quotes_data) != length:
            raise FdataError(f"Obtained and parsed data length does not match: {length} != {len(quotes_data)}.")

        return quotes_data

    # TODI MID For correct screeners work it should correspond the data in the main dataset.
    def get_recent_data(self, to_cache=False):
        """
            Get pseudo real time data. Used in screening demonstration.

            Args:
                to_cache(bool): indicates if real time data should be cached in a database.

            Returns:
                list: real time data.
        """
        data = yfin.download(tickers=self.symbol, period='1d', interval='1m', auto_adjust=False)
        row = data.iloc[-1]
        row = row.droplevel(1)

        dt = data.index[-1].to_pydatetime().replace(tzinfo=None)
        ts = int(datetime.timestamp(dt))

        volume = row['Volume'].astype(int)

        result = {'time_stamp': ts,
                  'date_time': dt.replace(microsecond=0).isoformat(' '),
                  'opened': row['Open'],
                  'high': row['High'],
                  'low': row['Low'],
                  'closed': row['Close'],
                  'volume': volume,
                  'transactions': None,
                  'adj_open': row['Open'],
                  'adj_high': row['High'],
                  'adj_low': row['Low'],
                  'adj_close': row['Close'],
                  'adj_volume': volume,
                  'divs_ex': 0.0,
                  'divs_pay': 0.0,
                  'splits': 1.0
                 }

        # TODO LOW caching should be implemented

        result = [result]
        result = get_labelled_ndarray(result)

        return result

    def get_cached_data(self):
        """
            Gets the cached data for dividends/splits.

            Returns:
                data instance for getting dividends/splits.
        """
        if self._data is None or self.symbol != self._data_symbol:
            self._data = yfin.Ticker(self.symbol)
            self._data.history(period='max')

            self._data_symbol = self.symbol

        return self._data

    def __fetch_splits(self):
        """
            Fetch the split data.

            Return:
                DataFrame: splits data
        """
        data = self.get_cached_data()
        splits = data.splits

        df_result = pd.DataFrame()
        # Keep splits at 00:00:00
        df_result['ts'] = splits.keys().tz_convert('UTC').normalize() + timedelta(hours=00, minutes=00, seconds=00)
        df_result['ts'] = df_result['ts'].astype(int).div(10**9).astype(int)  # One more astype to get rid of .0

        df_result['split_ratio'] = splits.reset_index()['Stock Splits']

        return df_result

    def __fetch_dividends(self):
        """
            Fetch cash dividends for the specified period.

            Note that YF dividend data may be incomplete

            Returns:
                DataFrame: cash dividend data.
        """
        data = self.get_cached_data()
        divs = data.dividends
        splits = self.__fetch_splits()

        df_result = pd.DataFrame()
        # Keep dividends at 00:00:00
        df_result['ex_ts'] = divs.keys().tz_convert('UTC').normalize() + timedelta(hours=00, minutes=00, seconds=00)
        df_result['ex_ts'] = df_result['ex_ts'].astype(int).div(10**9).astype(int)  # One more astype to get rid of .0

        df_result['amount'] = divs.reset_index()['Dividends']

        # Not used in this data source
        df_result['currency'] = self.get_currency()
        df_result['decl_ts'] = 'NULL'
        df_result['record_ts'] = 'NULL'
        df_result['pay_ts'] = 'NULL'

        # Reverse-adjust the dividends
        for i in range(len(splits)):
            ind = np.searchsorted(df_result['ex_ts'], [splits['ts'][i] ,], side='right')[0]

            df_result.loc[df_result.index < ind, 'amount'] = df_result.loc[df_result.index < ind, 'amount'] * splits['split_ratio'][i]

        return df_result

    # TODO MID Dividends are adjusted by default!
    def fetch_dividends(self):
        """
            Fetch cash dividends for the specified period.
        """
        return self.__fetch_dividends().T.to_dict().values()

    def fetch_splits(self):
        """
            Fetch the split data.
        """
        return self.__fetch_splits().T.to_dict().values()

    def fetch_info(self):
        """
            Fetch and return the info of the security.

            Returns:
                dict: dictionary with the info
        """
        ticker = yfin.Ticker(self.symbol)

        try:
            info = ticker.info
        except (urllib.error.HTTPError, urllib.error.URLError, http.client.HTTPException) as e:
            raise FdataError(f"Can't fetch info. Likely yfinance needs updating. Invoke pip install yfinance --upgrade: {e}") from e

        info['fc_time_zone'] = info['exchangeTimezoneName']
        info['fc_sec_type'] = SecType.Unknown

        sec_type = info['quoteType']

        if sec_type == 'EQUITY':
            info['fc_sec_type'] = SecType.Stock
        elif sec_type == 'CRYPTOCURRENCY':
            info['fc_sec_type'] = SecType.Crypto
        elif sec_type == 'ETF':
            info['fc_sec_type'] = SecType.ETF

        return info

    def fetch_income_statement(self):
        raise FdataError(f"Income statement data is not supported (yet) for the source {type(self).__name__}")

    def fetch_balance_sheet(self):
        raise FdataError(f"Balance sheet data is not supported (yet) for the source {type(self).__name__}")

    def fetch_cash_flow(self):
        raise FdataError(f"Cash flow data is not supported (yet) for the source {type(self).__name__}")

    def add_income_statement(self, reports):
        raise FdataError(f"Adding income statement data is not supported (yet) for the source {type(self).__name__}")

    def add_balance_sheet(self, reports):
        raise FdataError(f"Adding balance sheet data is not supported (yet) for the source {type(self).__name__}")

    def add_cash_flow(self, reports):
        raise FdataError(f"Adding cash flow data is not supported (yet) for the source {type(self).__name__}")
