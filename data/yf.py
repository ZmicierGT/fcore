"""Yahoo Finance wrapper.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

import yfinance as yfin

from data import stock
from data.fvalues import Timespans, SecType, Currency
from data.fdata import FdataError
from data.futils import get_labelled_ndarray, get_dt

import pytz

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

        self.sectype = SecType.Stock  # Be careful as theorefically multiple security types may be obtaines by similar YF queries
        self.currency = Currency.Unknown  # Currencies are not supported yet

        self._data = None  # Cached data for splits/divs
        self._data_symbol = self.symbol  # Symbol of cached data

        self._tz = None  # Cached time zone
        self._tz_symbol = self.symbol  # Symbl for cached time zone

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

    def get_timezone(self):
        """
            Get the time zone of the specified symbol.

            Returns:
                string: time zone.
        """
        # Check if time zone is already obtained
        if self._tz is None or self.symbol != self._tz_symbol:
            self._tz = yfin.Ticker(self.symbol).info['timeZoneFullName']

        return self._tz

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
        if first_ts is None:
            first_ts = self.first_date_ts
        if last_ts is None:
            last_ts = self.last_date_ts

        current_ts = int((datetime.now().replace(tzinfo=pytz.UTC) + timedelta(days=1)).timestamp())

        if last_ts > current_ts:
            last_ts = current_ts

        first_date = get_dt(first_ts, pytz.UTC)
        last_date = get_dt(last_ts, pytz.UTC)

        if (last_date - first_date).days == 0:
            first_date = first_date - timedelta(days=1)

        first_date_str = first_date.strftime('%Y-%m-%d')
        last_date_str = last_date.strftime('%Y-%m-%d')

        data = yfin.download(self.symbol,
                                interval=self.get_timespan_str(),
                                start=first_date_str,
                                end=last_date_str)

        length = len(data)

        if length == 0:
            self.log(f"Can not fetch quotes for {self.symbol}. No quotes fetched.")
            return

        data = data.reset_index()

        if self.is_intraday() is False:
            data['ts'] = data['Date'].dt.normalize() + timedelta(hours=23, minutes=59, seconds=59)
            data['ts'] = data['ts'].astype(int).div(10**9).astype(int)  # One more astype to get rid of .0

            # Reverse-adjust the quotes
            splits = self.__fetch_splits()

            for i in range(len(splits)):
                ind = np.searchsorted(data['ts'], [splits['ts'][i] ,], side='right')[0]

                data.loc[data.index < ind, 'Open'] = data.loc[data.index < ind, 'Open'] * splits['split_ratio'][i]
                data.loc[data.index < ind, 'High'] = data.loc[data.index < ind, 'High'] * splits['split_ratio'][i]
                data.loc[data.index < ind, 'Low'] = data.loc[data.index < ind, 'Low'] * splits['split_ratio'][i]
                data.loc[data.index < ind, 'Close'] = data.loc[data.index < ind, 'Close'] * splits['split_ratio'][i]
                data.loc[data.index < ind, 'Volume'] = round(data.loc[data.index < ind, 'Volume'] / splits['split_ratio'][i])
        else:
            # One more astype to get rid of .0
            data['ts'] = data['Datetime'].astype(int).div(10**9).astype(int)

        # Create a list of dictionaries with quotes
        quotes_data = []

        for ind in range(length):
            quote_dict = {
                'volume': data['Volume'][ind],
                'open': data['Open'][ind],
                'close': data['Close'][ind],
                'high': data['High'][ind],
                'low': data['Low'][ind],
                'transactions': 'NULL',
                'ts': data['ts'][ind]
            }

            quotes_data.append(quote_dict)

        if len(quotes_data) != length:
            raise FdataError(f"Obtained and parsed data length does not match: {length} != {len(quotes_data)}.")

        return quotes_data

    def get_recent_data(self, to_cache=False):
        """
            Get pseudo real time data. Used in screening demonstration.

            Args:
                to_cache(bool): indicates if real time data should be cached in a database.

            Returns:
                list: real time data.
        """
        data = yfin.download(tickers=self.symbol, period='1d', interval='1m')
        row = data.iloc[-1]

        dt = data.index[-1].to_pydatetime().replace(tzinfo=None)
        ts = int(datetime.timestamp(dt))

        result = {'time_stamp': ts,
                  'date_time': dt.replace(microsecond=0).isoformat(' '),
                  'opened': row['Open'],
                  'high': row['High'],
                  'low': row['Low'],
                  'closed': row['Close'],
                  'volume': row['Volume'],
                  'transactions': None,
                  'adj_close': row['Close'],
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

            Returns:
                DataFrame: cash dividend data.
        """
        data = self.get_cached_data()
        divs = data.dividends

        df_result = pd.DataFrame()
        # Keep dividends at 00:00:00
        df_result['ex_ts'] = divs.keys().tz_convert('UTC').normalize() + timedelta(hours=00, minutes=00, seconds=00)
        df_result['ex_ts'] = df_result['ex_ts'].astype(int).div(10**9).astype(int)  # One more astype to get rid of .0

        df_result['amount'] = divs.reset_index()['Dividends']

        # Not used in this data source
        df_result['currency'] = self.currency.value
        df_result['decl_ts'] = 'NULL'
        df_result['record_ts'] = 'NULL'
        df_result['pay_ts'] = 'NULL'

        return df_result

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
