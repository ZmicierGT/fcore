"""Yahoo Finance wrapper.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from datetime import datetime, timedelta
import pytz

import pandas as pd
import numpy as np

import yfinance as yfin

from data import stock
from data.fvalues import Timespans, SecType, Currency, def_first_date, def_last_date
from data.fdata import FdataError
from data.futils import get_dt, get_labelled_ndarray

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

        self.sectype = SecType.Unknown  # Multiple security types may be obtaines by similar YF queries
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
        elif self.timespan == Timespans.FiveDays:
            return '5d'
        elif self.timespan == Timespans.Week:
            return "1wk"
        elif self.timespan == Timespans.Month:
            return '1mo'
        elif self.timespan == Timespans.Quarter:
            return '3mo'
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

    def is_intraday(self):
        """
            Checks if current timespan is intraday.

            Returns:
                bool: if current timespan is intraday.
        """
        return self.timespan not in (Timespans.Day, Timespans.FiveDays, Timespans.Week, Timespans.Month, Timespans.Quarter)

    def fetch_quotes(self):
        """
            The method to fetch quotes.

            Returns:
                list: quotes data

            Raises:
                FdataError: network error, no data obtained, can't parse json or the date is incorrect.
        """
        if self.first_date_ts != def_first_date or self.last_date_ts != def_last_date:
            last_date = self.last_date.replace(tzinfo=pytz.utc)
            current_date = datetime.now().replace(tzinfo=pytz.utc) + timedelta(days=1)

            if last_date > current_date:
                last_date_str = current_date.strftime('%Y-%m-%d')
            else:
                last_date_str = self.last_date_str

            data = yfin.download(self.symbol,
                                 interval=self.get_timespan_str(),
                                 start=self.first_date_str,
                                 end=last_date_str)
        else:
            data = yfin.download(self.symbol, interval=self.get_timespan_str(), period='max')

        length = len(data)

        if length == 0:
            raise FdataError(f"Can not fetch quotes for {self.symbol}. No quotes fetched.")

        data = data.reset_index()
        # TODO LOW Currently EOD datetimes are set to 23:59:59 UTC (the same say).
        data['ts'] = data['Date'].dt.tz_localize('UTC').dt.normalize() + timedelta(hours=23, minutes=59, seconds=59)
        data['ts'] = data['ts'].astype(int).div(10**9).astype(int)  # One more astype to get rid of .0

        if self.is_intraday() is False:
            # Reverse-adjust the quotes
            splits = self.__fetch_splits()

            for i in range(len(splits)):
                ind = np.searchsorted(data['ts'], [splits['ts'][i] ,], side='right')[0]

                data.loc[data.index < ind, 'Open'] = data.loc[data.index < ind, 'Open'] * splits['split_ratio'][i]
                data.loc[data.index < ind, 'High'] = data.loc[data.index < ind, 'High'] * splits['split_ratio'][i]
                data.loc[data.index < ind, 'Low'] = data.loc[data.index < ind, 'Low'] * splits['split_ratio'][i]
                data.loc[data.index < ind, 'Close'] = data.loc[data.index < ind, 'Close'] * splits['split_ratio'][i]
                data.loc[data.index < ind, 'Volume'] = data.loc[data.index < ind, 'Volume'] / splits['split_ratio'][i]

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
                'ts': data['ts'][ind],
                'sectype': self.sectype.value,
                'currency': self.currency.value
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

        dt = data.index[-1].to_pydatetime().replace(tzinfo=pytz.utc)
        ts = int(datetime.timestamp(dt))

        result = {'time_stamp': ts,
                  'date_time': get_dt(ts).replace(microsecond=0).isoformat(' '),
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
        df_result['ts'] = splits.reset_index()['Date'].astype(int).div(10**9).astype(int)  # One more astype to get rid of .0

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
        df_result['ex_ts'] = divs.keys().tz_convert('UTC').normalize()
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

    def fetch_earnings(self):
        raise FdataError(f"Earnings statement data is not supported (yet) for the source {type(self).__name__}")
