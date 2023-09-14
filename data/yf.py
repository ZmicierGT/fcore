"""Yahoo Finance wrapper.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from datetime import datetime
import pytz

import pandas as pd

import yfinance as yfin

from data import stock
from data.fvalues import Timespans, SecType, Currency, def_first_date, def_last_date
from data.fdata import FdataError

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
            raise FdataError(f"Requested timespan is not supported by Polygon: {self.timespan.value}")

    def fetch_quotes(self):
        """
            The method to fetch quotes.

            Returns:
                list: quotes data

            Raises:
                FdataError: network error, no data obtained, can't parse json or the date is incorrect.
        """
        if self.first_date_ts != def_first_date or self.last_date_ts != def_last_date:
            last_date = self.last_date
            current_date = datetime.now().replace(tzinfo=pytz.utc)

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

        # Create a list of dictionaries with quotes
        quotes_data = []

        for ind in range(length):
            dt = data.index[ind]
            dt = dt.replace(tzinfo=pytz.utc)

            if self.timespan in [Timespans.Day, Timespans.Week, Timespans.Month]:
                # Add 23:59:59 to non-intraday quotes
                dt = dt.replace(hour=23, minute=59, second=59)

            quote_dict = {
                'volume': data['Volume'][ind],
                'open': data['Open'][ind],
                'close': data['Close'][ind],
                'high': data['High'][ind],
                'low': data['Low'][ind],
                'transactions': 'NULL',
                'ts': int(datetime.timestamp(dt)),
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
        ts = datetime.timestamp(dt)

        result = [int(ts),
                  row['Open'],
                  row['High'],
                  row['Low'],
                  row['Close'],
                  row['Volume'],
                  'NULL']  # Transactions

        # TODO LOW caching should be implemented

        return result

    def fetch_dividends(self):
        """
            Fetch cash dividends for the specified period.
        """
        # TODO HIGH Make it cached
        data = yfin.Ticker(self.symbol)
        divs = data.dividends

        df_result = pd.DataFrame()
        df_result['ex_ts'] = divs.keys().tz_convert('UTC').astype(int)
        df_result['ex_ts'] = df_result['ex_ts'].div(10**9).astype(int)  # One more astype to get rid of .0

        df_result['amount'] = divs.reset_index()['Dividends']

        # Not used in this data source
        df_result['currency'] = self.currency.value
        df_result['decl_ts'] = 'NULL'
        df_result['record_ts'] = 'NULL'
        df_result['pay_ts'] = 'NULL'

        return df_result.T.to_dict().values()

    def fetch_splits(self):
        """
            Fetch the split data.
        """
        data = yfin.Ticker(self.symbol)
        splits = data.splits

        df_result = pd.DataFrame()
        df_result['ts'] = splits.keys().tz_convert('UTC').astype(int)
        df_result['ts'] = df_result['ts'].div(10**9).astype(int)  # One more astype to get rid of .0

        df_result['split_ratio'] = splits.reset_index()['Stock Splits']

        return df_result.T.to_dict().values()

    def fetch_income_statement(self):
        raise FdataError(f"Income statement data is not supported (yet) for the source {type(self).__name__}")

    def fetch_balance_sheet(self):
        raise FdataError(f"Balance sheet data is not supported (yet) for the source {type(self).__name__}")

    def fetch_cash_flow(self):
        raise FdataError(f"Cash flow data is not supported (yet) for the source {type(self).__name__}")

    def fetch_earnings(self):
        raise FdataError(f"Earnings statement data is not supported (yet) for the source {type(self).__name__}")
