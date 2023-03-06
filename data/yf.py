"""Yahoo Finance wrapper.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""
from datetime import datetime
import pytz

import yfinance as yfin

from data import fdata
from data.fvalues import Timespans, def_first_date, def_last_date
from data.fdata import FdataError

class YF(fdata.BaseFetchData):
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

    def get_timespan(self):
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
            raise FdataError(f"Requested timespan is not supported by Polygon: {self.timespan}")

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
                last_date = current_date

            data = yfin.Ticker(self.symbol).history(interval=self.get_timespan(),
                                                        start=self.first_date_str,
                                                        end=last_date)
        else:
            data = yfin.Ticker(self.symbol).history(interval=self.get_timespan(), period='max')

        length = len(data)

        if length == 0:
            raise FdataError(f"Can not fetch quotes for {self.symbol}. No quotes fetched.")

        # Create a list of dictionaries with quotes
        quotes_data = []

        for ind in range(length):
            dt = data.index[ind]
            dt = dt.replace(tzinfo=pytz.utc)
            ts = int(datetime.timestamp(dt))

            if self.get_timespan() in [Timespans.Day, Timespans.Week, Timespans.Month]:
                # Add 23:59:59 to non-intraday quotes
                quote_dict['t'] = ts + 86399

                # Stock split coefficient 0 (reported by default) should be set to 1 as it makes more sense
                stock_splits = data['Stock Splits'][ind]
                if stock_splits == 0:
                    stock_splits = 1
            else:
                # Stock splits has no sense intraday
                stock_splits = 1

            quote_dict = {
                'volume': data['Volume'][ind],
                'open': data['Open'][ind],
                'adj_close': data['Close'][ind],
                'high': data['High'][ind],
                'low': data['Low'][ind],
                'raw_close': 'NULL',
                'transactions': 'NULL',
                'divs': data['Dividends'][ind],
                'split': stock_splits,
                'ts': ts
            }

            quotes_data.append(quote_dict)

        if len(quotes_data) != length:
            raise FdataError(f"Obtained and parsed data length does not match: {length} != {len(quotes_data)}.")

        return quotes_data

    # TODO this method should be abstract in the base class
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

        result = [self.symbol,
                  'NULL',
                  self.source_title,
                  # TODO check if such datetime manipulations may have an impact depending on a locale.
                  str(data.index[-1])[:16],
                  # TODO check if it is ok to set timespan this way and if it may cause some db operations issues
                  Timespans.Tick.value,
                  row['Open'],
                  row['High'],
                  row['Low'],
                  row['Close'],
                  row['Adj Close'],
                  row['Volume'],
                  'NULL',
                  'NULL',
                  'NULL']

        # TODO caching should be implemented

        return result
