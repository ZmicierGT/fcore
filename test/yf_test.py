import unittest

import sys
sys.path.append('../')

import yfinance
import pandas as pd

from datetime import datetime, timedelta
import pytz

from mockito import when, verify, unstub

from data.fvalues import Timespans
from data import yf
from data.fdata import FdataError

class History():
    def history(self, interval, start, end):
        pass

class Test(unittest.TestCase):
    def tearDown(self):
        unstub()

    def test_1_check_fetch_quotes(self):
        source = yf.YF()
        source.symbol = 'SPY'

        first_date = datetime(2022, 11, 28, 23, 55, 59).replace(tzinfo=pytz.utc)
        last_date = datetime(2022, 12, 28, 23, 55, 59).replace(tzinfo=pytz.utc)

        timespan = Timespans.Day

        source.first_date = first_date
        source.last_date = last_date
        source.timespan = timespan

        hist = History()

        quote_dict1 = {
            'volume': 11,
            'open': 1,
            'adj_close': 3,
            'high': 5,
            'low': 7,
            'raw_close': 'NULL',
            'transactions': 'NULL',
            'divs': 9,
            'split': 13,
            'ts': 1669679999,
            'sectype': source.sectype.value,
            'currency': source.currency.value
        }

        quote_dict2 = {
            'volume': 12,
            'open': 2,
            'adj_close': 4,
            'high': 6,
            'low': 8,
            'raw_close': 'NULL',
            'transactions': 'NULL',
            'divs': 10,
            'split': 14,
            'ts': 1672271999,
            'sectype': source.sectype.value,
            'currency': source.currency.value
        }

        quotes_data = [quote_dict1, quote_dict2]

        df = pd.DataFrame({'Date': [first_date, last_date],
                           'Open': [1, 2],
                           'Close': [3, 4],
                           'High': [5, 6],
                           'Low': [7, 8],
                           'Dividends': [9, 10],
                           'Volume': [11, 12],
                           'Stock Splits': [13, 14]
                         })
        df = df.set_index('Date')

        # Mocking
        when(yfinance).Ticker(source.symbol).thenReturn(hist)
        when(hist).history(interval=source.get_timespan_str(), \
                           start=source.first_date_str, \
                           end=source.last_date_str).thenReturn(df)

        return_data = source.fetch_quotes()

        verify(yfinance, times=1).Ticker(source.symbol)
        verify(hist, times=1).history(interval=source.get_timespan_str(), \
                                      start=source.first_date_str, \
                                      end=source.last_date_str)

        assert return_data == quotes_data

    def test_2_get_recent_data(self):
        source = yf.YF()

        ts1 = pd.Timestamp('2023-03-29 09:31:00-0400')
        ts2 = pd.Timestamp('2023-03-29 15:59:00-0400')

        df = pd.DataFrame()

        df['Datetime'] = [ts1, ts2]
        df = df.set_index('Datetime')

        df['Open'] = [1, 2]
        df['Close'] = [2, 3]
        df['Adj Close'] = [3, 4]
        df['High'] = [4, 5]
        df['Low'] = [5, 6]
        df['Volume'] = [0, 0]

        # Mocking
        when(yfinance).download(tickers=source.symbol, period='1d', interval='1m').thenReturn(df)

        return_data = source.get_recent_data()

        verify(yfinance, times=1).download(tickers=source.symbol, period='1d', interval='1m')

        expected_result = [1680105540, 2, 5, 6, 3, 4, 0, 'NULL', 'NULL', 'NULL']

        assert return_data == expected_result
