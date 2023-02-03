import unittest

import sys
sys.path.append('../')

import yfinance
import pandas as pd

from datetime import datetime, timedelta

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

    def test_0_check_arg_parser(self):
        source = yf.YF()
        source.symbol = 'SPY'

        last_date = datetime.now()
        first_date = last_date - timedelta(days=7)
        timespan = Timespans.Day

        source.first_date = first_date
        source.last_date = last_date
        source.timespan = timespan

        hist = History()

        quotes_data = [{'v': 11, 'o': 1, 'c': 3, 'h': 5, 'l': 7, 'cl': 'NULL', 'n': 'NULL', 'vw': 'NULL', 'd': 9, 't': source.first_date_ts}, \
                       {'v': 12, 'o': 2, 'c': 4, 'h': 6, 'l': 8, 'cl': 'NULL', 'n': 'NULL', 'vw': 'NULL', 'd': 10, 't': source.last_date_ts}]

        df = pd.DataFrame({'Date': [first_date, last_date],
                           'Open': [1, 2],
                           'Close': [3, 4],
                           'High': [5, 6],
                           'Low': [7, 8],
                           'Dividends': [9, 10],
                           'Volume': [11, 12]
                         })
        df = df.set_index('Date')

        # Mocking
        when(yfinance).Ticker(source.symbol).thenReturn(hist)
        when(hist).history(interval=source.get_timespan(), \
                           start=source.first_date_str, \
                           end=source.last_date_str).thenReturn(df)

        return_data = source.fetch_quotes()

        verify(yfinance, times=1).Ticker(source.symbol)
        verify(hist, times=1).history(interval=source.get_timespan(), \
                                      start=source.first_date_str, \
                                      end=source.last_date_str)

        assert return_data == quotes_data

    def test_1_get_rt_data(self):
        source = yf.YF()
        source.symbol = 'SPY'

        df = pd.DataFrame()

        df['Open'] = [1, 2]
        df['Close'] = [2, 3]
        df['Adj Close'] = [3, 4]
        df['High'] = [4, 5]
        df['Low'] = [5, 6]
        df['Volume'] = [0, 0]
        df['DateTime'] = [datetime.now(), datetime.now()]

        df.set_index('DateTime')

        # Mocking
        when(yfinance).download(tickers=source.symbol, period='1d', interval='1m').thenReturn(df)

        return_data = source.get_rt_data()

        verify(yfinance, times=1).download(tickers=source.symbol, period='1d', interval='1m')

        expected_result = ['SPY', None, 'YF', '1', 'Day', 2, 5, 6, 3, 4, 0, None, None, None]

        assert return_data == expected_result
