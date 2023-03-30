import unittest

import sys
sys.path.append('../')

from data.fvalues import DbTypes

import yf_cli

class Test(unittest.TestCase):
    def test_0_check_arg_parser(self):
        argv = ['./yf_cli.py', '-s', 'AAPL', '-d', 'test.sqlite', '-t', 'Month', '-f', '2019-07-22', '-l', '2020-08-30', '-r']

        source = yf_cli.arg_parser(argv)

        self.assertEqual(source.symbol, "AAPL")
        self.assertEqual(source.first_date_ts, 1563753600)
        self.assertEqual(source.last_date_ts, 1598745600)
        self.assertEqual(source.update, True)
        self.assertEqual(source.source_title, "YF")

        self.assertEqual(source.db_name, "test.sqlite")
        self.assertEqual(source.db_type, DbTypes.SQLite)
        self.assertEqual(source.database, None)
        self.assertEqual(source.conn, None)
        self.assertEqual(source.cur, None)
        self.assertEqual(source.Error, None)

    def test_1_check_arg_parser(self):

        argv = ['./yf_cli.py', '-s', 'SPY']

        source = yf_cli.arg_parser(argv)

        self.assertEqual(source.symbol, "SPY")
        self.assertEqual(source.first_date_ts, -2147483648)
        self.assertEqual(source.last_date_ts, 9999999999)
        self.assertEqual(source.update, False)
        self.assertEqual(source.source_title, "YF")

        self.assertEqual(source.db_name, "data.sqlite")
        self.assertEqual(source.db_type, DbTypes.SQLite)
        self.assertEqual(source.database, None)
        self.assertEqual(source.conn, None)
        self.assertEqual(source.cur, None)
        self.assertEqual(source.Error, None)
