import unittest

import sys
sys.path.append('../')

from dateutil.relativedelta import relativedelta
from datetime import date

import yf_cli

class Test(unittest.TestCase):
    def test_0_check_arg_parser(self):
        argv = ['./yf_cli.py', '-s', 'AAPL', '-d', 'test.sqlite', '-t', 'month', '-f', '2019-07-22', '-l', '2020-08-30', '-r']

        query = yf_cli.arg_parser(argv)

        self.assertEqual(query.symbol, "AAPL")
        self.assertEqual(query.first_date, 1563753600)
        self.assertEqual(query.last_date, 1598745600)
        self.assertEqual(query.update, "REPLACE")
        self.assertEqual(query.source_title, "YF")

        self.assertEqual(query.db_name, "test.sqlite")
        self.assertEqual(query.db_type, "sqlite")
        self.assertEqual(query.database, None)
        self.assertEqual(query.conn, None)
        self.assertEqual(query.cur, None)
        self.assertEqual(query.Error, None)

    def test_1_check_arg_parser(self):

        argv = ['./yf_cli.py', '-s', 'SPY']

        query = yf_cli.arg_parser(argv)

        self.assertEqual(query.symbol, "SPY")
        self.assertEqual(query.first_date, -2147483648)
        self.assertEqual(query.last_date, 9999999999999)
        self.assertEqual(query.update, "IGNORE")
        self.assertEqual(query.source_title, "YF")

        self.assertEqual(query.db_name, "data.sqlite")
        self.assertEqual(query.db_type, "sqlite")
        self.assertEqual(query.database, None)
        self.assertEqual(query.conn, None)
        self.assertEqual(query.cur, None)
        self.assertEqual(query.Error, None)