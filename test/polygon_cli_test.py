import unittest

import sys
sys.path.append('../')

from dateutil.relativedelta import relativedelta
from datetime import date

import polygon_cli

class Test(unittest.TestCase):
    def test_0_check_arg_parser(self):
        argv = ['./polygon_cli.py', '-s', 'AAPL', '-d', 'test.sqlite', '-t', 'intraday', '-f', '2019-07-22', '-l', '2020-08-30', '-r']

        query = polygon_cli.arg_parser(argv)

        self.assertEqual(query.symbol, "AAPL")
        self.assertEqual(query.first_date, "2019-07-22")
        self.assertEqual(query.last_date, "2020-08-30")
        self.assertEqual(query.update, "REPLACE")
        self.assertEqual(query.source_title, "Polygon.io")
        self.assertEqual(query.year_delta, "2")

        self.assertEqual(query.db_name, "test.sqlite")
        self.assertEqual(query.db_type, "sqlite")
        self.assertEqual(query.database, None)
        self.assertEqual(query.conn, None)
        self.assertEqual(query.cur, None)
        self.assertEqual(query.Error, None)

    def test_1_check_arg_parser(self):

        argv = ['./polygon_cli.py', '-s', 'SPY']

        query = polygon_cli.arg_parser(argv)

        self.assertEqual(query.symbol, "SPY")
        self.assertEqual(query.year_delta, "2")
        self.assertEqual(query.first_date, 1606176000)
        self.assertEqual(query.last_date, 1669334399)
        self.assertEqual(query.update, "IGNORE")
        self.assertEqual(query.source_title, "Polygon.io")

        self.assertEqual(query.db_name, "data.sqlite")
        self.assertEqual(query.db_type, "sqlite")
        self.assertEqual(query.database, None)
        self.assertEqual(query.conn, None)
        self.assertEqual(query.cur, None)
        self.assertEqual(query.Error, None)
