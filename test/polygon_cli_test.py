import unittest

import sys
sys.path.append('../')

import polygon_cli

import settings

class Test(unittest.TestCase):
    def test_0_check_arg_parser(self):
        argv = ['./polygon_cli.py', '-s', 'AAPL', '-d', 'test.sqlite', '-t', 'Minute', '-f', '2019-07-22', '-l', '2020-08-30', '-r']

        settings.Polygon.api_key = 'test'
        source = polygon_cli.arg_parser(argv)

        self.assertEqual(source.symbol, "AAPL")
        self.assertEqual(source.first_datetime_str, "2019-07-22 00:00:00")
        self.assertEqual(source.last_datetime_str, "2020-08-30 00:00:00")
        self.assertEqual(source.update, True)
        self.assertEqual(source.source_title, "Polygon.io")
        self.assertEqual(source.year_delta, 2)

        self.assertEqual(source.db_name, "test.sqlite")
        self.assertEqual(source.db_type, "sqlite")
        self.assertEqual(source.database, None)
        self.assertEqual(source.conn, None)
        self.assertEqual(source.cur, None)
        self.assertEqual(source.Error, None)
