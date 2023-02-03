import unittest

import sys
sys.path.append('../')

import quotes

class Test(unittest.TestCase):
    def test_0_check_arg_parser(self):
        argv = ['.\\quotes.py', '-s', 'SPY', '-f', '2022-06-10', '-l', '2022-06-13', '-r']

        source = quotes.arg_parser(argv)

        self.assertTrue(source.to_remove_quotes)
        self.assertFalse(source.to_print_quotes)
        self.assertFalse(source.to_build_chart)
        self.assertFalse(source.to_print_all)

        self.assertEqual(source.symbol, "SPY")
        self.assertEqual(source.first_date_ts, 1654819200)
        self.assertEqual(source.last_date_ts, 1655164799)
        self.assertEqual(source.update, False)
        self.assertEqual(source.source_title, "")

        self.assertEqual(source.db_name, "data.sqlite")
        self.assertEqual(source.db_type, "sqlite")
        self.assertEqual(source.database, None)
        self.assertEqual(source.conn, None)
        self.assertEqual(source.cur, None)
        self.assertEqual(source.Error, None)

    def test_1_check_arg_parser(self):
        argv = ['.\\quotes.py', '-d', 'test.sqlite', '-s', 'SPY', '-c']

        source = quotes.arg_parser(argv)

        self.assertFalse(source.to_remove_quotes)
        self.assertFalse(source.to_print_quotes)
        self.assertTrue(source.to_build_chart)
        self.assertFalse(source.to_print_all)

        self.assertEqual(source.symbol, "SPY")
        self.assertEqual(source.first_date_ts, -2147483648)
        self.assertEqual(source.last_date_ts, 9999999999)
        self.assertEqual(source.update, False)
        self.assertEqual(source.source_title, "")

        self.assertEqual(source.db_name, "test.sqlite")
        self.assertEqual(source.db_type, "sqlite")
        self.assertEqual(source.database, None)
        self.assertEqual(source.conn, None)
        self.assertEqual(source.cur, None)
        self.assertEqual(source.Error, None)

    def test_2_check_arg_parser(self):
        argv = ['.\\quotes.py', '-a']

        source = quotes.arg_parser(argv)

        self.assertFalse(source.to_remove_quotes)
        self.assertFalse(source.to_print_quotes)
        self.assertFalse(source.to_build_chart)
        self.assertTrue(source.to_print_all)

        self.assertEqual(source.symbol, "")
        self.assertEqual(source.first_date_ts, -2147483648)
        self.assertEqual(source.last_date_ts, 9999999999)
        self.assertEqual(source.update, False)
        self.assertEqual(source.source_title, "")

        self.assertEqual(source.db_name, "data.sqlite")
        self.assertEqual(source.db_type, "sqlite")
        self.assertEqual(source.database, None)
        self.assertEqual(source.conn, None)
        self.assertEqual(source.cur, None)
        self.assertEqual(source.Error, None)

    def test_3_check_arg_parser(self):
        argv = ['.\\quotes.py', '-s', 'SPY', '-q']

        source = quotes.arg_parser(argv)

        self.assertFalse(source.to_remove_quotes)
        self.assertTrue(source.to_print_quotes)
        self.assertFalse(source.to_build_chart)
        self.assertFalse(source.to_print_all)

        self.assertEqual(source.symbol, "SPY")
        self.assertEqual(source.first_date_ts, -2147483648)
        self.assertEqual(source.last_date_ts, 9999999999)
        self.assertEqual(source.update, False)
        self.assertEqual(source.source_title, "")

        self.assertEqual(source.db_name, "data.sqlite")
        self.assertEqual(source.db_type, "sqlite")
        self.assertEqual(source.database, None)
        self.assertEqual(source.conn, None)
        self.assertEqual(source.cur, None)
        self.assertEqual(source.Error, None)
