import unittest

import sys
sys.path.append('../')

import quotes

class Test(unittest.TestCase):
    def test_0_check_arg_parser(self):
        argv = ['.\\quotes.py', '-s', 'SPY', '-f', '2022-06-10', '-l', '2022-06-13', '-r']

        query = quotes.arg_parser(argv)

        self.assertTrue(query.to_remove_quotes)
        self.assertFalse(query.to_print_quotes)
        self.assertFalse(query.to_build_chart)
        self.assertFalse(query.to_print_all)

        self.assertEqual(query.symbol, "SPY")
        self.assertEqual(query.first_date_ts, 1654819200)
        self.assertEqual(query.last_date_ts, 1655164799)
        self.assertEqual(query.update, False)
        self.assertEqual(query.source_title, "Quotes")

        self.assertEqual(query.db_name, "data.sqlite")
        self.assertEqual(query.db_type, "sqlite")
        self.assertEqual(query.database, None)
        self.assertEqual(query.conn, None)
        self.assertEqual(query.cur, None)
        self.assertEqual(query.Error, None)

    def test_1_check_arg_parser(self):
        argv = ['.\\quotes.py', '-d', 'test.sqlite', '-s', 'SPY', '-c']

        query = quotes.arg_parser(argv)

        self.assertFalse(query.to_remove_quotes)
        self.assertFalse(query.to_print_quotes)
        self.assertTrue(query.to_build_chart)
        self.assertFalse(query.to_print_all)

        self.assertEqual(query.symbol, "SPY")
        self.assertEqual(query.first_date_ts, -2147483648)
        self.assertEqual(query.last_date_ts, 9999999999)
        self.assertEqual(query.update, False)
        self.assertEqual(query.source_title, "Quotes")

        self.assertEqual(query.db_name, "test.sqlite")
        self.assertEqual(query.db_type, "sqlite")
        self.assertEqual(query.database, None)
        self.assertEqual(query.conn, None)
        self.assertEqual(query.cur, None)
        self.assertEqual(query.Error, None)

    def test_2_check_arg_parser(self):
        argv = ['.\\quotes.py', '-a']

        query = quotes.arg_parser(argv)

        self.assertFalse(query.to_remove_quotes)
        self.assertFalse(query.to_print_quotes)
        self.assertFalse(query.to_build_chart)
        self.assertTrue(query.to_print_all)

        self.assertEqual(query.symbol, "")
        self.assertEqual(query.first_date_ts, -2147483648)
        self.assertEqual(query.last_date_ts, 9999999999)
        self.assertEqual(query.update, False)
        self.assertEqual(query.source_title, "Quotes")

        self.assertEqual(query.db_name, "data.sqlite")
        self.assertEqual(query.db_type, "sqlite")
        self.assertEqual(query.database, None)
        self.assertEqual(query.conn, None)
        self.assertEqual(query.cur, None)
        self.assertEqual(query.Error, None)

    def test_3_check_arg_parser(self):
        argv = ['.\\quotes.py', '-s', 'SPY', '-q']

        query = quotes.arg_parser(argv)

        self.assertFalse(query.to_remove_quotes)
        self.assertTrue(query.to_print_quotes)
        self.assertFalse(query.to_build_chart)
        self.assertFalse(query.to_print_all)

        self.assertEqual(query.symbol, "SPY")
        self.assertEqual(query.first_date_ts, -2147483648)
        self.assertEqual(query.last_date_ts, 9999999999)
        self.assertEqual(query.update, False)
        self.assertEqual(query.source_title, "Quotes")

        self.assertEqual(query.db_name, "data.sqlite")
        self.assertEqual(query.db_type, "sqlite")
        self.assertEqual(query.database, None)
        self.assertEqual(query.conn, None)
        self.assertEqual(query.cur, None)
        self.assertEqual(query.Error, None)
