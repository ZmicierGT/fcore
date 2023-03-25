import unittest

from mockito import when, mock, verify, unstub

import sys
sys.path.append('../')

from data.fdata import ReadOnlyData, ReadWriteData, BaseFetcher
from data.fvalues import Timespans

import sqlite3
from sqlite3 import Cursor, Connection

class FetchData(BaseFetcher):
    # Implement abstract method
    def fetch_quotes(self):
        pass

class Test(unittest.TestCase):
    def setUp(self):
        self.read_data = ReadOnlyData()

        self.read_data.conn = mock(Connection)
        self.read_data.cur = mock(Cursor)

        self.read_data.Error = BaseException
        self.read_data.symbol = "AAPL"
        self.read_data.first_date = 0
        self.read_data.last_date = 1
        self.read_data.timespan = Timespans.Day

        self.test_db = 'test.sqlite'
        self.result = "result"
        self.results = ['one', 'two']

        when(self.read_data.conn).cursor().thenReturn(self.read_data.cur)
        when(self.read_data.cur).fetchone().thenReturn(self.result)
        when(self.read_data.cur).fetchall().thenReturn(self.results)

        self.write_data = ReadWriteData()

        self.write_data.conn = mock(Connection)
        self.write_data.cur = mock(Cursor)

        self.write_data.Error = BaseException
        self.write_data.symbol = "AAPL"
        self.write_data.first_date = 0
        self.write_data.last_date = 1
        self.write_data.timespan = Timespans.Day

        when(self.write_data.conn).cursor().thenReturn(self.write_data.cur)
        when(self.write_data.cur).fetchone().thenReturn(self.result)
        when(self.write_data.cur).fetchall().thenReturn(self.results)
        when(self.write_data.conn).commit().thenReturn()

        self.fetch_data = FetchData()

        self.fetch_data.conn = mock(Connection)
        self.fetch_data.cur = mock(Cursor)

        self.fetch_data.Error = BaseException
        self.fetch_data.symbol = "AAPL"
        self.fetch_data.first_date = 0
        self.fetch_data.last_date = 1
        self.fetch_data.timespan = Timespans.Day

    def tearDown(self):
        unstub()

    # Read data methods test

    def test_0_check_base_query_connect(self):
        sql_query = "SELECT title FROM sources WHERE title = '';"
        when(self.read_data.cur).execute(sql_query).thenReturn()

        assert self.read_data.check_source() == len(self.results)

        verify(self.read_data.cur, times=1).execute(sql_query)
        verify(self.read_data.cur, times=1).fetchall()

    def test_16_check_source(self):
        sql_query = f"SELECT title FROM sources WHERE title = '{self.read_data.source_title}';"

        when(self.read_data.cur).execute(sql_query).thenReturn()

        assert self.read_data.check_source() == len(self.results)

        verify(self.read_data.cur, times=1).execute(sql_query)
        verify(self.read_data.cur, times=1).fetchall()

    def test_1_check_get_all_symbols(self):
        sql_query = "SELECT ticker, ISIN, description FROM symbols;"

        when(self.read_data.cur).execute(sql_query).thenReturn()

        assert self.read_data.get_all_symbols() == self.results

        verify(self.read_data.cur, times=1).execute(sql_query)
        verify(self.read_data.cur, times=1).fetchall()

    def test_3_check_get_quotes(self):
        sql_query = f"""SELECT ticker,
                                ISIN,
                                sources.title,
                                datetime(time_stamp, 'unixepoch'),
                                timespans.title,
                                "Open",
                                High,
                                Low,
                                "Close",
                                AdjClose,
                                Volume,
                                Dividends,
                                Transactions,
                                VWAP
                            FROM quotes INNER JOIN symbols ON quotes.symbol_id = symbols.symbol_id
                            INNER JOIN sources ON quotes.source_id = sources.source_id
                            INNER JOIN timespans ON quotes.timespan_id = timespans.timespan_id
                            WHERE symbols.ticker = '{self.read_data.symbol}'
                            AND timespans.title = 'Day'
                            AND time_stamp >= {self.read_data.first_date_ts}
                            AND time_stamp <= {self.read_data.last_date_ts} ORDER BY time_stamp;"""

        when(self.read_data.cur).execute(sql_query).thenReturn()

        assert self.read_data.get_quotes() == self.results

        verify(self.read_data.cur, times=1).execute(sql_query)
        verify(self.read_data.cur, times=1).fetchall()

    def test_15_check_get_last_quotes(self):
        num = 15

        sql_query = f"""SELECT ticker,
                                ISIN,
                                sources.title,
                                datetime(time_stamp, 'unixepoch'),
                                timespans.title,
                                "Open",
                                High,
                                Low,
                                "Close",
                                AdjClose,
                                Volume,
                                Dividends,
                                Transactions,
                                VWAP
                            FROM quotes INNER JOIN symbols ON quotes.symbol_id = symbols.symbol_id
                            INNER JOIN sources ON quotes.source_id = sources.source_id
                            INNER JOIN timespans ON quotes.timespan_id = timespans.timespan_id
                            WHERE symbols.ticker = '{self.read_data.symbol}'
                            AND timespans.title = 'Day'
                            ORDER BY time_stamp DESC
                            LIMIT {num};"""

        when(self.read_data.cur).execute(sql_query).thenReturn()

        assert self.read_data.get_last_quotes(num) == self.results

        verify(self.read_data.cur, times=1).execute(sql_query)
        verify(self.read_data.cur, times=1).fetchall()
        
    def test_4_get_quotes_num(self):
        sql_query = "SELECT COUNT(*) FROM quotes;"

        when(self.read_data.cur).execute(sql_query).thenReturn()

        assert self.read_data.get_quotes_num() == 'r'

        verify(self.read_data.cur, times=1).execute(sql_query)
        verify(self.read_data.cur, times=1).fetchone()

    def test_5_get_symbol_quotes_num(self):
        sql_query = f"SELECT COUNT(*) FROM quotes WHERE symbol_id = (SELECT symbol_id FROM symbols where ticker = '{self.read_data.symbol}');"

        when(self.read_data.cur).execute(sql_query).thenReturn()

        assert self.read_data.get_symbol_quotes_num() == 'r'

        verify(self.read_data.cur, times=1).execute(sql_query)
        verify(self.read_data.cur, times=1).fetchone()

    def test_17_get_symbol_quotes_num_dt(self):
        sql_query = f"""SELECT COUNT(*) FROM quotes WHERE symbol_id =
                        (SELECT symbol_id FROM symbols where ticker = '{self.read_data.symbol}') AND
                        time_stamp >= {self.read_data.first_date_ts} AND time_stamp <= {self.read_data.last_date_ts};"""

        when(self.read_data.cur).execute(sql_query).thenReturn()

        assert self.read_data.get_symbol_quotes_num_dt() == 'r'

        verify(self.read_data.cur, times=1).execute(sql_query)
        verify(self.read_data.cur, times=1).fetchone()

    def test_14_get_max_datetime(self):
        sql_query = f"""SELECT MAX(datetime(time_stamp, 'unixepoch')) FROM quotes
                                    INNER JOIN symbols ON quotes.symbol_id = symbols.symbol_id
                                    WHERE symbols.ticker = '{self.read_data.symbol}'"""

        when(self.read_data.cur).execute(sql_query).thenReturn()

        assert self.read_data.get_max_datetime() == 'r'

        verify(self.read_data.cur, times=1).execute(sql_query)
        verify(self.read_data.cur, times=1).fetchone()

    # Write data methods test

    def test_6_check_database(self):
        sql_query1 = "SELECT name FROM sqlite_master WHERE type='table' AND name='quotes';"
        when(self.write_data.cur).execute(sql_query1).thenReturn()

        sql_query2 = """CREATE TABLE quotes (
                            quote_id INTEGER PRIMARY KEY,
                            symbol_id INTEGER NOT NULL,
                            source_id INTEGER NOT NULL,
                            time_stamp INTEGER NOT NULL,
                            timespan_id INTEGER NOT NULL,
                            Open REAL,
                            High REAL,
                            Low REAL,
                            Close REAL,
                            AdjClose REAL NOT NULL,
                            Volume INTEGER,
                            Dividends REAL,
                            Transactions INTEGER,
                            VWAP REAL,
                            CONSTRAINT fk_symols
                                FOREIGN KEY (symbol_id)
                                REFERENCES symbols(symbol_id),
                            CONSTRAINT fk_sources
                                FOREIGN KEY (source_id)
                                REFERENCES sources(source_id),
                            CONSTRAINT fk_timespan
                                FOREIGN KEY (timespan_id)
                                REFERENCES timespans(timespan_id),
                            UNIQUE(symbol_id, time_stamp, timespan_id)
                            );"""
        when(self.write_data.cur).execute(sql_query2).thenReturn()

        sql_query3 = "SELECT name FROM sqlite_master WHERE type='table' AND name='symbols';"
        when(self.write_data.cur).execute(sql_query3).thenReturn()

        sql_query4 = """CREATE TABLE symbols(
                                symbol_id INTEGER PRIMARY KEY,
                                ticker TEXT NOT NULL UNIQUE,
                                ISIN TEXT UNIQUE,
                                description TEXT
                                );"""
        when(self.write_data.cur).execute(sql_query4).thenReturn()

        sql_query5 = "SELECT name FROM sqlite_master WHERE type='table' AND name='sources';"
        when(self.write_data.cur).execute(sql_query5).thenReturn()

        sql_query6 = """CREATE TABLE sources(
                                source_id INTEGER PRIMARY KEY,
                                title TEXT NOT NULL UNIQUE,
                                description TEXT
                                );"""
        when(self.write_data.cur).execute(sql_query6).thenReturn()

        sql_query7 = "SELECT name FROM sqlite_master WHERE type='table' AND name='timespans';"
        when(self.write_data.cur).execute(sql_query7).thenReturn()

        sql_query8 = """CREATE TABLE timespans(
                                    timespan_id INTEGER PRIMARY KEY,
                                    title TEXT NOT NULL UNIQUE
                                );"""
        when(self.write_data.cur).execute(sql_query8).thenReturn()

        sql_query9 = "SELECT * FROM timespans;"
        when(self.write_data.cur).execute(sql_query9).thenReturn()

        # Prepare the query with all supported timespans
        ts = ""

        for timespan in Timespans:
            if timespan != Timespans.All:
                ts += f"('{timespan.value}'),"

        ts = ts[:len(ts) - 2]

        sql_query10 = f"""INSERT INTO timespans (title)
                                    VALUES {ts});"""
        when(self.write_data.cur).execute(sql_query10).thenReturn()

        when(self.write_data.cur).fetchall().thenReturn([])

        self.write_data.check_database()

        verify(self.write_data.cur, times=1).execute(sql_query1)
        verify(self.write_data.cur, times=1).execute(sql_query2)
        verify(self.write_data.cur, times=1).execute(sql_query3)
        verify(self.write_data.cur, times=1).execute(sql_query4)
        verify(self.write_data.cur, times=1).execute(sql_query5)
        verify(self.write_data.cur, times=1).execute(sql_query6)
        verify(self.write_data.cur, times=1).execute(sql_query7)
        verify(self.write_data.cur, times=1).execute(sql_query8)
        verify(self.write_data.cur, times=1).execute(sql_query9)
        verify(self.write_data.cur, times=1).execute(sql_query10)
        verify(self.write_data.cur, times=5).fetchall()
        verify(self.write_data.conn, times=1).commit()

    def test_7_check_commit(self):
        self.write_data.commit()

        verify(self.write_data.conn, times=1).commit()

    def test_8_check_add_symbol(self):
        sql_query = f"INSERT OR IGNORE INTO symbols (ticker) VALUES ('{self.write_data.symbol}');"
        when(self.write_data.cur).execute(sql_query).thenReturn()

        self.write_data.add_symbol()

        verify(self.write_data.cur, times=1).execute(sql_query)
        verify(self.write_data.conn, times=1).commit()

    def test_8_check_remove_symbol(self):
        sql_query1 = f"DELETE FROM quotes WHERE symbol_id = (SELECT symbol_id FROM symbols WHERE ticker = '{self.write_data.symbol}');"
        when(self.write_data.cur).execute(sql_query1).thenReturn()

        sql_query2 = f"DELETE FROM symbols WHERE symbol_id = (SELECT symbol_id FROM symbols WHERE ticker = '{self.write_data.symbol}');"
        when(self.write_data.cur).execute(sql_query2).thenReturn()

        self.write_data.remove_symbol()

        verify(self.write_data.cur, times=1).execute(sql_query1)
        verify(self.write_data.cur, times=1).execute(sql_query2)
        verify(self.write_data.conn, times=1).commit()

    def test_9_check_add_quotes(self):
        quote_dict = {
            "v": 1,
            "o": 2,
            "c": 3,
            "h": 4,
            "l": 5,
            "cl": 6,
            "n": 7,
            "vw": 8,
            "d": 9,
            "t": 999
        }

        quotes = [quote_dict]

        sql_query = f"""INSERT OR {self.write_data._update} INTO quotes (symbol_id, source_id, time_stamp, timespan_id, "Open", High, Low, Close, AdjClose, Volume, Transactions, VWAP, Dividends)
                                VALUES (
                                (SELECT symbol_id FROM symbols WHERE ticker = '{self.write_data.symbol}'),
                                (SELECT source_id FROM sources WHERE title = '{self.write_data.source_title}'),
                                ({quote_dict['t']}),
                                (SELECT timespan_id FROM timespans WHERE title = '{self.write_data.timespan}' COLLATE NOCASE),
                                ({quote_dict['o']}),
                                ({quote_dict['h']}),
                                ({quote_dict['l']}),
                                ({quote_dict['cl']}),
                                ({quote_dict['c']}),
                                ({quote_dict['v']}),
                                ({quote_dict['n']}),
                                ({quote_dict['vw']}),
                                ({quote_dict['d']})
                            );"""

        when(self.write_data.cur).execute(sql_query).thenReturn()

        self.write_data.add_quotes(quotes)

        verify(self.write_data.cur, times=1).execute(sql_query)

    def test_10_check_remove_quotes(self):
            sql_query = f"""DELETE FROM quotes WHERE symbol_id = (SELECT symbol_id FROM symbols WHERE ticker = '{self.write_data.symbol}')
                            AND time_stamp >= {self.write_data.first_date_ts} AND time_stamp <= {self.write_data.last_date_ts};"""

            when(self.write_data.cur).execute(sql_query).thenReturn()

            when(self.write_data).get_symbol_quotes_num().thenReturn(0)
            when(self.write_data).remove_symbol().thenReturn(True)

            self.write_data.remove_quotes()

            verify(self.write_data.cur, times=1).execute(sql_query)
            verify(self.write_data.conn, times=1).commit()

    def test_11_check_add_source(self):
        sql_query = f"INSERT INTO sources (title) VALUES ('{self.write_data.source_title}')"

        when(self.write_data.cur).execute(sql_query).thenReturn()

        self.write_data.add_source()

        verify(self.write_data.cur, times=1).execute(sql_query)
        verify(self.write_data.conn, times=1).commit()

    # TODO LOW add insert_quotes was removed, it is needed to be alteres
    def test_13_check_add_quotes(self):
        when(self.fetch_data).add_symbol().thenReturn()
        when(self.fetch_data).get_quotes_num().thenReturn(1)
        when(self.fetch_data).add_quotes(self.results).thenReturn()
        when(self.fetch_data).commit().thenReturn()

        before, after = self.fetch_data.add_quotes(self.results)

        assert before == 1
        assert after == 1

        verify(self.fetch_data, times=1).add_symbol()
        verify(self.fetch_data, times=2).get_quotes_num()
        verify(self.fetch_data, times=1).add_quotes(self.results)
        verify(self.fetch_data, times=1).commit()

    def test_18_check_fetch_if_none(self):
        nums = (0, 200)

        when(self.fetch_data).db_connect().thenReturn()
        when(self.fetch_data.cur).execute("SELECT COUNT(*) FROM quotes;").thenReturn()
        when(self.fetch_data.cur).execute("INSERT OR IGNORE INTO symbols (ticker) VALUES ('AAPL');").thenReturn()
        when(self.fetch_data).add_quotes(nums).thenReturn()
        when(self.fetch_data).get_symbol_quotes_num_dt().thenReturn(100)
        when(self.fetch_data).fetch_quotes().thenReturn(nums)
        when(self.fetch_data).add_quotes(nums).thenReturn(nums)
        when(self.fetch_data).get_quotes().thenReturn(self.results)
        when(self.fetch_data).db_close().thenReturn()

        rows, num = self.fetch_data.fetch_if_none(110)

        assert num == 200
        assert rows == self.results

        verify(self.fetch_data, times=1).db_connect()
        verify(self.fetch_data, times=1).get_symbol_quotes_num_dt()
        verify(self.fetch_data, times=1).fetch_quotes()
        verify(self.fetch_data, times=1).get_quotes()
        verify(self.fetch_data, times=1).db_close()
