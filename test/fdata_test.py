import unittest

from mockito import when, mock, verify, unstub

import sys
sys.path.append('../')

from data.fdata import ReadOnlyData, ReadWriteData, BaseFetcher, DB_VERSION, FdataError
from data.fvalues import Timespans, SecType, Currency, DbTypes

from sqlite3 import Cursor, Connection

from datetime import datetime
import pytz

class DataMocker():
    """
        Data abstraction mocking class.
    """
    def check_database_preparation(self, data_instance):
        """
            Mocking funciton for check_database()

            Args:
                data_instance(ReadOnlyData): db abstraction instance.

            Returns:
                list: list of mocked queries.
        """
        queries = []

        when(data_instance.cur).fetchall().thenReturn([], [])

        sql_query1 = "SELECT name FROM sqlite_master WHERE type='table' AND name='environment';"
        when(data_instance.cur).execute(sql_query1).thenReturn()
        queries.append(sql_query1)

        sql_query2 = """CREATE TABLE environment(
                                    version INTEGER NOT NULL UNIQUE
                                );"""
        when(data_instance.cur).execute(sql_query2).thenReturn()
        queries.append(sql_query2)

        sql_query3 = "SELECT * FROM environment;"
        when(data_instance.cur).execute(sql_query3).thenReturn()
        queries.append(sql_query3)

        sql_query4 = f"""INSERT INTO environment (version)
                                    VALUES ({DB_VERSION});"""
        when(data_instance.cur).execute(sql_query4).thenReturn()
        queries.append(sql_query4)

        sql_query5 = "SELECT name FROM sqlite_master WHERE type='table' AND name='symbols';"
        when(data_instance.cur).execute(sql_query5).thenReturn()
        queries.append(sql_query5)

        sql_query6 = """CREATE TABLE symbols(
                                symbol_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                ticker TEXT NOT NULL UNIQUE,
                                isin TEXT UNIQUE,
                                description TEXT
                                );"""
        when(data_instance.cur).execute(sql_query6).thenReturn()
        queries.append(sql_query6)

        sql_query6idx = "CREATE INDEX idx_ticker ON symbols(ticker);"
        when(data_instance.cur).execute(sql_query6idx).thenReturn()
        queries.append(sql_query6idx)

        sql_query7 = "SELECT name FROM sqlite_master WHERE type='table' AND name='sources';"
        when(data_instance.cur).execute(sql_query7).thenReturn()
        queries.append(sql_query7)

        sql_query8 = """CREATE TABLE sources(
                                source_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                title TEXT NOT NULL UNIQUE,
                                description TEXT
                                );"""
        when(data_instance.cur).execute(sql_query8).thenReturn()
        queries.append(sql_query8)

        sql_query8idx = "CREATE INDEX idx_source_title ON sources(title);"
        when(data_instance.cur).execute(sql_query8idx).thenReturn()
        queries.append(sql_query8idx)

        # Timespans

        sql_query9 = "SELECT name FROM sqlite_master WHERE type='table' AND name='timespans';"
        when(data_instance.cur).execute(sql_query9).thenReturn()
        queries.append(sql_query9)

        sql_query10 = """CREATE TABLE timespans(
                                    time_span_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    title TEXT NOT NULL UNIQUE
                                );"""
        when(data_instance.cur).execute(sql_query10).thenReturn()
        queries.append(sql_query10)


        sql_query10idx = "CREATE INDEX idx_timespan_title ON timespans(title);"
        when(data_instance.cur).execute(sql_query10idx).thenReturn()
        queries.append(sql_query10idx)

        sql_query11 = "SELECT * FROM timespans;"
        when(data_instance.cur).execute(sql_query11).thenReturn()
        queries.append(sql_query11)

        # Prepare the query with all supported timespans
        ts = ""

        for timespan in Timespans:
            if timespan != Timespans.All:
                ts += f"('{timespan.value}'),"

        ts = ts[:len(ts) - 2]

        sql_query12 = f"""INSERT OR IGNORE INTO timespans (title)
                                    VALUES {ts});"""
        when(data_instance.cur).execute(sql_query12).thenReturn()
        queries.append(sql_query12)

        # Sectypes

        sql_query13 = "SELECT name FROM sqlite_master WHERE type='table' AND name='sectypes';"
        when(data_instance.cur).execute(sql_query13).thenReturn()
        queries.append(sql_query13)

        sql_query14 = """CREATE TABLE sectypes(
                                    sec_type_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    title TEXT NOT NULL UNIQUE
                                );"""
        when(data_instance.cur).execute(sql_query14).thenReturn()
        queries.append(sql_query14)

        sql_query14idx = "CREATE INDEX idx_sectype_title ON sectypes(title);"
        when(data_instance.cur).execute(sql_query14idx).thenReturn()
        queries.append(sql_query14idx)

        sql_query15 = "SELECT * FROM sectypes;"
        when(data_instance.cur).execute(sql_query15).thenReturn()
        queries.append(sql_query15)

        # Prepare the query with all supported sectypes
        st = ""

        for sec_type in SecType:
            if sec_type != SecType.All:
                st += f"('{sec_type.value}'),"

        st = st[:len(st) - 2]

        sql_query16 = f"""INSERT OR IGNORE INTO sectypes (title)
                                    VALUES {st});"""
        when(data_instance.cur).execute(sql_query16).thenReturn()
        queries.append(sql_query16)

        # Currency

        sql_query17 = "SELECT name FROM sqlite_master WHERE type='table' AND name='currency';"
        when(data_instance.cur).execute(sql_query17).thenReturn()
        queries.append(sql_query17)

        sql_query18 = """CREATE TABLE currency(
                                    currency_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    title TEXT NOT NULL UNIQUE
                                );"""
        when(data_instance.cur).execute(sql_query18).thenReturn()
        queries.append(sql_query18)

        sql_query18idx = "CREATE INDEX idx_currency_title ON currency(title);"
        when(data_instance.cur).execute(sql_query18idx).thenReturn()
        queries.append(sql_query18idx)

        sql_query19 = "SELECT * FROM currency;"
        when(data_instance.cur).execute(sql_query19).thenReturn()
        queries.append(sql_query19)

        # Prepare the query with all supported currencies
        c = ""

        for currency in Currency:
            if currency != Currency.All:
                c += f"('{currency.value}'),"

        c = c[:len(c) - 2]

        sql_query20 = f"""INSERT OR IGNORE INTO currency (title)
                                    VALUES {c});"""
        when(data_instance.cur).execute(sql_query20).thenReturn()
        queries.append(sql_query20)

        sql_query21 = "SELECT name FROM sqlite_master WHERE type='table' AND name='quotes';"
        when(data_instance.cur).execute(sql_query21).thenReturn()
        queries.append(sql_query21)

        sql_query22 = """CREATE TABLE quotes (
                            quote_id INTEGER PRIMARY KEY AUTOINCREMENT,
                            symbol_id INTEGER NOT NULL,
                            source_id INTEGER NOT NULL,
                            time_stamp INTEGER NOT NULL,
                            time_span_id INTEGER NOT NULL,
                            sec_type_id INTEGER NOT NULL,
                            currency_id INTEGER NOT NULL,
                            opened REAL,
                            high REAL,
                            low REAL,
                            closed REAL NOT NULL,
                            volume INTEGER,
                            transactions INTEGER,
                                CONSTRAINT fk_timespans
                                    FOREIGN KEY (time_span_id)
                                    REFERENCES timespans(time_span_id)
                                    ON DELETE CASCADE
                                CONSTRAINT fk_sectypes
                                    FOREIGN KEY (sec_type_id)
                                    REFERENCES sectypes(sec_type_id)
                                    ON DELETE CASCADE
                                CONSTRAINT fk_source
                                    FOREIGN KEY (source_id)
                                    REFERENCES sources(source_id)
                                    ON DELETE CASCADE
                                CONSTRAINT fk_symbols
                                    FOREIGN KEY (symbol_id)
                                    REFERENCES symbols(symbol_id)
                                    ON DELETE CASCADE
                                CONSTRAINT fk_currency
                                    FOREIGN KEY (currency_id)
                                    REFERENCES currency(currency_id)
                                    ON DELETE CASCADE
                            UNIQUE(symbol_id, time_stamp, time_span_id)
                            );"""
        when(data_instance.cur).execute(sql_query22).thenReturn()
        queries.append(sql_query22)

        sql_query22idx = "CREATE INDEX idx_quotes ON quotes(symbol_id, time_stamp, time_span_id);"
        when(data_instance.cur).execute(sql_query22idx).thenReturn()
        queries.append(sql_query22idx)

        when(data_instance.conn).commit().thenReturn()

        return queries

# TODO MID check why data.sqlite is created by the test
class FetchData(BaseFetcher):
    # Implement abstract methods
    def fetch_quotes(self):
        pass

    def get_recent_data(self, to_cache=False):
        pass

class FdataTest(unittest.TestCase, DataMocker):
    def setUp(self):
        self.read_data = ReadOnlyData()

        self.read_data.conn = mock(Connection)
        self.read_data.cur = mock(Cursor)

        self.read_data.Error = BaseException
        self.read_data.symbol = "AAPL"
        self.read_data.first_date = 0
        self.read_data.last_date = 1
        self.read_data.timespan = Timespans.Day

        self.read_data._connected = True

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

        self.write_data._connected = True

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

        self.fetch_data._connected = True

    def tearDown(self):
        unstub()

    # Read data methods test

    def test_1_check_datetime_properties(self):
        data = ReadOnlyData()
        
        data.first_date = datetime(2022, 11, 28, 23, 55, 59).replace(tzinfo=pytz.utc)
        data.last_date = datetime(2022, 12, 28, 23, 55, 59).replace(tzinfo=pytz.utc)

        assert data.first_date_ts == 1669679759
        assert data.last_date_ts == 1672271759

        assert data.first_datetime_str == '2022-11-28 23:55:59'
        assert data.last_datetime_str == '2022-12-28 23:55:59'

        assert data.first_date_str == '2022-11-28'
        assert data.last_date_str == '2022-12-28'

        data.first_date_set_eod()
        data.last_date_set_eod()

        assert data.first_datetime_str == '2022-11-28 23:59:59'
        assert data.last_datetime_str == '2022-12-28 23:59:59'

    def test_2_check_is_connected(self):
        assert self.read_data.is_connected() == True

    def test_3_chech_if_connected(self):
        self.read_data.check_if_connected()

        self.read_data._connected = False

        self.assertRaises(FdataError, self.read_data.check_if_connected)

        self.read_data._connected = True

    def test_4_check_get_db_type(self):
        assert self.read_data.get_db_type() == DbTypes.SQLite

    def test_5_check_db_connect(self):
        when(self.read_data).check_database().thenReturn()
        when(self.read_data).check_source().thenReturn(False)
        when(self.read_data).add_source().thenReturn()

        self.read_data.db_connect()

        assert self.read_data._connected == True

        verify(self.read_data, times=1).check_database()
        verify(self.read_data, times=1).check_source()
        verify(self.read_data, times=1).add_source()

    def test_6_check_db_close(self):
        # TODO MID All expectations of mocks should be checked.
        when(self.read_data).check_database().thenReturn()
        when(self.read_data).check_source().thenReturn(False)
        when(self.read_data).add_source().thenReturn()

        self.read_data.db_connect()

        when(self.read_data.database).db_close().thenReturn()

        self.read_data.db_close()

        assert self.read_data.is_connected() == False

        verify(self.read_data.database, times=1).db_close()

    def test_7_check_database(self):
        queries = self.check_database_preparation(self.read_data)

        self.read_data.check_database()

        for query in queries:
            verify(self.read_data.cur, times=1).execute(query)

        verify(self.read_data.cur, times=11).fetchall()
        verify(self.read_data.conn, times=1).commit()

    def test_8_check_source(self):
        sql_query = f"SELECT title FROM sources WHERE title = '{self.read_data.source_title}';"

        when(self.read_data.cur).execute(sql_query).thenReturn()

        assert self.read_data.check_source() == len(self.results)

        verify(self.read_data.cur, times=1).execute(sql_query)
        verify(self.read_data.cur, times=1).fetchall()

    def test_9_check_add_source(self):
        sql_query = f"INSERT OR IGNORE INTO sources (title) VALUES ('{self.write_data.source_title}')"

        when(self.write_data.cur).execute(sql_query).thenReturn()

        self.write_data.add_source()

        verify(self.write_data.cur, times=1).execute(sql_query)
        verify(self.write_data.conn, times=1).commit()

    def test_10_check_get_all_symbols(self):
        sql_query = "SELECT ticker, isin, description FROM symbols;"

        when(self.read_data.cur).execute(sql_query).thenReturn()

        assert self.read_data.get_all_symbols() == self.results

        verify(self.read_data.cur, times=1).execute(sql_query)
        verify(self.read_data.cur, times=1).fetchall()

    def test_11_check_get_quotes(self):
        additional_columns = ""
        additional_queries = ""
        additional_joins = ""
        timespan_query = "AND timespans.title = 'Day'"
        sectype_query = ""
        currency_query = ""
        num_query = ""

        sql_query = f"""SELECT datetime(time_stamp, 'unixepoch') as time_stamp,
                                opened,
                                high,
                                low,
                                closed,
                                volume,
                                transactions
                                {additional_columns}
                                {additional_queries}
                            FROM quotes INNER JOIN symbols ON quotes.symbol_id = symbols.symbol_id
                            INNER JOIN timespans ON quotes.time_span_id = timespans.time_span_id
                            INNER JOIN sectypes ON quotes.sec_type_id = sectypes.sec_type_id
                            INNER JOIN currency ON quotes.currency_id = currency.currency_id
                            {additional_joins}
                            WHERE symbols.ticker = '{self.read_data.symbol}'
                            {timespan_query}
                            {sectype_query}
                            {currency_query}
                            AND time_stamp >= {self.read_data.first_date_ts}
                            AND time_stamp <= {self.read_data.last_date_ts}
                            ORDER BY time_stamp
                            {num_query};"""

        when(self.read_data.cur).execute(sql_query).thenReturn()

        assert self.read_data.get_quotes() == self.results

        verify(self.read_data.cur, times=1).execute(sql_query)
        verify(self.read_data.cur, times=1).fetchall()
        
    def test_12_get_quotes_num(self):
        sql_query = "SELECT COUNT(*) FROM quotes;"

        when(self.read_data.cur).execute(sql_query).thenReturn()

        assert self.read_data.get_quotes_num() == 'r'

        verify(self.read_data.cur, times=1).execute(sql_query)
        verify(self.read_data.cur, times=1).fetchone()

    def test_13_get_symbol_quotes_num(self):
        sql_query = f"SELECT COUNT(*) FROM quotes WHERE symbol_id = (SELECT symbol_id FROM symbols where ticker = '{self.read_data.symbol}');"

        when(self.read_data.cur).execute(sql_query).thenReturn()

        assert self.read_data.get_symbol_quotes_num() == 'r'

        verify(self.read_data.cur, times=1).execute(sql_query)
        verify(self.read_data.cur, times=1).fetchone()

    def test_14_get_symbol_quotes_num_dt(self):
        sql_query = f"""SELECT COUNT(*) FROM quotes WHERE symbol_id =
                        (SELECT symbol_id FROM symbols where ticker = '{self.read_data.symbol}') AND
                        time_stamp >= {self.read_data.first_date_ts} AND time_stamp <= {self.read_data.last_date_ts};"""

        when(self.read_data.cur).execute(sql_query).thenReturn()

        assert self.read_data.get_symbol_quotes_num_dt() == 'r'

        verify(self.read_data.cur, times=1).execute(sql_query)
        verify(self.read_data.cur, times=1).fetchone()

    def test_15_get_max_datetime(self):
        sql_query = f"""SELECT MAX(datetime(time_stamp, 'unixepoch')) FROM quotes
                                    INNER JOIN symbols ON quotes.symbol_id = symbols.symbol_id
                                    WHERE symbols.ticker = '{self.read_data.symbol}'"""

        when(self.read_data.cur).execute(sql_query).thenReturn()

        assert self.read_data.get_max_datetime() == 'r'

        verify(self.read_data.cur, times=1).execute(sql_query)
        verify(self.read_data.cur, times=1).fetchone()

    def test_16_check_commit(self):
        self.write_data.commit()

        verify(self.write_data.conn, times=1).commit()

    #######################
    # Read/Write operations
    #######################

    def test_17_check_update(self):
        self.write_data.update = True
        assert self.write_data._update == 'REPLACE'

        self.write_data.update = False
        assert self.write_data._update == 'IGNORE'

    def test_18_check_add_symbol(self):
        sql_query = f"INSERT OR IGNORE INTO symbols (ticker) VALUES ('{self.write_data.symbol}');"
        when(self.write_data.cur).execute(sql_query).thenReturn()

        self.write_data.add_symbol()

        verify(self.write_data.cur, times=1).execute(sql_query)
        verify(self.write_data.conn, times=1).commit()

    def test_19_check_remove_symbol(self):
        sql_query = f"DELETE FROM symbols WHERE symbol_id = (SELECT symbol_id FROM symbols WHERE ticker = '{self.write_data.symbol}');"
        when(self.write_data.cur).execute(sql_query).thenReturn()

        self.write_data.remove_symbol()

        verify(self.write_data.cur, times=1).execute(sql_query)
        verify(self.write_data.conn, times=1).commit()

    def test_20_add_base_quote_data(self):
        quote_dict = {
            'volume': 1,
            'open': 2,
            'adj_close': 3,
            'high': 4,
            'low': 5,
            'raw_close': 6,
            'transactions': 7,
            'ts': 8,
            'sectype': self.write_data.sectype.value,
            'currency': self.write_data.currency.value
        }

        sql_query = f"""INSERT OR {self.write_data._update} INTO quotes (symbol_id,
                                                                    source_id,
                                                                    time_stamp,
                                                                    time_span_id,
                                                                    sec_type_id,
                                                                    currency_id,
                                                                    opened,
                                                                    high,
                                                                    low,
                                                                    closed,
                                                                    volume,
                                                                    transactions)
                            VALUES (
                            (SELECT symbol_id FROM symbols WHERE ticker = '{self.write_data.symbol}'),
                            (SELECT source_id FROM sources WHERE title = '{self.write_data.source_title}'),
                            ({quote_dict['ts']}),
                            (SELECT time_span_id FROM timespans WHERE title = '{self.write_data.timespan}' COLLATE NOCASE),
                            (SELECT sec_type_id FROM sectypes WHERE title = '{quote_dict['sectype']}' COLLATE NOCASE),
                            (SELECT currency_id FROM currency WHERE title = '{quote_dict['currency']}' COLLATE NOCASE),
                            ({quote_dict['open']}),
                            ({quote_dict['high']}),
                            ({quote_dict['low']}),
                            ({quote_dict['adj_close']}),
                            ({quote_dict['volume']}),
                            ({quote_dict['transactions']})
                        );"""

        when(self.write_data.cur).execute(sql_query).thenReturn()

        self.write_data.cur.lastrowid = 10

        lastrowid = self.write_data._add_base_quote_data(quote_dict)

        assert lastrowid == self.write_data.cur.lastrowid

        verify(self.write_data.cur, times=1).execute(sql_query)

    def test_21_check_add_quotes(self):
        quote_dict = {
            'volume': 1,
            'open': 2,
            'adj_close': 3,
            'high': 4,
            'low': 5,
            'transactions': 6,
            'ts': 7,
            'sectype': self.write_data.sectype.value,
            'currency': self.write_data.currency.value
        }

        quotes = [quote_dict]

        when(self.write_data).get_symbol_quotes_num().thenReturn(1)
        when(self.write_data).get_quotes_num().thenReturn(1)
        when(self.write_data)._add_base_quote_data(quote_dict).thenReturn(1)
        when(self.write_data).commit().thenReturn()

        before, after = self.write_data.add_quotes(quotes)

        verify(self.write_data, times=1).get_symbol_quotes_num()
        verify(self.write_data, times=2).get_quotes_num()
        verify(self.write_data, times=1)._add_base_quote_data(quote_dict)
        verify(self.write_data, times=1).commit()

        assert before == 1
        assert after == 1

    def test_22_check_remove_quotes(self):
            sql_query = f"""DELETE FROM quotes WHERE symbol_id = (SELECT symbol_id FROM symbols WHERE ticker = '{self.write_data.symbol}')
                            AND time_stamp >= {self.write_data.first_date_ts} AND time_stamp <= {self.write_data.last_date_ts};"""

            when(self.write_data.cur).execute(sql_query).thenReturn()

            when(self.write_data).get_symbol_quotes_num().thenReturn(0)
            when(self.write_data).remove_symbol().thenReturn(True)

            self.write_data.remove_quotes()

            verify(self.write_data.cur, times=1).execute(sql_query)
            verify(self.write_data.conn, times=1).commit()

    def test_23_check_fetch_if_none(self):
        nums = (0, 200)

        when(self.fetch_data.cur).execute("SELECT COUNT(*) FROM quotes;").thenReturn()
        when(self.fetch_data.cur).execute("INSERT OR IGNORE INTO symbols (ticker) VALUES ('AAPL');").thenReturn()
        when(self.fetch_data).add_quotes(nums).thenReturn()
        when(self.fetch_data).get_symbol_quotes_num_dt().thenReturn(100)
        when(self.fetch_data).fetch_quotes().thenReturn(nums)
        when(self.fetch_data).add_quotes(nums).thenReturn(nums)
        when(self.fetch_data).get_quotes().thenReturn(self.results)

        rows, num = self.fetch_data.fetch_if_none(110)

        assert num == 200
        assert rows == self.results

        verify(self.fetch_data, times=1).get_symbol_quotes_num_dt()
        verify(self.fetch_data, times=1).fetch_quotes()
        verify(self.fetch_data, times=1).get_quotes()
