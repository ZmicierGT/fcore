"""Data abstraction module.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
import abc

from time import sleep, perf_counter

import http.client
import urllib.error
import requests

from data import fdatabase

from data.fvalues import Timespans, SecType, Currency, def_first_date, def_last_date, DbTypes, Timezones
from data.futils import get_dt, get_labelled_ndarray, logger

import settings

import json

from datetime import datetime, timedelta
from dateutil import tz
import calendar

# TODO MID Use sql-formatter on SQL code

# Current database compatibility version
DB_VERSION = 22

# TODO LOW Consider checking of sqlite version as well

class Subquery():
    """
        Class which represents additional subqueries for optional data (fundamentals, global economic, customer data and so on).
    """
    def __init__(self, table, column, condition='', title=None, fill=True):
        """
            Initializes the instance of Subquery class.

            Args:
                table(str): table for subquery.
                column(str): column to obtain.
                condition(str): additional SQL condition for the subquery.
                title(str): optional title for the output column (the same as column name by default)
                fill(bool): Indicates if all rows should have the value. False if only a row with the most
                            suitable data (according to time stamp) should have it.
        """
        self.table = table
        self.column = column
        self.condition = condition
        self.fill = fill

        # Use the default column name as the title if the title is not specified
        if title is None:
            self.title = column
        else:
            self.title = title

    def generate(self):
        """
            Generates the subquery based on the provided data.

            Returns:
                str: SQL expression for the subquery
        """
        ts_query = ''

        if self.fill is False:
            ts_query = """ AND report_tbl.time_stamp >
                           (SELECT time_stamp FROM quotes qqq WHERE qqq.quote_id > quotes.quote_id ORDER BY qqq.quote_id ASC LIMIT 1)"""

        subquery = f"""(SELECT {self.column}
                            FROM {self.table} report_tbl
                            WHERE report_tbl.time_stamp <= quotes.time_stamp{ts_query}
                            AND symbol_id = quotes.symbol_id
                            {self.condition}
                            ORDER BY report_tbl.time_stamp DESC LIMIT 1) AS {self.title}\n"""

        return subquery

class FdataError(Exception):
    """
        Base data exception class.
    """

class ReadOnlyData():
    """
        Base class for SQL 'read only' data operations and database integrity check.
    """
    def __init__(self,
                 symbol="",
                 first_date=def_first_date,
                 last_date=def_last_date,
                 timespan=Timespans.Day,
                 verbosity=False
                ):
        """
            Initialize base database read only/integrity class.

            Args:
                symbol(str): the symbol to use.
                first_date(datetime, str, int): the first date for queries.
                last_date(datetime, str, int): the last date for queries.
                timespan(Timespans): timespan to use in queries.
                verbosity(bool): indicates if additional outputs are needed (logging and so on).
        """
        # Setting the default values
        self.symbol = symbol

        # Underlying variables for getters/setter
        self._first_date = None
        self._last_date = None

        # Getter/setter will be invoked
        self.first_date = first_date
        self.last_date = last_date

        if self.first_date > self.last_date:
            raise FdataError(f"First date can't be bigger than the last date: {self.first_date} > {self.last_date}")

        self.timespan = timespan

        # Source title should be overridden in derived classes for particular data sources
        self.source_title = ''

        # Default setting for the base data source
        self.db_type = settings.Quotes.db_type
        self.db_name = settings.Quotes.db_name

        self.database = None
        self.conn = None
        self.cur = None

        # Type of exception for db queries
        self.Error = None

        # Flag which indicates if the database is connected
        self._connected = False

        self._verbosity = verbosity

        self._sec_info_supported = False  # Indicates if security info is supported
        self._time_zone = None  # Cached time zone to avoid too many db queries
        self._sec_type = None  # Cached security type to avoid too many db queries
        self._currency = None  # Cached security type to avoid too many db queries

    ########################################################
    # Get/set datetimes (depending on the input value type).
    ########################################################
    @property
    def first_date(self):
        """
            Get the first datetime.

            Returns:
                datetime: the first datetime.
        """
        return self._first_date

    @first_date.setter
    def first_date(self, value):
        """
            Set the first datetime.

            value(int, str, datetime): datetime representation to set.

            Raises:
                ValueError, OSError: incorrect datetime representation.
        """
        self._first_date = get_dt(value, tz.UTC)

    @property
    def last_date(self):
        """
            Get the last datetime.

            Returns:
                datetime: the last datetime.
        """
        return self._last_date

    @last_date.setter
    def last_date(self, value):
        """
            Set the last datetime.

            value(int, str, datetime): datetime representation to set.

            Raises:
                ValueError, OSError: incorrect datetime representation.
        """
        self._last_date = get_dt(value, tz.UTC)

    @property
    def first_date_ts(self):
        """
            Get the first datetime's timestamp.

            Returns:
                int: the first datetime's timestamp in queries.
        """
        return calendar.timegm(self.first_date.utctimetuple())

    @property
    def last_date_ts(self):
        """
            Get the last datetime's timestamp.

            Returns:
                int: the last datetime's timestamp.
        """
        return calendar.timegm(self.last_date.utctimetuple())

    @property
    def first_datetime_str(self):
        """
            Get the first datetime's string representation.

            Returns:
                datetime: the first datetime's string representation.
        """
        return self.first_date.strftime('%Y-%m-%d %H:%M:%S')

    @property
    def last_datetime_str(self):
        """
            Get the last datetime's string representation.

            Returns:
                datetime: the last datetime's string representation.
        """
        return self.last_date.strftime('%Y-%m-%d %H:%M:%S')

    @property
    def first_date_str(self):
        """
            Get the first datetime's string representation.

            Returns:
                datetime: the first datetime's string representation.
        """
        return self.first_date.strftime('%Y-%m-%d')

    @property
    def last_date_str(self):
        """
            Get the last datetime's string representation.

            Returns:
                datetime: the last datetime's string representation.
        """
        return self.last_date.strftime('%Y-%m-%d')

    def set_eod_time(self, dt):
        """
            Set the time to 23:59:59 which is used in EOD quotes.

            Args:
                dt(datetime, int, str): The initial datetime

            Returns:
                datetime: the adjustd datetime.
        """
        dt = get_dt(dt, tz.UTC)
        return dt.replace(hour=23, minute=59, second=59, tzinfo=tz.UTC)

    # TODO LOW It is not used now. Is there a sence to keep it?
    def first_date_set_eod(self):
        """
            Set the first date's h/m/s/ to EOD (23:59:59)
        """
        self._first_date = self.set_eod_time(self._first_date)

    def last_date_set_eod(self):
        """
            Set the last date's h/m/s/ to EOD (23:59:59)
        """
        self._last_date = self.set_eod_time(self._last_date.replace)

    ##############################################
    # End of datetime handling methods/properties.
    ##############################################

    def log(self, message):
        """
            Display a logging message depending on verbotisy flag.

            Args:
                message(str): the message to display.
        """
        logger(self._verbosity, message)

    def get_db_type(self):
        """
            Get used database type.

            Returns:
                DbTypes: database type.
        """
        return self.db_type

    def is_connected(self):
        """Returns True/False if db is connected."""
        return self._connected

    def check_if_connected(self):
        """
            Raise an exception if db is not connected.
        """
        if self.is_connected() is False:
            raise FdataError("The database is not connected. Invoke db_connect() at first.")

    def db_connect(self):
        """
            Connect to the databse.
        """
        if self.db_type == DbTypes.SQLite:
            self.database = fdatabase.SQLiteConn(self)
            self.database.db_connect()
            self._connected = True

            # Check the database integrity
            self.check_database()

            if self.check_source() == False:
                self.add_source()

    def db_close(self):
        """
            Close the database connection.
        """
        self.check_if_connected()

        self.database.db_close()
        self._connected = False

    def check_database(self):
        """
            Database create/integrity check method.
            Checks if the database exists. Otherwise, creates it. Checks if the database has required tables.

            Raises:
                FdataError: sql error happened.
        """
        self.check_if_connected()

        # Check if we need to create table 'environment'
        try:
            check_environment = "SELECT name FROM sqlite_master WHERE type='table' AND name='environment';"

            self.cur.execute(check_environment)
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table 'environment': {e}\n{check_environment}") from e

        if len(rows) == 0:
            create_environment = """CREATE TABLE environment(
                                    version INTEGER NOT NULL UNIQUE
                                );"""

            try:
                self.cur.execute(create_environment)
            except self.Error as e:
                raise FdataError(f"Can't execute a query on a table 'environment': {e}\n{create_environment}") from e

        # Check if environment table is empty
        try:
            all_environment = "SELECT * FROM environment;"

            self.cur.execute(all_environment)
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table 'environment': {e}\n{all_environment}") from e

        # Check if environment table has data
        if len(rows) > 1:  # This table should have one row only
            raise FdataError(f"The environment table is broken. Please, delete the database file {self.db_name} or change db patch in settings.py")
        elif len(rows) == 0:
            # Insert the environment data to the table
            insert_environment = f"""INSERT INTO environment (version)
                                    VALUES ({DB_VERSION});"""

            try:
                self.cur.execute(insert_environment)
            except self.Error as e:
                raise FdataError(f"Can't execute a query on a table 'environment': {e}\n{insert_environment}") from e
        else:  # One row present in the table so it is expected
            environment_query = "SELECT version FROM environment;"

            try:
                self.cur.execute(environment_query)
            except self.Error as e:
                raise FdataError(f"Can't execute a query on a table 'environment': {e}\n{environment_query}") from e

            version = self.cur.fetchone()[0]

            if version != DB_VERSION:
                raise FdataError(f"DB Version is unexpected. Please, delete the database file {self.db_name} or change db patch in settings.py")

        # Check if we need to create table 'currency'
        try:
            check_currency = "SELECT name FROM sqlite_master WHERE type='table' AND name='currency';"

            self.cur.execute(check_currency)
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table 'currency': {e}\n{check_currency}") from e

        if len(rows) == 0:
            create_currency = """CREATE TABLE currency(
                                    currency_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    title TEXT NOT NULL UNIQUE
                                );"""

            try:
                self.cur.execute(create_currency)
            except self.Error as e:
                raise FdataError(f"Can't execute a query on a table 'currency': {e}\n{create_currency}") from e

            # Create index for sectype title
            create_currency_title_idx = "CREATE INDEX idx_currency_title ON currency(title);"

            try:
                self.cur.execute(create_currency_title_idx)
            except self.Error as e:
                raise FdataError(f"Can't create index for currency(title): {e}") from e

        # Check if currency table is empty
        try:
            all_currency = "SELECT * FROM currency;"
            self.cur.execute(all_currency)
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table 'currency': {e}\n{all_currency}") from e

        # Check if currency table has data
        if len(rows) < len(Currency) - 1:
            # Prepare the query with all supported currencies
            currencies = ""

            for currency in Currency:
                if currency != Currency.All:
                    currencies += f"('{currency.value}'),"

            currencies = currencies[:len(currencies) - 2]

            insert_currency = f"""INSERT OR IGNORE INTO currency (title)
                                    VALUES {currencies});"""

            try:
                self.cur.execute(insert_currency)
            except self.Error as e:
                raise FdataError(f"Can't execute a query on a table 'currency': {e}\n{insert_currency}") from e

        # Check if we need to create table 'sectypes'
        try:
            check_sectypes = "SELECT name FROM sqlite_master WHERE type='table' AND name='sectypes';"

            self.cur.execute(check_sectypes)
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table 'sectypes': {e}\n{check_sectypes}") from e

        if len(rows) == 0:
            create_sectypes = """CREATE TABLE sectypes(
                                    sec_type_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    title TEXT NOT NULL UNIQUE
                                );"""

            try:
                self.cur.execute(create_sectypes)
            except self.Error as e:
                raise FdataError(f"Can't execute a query on a table 'sectypes': {e}\n{create_sectypes}") from e

            # Create index for sectype title
            create_sectype_title_idx = "CREATE INDEX idx_sectype_title ON sectypes(title);"

            try:
                self.cur.execute(create_sectype_title_idx)
            except self.Error as e:
                raise FdataError(f"Can't create index for sectypes(title): {e}") from e

        # Check if sectypes table is empty
        try:
            all_sectypes = "SELECT * FROM sectypes;"

            self.cur.execute(all_sectypes)
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table 'sectypes': {e}\n{all_sectypes}") from e

        # Check if sectypes table has data
        if len(rows) < len(SecType) - 1:
            # Prepare the query with all supported sectypes
            sec_types = ""

            for sectype in SecType:
                if sectype != SecType.All:
                    sec_types += f"('{sectype.value}'),"

            sec_types = sec_types[:len(sec_types) - 2]

            insert_sectypes = f"""INSERT OR IGNORE INTO sectypes (title)
                                    VALUES {sec_types});"""

            try:
                self.cur.execute(insert_sectypes)
            except self.Error as e:
                raise FdataError(f"Can't execute a query on a table 'sectypes': {e}\n{insert_sectypes}") from e

        # Check if we need to create table 'symbols'
        try:
            check_symbols = "SELECT name FROM sqlite_master WHERE type='table' AND name='symbols';"

            self.cur.execute(check_symbols)
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table 'symbols': {e}\n{check_symbols}") from e

        if len(rows) == 0:
            create_symbols = """CREATE TABLE symbols(
                                symbol_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                ticker TEXT NOT NULL UNIQUE,
                                isin TEXT UNIQUE,
                                description TEXT,
                                UNIQUE(symbol_id)
                                );"""

            try:
                self.cur.execute(create_symbols)
            except self.Error as e:
                raise FdataError(f"Can't execute a query on a table 'symbols': {e}\n{create_symbols}") from e

            # Create index for ticker
            create_ticker_idx = "CREATE INDEX idx_ticker ON symbols(ticker);"

            try:
                self.cur.execute(create_ticker_idx)
            except self.Error as e:
                raise FdataError(f"Can't create index for symbols(ticker): {e}") from e

        # Check if we need to create table 'sources'
        try:
            check_sources = "SELECT name FROM sqlite_master WHERE type='table' AND name='sources';"

            self.cur.execute(check_sources)
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table 'sources': {e}\n{check_sources}") from e

        if len(rows) == 0:
            create_sources = """CREATE TABLE sources(
                                source_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                title TEXT NOT NULL UNIQUE,
                                description TEXT
                                );"""

            try:
                self.cur.execute(create_sources)
            except self.Error as e:
                raise FdataError(f"Can't execute a query on a table 'sources': {e}\n{create_sources}") from e

            # Create index for source title
            create_source_title_idx = "CREATE INDEX idx_source_title ON sources(title);"

            try:
                self.cur.execute(create_source_title_idx)
            except self.Error as e:
                raise FdataError(f"Can't create index for sources(title): {e}") from e

        # Check if we need to create table 'timespans'
        try:
            check_timespans = "SELECT name FROM sqlite_master WHERE type='table' AND name='timespans';"

            self.cur.execute(check_timespans)
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table 'timespans': {e}\n{check_timespans}") from e

        if len(rows) == 0:
            create_timespans = """CREATE TABLE timespans(
                                    time_span_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    title TEXT NOT NULL UNIQUE
                                );"""

            try:
                self.cur.execute(create_timespans)
            except self.Error as e:
                raise FdataError(f"Can't execute a query on a table 'timespans': {e}\n{create_timespans}") from e

            # Create index for timespan title
            create_timespan_title_idx = "CREATE INDEX idx_timespan_title ON timespans(title);"

            try:
                self.cur.execute(create_timespan_title_idx)
            except self.Error as e:
                raise FdataError(f"Can't create index for timespans(title): {e}") from e

        # Check if timespans table is empty
        try:
            all_timespans = "SELECT * FROM timespans;"

            self.cur.execute(all_timespans)
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table 'timespans': {e}\n{all_timespans}") from e

        # Check if timespans table has data
        if len(rows) < len(Timespans) - 1:
            # Prepare the query with all supported timespans
            timespans = ""

            for timespan in Timespans:
                if timespan != Timespans.All:
                    timespans += f"('{timespan.value}'),"

            timespans = timespans[:len(timespans) - 2]

            insert_timespans = f"""INSERT OR IGNORE INTO timespans (title)
                                    VALUES {timespans});"""

            try:
                self.cur.execute(insert_timespans)
            except self.Error as e:
                raise FdataError(f"Can't execute a query on a table 'timespans': {e}\n{insert_timespans}") from e

        # Check if we need to create table 'quote_intervals'
        try:
            check_quote_intervals = "SELECT name FROM sqlite_master WHERE type='table' AND name='quote_intervals';"

            self.cur.execute(check_quote_intervals)
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table 'quote_intervals': {e}\n{check_quote_intervals}") from e

        if len(rows) == 0:
            create_quote_intervals = """CREATE TABLE quote_intervals (
                                            quote_interval_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                            symbol_id INTEGER NOT NULL,
                                            source_id INTEGER NOT NULL,
                                            time_span_id INTEGER NOT NULL,
                                            min_request_ts INTEGER NOT NULL,
                                            max_request_ts INTEGER NOT NULL,
                                                CONSTRAINT fk_timespans
                                                    FOREIGN KEY (time_span_id)
                                                    REFERENCES timespans(time_span_id)
                                                    ON DELETE CASCADE
                                                CONSTRAINT fk_source
                                                    FOREIGN KEY (source_id)
                                                    REFERENCES sources(source_id)
                                                    ON DELETE CASCADE
                                                CONSTRAINT fk_symbols
                                                    FOREIGN KEY (symbol_id)
                                                    REFERENCES symbols(symbol_id)
                                                    ON DELETE CASCADE
                                            UNIQUE(symbol_id, source_id, time_span_id)
                                            );"""

            try:
                self.cur.execute(create_quote_intervals)
            except self.Error as e:
                raise FdataError(f"Can't create table quote_intervals: {e}") from e

            # Create indexes for quote_intervals
            create_quote_intervals_idx = "CREATE INDEX idx_quote_intervals ON quote_intervals(symbol_id, source_id, time_span_id);"

            try:
                self.cur.execute(create_quote_intervals_idx)
            except self.Error as e:
                raise FdataError(f"Can't create indexes for quote_intervals table: {e}") from e

        # TODO Mid need to think of a better way how to combine data from various sources
        # Check if we need to create table 'quotes'
        try:
            check_quotes = "SELECT name FROM sqlite_master WHERE type='table' AND name='quotes';"

            self.cur.execute(check_quotes)
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table 'quotes': {e}\n{check_quotes}") from e

        if len(rows) == 0:
            create_quotes = """CREATE TABLE quotes (
                            quote_id INTEGER PRIMARY KEY AUTOINCREMENT,
                            symbol_id INTEGER NOT NULL,
                            source_id INTEGER NOT NULL,
                            time_stamp INTEGER NOT NULL,
                            time_span_id INTEGER NOT NULL,
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
                                CONSTRAINT fk_source
                                    FOREIGN KEY (source_id)
                                    REFERENCES sources(source_id)
                                    ON DELETE CASCADE
                                CONSTRAINT fk_symbols
                                    FOREIGN KEY (symbol_id)
                                    REFERENCES symbols(symbol_id)
                                    ON DELETE CASCADE
                            UNIQUE(symbol_id, time_stamp, time_span_id, source_id)
                            );"""

            try:
                self.cur.execute(create_quotes)
            except self.Error as e:
                raise FdataError(f"Can't create table quotes: {e}") from e

            # Create indexes for quotes
            create_quotes_idx = "CREATE INDEX idx_quotes ON quotes(symbol_id, time_stamp, time_span_id);"

            try:
                self.cur.execute(create_quotes_idx)
            except self.Error as e:
                raise FdataError(f"Can't create indexes for quotes table: {e}") from e

        # Check if we need to create table 'sec_info'
        try:
            check_sec_info = "SELECT name FROM sqlite_master WHERE type='table' AND name='sec_info';"

            self.cur.execute(check_sec_info)
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table 'sec_info': {e}\n{check_sec_info}") from e

        if len(rows) == 0:

            create_sec_info = """CREATE TABLE sec_info (
                                                sec_info_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                symbol_id INTEGER NOT NULL,
                                                source_id INTEGER NOT NULL,
                                                time_zone TEXT NOT NULL,
                                                sec_type_id INTEGER NOT NULL,
                                                currency_id INTEGER NOT NULL,
                                                modified INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                                                UNIQUE(symbol_id, sec_info_id)
                                                    CONSTRAINT fk_source
                                                        FOREIGN KEY (source_id)
                                                        REFERENCES sources(source_id)
                                                        ON DELETE CASCADE
                                                    CONSTRAINT fk_symbols
                                                        FOREIGN KEY (symbol_id)
                                                        REFERENCES symbols(symbol_id)
                                                        ON DELETE CASCADE
                                                    CONSTRAINT fk_sectypes
                                                        FOREIGN KEY (sec_type_id)
                                                        REFERENCES sectypes(sec_type_id)
                                                        ON DELETE CASCADE
                                                    CONSTRAINT fk_currency
                                                        FOREIGN KEY (currency_id)
                                                        REFERENCES currency(currency_id)
                                                        ON DELETE CASCADE
                                                UNIQUE(symbol_id, source_id)
                                            );"""

            try:
                self.cur.execute(create_sec_info)
            except self.Error as e:
                raise FdataError(f"Can't create table sec_info: {e}") from e

            # Create indexes for sec_info
            create_sec_info_idx_symbol = "CREATE INDEX idx_sec_info_symbol ON sec_info(symbol_id);"
            create_sec_info_idx_sectype = "CREATE INDEX idx_sec_info_sectype ON sec_info(sec_type_id);"
            create_sec_info_idx_currency = "CREATE INDEX idx_sec_info_currency ON sec_info(currency_id);"

            try:
                self.cur.execute(create_sec_info_idx_symbol)
                self.cur.execute(create_sec_info_idx_sectype)
                self.cur.execute(create_sec_info_idx_currency)
            except self.Error as e:
                raise FdataError(f"Can't create indexes for sec_info table: {e}") from e

            # Create trigger to last modified time on sec_info
            create_fmp_cap_trigger = """CREATE TRIGGER update_sec_info
                                                BEFORE UPDATE
                                                    ON sec_info
                                        BEGIN
                                            UPDATE sec_info
                                            SET modified = strftime('%s', 'now')
                                            WHERE sec_info_id = old.sec_info_id;
                                        END;"""

            try:
                self.cur.execute(create_fmp_cap_trigger)
            except self.Error as e:
                raise FdataError(f"Can't create trigger for sec_info: {e}") from e

        self.conn.commit()

    def check_source(self):
        """
            Check if the current source exists in the table 'sources'

            Returns:
                int: the number of rows in 'sources' table.

            Raises:
                FdataError: sql error happened.
        """
        self.check_if_connected()

        try:
            source_exists = f"SELECT title FROM sources WHERE title = '{self.source_title}';"

            self.cur.execute(source_exists)
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table 'sources': {e}\n{source_exists}") from e

        # Check if sources table has the required row
        return len(rows)

    def add_source(self):
        """
            Add source to the database.

            Raises:
                FdataError: sql error happened.
        """
        self.check_if_connected()

        insert_source = f"INSERT OR IGNORE INTO sources (title) VALUES ('{self.source_title}')"

        try:
            self.cur.execute(insert_source)
            self.conn.commit()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table 'sources': {e}\n{insert_source}") from e

    ##################################
    # Read only methods to obtain data
    ##################################

    def get_all_symbols(self):
        """
            Get all symbols in the database.

            Returns:
                list: list with all the symbols.

            Raises:
                FdataError: sql error happened.
        """
        self.check_if_connected()

        try:
            get_all_symbols = "SELECT ticker, isin, description FROM symbols;"

            self.cur.execute(get_all_symbols)
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table 'symbols': {e}\n{get_all_symbols}") from e

        return rows

    def get_quotes(self, num=0, columns=None, joins=None, queries=None, ignore_last_date=False, ignore_source=False):
        """
            Get quotes for specified symbol, dates and timespan (if any). Additional columns from other tables
            linked by symbol_id may be requested (like fundamental data)

            Args:
                num(int): the number of rows to get. 0 gets all the quotes.
                columns(list): additional columns to query.
                joins(list): additional joins to get data from other tables.
                queries(list): additional queries from other tables (like funamental, global economic data).
                ignore_last_date(bool): indicates if last date should be ignored (all recent history is obtained)
                ignore_souce(bool): indicates if quotes should be obtained only from a particular source

            Returns:
                list: list with quotes data.

            Raises:
                FdataError: sql error happened.
        """
        self.check_if_connected()

        # Timespan subquery
        timespan_query = ""

        if self.timespan != Timespans.All:
            timespan_query = "AND timespans.title = '" + self.timespan.value + "'"

        # TODO LOW Think what to do with sectype and currency. Ignore it for now.
        # # Sectype subquery
        # sectype_query = ""

        # if self.get_sectype() != SecType.All:
        #     sectype_query = "AND sectypes.title = '" + self.get_sectype() + "'"

        # # Currency subquery
        # currency_query = ""

        # if self.get_currency() != Currency.All:
        #     currency_query = "AND currency.title = '" + self.get_currency() + "'"

        # Quotes number subquery
        num_query = ""

        if num > 0:
            num_query = f"LIMIT {num}"

        additional_columns = ""

        if isinstance(columns, list):
            for column in columns:
                additional_columns += ", " + column

        additional_queries = ""

        if isinstance(queries, list):
            # Generate the subqueries for additional data
            for query in queries:
                additional_queries += f", {query.generate()}"

        additional_joins = ""

        if isinstance(joins, list):
            # Generate the string with additional joins
            for join in joins:
                additional_joins += join + '\n'

        last_date_ts = calendar.timegm(self.set_eod_time(self.last_date).utctimetuple())

        if ignore_last_date:
            last_date_ts = def_last_date

        source_query = ''

        if ignore_source is False:
            source_query = f"AND source_id = (SELECT source_id FROM sources WHERE title = '{self.source_title}')"

        # select_quotes = f"""SELECT time_stamp,
        #                         datetime(time_stamp, 'unixepoch') AS date_time,
        #                         opened,
        #                         high,
        #                         low,
        #                         closed,
        #                         volume,
        #                         transactions
        #                         {additional_columns}
        #                         {additional_queries}
        #                     FROM quotes INNER JOIN symbols ON quotes.symbol_id = symbols.symbol_id
        #                     INNER JOIN timespans ON quotes.time_span_id = timespans.time_span_id
        #                     INNER JOIN sectypes ON quotes.sec_type_id = sectypes.sec_type_id
        #                     INNER JOIN currency ON quotes.currency_id = currency.currency_id
        #                     {additional_joins}
        #                     WHERE symbols.ticker = '{self.symbol}'
        #                     {timespan_query}
        #                     {sectype_query}
        #                     {currency_query}
        #                     AND time_stamp >= {self.first_date_ts}
        #                     AND time_stamp <= {self.last_date_ts}
        #                     ORDER BY time_stamp
        #                     {num_query};"""

        select_quotes = f"""SELECT time_stamp,
                                datetime(time_stamp, 'unixepoch') AS date_time,
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
                            {additional_joins}
                            WHERE symbols.ticker = '{self.symbol}'
                            {timespan_query}
                            AND time_stamp >= {self.first_date_ts}
                            AND time_stamp <= {last_date_ts}
                            {source_query}
                            ORDER BY time_stamp
                            {num_query};"""

        try:
            self.cur.execute(select_quotes)
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table 'quotes': {e}\n{select_quotes}") from e

        if len(rows) == 0:
            self.log("No data obtained.")
            return None

        return get_labelled_ndarray(rows)

    def get_quotes_num(self):
        """
            Get the number of quotes in the database.

            Returns:
                int: the total number of all quotes in the database.

            Raises:
                FdataError: sql error happened.
        """
        initially_connected = self.is_connected()

        if self.is_connected() is False:
            self.db_connect()

        quotes_num = "SELECT COUNT(*) FROM quotes;"

        try:
            self.cur.execute(quotes_num)
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table 'quotes': {e}\n{quotes_num}") from e

        result = self.cur.fetchone()[0]

        if result is None:
            result = 0

        if initially_connected is False:
            self.db_close()

        return result

    def _get_data_num(self, table):
        """Get the number additional data entries for the symbol.

            Args:
                table(string): the table with reports.

            Returns:
                int: the number of entries in the specified table.

            Raises:
                FdataError: sql error happened.
        """
        self.check_if_connected()

        get_num = f"""SELECT COUNT(*) FROM {table}
                        WHERE symbol_id = (SELECT symbol_id FROM symbols where ticker = '{self.symbol}');"""
        try:
            self.cur.execute(get_num)
        except self.Error as e:
            raise FdataError(f"Can't query table '{table}': {e}\n\nThe query is\n{get_num}") from e

        result = self.cur.fetchone()[0]

        if result is None:
            result = 0

        return result

    def get_max_value(self, table, column):
        """
            Get the maximum value from a table.

            Args:
                table(str): table to take the value
                column(str): columnt to take the value

            Returns:
                The maximum value from the column
        """
        self.check_if_connected()

        get_mod_ts = f"""SELECT {column} FROM {table}
                            WHERE symbol_id = (SELECT symbol_id FROM symbols where ticker = '{self.symbol}')
                            ORDER BY modified DESC LIMIT 1;"""

        try:
            self.cur.execute(get_mod_ts)
        except self.Error as e:
            raise FdataError(f"Can't query table '{table}': {e}\n\nThe query is\n{get_mod_ts}") from e

        result = self.cur.fetchone()

        if result is None:
            return result

        return result[0]

    def get_last_modified(self, table):
        """
            Get the last modification timestamp from a table.

            Args:
                table(str): table name

            Returns:
                int: last modification timestamp.
        """
        return self.get_max_value(table, 'modified')

    def get_last_timestamp(self, table):
        """
            Get the last timestamp from a table.

            Args:
                table(str): table name

            Returns:
                int: last timestamp.
        """
        return self.get_max_value(table, 'time_stamp')

    def get_total_symbol_quotes_num(self):
        """
            Get the number of quotes in the database per symbol.

            Returns:
                int: the number of quotes in the database per symbol.

            Raises:
                FdataError: sql error happened.
        """
        return self._get_data_num('quotes')

    def get_symbol_quotes_num(self, dt=True):
        """
            Get the number of quotes in the database per symbol for specified dates, time span and source.

            Args:
                dt(bool): check for the particular datetime.

            Returns:
                int: the number of quotes in the database per symbol for specified dates, time span and source.

            Raises:
                FdataError: sql error happened.
        """
        self.check_if_connected()

        dt_str = ''

        last_date_ts = calendar.timegm(self.set_eod_time(self.last_date).utctimetuple())

        if dt:
            dt_str = f"AND time_stamp >= {self.first_date_ts} AND time_stamp <= {last_date_ts}"

        num_query = f"""SELECT COUNT(*) FROM quotes
                            WHERE symbol_id = (SELECT symbol_id FROM symbols where ticker = '{self.symbol}')
                            {dt_str}
                            AND time_span_id = (SELECT time_span_id FROM timespans where title = '{self.timespan}')
                            AND source_id = (SELECT source_id FROM sources where title = '{self.source_title}')
                        ;"""

        try:
            self.cur.execute(num_query)
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table 'quotes': {e}\n{num_query}") from e

        result = self.cur.fetchone()[0]

        if result is None:
            result = 0

        return result

    def _get_ts(self, is_max=True, table='quotes', column='time_stamp'):
        """
            Get Min/Max timestamp for a particular symbol, source, timespan from the specified table.

            Args:
                is_max(bool): indicates if Min or Max timestamp should be obtained.
                table(str): table to request.
                column(str): column to request.

            Returns:
                int: timestamp of min/max timestamp.

            Raises:
                FdataError: sql error happened.
        """
        minmax = 'MIN'

        if is_max:
            minmax = 'MAX'

        self.check_if_connected()

        timestamp_query = f"""SELECT {minmax}({column}) FROM {table}
                                    INNER JOIN symbols ON {table}.symbol_id = symbols.symbol_id
                                    INNER JOIN sources on {table}.source_id = sources.source_id
                                    INNER JOIN timespans on {table}.time_span_id = timespans.time_span_id
                                    WHERE symbols.ticker = '{self.symbol}'
                                    AND sources.title = '{self.source_title}'
                                    AND timespans.title = '{self.timespan}';"""

        try:
            self.cur.execute(timestamp_query)
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table '{table}': {e}\n{timestamp_query}") from e

        return self.cur.fetchone()[0]

    def get_min_request_ts(self):
        """
            Get the earliest request timestamp to obtain quotes for a particular symbol,
            timespan, source.

            Return:
                int: the earliest request timestamp.
        """
        return self._get_ts(table='quote_intervals', column='min_request_ts')

    def get_max_request_ts(self):
        """
            Get the earliest request timestamp to obtain quotes for a particular symbol,
            timespan, source.

            Return:
                int: the earliest request timestamp.
        """
        return self._get_ts(table='quote_intervals', column='max_request_ts')

    def get_max_ts(self):
        """
            Get maximum timestamp for a particular symbol, source, timespan.

            Returns:
                int: timestamp of a maximum timestamp.
        """
        return self._get_ts(is_max=True)

    def get_min_ts(self):
        """
            Get minimum timestamp for a particular symbol, source, timespan.

            Returns:
                int: timestamp of a minimum timestamp.
        """
        return self._get_ts(is_max=False)

    def get_info(self):
        """
            Fetch (if needed) and return security info data.
        """
        if self._sec_info_supported is False:
            return {}

        initially_connected = self.is_connected()

        if self.is_connected() is False:
            self.db_connect()

        mod_ts = self.get_last_modified('sec_info')

        # Fetch data if no data present
        if mod_ts is None:
            self.add_info(self.fetch_info())

        # Just time zone is used from info for now
        info_query = f"""SELECT time_zone, s.title as sec_type, c.title as curr FROM sec_info si
                            INNER JOIN sectypes s ON si.sec_type_id = s.sec_type_id
                            INNER JOIN currency c ON si.currency_id = c.currency_id
                            WHERE symbol_id = (SELECT symbol_id FROM symbols WHERE ticker='{self.symbol}')"""

        try:
            self.cur.execute(info_query)
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table 'sec_info': {e}\n{info_query}") from e

        if initially_connected is False:
            self.db_close()

        row = rows[0]

        return {'time_zone': row['time_zone'], 'sec_type': row['sec_type'], 'currency': row['curr']}

    def get_timezone(self):
        """
            Get the time zone of the specified symbol.

            Returns:
                tz: time zone.
        """
        if self._sec_info_supported is False:
            self._time_zone = tz.gettz('America/New_York')  # Return ET by default. Supposed to be overridden.

        if self._time_zone is None:
            info = self.get_info()

            if info is not None and len(info.keys()) and 'time_zone' in info.keys():
                timezone = tz.gettz(info['time_zone'])

                if timezone is None:
                    self._time_zone = tz.gettz(Timezones[info['time_zone']])
                else:
                    self._time_zone = timezone
            else:
                self.log("Time zone data is not found. Returning ET.")
                self._time_zone = tz.gettz('America/New_York')

        return self._time_zone

    def get_sectype(self):
        """
            Get the security type of the specified symbol.

            Returns:
                (SecType): security typy.
        """
        if self._sec_info_supported is False:
            self._sec_type = SecType.Unknown  # Return Unknown by default.

        if self._sec_type is None:
            info = self.get_info()

            if info is not None and len(info.keys()) and 'sec_type' in info.keys():
                self._sec_type = info['sec_type']
            else:
                self._sec_type = SecType.Unknown

                self.log(f"Security type data is not found. Returning {self._sec_type.value}.")

        return self._sec_type

    # TODO LOW Note that Unknown will be returned each time as currencies are not supported yet.
    def get_currency(self):
        """
            Get the currency of the specified symbol.

            Returns:
                (Currency): security typy.
        """
        if self._sec_info_supported is False:
            self._currency = Currency.Unknown  # Return Unknown by default.

        if self._currency is None:
            info = self.get_info()

            if info is not None and len(info.keys()) and 'currency' in info.keys():
                self._currency = info['currency']
            else:
                self._currency = Currency.Unknown

                self.log(f"Currency data is not found. Returning {self._currency.value}.")

        return self._currency

    def is_intraday(self, timespan=None):
        """
            Checks if current timespan is intraday.

            Args:
                timespan(Timespan): timespan to override.

            Returns:
                bool: if current timespan is intraday.
        """
        if timespan is None:
            timespan = self.timespan

        return timespan != Timespans.Day

    def current_ts(self, adjusted=False, timespan=None):
        """
            Get the current UTC and time span adjusted timestamp.

            Args:
                adjusted(bool): indicates if the timestamp is adjusted for timespan.
                timespan(Timespan): timespan to override

            Returns:
                int: the current UTC and time span adjusted timestamp.
        """
        now = datetime.now(tz.UTC)

        if timespan is None:
            timespan = self.timespan

        if adjusted:
            if self.is_intraday(timespan) is False:
                now = self.set_eod_time(now)
            elif timespan == Timespans.Minute:
                now += timedelta(minutes=1)
            if timespan == Timespans.TwoMinutes:
                now += timedelta(minutes=2)
            elif timespan == Timespans.FiveMinutes:
                now += timedelta(minutes=5)
            elif timespan == Timespans.TenMinutes:
                now += timedelta(minutes=10)
            elif timespan == Timespans.FifteenMinutes:
                now += timedelta(minutes=15)
            elif timespan == Timespans.TwentyMinutes:
                now += timedelta(minutes=20)
            elif timespan == Timespans.ThirtyMinutes:
                now += timedelta(minutes=30)
            elif timespan == Timespans.Hour:
                now += timedelta(minutes=60)
            elif timespan == Timespans.NinetyMinutes:
                now += timedelta(minutes=90)
            elif timespan == Timespans.FourHour:
                now += timedelta(minutes=240)

        ts = calendar.timegm(now.utctimetuple())

        return ts

    def commit(self):
        """
            Commit the change to the database.

            Raises:
                FdataError: sql error happened.
        """
        self.check_if_connected()

        try:
            self.conn.commit()
        except self.Error as e:
            raise FdataError(f"Can't commit: {e}") from e

#############################
# Read/Write operations class
#############################

class ReadWriteData(ReadOnlyData):
    """
        Base class for read/write SQL operations.
    """
    def __init__(self, update=True, **kwargs):
        """
            Initialize read/write SQL abstraction class.

            Args:
                update(bool): indicates if existing quotes should be updated.
        """
        super().__init__(**kwargs)

        # Underlying variable for getter/setter
        self._update = None

        # Indicates if existed quotes should be updated
        self.update = update

    @property
    def update(self):
        """
            Getter for update.
        """
        if self._update == 'IGNORE':
            return False
        elif self._update == 'REPLACE':
            return True
        else:
            raise FdataError("Unknown update value.")

    @update.setter
    def update(self, value):
        """
            Setter fo update.
        """
        if value is False:
            self._update = 'IGNORE'
        else:
            self._update = 'REPLACE'

    def add_symbol(self):
        """
            Add new symbol to the database.

            Raises:
                FdataError: sql error happened.
        """
        self.check_if_connected()

        insert_symbol = f"""INSERT OR IGNORE INTO symbols (ticker) VALUES (
                                '{self.symbol}');"""

        try:
            self.cur.execute(insert_symbol)
            self.conn.commit()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table 'symbols': {e}\n{insert_symbol}") from e

    def remove_symbol(self):
        """
            Remove a symbol completely.

            All corresponding records in quotes table will be deleted because of foreign key linking (cascade deletion).

            Raises:
                FdataError: sql error happened.
        """
        self.check_if_connected()

        # Cascade delete will remove the corresponding entries in tables related to specific security data
        # like fundamentals for stock
        delete_symbol = f"DELETE FROM symbols WHERE symbol_id = (SELECT symbol_id FROM symbols WHERE ticker = '{self.symbol}');"

        try:
            self.cur.execute(delete_symbol)
            self.conn.commit()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table 'symbols': {e}\n{delete_symbol}") from e

    def _add_base_quote_data(self, quote):
        """
            Add base quote data (similar for all security types) to the database but do not perform commit.

            Args:
                quotes_dict(list of dictionaries): quotes obtained from an API wrapper.

            Returns:
                int: last row id of the operation.

            Raises:
                FdataError: sql error happened.
        """
        self.check_if_connected()

        insert_quote = f"""INSERT OR {self._update} INTO quotes (symbol_id,
                                                                    source_id,
                                                                    time_stamp,
                                                                    time_span_id,
                                                                    opened,
                                                                    high,
                                                                    low,
                                                                    closed,
                                                                    volume,
                                                                    transactions)
                            VALUES (
                            (SELECT symbol_id FROM symbols WHERE ticker = '{self.symbol}'),
                            (SELECT source_id FROM sources WHERE title = '{self.source_title}'),
                            ({quote['ts']}),
                            (SELECT time_span_id FROM timespans WHERE title = '{self.timespan.value}' COLLATE NOCASE),
                            ({quote['open']}),
                            ({quote['high']}),
                            ({quote['low']}),
                            ({quote['close']}),
                            ({quote['volume']}),
                            ({quote['transactions']})
                        );"""

        try:
            self.cur.execute(insert_quote)
        except self.Error as e:
            raise FdataError(f"Can't add quotes data to a table 'quotes': {e}\n\nThe query is\n{insert_quote}") from e

        return self.cur.lastrowid

    def add_quotes(self, quotes_dict):
        """
            Add quotes to the database.

            Args:
                quotes_dict(list of dictionaries): quotes obtained from an API wrapper.

            Returns:
                (int, int): the total number of quotes before and after the operation.

            Raises:
                FdataError: sql error happened.
        """
        self.check_if_connected()

        # Insert new symbols to 'symbols' table (if the symbol does not exist)
        if self.get_total_symbol_quotes_num() == 0:
            self.add_symbol()

        num_before = self.get_quotes_num()

        if quotes_dict is not None:
            for quote in quotes_dict:
                self._add_base_quote_data(quote)

            self.commit()

        num_after = self.get_quotes_num()

        self.update_quote_intervals()

        return (num_before, num_after)

    def update_quote_intervals(self):
        """
            Update the earliest requested quote (if needed).
        """
        now = self.current_ts(adjusted=True)
        ts = min(now, self.last_date_ts)

        # TODO LOW Write it in a more rational way (if it is ever possible on sqlite)
        update_fetched = f"""INSERT OR REPLACE INTO quote_intervals (symbol_id, time_span_id, source_id, min_request_ts, max_request_ts)
                              VALUES ((SELECT symbol_id FROM symbols WHERE ticker = '{self.symbol}'),
                                      (SELECT time_span_id FROM timespans WHERE title = '{self.timespan}'),
                                      (SELECT source_id FROM sources WHERE title = '{self.source_title}'),
                                      (SELECT ifnull(
                                                     (SELECT min(min_request_ts, {self.first_date_ts})
	                                                  FROM quote_intervals
	                                                  WHERE symbol_id = (SELECT symbol_id FROM symbols WHERE ticker = '{self.symbol}')
	                                                  AND source_id = (SELECT source_id FROM sources WHERE title = '{self.source_title}')
	                                                  AND time_span_id = (SELECT time_span_id FROM timespans WHERE title = '{self.timespan}')
                                              ), {self.first_date_ts})),
                                      (SELECT ifnull(
                                                     (SELECT max(max_request_ts, {ts})
                                                      FROM quote_intervals
                                                      WHERE symbol_id = (SELECT symbol_id FROM symbols WHERE ticker = '{self.symbol}')
                                                      AND source_id = (SELECT source_id FROM sources WHERE title = '{self.source_title}')
                                                      AND time_span_id = (SELECT time_span_id FROM timespans WHERE title = '{self.timespan}')
                                              ), {ts}))
                           );"""

        try:
            self.cur.execute(update_fetched)
            self.conn.commit()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table 'quote_intervals': {e}\n{update_fetched}") from e

    def remove_quotes(self):
        """
            Remove quotes from the database.

            Raises:
                FdataError: sql error happened.
        """
        self.check_if_connected()

        remove_quotes = f"""DELETE FROM quotes WHERE symbol_id = (SELECT symbol_id FROM symbols WHERE ticker = '{self.symbol}')
                            AND time_stamp >= {self.first_date_ts} AND time_stamp <= {self.last_date_ts};"""

        try:
            self.cur.execute(remove_quotes)
            self.conn.commit()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table 'quotes': {e}\n{remove_quotes}") from e

        # Check if symbol is removed completely
        if self.get_total_symbol_quotes_num() == 0:
            self.remove_symbol()

    def add_info(self, info):
        """
            Add security info to the database.

            Args:
                info(dict): Security info obtained from an API wrapper.

            Raises:
                FdataError: sql error happened.
        """
        if self._sec_info_supported:
            self.check_if_connected()

            # Insert new symbols to 'symbols' table (if the symbol does not exist)
            if self.get_total_symbol_quotes_num() == 0:
                self.add_symbol()

            try:
                time_zone = info['fc_time_zone']
                sec_type = info['fc_sec_type']
            except KeyError as e:
                raise FdataError(f"Key is not found. Likely broken data is obtained (due to data soruce issues): {e}")

            currency = Currency.Unknown  # Currencies are not supported yet

            insert_info = f"""INSERT OR {self._update} INTO sec_info (symbol_id,
                                        source_id,
                                        time_zone,
                                        sec_type_id,
                                        currency_id)
                                    VALUES (
                                            (SELECT symbol_id FROM symbols WHERE ticker = '{self.symbol}'),
                                            (SELECT source_id FROM sources WHERE title = '{self.source_title}'),
                                            ('{time_zone}'),
                                            (SELECT sec_type_id FROM sectypes WHERE title = '{sec_type}'),
                                            (SELECT currency_id FROM currency WHERE title = '{currency}')
                                        );"""

            try:
                self.cur.execute(insert_info)
            except self.Error as e:
                raise FdataError(f"Can't add a record to a table 'sec_info': {e}\n\nThe query is\n{insert_info}") from e

            self.commit()

##########################
# Base data fetching class
##########################
class BaseFetcher(ReadWriteData, metaclass=abc.ABCMeta):
    """
        Abstract class to fetch quotes by API wrapper and add them to the database.
    """
    def __init__(self, **kwargs):
        """Initialize the instance of BaseFetcher class."""
        super().__init__(**kwargs)

        self.max_queries = None # Maximul allowed number of API queries per minute
        self._queries = []  # List of queries to calculate API call pauses

    # TODO LOW Think of adding an argument flag which indicates if quotes should be re-fetched
    def get(self, num=0, columns=None, joins=None, queries=None, ignore_last_date=False):
        """
            Check is the required number of quotes exist in the database and fetch if not.
            The data will be cached in the database. This method will connect to the database automatically if needed.
            At the end the connection status will be resumed.

            Args:
                num(int): the number of rows to get. 0 gets all the quotes.
                columns(list): additional columns to query.
                joins(list): additional joins to get data from other tables.
                queries(list): additional queries from other tables (like funamental, global economic data).
                ignore_last_date(bool): indicates if last date should be ignored (all recent history is obtained)

            Returns:
                array: the fetched data.
                int: the number of fetched quotes.
        """
        initially_connected = self.is_connected()

        if self.is_connected() is False:
            self.db_connect()

        current_num = self.get_symbol_quotes_num()
        total_num = self.get_symbol_quotes_num(dt=False)

        last_ts_adj = min(self.last_date_ts, self.current_ts())

        # We need to check if the earliest and latest dates in database exceed the requested date for specified
        # source and time span. If not, no need to fetch.
        if current_num == 0 or self.first_date_ts < self.get_min_request_ts() or last_ts_adj > self.get_max_request_ts():
            intervals = []

            # Adjust intervals to avoid gaps in quotes database and also to avoid excessive fetching of quotes
            # if they already present in DB.
            if total_num:
                # New interval exceeds the old one on both sides
                if self.first_date_ts < self.get_min_request_ts() and last_ts_adj > self.get_max_request_ts():
                    intervals.append([self.first_date_ts, self.get_min_request_ts()])
                    intervals.append([self.get_max_request_ts(), last_ts_adj])

                # New interval is completely before the old interval
                elif self.first_date_ts < self.get_min_request_ts() and last_ts_adj < self.get_min_request_ts():
                    intervals.append([self.first_date_ts, self.get_min_request_ts()])

                # New interval is completely after the old interval
                elif self.first_date_ts > self.get_max_request_ts() and last_ts_adj > self.get_max_request_ts():
                    intervals.append([self.get_max_request_ts(), last_ts_adj])

                # New interval is before the old inverval but has an overlap with the old one
                elif self.first_date_ts < self.get_min_request_ts() and last_ts_adj > self.get_min_request_ts():
                    intervals.append([self.first_date_ts, self.get_min_request_ts()])

                # New interval is after the old interval but has an overlap with the old one
                elif self.first_date_ts < self.get_max_request_ts() and last_ts_adj > self.get_max_request_ts():
                    intervals.append([self.get_max_request_ts(), last_ts_adj])
            else:
                intervals.append([self.first_date_ts, last_ts_adj])

            for first_ts, last_ts in intervals:
                self.log(f"Fetching contiguous data for {self.symbol} from {get_dt(first_ts)} to {get_dt(last_ts)}...")

                self.add_quotes(self.fetch_quotes(first_ts=first_ts, last_ts=last_ts))

        rows = self.get_quotes(num=num, columns=columns, joins=joins, queries=queries, ignore_last_date=ignore_last_date)

        if initially_connected is False:
            self.db_close()

        return rows

    def query_api(self, url, timeout=30):
        """
            Check if we need to wait before the next API query, wait if needed and query the API.

            Args:
                url(string): URL to fetch
                timeout(int): timeout for a response

            Returns:
                Response: obtained data
        """
        # Check if we are about to reach the API key limit for queries
        if len(self._queries) >= self.max_queries:
            # Get the first query time from the array
            first_query_time = self._queries[0]

            # Calculate time to sleep and sleep if needed
            sleep_time = max(0, 60 - (perf_counter() - first_query_time))

            self.log(f"Sleeping for {round(sleep_time, 2)} seconds to avoid API key queries limit..")

            sleep(sleep_time)

            self._queries = []

        # Perform the query
        try:
            self.log(f"Fetching URL: {url}")

            session = requests.Session()
            headers = {'Cache-Control': 'no-cache'}  # Disable cache for the request
            response = session.get(url, headers=headers, timeout=timeout)
            session.close()
        except (urllib.error.HTTPError, urllib.error.URLError, http.client.HTTPException, json.decoder.JSONDecodeError) as e:
            raise FdataError(f"Can't fetch quotes: {e}") from e
        finally:
            self._queries.append(perf_counter())

        return response

    def get_request_datetimes(self, first_ts, last_ts, trim_last=False):
        """
            Get the datetimes adjusted to the time zone of symbol's exchange for the request.

            Args:
                num(int): the number of days to limit the request.
                first_ts(int): overridden first ts to fetch.
                last_ts(int): overridden last ts to fetch.
                trim_last(bool): indicates if the last date should be set to the current date if it exceeds it.

            Returns:
                tuple(datetime): the adjusted datetimes.
        """
        if first_ts is not None:
            first_dt = get_dt(first_ts)
        else:
            first_dt = self.first_date

        if trim_last:
            current_ts = int(datetime.now(tz.UTC).timestamp())

            if last_ts is None:
                last_ts = current_ts
            else:
                last_ts = min(last_ts, current_ts)

        if last_ts is not None:
            last_dt = get_dt(last_ts)
        else:
            last_dt = self.last_date

        # Convert dates to the symbol's time zome for the request. In DB timestamps are always UTC adjusted,
        # but data source usually expect dates in the timezone of the exchange. When we convert dates
        # consider that the current time is noon to avoid excessive dates shift if time zone difference is not big.
        first_datetime = first_dt.replace(tzinfo=tz.UTC, hour=12).astimezone(self.get_timezone()).replace(tzinfo=None)
        last_datetime = last_dt.replace(tzinfo=tz.UTC, hour=12).astimezone(self.get_timezone()).replace(tzinfo=None)

        return (first_datetime, last_datetime)

    def get_request_dates(self, first_ts, last_ts, trim_last=False):
        """
            Get the dates adjusted to the time zone of symbol's exchange for the request.

            Args:
                num(int): the number of days to limit the request.
                first_ts(int): overridden first ts to fetch.
                last_ts(int): overridden last ts to fetch.
                trim_last(bool): indicates if the last date should be set to the current date if it exceeds it.

            Returns:
                tuple(datetime.date): the adjusted dates.
        """
        first_dt, last_dt = self.get_request_datetimes(first_ts=first_ts, last_ts=last_ts, trim_last=trim_last)

        first_date = first_dt.date()
        last_date = last_dt.date()

        return (first_date, last_date)

    @abc.abstractmethod
    def get_recent_data(self, to_cache=False):
        """
            Get real time data. Used in screening. This method should be overloaded if real time data fetching is possible
            for a particular data source.

            Args:
                to_cache(bool): indicates if real time data should be cached in a database.

            Returns:
                list: real time data.
        """

    @abc.abstractmethod
    def fetch_quotes(self, first_ts=None, last_ts=None):
        """
            Abstract method to fetch quotes.

            Args:
                first_ts(int): overridden first ts to fetch.
                last_ts(int): overridden last ts to fetch.

            Returns:
                list(dict): obtained quotes.
        """

    @abc.abstractmethod
    def get_timespan_str(self):
        """
            Get timespan string (like '5min' and so on) to query a particular data source based on the timespan specified
            in the datasource instance.

            Returns:
                str: timespan string.
        """

    @abc.abstractmethod
    def fetch_info(self):
        """Abstract method to fetch security info"""

    # TODO MID Think if it be implemented here or made abstract
    def query_and_parse(self, url, timeout=30):
        """
            Query the data source and parse the response. Used to handle data source API call limit.

            Args:
                url(str): the url for a request.
                timeout(int): timeout for the request.

            Returns:
                Parsed data.
        """
