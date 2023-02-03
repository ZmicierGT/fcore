"""Data abstraction module.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

from enum import Enum

import abc

from data import fdatabase

from data.fvalues import Timespans, def_first_date, def_last_date
from data.futils import get_dt

import settings

class DbTypes(Enum):
    """
        Database types enum. Currently only SQLite is supported.
    """
    SQLite = "sqlite"

class FdataError(Exception):
    """
        Base data exception class.
    """

# TODO Query type should be omitted and joined with the base data class. Fix the related pylint warnings.
class Query():
    """
        Base database query class.
    """
    def __init__(self,
                 symbol="",
                 first_date=def_first_date,
                 last_date=def_last_date,
                 timespan=Timespans.Day,
                 update="IGNORE",
                 ):
        """
            Initialize base database query class.
        """
        # Setting the default values
        self.symbol = symbol

        # Underlying variables for getters/setter
        self._first_date = None
        self._last_date = None

        # Getter/setter will be invoked
        self.first_date = first_date
        self.last_date = last_date

        self.update = update
        self.timespan = timespan

        # Source title should be overridden in derived classes for particular data sources
        self.source_title = ""

        # Default setting for the base data source
        self.db_type = settings.Quotes.db_type
        self.db_name = settings.Quotes.db_name

        self.database = None
        self.conn = None
        self.cur = None

        # Type of exception for db queries
        self.Error = None

        # Flag which indicates if the database is connected
        self.Connected = False

    ########################################################
    # Get/set datetimes (depending on the input value type).
    ########################################################
    @property
    def first_date(self):
        """
            Get the first datetime of the query.

            Returns:
                datetime: the first datetime in the query.
        """
        return self._first_date

    @first_date.setter
    def first_date(self, value):
        """
            Set the first datetime of the query.

            value(int, str, datetime): datetime representation to set.

            Raises:
                ValueError, OSError: incorrect datetime representation.
        """
        self._first_date = get_dt(value)

    @property
    def last_date(self):
        """
            Get the last datetime of the query.

            Returns:
                datetime: the last datetime in the query.
        """
        return self._last_date

    @last_date.setter
    def last_date(self, value):
        """
            Set the last datetime of the query.

            value(int, str, datetime): datetime representation to set.

            Raises:
                ValueError, OSError: incorrect datetime representation.
        """
        self._last_date = get_dt(value)

    @property
    def first_date_ts(self):
        """
            Get the first datetime's timestamp of the query.

            Returns:
                int: the first datetime's timestamp in the query.
        """
        return int(self.first_date.timestamp())

    @property
    def last_date_ts(self):
        """
            Get the last datetime's timestamp of the query.

            Returns:
                int: the last datetime's timestamp in the query.
        """
        return int(self.last_date.timestamp())

    @property
    def first_datetime_str(self):
        """
            Get the first datetime's string representation of the query.

            Returns:
                datetime: the first datetime's string representation in the query.
        """
        return self.first_date.strftime('%Y-%m-%d %H:%M:%S')

    @property
    def last_datetime_str(self):
        """
            Get the last datetime's string representation of the query.

            Returns:
                datetime: the last datetime's string representation in the query.
        """
        return self.last_date.strftime('%Y-%m-%d %H:%M:%S')

    @property
    def first_date_str(self):
        """
            Get the first datetime's string representation of the query.

            Returns:
                datetime: the first datetime's string representation in the query.
        """
        return self.first_date.strftime('%Y-%m-%d')

    @property
    def last_date_str(self):
        """
            Get the last datetime's string representation of the query.

            Returns:
                datetime: the last datetime's string representation in the query.
        """
        return self.last_date.strftime('%Y-%m-%d')

    def first_date_set_eod(self):
        """
            Set the first date's h/m/s/ to EOD (23:59:59)
        """
        self._first_date = self._first_date.replace(hour=23, minute=59, second=59)

    def last_date_set_eod(self):
        """
            Set the last date's h/m/s/ to EOD (23:59:59)
        """
        self._last_date = self._last_date.replace(hour=23, minute=59, second=59)

    ##############################################
    # End of datetime handling methods/properties.
    ##############################################

    def get_db_type(self):
        """
            Get used database type.

            Returns:
                DbTypes: database type.
        """
        return DbTypes("sqlite")

    def db_connect(self):
        """
            Connect to the databse.
        """
        if DbTypes('sqlite') == DbTypes.SQLite:
            self.database = fdatabase.SQLiteConn(self)
            self.database.db_connect()
            self.Connected = True

            # Check the database integrity
            self.check_database()

            if self.check_source() == False:
                self.add_source()

    def db_close(self):
        """
            Close the database connection.
        """
        self.database.db_close()
        self.Connected = False

    def check_database(self):
        """
            Database create/integrity check method.
            Checks if the database exists. Otherwise, creates it. Checks if the database has required tables.

            Raises:
                FdataError: sql error happened.
        """
        # Check if we need to create table 'quotes'
        try:
            self.cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='quotes';")
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't query table: {e}") from e

        if len(rows) == 0:
            create_quotes = """CREATE TABLE quotes (
                            quote_id INTEGER PRIMARY KEY,
                            symbol_id INTEGER NOT NULL,
                            source_id INTEGER NOT NULL,
                            "TimeStamp" INTEGER NOT NULL,
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
                            UNIQUE(symbol_id, "TimeStamp", timespan_id)
                            );"""

            try:
                self.cur.execute(create_quotes)
            except self.Error as e:
                raise FdataError(f"Can't create table: {e}") from e

        # Check if we need to create table 'symbols'
        try:
            self.cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='symbols';")
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't query table: {e}") from e

        if len(rows) == 0:
            create_symbols = """CREATE TABLE symbols(
                                symbol_id INTEGER PRIMARY KEY,
                                ticker TEXT NOT NULL UNIQUE,
                                ISIN TEXT UNIQUE,
                                description TEXT
                                );"""

            try:
                self.cur.execute(create_symbols)
            except self.Error as e:
                raise FdataError(f"Can't create table: {e}") from e

        # Check if we need to create table 'sources'
        try:
            self.cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sources';")
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't query table: {e}") from e

        if len(rows) == 0:
            create_sources = """CREATE TABLE sources(
                                source_id INTEGER PRIMARY KEY,
                                title TEXT NOT NULL UNIQUE,
                                description TEXT
                                );"""

            try:
                self.cur.execute(create_sources)
            except self.Error as e:
                raise FdataError(f"Can't create table: {e}") from e

        # Check if we need to create table 'timespans'
        try:
            self.cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='timespans';")
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't query table: {e}") from e

        if len(rows) == 0:
            create_timespans = """CREATE TABLE timespans(
                                    timespan_id INTEGER PRIMARY KEY,
                                    title TEXT NOT NULL UNIQUE
                                );"""

            try:
                self.cur.execute(create_timespans)
            except self.Error as e:
                raise FdataError(f"Can't create table: {e}") from e

        # Check if timespans table is empty
        try:
            self.cur.execute("SELECT * FROM timespans;")
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't query table: {e}") from e

        # Check if timespans table has data
        if len(rows) < 6:
            insert_timespans = f"""INSERT INTO timespans (title)
                                    VALUES
                                    ('{Timespans.Unknown}'),
                                    ('{Timespans.Intraday}'),
                                    ('{Timespans.Day}'),
                                    ('{Timespans.Week}'),
                                    ('{Timespans.Month}'),
                                    ('{Timespans.Year}');"""

            try:
                self.cur.execute(insert_timespans)
                self.conn.commit()
            except self.Error as e:
                raise FdataError(f"Can't insert data to a table 'timespans': {e}") from e

    def check_source(self):
        """
            Check if the current source exists in the table 'sources'

            Returns:
                int: the number of rows in 'sources' table.

            Raises:
                FdataError: sql error happened.
        """
        try:
            self.cur.execute(f"SELECT title FROM sources WHERE title = '{self.source_title}';")
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't query table: {e}") from e

        # Check if sources table has the required row
        return len(rows)

    def add_source(self):
        """
            Add source to the database.

            Raises:
                FdataError: sql error happened.
        """
        insert_source = f"INSERT INTO sources (title) VALUES ('{self.source_title}')"

        try:
            self.cur.execute(insert_source)
            self.conn.commit()
        except self.Error as e:
            raise FdataError(f"Can't insert data to a table 'sources': {e}") from e

class ReadOnlyData(metaclass=abc.ABCMeta):
    """
        Base class for SQL 'read only' data operations.
    """
    def __init__(self, query):
        """
            Initialize base read only data abstraction class.

            Args:
                query(Query): database query.
        """
        self.query = query

    def get_all_symbols(self):
        """
            Get all symbols in the database.

            Returns:
                list: list with all the symbols.

            Raises:
                FdataError: sql error happened.
        """
        try:
            self.query.cur.execute("SELECT ticker, ISIN, description FROM symbols;")
            rows = self.query.cur.fetchall()
        except self.query.Error as e:
            raise FdataError(f"Can't query table: {e}") from e

        return rows

    def get_quotes(self):
        """
            Get quotes for specified symbol, dates and timespan (if any).

            Returns:
                list: list with quotes data.

            Raises:
                FdataError: sql error happened.
        """
        timespan_query = ""

        if self.query.timespan != Timespans.All:
            timespan_query = "AND timespans.title = '" + self.query.timespan + "'"

        select_quotes = f"""SELECT ticker,
                                ISIN,
                                sources.title,
                                datetime("TimeStamp", 'unixepoch'),
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
                            WHERE symbols.ticker = '{self.query.symbol}'
                            {timespan_query}
                            AND "TimeStamp" >= {self.query.first_date_ts}
                            AND "TimeStamp" <= {self.query.last_date_ts} ORDER BY "TimeStamp";"""

        try:
            self.query.cur.execute(select_quotes)
            rows = self.query.cur.fetchall()
        except self.query.Error as e:
            raise FdataError(f"Can't query table: {e}") from e

        return rows

    def get_last_quotes(self, num):
        """
            Return a requested number of newest quotes in the database.

            Args:
                num(int): the number of rows to get.

            Returns:
                list: the list with the requested number of newest quotes.

            Raises:
                FdataError: sql error happened.
        """
        timespan_query = ""

        if self.query.timespan != Timespans.All:
            timespan_query = "AND timespans.title = '" + self.query.timespan + "'"

        select_quotes = f"""SELECT ticker,
                                ISIN,
                                sources.title,
                                datetime("TimeStamp", 'unixepoch'),
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
                            WHERE symbols.ticker = '{self.query.symbol}'
                            {timespan_query}
                            ORDER BY "TimeStamp" DESC
                            LIMIT {num};"""

        try:
            self.query.cur.execute(select_quotes)
            rows = self.query.cur.fetchall()
        except self.query.Error as e:
            raise FdataError(f"Can't query table: {e}") from e

        rows.reverse()

        return rows

    def get_quotes_num(self):
        """
            Get the number of quotes in the database.

            Returns:
                int: the total number of all quotes in the database.

            Raises:
                FdataError: sql error happened.
        """
        try:
            self.query.cur.execute("SELECT COUNT(*) FROM quotes;")
        except self.query.Error as e:
            raise FdataError(f"Can't query table: {e}") from e

        result = self.query.cur.fetchone()[0]

        if result is None:
            result = 0

        return result

    def get_symbol_quotes_num(self):
        """
            Get the number of quotes in the database per symbol.

            Returns:
                int: the number of quotes in the database per symbol.

            Raises:
                FdataError: sql error happened.
        """
        try:
            self.query.cur.execute(f"SELECT COUNT(*) FROM quotes WHERE symbol_id = (SELECT symbol_id FROM symbols where ticker = '{self.query.symbol}');")
        except self.query.Error as e:
            raise FdataError(f"Can't query table: {e}") from e

        result = self.query.cur.fetchone()[0]

        if result is None:
            result = 0

        return result

    def get_symbol_quotes_num_dt(self):
        """
            Get the number of quotes in the database per symbol for specified dates.

            Returns:
                int: the number of quotes in the database per symbol.

            Raises:
                FdataError: sql error happened.
        """
        num_query = f"""SELECT COUNT(*) FROM quotes WHERE symbol_id =
                        (SELECT symbol_id FROM symbols where ticker = '{self.query.symbol}') AND
                        "TimeStamp" >= {self.query.first_date_ts} AND "TimeStamp" <= {self.query.last_date_ts};"""

        try:
            self.query.cur.execute(num_query)
        except self.query.Error as e:
            raise FdataError(f"Can't query table: {e}") from e

        result = self.query.cur.fetchone()[0]

        if result is None:
            result = 0

        return result

    def get_max_datetime(self):
        """
            Get maximum datetime for a particular symbol.

            Returns:
                int: the number of quotes in the database for a specified symbol.

            Raises:
                FdataError: sql error happened.
        """
        max_datetime_query = f"""SELECT MAX(datetime("TimeStamp", 'unixepoch')) FROM quotes
                                    INNER JOIN symbols ON quotes.symbol_id = symbols.symbol_id
                                    WHERE symbols.ticker = '{self.query.symbol}'"""

        try:
            self.query.cur.execute(max_datetime_query)
        except self.query.Error as e:
            raise FdataError(f"Can't get max datetime: {e}") from e

        return self.query.cur.fetchone()[0]

class ReadWriteData(ReadOnlyData):
    """
        Base class for read/write SQL operations.
    """
    def __init__(self, query):
        """
            Initialize read/write SQL abstraction class.

            Args:
                query(Query): database query instance.
        """
        super().__init__(query)

    def commit(self):
        """
            Commit the change to the database.

            Raises:
                FdataError: sql error happened.
        """
        try:
            self.query.conn.commit()
        except self.query.Error as e:
            raise FdataError(f"Can't commit: {e}") from e

    def add_symbol(self):
        """
            Add new symbol to the database.

            Raises:
                FdataError: sql error happened.
        """
        insert_symbol = f"INSERT OR IGNORE INTO symbols (ticker) VALUES ('{self.query.symbol}');"

        try:
            self.query.cur.execute(insert_symbol)
            self.query.conn.commit()
        except self.query.Error as e:
            raise FdataError(f"Can't add ticker to a table 'symbols': {e}") from e

    def remove_symbol(self):
        """
            Remove a symbol completely.

            Raises:
                FdataError: sql error happened.
        """
        delete_quotes = f"DELETE FROM quotes WHERE symbol_id = (SELECT symbol_id FROM symbols WHERE ticker = '{self.query.symbol}');"
        delete_symbol = f"DELETE FROM symbols WHERE symbol_id = (SELECT symbol_id FROM symbols WHERE ticker = '{self.query.symbol}');"

        try:
            self.query.cur.execute(delete_quotes)
            self.query.cur.execute(delete_symbol)
            self.query.conn.commit()
        except self.query.Error as e:
            raise FdataError(f"Can't remove {self.query.symbol}: {e}") from e

    def add_quotes(self, quotes_dict):
        """
            Add quotes to the database.

            Args:
                quotes_dict(list of dictionaries): quotes obtained from an API wrapper.

            Raises:
                FdataError: sql error happened.
        """
        for row in quotes_dict:
            volume = row['v']
            VWAP = row['vw']
            opened = row['o']
            adjclose = row['c']
            close = row['cl']
            high = row['h']
            low = row['l']
            timestamp = row['t']
            transactions = row['n']
            dividends = row['d']

            insert_quote = f"""INSERT OR {self.query.update} INTO quotes (symbol_id, source_id, "TimeStamp", timespan_id, "Open", High, Low, Close, AdjClose, Volume, Transactions, VWAP, Dividends)
                                VALUES (
                                (SELECT symbol_id FROM symbols WHERE ticker = '{self.query.symbol}'),
                                (SELECT source_id FROM sources WHERE title = '{self.query.source_title}'),
                                ({timestamp}),
                                (SELECT timespan_id FROM timespans WHERE title = '{self.query.timespan}' COLLATE NOCASE),
                                ({opened}),
                                ({high}),
                                ({low}),
                                ({close}),
                                ({adjclose}),
                                ({volume}),
                                ({transactions}),
                                ({VWAP}),
                                ({dividends})
                            );"""

            try:
                self.query.cur.execute(insert_quote)
            except self.query.Error as e:
                raise FdataError(f"Can't add ticker to a table 'symbols': {e}\n\nThe query is\n{insert_quote}") from e

    def remove_quotes(self):
        """
            Remove quotes from the database.

            Raises:
                FdataError: sql error happened.
        """
        remove_quotes = f"""DELETE FROM quotes WHERE symbol_id = (SELECT symbol_id FROM symbols WHERE ticker = '{self.query.symbol}')
                            AND "TimeStamp" >= {self.query.first_date_ts} AND "TimeStamp" <= {self.query.last_date_ts};"""

        try:
            self.query.cur.execute(remove_quotes)
            self.query.conn.commit()
        except self.query.Error as e:
            raise FdataError(f"Can't remove quotes from a table 'sources': {e}") from e

        # Check if symbol is removed completely
        if self.get_symbol_quotes_num() == 0:
            self.remove_symbol()

class BaseFetchData(ReadWriteData, metaclass=abc.ABCMeta):
    """
        Abstract class to fetch quotes by API wrapper and add them to the database.
    """
    def __init__(self, query):
        """Initialize the instance of BaseFetchData class."""
        super().__init__(query)

    def insert_quotes(self, rows):
        """
            Insert fetched and parsed quotes to the database.

            Args:
                rows(list): the list of quotes to insert.

            Returns:
                num_before(int): the number of quotes before the operation.
                num_after(int): the number of quotes after the operatioon.
        """
        # Insert new symbols to 'symbols' table (if the symbol does not exist)
        self.add_symbol()

        num_before = self.get_quotes_num()

        self.add_quotes(rows)
        self.commit()

        num_after = self.get_quotes_num()

        return (num_before, num_after)

    def fetch_if_none(self, threshold):
        """
            Check is the required number of quotes exist in the database and fetch if not.
            The data will be cached in the database. This method will connect to the database automatically if needed.
            At the end the connection status will be resumed.

            Args:
                treshold(int): the minimum required number of quotes in the database.

            Returns:
                array: the fetched data.
                int: the number of fetched quotes.
        """
        initially_connected = self.query.Connected

        if self.query.Connected == False:
            self.query.db_connect()

        current_num = self.get_symbol_quotes_num_dt()

        # Fetch quotes if there are less than a threshold number of records in the database for a selected timespan.
        if current_num < threshold:
            num_before, num_after = self.insert_quotes(self.fetch_quotes())
            num = num_after - num_before

            if num == 0:
                raise FdataError(f"Threshold {threshold} can't be met on specified date/time interval. Decrease the threshold.")
        else:
            num = 0

        rows = self.get_quotes()

        if initially_connected is False:
            self.query.db_close()

        return (rows, num)

    def get_rt_data(self, to_cache=False):
        """
            Get real time data. Used in screening. This method should be overloaded if real time data fetching is possible
            for a particular data source.

            Args:
                to_cache(bool): indicates if real time data should be cached in a database.

            Returns:
                list: real time data.
        """
        raise FdataError(f"Real time data is not supported (yet) for the source {type(self).__name__}")

    @abc.abstractmethod
    def fetch_quotes(self):
        """
            Abstract method to fetch quotes.
        """
