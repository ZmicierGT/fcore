"""Data abstraction module.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

from enum import Enum

import abc

from data import fdatabase

from data.fvalues import Timespans

# Enum class for database types
class DbTypes(Enum):
    """
        Database types enum. Currently only SQLite is supported.
    """
    SQLite = "sqlite"

# Exception class for general data errors
class FdataError(Exception):
    """
        Base data exception class.
    """
    pass

# Base query object class
class Query():
    """
        Base database query class.
    """
    def __init__(self):
        """
            Initialize base database query class.
        """
        # Setting the default values
        self.symbol = ""
        self.first_date = -2147483648
        self.last_date = 9999999999999
        self.update = "IGNORE"
        self.source_title = ""
        self.timespan = Timespans.Day

        self.db_type = "sqlite"
        self.db_name = "data.sqlite"
        self.database = None
        self.conn = None
        self.cur = None

        # Type of exception for db queries
        self.Error = None

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

    def db_close(self):
        """
            Close the database connection.
        """
        self.database.db_close()

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

    def check_source(self):
        """
            Check if the current source exists in the table 'sources'

            Returns:
                int: the number of rows in 'sources' table.

            Raises:
                FdataError: sql error happened.
        """
        try:
            self.query.cur.execute(f"SELECT title FROM sources WHERE title = '{self.query.source_title}';")
            rows = self.query.cur.fetchall()
        except self.query.Error as e:
            raise FdataError(f"Can't query table: {e}") from e

        # Check if sources table has the required row
        return len(rows)

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
                            AND "TimeStamp" >= {self.query.first_date}
                            AND "TimeStamp" <= {self.query.last_date} ORDER BY "TimeStamp";"""

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
        self.query = query

    def check_database(self):
        """
            Database create/integrity check function.
            Checks if the database exists. Otherwise, creates it. Checks if the database has required tables.

            Raises:
                FdataError: sql error happened.
        """
        # Check if we need to create table 'quotes'
        try:
            self.query.cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='quotes';")
            rows = self.query.cur.fetchall()
        except self.query.Error as e:
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
                self.query.cur.execute(create_quotes)
            except self.query.Error as e:
                raise FdataError(f"Can't create table: {e}") from e

        # Check if we need to create table 'symbols'
        try:
            self.query.cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='symbols';")
            rows = self.query.cur.fetchall()
        except self.query.Error as e:
            raise FdataError(f"Can't query table: {e}") from e

        if len(rows) == 0:
            create_symbols = """CREATE TABLE symbols(
                                symbol_id INTEGER PRIMARY KEY,
                                ticker TEXT NOT NULL UNIQUE,
                                ISIN TEXT UNIQUE,
                                description TEXT
                                );"""

            try:
                self.query.cur.execute(create_symbols)
            except self.query.Error as e:
                raise FdataError(f"Can't create table: {e}") from e

        # Check if we need to create table 'sources'
        try:
            self.query.cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sources';")
            rows = self.query.cur.fetchall()
        except self.query.Error as e:
            raise FdataError(f"Can't query table: {e}") from e

        if len(rows) == 0:
            create_sources = """CREATE TABLE sources(
                                source_id INTEGER PRIMARY KEY,
                                title TEXT NOT NULL UNIQUE,
                                description TEXT
                                );"""

            try:
                self.query.cur.execute(create_sources)
            except self.query.Error as e:
                raise FdataError(f"Can't create table: {e}") from e

        # Check if we need to create table 'timespans'
        try:
            self.query.cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='timespans';")
            rows = self.query.cur.fetchall()
        except self.query.Error as e:
            raise FdataError(f"Can't query table: {e}") from e

        if len(rows) == 0:
            create_timespans = """CREATE TABLE timespans(
                                    timespan_id INTEGER PRIMARY KEY,
                                    title TEXT NOT NULL UNIQUE
                                );"""

            try:
                self.query.cur.execute(create_timespans)
            except self.query.Error as e:
                raise FdataError(f"Can't create table: {e}") from e

        # Check if timespans table is empty
        try:
            self.query.cur.execute("SELECT * FROM timespans;")
            rows = self.query.cur.fetchall()
        except self.query.Error as e:
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
                self.query.cur.execute(insert_timespans)
                self.query.conn.commit()
            except self.query.Error as e:
                raise FdataError(f"Can't insert data to a table 'timespans': {e}") from e

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
                            AND "TimeStamp" >= {self.query.first_date} AND "TimeStamp" <= {self.query.last_date};"""

        try:
            self.query.cur.execute(remove_quotes)
            self.query.conn.commit()
        except self.query.Error as e:
            raise FdataError(f"Can't remove quotes from a table 'sources': {e}") from e

        # Check if symbol is removed completely
        if self.get_symbol_quotes_num() == 0:
            self.remove_symbol()

    def add_source(self):
        """
            Add source to the database.

            Raises:
                FdataError: sql error happened.
        """
        insert_source = f"INSERT INTO sources (title) VALUES ('{self.query.source_title}')"

        try:
            self.query.cur.execute(insert_source)
            self.query.conn.commit()
        except self.query.Error as e:
            raise FdataError(f"Can't insert data to a table 'sources': {e}") from e

class BaseFetchData(ReadWriteData, metaclass=abc.ABCMeta):
    """
        Abstract class to fetch quotes by API wrapper and add them to the database.
    """
    def check_and_fetch(self):
        """
            Check the database and fetch quotes.

            Returns:
                int: the number of fetched quotes.
        """
        self.check_database()

        if self.check_source() == False:
            self.add_source()

        return self.insert_quotes(self.fetch_quotes())

    def insert_quotes(self, rows):
        """
            Insert fetched and parsed quotes to the database.

            Args:
                rows(list): the list of quotes to insert.
        """
        # Insert new symbols to 'symbols' table (if the symbol does not exist)
        self.add_symbol()

        num_before = self.get_quotes_num()

        self.add_quotes(rows)
        self.commit()

        num_after = self.get_quotes_num()

        return (num_before, num_after)

    @abc.abstractmethod
    def fetch_quotes(self):
        """
            Abstract method to fetch quotes.
        """
        pass
