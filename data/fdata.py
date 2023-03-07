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

class ReadOnlyData():
    """
        Base class for SQL 'read only' data operations and database integrity check.
    """
    def __init__(self,
                 symbol="",
                 first_date=def_first_date,
                 last_date=def_last_date,
                 timespan=Timespans.Day
                ):
        """
            Initialize base database read only/integrity class.

            Args:
                symbol(str): the symbol to use.
                first_date(datetime, str, int): the first date for queries.
                last_date(datetime, str, int): the last date for queries.
                timespan(Timespans): timespan to use in queries.
        """
        # Setting the default values
        self.symbol = symbol

        # Underlying variables for getters/setter
        self._first_date = None
        self._last_date = None

        # Getter/setter will be invoked
        self.first_date = first_date
        self.last_date = last_date

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
        self.Connected = False

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
        self._first_date = get_dt(value)

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
        self._last_date = get_dt(value)

    @property
    def first_date_ts(self):
        """
            Get the first datetime's timestamp.

            Returns:
                int: the first datetime's timestamp in queries.
        """
        return int(self.first_date.timestamp())

    @property
    def last_date_ts(self):
        """
            Get the last datetime's timestamp.

            Returns:
                int: the last datetime's timestamp.
        """
        return int(self.last_date.timestamp())

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

            # Enable foreign keys
            # Check if we need to create table 'quotes'
            try:
                self.cur.execute("PRAGMA foreign_keys=on;")
            except self.Error as e:
                raise FdataError(f"Can't enable foreign keys: {e}") from e

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
        # Check if we need to create table 'symbols'
        try:
            self.cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='symbols';")
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't query table: {e}") from e

        if len(rows) == 0:
            create_symbols = """CREATE TABLE symbols(
                                symbol_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                ticker TEXT NOT NULL UNIQUE,
                                isin TEXT UNIQUE,
                                description TEXT
                                );"""

            try:
                self.cur.execute(create_symbols)
            except self.Error as e:
                raise FdataError(f"Can't create table: {e}") from e

            # Create index for ticker
            create_ticker_idx = "CREATE INDEX idx_ticker ON symbols(ticker);"

            try:
                self.cur.execute(create_ticker_idx)
            except self.Error as e:
                raise FdataError(f"Can't create index for ticker: {e}") from e

        # Check if we need to create table 'sources'
        try:
            self.cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sources';")
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't query table: {e}") from e

        if len(rows) == 0:
            create_sources = """CREATE TABLE sources(
                                source_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                title TEXT NOT NULL UNIQUE,
                                description TEXT
                                );"""

            try:
                self.cur.execute(create_sources)
            except self.Error as e:
                raise FdataError(f"Can't create table: {e}") from e

            # Create index for source title
            create_source_title_idx = "CREATE INDEX idx_source_title ON sources(title);"

            try:
                self.cur.execute(create_source_title_idx)
            except self.Error as e:
                raise FdataError(f"Can't create index for source title: {e}") from e

        # Check if we need to create table 'timespans'
        try:
            self.cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='timespans';")
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't query table: {e}") from e

        if len(rows) == 0:
            create_timespans = """CREATE TABLE timespans(
                                    time_span_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    title TEXT NOT NULL UNIQUE
                                );"""

            try:
                self.cur.execute(create_timespans)
            except self.Error as e:
                raise FdataError(f"Can't create table: {e}") from e

            # Create index for timespan title
            create_timespan_title_idx = "CREATE INDEX idx_timespan_title ON timespans(title);"

            try:
                self.cur.execute(create_timespan_title_idx)
            except self.Error as e:
                raise FdataError(f"Can't create index for timespan title: {e}") from e

        # Check if timespans table is empty
        try:
            self.cur.execute("SELECT * FROM timespans;")
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't query table: {e}") from e

        # Check if timespans table has data
        if len(rows) < 6:
            # Prepare the query with all supported timespans
            ts = ""

            for timespan in Timespans:
                if timespan != Timespans.All:
                    ts += f"('{timespan.value}'),"

            ts = ts[:len(ts) - 2]

            insert_timespans = f"""INSERT INTO timespans (title)
                                    VALUES {ts});"""

            try:
                self.cur.execute(insert_timespans)
                self.conn.commit()
            except self.Error as e:
                raise FdataError(f"Can't insert data to a table 'timespans': {e}") from e

        # Check if we need to create table 'quotes'
        try:
            self.cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='quotes';")
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't query table: {e}") from e

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
                            UNIQUE(symbol_id, time_stamp, time_span_id)
                            );"""

            try:
                self.cur.execute(create_quotes)
            except self.Error as e:
                raise FdataError(f"Can't create table: {e}") from e

            # Create indexes for quotes
            create_quotes_idx = "CREATE INDEX idx_quotes ON quotes(symbol_id, time_stamp, time_span_id);"

            try:
                self.cur.execute(create_quotes_idx)
            except self.Error as e:
                raise FdataError(f"Can't create index for quotes table: {e}") from e

        # Check if we need to create a table stock core
        try:
            self.cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='stock_core';")
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't query table: {e}") from e

        if len(rows) == 0:
            create_symbols = """CREATE TABLE stock_core(
                                stock_core_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                quote_id INTEGER NOT NULL UNIQUE,
                                raw_close REAL,
                                dividends REAL,
                                split_coefficient REAL,
                                CONSTRAINT fk_quotes,
                                    FOREIGN KEY (quote_id)
                                    REFERENCES quotes(quote_id)
                                    ON DELETE CASCADE
                                );"""

            try:
                self.cur.execute(create_symbols)
            except self.Error as e:
                raise FdataError(f"Can't create table: {e}") from e

            # Create index for quote_id
            create_quoteid_idx = "CREATE INDEX idx_quote ON quotes(quote_id);"

            try:
                self.cur.execute(create_quoteid_idx)
            except self.Error as e:
                raise FdataError(f"Can't create index for quote id: {e}") from e

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
        insert_source = f"INSERT OR IGNORE INTO sources (title) VALUES ('{self.source_title}')"

        try:
            self.cur.execute(insert_source)
            self.conn.commit()
        except self.Error as e:
            raise FdataError(f"Can't insert data to a table 'sources': {e}") from e

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
        try:
            self.cur.execute("SELECT ticker, isin, description FROM symbols;")
            rows = self.cur.fetchall()
        except self.Error as e:
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

        if self.timespan != Timespans.All:
            timespan_query = "AND timespans.title = '" + self.timespan + "'"

        select_quotes = f"""SELECT datetime(time_stamp, 'unixepoch'),
                                opened,
                                high,
                                low,
                                closed,
                                raw_close,
                                volume,
                                dividends,
                                split_coefficient,
                                transactions
                            FROM quotes INNER JOIN stock_core ON quotes.quote_id = stock_core.quote_id
                            INNER JOIN symbols ON quotes.symbol_id = symbols.symbol_id
                            INNER JOIN timespans ON quotes.time_span_id = timespans.time_span_id
                            WHERE symbols.ticker = '{self.symbol}'
                            {timespan_query}
                            AND time_stamp >= {self.first_date_ts}
                            AND time_stamp <= {self.last_date_ts} ORDER BY time_stamp;"""

        try:
            self.cur.execute(select_quotes)
            rows = self.cur.fetchall()
        except self.Error as e:
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

        if self.timespan != Timespans.All:
            timespan_query = "AND timespans.title = '" + self.timespan + "'"

        select_quotes = f"""SELECT datetime(time_stamp, 'unixepoch'),
                                opened,
                                high,
                                low,
                                closed,
                                raw_close,
                                volume,
                                dividends,
                                split_coefficient,
                                transactions
                            FROM quotes INNER JOIN stock_core ON quotes.quote_id = stock_core.quote_id
                            INNER JOIN symbols ON quotes.symbol_id = symbols.symbol_id
                            INNER JOIN timespans ON quotes.time_span_id = timespans.time_span_id
                            WHERE symbols.ticker = '{self.symbol}'
                            {timespan_query}
                            ORDER BY time_stamp DESC
                            LIMIT {num};"""

        try:
            self.cur.execute(select_quotes)
            rows = self.cur.fetchall()
        except self.Error as e:
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
            self.cur.execute("SELECT COUNT(*) FROM quotes;")
        except self.Error as e:
            raise FdataError(f"Can't query table: {e}") from e

        result = self.cur.fetchone()[0]

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
            self.cur.execute(f"SELECT COUNT(*) FROM quotes WHERE symbol_id = (SELECT symbol_id FROM symbols where ticker = '{self.symbol}');")
        except self.Error as e:
            raise FdataError(f"Can't query table: {e}") from e

        result = self.cur.fetchone()[0]

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
                        (SELECT symbol_id FROM symbols where ticker = '{self.symbol}') AND
                        time_stamp >= {self.first_date_ts} AND time_stamp <= {self.last_date_ts};"""

        try:
            self.cur.execute(num_query)
        except self.Error as e:
            raise FdataError(f"Can't query table: {e}") from e

        result = self.cur.fetchone()[0]

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
        max_datetime_query = f"""SELECT MAX(datetime(time_stamp, 'unixepoch')) FROM quotes
                                    INNER JOIN symbols ON quotes.symbol_id = symbols.symbol_id
                                    WHERE symbols.ticker = '{self.symbol}'"""

        try:
            self.cur.execute(max_datetime_query)
        except self.Error as e:
            raise FdataError(f"Can't get max datetime: {e}") from e

        return self.cur.fetchone()[0]

#############################
# Read/Write operations class
#############################

class ReadWriteData(ReadOnlyData):
    """
        Base class for read/write SQL operations.
    """
    def __init__(self, update=False, **kwargs):
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

    def commit(self):
        """
            Commit the change to the database.

            Raises:
                FdataError: sql error happened.
        """
        try:
            self.conn.commit()
        except self.Error as e:
            raise FdataError(f"Can't commit: {e}") from e

    def add_symbol(self):
        """
            Add new symbol to the database.

            Raises:
                FdataError: sql error happened.
        """
        insert_symbol = f"INSERT OR IGNORE INTO symbols (ticker) VALUES ('{self.symbol}');"

        try:
            self.cur.execute(insert_symbol)
            self.conn.commit()
        except self.Error as e:
            raise FdataError(f"Can't add ticker to a table 'symbols': {e}") from e

    def remove_symbol(self):
        """
            Remove a symbol completely.

            All corresponding records in quotes table will be deleted because of foreign key linking (cascade deletion).

            Raises:
                FdataError: sql error happened.
        """
        delete_symbol = f"DELETE FROM symbols WHERE symbol_id = (SELECT symbol_id FROM symbols WHERE ticker = '{self.symbol}');"

        try:
            self.cur.execute(delete_symbol)
            self.conn.commit()
        except self.Error as e:
            raise FdataError(f"Can't remove {self.symbol}: {e}") from e

    def add_quotes(self, quotes_dict):
        """
            Add quotes to the database.

            Args:
                quotes_dict(list of dictionaries): quotes obtained from an API wrapper.

            Raises:
                FdataError: sql error happened.
        """
        for row in quotes_dict:
            volume = row['volume']
            opened = row['open']
            adjclose = row['adj_close']
            rawclose = row['raw_close']
            high = row['high']
            low = row['low']
            timestamp = row['ts']
            transactions = row['transactions']
            dividends = row['divs']
            split_coefficient = row['split']

            insert_quote = f"""INSERT OR {self._update} INTO quotes (symbol_id, source_id, time_stamp, time_span_id, opened, high, low, closed, volume, transactions)
                                VALUES (
                                (SELECT symbol_id FROM symbols WHERE ticker = '{self.symbol}'),
                                (SELECT source_id FROM sources WHERE title = '{self.source_title}'),
                                ({timestamp}),
                                (SELECT time_span_id FROM timespans WHERE title = '{self.timespan}' COLLATE NOCASE),
                                ({opened}),
                                ({high}),
                                ({low}),
                                ({adjclose}),
                                ({volume}),
                                ({transactions})
                            );"""

            try:
                self.cur.execute(insert_quote)
            except self.Error as e:
                raise FdataError(f"Can't add ticker to a table 'symbols': {e}\n\nThe query is\n{insert_quote}") from e

            quote_id = self.cur.lastrowid

            if quote_id != 0:
                insert_core = f"""INSERT OR {self._update} INTO stock_core (quote_id, raw_close, dividends, split_coefficient)
                                VALUES (
                                    ({quote_id}),
                                    ({rawclose}),
                                    ({dividends}),
                                    ({split_coefficient})
                                )
                """

                try:
                    self.cur.execute(insert_core)
                except self.Error as e:
                    raise FdataError(f"Can't add data to a table 'stock_core': {e}\n\nThe query is\n{insert_core}") from e

    def remove_quotes(self):
        """
            Remove quotes from the database.

            Raises:
                FdataError: sql error happened.
        """
        # Cascade delete will remove the corresponding entries in stock_core table as well
        remove_quotes = f"""DELETE FROM quotes WHERE symbol_id = (SELECT symbol_id FROM symbols WHERE ticker = '{self.symbol}')
                            AND time_stamp >= {self.first_date_ts} AND time_stamp <= {self.last_date_ts};"""

        try:
            self.cur.execute(remove_quotes)
            self.conn.commit()
        except self.Error as e:
            raise FdataError(f"Can't remove quotes from a table 'sources': {e}") from e

        # Check if symbol is removed completely
        if self.get_symbol_quotes_num() == 0:
            self.remove_symbol()

##########################
# Base data fetching class
##########################

class BaseFetchData(ReadWriteData, metaclass=abc.ABCMeta):
    """
        Abstract class to fetch quotes by API wrapper and add them to the database.
    """
    def __init__(self, **kwargs):
        """Initialize the instance of BaseFetchData class."""
        super().__init__(**kwargs)

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
        if self.get_symbol_quotes_num() == 0:
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
        initially_connected = self.Connected

        if self.Connected == False:
            self.db_connect()

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
            self.db_close()

        return (rows, num)

    def get_recent_data(self, to_cache=False):
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
