"""Data abstraction module.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""
# TODO HIGH Fix UT for fdata
from enum import Enum

import abc

from data import fdatabase

from data.fvalues import Timespans, SecType, Currency, def_first_date, def_last_date
from data.futils import get_dt

import settings

# Current database compatibility version
DB_VERSION = 5

class DbTypes(Enum):
    """
        Database types enum. Currently only SQLite is supported.
    """
    SQLite = "sqlite"

# TODO MID Add extended output in case of exception: table_name, query
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

        # Get all security types nominated in all currencies by default
        self.sectype = SecType.All
        self.currency = Currency.All

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
        # TODO LOW Switch to private and create a method to check if db is connected
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
        # TODO LOW Get rid of value here
        if self.db_type == DbTypes.SQLite.value:
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
        # Check if we need to create table 'environment'
        try:
            self.cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='environment';")
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't query table: {e}") from e

        if len(rows) == 0:
            create_environment = """CREATE TABLE environment(
                                    version INTEGER NOT NULL UNIQUE
                                );"""

            try:
                self.cur.execute(create_environment)
            except self.Error as e:
                raise FdataError(f"Can't create table: {e}") from e

        # Check if environment table is empty
        try:
            self.cur.execute("SELECT * FROM environment;")
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't query environment table: {e}") from e

        # Check if environment table has data
        if len(rows) > 1:  # This table should have one row only
            raise FdataError(f"The environment table is broken. Please, delete the database file {settings.Quotes.db_name} or change db patch in settings.py")
        elif len(rows) == 0:
            # Insert the environment data to the table
            insert_environment = f"""INSERT INTO environment (version)
                                    VALUES ({DB_VERSION});"""

            try:
                self.cur.execute(insert_environment)
                self.conn.commit()
            except self.Error as e:
                raise FdataError(f"Can't insert data to a table 'environment': {e}") from e
        else:  # One row present in the table so it is expected
            environment_query = "SELECT version FROM environment;"

            try:
                self.cur.execute(environment_query)
            except self.Error as e:
                raise FdataError(f"Can't query table 'environment': {e}") from e

            version = self.cur.fetchone()[0]

            if version != DB_VERSION:
                raise FdataError(f"DB Version is unexpected. Please, delete the database file {settings.Quotes.db_name} or change db patch in settings.py")

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
                self.conn.commit()
            except self.Error as e:
                raise FdataError(f"Can't insert data to a table 'timespans': {e}") from e

        # Check if we need to create table 'sectypes'
        try:
            self.cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sectypes';")
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't query table: {e}") from e

        if len(rows) == 0:
            create_sectypes = """CREATE TABLE sectypes(
                                    sec_type_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    title TEXT NOT NULL UNIQUE
                                );"""

            try:
                self.cur.execute(create_sectypes)
            except self.Error as e:
                raise FdataError(f"Can't create table: {e}") from e

            # Create index for sectype title
            create_sectype_title_idx = "CREATE INDEX idx_sectype_title ON sectypes(title);"

            try:
                self.cur.execute(create_sectype_title_idx)
            except self.Error as e:
                raise FdataError(f"Can't create index for sectype title: {e}") from e

        # Check if sectypes table is empty
        try:
            self.cur.execute("SELECT * FROM sectypes;")
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't query table: {e}") from e

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
                self.conn.commit()
            except self.Error as e:
                raise FdataError(f"Can't insert data to a table 'sectypes': {e}\n{insert_sectypes}") from e

        # Check if we need to create table 'currency'
        try:
            self.cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='currency';")
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't query table: {e}") from e

        if len(rows) == 0:
            create_currency = """CREATE TABLE currency(
                                    currency_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    title TEXT NOT NULL UNIQUE
                                );"""

            try:
                self.cur.execute(create_currency)
            except self.Error as e:
                raise FdataError(f"Can't create table 'currency': {e}") from e

            # Create index for sectype title
            create_currency_title_idx = "CREATE INDEX idx_currency_title ON currency(title);"

            try:
                self.cur.execute(create_currency_title_idx)
            except self.Error as e:
                raise FdataError(f"Can't create index for currency title: {e}") from e

        # Check if currency table is empty
        try:
            self.cur.execute("SELECT * FROM currency;")
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't query table: {e}") from e

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
                self.conn.commit()
            except self.Error as e:
                raise FdataError(f"Can't insert data to a table 'currency': {e}\n{insert_currency}") from e

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

    def get_quotes(self, num=0, columns=[], joins=[], queries=[]):
        """
            Get quotes for specified symbol, dates and timespan (if any). Additional columns from other tables
            linked by symbol_id may be requested (like fundamental data)

            Args:
                num(int): the number of rows to get. 0 gets all the quotes.
                columns(list of tuple): additional pairs of (table, column) to query.
                joins(list): additional joins to get data from other tables.
                queries(list): additional queries from other tables (like funamental, global economic data).

            Returns:
                list: list with quotes data.

            Raises:
                FdataError: sql error happened.
        """
        # Timespan subquery
        timespan_query = ""

        if self.timespan != Timespans.All:
            timespan_query = "AND timespans.title = '" + self.timespan + "'"

        # Sectype subquery
        sectype_query = ""

        if self.sectype != SecType.All:
            sectype_query = "AND sectypes.title = '" + self.sectype + "'"

        # Currency subquery
        currency_query = ""

        if self.currency != Currency.All:
            currency_query = "AND currency.title = '" + self.currency + "'"

        # Quotes number subquery
        num_query = ""

        if num > 0:
            num_query = f"LIMIT {num}"

        additional_columns = ""

        if len(columns) > 0:
            for column in columns:
                additional_columns += ", " + column

        additional_queries = ""

        if len(queries) > 0:
            # Generate the subqueries for additional data
            for query in queries:
                data_column = query[1]
                table = query[0]

                additional_queries += f""", (SELECT {data_column}
                                                FROM {table}
                                                WHERE reported_date <= time_stamp
                                                AND symbol_id = quotes.symbol_id
                                                ORDER BY reported_date DESC LIMIT 1) AS {data_column}\n"""

        additional_joins = ""

        # Generate the string with additional joins
        for join in joins:
            additional_joins += join + '\n'

        select_quotes = f"""SELECT datetime(time_stamp, 'unixepoch') as time_stamp,
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
                            WHERE symbols.ticker = '{self.symbol}'
                            {timespan_query}
                            {sectype_query}
                            {currency_query}
                            AND time_stamp >= {self.first_date_ts}
                            AND time_stamp <= {self.last_date_ts}
                            ORDER BY time_stamp
                            {num_query};"""

        try:
            self.cur.execute(select_quotes)
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't query table: {e}") from e

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
        # Cascade delete will remove the corresponding entries in stock_core and fundamentals tables as well
        delete_symbol = f"DELETE FROM symbols WHERE symbol_id = (SELECT symbol_id FROM symbols WHERE ticker = '{self.symbol}');"

        try:
            self.cur.execute(delete_symbol)
            self.conn.commit()
        except self.Error as e:
            raise FdataError(f"Can't remove {self.symbol}: {e}") from e

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
        insert_quote = f"""INSERT OR {self._update} INTO quotes (symbol_id,
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
                            (SELECT symbol_id FROM symbols WHERE ticker = '{self.symbol}'),
                            (SELECT source_id FROM sources WHERE title = '{self.source_title}'),
                            ({quote['ts']}),
                            (SELECT time_span_id FROM timespans WHERE title = '{self.timespan}' COLLATE NOCASE),
                            (SELECT sec_type_id FROM sectypes WHERE title = '{quote['sectype']}' COLLATE NOCASE),
                            (SELECT currency_id FROM currency WHERE title = '{quote['currency']}' COLLATE NOCASE),
                            ({quote['open']}),
                            ({quote['high']}),
                            ({quote['low']}),
                            ({quote['adj_close']}),
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
        # Insert new symbols to 'symbols' table (if the symbol does not exist)
        if self.get_symbol_quotes_num() == 0:
            self.add_symbol()

        num_before = self.get_quotes_num()

        for quote in quotes_dict:
            self._add_base_quote_data(quote)

        self.commit()

        num_after = self.get_quotes_num()

        return (num_before, num_after)

    def remove_quotes(self):
        """
            Remove quotes from the database.

            Raises:
                FdataError: sql error happened.
        """
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
class BaseFetcher(ReadWriteData, metaclass=abc.ABCMeta):
    """
        Abstract class to fetch quotes by API wrapper and add them to the database.
    """
    def __init__(self, **kwargs):
        """Initialize the instance of BaseFetcher class."""
        super().__init__(**kwargs)

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
            num_before, num_after = self.add_quotes(self.fetch_quotes())
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
