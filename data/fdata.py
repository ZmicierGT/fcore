"""Data abstraction module.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
import abc

from enum import Enum

from time import sleep, perf_counter

import http.client
import urllib.error
import requests

from data import fdatabase

from data.fvalues import Timespans, SecType, Currency, def_first_date, def_last_date, DbTypes, Timezones
from data.futils import get_dt, get_labelled_ndarray, Log

import settings

from datetime import datetime, timedelta
from dateutil import tz
import calendar

# Current database compatibility version
_DB_VERSION = 32

class DataEntriesEnum(Enum):
    """
        Base class for data-entry enums with intervals/freshness tracking.

        Value format: (title, fresh_days, cadence_days):
            title(str): DB data entry title.
            fresh_days(int): min days between fetches (poll throttle).
            cadence_days(int or None): approx period between data events
                (None = unknown, polling is throttled by fresh_days only).
    """
    def __init__(self, title, fresh_days, cadence_days):
        self._title_ = title
        self._fresh_days_ = fresh_days
        self._cadence_days_ = cadence_days

    @property
    def title(self):
        """DB data entry title."""
        return self._title_

    @property
    def fresh_days(self):
        """Min days between fetches (poll throttle)."""
        return self._fresh_days_

    @property
    def cadence_days(self):
        """Approx period (days) between data events. None = unknown."""
        return self._cadence_days_

class CommonDataEntries(DataEntriesEnum):
    """Data entries registered by the base class (usable by all sources)."""
    SecurityInfo = ('sec_info', 1, 180)  # changes very rarely

class Subquery():
    """
        Class which represents additional subqueries for optional data (fundamentals, global economic, customer data and so on).

        Note that this class is not really sql-injection proof so it should be used internally only -
        meaning not exposing it through web-interface or whatever.
    """
    def __init__(self, table, column, condition='', title=None, fill=True, symbol=None):
        """
            Initializes the instance of Subquery class.

            Args:
                table(str): table for subquery.
                column(str): column to obtain.
                condition(str): additional SQL condition for the subquery.
                title(str): optional title for the output column (the same as column name by default)
                fill(bool): Indicates if all rows should have the value. False if only a row with the most
                            suitable data (according to time stamp) should have it.
                symbol(str): optional ticker which symbol_id to bind the subquery to in addition to the
                             quotes symbol_id. Useful when the target table data is stored under a different
                             ticker of the same security in another data source.
        """
        self._table = table
        self._column = column
        self._condition = condition
        self._fill = fill
        self._symbol = symbol

        # Use the default column name as the title if the title is not specified
        if title is None:
            self._title = column
        else:
            self._title = title

    def generate(self):
        """
            Generates the subquery based on the provided data.

            Returns:
                str: SQL expression for the subquery
        """
        ts_query = ''

        if self._fill is False:
            ts_query = """ AND report_tbl.time_stamp >
                           (SELECT time_stamp FROM quotes qqq WHERE qqq.quote_id > quotes.quote_id ORDER BY qqq.quote_id ASC LIMIT 1)"""

        symbol_query = "symbol_id = quotes.symbol_id"

        if self._symbol is not None:
            symbol_query = f"""(symbol_id = quotes.symbol_id
                               OR symbol_id = (SELECT symbol_id FROM symbols WHERE ticker = '{self._symbol}'))"""

        subquery = f"""(SELECT {self._column}
                            FROM {self._table} report_tbl
                            WHERE report_tbl.time_stamp <= quotes.time_stamp{ts_query}
                            AND {symbol_query}
                            {self._condition}
                            ORDER BY report_tbl.time_stamp DESC LIMIT 1) AS {self._title}\n"""

        return subquery

class FdataError(Exception):
    """
        Base data exception class.
    """

##########################
# Base data fetcher class (pure external API layer)
##########################
class SecFetcher(object, metaclass=abc.ABCMeta):
    """
        Abstract class to fetch quotes by API wrapper and add them to the database.
    """
    def __init__(self, **kwargs):
        """Initialize the instance of SecFetcher class."""
        super().__init__(**kwargs)

        self._max_queries = None # Maximul allowed number of API queries per minute
        self._queries = []  # List of queries to calculate API call pauses

    def _query_api(self, url, timeout=30):
        """
            Check if we need to wait before the next API query, wait if needed and query the API.

            Args:
                url(string): URL to fetch
                timeout(int): timeout for a response

            Returns:
                Response: obtained data

            Raises:
                FdataError: if the request fails (timeout, connection error, HTTP error, etc.).
        """
        # Check if we are about to reach the API key limit for queries
        if len(self._queries) >= self._max_queries:
            # Get the first query time from the array
            first_query_time = self._queries[0]

            # Calculate time to sleep and sleep if needed
            sleep_time = max(0, 60 - (perf_counter() - first_query_time))

            self._lg.highlight(f"Sleeping for {round(sleep_time, 2)} seconds to avoid API key queries limit..")

            sleep(sleep_time)

            self._queries = []

        # Uncomment for debug purposes
        #self._lg.plain(f"Fetching URL: {url}")
        headers = {'Cache-Control': 'no-cache'}

        # Perform the query
        try:
            with requests.Session() as session:
                response = session.get(url, headers=headers, timeout=timeout)
        except (requests.exceptions.RequestException, urllib.error, http.client.HTTPException) as e:
            raise FdataError(f"Can't fetch quotes: {e}") from e
        finally:
            self._queries.append(perf_counter())

        return response

    def _get_request_datetimes(self, first_ts, last_ts, trim_last=False):
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
        first_datetime = first_dt.replace(tzinfo=tz.UTC, hour=12).astimezone(self.timezone).replace(tzinfo=None)
        last_datetime = last_dt.replace(tzinfo=tz.UTC, hour=12).astimezone(self.timezone).replace(tzinfo=None)

        return (first_datetime, last_datetime)

    def _get_request_dates(self, first_ts, last_ts, trim_last=False):
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
        first_dt, last_dt = self._get_request_datetimes(first_ts=first_ts, last_ts=last_ts, trim_last=trim_last)

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
                ndarray: real time data.
        """

    @abc.abstractmethod
    def _fetch_quotes(self, first_ts=None, last_ts=None):
        """
            Abstract method to fetch quotes.

            Args:
                first_ts(int): overridden first ts to fetch.
                last_ts(int): overridden last ts to fetch.

            Returns:
                list(dict): obtained quotes.
        """

    @abc.abstractmethod
    def _get_timespan_str(self):
        """
            Get timespan string (like '5min' and so on) to query a particular data source based on the timespan specified
            in the datasource instance.

            Returns:
                str: timespan string.
        """

    def _fetch_info(self):
        """
            Fetch security info. Default for sources without a dedicated info API.

            Returns Unknown security type, Unknown currency, and America/New_York timezone.
            Concrete sources are expected to override this to at least perform a security
            existence check (returning SecType.NotExist for delisted/non-existent tickers).

            Returns:
                dict: info with fc_sec_type, fc_currency, fc_time_zone keys.
        """
        return {
            'fc_sec_type': SecType.Unknown,
            'fc_currency': Currency.Unknown,
            'fc_time_zone': 'America/New_York',
        }

    # TODO LOW Kept for possible usage with data sources which have API request limits per time interval
    def _query_and_parse(self, url, timeout=30):
        """
            Query the data source and parse the response. Used to handle data source API call limit.

            Args:
                url(str): the url for a request.
                timeout(int): timeout for the request.

            Returns:
                Parsed data.
        """


class SecData(SecFetcher):
    """
        Base class for SQL data operations and database integrity check.
    """
    def __init__(self,
                 symbol="",
                 first_date=def_first_date,
                 last_date=def_last_date,
                 timespan=Timespans.Day,
                 verbosity=False,
                 refetch=False,
                 db_name=None,
                 **kwargs):
        """
            Initialize the base database class.

            Args:
                symbol(str): the symbol to use.
                first_date(datetime, str, int): the first date for queries.
                last_date(datetime, str, int): the last date for queries.
                timespan(Timespans): timespan to use in queries.
                verbosity(bool): indicates if additional outputs are needed (logging and so on).
                refetch(bool): if True, bypass interval-based gating and re-fetch data even when
                               intervals already cover the requested range; existing rows are updated in place.
                db_name(str): database name. Defaults to settings.Quotes.db_name.
        """
        # Setting the default values
        self._symbol = symbol

        # Underlying variables for getters/setter
        self._first_date = None
        self._last_date = None

        # Getter/setter will be invoked
        self.first_date = first_date
        self.last_date = last_date

        if self.first_date > self.last_date:
            raise FdataError(f"First date can't be bigger than the last date: {self.first_date} > {self.last_date}")

        self._timespan = timespan

        # Source title should be overridden in derived classes for particular data sources
        self._source_title = ''

        # Default setting for the base data source
        self._db_type = settings.Quotes.db_type
        self._db_name = db_name if db_name is not None else settings.Quotes.db_name

        self._database = None
        self._conn = None
        self._cur = None

        # Type of exception for db queries
        self._error = None

        # Flag which indicates if the database is connected
        self._connected = False

        # Indicates if the database schema and source have been initialized for this instance
        self._db_initialized = False

        self._verbosity = verbosity

        self._lg = Log(verbosity=verbosity)

        self._refetch = refetch

        self._time_zone = None  # Cached time zone to avoid too many db queries
        self._sec_type = None  # Cached security type to avoid too many db queries
        self._currency = None  # Cached security type to avoid too many db queries

        # Cached security info
        self._info = None

        # Cooperative MI: forward any remaining kwargs down the MRO.
        super().__init__(**kwargs)

    @property
    def timespan(self):
        """
            Getter for the timespan used in queries (read-only).

            Returns:
                Timespans: the timespan to use in queries.
        """
        return self._timespan

    @property
    def refetch(self):
        """
            Getter for refetch flag (read-only).

            Returns:
                bool: True if this instance was initialized with refetch=True,
                    bypassing interval-based gating to force re-fetch.
        """
        return self._refetch

    def get(self, num=0, columns=[], joins=None, queries=None, ignore_last_date=False):
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
        initially_connected = self.is_connected

        if self.is_connected is False:
            self.db_connect()

        try:
            # Detect delisted/non-existent tickers before any quote fetch. Fetches/persists
            # sec_info once and raises FdataError here if the symbol is NotExist, so we never
            # waste a quote fetch or falsely mark intervals for a non-existent ticker.
            self.get_info()

            total_num = self.get_quotes_num(dt=False)

            last_ts_adj = min(self.last_date_ts, self._current_ts())

            if self._need_to_update():
                intervals = []

                min_request_ts = self._get_min_request_ts()
                max_request_ts = self._get_max_request_ts()

                if self._refetch:
                    intervals.append([self.first_date_ts, last_ts_adj])
                elif total_num and min_request_ts is not None and max_request_ts is not None:
                    # Fetch the requested range excluding the part already covered by recorded
                    # intervals. Two independent checks: a request can extend on
                    # one side, both sides, or touch the recorded boundary exactly.
                    if self.first_date_ts < min_request_ts:
                        intervals.append([self.first_date_ts, min_request_ts])

                    if last_ts_adj > max_request_ts:
                        intervals.append([max_request_ts, last_ts_adj])
                else:
                    intervals.append([self.first_date_ts, last_ts_adj])

                for first_ts, last_ts in intervals:
                    self._lg.plain(f"Fetching contiguous data for {self._symbol} from {get_dt(first_ts)} to {get_dt(last_ts)}...")

                    self._add_quotes(self._fetch_quotes(first_ts=first_ts, last_ts=last_ts))

                # Mark the fetched range. Runs even when a sub-interval returned zero quotes
                # (e.g. a valid symbol with no quotes in that range) so we don't re-fetch
                # known-empty ranges. A fetch failure (e.g. connection error) raises before
                # reaching here, so intervals are not updated and the range is retried next time.
                self._update_data_interval()

            rows = self._get_quotes(num=num, columns=columns, joins=joins, queries=queries, ignore_last_date=ignore_last_date)
        finally:
            if initially_connected is False:
                self.db_close()

        # TODO MID Think if we should return None here without raising an exception
        if rows is None:
            raise FdataError(f"No quotes for ticker {self._symbol}")

        return rows

    ########################################################
    # Get/set datetimes (depending on the input value type).
    ########################################################
    @property
    def source_title(self):
        """
            Get the source title (read-only).

            Concrete data sources must assign self._source_title in their
            own __init__ before any database operation is attempted.

            Returns:
                str: the title identifying the data source.
        """
        return self._source_title

    @property
    def first_date(self):
        """
            Get the first datetime.

            Returns:
                datetime: the first datetime.
        """
        return self._first_date

    # TODO MID Do we need these setters or dates should be only set on instance creation?
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

    def _set_eod_time(self, dt):
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
    def _first_date_set_eod(self):
        """
            Set the first date's h/m/s/ to EOD (23:59:59)
        """
        self._first_date = self._set_eod_time(self._first_date)

    def _last_date_set_eod(self):
        """
            Set the last date's h/m/s/ to EOD (23:59:59)
        """
        self._last_date = self._set_eod_time(self._last_date)

    ##############################################
    # End of datetime handling methods/properties.
    ##############################################

    @property
    def db_type(self):
        """
            Get used database type.

            Returns:
                DbTypes: database type.
        """
        return self._db_type

    @property
    def is_connected(self):
        """Returns True/False if db is connected."""
        return self._connected

    def _check_if_connected(self):
        """
            Raise an exception if db is not connected.
        """
        if self.is_connected is False:
            raise FdataError("The database is not connected. Invoke db_connect() at first.")

    # TODO MID We may switch auto connect logic to use a decorator:
    # Introduce a small private @_auto_connect decorator in fdata.py and apply it to ALL public DB-reading
    def db_connect(self):
        """
            Connect to the database.

            If the connection is already open, a warning is logged and the call is a no-op.
        """
        if self.is_connected:
            self._lg.warning("db_connect() is invoked while the database is already connected. The call is skipped.")
            return

        if self._db_type == DbTypes.SQLite:
            self._database = fdatabase.SQLiteConn(self._db_name)
            self._database.db_connect()

            self._conn = self._database.conn
            self._cur = self._database.cur
            self._error = self._database.error

            self._connected = True

            # Check the database integrity and register the source only once per instance
            if self._db_initialized is False:
                # Run schema bootstrap + source registration as a single transaction.
                try:
                    self._cur.execute("BEGIN IMMEDIATE;")

                    self._check_database()

                    # Register the source idempotently. INSERT OR IGNORE makes
                    # it race-safe; inserted as the last step of the init tx,
                    # so its presence certifies the source's tables exist too.
                    self._add_source()

                    self._conn.commit()
                except Exception:
                    self._conn.rollback()
                    raise

                self._db_initialized = True

    def db_close(self):
        """
            Close the database connection.
        """
        self._check_if_connected()

        self._database.db_close()
        self._connected = False

        self._conn = None
        self._cur = None
        self._error = None

    def _table_exists(self, table):
        """
            Check if a table exists in the database.

        Args:
            table(str): the table name.

        Returns:
            bool: True if the table exists, False otherwise.

        Raises:
            FdataError: sql error happened.
        """
        check_query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}';"

        try:
            self._cur.execute(check_query)
            rows = self._cur.fetchall()
        except self._error as e:
            raise FdataError(f"Can't execute a query on a table '{table}': {e}\n{check_query}") from e

        return len(rows) > 0

    def _populate_lookup(self, table, values):
        """
            Populate a lookup table with the given titles if not yet fully populated.

            Existing entries are kept (INSERT OR IGNORE). Does not commit — the caller
            is expected to commit once the surrounding initialization is complete.

        Args:
            table(str): the lookup table name.
            values(list of str): the titles to insert.

        Raises:
            FdataError: sql error happened.
        """
        count = self._get_data_num(table, symbol=False, source=False)

        if count < len(values):
            insert_query = f"INSERT OR IGNORE INTO {table} (title) VALUES (?);"

            try:
                self._cur.executemany(insert_query, [(v,) for v in values])
            except self._error as e:
                raise FdataError(f"Can't insert data to a table '{table}': {e}\n{insert_query}") from e

    def _register_data_entries(self, entries_enum):
        """
            Register dataset entries in the data_entries lookup table and verify
            that each entry has a corresponding table.

            Args:
                entries_enum(DataEntriesEnum class): enum whose members' titles are the table names to track.

            Raises:
                FdataError: a table for an entry does not exist or sql error happened.
        """
        for entry in entries_enum:
            check_query = "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?;"

            try:
                self._cur.execute(check_query, (entry.title,))
                row = self._cur.fetchone()
            except self._error as e:
                raise FdataError(f"Can't check a table '{entry.title}': {e}\n{check_query}") from e

            if row is None:
                raise FdataError(f"{type(self).__name__} must create a table '{entry.title}' in "
                                 f"_check_database() before registering {entries_enum.__name__}.{entry.name}")

        insert_query = "INSERT OR IGNORE INTO data_entries (title) VALUES (?);"

        try:
            self._cur.executemany(insert_query, [(e.title,) for e in entries_enum])
        except self._error as e:
            raise FdataError(f"Can't insert data to a table 'data_entries': {e}\n{insert_query}") from e

    def _check_database(self):
        """
            Database create/integrity check method.
            Checks if the database exists. Otherwise, create it. Checks if the database has required tables/data.

            All DDL is idempotent (CREATE ... IF NOT EXISTS) and all lookup
            population uses INSERT OR IGNORE, so this method is safe to run
            concurrently from multiple connections/processes.

            Required invariants for subclasses overriding this method:
                1. super()._check_database() MUST be called first in every
                   override, so common tables exist before source-specific
                   ones are created.
                2. Source-specific tables may only have FOREIGN KEYs to
                   base/common tables.
                3. No mid-method commits.
                4. Tables that need intervals tracking must have a matching
                   member in the class's entries enum (see _register_data_entries)
                   and be registered before this method returns. sec_info and
                   stock_info are tracked as well (CommonDataEntries.SecurityInfo).

            Raises:
                FdataError: sql error happened.
        """
        self._check_if_connected()

        # Create the environment table if needed
        create_environment = """CREATE TABLE IF NOT EXISTS environment(
                                version INTEGER NOT NULL UNIQUE
                            );"""

        try:
            self._cur.execute(create_environment)
        except self._error as e:
            raise FdataError(f"Can't execute a query on a table 'environment': {e}\n{create_environment}") from e

        # Idempotently seed the version row. INSERT OR IGNORE makes this race-safe.
        insert_environment = "INSERT OR IGNORE INTO environment (version) VALUES (?);"

        try:
            self._cur.execute(insert_environment, (_DB_VERSION,))
        except self._error as e:
            raise FdataError(f"Can't execute a query on a table 'environment': {e}\n{insert_environment}") from e

        try:
            self._cur.execute("SELECT version FROM environment;")
            env_rows = self._cur.fetchall()
        except self._error as e:
            raise FdataError(f"Can't execute a query on a table 'environment': {e}\nSELECT version FROM environment;") from e

        if len(env_rows) != 1:
            raise FdataError(f"The environment table is broken. Please, delete the database file {self._db_name} or change db path in settings.py")

        if env_rows[0][0] != _DB_VERSION:
            raise FdataError(f"DB Version is unexpected. Please, delete the database file {self._db_name} or change db path in settings.py")

        # Create table 'currency' if needed
        create_currency = """CREATE TABLE IF NOT EXISTS currency(
                                currency_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                title TEXT NOT NULL UNIQUE
                            );"""

        try:
            self._cur.execute(create_currency)
        except self._error as e:
            raise FdataError(f"Can't execute a query on a table 'currency': {e}\n{create_currency}") from e

        # Create index for sectype title
        create_currency_title_idx = "CREATE INDEX IF NOT EXISTS idx_currency_title ON currency(title);"

        try:
            self._cur.execute(create_currency_title_idx)
        except self._error as e:
            raise FdataError(f"Can't create index for currency(title): {e}") from e

        # Populate currency table if not yet fully populated
        self._populate_lookup('currency', [c for c in Currency if c != Currency.All])

        # Create table 'sectypes' if needed
        create_sectypes = """CREATE TABLE IF NOT EXISTS sectypes(
                                sec_type_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                title TEXT NOT NULL UNIQUE
                            );"""

        try:
            self._cur.execute(create_sectypes)
        except self._error as e:
            raise FdataError(f"Can't execute a query on a table 'sectypes': {e}\n{create_sectypes}") from e

        # Create index for sectype title
        create_sectype_title_idx = "CREATE INDEX IF NOT EXISTS idx_sectype_title ON sectypes(title);"

        try:
            self._cur.execute(create_sectype_title_idx)
        except self._error as e:
            raise FdataError(f"Can't create index for sectypes(title): {e}") from e

        # Populate sectypes table if not yet fully populated
        self._populate_lookup('sectypes', [s for s in SecType if s != SecType.All])

        # Create table 'symbols' if needed
        create_symbols = """CREATE TABLE IF NOT EXISTS symbols(
                            symbol_id INTEGER PRIMARY KEY AUTOINCREMENT,
                            ticker TEXT NOT NULL UNIQUE,
                            isin TEXT UNIQUE,
                            description TEXT
                            );"""

        try:
            self._cur.execute(create_symbols)
        except self._error as e:
            raise FdataError(f"Can't execute a query on a table 'symbols': {e}\n{create_symbols}") from e

        # Create index for ticker
        create_ticker_idx = "CREATE INDEX IF NOT EXISTS idx_ticker ON symbols(ticker);"

        try:
            self._cur.execute(create_ticker_idx)
        except self._error as e:
            raise FdataError(f"Can't create index for symbols(ticker): {e}") from e

        # Create table 'sources' if needed
        create_sources = """CREATE TABLE IF NOT EXISTS sources(
                            source_id INTEGER PRIMARY KEY AUTOINCREMENT,
                            title TEXT NOT NULL UNIQUE,
                            description TEXT
                            );"""

        try:
            self._cur.execute(create_sources)
        except self._error as e:
            raise FdataError(f"Can't execute a query on a table 'sources': {e}\n{create_sources}") from e

        # Create index for source title
        create_source_title_idx = "CREATE INDEX IF NOT EXISTS idx_source_title ON sources(title);"

        try:
            self._cur.execute(create_source_title_idx)
        except self._error as e:
            raise FdataError(f"Can't create index for sources(title): {e}") from e

        # Create table 'timespans' if needed
        create_timespans = """CREATE TABLE IF NOT EXISTS timespans(
                                time_span_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                title TEXT NOT NULL UNIQUE
                            );"""

        try:
            self._cur.execute(create_timespans)
        except self._error as e:
            raise FdataError(f"Can't execute a query on a table 'timespans': {e}\n{create_timespans}") from e

        # Create index for timespan title
        create_timespan_title_idx = "CREATE INDEX IF NOT EXISTS idx_timespan_title ON timespans(title);"

        try:
            self._cur.execute(create_timespan_title_idx)
        except self._error as e:
            raise FdataError(f"Can't create index for timespans(title): {e}") from e

        # Populate timespans table if not yet fully populated
        self._populate_lookup('timespans', [t for t in Timespans if t != Timespans.All])

        # Create table 'data_entries' if needed
        create_data_entries = """CREATE TABLE IF NOT EXISTS data_entries(
                                    data_entry_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    title TEXT NOT NULL UNIQUE
                                );"""

        try:
            self._cur.execute(create_data_entries)
        except self._error as e:
            raise FdataError(f"Can't execute a query on a table 'data_entries': {e}\n{create_data_entries}") from e

        # Create index for data_entries title
        create_data_entries_idx = "CREATE INDEX IF NOT EXISTS idx_data_entries_title ON data_entries(title);"

        try:
            self._cur.execute(create_data_entries_idx)
        except self._error as e:
            raise FdataError(f"Can't create index for data_entries(title): {e}") from e

        # Populate data_entries table with Timespans (excluding All/Unknown) - needed for interval tracking for each timespan.
        entries = [e for e in Timespans if e not in (Timespans.All, Timespans.Unknown)]
        self._populate_lookup('data_entries', entries)

        # Create table 'data_intervals' if needed
        create_data_intervals = """CREATE TABLE IF NOT EXISTS data_intervals (
                                        interval_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                        symbol_id INTEGER NOT NULL,
                                        source_id INTEGER NOT NULL,
                                        data_entry_id INTEGER NOT NULL,
                                        min_ts INTEGER,
                                        max_ts INTEGER,
                                        modified_ts INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                                            CONSTRAINT fk_data_entries
                                                FOREIGN KEY (data_entry_id)
                                                REFERENCES data_entries(data_entry_id)
                                                ON DELETE CASCADE
                                            CONSTRAINT fk_source
                                                FOREIGN KEY (source_id)
                                                REFERENCES sources(source_id)
                                                ON DELETE CASCADE
                                            CONSTRAINT fk_symbols
                                                FOREIGN KEY (symbol_id)
                                                REFERENCES symbols(symbol_id)
                                                ON DELETE CASCADE
                                        UNIQUE(symbol_id, source_id, data_entry_id)
                                        );"""

        try:
            self._cur.execute(create_data_intervals)
        except self._error as e:
            raise FdataError(f"Can't create table data_intervals: {e}") from e

        # Create indexes for data_intervals
        create_data_intervals_idx = "CREATE INDEX IF NOT EXISTS idx_data_intervals ON data_intervals(symbol_id, source_id, data_entry_id);"

        try:
            self._cur.execute(create_data_intervals_idx)
        except self._error as e:
            raise FdataError(f"Can't create indexes for data_intervals table: {e}") from e

        # Create trigger to last modified time on data_intervals
        create_intervals_trigger = """CREATE TRIGGER IF NOT EXISTS update_data_intervals
                                            BEFORE UPDATE
                                                ON data_intervals
                                    BEGIN
                                        UPDATE data_intervals
                                        SET modified_ts = strftime('%s', 'now')
                                        WHERE interval_id = old.interval_id;
                                    END;"""

        try:
            self._cur.execute(create_intervals_trigger)
        except self._error as e:
            raise FdataError(f"Can't create trigger for data_intervals: {e}") from e

        # TODO Mid need to think of a better way how to combine data from various sources
        # Create table 'quotes' if needed
        create_quotes = """CREATE TABLE IF NOT EXISTS quotes (
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
            self._cur.execute(create_quotes)
        except self._error as e:
            raise FdataError(f"Can't create table quotes: {e}") from e

        # Create indexes for quotes
        # TODO LOW Think if index for source_id should be added (covering index-only scans in get_quotes_num).
        create_quotes_idx = "CREATE INDEX IF NOT EXISTS idx_quotes ON quotes(symbol_id, time_stamp, time_span_id, source_id);"

        try:
            self._cur.execute(create_quotes_idx)
        except self._error as e:
            raise FdataError(f"Can't create indexes for quotes table: {e}") from e

        # Create table 'sec_info' if needed
        create_sec_info = """CREATE TABLE IF NOT EXISTS sec_info (
                                            sec_info_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                            symbol_id INTEGER NOT NULL,
                                            source_id INTEGER NOT NULL,
                                            time_zone TEXT NOT NULL,
                                            sec_type_id INTEGER NOT NULL,
                                            currency_id INTEGER NOT NULL,
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
            self._cur.execute(create_sec_info)
        except self._error as e:
            raise FdataError(f"Can't create table sec_info: {e}") from e

        # Create indexes for sec_info
        create_sec_info_idx_symbol = "CREATE INDEX IF NOT EXISTS idx_sec_info ON sec_info(symbol_id, source_id);"

        try:
            self._cur.execute(create_sec_info_idx_symbol)
        except self._error as e:
            raise FdataError(f"Can't create indexes for sec_info table: {e}") from e

        # Register base data entries (e.g. sec_info) for intervals tracking
        self._register_data_entries(CommonDataEntries)

    def _check_source(self):
        """
            Check if the current source exists in the table 'sources'

            Returns:
                int: the number of rows in 'sources' table.

            Raises:
                FdataError: sql error happened.
        """
        self._check_if_connected()

        source_exists = "SELECT title FROM sources WHERE title = ?;"

        try:
            self._cur.execute(source_exists, (self._source_title,))
            rows = self._cur.fetchall()
        except self._error as e:
            raise FdataError(f"Can't execute a query on a table 'sources': {e}\n{source_exists}") from e

        # Check if sources table has the required row
        return len(rows)

    def _add_source(self):
        """
            Add source to the database.

            Note: does not commit. The caller (db_connect) commits once the
            surrounding init transaction is complete.

            Raises:
                FdataError: sql error happened.
        """
        self._check_if_connected()

        insert_source = "INSERT OR IGNORE INTO sources (title) VALUES (?);"

        try:
            self._cur.execute(insert_source, (self._source_title,))
        except self._error as e:
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
        initially_connected = self.is_connected

        if self.is_connected is False:
            self.db_connect()

        try:
            get_all_symbols = "SELECT ticker, isin, description FROM symbols;"

            self._cur.execute(get_all_symbols)
            rows = self._cur.fetchall()
        except self._error as e:
            raise FdataError(f"Can't execute a query on a table 'symbols': {e}\n{get_all_symbols}") from e
        finally:
            if initially_connected is False:
                self.db_close()

        return rows

    # TODO MID Likely we do not need ignore_source and its usage in some cases may be unsafe
    def _get_quotes(self, num=0, columns=[], joins=None, queries=None, ignore_last_date=False, ignore_source=False):
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
        self._check_if_connected()

        # Timespan subquery
        timespan_query = ""

        if self._timespan != Timespans.All:
            timespan_query = "AND timespans.title = '" + self._timespan + "'"

        # TODO LOW Think what to do with sectype and currency. Ignore it for now.
        # # Sectype subquery
        # sectype_query = ""

        # if self.sectype != SecType.All:
        #     sectype_query = "AND sectypes.title = '" + self.sectype + "'"

        # # Currency subquery
        # currency_query = ""

        # if self.currency != Currency.All:
        #     currency_query = "AND currency.title = '" + self.currency + "'"

        # Quotes number subquery
        num_query = ""
        select_params = []

        if num > 0:
            num_query = "LIMIT ?"
            select_params.append(num)

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

        last_date_ts = calendar.timegm(self._set_eod_time(self.last_date).utctimetuple())

        if ignore_last_date:
            last_date_ts = def_last_date

        source_query = ''
        source_param = None

        if ignore_source is False:
            source_query = "AND source_id = (SELECT source_id FROM sources WHERE title = ?)"
            source_param = self._source_title

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
        #                     WHERE symbols.ticker = '{self._symbol}'
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
                            WHERE symbols.ticker = ?
                            {timespan_query}
                            AND time_stamp >= ?
                            AND time_stamp <= ?
                            {source_query}
                            ORDER BY time_stamp
                            {num_query};"""

        select_params = [self._symbol, self.first_date_ts, last_date_ts] + ([source_param] if source_param is not None else []) + select_params

        try:
            self._cur.execute(select_quotes, select_params)
            rows = self._cur.fetchall()
        except self._error as e:
            raise FdataError(f"Can't execute a query on a table 'quotes': {e}\n{select_quotes}") from e

        if len(rows) == 0:
            self._lg.warning("No data obtained.")
            return None

        return get_labelled_ndarray(rows)

    # TODO LOW Querying COUNT(*) may impact performance. In the future a faster approach should be used.
    def get_quotes_num(self, symbol=True, source=True, timespan=True, dt=True):
        """
            Get the number of quotes in the database.

            Args:
                symbol(bool): filter by the symbol configured on the instance.
                source(bool): filter by the source configured on the instance.
                timespan(bool): filter by the timespan configured on the instance (applies to 'quotes' table only).
                dt(bool): filter by the date range configured on the instance (applies to 'quotes' table only).

            Returns:
                int: the number of quotes matching the filters.

            Raises:
                FdataError: sql error happened.
        """
        initially_connected = self.is_connected

        if self.is_connected is False:
            self.db_connect()

        try:
            return self._get_data_num('quotes', symbol=symbol, source=source, timespan=timespan, dt=dt)
        finally:
            if initially_connected is False:
                self.db_close()

    def _get_data_num(self, table, symbol=True, source=True, timespan=True, dt=False):
        """Get the number of entries for the symbol in the specified table.

            Note that this method is not really sql-injection proof so it should be used internally only -
            meaning not exposing it through web-interface or whatever.

            Args:
                table(string): the table to query.
                symbol(bool): filter by the symbol configured on the instance.
                source(bool): filter by the source configured on the instance.
                timespan(bool): filter by the timespan configured on the instance (applies to 'quotes' table only).
                dt(bool): filter by the date range configured on the instance (applies to 'quotes' table only).

            Returns:
                int: the number of entries in the specified table.

            Raises:
                FdataError: sql error happened.
        """
        self._check_if_connected()

        conditions = []

        if symbol:
            conditions.append(f"symbol_id = (SELECT symbol_id FROM symbols WHERE ticker = '{self._symbol}')")

        if source:
            conditions.append(f"source_id = (SELECT source_id FROM sources WHERE title = '{self._source_title}')")

        # timespan and dt filters are applicable to the 'quotes' table only.
        if table == 'quotes':
            if timespan:
                conditions.append(f"time_span_id = (SELECT time_span_id FROM timespans WHERE title = '{self._timespan}')")

            if dt:
                last_date_ts = calendar.timegm(self._set_eod_time(self.last_date).utctimetuple())
                conditions.append(f"time_stamp >= {self.first_date_ts} AND time_stamp <= {last_date_ts}")

        where = ""

        if conditions:
            where = f"WHERE {' AND '.join(conditions)}"

        get_num = f"SELECT COUNT(*) FROM {table} {where};"

        try:
            self._cur.execute(get_num)
        except self._error as e:
            raise FdataError(f"Can't query table '{table}': {e}\n\nThe query is\n{get_num}") from e

        result = self._cur.fetchone()[0]

        if result is None:
            result = 0

        return result

    def _get_ts(self, is_max=True, table='quotes', column='time_stamp'):
        """
            Get Min/Max timestamp for a particular symbol, source (and timespan when
            the table has a time_span_id column) from the specified table.

            Note that this method is not really sql-injection proof so it should be used internally only -
            meaning not exposing it through web-interface or whatever.

            Args:
                is_max(bool): indicates if Min or Max timestamp should be obtained.
                table(str): table to request.
                column(str): column to request.

            Returns:
                int: timestamp of min/max timestamp (None if the column does not
                     exist in the table or no rows are present).

            Raises:
                FdataError: sql error happened.
        """
        self._check_if_connected()

        try:
            self._cur.execute(f"PRAGMA table_info({table})")
            columns = [row[1] for row in self._cur.fetchall()]
        except self._error as e:
            raise FdataError(f"Can't query table '{table}': {e}") from e

        if column not in columns:
            return None

        minmax = 'MIN'

        if is_max:
            minmax = 'MAX'

        join_timespans = ''
        timespan_clause = ''

        if table == 'quotes':
            join_timespans = f"INNER JOIN timespans on {table}.time_span_id = timespans.time_span_id"
            timespan_clause = f"AND timespans.title = '{self._timespan}'"

        timestamp_query = f"""SELECT {minmax}({column}) FROM {table}
                                    INNER JOIN symbols ON {table}.symbol_id = symbols.symbol_id
                                    INNER JOIN sources on {table}.source_id = sources.source_id
                                    {join_timespans}
                                    WHERE symbols.ticker = '{self._symbol}'
                                    AND sources.title = '{self._source_title}'
                                    {timespan_clause};"""

        try:
            self._cur.execute(timestamp_query)
        except self._error as e:
            raise FdataError(f"Can't query table '{table}': {e}\n\nThe query is\n{timestamp_query}") from e

        return self._cur.fetchone()[0]

    def _get_interval_ts(self, data_entry, is_max=True):
        """
            Get Min/Max timestamp for a particular symbol, source and data entry
            (a Timespans value for quote intervals or a dataset table title)
            from the 'data_intervals' table.

            Note that this method is not really sql-injection proof so it should be used internally only -
            meaning not exposing it through web-interface or whatever.

            Args:
                data_entry(str): data entry title.
                is_max(bool): indicates if Min or Max timestamp should be obtained.

            Returns:
                int: timestamp of min/max timestamp (None if no row present).

            Raises:
                FdataError: sql error happened.
        """
        column = 'max_ts'

        if is_max is False:
            column = 'min_ts'

        self._check_if_connected()

        timestamp_query = f"""SELECT {('MAX' if is_max else 'MIN')}(di.{column}) FROM data_intervals di
                                    INNER JOIN symbols ON di.symbol_id = symbols.symbol_id
                                    INNER JOIN sources on di.source_id = sources.source_id
                                    INNER JOIN data_entries on di.data_entry_id = data_entries.data_entry_id
                                    WHERE symbols.ticker = '{self._symbol}'
                                    AND sources.title = '{self._source_title}'
                                    AND data_entries.title = '{data_entry}';"""

        try:
            self._cur.execute(timestamp_query)
        except self._error as e:
            raise FdataError(f"Can't execute a query on a table 'data_intervals': {e}\n{timestamp_query}") from e

        return self._cur.fetchone()[0]

    def _get_min_request_ts(self):
        """
            Get the earliest request timestamp to obtain quotes for a particular symbol,
            timespan, source.

            Return:
                int: the earliest request timestamp.
        """
        self._check_if_connected()

        return self._get_interval_ts(self._timespan, is_max=False)

    def _get_max_request_ts(self):
        """
            Get the earliest request timestamp to obtain quotes for a particular symbol,
            timespan, source.

            Return:
                int: the earliest request timestamp.
        """
        self._check_if_connected()

        return self._get_interval_ts(self._timespan, is_max=True)

    def _get_modified_ts(self, entry=None):
        """
            Get the last modification timestamp of the data_intervals row for the
            given entry - i.e. when the interval record itself was last written.

            Args:
                entry(str or None): the data entry/timespan title to check.
                    None resolves to the current timespan.

            Returns:
                int: modification timestamp, None if no interval record exists
                     (covers unregistered entries as well).

            Raises:
                FdataError: sql error happened.
        """
        self._check_if_connected()

        interval_title = self._timespan if entry is None else entry

        modified_query = """SELECT di.modified_ts FROM data_intervals di
                                INNER JOIN data_entries ON di.data_entry_id = data_entries.data_entry_id
                                INNER JOIN symbols ON di.symbol_id = symbols.symbol_id
                                INNER JOIN sources ON di.source_id = sources.source_id
                                WHERE data_entries.title = ?
                                AND symbols.ticker = ?
                                AND sources.title = ?;"""

        try:
            self._cur.execute(modified_query, (interval_title, self._symbol, self._source_title))
            row = self._cur.fetchone()
        except self._error as e:
            raise FdataError(f"Can't query data_intervals for '{interval_title}': {e}\n{modified_query}") from e

        if row is None:
            return None

        return row[0]

    def _need_to_update(self, data_entry=None):
        """
            Check if we need to update data based on the last fetch marker.

            Args:
                data_entry(DataEntriesEnum or None): the data entry to check if we need to update it. None for quotes.

            Returns:
                bool: indicates if update is needed.
        """
        if self._refetch:
            return True

        self._check_if_connected()

        last_ts_adj = min(self.last_date_ts, self._current_ts())

        if data_entry is None:  # Quotes path
            title = self._timespan
            min_ts = self._get_interval_ts(title, is_max=False)
            max_ts = self._get_interval_ts(title, is_max=True)

            return (min_ts is None or max_ts is None or self.first_date_ts < min_ts or last_ts_adj > max_ts)

        # Entries path
        title = data_entry.title
        max_ts = self._get_interval_ts(title)

        if max_ts is None:
            return True

        if last_ts_adj <= max_ts:
            return False

        # If the approximate data cadence is known, skip fetching until the next
        # data event is due (derived from the stored data itself if the table has
        # a time_stamp column, otherwise from the fetch marker).
        if data_entry.cadence_days is not None:
            base_ts = self._get_max_ts(title) or max_ts

            if last_ts_adj <= base_ts + data_entry.cadence_days * 86400:
                return False

        # Poll, but not more often than fresh_days
        return last_ts_adj - max_ts > data_entry.fresh_days * 86400

    def _get_max_ts(self, table='quotes'):
        """
            Get maximum timestamp for a particular symbol, source (timespan) from
            the specified table.

            Args:
                table(str): table to request.

            Returns:
                int: timestamp of a maximum timestamp (None if the table has no
                     time_stamp column or no rows are present).
        """
        initially_connected = self.is_connected

        if self.is_connected is False:
            self.db_connect()

        try:
            return self._get_ts(is_max=True, table=table)
        finally:
            if initially_connected is False:
                self.db_close()

    def get_min_ts(self):
        """
            Get minimum timestamp for a particular symbol, source, timespan.

            Returns:
                int: timestamp of a minimum timestamp.
        """
        initially_connected = self.is_connected

        if self.is_connected is False:
            self.db_connect()

        try:
            return self._get_ts(is_max=False)
        finally:
            if initially_connected is False:
                self.db_close()

    def get_info(self):
        """
            Fetch (if needed) and return security info data.
        """
        # Use the cached value (if any)
        if self._info is None or self._refetch:
            initially_connected = self.is_connected

            if self.is_connected is False:
                self.db_connect()

            # Fetch data if the interval marker is missing/stale
            if self._need_to_update(CommonDataEntries.SecurityInfo):
                self._add_info(self._fetch_info())
                self._update_data_interval(CommonDataEntries.SecurityInfo.title)

            # Just time zone is used from info for now
            info_query = """SELECT time_zone, s.title as sec_type, c.title as curr FROM sec_info si
                                INNER JOIN sectypes s ON si.sec_type_id = s.sec_type_id
                                INNER JOIN currency c ON si.currency_id = c.currency_id
                                WHERE symbol_id = (SELECT symbol_id FROM symbols WHERE ticker = ?)"""

            try:
                self._cur.execute(info_query, (self._symbol,))
                rows = self._cur.fetchall()
            except self._error as e:
                raise FdataError(f"Can't execute a query on a table 'sec_info': {e}\n{info_query}") from e
            finally:
                if initially_connected is False:
                    self.db_close()

            self._info = rows[0]

        # TODO MID Think if exception here is rational or better to return the corresponding dict (with NotExist sec_type)
        if self._info['sec_type'] == SecType.NotExist:
            raise FdataError(f"Ticker {self._symbol} is likely delisted or incorrect as it is marked as not-existent.")

        return {'time_zone': self._info['time_zone'], 'sec_type': self._info['sec_type'], 'currency': self._info['curr']}

    @property
    def timezone(self):
        """
            Get the time zone of the specified symbol.

            Returns:
                tz: time zone.
        """
        if self._time_zone is None:
            info = self.get_info()

            if info is not None and len(info.keys()) and 'time_zone' in info.keys():
                timezone = tz.gettz(info['time_zone'])

                if timezone is None:
                    self._time_zone = tz.gettz(Timezones[info['time_zone']])
                else:
                    self._time_zone = timezone
            else:
                self._lg.warning("Time zone data is not found. Returning ET.")
                self._time_zone = tz.gettz('America/New_York')

        return self._time_zone

    @property
    def sectype(self):
        """
            Get the security type of the specified symbol.

            Returns:
                (SecType): security typy.
        """
        if self._sec_type is None:
            info = self.get_info()

            if info is not None and len(info.keys()) and 'sec_type' in info.keys():
                self._sec_type = info['sec_type']
            else:
                self._sec_type = SecType.Unknown

                self._lg.warning(f"Security type data is not found. Returning {self._sec_type}.")

        return self._sec_type

    # TODO LOW Note that Unknown will be returned each time as currencies are not supported yet.
    @property
    def currency(self):
        """
            Get the currency of the specified symbol.

            Returns:
                (Currency): security typy.
        """
        if self._currency is None:
            info = self.get_info()

            if info is not None and len(info.keys()) and 'currency' in info.keys():
                self._currency = info['currency']
            else:
                self._currency = Currency.Unknown

                self._lg.warning(f"Currency data not found. Returning {self._currency}.")

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
            timespan = self._timespan

        return timespan != Timespans.Day

    def _current_ts(self, adjusted=False, timespan=None):
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
            timespan = self._timespan

        if adjusted:
            if self.is_intraday(timespan) is False:
                now = self._set_eod_time(now)
            elif timespan == Timespans.Minute:
                now += timedelta(minutes=1)
            elif timespan == Timespans.TwoMinutes:
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

        ts = calendar.timegm(now.utctimetuple())

        return ts

    def _commit(self):
        """
            Commit the change to the database.

            Raises:
                FdataError: sql error happened.
        """
        self._check_if_connected()

        try:
            self._conn.commit()
        except self._error as e:
            raise FdataError(f"Can't commit: {e}") from e

    @property
    def symbol_exists(self):
        """
            Return True if the current symbol is present in the 'symbols' table.

            Returns:
                bool: True if the symbol exists, False otherwise.

            Raises:
                FdataError: sql error happened.
        """
        initially_connected = self.is_connected

        if self.is_connected is False:
            self.db_connect()

        query = "SELECT 1 FROM symbols WHERE ticker = ? LIMIT 1"

        try:
            self._cur.execute(query, (self._symbol,))
            return self._cur.fetchone() is not None
        except self._error as e:
            raise FdataError(f"Can't check symbol existence: {e}\n{query}") from e
        finally:
            if initially_connected is False:
                self.db_close()

    def _add_symbol(self):
        """
            Add new symbol to the database.

            Raises:
                FdataError: sql error happened.
        """
        self._check_if_connected()

        insert_symbol = "INSERT INTO symbols (ticker) VALUES (?) ON CONFLICT(ticker) DO NOTHING;"

        try:
            self._cur.execute(insert_symbol, (self._symbol,))
            if self._cur.rowcount == 1:
                self._commit()
        except self._error as e:
            raise FdataError(f"Can't execute a query on a table 'symbols': {e}\n{insert_symbol}") from e

    def remove_symbol(self):
        """
            Remove a symbol completely.

            All corresponding records in quotes table will be deleted because of foreign key linking (cascade deletion).
        """
        initially_connected = self.is_connected

        if self.is_connected is False:
            self.db_connect()

        # Cascade delete will remove the corresponding entries in tables related to specific security data
        # like fundamentals for stock
        delete_symbol = "DELETE FROM symbols WHERE symbol_id = (SELECT symbol_id FROM symbols WHERE ticker = ?);"

        try:
            self._cur.execute(delete_symbol, (self._symbol,))
            self._conn.commit()
        except self._error as e:
            raise FdataError(f"Can't execute a query on a table 'symbols': {e}\n{delete_symbol}") from e
        finally:
            if initially_connected is False:
                self.db_close()

    def _add_quotes(self, quotes_dict):
        """
            Add quotes to the database.

            Args:
                quotes_dict(list of dictionaries): quotes obtained from an API wrapper.

            Returns:
                (int, int): the total number of quotes before and after the operation.

            Raises:
                FdataError: sql error happened.
        """
        self._check_if_connected()

        # Insert new symbols to 'symbols' table (if the symbol does not exist)
        if not self.symbol_exists:
            self._add_symbol()

        num_before = self.get_quotes_num()

        if quotes_dict is not None:
            insert_quote = """INSERT INTO quotes (symbol_id,
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
                                          (SELECT symbol_id FROM symbols WHERE ticker = ?),
                                          (SELECT source_id FROM sources WHERE title = ?),
                                          ?,  -- ts
                                          (SELECT time_span_id FROM timespans WHERE title = ? COLLATE NOCASE),
                                          ?,  -- open
                                          ?,  -- high
                                          ?,  -- low
                                          ?,  -- close
                                          ?,  -- volume
                                          ?  -- transactions
                                      )
                                      ON CONFLICT(symbol_id, time_stamp, time_span_id, source_id)
                                      DO UPDATE SET opened = excluded.opened,
                                                    high = excluded.high,
                                                    low = excluded.low,
                                                    closed = excluded.closed,
                                                    volume = excluded.volume,
                                                    transactions = excluded.transactions;"""

            rows = (
                (self._symbol,
                 self._source_title,
                 int(quote['ts']),
                 self._timespan,
                 quote['open'],
                 quote['high'],
                 quote['low'],
                 quote['close'],
                 int(quote['volume']) if quote['volume'] is not None else None,
                 quote['transactions'])
                for quote in quotes_dict
            )

            try:
                self._cur.executemany(insert_quote, rows)
            except self._error as e:
                raise FdataError(f"Can't add quotes data to a table 'quotes': {e}\n\nThe query is\n{insert_quote}") from e

            self._commit()

        num_after = self.get_quotes_num()

        # Intervals are updated by get() after all sub-intervals succeed to avoid
        # marking a range as fetched when a temporary failure prevented fetching it.
        return (num_before, num_after)

    def _reset_cached_info(self):
        """Reset cached security info so the next fetch is not served from memory."""
        self._info = None
        self._time_zone = None
        self._sec_type = None
        self._currency = None

    def drop_symbol_intervals(self):
        """
            Drop intervals for the current symbol and the current data source only.

            Intervals are markers preventing re-fetches of already covered date ranges.
            Dropping them makes the next fetch pull the data again (useful for manual
            data refreshes besides the refetch argument).

            Returns:
                int: the number of dropped interval rows.

            Raises:
                FdataError: sql error happened.
        """
        initially_connected = self.is_connected

        if self.is_connected is False:
            self.db_connect()

        delete_intervals = """DELETE FROM data_intervals
                                WHERE symbol_id = (SELECT symbol_id FROM symbols WHERE ticker = ?)
                                AND source_id = (SELECT source_id FROM sources WHERE title = ?);"""

        try:
            self._cur.execute(delete_intervals, (self._symbol, self._source_title))
            deleted = self._cur.rowcount
            self._commit()
        except self._error as e:
            raise FdataError(f"Can't drop intervals for '{self._symbol}': {e}\n{delete_intervals}") from e
        finally:
            if initially_connected is False:
                self.db_close()

        self._reset_cached_info()

        return deleted

    def drop_datasource_intervals(self):
        """
            Drop intervals for the current data source across all symbols.

            Intervals are markers preventing re-fetches of already covered date ranges.
            Dropping them makes the next fetch pull the data again (useful for manual
            data refreshes besides the refetch argument).

            Returns:
                int: the number of dropped interval rows.

            Raises:
                FdataError: sql error happened.
        """
        initially_connected = self.is_connected

        if self.is_connected is False:
            self.db_connect()

        delete_intervals = """DELETE FROM data_intervals
                                WHERE source_id = (SELECT source_id FROM sources WHERE title = ?);"""

        try:
            self._cur.execute(delete_intervals, (self._source_title,))
            deleted = self._cur.rowcount
            self._commit()
        except self._error as e:
            raise FdataError(f"Can't drop intervals for the '{self._source_title}' source: "
                             f"{e}\n{delete_intervals}") from e
        finally:
            if initially_connected is False:
                self.db_close()

        self._reset_cached_info()

        return deleted

    def _update_data_interval(self, data_entry=None):
        """
            Update the data_intervals row for a quote timespan (when data_entry is None)
            or a dataset (when data_entry is set).

            For data_entry=None (quote interval): min_ts is extended with first_date_ts,
            max_ts is capped at last_date_ts (timespan-adjusted).
            For data_entry set (fetch marker): min_ts stays NULL, max_ts is uncapped (timespan-adjusted).

            Args:
                data_entry(str or None): the data entry to update, or None to
                    update the quote interval for self._timespan.

            Raises:
                FdataError: sql error happened.
        """
        self._check_if_connected()

        now = self._current_ts(adjusted=True)

        if data_entry is None:
            title = self._timespan
            min_ts_val = self.first_date_ts
            max_ts_val = min(now, self.last_date_ts)
        else:
            title = data_entry
            min_ts_val = None
            max_ts_val = now

        update_fetched = """INSERT INTO data_intervals (symbol_id, data_entry_id, source_id, min_ts, max_ts)
                              VALUES ((SELECT symbol_id FROM symbols WHERE ticker = ?),
                                      (SELECT data_entry_id FROM data_entries WHERE title = ?),
                                      (SELECT source_id FROM sources WHERE title = ?),
                                      ?,  -- min_ts_val
                                      ?)  -- max_ts_val
                              ON CONFLICT(symbol_id, source_id, data_entry_id)
                              DO UPDATE SET
                                  min_ts = COALESCE(MIN(data_intervals.min_ts, excluded.min_ts),
                                                    excluded.min_ts,
                                                    data_intervals.min_ts),
                                  max_ts = COALESCE(MAX(data_intervals.max_ts, excluded.max_ts),
                                                    excluded.max_ts,
                                                    data_intervals.max_ts);"""

        try:
            self._cur.execute(update_fetched,
                              (self._symbol,
                               title,
                               self._source_title,
                               min_ts_val,
                               max_ts_val))
            self._commit()
        except self._error as e:
            raise FdataError(f"Can't update data_intervals for '{title}' "
                             f"(is the entry registered?): {e}\n{update_fetched}") from e

    def _add_info(self, info):
        """
            Add security info to the database.

            Args:
                info(dict): Security info obtained from an API wrapper.

            Raises:
                FdataError: sql error happened.
        """
        self._check_if_connected()

        # Insert new symbols to 'symbols' table (if the symbol does not exist)
        if not self.symbol_exists:
            self._add_symbol()

        try:
            time_zone = info['fc_time_zone']
            sec_type = info['fc_sec_type']
        except KeyError as e:
            raise FdataError(f"Key is not found. Likely broken data is obtained (due to data soruce issues): {e}")

        currency = info.get('fc_currency', Currency.Unknown)

        insert_info = """INSERT INTO sec_info (symbol_id,
                                        source_id,
                                        time_zone,
                                        sec_type_id,
                                        currency_id)
                                    VALUES (
                                        (SELECT symbol_id FROM symbols WHERE ticker = ?),
                                        (SELECT source_id FROM sources WHERE title = ?),
                                        ?,  -- time_zone
                                        (SELECT sec_type_id FROM sectypes WHERE title = ?),
                                        (SELECT currency_id FROM currency WHERE title = ?)
                                    )
                                    ON CONFLICT(symbol_id, source_id)
                                    DO UPDATE SET time_zone = excluded.time_zone,
                                                  sec_type_id = excluded.sec_type_id,
                                                  currency_id = excluded.currency_id;"""

        try:
            self._cur.execute(insert_info,
                              (self._symbol,
                               self._source_title,
                               time_zone,
                               sec_type,
                               currency))
            self._commit()
        except self._error as e:
            raise FdataError(f"Can't add a record to a table 'sec_info': {e}\n\nThe query is\n{insert_info}") from e

