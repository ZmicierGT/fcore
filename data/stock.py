"""Data abstraction module for stocks data.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from data.fdata import FdataError, SecData
from data.fvalues import StrEnum, SecType, ReportPeriod, StockQuotes, Dividends, StockSplits, def_last_date, Sector

from data.futils import get_labelled_ndarray

import abc

import numpy as np

import calendar

report_quarter = "AND report_tbl.reported_period = (SELECT period_id FROM report_periods where title = 'Quarter')"
report_year = "AND report_tbl.reported_period = (SELECT period_id FROM report_periods where title = 'Year')"

class StockDataEntries(StrEnum):
    """
        Enum class for stock dataset entries with intervals tracking.
        The value is the name of the corresponding database table.
    """
    Dividends = 'cash_dividends'
    Splits = 'stock_splits'

class StockFetcher(object, metaclass=abc.ABCMeta):
    """
        Abstract class providing the stock-specific external API fetch contract.
        Sibling to SecFetcher (does NOT inherit from it): only stock-specific
        fetch methods live here. DB and orchestration concerns live in StockData.
    """
    @abc.abstractmethod
    def _fetch_income_statement(self):
        """Abstract method to fetch income statement"""

    @abc.abstractmethod
    def _fetch_balance_sheet(self):
        """Abstract method to fetch balance sheet"""

    @abc.abstractmethod
    def _fetch_cash_flow(self):
        """Abstract method to fetch cash flow"""

    @abc.abstractmethod
    def _fetch_dividends(self):
        """Abstract method to fetch dividends"""

    @abc.abstractmethod
    def _fetch_splits(self):
        """Abstract method to fetch splits"""


class StockData(SecData, StockFetcher):
    """
        The class for stock operations and database integrity check for storing stock data.
    """
    def __init__(self, **kwargs):
        """
            Initializes the stock operations class.
        """
        super().__init__(**kwargs)

        # Data entries (also used as table names) for fundamental datasets
        # (per-source). Need to be overridden in the derived class together with
        # fetch_*/add_* methods to enable fundamental data fetching via
        # data_intervals. None means the dataset is not supported by the source.
        self._income_statement_entry = None
        self._balance_sheet_entry = None
        self._cash_flow_entry = None

        # TODO LOW Think if we should always consider that stock info is supported (same as with a 'generic' security)
        self._stock_info_supported = False  # Indicates if stock info is supported

        self._annual_report_supported = False
        self._quarter_report_supported = False

        # TODO MID Think if we can refactor it to use one cache per security (not additional for a particular security type)
        # Cached stock info
        self._stock_info = None

    def _check_database(self):
        """
            Database create/integrity check method for stock data related tables.
            Checks if the database exists. Otherwise, creates it. Checks if the database has required tables.

            Runs inside the BEGIN IMMEDIATE init transaction opened by
            db_connect(); no commits are issued here.

            Raises:
                FdataError: sql error happened.
        """
        super()._check_database()

        #############################
        # Fundamental data
        #############################

        # Create table 'report_periods' if needed
        create_report_periods = """CREATE TABLE IF NOT EXISTS report_periods(
                                period_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                title TEXT NOT NULL UNIQUE
                            );"""

        try:
            self._cur.execute(create_report_periods)
        except self._error as e:
            raise FdataError(f"Can't execute a query on a table 'report_periods': {e}\n{create_report_periods}") from e

        # Create index for sectype title
        create_report_period_title_idx = "CREATE INDEX IF NOT EXISTS idx_report_period_title ON report_periods(title);"

        try:
            self._cur.execute(create_report_period_title_idx)
        except self._error as e:
            raise FdataError(f"Can't create index for report_periods(title): {e}") from e

        # Populate report_periods table if not yet fully populated
        self._populate_lookup('report_periods', [r for r in ReportPeriod if r != ReportPeriod.All])

        # Create table for cash dividends if needed
        create_cash_divs = f"""CREATE TABLE IF NOT EXISTS {StockDataEntries.Dividends}(
                            cash_div_id INTEGER PRIMARY KEY AUTOINCREMENT,
                            source_id INTEGER NOT NULL,
                            symbol_id INTEGER NOT NULL,
                            currency_id INTEGER NOT NULL,
                            declaration_date INTEGER,
                            ex_date INTEGER NOT NULL,
                            record_date INTEGER,
                            payment_date INTEGER,
                            amount REAL NOT NULL,
                            UNIQUE(symbol_id, ex_date, source_id)
                            CONSTRAINT fk_symbols
                                FOREIGN KEY (symbol_id)
                                REFERENCES symbols(symbol_id)
                                ON DELETE CASCADE
                            CONSTRAINT fk_sources
                                FOREIGN KEY (source_id)
                                REFERENCES sources(source_id)
                                ON DELETE CASCADE
                            CONSTRAINT fk_currency
                                FOREIGN KEY (currency_id)
                                REFERENCES currency(currency_id)
                                ON DELETE CASCADE
                            );"""

        try:
            self._cur.execute(create_cash_divs)
        except self._error as e:
            raise FdataError(f"Can't execute a query on a table '{StockDataEntries.Dividends}': {e}\n{create_cash_divs}") from e

        # Create index for symbol_id
        create_symbol_date_cash_divs_idx = f"""CREATE INDEX IF NOT EXISTS idx_{StockDataEntries.Dividends}
                                            ON {StockDataEntries.Dividends}(symbol_id, ex_date);"""

        try:
            self._cur.execute(create_symbol_date_cash_divs_idx)
        except self._error as e:
            raise FdataError(f"Can't create index {StockDataEntries.Dividends}(symbol_id, symbol_id, ex_date): {e}") from e

        # Create table for stock splits if needed
        create_stock_splits = f"""CREATE TABLE IF NOT EXISTS {StockDataEntries.Splits}(
                                stock_split_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                source_id INTEGER NOT NULL,
                                symbol_id INTEGER NOT NULL,
                                split_date INTEGER NOT NULL,
                                split_ratio REAL,
                                UNIQUE(symbol_id, split_date, source_id)
                                CONSTRAINT fk_symbols
                                    FOREIGN KEY (symbol_id)
                                    REFERENCES symbols(symbol_id)
                                    ON DELETE CASCADE
                                CONSTRAINT fk_sources
                                    FOREIGN KEY (source_id)
                                    REFERENCES sources(source_id)
                                    ON DELETE CASCADE
                                );"""

        try:
            self._cur.execute(create_stock_splits)
        except self._error as e:
            raise FdataError(f"Can't execute a query on a table '{StockDataEntries.Splits}': {e}\n{create_stock_splits}") from e

        # Create index for symbol_id
        create_symbol_date_stock_splits_idx = f"""CREATE INDEX IF NOT EXISTS idx_{StockDataEntries.Splits}
                                              ON {StockDataEntries.Splits}(symbol_id, split_date);"""

        try:
            self._cur.execute(create_symbol_date_stock_splits_idx)
        except self._error as e:
            raise FdataError(f"Can't create index {StockDataEntries.Splits}(symbol_id, symbol_id, split_date): {e}") from e

        # Create table 'stock_sectors' if needed
        create_stock_sectors = """CREATE TABLE IF NOT EXISTS stock_sectors (
                                            stock_sector_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                            title TEXT NOT NULL UNIQUE
                                        );"""

        try:
            self._cur.execute(create_stock_sectors)
        except self._error as e:
            raise FdataError(f"Can't create table stock_sectors: {e}") from e

        # Create index for stock_sectors title
        create_stock_sectors_title_idx = "CREATE INDEX IF NOT EXISTS idx_stock_sectors_title ON stock_sectors(title);"

        try:
            self._cur.execute(create_stock_sectors_title_idx)
        except self._error as e:
            raise FdataError(f"Can't create index for stock_sectors(title): {e}") from e

        # Populate stock_sectors table if not yet fully populated
        self._populate_lookup('stock_sectors', [s for s in Sector])

        # Create table 'stock_info' if needed
        create_stock_info = """CREATE TABLE IF NOT EXISTS stock_info (
                                            stock_info_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                            symbol_id INTEGER NOT NULL,
                                            source_id INTEGER NOT NULL,
                                            stock_sector_id INTEGER,
                                            modified INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                                                CONSTRAINT fk_source
                                                    FOREIGN KEY (source_id)
                                                    REFERENCES sources(source_id)
                                                    ON DELETE CASCADE
                                                CONSTRAINT fk_symbols
                                                    FOREIGN KEY (symbol_id)
                                                    REFERENCES symbols(symbol_id)
                                                    ON DELETE CASCADE
                                                CONSTRAINT fk_stock_sectors
                                                    FOREIGN KEY (stock_sector_id)
                                                    REFERENCES stock_sectors(stock_sector_id)
                                                    ON DELETE CASCADE
                                            UNIQUE(symbol_id, source_id)
                                        );"""

        try:
            self._cur.execute(create_stock_info)
        except self._error as e:
            raise FdataError(f"Can't create table stock_info: {e}") from e

        # Create indexes for stock_info
        create_stock_info_idx = "CREATE INDEX IF NOT EXISTS idx_stock_info ON stock_info(symbol_id);"

        try:
            self._cur.execute(create_stock_info_idx)
        except self._error as e:
            raise FdataError(f"Can't create indexes for stock_info table: {e}") from e

        # Create trigger to last modified time on stock_info
        create_cap_trigger = """CREATE TRIGGER IF NOT EXISTS update_stock_info
                                            BEFORE UPDATE
                                                ON stock_info
                                    BEGIN
                                        UPDATE stock_info
                                        SET modified = strftime('%s', 'now')
                                        WHERE stock_info_id = old.stock_info_id;
                                    END;"""

        try:
            self._cur.execute(create_cap_trigger)
        except self._error as e:
            raise FdataError(f"Can't create trigger for stock_info: {e}") from e

        # Register the stock dataset entries for intervals tracking
        self._register_data_entries(StockDataEntries)

    def _get_db_dividends(self, last_ts=def_last_date):
        """
            Get dividends.

            Args:
                last_ts(int): override last time stamp to get data.

            Returns:
                ndarray: dividends for a symbol.
        """
        self._check_if_connected()

        get_divs = f"""SELECT	declaration_date,
                                ex_date,
                                record_date,
                                payment_date,
                                amount,
                                (SELECT title FROM currency c WHERE cd.currency_id = c.currency_id) AS currency,
                                (SELECT title FROM sources s2 WHERE cd.source_id = s2.source_id) AS source
                            FROM {StockDataEntries.Dividends} cd INNER JOIN symbols s ON cd.symbol_id = s.symbol_id
                            WHERE s.ticker = ?
                            AND ex_date >= ?
                            AND ex_date <= ?
                            AND source_id = (SELECT source_id FROM sources WHERE title = ?)
                            ORDER BY ex_date;"""

        try:
            self._cur.execute(get_divs, (self._symbol, self.first_date_ts, last_ts, self._source_title))
            divs = self._cur.fetchall()
        except self._error as e:
            raise FdataError(f"Can't obtain cash dividends: {e}\n\nThe query is\n{get_divs}") from e

        if len(divs):
            divs = get_labelled_ndarray(divs)
        else:
            divs = None

        return divs

    def _get_db_splits(self, last_ts=def_last_date):
        """
            Get stock splits for a specified symbol and time interval.

            Args:
                last_ts(int): override last time stamp to get data.

            Returns:
                ndarray: splits for a symbol.
        """
        self._check_if_connected()

        get_splits = f"""SELECT	split_date,
	                        split_ratio,
	                        (SELECT title FROM sources s2 WHERE ss.source_id = s2.source_id) AS source
                        FROM {StockDataEntries.Splits} ss INNER JOIN symbols s ON ss.symbol_id = s.symbol_id
                        WHERE s.ticker = ?
                            AND split_date >= ?
                            AND split_date <= ?
                            AND source_id = (SELECT source_id FROM sources WHERE title = ?)
                            ORDER BY split_date;"""

        try:
            self._cur.execute(get_splits, (self._symbol, self.first_date_ts, last_ts, self._source_title))
            splits = self._cur.fetchall()
        except self._error as e:
            raise FdataError(f"Can't obtain split data: {e}\n\nThe query is\n{get_splits}") from e

        if len(splits):
            splits = get_labelled_ndarray(splits)
        else:
            splits = None
            self._log(f"No split data for {self._symbol}")

        return splits

    # TODO MID Think if ignore last date is needed here
    def _get_quotes(self, num=0, columns=[], joins=None, queries=None, ignore_last_date=False, ignore_source=False):
        """
            Get quotes for specified symbol, dates and timespan (if any). Additional columns from other tables
            linked by symbol_id may be requested (like fundamental data)

            Args:
                num(int): the number of rows to get. 0 gets all the quotes.
                columns(list of tuple): additional pairs of (table, column) to query.
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

        if not isinstance(columns, list):
            self._log('Incorrect columns list provided. Overriding as list with stock-related data.')
            columns = []

        stock_columns = list(columns)  # Make a copy of columns so the caller's data is not affected

        stock_columns.append('opened AS adj_open')
        stock_columns.append('high AS adj_high')
        stock_columns.append('low AS adj_low')
        stock_columns.append('closed AS adj_close')
        stock_columns.append('volume AS adj_volume')
        stock_columns.append('0.0 AS divs_ex')
        stock_columns.append('0.0 AS divs_pay')
        stock_columns.append('1.0 AS splits')

        quotes = super()._get_quotes(num=num,
                                    columns=stock_columns,
                                    joins=joins,
                                    queries=queries,
                                    ignore_last_date=ignore_last_date,
                                    ignore_source=ignore_source)

        if quotes is None:
            return

        # Calculate the adjusted close price.

        last_ts = quotes[StockQuotes.TimeStamp][-1]

        # Get all dividend data
        divs = self._get_db_dividends(last_ts=last_ts)

        # Get all split data
        splits = self._get_db_splits(last_ts=last_ts)

        # TODO MID Find out why adjustment precision is a bit less than expected
        # Adjust the price for dividends
        if divs is not None:
            # Need to establish if we have a payment date in the database. If we have no,
            # then add one week to the ex-date.
            payment_date_num = np.count_nonzero(~np.isnan(divs[Dividends.PaymentDate].astype(float)))
            ex_date_num = np.count_nonzero(~np.isnan(divs[Dividends.ExDate].astype(float)))

            if payment_date_num != ex_date_num or payment_date_num == ex_date_num - 1:
                self._log("Warning: Number of ex_date and payment entries do not correspond each other. Calculating payment date manually (ex_date + 1 month)")

                # Wipe the values in payment_date column
                divs[Dividends.PaymentDate] = np.nan
                divs[Dividends.PaymentDate] = divs[Dividends.ExDate] + 604800  # Add 7 days to ex_date to estimate a payment date

            for i in range(len(divs)):
                idx_ex = np.searchsorted(quotes[StockQuotes.TimeStamp], [divs[Dividends.ExDate][i], ], side='right')[0]

                amount = divs[Dividends.Amount][i]

                try:
                    quotes[StockQuotes.ExDividends][idx_ex] = amount

                    opened = quotes[StockQuotes.Open][idx_ex]
                    high = quotes[StockQuotes.High][idx_ex]
                    low = quotes[StockQuotes.Low][idx_ex]
                    closed = quotes[StockQuotes.Close][idx_ex]

                    o_ratio = 1
                    h_ratio = 1
                    l_ratio = 1
                    c_ratio = 1

                    # In some cases the values may be 0. Need to skip such cases.
                    if opened:
                        o_ratio -= amount / opened

                    if high:
                        h_ratio -= amount / high

                    if low:
                        l_ratio -= amount / low

                    if closed:
                        c_ratio -= amount / closed

                    quotes[StockQuotes.AdjOpen][:idx_ex] = quotes[StockQuotes.AdjOpen][:idx_ex] * o_ratio
                    quotes[StockQuotes.AdjHigh][:idx_ex] = quotes[StockQuotes.AdjHigh][:idx_ex] * h_ratio
                    quotes[StockQuotes.AdjLow][:idx_ex] = quotes[StockQuotes.AdjLow][:idx_ex] * l_ratio
                    quotes[StockQuotes.AdjClose][:idx_ex] = quotes[StockQuotes.AdjClose][:idx_ex] * c_ratio
                except IndexError:
                    pass
                    # No need to do anything - just requested quote data is shorter than available dividend data

                idx_pay = np.searchsorted(quotes[StockQuotes.TimeStamp], [divs[Dividends.PaymentDate][i], ], side='right')[0]

                try:
                    quotes[StockQuotes.PayDividends][idx_pay] = amount
                except IndexError:
                    pass
                    # No need to do anything as just payment haven't happened in the current stock history
        else:
            self._log(f"Warning: No dividend data for {self._symbol} in the requested period.")

        # Adjust the price to stock splits
        if splits is not None:
            for i in range(len(splits)):
                idx_split = np.searchsorted(quotes[StockQuotes.TimeStamp], [splits[StockSplits.Date][i], ], side='right')[0]

                try:
                    ratio = splits[StockSplits.Ratio][i]
                    quotes[StockQuotes.Splits][idx_split] = ratio

                    if ratio != 1:
                        # TODO LOW Think if such approach may be dangerous (whe value assigned to the copy of the array)
                        quotes[StockQuotes.AdjOpen][:idx_split] = quotes[StockQuotes.AdjOpen][:idx_split] / ratio
                        quotes[StockQuotes.AdjHigh][:idx_split] = quotes[StockQuotes.AdjHigh][:idx_split] / ratio
                        quotes[StockQuotes.AdjLow][:idx_split] = quotes[StockQuotes.AdjLow][:idx_split] / ratio
                        quotes[StockQuotes.AdjClose][:idx_split] = quotes[StockQuotes.AdjClose][:idx_split] / ratio
                        quotes[StockQuotes.AdjVolume][:idx_split] = quotes[StockQuotes.AdjVolume][:idx_split] * ratio
                except IndexError:
                    # No need to do anything - just requested quote data is shorter than available split data
                    pass
        else:
            self._log(f"Warning: No split data for {self._symbol} in the requested period.")

        last_date_ts = calendar.timegm(self._set_eod_time(self.last_date).utctimetuple())

        idx = np.where(quotes[StockQuotes.TimeStamp] <= last_date_ts)[0]

        if len(idx):
            max_idx = min(len(quotes), max(idx) + 1)
        else:
            max_idx = 0

        return quotes[:max_idx]

    def get_income_statement_num(self):
        """Get the number of income statement reports.

            Returns:
                int: the number of income statements in the database.

            Raises:
                FdataError: sql error happened.
        """
        initially_connected = self.is_connected

        if self.is_connected is False:
            self.db_connect()

        try:
            return self._get_data_num(self._income_statement_entry)
        finally:
            if initially_connected is False:
                self.db_close()

    def get_balance_sheet_num(self):
        """Get the number of balance sheet reports.

            Returns:
                int: the number of balance sheets in the database.

            Raises:
                FdataError: sql error happened.
        """
        initially_connected = self.is_connected

        if self.is_connected is False:
            self.db_connect()

        try:
            return self._get_data_num(self._balance_sheet_entry)
        finally:
            if initially_connected is False:
                self.db_close()

    def get_cash_flow_num(self):
        """Get the number of cash flow reports.

            Returns:
                int: the number of cash flow entries in the database.

            Raises:
                FdataError: sql error happened.
        """
        initially_connected = self.is_connected

        if self.is_connected is False:
            self.db_connect()

        try:
            return self._get_data_num(self._cash_flow_entry)
        finally:
            if initially_connected is False:
                self.db_close()

    #################################
    # Dividends / splits data methods
    #################################

    def get_dividends_num(self):
        """Get the number of dividends entries for the symbol.

            Returns:
                int: the number of dividend entries.

            Raises:
                FdataError: sql error happened.
        """
        initially_connected = self.is_connected

        if self.is_connected is False:
            self.db_connect()

        try:
            return self._get_data_num(StockDataEntries.Dividends)
        finally:
            if initially_connected is False:
                self.db_close()

    def get_split_num(self):
        """Get the number of stock splits.

            Returns:
                int: the number of stock splits.

            Raises:
                FdataError: sql error happened.
        """
        initially_connected = self.is_connected

        if self.is_connected is False:
            self.db_connect()

        try:
            return self._get_data_num(StockDataEntries.Splits)
        finally:
            if initially_connected is False:
                self.db_close()

    def _add_dividends(self, divs):
        """
            Add cash dividend entries to the database.

            Args:
                divs(list of dictionaries): dividend entries obtained from an API wrapper.

            Returns:
                (int, int): total number of dividend reports before and after the operation.

            Raises:
                FdataError: sql error happened.
        """
        self._check_if_connected()

        # Insert new symbols to 'symbols' table (if the symbol does not exist)
        if not self.symbol_exists:
            self._add_symbol()

        num_before = self.get_dividends_num()

        insert_dividends = f"""INSERT INTO {StockDataEntries.Dividends} (symbol_id,
                                    source_id,
                                    currency_id,
                                    declaration_date,
                                    ex_date,
                                    record_date,
                                    payment_date,
                                    amount)
                                VALUES (
                                    (SELECT symbol_id FROM symbols WHERE ticker = ?),
                                    (SELECT source_id FROM sources WHERE title = ?),
                                    (SELECT currency_id FROM currency WHERE title = ?),
                                    ?,  -- decl_ts
                                    ?,  -- ex_ts
                                    ?,  -- record_ts
                                    ?,  -- pay_ts
                                    ?  -- amount
                                )
                                ON CONFLICT(symbol_id, ex_date, source_id)
                                DO UPDATE SET currency_id = excluded.currency_id,
                                              declaration_date = excluded.declaration_date,
                                              record_date = excluded.record_date,
                                              payment_date = excluded.payment_date,
                                              amount = excluded.amount;"""

        rows = (
            (self._symbol,
             self._source_title,
             div['currency'],
             int(div['decl_ts']) if div['decl_ts'] is not None else None,
             int(div['ex_ts']) if div['ex_ts'] is not None else None,
             int(div['record_ts']) if div['record_ts'] is not None else None,
             int(div['pay_ts']) if div['pay_ts'] is not None else None,
             float(div['amount']))
            for div in divs
        )

        try:
            self._cur.executemany(insert_dividends, rows)
        except self._error as e:
            raise FdataError(f"Can't add a record to a table 'dividends': {e}\n\nThe query is\n{insert_dividends}") from e

        self._commit()
        self._update_data_interval(StockDataEntries.Dividends)

        return(num_before, self.get_dividends_num())

    def _add_splits(self, splits):
        """
            Add split entries to the database.

            Args:
                splits(list of dictionaries): splits entries obtained from an API wrapper.

            Returns:
                (int, int): total number of split reports before and after the operation.

            Raises:
                FdataError: sql error happened.
        """
        self._check_if_connected()

        # Insert new symbols to 'symbols' table (if the symbol does not exist)
        if not self.symbol_exists:
            self._add_symbol()

        num_before = self.get_split_num()

        insert_splits = f"""INSERT INTO {StockDataEntries.Splits} (symbol_id,
                                    source_id,
                                    split_date,
                                    split_ratio)
                                VALUES (
                                    (SELECT symbol_id FROM symbols WHERE ticker = ?),
                                    (SELECT source_id FROM sources WHERE title = ?),
                                    ?,  -- ts
                                    ?  -- split_ratio
                                )
                                ON CONFLICT(symbol_id, split_date, source_id)
                                DO UPDATE SET split_ratio = excluded.split_ratio;"""

        rows = (
            (self._symbol,
             self._source_title,
             int(split['ts']),
             float(split['split_ratio']))
            for split in splits
        )

        try:
            self._cur.executemany(insert_splits, rows)
        except self._error as e:
            raise FdataError(f"Can't add a record to a table 'stock_splits': {e}\n\nThe query is\n{insert_splits}") from e

        self._commit()
        self._update_data_interval(StockDataEntries.Splits)

        return(num_before, self.get_split_num())

    def _add_info(self, info):
        """
            Add stock info to the database.

            Args:
                info(dict): Stock info obtained from an API wrapper.

            Raises:
                FdataError: sql error happened.
        """
        self._check_if_connected()

        # Insert new symbols to 'symbols' table (if the symbol does not exist)
        if not self.symbol_exists:
            self._add_symbol()

        super()._add_info(info)

        if self._stock_info_supported and info['fc_sec_type'] == SecType.Stock:
            try:
                sector = info['sector']
            except KeyError as e:
                sector = Sector.Unknown
                self._log(f"Sector data not found. Likely incomplete data is obtained (due to data source issues): {e}")

            insert_info = """INSERT INTO stock_info (symbol_id,
                                        source_id,
                                        stock_sector_id)
                                    VALUES (
                                            (SELECT symbol_id FROM symbols WHERE ticker = ?),
                                            (SELECT source_id FROM sources WHERE title = ?),
                                            (SELECT stock_sector_id FROM stock_sectors WHERE title = ?)
                                        )
                                        ON CONFLICT(symbol_id, source_id)
                                        DO UPDATE SET stock_sector_id = excluded.stock_sector_id;"""

            try:
                self._cur.execute(insert_info, (self._symbol, self._source_title, sector))
            except self._error as e:
                raise FdataError(f"Can't add a record to a table 'stock_info': {e}\n\nThe query is\n{insert_info}") from e

            self._commit()

    def get(self, num=0, columns=[], joins=None, queries=None, ignore_last_date=False, quotes_only=False):
        """
            Get stock quotes, divs and splits data if needed.

            Args:
                num(int): the number of rows to get. 0 gets all the quotes.
                columns(list): additional columns to query.
                joins(list): additional joins to get data from other tables.
                queries(list): additional queries from other tables (like funamental, global economic data).
                ignore_last_date(bool): indicates if last date should be ignored (all recent history is obtained)
                quotes_only(bool): if True, get unadjusted stock quotes only (without dividends and splits data).

            Returns:
                array: the fetched quote entries.
        """
        # Establish the connection up front so all nested DB queries share a single connection.
        initially_connected = self.is_connected

        if self.is_connected is False:
            self.db_connect()

        try:
            if not quotes_only and self.sectype in (SecType.Stock, SecType.ETF):
                self._get_dividends()
                self._get_splits()
            elif not quotes_only:
                self._log(f"Warning! Security type is not stock or ETF ({self.sectype}) so split/dividend data is not obtained.")

            return super().get(num=num, columns=columns, joins=joins, queries=queries, ignore_last_date=ignore_last_date)
        finally:
            if initially_connected is False:
                self.db_close()

    def get_info(self):
        """
            Fetch (if needed) and return stock info data.
        """
        # Get base security info
        base_info = super().get_info()

        # Some data sources may allow to query different security types with same requests. Need to ensure that our type
        # is really stock.
        sec_type = base_info['sec_type']

        if self._stock_info is None:
            if self._stock_info_supported and sec_type == SecType.Stock:
                # Fetch data if no data present
                initially_connected = self.is_connected

                if self.is_connected is False:
                    self.db_connect()

                if self._get_data_num('stock_info') == 0:
                    self._add_info(self._fetch_info())

                # Just sector title is used from info for now
                info_query = """SELECT title FROM stock_sectors WHERE stock_sector_id =
                                    (SELECT stock_sector_id FROM stock_info WHERE symbol_id =
                                        (SELECT symbol_id FROM symbols WHERE ticker=?))"""

                try:
                    self._cur.execute(info_query, (self._symbol,))
                    row = self._cur.fetchone()[0]
                except (self._error, TypeError) as e:
                    raise FdataError(f"Can't execute a query on a table 'stock_info': {e}\n{info_query}") from e
                finally:
                    if initially_connected is False:
                        self.db_close()

                self._stock_info = {'sector': row}
                base_info.update(self._stock_info)

        return base_info

    # TODO LOW Think if need to move it to the base class
    def _fetch_data_if_none(self,
                            data_entry,
                            num_method,
                            add_method,
                            fetch_method):
        """
            Fetch all the available additional data if needed.

            Args:
                data_entry(StrEnum or None): data entry to check the fetch marker.
                    None means the dataset is not configured for this data source
                    (e.g. fundamentals in a source that doesn't override the
                    corresponding _*_entry instance variable): the method logs
                    a skip and returns 0 fetched.
                num_method(method): method to get the current entries number.
                add_method(method): method to add the entries to the database.
                fetch_method(method): method to fetch the entries.

            Returns:
                int: the number of fetched entries.
        """
        # Feature not configured for this data source. Skip gracefully.
        if data_entry is None:
            self._log(f"Data entry is not configured. Skipping fetch for {self._symbol}.")
            return 0

        self._check_if_connected()

        current_num = num_method()
        num = current_num

        # Check if we need to fetch the data
        if self._need_to_update(data_entry=data_entry):
            add_method(fetch_method())
            num = num_method()

        return num - current_num

    def get_income_statement(self):
        """
            Fetch all the available income statement reports if needed.

            Returns:
                array: the fetched reports.
                int: the number of fetched reports.
        """
        return self._fetch_data_if_none(data_entry=self._income_statement_entry,
                                        num_method=self.get_income_statement_num,
                                        add_method=self._add_income_statement,
                                        fetch_method=self._fetch_income_statement)

    def get_balance_sheet(self):
        """
            Fetch all the available balance sheet reports if needed.

            Returns:
                array: the fetched reports.
                int: the number of fetched reports.
        """
        return self._fetch_data_if_none(data_entry=self._balance_sheet_entry,
                                        num_method=self.get_balance_sheet_num,
                                        add_method=self._add_balance_sheet,
                                        fetch_method=self._fetch_balance_sheet)

    def get_cash_flow(self):
        """
            Fetch all the available cash flow reports if needed.

            Returns:
                array: the fetched reports.
                int: the number of fetched reports.
        """
        return self._fetch_data_if_none(data_entry=self._cash_flow_entry,
                                        num_method=self.get_cash_flow_num,
                                        add_method=self._add_cash_flow,
                                        fetch_method=self._fetch_cash_flow)

    # TODO MID Maybe these methods should be public like fundamentals getters? Or maybe expose just _get_db_dividends()?
    def _get_dividends(self):
        """
            Fetch all the available cash dividends if needed.

            Returns:
                array: the fetched entries.
                int: the number of fetched entries.
        """
        return self._fetch_data_if_none(data_entry=StockDataEntries.Dividends,
                                        num_method=self.get_dividends_num,
                                        add_method=self._add_dividends,
                                        fetch_method=self._fetch_dividends)

    def _get_splits(self):
        """
            Fetch all the available splits if needed.

            Returns:
                array: the fetched entries.
                int: the number of fetched entries.
        """
        return self._fetch_data_if_none(data_entry=StockDataEntries.Splits,
                                        num_method=self.get_split_num,
                                        add_method=self._add_splits,
                                        fetch_method=self._fetch_splits)
    @abc.abstractmethod
    def _add_income_statement(self, reports):
        """Add income statement report."""

    @abc.abstractmethod
    def _add_balance_sheet(self, reports):
        """Add balance sheet report."""

    @abc.abstractmethod
    def _add_cash_flow(self, reports):
        """Add cash flow report."""

