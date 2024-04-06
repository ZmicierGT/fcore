"""Data abstraction module for stocks data.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from data.fdata import FdataError, ReadOnlyData, ReadWriteData, BaseFetcher
from data.fvalues import SecType, ReportPeriod, StockQuotes, Dividends, StockSplits, def_last_date

from data.futils import get_labelled_ndarray, get_dt

import abc

import numpy as np

from dateutil.relativedelta import relativedelta

import calendar

report_quearter = "AND report_tbl.reported_period = (SELECT period_id FROM report_periods where title = 'Quarter')"
report_year = "AND report_tbl.reported_period = (SELECT period_id FROM report_periods where title = 'Year')"

class ROStockData(ReadOnlyData):
    """
        The class for read only stock operations and database integrity check for storing stock data.
    """
    def __init__(self, **kwargs):
        """
            Initializes the read only stock operations class.
        """
        super().__init__(**kwargs)

        self.sectype = SecType.Stock

        # Data related to fundamental tables. Need to be overridden in the derived class.
        self._fundamental_intervals_tbl = None
        self._income_statement_tbl = None
        self._balance_sheet_tbl = None
        self._cash_flow_tbl = None

        self._stock_info_supported = False  # Indicates if stock info is supported

    def check_database(self):
        """
            Database create/integrity check method for stock data related tables.
            Checks if the database exists. Otherwise, creates it. Checks if the database has required tables.

            Raises:
                FdataError: sql error happened.
        """
        super().check_database()

        #############################
        # Fundamental data
        #############################

        # Check if we need to create table 'report_periods'
        try:
            check_report_periods = "SELECT name FROM sqlite_master WHERE type='table' AND name='report_periods';"

            self.cur.execute(check_report_periods)
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table 'report_periods': {e}\n{check_report_periods}") from e

        if len(rows) == 0:
            create_report_periods = """CREATE TABLE report_periods(
                                    period_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    title TEXT NOT NULL UNIQUE
                                );"""

            try:
                self.cur.execute(create_report_periods)
            except self.Error as e:
                raise FdataError(f"Can't execute a query on a table 'report_periods': {e}\n{create_report_periods}") from e

            # Create index for sectype title
            create_report_period_title_idx = "CREATE INDEX idx_report_period_title ON report_periods(title);"

            try:
                self.cur.execute(create_report_period_title_idx)
            except self.Error as e:
                raise FdataError(f"Can't create index for report_periods(title): {e}") from e

        # Check if report_periods table is empty
        try:
            all_report_periods = "SELECT * FROM report_periods;"

            self.cur.execute(all_report_periods)
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table 'report_periods': {e}\n{all_report_periods}") from e

        # Check if reports_periods table has data
        if len(rows) < len(ReportPeriod) - 1:
            # Prepare the query with all supported report periods
            report_periods = ""

            for report_period in ReportPeriod:
                if report_period != ReportPeriod.All:
                    report_periods += f"('{report_period.value}'),"

            report_periods = report_periods[:len(report_periods) - 2]

            insert_report_periods = f"""INSERT OR IGNORE INTO report_periods (title)
                                    VALUES {report_periods});"""

            try:
                self.cur.execute(insert_report_periods)
            except self.Error as e:
                raise FdataError(f"Can't insert data to a table 'report_periods': {e}\n{insert_report_periods}") from e

        # Check if we need a separate table for cash dividends
        try:
            check_cash_divs = "SELECT name FROM sqlite_master WHERE type='table' AND name='cash_dividends';"

            self.cur.execute(check_cash_divs)
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table 'cash_dividends': {e}\n{check_cash_divs}") from e

        if len(rows) == 0:
            create_cash_divs = """CREATE TABLE cash_dividends(
                                cash_div_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                source_id INTEGER NOT NULL,
                                symbol_id INTEGER NOT NULL,
                                currency_id INTEGER NOT NULL,
                                declaration_date INTEGER,
                                ex_date INTEGER NOT NULL,
                                record_date INTEGER,
                                payment_date INTEGER,
                                amount REAL NOT NULL,
                                UNIQUE(symbol_id, ex_date)
                                CONSTRAINT fk_symbols,
                                    FOREIGN KEY (symbol_id)
                                    REFERENCES symbols(symbol_id)
                                    ON DELETE CASCADE
                                CONSTRAINT fk_sources,
                                    FOREIGN KEY (source_id)
                                    REFERENCES sources(source_id)
                                    ON DELETE CASCADE
                                CONSTRAINT fk_currency,
                                    FOREIGN KEY (currency_id)
                                    REFERENCES currency(currency_id)
                                    ON DELETE CASCADE
                                );"""

            try:
                self.cur.execute(create_cash_divs)
            except self.Error as e:
                raise FdataError(f"Can't execute a query on a table 'cash_dividends': {e}\n{create_cash_divs}") from e

            # Create index for symbol_id
            create_symbol_date_cash_divs_idx = "CREATE INDEX idx_cash_dividends ON cash_dividends(symbol_id, ex_date);"

            try:
                self.cur.execute(create_symbol_date_cash_divs_idx)
            except self.Error as e:
                raise FdataError(f"Can't create index cash_dividends(symbol_id, symbol_id, ex_date): {e}") from e

        # Check if we need a separate table for stock splits
        try:
            check_stock_splits = "SELECT name FROM sqlite_master WHERE type='table' AND name='stock_splits';"

            self.cur.execute(check_stock_splits)
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table 'stock_splits': {e}\n{check_stock_splits}") from e

        if len(rows) == 0:
            create_stock_splits = """CREATE TABLE stock_splits(
                                    stock_split_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    source_id INTEGER NOT NULL,
                                    symbol_id INTEGER NOT NULL,
                                    split_date INTEGER NOT NULL,
                                    split_ratio REAL,
                                    UNIQUE(symbol_id, split_date)
                                    CONSTRAINT fk_symbols,
                                        FOREIGN KEY (symbol_id)
                                        REFERENCES symbols(symbol_id)
                                        ON DELETE CASCADE
                                    CONSTRAINT fk_sources,
                                        FOREIGN KEY (source_id)
                                        REFERENCES sources(source_id)
                                        ON DELETE CASCADE
                                    );"""

            try:
                self.cur.execute(create_stock_splits)
            except self.Error as e:
                raise FdataError(f"Can't execute a query on a table 'stock_splits': {e}\n{create_stock_splits}") from e

            # Create index for symbol_id
            create_symbol_date_stock_splits_idx = "CREATE INDEX idx_stock_splits ON stock_splits(symbol_id, split_date);"

            try:
                self.cur.execute(create_symbol_date_stock_splits_idx)
            except self.Error as e:
                raise FdataError(f"Can't create index stock_splits(symbol_id, symbol_id, split_date): {e}") from e

        # Check if we need to create table 'stock_intervals'
        try:
            check_stock_intervals = "SELECT name FROM sqlite_master WHERE type='table' AND name='stock_intervals';"

            self.cur.execute(check_stock_intervals)
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table 'stock_intervals': {e}\n{check_stock_intervals}") from e

        if len(rows) == 0:
            create_stock_intervals = """CREATE TABLE stock_intervals (
                                                interval_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                symbol_id INTEGER NOT NULL,
                                                source_id INTEGER NOT NULL,
                                                div_max_ts INTEGER,
                                                split_max_ts INTEGER,
                                                    CONSTRAINT fk_source
                                                        FOREIGN KEY (source_id)
                                                        REFERENCES sources(source_id)
                                                        ON DELETE CASCADE
                                                    CONSTRAINT fk_symbols
                                                        FOREIGN KEY (symbol_id)
                                                        REFERENCES symbols(symbol_id)
                                                        ON DELETE CASCADE
                                                UNIQUE(symbol_id, source_id)
                                            );"""

            try:
                self.cur.execute(create_stock_intervals)
            except self.Error as e:
                raise FdataError(f"Can't create table stock_intervals: {e}") from e

            # Create indexes for stock_intervals
            create_stock_intervals_idx = "CREATE INDEX idx_stock_intervals ON stock_intervals(symbol_id, source_id);"

            try:
                self.cur.execute(create_stock_intervals_idx)
            except self.Error as e:
                raise FdataError(f"Can't create indexes for stock_intervals table: {e}") from e

        # Check if we need to create table 'stock_sectors'
        try:
            check_stock_sectors = "SELECT name FROM sqlite_master WHERE type='table' AND name='stock_sectors';"

            self.cur.execute(check_stock_sectors)
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table 'stock_sectors': {e}\n{check_stock_sectors}") from e

        if len(rows) == 0:
            create_stock_sectors = """CREATE TABLE stock_sectors (
                                                stock_sector_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                title TEXT NOT NULL UNIQUE
                                            );"""

            try:
                self.cur.execute(create_stock_sectors)
            except self.Error as e:
                raise FdataError(f"Can't create table stock_sectors: {e}") from e

            # Create index for stock_sectors title
            create_stock_sectors_title_idx = "CREATE INDEX idx_stock_sectors_title ON stock_sectors(title);"

            try:
                self.cur.execute(create_stock_sectors_title_idx)
            except self.Error as e:
                raise FdataError(f"Can't create index for stock_sectors(title): {e}") from e

        # Check if stock_sectors table is empty
        try:
            all_sectors = "SELECT COUNT(*) FROM stock_sectors;"

            self.cur.execute(all_sectors)
            sectors_length = self.cur.fetchone()[0]
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table 'stock_sectors': {e}\n{all_sectors}") from e

        if sectors_length != 12:
            # Insert data into stock sectors

            # TODO MID Replace if with the values from sectors list
            insert_sectors = """INSERT INTO stock_sectors ('title') VALUES
                                    ('Unknown'), ('Technology'), ('Financial Services'),
                                    ('Healthcare'), ('Consumer Cyclical'), ('Industrials'),
                                    ('Communication Services'), ('Consumer Defensive'), ('Energy'),
                                    ('Basic Materials'), ('Real Estate'), ('Utilities');"""

            try:
                self.cur.execute(insert_sectors)
                self.commit()
            except self.Error as e:
                raise FdataError(f"Can't execute a query on a table 'stock_sectors': {e}\n{insert_sectors}") from e

        # Check if we need to create table 'stock_info'
        try:
            check_stock_info = "SELECT name FROM sqlite_master WHERE type='table' AND name='stock_info';"

            self.cur.execute(check_stock_info)
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table 'stock_info': {e}\n{check_stock_info}") from e

        if len(rows) == 0:

            create_stock_info = """CREATE TABLE stock_info (
                                                stock_info_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                symbol_id INTEGER NOT NULL,
                                                source_id INTEGER NOT NULL,
                                                stock_sector_id INTEGER,
                                                time_zone TEXT,
                                                modified INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                                                UNIQUE(symbol_id, stock_info_id)
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
                self.cur.execute(create_stock_info)
            except self.Error as e:
                raise FdataError(f"Can't create table stock_info: {e}") from e

            # Create indexes for stock_info
            create_stock_info_idx = "CREATE INDEX idx_stock_info ON stock_info(symbol_id);"

            try:
                self.cur.execute(create_stock_info_idx)
            except self.Error as e:
                raise FdataError(f"Can't create indexes for stock_info table: {e}") from e

            # Create trigger to last modified time on stock_info
            create_fmp_cap_trigger = """CREATE TRIGGER update_stock_info
                                                BEFORE UPDATE
                                                    ON stock_info
                                        BEGIN
                                            UPDATE stock_info
                                            SET modified = strftime('%s', 'now')
                                            WHERE stock_info_id = old.stock_info_id;
                                        END;"""

            try:
                self.cur.execute(create_fmp_cap_trigger)
            except self.Error as e:
                raise FdataError(f"Can't create trigger for stock_info: {e}") from e

    def get_db_dividends(self, last_ts=def_last_date):
        """
            Get dividends.

            Args:
                last_ts(int): override last time stamp to get data.

            Returns:
                ndarray: dividends for a symbol.
        """
        get_divs = f"""SELECT	declaration_date,
                                ex_date,
                                record_date,
                                payment_date,
                                amount,
                                (SELECT title FROM currency c WHERE cd.currency_id = c.currency_id) AS currency,
                                (SELECT title FROM sources s2 WHERE cd.source_id = s2.source_id) AS source
                            FROM cash_dividends cd INNER JOIN symbols s ON cd.symbol_id = s.symbol_id
                            WHERE s.ticker = '{self.symbol}'
                            AND ex_date >= {self.first_date_ts}
                            AND ex_date <= {last_ts}
                            ORDER BY ex_date;"""

        try:
            self.cur.execute(get_divs)
            divs = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't obtain cash dividends: {e}\n\nThe query is\n{get_divs}") from e

        if len(divs):
            divs = get_labelled_ndarray(divs)
        else:
            divs = None

        return divs

    def get_db_splits(self, last_ts=def_last_date):
        """
            Get stock splits for a specified symbol and time interval.

            Args:
                last_ts(int): override last time stamp to get data.

            Returns:
                ndarray: splits for a symbol.
        """
        get_splits = f"""SELECT	split_date,
		                        split_ratio,
		                        (SELECT title FROM sources s2 WHERE ss.source_id = s2.source_id) AS source
	                        FROM stock_splits ss INNER JOIN symbols s ON ss.symbol_id = s.symbol_id
	                        WHERE s.ticker = '{self.symbol}'
                            AND split_date >= {self.first_date_ts}
                            AND split_date <= {last_ts}
                            ORDER BY split_date;"""

        try:
            self.cur.execute(get_splits)
            splits = self.cur.fetchall()
        except IndexError:
            self.log(f"No split data for {self.symbol}")
        except self.Error as e:
            raise FdataError(f"Can't obtain split data: {e}\n\nThe query is\n{get_splits}") from e

        if len(splits):
            splits = get_labelled_ndarray(splits)
        else:
            splits = None

        return splits

    # TODO MID Think if ignore last date is needed here
    def get_quotes(self, num=0, columns=None, joins=None, queries=None, ignore_last_date=True):
        """
            Get quotes for specified symbol, dates and timespan (if any). Additional columns from other tables
            linked by symbol_id may be requested (like fundamental data)

            Args:
                num(int): the number of rows to get. 0 gets all the quotes.
                columns(list of tuple): additional pairs of (table, column) to query.
                joins(list): additional joins to get data from other tables.
                queries(list): additional queries from other tables (like funamental, global economic data).
                ignore_last_date(bool): indicates if last date should be ignored (all recent history is obtained)

            Returns:
                list: list with quotes data.

            Raises:
                FdataError: sql error happened.
        """
        if isinstance(columns, list) is False:
            columns = []

        columns.append('opened AS adj_open')
        columns.append('high AS adj_high')
        columns.append('low AS adj_low')
        columns.append('closed AS adj_close')
        columns.append('volume AS adj_volume')
        columns.append('0.0 AS divs_ex')
        columns.append('0.0 AS divs_pay')
        columns.append('1.0 AS splits')

        quotes = super().get_quotes(num=num, columns=columns, joins=joins, queries=queries, ignore_last_date=ignore_last_date)

        if quotes is None:
            return

        # Calculate the adjusted close price.

        last_ts = quotes[StockQuotes.TimeStamp][-1]

        # Get all dividend data
        divs = self.get_db_dividends(last_ts=last_ts)

        # Get all split data
        splits = self.get_db_splits(last_ts=last_ts)

        # TODO MID Find out why adjustment precision is a bit less than expected
        # Adjust the price for dividends
        if divs is not None:
            # Need to establish if we have a payment date in the database. If we have no,
            # then add one month to the execution date.
            payment_date_num = np.count_nonzero(~np.isnan(divs[Dividends.PaymentDate].astype(float)))
            ex_date_num = np.count_nonzero(~np.isnan(divs[Dividends.ExDate].astype(float)))

            if payment_date_num != ex_date_num or payment_date_num == ex_date_num - 1:
                self.log("Warning: Number of ex_date and payment entries do not correspond each other. Calculating payment date manually (ex_date + 1 month)")

                # Wipe the values in payment_date column
                divs[Dividends.PaymentDate] = np.nan
                divs[Dividends.PaymentDate] = divs[Dividends.ExDate] + 2592000  # Add 30 days to ex_date to estimate a payment date

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

                    quotes[StockQuotes.AdjOpen][:idx_ex] = quotes[StockQuotes.Open][:idx_ex] * o_ratio
                    quotes[StockQuotes.AdjHigh][:idx_ex] = quotes[StockQuotes.High][:idx_ex] * h_ratio
                    quotes[StockQuotes.AdjLow][:idx_ex] = quotes[StockQuotes.Low][:idx_ex] * l_ratio
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
            self.log(f"Warning: No dividend data for {self.symbol} in the requested period.")

        # Adjust the price to stock splits
        if splits is not None:
            for i in range(len(splits)):
                idx_split = np.searchsorted(quotes[StockQuotes.TimeStamp], [splits[StockSplits.Date][i], ], side='right')[0]

                try:
                    ratio = splits[StockSplits.Ratio][i]
                    quotes[StockQuotes.Splits][idx_split] = ratio

                    if ratio != 1:
                        # TODO LOW Think if such approach may be dangerous (whe value assigned to the copy of the array)
                        quotes[StockQuotes.AdjOpen][:idx_split] = quotes[StockQuotes.Open][:idx_split] / ratio
                        quotes[StockQuotes.AdjHigh][:idx_split] = quotes[StockQuotes.High][:idx_split] / ratio
                        quotes[StockQuotes.AdjLow][:idx_split] = quotes[StockQuotes.Low][:idx_split] / ratio
                        quotes[StockQuotes.AdjClose][:idx_split] = quotes[StockQuotes.AdjClose][:idx_split] / ratio
                        quotes[StockQuotes.AdjVolume][:idx_split] = quotes[StockQuotes.Volume][:idx_split] * ratio
                except IndexError:
                    # No need to do anything - just requested quote data is shorter than available split data
                    pass
        else:
            self.log(f"Warning: No split data for {self.symbol} in the requested period.")

        last_date_ts = calendar.timegm(self.set_eod_time(self.last_date).utctimetuple())

        idx = np.where(quotes[StockQuotes.TimeStamp] <= last_date_ts)[0]

        if len(idx):
            max_idx = min(len(quotes), max(idx) + 1)
        else:
            max_idx = 0

        return quotes[:max_idx]

class RWStockData(ROStockData, ReadWriteData):
    """
        Base class for read/write stock data SQL operations.
    """

    def get_income_statement_num(self):
        """Get the number of income statement reports.

            Returns:
                int: the number of income statements in the database.

            Raises:
                FdataError: sql error happened.
        """
        return self._get_data_num(self._income_statement_tbl)

    def get_balance_sheet_num(self):
        """Get the number of balance sheet reports.

            Returns:
                int: the number of balance sheets in the database.

            Raises:
                FdataError: sql error happened.
        """
        return self._get_data_num(self._balance_sheet_tbl)

    def get_cash_flow_num(self):
        """Get the number of cash flow reports.

            Returns:
                int: the number of cash flow entries in the database.

            Raises:
                FdataError: sql error happened.
        """
        return self._get_data_num(self._cash_flow_tbl)

    # TODO LOW Write it in a more rational way (if it is ever possible on sqlite)
    def _update_intervals(self, column, table):
        """
            Update (if needed) the requested timestamps for stock-related data.

            Args:
                column(str): the columns to update
                table(str): the table to update

            Raises:
                db error: can't update data.
        """
        get_columns = f"""SELECT name FROM PRAGMA_TABLE_INFO('{table}')
                            WHERE name NOT IN ('symbol_id', 'source_id', 'interval_id', '{column}')"""

        try:
            self.cur.execute(get_columns)
            columns = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't execute a query to update intervals: {e}\n{get_columns}") from e

        condition = f"""WHERE symbol_id = (SELECT symbol_id FROM symbols WHERE ticker = '{self.symbol}')
                        AND source_id = (SELECT source_id FROM sources WHERE title = '{self.source_title}')"""

        to_insert = ''
        values = ''

        for col in columns:
            to_insert += f"{col[0]}, "
            values += f"(SELECT {col[0]} FROM {table} {condition}),"

        now = self.current_ts(adjusted=False)

        update_intervals = f"""INSERT OR REPLACE INTO {table} (symbol_id, source_id, {to_insert} {column})
                                VALUES ((SELECT symbol_id FROM symbols WHERE ticker = '{self.symbol}'),
                                        (SELECT source_id FROM sources WHERE title = '{self.source_title}'),
                                        {values}
                                        (SELECT ifnull(
                                                        (SELECT max({column}, {now})
                                                        FROM {table}
                                                        {condition}
                                                ), {now}))
                            );"""

        try:
            self.cur.execute(update_intervals)
            self.conn.commit()
        except self.Error as e:
            raise FdataError(f"Can't execute a query to update intervals: {e}\n{update_intervals}") from e

    def _get_requested_ts(self, column, table, period=None):
        """
            Get the timestamp of a particular data entry for varios stock data entries.

            Args:
                column(str): the column to query
                table(str): the table to query
                period(ReportPeriod): period to get the data for fundamental reports.

            Returns:
                int: last modification timestamp.

            Raises:
                db error: database error during querying happened.
        """
        initially_connected = self.is_connected()

        if self.is_connected() is False:
            self.db_connect()

        period_query = ''

        if period is not None and period not in (ReportPeriod.All, ReportPeriod.Unknown):
            period_query = f"AND reported_period = (SELECT period_id FROM report_periods WHERE title='{period}')"

        query_requested_ts = f"""SELECT MAX({column}) FROM {table}
                                WHERE symbol_id = (SELECT symbol_id FROM symbols WHERE ticker = '{self.symbol}')
                                AND source_id = (SELECT source_id FROM sources WHERE title = '{self.source_title}')
                                {period_query};"""

        try:
            self.cur.execute(query_requested_ts)
            result = self.cur.fetchone()[0]
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table '{table}': {e}\n{query_requested_ts}") from e
        finally:
            if initially_connected is False:
                self.db_close()

        return result

    def get_fiscal_date_ending(self, table, period):
        """
            Get fiscal date ending timestamp.

            Args:
                table(str): the table to query
                period(ReportPeriod): period to get the data (for fundamental reports).

            Return:
                int: fiscal date ending timestamp.
        """
        return self._get_requested_ts(column='fiscal_date_ending', table=table, period=period)

    def need_to_update(self, modified_ts, table=None):
        """
            Check if we need to update data in the table.

            Args:
                table(str): table to perform the check.
                modified_ts(int): the timestamp of last data request.
                funadamental(bool): indicates if fundamental data should be checked as well.

            Returns:
                bool: indicates if update is needed.
        """
        current = get_dt(self.current_ts())

        # No data fetched yet
        if modified_ts is None:
            return True

        # No need to fetch if the requested last date is less than modified
        if self.last_date_ts < modified_ts:
            return False

        modified = get_dt(modified_ts)

        # Due to this condition the data will be checked no more than once a day even if the most recent last_date is requested.
        if (current - modified).days < 1:
            return False

        # Check fundamental data if needed
        if table is not None:
            # Need to check reports if the difference between the current date and the last annual fiscal date ending
            # is more than a year.
            if relativedelta(current, get_dt(self.get_fiscal_date_ending(table, ReportPeriod.Year))).years > 0:
                return True

            # Need to recheck reports if the difference between any report is more than 3 months
            # and 6 months for the third quarter report as some companies do not issue the 4-th quarter report.
            months_delta = relativedelta(current, get_dt(self.get_fiscal_date_ending(table, ReportPeriod.All))).months

            if get_dt(self.get_fiscal_date_ending(table, ReportPeriod.Quarter)).month != 9:
                return months_delta >= 3
            else:
                return months_delta >= 6

        # Better to re-fetch the data in unexpected situation
        self.log(f"Warning! Can't determine if data should be updated for {self.symbol}. Updating by default.")

        return True

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
        return self._get_data_num('cash_dividends')

    def get_split_num(self):
        """Get the number of stock splits.

            Returns:
                int: the number of stock splits.

            Raises:
                FdataError: sql error happened.
        """
        return self._get_data_num('stock_splits')

    def add_dividends(self, divs):
        """
            Add cash dividend entries to the database.

            Args:
                divs(list of dictionaries): dividend entries obtained from an API wrapper.

            Returns:
                (int, int): total number of dividend reports before and after the operation.

            Raises:
                FdataError: sql error happened.
        """
        self.check_if_connected()

        # Insert new symbols to 'symbols' table (if the symbol does not exist)
        if self.get_total_symbol_quotes_num() == 0:
            self.add_symbol()

        num_before = self.get_dividends_num()

        for div in divs:
            insert_dividends = f"""INSERT OR {self._update} INTO cash_dividends (symbol_id,
                                        source_id,
                                        currency_id,
										declaration_date,
										ex_date,
										record_date,
										payment_date,
                                        amount)
									VALUES (
											(SELECT symbol_id FROM symbols WHERE ticker = '{self.symbol}'),
                                            (SELECT source_id FROM sources WHERE title = '{self.source_title}'),
                                            (SELECT currency_id FROM currency WHERE title = '{div['currency']}'),
											{div['decl_ts']},
											{div['ex_ts']},
											{div['record_ts']},
											{div['pay_ts']},
                                            {div['amount']});"""

            try:
                self.cur.execute(insert_dividends)
            except self.Error as e:
                raise FdataError(f"Can't add a record to a table 'dividends': {e}\n\nThe query is\n{insert_dividends}") from e

        self.commit()

        self._update_intervals('div_max_ts', 'stock_intervals')

        return(num_before, self.get_dividends_num())

    def add_splits(self, splits):
        """
            Add split entries to the database.

            Args:
                splits(list of dictionaries): splits entries obtained from an API wrapper.

            Returns:
                (int, int): total number of split reports before and after the operation.

            Raises:
                FdataError: sql error happened.
        """
        self.check_if_connected()

        # Insert new symbols to 'symbols' table (if the symbol does not exist)
        if self.get_total_symbol_quotes_num() == 0:
            self.add_symbol()

        num_before = self.get_split_num()

        for split in splits:
            insert_splits = f"""INSERT OR {self._update} INTO stock_splits (symbol_id,
                                        source_id,
										split_date,
                                        split_ratio)
									VALUES (
											(SELECT symbol_id FROM symbols WHERE ticker = '{self.symbol}'),
                                            (SELECT source_id FROM sources WHERE title = '{self.source_title}'),
											{split['ts']},
											{split['split_ratio']});"""

            try:
                self.cur.execute(insert_splits)
            except self.Error as e:
                raise FdataError(f"Can't add a record to a table 'dividends': {e}\n\nThe query is\n{insert_splits}") from e

        self.commit()

        self._update_intervals('split_max_ts', 'stock_intervals')

        return(num_before, self.get_split_num())

    def add_info(self, info):
        """
            Add stock info to the database.

            Args:
                info(dict): Stock info obtained from an API wrapper.

            Raises:
                FdataError: sql error happened.
        """
        self.check_if_connected()

        # Insert new symbols to 'symbols' table (if the symbol does not exist)
        if self.get_total_symbol_quotes_num() == 0:
            self.add_symbol()

        super().add_info(info)

        try:
            sector = info['sector']
        except KeyError as e:
            raise FdataError(f"Key is not found. Likely broken data is obtained (due to data source issues): {e}")

        if self._stock_info_supported:
            insert_info = f"""INSERT OR {self._update} INTO stock_info (symbol_id,
                                        source_id,
                                        stock_sector_id)
                                    VALUES (
                                            (SELECT symbol_id FROM symbols WHERE ticker = '{self.symbol}'),
                                            (SELECT source_id FROM sources WHERE title = '{self.source_title}'),
                                            (SELECT stock_sector_id FROM stock_sectors WHERE title = '{sector}')
                                        );"""

            try:
                self.cur.execute(insert_info)
            except self.Error as e:
                raise FdataError(f"Can't add a record to a table 'stock_info': {e}\n\nThe query is\n{insert_info}") from e

            self.commit()

class StockFetcher(RWStockData, BaseFetcher, metaclass=abc.ABCMeta):
    """
        Abstract class to fetch quotes by API wrapper and add them to the database.
    """
    def get(self, num=0, columns=None, joins=None, queries=None, ignore_last_date=False):
        """
            Get stock quotes, divs and splits data if needed.

            Args:
                num(int): the number of rows to get. 0 gets all the quotes.
                columns(list): additional columns to query.
                joins(list): additional joins to get data from other tables.
                queries(list): additional queries from other tables (like funamental, global economic data).
                ignore_last_date(bool): indicates if last date should be ignored (all recent history is obtained)

            Returns:
                array: the fetched quote entries.
        """
        # Get also divs and splits for stock and etf as theoretically the instance may be used for other sec types
        if self.sectype in (SecType.Stock, SecType.ETF):
            self.get_dividends()
            self.get_splits()
        else:
            self.log(f"Warning! Security type is not stock or ETF ({self.sectype}) so split/dividend data is not obtained.")

        return super().get(num=num, columns=columns, joins=joins, queries=queries, ignore_last_date=ignore_last_date)

    def get_quotes_only(self):
        """
            Get stock quotes only (without dividends and splits data)

            Returns:
                array: the fetched quote entries.
        """
        return super().get()

    def get_info(self):
        """
            Fetch (if needed) and return stock info data.
        """
        initially_connected = self.is_connected()

        if self.is_connected() is False:
            self.db_connect()

        # Get base security info
        base_info = super().get_info()

        if self._stock_info_supported:
            mod_ts = self.get_last_modified('stock_info')

            # Fetch data if no data present
            if mod_ts is None:
                self.add_info(self.fetch_info())

            # Just sector title is used from info for now
            info_query = f"""SELECT title FROM stock_sectors WHERE stock_sector_id =
                                (SELECT stock_sector_id FROM stock_info WHERE symbol_id =
                                    (SELECT symbol_id FROM symbols WHERE ticker='{self.symbol}'))"""

            try:
                self.cur.execute(info_query)
                row = self.cur.fetchone()[0]
            except self.Error as e:
                raise FdataError(f"Can't execute a query on a table 'stock_info': {e}\n{info_query}") from e

            stock_info = {'sector': row}
            base_info.update(stock_info)

        if initially_connected is False:
            self.db_close()

        return base_info

    # TODO LOW Think if need to move it to the base class
    def _fetch_data_if_none(self,
                            column,
                            interval_table,
                            num_method,
                            add_method,
                            fetch_method,
                            data_table=None):
        """
            Fetch all the available additional data if needed.

            Args:
                column(str): column to check maximum requested timestamp.
                interval_table(str): table to get max requested timestamp from
                num_method(method): method to get the current entries number.
                add_method(method): method to add the entries to the database.
                fetch_method(method): method to fetch the entries.
                data_table(str): table with data to check for maximum fiscal date ending

            Returns:
                int: the number of fetched entries.
        """
        initially_connected = self.is_connected()

        if self.is_connected() is False:
            self.db_connect()

        current_num = num_method()
        num = current_num

        # Check if we need to fetch the data
        if self.need_to_update(modified_ts=self._get_requested_ts(column, interval_table), table=data_table):
            add_method(fetch_method())
            num = num_method()

        if initially_connected is False:
            self.db_close()

        return num - current_num

    def get_income_statement(self):
        """
            Fetch all the available income statement reports if needed.

            Returns:
                array: the fetched reports.
                int: the number of fetched reports.
        """
        return self._fetch_data_if_none(column='income_statement_max_ts',
                                        interval_table=self._fundamental_intervals_tbl,
                                        data_table=self._income_statement_tbl,
                                        num_method=self.get_income_statement_num,
                                        add_method=self.add_income_statement,
                                        fetch_method=self.fetch_income_statement)

    def get_balance_sheet(self):
        """
            Fetch all the available balance sheet reports if needed.

            Returns:
                array: the fetched reports.
                int: the number of fetched reports.
        """
        return self._fetch_data_if_none(column='balance_sheet_max_ts',
                                        interval_table=self._fundamental_intervals_tbl,
                                        data_table=self._balance_sheet_tbl,
                                        num_method=self.get_balance_sheet_num,
                                        add_method=self.add_balance_sheet,
                                        fetch_method=self.fetch_balance_sheet)

    def get_cash_flow(self):
        """
            Fetch all the available cash flow reports if needed.

            Returns:
                array: the fetched reports.
                int: the number of fetched reports.
        """
        return self._fetch_data_if_none(column='cash_flow_max_ts',
                                        interval_table=self._fundamental_intervals_tbl,
                                        data_table=self._cash_flow_tbl,
                                        num_method=self.get_cash_flow_num,
                                        add_method=self.add_cash_flow,
                                        fetch_method=self.fetch_cash_flow)

    def get_dividends(self):
        """
            Fetch all the available cash dividends if needed.

            Returns:
                array: the fetched entries.
                int: the number of fetched entries.
        """
        return self._fetch_data_if_none(column='div_max_ts',
                                        interval_table='stock_intervals',
                                        num_method=self.get_dividends_num,
                                        add_method=self.add_dividends,
                                        fetch_method=self.fetch_dividends)

    def get_splits(self):
        """
            Fetch all the available splits if needed.

            Returns:
                array: the fetched entries.
                int: the number of fetched entries.
        """
        return self._fetch_data_if_none(column='split_max_ts',
                                        interval_table='stock_intervals',
                                        num_method=self.get_split_num,
                                        add_method=self.add_splits,
                                        fetch_method=self.fetch_splits)

    @abc.abstractmethod
    def fetch_income_statement(self):
        """Abstract method to fetch income statement"""

    @abc.abstractmethod
    def fetch_balance_sheet(self):
        """Abstract method to fetch balance sheet"""

    @abc.abstractmethod
    def fetch_cash_flow(self):
        """Abstract method to fetch cash flow"""

    @abc.abstractmethod
    def fetch_dividends(self):
        """Abstract method to fetch dividends"""

    @abc.abstractmethod
    def fetch_splits(self):
        """Abstract method to fetch splits"""

    @abc.abstractmethod
    def add_income_statement(self, reports):
        """Add income statement report."""

    @abc.abstractmethod
    def add_balance_sheet(self, reports):
        """Add balance sheet report."""

    @abc.abstractmethod
    def add_cash_flow(self, reports):
        """Add cash flow report."""
