"""FMP API wrapper.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from data import stock
from data.fvalues import SecType, Currency
from data.fdata import FdataError

from data.futils import get_dt

import settings

from datetime import datetime, timedelta

import pytz

class FmpSubquery():
    """
        Class which represents additional subqueries for optional data (fundamentals, global economic, customer data and so on).
    """
    def __init__(self, table, column, condition='', title=None):
        """
            Initializes the instance of Subquery class.

            Args:
                table(str): table for subquery.
                column(str): column to obtain.
                condition(str): additional SQL condition for the subquery.
                title(str): optional title for the output column (the same as column name by default)
        """
        self.table = table
        self.column = column
        self.condition = condition

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
        subquery = f"""(SELECT {self.column}
                            FROM {self.table} report_tbl
                            WHERE report_tbl.time_stamp <= quotes.time_stamp
                            AND symbol_id = quotes.symbol_id
                            {self.condition}
                            ORDER BY report_tbl.time_stamp DESC LIMIT 1) AS {self.title}\n"""

        return subquery

class FmpStock(stock.StockFetcher):
    """
        FMP API wrapper class.
    """
    def __init__(self, **kwargs):
        """Initialize the instance of FMPStock class."""
        super().__init__(**kwargs)

        # Default values
        self.source_title = "FMP"
        self.api_key = settings.FMP.api_key

        self.sectype = SecType.Stock  # TODO LOW Distinguish stock and ETF for FMP
        self.currency = Currency.Unknown  # Currencies are not supported yet

        if settings.FMP.plan == settings.FMP.Plan.Basic:
            self.max_queries = 250
        if settings.AV.plan == settings.FMP.Plan.Starter:
            self.max_queries = 300
        if settings.AV.plan == settings.FMP.Plan.Premium:
            self.max_queries = 750
        if settings.AV.plan == settings.FMP.Plan.Ultimate:
            self.max_queries = 3000

    def check_database(self):
        """
            Database create/integrity check method for stock data related tables.
            Checks if the database exists. Otherwise, creates it. Checks if the database has required tables.

            Raises:
                FdataError: sql error happened.
        """
        super().check_database()

        # Check if we need to create a table for company capitalization
        try:
            check_capitalization = "SELECT name FROM sqlite_master WHERE type='table' AND name='fmp_capitalization';"

            self.cur.execute(check_capitalization)
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table 'fmp_capitalization': {e}\n{check_capitalization}") from e

        if len(rows) == 0:
            create_capitalization = f"""CREATE TABLE fmp_capitalization(
                                    fmp_cap_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    source_id INTEGER NOT NULL,
                                    symbol_id INTEGER NOT NULL,
                                    time_stamp INTEGER NOT NULL,
                                    cap INTEGER NOT NULL,
                                    modified INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                                    UNIQUE(symbol_id, time_stamp)
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
                self.cur.execute(create_capitalization)
            except self.Error as e:
                raise FdataError(f"Can't execute a query on a table 'fmp_capitalization': {e}\n{create_capitalization}") from e

            # Create index for symbol_id
            create_symbol_date_cap_idx = "CREATE INDEX idx_fmp_capitalization ON fmp_capitalization(symbol_id, time_stamp);"

            try:
                self.cur.execute(create_symbol_date_cap_idx)
            except self.Error as e:
                raise FdataError(f"Can't create index fmp_capitalization(symbol_id, time_stamp): {e}") from e

            # Create trigger to last modified time
            create_fmp_cap_trigger = """CREATE TRIGGER update_fmp_capitalization
                                                BEFORE UPDATE
                                                    ON fmp_capitalization
                                        BEGIN
                                            UPDATE fmp_capitalization
                                            SET modified = strftime('%s', 'now')
                                            WHERE fmp_cap_id = old.fmp_cap_id;
                                        END;"""

            try:
                self.cur.execute(create_fmp_cap_trigger)
            except self.Error as e:
                raise FdataError(f"Can't create trigger for fmp_capitalization: {e}") from e

    def get_cap_num(self):
        """Get the number of capitalization data entries.

            Returns:
                int: the number of capitalization data entries.

            Raises:
                FdataError: sql error happened.
        """
        return self._get_data_num('fmp_capitalization')

    def fetch_cap(self, num=None):
        """
            Fetch the capitalization data.

            Args:
                num(int): the number of days to limit the request.

            Returns:
                list: capitalization data.
        """
        if num is not None:
            cap_url = f"https://financialmodelingprep.com/api/v3/historical-market-capitalization/{self.symbol}?limit={num}&apikey={self.api_key}"
        else:
            cap_url = f"https://financialmodelingprep.com/api/v3/historical-market-capitalization/{self.symbol}?apikey={self.api_key}"

        # Get capitalization data
        response = self.query_api(cap_url)

        # Get json
        json_data = response.json()

        results = list(json_data)

        if len(results) == 0 or results == ['Error Message']:
            self.log(f"No capitalization data obtained for {self.symbol}")

        return results

    def add_cap(self, results):
        """
            Add capitalization data to the database.

            Args:
                results(list): the capitalization data

            Returns:
                (int, int): total number of earnings reports before and after the operation.

            Raises:
                FdataError: sql error happened.
        """
        self.check_if_connected()

        # Insert new symbols to 'symbols' table (if the symbol does not exist)
        if self.get_total_symbol_quotes_num() == 0:
            self.add_symbol()

        num_before = self.get_cap_num()

        for result in results:
            # Need to convert date to a time stamp
            result['date'] = int(get_dt(result['date'], pytz.UTC).replace(hour=23, minute=59, second=59).timestamp())

            insert_cap = f"""INSERT OR {self._update} INTO fmp_capitalization (symbol_id,
                                        source_id,
                                        time_stamp,
                                        cap)
                                    VALUES (
                                            (SELECT symbol_id FROM symbols WHERE ticker = '{self.symbol}'),
                                            (SELECT source_id FROM sources WHERE title = '{self.source_title}'),
                                            {result['date']},
                                            {result['marketCap']});"""

            try:
                self.cur.execute(insert_cap)
            except self.Error as e:
                raise FdataError(f"Can't add a record to a table 'fmp_capitalization': {e}\n\nThe query is\n{insert_cap}") from e

        self.commit()

        return(num_before, self.get_cap_num())

    def get_cap(self):
        """
            Fetch (if needed) and return capitalization data.
        """
        initially_connected = self.is_connected()

        if self.is_connected() is False:
            self.db_connect()

        num = self.get_last_modified('fmp_capitalization')

        current = min(datetime.now().replace(tzinfo=pytz.UTC), self.last_date)

        # Fetch data if no data presend or day difference between current/requested data more than 1 day
        if num is None:
            self.add_cap(self.fetch_cap())
        else:
            days_delta = (current - get_dt(num, pytz.UTC)).days
            self.add_cap(self.fetch_cap(days_delta + 1))

        new_num = self.get_cap_num()

        if initially_connected is False:
            self.db_close()

        return (new_num - num)

    ##########################################
    # Methods which are not implemented (yet).
    ##########################################

    def get_timespan_str(self):
        """
            Get timespan string (like '5min' and so on) to query a particular data source based on the timespan specified
            in the datasource instance.

            Returns:
                str: timespan string.
        """
        raise FdataError("Timespans are not supported yet in FMP datasource.")

    def fetch_quotes(self, first_ts=None, last_ts=None):
        """
            The method to fetch quotes.

            Args:
                first_ts(int): overridden first ts to fetch.
                last_ts(int): overridden last ts to fetch.

            Returns:
                list: quotes data

            Raises:
                FdataError: incorrect API key(limit reached), http error happened, invalid timespan or no data obtained.
        """
        raise FdataError(f"Fetching quotes is not supported (yet) for the source {type(self).__name__}")

    def fetch_income_statement(self):
        raise FdataError(f"Income statement data is not supported (yet) for the source {type(self).__name__}")

    def fetch_balance_sheet(self):
        raise FdataError(f"Balance sheet data is not supported (yet) for the source {type(self).__name__}")

    def fetch_cash_flow(self):
        raise FdataError(f"Cash flow data is not supported (yet) for the source {type(self).__name__}")

    def fetch_dividends(self):
        raise FdataError(f"Dividends data is not supported (yet) for the source {type(self).__name__}")

    def fetch_splits(self):
        raise FdataError(f"Splits statement data is not supported (yet) for the source {type(self).__name__}")

    def add_income_statement(self, reports):
        raise FdataError(f"Adding income statement data is not supported (yet) for the source {type(self).__name__}")

    def add_balance_sheet(self, reports):
        raise FdataError(f"Adding balance sheet data is not supported (yet) for the source {type(self).__name__}")

    def add_cash_flow(self, reports):
        raise FdataError(f"Adding cash flow data is not supported (yet) for the source {type(self).__name__}")

    def get_recent_data(self, to_cache=False):
        raise FdataError(f"Real time data is not supported (yet) for the source {type(self).__name__}")
