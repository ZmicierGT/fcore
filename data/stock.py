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

import pytz

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
                                modified INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
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
                                    modified INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
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

        # Check if we need to create a table income_statement
        try:
            check_income_statement = "SELECT name FROM sqlite_master WHERE type='table' AND name='income_statement';"

            self.cur.execute(check_income_statement)
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table 'income_statement': {e}\n{check_income_statement}") from e

        if len(rows) == 0:
            create_is = """CREATE TABLE income_statement(
                                is_report_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                source_id INTEGER NOT NULL,
                                symbol_id INTEGER NOT NULL,
                                reported_date INTEGER NOT NULL,
                                reported_period INTEGER NOT NULL,
                                fiscal_date_ending INTEGER NOT NULL,
                                gross_profit INTEGER,
                                total_revenue INTEGER,
                                cost_of_revenue INTEGER,
                                cost_of_goods_and_services_sold INTEGER,
                                operating_income INTEGER,
                                selling_general_and_administrative INTEGER,
                                research_and_development INTEGER,
                                operating_expenses INTEGER,
                                investment_income_net INTEGER,
								net_interest_income INTEGER,
								interest_income INTEGER,
								interest_expense INTEGER,
								non_interest_income INTEGER,
								other_non_operating_income INTEGER,
								depreciation INTEGER,
								depreciation_and_amortization INTEGER,
								income_before_tax INTEGER,
								income_tax_expense INTEGER,
								interest_and_debt_expense INTEGER,
								net_income_from_continuing_operations INTEGER,
								comprehensive_income_net_of_tax INTEGER,
								ebit INTEGER,
								ebitda INTEGER,
								net_income INTEGER,
                                modified INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                                UNIQUE(symbol_id, fiscal_date_ending, reported_period)
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
                self.cur.execute(create_is)
            except self.Error as e:
                raise FdataError(f"Can't execute a query on a table 'income_statement': {e}\n{create_is}") from e

            # Create index for symbol_id
            create_symbol_date_is_idx = "CREATE INDEX idx_income_statement ON income_statement(symbol_id, reported_date);"

            try:
                self.cur.execute(create_symbol_date_is_idx)
            except self.Error as e:
                raise FdataError(f"Can't create index income_statement(symbol_id, reported_date): {e}") from e

        # Check if we need to create a table balance_sheet
        try:
            check_balance_sheet = "SELECT name FROM sqlite_master WHERE type='table' AND name='balance_sheet';"

            self.cur.execute(check_balance_sheet)
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table 'balance_sheet': {e}\n{check_balance_sheet}") from e

        if len(rows) == 0:
            create_bs = """CREATE TABLE balance_sheet(
                                bs_report_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                source_id INTEGER NOT NULL,
                                symbol_id INTEGER NOT NULL,
                                reported_date INTEGER NOT NULL,
                                reported_period INTEGER NOT NULL,
                                fiscal_date_ending INTEGER NOT NULL,
                                total_assets INTEGER,
                                total_current_assets INTEGER,
                                cash_and_cash_equivalents_at_carrying_value INTEGER,
                                cash_and_short_term_investments INTEGER,
                                inventory INTEGER,
                                current_net_receivables INTEGER,
                                total_non_current_assets INTEGER,
                                property_plant_equipment INTEGER,
                                accumulated_depreciation_amortization_ppe INTEGER,
								intangible_assets INTEGER,
								intangible_assets_excluding_goodwill INTEGER,
								goodwill INTEGER,
								investments INTEGER,
								long_term_investments INTEGER,
								short_term_investments INTEGER,
								other_current_assets INTEGER,
								other_non_current_assets INTEGER,
								total_liabilities INTEGER,
								total_current_liabilities INTEGER,
								current_accounts_payable INTEGER,
								deferred_revenue INTEGER,
								current_debt INTEGER,
								short_term_debt INTEGER,
								total_non_current_liabilities INTEGER,
								capital_lease_obligations INTEGER,
								long_term_debt INTEGER,
								current_long_term_debt INTEGER,
								long_term_debt_noncurrent INTEGER,
								short_long_term_debt_total INTEGER,
								other_noncurrent_liabilities INTEGER,
								other_non_current_liabilities INTEGER,
								total_shareholder_equity INTEGER,
								treasury_stock INTEGER,
								retained_earnings INTEGER,
								common_stock INTEGER,
								common_stock_shares_outstanding INTEGER,
                                modified INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                                UNIQUE(symbol_id, fiscal_date_ending, reported_period)
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
                self.cur.execute(create_bs)
            except self.Error as e:
                raise FdataError(f"Can't execute a query on a table 'balance_sheet': {e}\n{create_bs}") from e

            # Create index for symbol_id
            create_symbol_date_bs_idx = "CREATE INDEX idx_balance_sheet ON balance_sheet(symbol_id, reported_date);"

            try:
                self.cur.execute(create_symbol_date_bs_idx)
            except self.Error as e:
                raise FdataError(f"Can't create index balance_sheet(symbol_id, reported_date): {e}") from e

        # Check if we need to create a table cash_flow
        try:
            check_cash_flow = "SELECT name FROM sqlite_master WHERE type='table' AND name='cash_flow';"

            self.cur.execute(check_cash_flow)
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table 'cash_flow': {e}\n{check_cash_flow}") from e

        # TODO LOW Get rid of tabulations above
        if len(rows) == 0:
            create_cf = """CREATE TABLE cash_flow(
                                cf_report_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                source_id INTEGER NOT NULL,
                                symbol_id INTEGER NOT NULL,
                                reported_date INTEGER NOT NULL,
                                reported_period INTEGER NOT NULL,
                                fiscal_date_ending INTEGER NOT NULL,
                                operating_cashflow INTEGER,
                                payments_for_operating_activities INTEGER,
                                proceeds_from_operating_activities INTEGER,
                                change_in_operating_liabilities INTEGER,
                                change_in_operating_assets INTEGER,
                                depreciation_depletion_and_amortization INTEGER,
                                capital_expenditures INTEGER,
                                change_in_receivables INTEGER,
                                change_in_inventory INTEGER,
								profit_loss INTEGER,
								cashflow_from_investment INTEGER,
								cashflow_from_financing INTEGER,
								proceeds_from_repayments_of_short_term_debt INTEGER,
								payments_for_repurchase_of_common_stock INTEGER,
								payments_for_repurchase_of_equity INTEGER,
								payments_for_repurchase_of_preferred_stock INTEGER,
								dividend_payout INTEGER,
								dividend_payout_common_stock INTEGER,
								dividend_payout_preferred_stock INTEGER,
								proceeds_from_issuance_of_common_stock INTEGER,
								proceeds_from_issuance_of_long_term_debt_and_capital_securities_net INTEGER,
								proceeds_from_issuance_of_preferred_stock INTEGER,
								proceeds_from_repurchase_of_equity INTEGER,
								proceeds_from_sale_of_treasury_stock INTEGER,
								change_in_cash_and_cash_equivalents INTEGER,
								change_in_exchange_rate INTEGER,
								net_income INTEGER,
                                modified INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                                UNIQUE(symbol_id, fiscal_date_ending, reported_period)
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
                self.cur.execute(create_cf)
            except self.Error as e:
                raise FdataError(f"Can't execute a query on a table 'cash_flow': {e}\n{create_cf}") from e

            # Create index for symbol_id
            create_symbol_date_cf_idx = "CREATE INDEX idx_cash_flow ON cash_flow(symbol_id, reported_date);"

            try:
                self.cur.execute(create_symbol_date_cf_idx)
            except self.Error as e:
                raise FdataError(f"Can't create index cash_flow(symbol_id, reported_date): {e}") from e

        # Check if we need to create a table earnings
        try:
            check_earnings = "SELECT name FROM sqlite_master WHERE type='table' AND name='earnings';"

            self.cur.execute(check_earnings)
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table 'earnings': {e}\n{check_earnings}") from e

        if len(rows) == 0:
            create_earnings = """CREATE TABLE earnings(
                                    earnings_report_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    source_id INTEGER NOT NULL,
                                    symbol_id INTEGER NOT NULL,
                                    reported_date INTEGER NOT NULL,
                                    reported_period INTEGER NOT NULL,
                                    fiscal_date_ending INTEGER NOT NULL,
                                    reported_eps INTEGER NOT NULL,
                                    estimated_eps INTEGER,
                                    surprise INTEGER,
                                    surprise_percentage INTEGER,
                                    modified INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                                    UNIQUE(symbol_id, fiscal_date_ending, reported_period)
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
                self.cur.execute(create_earnings)
            except self.Error as e:
                raise FdataError(f"Can't execute a query on a table 'earnings': {e}\n{create_earnings}") from e

            # Create index for symbol_id
            create_symbol_date_is_idx = "CREATE INDEX idx_earnings ON earnings(symbol_id, reported_date);"

            try:
                self.cur.execute(create_symbol_date_is_idx)
            except self.Error as e:
                raise FdataError(f"Can't create index earnings(symbol_id, reported_date): {e}") from e

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

        # Check if we need to create table 'fundamental_intervals'
        try:
            check_fundamental_intervals = "SELECT name FROM sqlite_master WHERE type='table' AND name='fundamental_intervals';"

            self.cur.execute(check_fundamental_intervals)
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table 'fundamental_intervals': {e}\n{check_fundamental_intervals}") from e

        if len(rows) == 0:
            create_fundamental_intervals = """CREATE TABLE fundamental_intervals (
                                                interval_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                symbol_id INTEGER NOT NULL,
                                                source_id INTEGER NOT NULL,
                                                income_statement_max_ts INTEGER,
                                                balance_sheet_max_ts INTEGER,
                                                cash_flow_max_ts INTEGER,
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
                self.cur.execute(create_fundamental_intervals)
            except self.Error as e:
                raise FdataError(f"Can't create table fundamental_intervals: {e}") from e

            # Create indexes for fundamental_intervals
            create_fundamental_intervals_idx = "CREATE INDEX idx_fundamental_intervals ON fundamental_intervals(symbol_id, source_id);"

            try:
                self.cur.execute(create_fundamental_intervals_idx)
            except self.Error as e:
                raise FdataError(f"Can't create indexes for fundamental_intervals table: {e}") from e

        # Check if we need to create table 'earnings_intervals'
        try:
            check_earnings_intervals = "SELECT name FROM sqlite_master WHERE type='table' AND name='earnings_intervals';"

            self.cur.execute(check_earnings_intervals)
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table 'earnings_intervals': {e}\n{check_earnings_intervals}") from e

        if len(rows) == 0:
            create_earnings_intervals = """CREATE TABLE earnings_intervals (
                                                interval_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                symbol_id INTEGER NOT NULL,
                                                source_id INTEGER NOT NULL,
                                                earnings_max_ts INTEGER NOT NULL,
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
                self.cur.execute(create_earnings_intervals)
            except self.Error as e:
                raise FdataError(f"Can't create table earnings_intervals: {e}") from e

            # Create indexes for earnings_intervals
            create_earnings_intervals_idx = "CREATE INDEX idx_earnings_intervals ON earnings_intervals(symbol_id, source_id);"

            try:
                self.cur.execute(create_earnings_intervals_idx)
            except self.Error as e:
                raise FdataError(f"Can't create indexes for earnings_intervals table: {e}") from e

    def get_income_statement_num(self):
        """Get the number of income statement reports.

            Returns:
                int: the number of income statements in the database.

            Raises:
                FdataError: sql error happened.
        """
        return self._get_data_num('income_statement')

    def get_balance_sheet_num(self):
        """Get the number of balance sheet reports.

            Returns:
                int: the number of balance sheets in the database.

            Raises:
                FdataError: sql error happened.
        """
        return self._get_data_num('balance_sheet')

    def get_cash_flow_num(self):
        """Get the number of cash flow reports.

            Returns:
                int: the number of cash flow entries in the database.

            Raises:
                FdataError: sql error happened.
        """
        return self._get_data_num('cash_flow')

    def get_earnings_num(self):
        """Get the number of earnings reports.

            Returns:
                int: the number of earnings entries in the database.

            Raises:
                FdataError: sql error happened.
        """
        return self._get_data_num('earnings')

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

        columns.append('closed AS adj_close')
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

                    quotes[StockQuotes.Open][:idx_ex] = quotes[StockQuotes.Open][:idx_ex] * o_ratio
                    quotes[StockQuotes.High][:idx_ex] = quotes[StockQuotes.High][:idx_ex] * h_ratio
                    quotes[StockQuotes.Low][:idx_ex] = quotes[StockQuotes.Low][:idx_ex] * l_ratio
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
                        quotes[StockQuotes.Open][:idx_split] = quotes[StockQuotes.Open][:idx_split] / ratio
                        quotes[StockQuotes.High][:idx_split] = quotes[StockQuotes.High][:idx_split] / ratio
                        quotes[StockQuotes.Low][:idx_split] = quotes[StockQuotes.Low][:idx_split] / ratio
                        quotes[StockQuotes.Volume][:idx_split] = quotes[StockQuotes.Volume][:idx_split] * ratio

                        quotes[StockQuotes.AdjClose][:idx_split] = quotes[StockQuotes.AdjClose][:idx_split] / ratio
                except IndexError:
                    # No need to do anything - just requested quote data is shorter than available split data
                    pass
        else:
            self.log(f"Warning: No split data for {self.symbol} in the requested period.")

        max_idx = min(len(quotes), max(np.where(quotes[StockQuotes.TimeStamp] <= self.last_date_ts)[0]) + 1)

        return quotes[:max_idx]

class RWStockData(ROStockData, ReadWriteData):
    """
        Base class for read/write stock data SQL operations.
    """

    ##########################
    # Fundamental data methods
    ##########################

    def add_income_statement(self, reports):
        """
            Add income_statement entries to the database.

            Args:
                quotes_dict(list of dictionaries): income statements entries obtained from an API wrapper.

            Returns:
                (int, int): total number of income statements reports before and after the operation.

            Raises:
                FdataError: sql error happened.
        """
        self.check_if_connected()

        # Insert new symbols to 'symbols' table (if the symbol does not exist)
        if self.get_total_symbol_quotes_num() == 0:
            self.add_symbol()

        num_before = self.get_income_statement_num()

        for report in reports:
            insert_report = f"""INSERT OR {self._update} INTO income_statement (symbol_id,
                                        source_id,
										reported_date,
										reported_period,
										fiscal_date_ending,
										gross_profit,
										total_revenue,
										cost_of_revenue,
										cost_of_goods_and_services_sold,
										operating_income,
										selling_general_and_administrative,
										research_and_development,
										operating_expenses,
										investment_income_net,
										net_interest_income,
										interest_income,
										interest_expense,
										non_interest_income,
										other_non_operating_income,
										depreciation,
										depreciation_and_amortization,
										income_before_tax,
										income_tax_expense,
										interest_and_debt_expense,
										net_income_from_continuing_operations,
										comprehensive_income_net_of_tax,
										ebit,
										ebitda,
										net_income)
									VALUES (
											(SELECT symbol_id FROM symbols WHERE ticker = '{self.symbol}'),
                                            (SELECT source_id FROM sources WHERE title = '{self.source_title}'),
											{report['reportedDate']},
											(SELECT period_id FROM report_periods WHERE title = '{report['period']}'),
											{report['fiscalDateEnding']},
											{report['grossProfit']},
											{report['totalRevenue']},
											{report['costOfRevenue']},
											{report['costofGoodsAndServicesSold']},
											{report['operatingIncome']},
											{report['sellingGeneralAndAdministrative']},
											{report['researchAndDevelopment']},
											{report['operatingExpenses']},
											{report['investmentIncomeNet']},
											{report['netInterestIncome']},
											{report['interestIncome']},
											{report['interestExpense']},
											{report['nonInterestIncome']},
											{report['otherNonOperatingIncome']},
											{report['depreciation']},
											{report['depreciationAndAmortization']},
											{report['incomeBeforeTax']},
											{report['incomeTaxExpense']},
											{report['interestAndDebtExpense']},
											{report['netIncomeFromContinuingOperations']},
											{report['comprehensiveIncomeNetOfTax']},
											{report['ebit']},
											{report['ebitda']},
											{report['netIncome']});"""

            try:
                self.cur.execute(insert_report)
            except self.Error as e:
                raise FdataError(f"Can't add a record to a table 'income_statement': {e}\n\nThe query is\n{insert_report}") from e

        self.commit()

        self._update_intervals('income_statement_max_ts', 'fundamental_intervals')

        return(num_before, self.get_income_statement_num())

    def add_balance_sheet(self, reports):
        """
            Add balance sheet entries to the database.

            Args:
                quotes_dict(list of dictionaries): balance sheet entries obtained from an API wrapper.

            Returns:
                (int, int): total number of income statements reports before and after the operation.

            Raises:
                FdataError: sql error happened.
        """
        self.check_if_connected()

        # Insert new symbols to 'symbols' table (if the symbol does not exist)
        if self.get_total_symbol_quotes_num() == 0:
            self.add_symbol()

        num_before = self.get_balance_sheet_num()

        for report in reports:
            print(report['fiscalDateEnding'])
            insert_report = f"""INSERT OR {self._update} INTO balance_sheet (symbol_id,
                                        source_id,
										reported_date,
										reported_period,
										fiscal_date_ending,
										total_assets,
										total_current_assets,
										cash_and_cash_equivalents_at_carrying_value,
										cash_and_short_term_investments,
										inventory,
										current_net_receivables,
										total_non_current_assets,
										property_plant_equipment,
										accumulated_depreciation_amortization_ppe,
										intangible_assets,
										intangible_assets_excluding_goodwill,
										goodwill,
										investments,
										long_term_investments,
										short_term_investments,
										other_current_assets,
										other_non_current_assets,
										total_liabilities,
										total_current_liabilities,
										current_accounts_payable,
										deferred_revenue,
										current_debt,
										short_term_debt,
										total_non_current_liabilities,
                                        capital_lease_obligations,
                                        long_term_debt,
                                        current_long_term_debt,
                                        long_term_debt_noncurrent,
                                        short_long_term_debt_total,
                                        other_noncurrent_liabilities,
                                        total_shareholder_equity,
                                        treasury_stock,
                                        retained_earnings,
                                        common_stock,
                                        common_stock_shares_outstanding)
									VALUES (
											(SELECT symbol_id FROM symbols WHERE ticker = '{self.symbol}'),
                                            (SELECT source_id FROM sources WHERE title = '{self.source_title}'),
											{report['reportedDate']},
											(SELECT period_id FROM report_periods WHERE title = '{report['period']}'),
											{report['fiscalDateEnding']},
											{report['totalAssets']},
											{report['totalCurrentAssets']},
											{report['cashAndCashEquivalentsAtCarryingValue']},
											{report['cashAndShortTermInvestments']},
											{report['inventory']},
											{report['currentNetReceivables']},
											{report['totalNonCurrentAssets']},
											{report['propertyPlantEquipment']},
											{report['accumulatedDepreciationAmortizationPPE']},
											{report['intangibleAssets']},
											{report['intangibleAssetsExcludingGoodwill']},
											{report['goodwill']},
											{report['investments']},
											{report['longTermInvestments']},
											{report['shortTermInvestments']},
											{report['otherCurrentAssets']},
											{report['otherNonCurrentAssets']},
											{report['totalLiabilities']},
											{report['totalCurrentLiabilities']},
											{report['currentAccountsPayable']},
											{report['deferredRevenue']},
											{report['currentDebt']},
											{report['shortTermDebt']},
											{report['totalNonCurrentLiabilities']},
                                            {report['capitalLeaseObligations']},
											{report['longTermDebt']},
											{report['currentLongTermDebt']},
											{report['longTermDebtNoncurrent']},
											{report['shortLongTermDebtTotal']},
											{report['otherNonCurrentLiabilities']},
											{report['totalShareholderEquity']},
											{report['treasuryStock']},
											{report['retainedEarnings']},
											{report['commonStock']},
											{report['commonStockSharesOutstanding']});"""

            try:
                self.cur.execute(insert_report)
            except self.Error as e:
                raise FdataError(f"Can't add a record to a table 'balance_sheet': {e}\n\nThe query is\n{insert_report}") from e

        self.commit()

        self._update_intervals('balance_sheet_max_ts', 'fundamental_intervals')

        return(num_before, self.get_balance_sheet_num())

    def add_cash_flow(self, reports):
        """
            Add cash flow entries to the database.

            Args:
                quotes_dict(list of dictionaries): cash flow entries obtained from an API wrapper.

            Returns:
                (int, int): total number of cash flow reports before and after the operation.

            Raises:
                FdataError: sql error happened.
        """
        self.check_if_connected()

        # Insert new symbols to 'symbols' table (if the symbol does not exist)
        if self.get_total_symbol_quotes_num() == 0:
            self.add_symbol()

        num_before = self.get_cash_flow_num()

        for report in reports:
            insert_report = f"""INSERT OR {self._update} INTO cash_flow (symbol_id,
                                        source_id,
										reported_date,
										reported_period,
										fiscal_date_ending,
										operating_cashflow,
										payments_for_operating_activities,
										proceeds_from_operating_activities,
										change_in_operating_liabilities,
										change_in_operating_assets,
										depreciation_depletion_and_amortization,
										capital_expenditures,
										change_in_receivables,
										change_in_inventory,
										profit_loss,
										cashflow_from_investment,
										cashflow_from_financing,
										proceeds_from_repayments_of_short_term_debt,
										payments_for_repurchase_of_common_stock,
										payments_for_repurchase_of_equity,
										payments_for_repurchase_of_preferred_stock,
										dividend_payout,
										dividend_payout_common_stock,
										dividend_payout_preferred_stock,
										proceeds_from_issuance_of_common_stock,
										proceeds_from_issuance_of_long_term_debt_and_capital_securities_net,
										proceeds_from_issuance_of_preferred_stock,
										proceeds_from_repurchase_of_equity,
										proceeds_from_sale_of_treasury_stock,
                                        change_in_cash_and_cash_equivalents,
                                        change_in_exchange_rate,
                                        net_income)
									VALUES (
											(SELECT symbol_id FROM symbols WHERE ticker = '{self.symbol}'),
                                            (SELECT source_id FROM sources WHERE title = '{self.source_title}'),
											{report['reportedDate']},
											(SELECT period_id FROM report_periods WHERE title = '{report['period']}'),
											{report['fiscalDateEnding']},
											{report['operatingCashflow']},
											{report['paymentsForOperatingActivities']},
											{report['proceedsFromOperatingActivities']},
											{report['changeInOperatingLiabilities']},
											{report['changeInOperatingAssets']},
											{report['depreciationDepletionAndAmortization']},
											{report['capitalExpenditures']},
											{report['changeInReceivables']},
											{report['changeInInventory']},
											{report['profitLoss']},
											{report['cashflowFromInvestment']},
											{report['cashflowFromFinancing']},
											{report['proceedsFromRepaymentsOfShortTermDebt']},
											{report['paymentsForRepurchaseOfCommonStock']},
											{report['paymentsForRepurchaseOfEquity']},
											{report['paymentsForRepurchaseOfPreferredStock']},
											{report['dividendPayout']},
											{report['dividendPayoutCommonStock']},
											{report['dividendPayoutPreferredStock']},
											{report['proceedsFromIssuanceOfCommonStock']},
											{report['proceedsFromIssuanceOfLongTermDebtAndCapitalSecuritiesNet']},
											{report['proceedsFromIssuanceOfPreferredStock']},
											{report['proceedsFromRepurchaseOfEquity']},
											{report['proceedsFromSaleOfTreasuryStock']},
                                            {report['changeInCashAndCashEquivalents']},
											{report['changeInExchangeRate']},
											{report['netIncome']});"""

            try:
                self.cur.execute(insert_report)
            except self.Error as e:
                raise FdataError(f"Can't add record to a table 'cash_flow': {e}\n\nThe query is\n{insert_report}") from e

        self.commit()

        self._update_intervals('cash_flow_max_ts', 'fundamental_intervals')

        return(num_before, self.get_cash_flow_num())

    def add_earnings(self, reports):
        """
            Add earnings entries to the database.

            Args:
                quotes_dict(list of dictionaries): earnings entries obtained from an API wrapper.

            Returns:
                (int, int): total number of earnings reports before and after the operation.

            Raises:
                FdataError: sql error happened.
        """
        self.check_if_connected()

        # Insert new symbols to 'symbols' table (if the symbol does not exist)
        if self.get_total_symbol_quotes_num() == 0:
            self.add_symbol()

        num_before = self.get_earnings_num()

        for report in reports:
            insert_report = f"""INSERT OR {self._update} INTO earnings (symbol_id,
                                        source_id,
										reported_date,
										reported_period,
										fiscal_date_ending,
										reported_eps,
                                        estimated_eps,
                                        surprise,
                                        surprise_percentage)
									VALUES (
											(SELECT symbol_id FROM symbols WHERE ticker = '{self.symbol}'),
                                            (SELECT source_id FROM sources WHERE title = '{self.source_title}'),
											{report['reportedDate']},
											(SELECT period_id FROM report_periods WHERE title = '{report['period']}'),
											{report['fiscalDateEnding']},
											{report['reportedEPS']},
											{report['estimatedEPS']},
											{report['surprise']},
											{report['surprisePercentage']});"""

            try:
                self.cur.execute(insert_report)
            except self.Error as e:
                raise FdataError(f"Can't add a record to a table 'earnings': {e}\n\nThe query is\n{insert_report}") from e

        self.commit()

        self._update_intervals('earnings_max_ts', 'earnings_intervals')

        return(num_before, self.get_earnings_num())

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
                period(ReportPeriod): period to get the data (for fundamental reports and earnings).

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
        current = get_dt(self.current_ts(), pytz.UTC)

        # No data fetched yet
        if modified_ts is None:
            return True

        # No need to fetch if the requested last date is less than modified
        if self.last_date_ts < modified_ts:
            return False

        modified = get_dt(modified_ts, pytz.UTC)

        # Due to this condition the data will be checked no more than once a day even if the most recent last_date is requested.
        if (current - modified).days < 1:
            return False

        # Check fundamental data if needed
        if table is not None:
            # Need to check reports if the difference between the current date and the last annual fiscal date ending
            # is more than a year.
            if relativedelta(current, get_dt(self.get_fiscal_date_ending(table, ReportPeriod.Year), pytz.UTC)).years > 0:
                return True

            # Need to recheck reports if the difference between any report is more than 3 months
            # and 6 months for the third quarter report as some companies do not issue the 4-th quarter report.
            months_delta = relativedelta(current, get_dt(self.get_fiscal_date_ending(table, ReportPeriod.All), pytz.UTC)).months

            if get_dt(self.get_fiscal_date_ending(table, ReportPeriod.Quarter), pytz.UTC).month != 9:
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
                array: the fetched entries.
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
                                        interval_table='fundamental_intervals',
                                        data_table='income_statement',
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
                                        interval_table='fundamental_intervals',
                                        data_table='balance_sheet',
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
                                        interval_table='fundamental_intervals',
                                        data_table='cash_flow',
                                        num_method=self.get_cash_flow_num,
                                        add_method=self.add_cash_flow,
                                        fetch_method=self.fetch_cash_flow)

    def get_earnings(self):
        """
            Fetch all the available earnings reports if needed.

            Returns:
                array: the fetched reports.
                int: the number of fetched reports.
        """
        return self._fetch_data_if_none(column='earnings_max_ts',
                                        interval_table='earnings_intervals',
                                        data_table='earnings',
                                        num_method=self.get_earnings_num,
                                        add_method=self.add_earnings,
                                        fetch_method=self.fetch_earnings)

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
    def fetch_earnings(self):
        """Abstract method to fetch earnings"""

    @abc.abstractmethod
    def fetch_dividends(self):
        """Abstract method to fetch dividends"""

    @abc.abstractmethod
    def fetch_splits(self):
        """Abstract method to fetch splits"""
