"""Data abstraction module for stocks data.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from data.fdata import FdataError, ReadOnlyData, ReadWriteData, BaseFetcher
from data.fvalues import SecType, ReportPeriod, five_hundred_days

import abc

import time

# TODO HIGH Think how to better query for quarterly and annual fundamental data.
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

        # Check if we need to create a table stock_core
        try:
            check_stock_core = "SELECT name FROM sqlite_master WHERE type='table' AND name='stock_core';"

            self.cur.execute(check_stock_core)
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table 'stock_core': {e}\n{check_stock_core}") from e

        if len(rows) == 0:
            create_core = """CREATE TABLE stock_core(
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
                self.cur.execute(create_core)
            except self.Error as e:
                raise FdataError(f"Can't execute a query on a table 'stock_core': {e}\n{create_core}") from e

            # Create index for quote_id
            create_quoteid_idx = "CREATE INDEX idx_quote ON stock_core(quote_id);"

            try:
                self.cur.execute(create_quoteid_idx)
            except self.Error as e:
                raise FdataError(f"Can't create index for stock_core(quote_id) in stock_core: {e}") from e

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
            create_symbol_time_is_idx = "CREATE INDEX idx_income_statement ON income_statement(symbol_id, reported_date);"

            try:
                self.cur.execute(create_symbol_time_is_idx)
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
            create_symbol_time_bs_idx = "CREATE INDEX idx_balance_sheet ON balance_sheet(symbol_id, reported_date);"

            try:
                self.cur.execute(create_symbol_time_bs_idx)
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
            create_symbol_time_cf_idx = "CREATE INDEX idx_cash_flow ON cash_flow(symbol_id, reported_date);"

            try:
                self.cur.execute(create_symbol_time_cf_idx)
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
            create_symbol_time_is_idx = "CREATE INDEX idx_earnings ON earnings(symbol_id, reported_date);"

            try:
                self.cur.execute(create_symbol_time_is_idx)
            except self.Error as e:
                raise FdataError(f"Can't create index earnings(symbol_id, reported_date): {e}") from e

    def _get_fundamentals_num(self, table):
        """Get the number of income statement reports for specified interval (+- 500 days) for a stock.

            Args:
                table(string): the table with reports.

            Returns:
                int: the number of income statements in the database.

            Raises:
                FdataError: sql error happened.
        """
        self.check_if_connected()

        get_num = f"""SELECT COUNT(*) FROM {table}
	                    WHERE reported_date >= ({self.first_date_ts} - {five_hundred_days})
                        AND reported_date <= ({self.last_date_ts} + {five_hundred_days})
                        AND symbol_id = (SELECT symbol_id FROM symbols where ticker = '{self.symbol}');"""
        try:
            self.cur.execute(get_num)
        except self.Error as e:
            raise FdataError(f"Can't query table '{table}': {e}\n\nThe query is\n{get_num}") from e

        result = self.cur.fetchone()[0]

        if result is None:
            result = 0

        return result

    def get_income_statement_num(self):
        """Get the number of income statement reports for specified interval (+- 500 days) for a stock.

            Returns:
                int: the number of income statements in the database.

            Raises:
                FdataError: sql error happened.
        """
        return self._get_fundamentals_num('income_statement')

    def get_balance_sheet_num(self):
        """Get the number of balance sheet reports for specified interval (+- 500 days) for a stock.

            Returns:
                int: the number of balance sheets in the database.

            Raises:
                FdataError: sql error happened.
        """
        return self._get_fundamentals_num('balance_sheet')

    def get_cash_flow_num(self):
        """Get the number of cash flow reports for specified interval (+- 500 days) for a stock.

            Returns:
                int: the number of cash flow entries in the database.

            Raises:
                FdataError: sql error happened.
        """
        return self._get_fundamentals_num('cash_flow')

    def get_earnings_num(self):
        """Get the number of earnings reports for specified interval (+- 500 days) for a stock.

            Returns:
                int: the number of earnings entries in the database.

            Raises:
                FdataError: sql error happened.
        """
        return self._get_fundamentals_num('earnings')

    def get_quotes(self, num=0, columns=None, joins=None, queries=None):
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
        if isinstance(columns, list) is False:
            columns = []

        columns.append('raw_close')
        columns.append('dividends')
        columns.append('split_coefficient')

        if isinstance(joins, list) is False:
            joins = []

        joins.append('INNER JOIN stock_core ON quotes.quote_id = stock_core.quote_id')

        return super().get_quotes(num=num, columns=columns, joins=joins, queries=queries)

class RWStockData(ROStockData, ReadWriteData):
    """
        Base class for read/write stock data SQL operations.
    """
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
        if self.get_symbol_quotes_num() == 0:
            self.add_symbol()

        num_before = self.get_quotes_num()

        for quote in quotes_dict:
            quote_id = self._add_base_quote_data(quote)

            if quote_id != 0:
                insert_core = f"""INSERT OR {self._update} INTO stock_core (quote_id, raw_close, dividends, split_coefficient)
                                VALUES (
                                    ({quote_id}),
                                    ({quote['raw_close']}),
                                    ({quote['divs']}),
                                    ({quote['split']})
                                );"""

                try:
                    self.cur.execute(insert_core)
                except self.Error as e:
                    raise FdataError(f"Can't add data to a table 'stock_core': {e}\n\nThe query is\n{insert_core}") from e

        self.commit()

        num_after = self.get_quotes_num()

        return (num_before, num_after)

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
        if self.get_symbol_quotes_num() == 0:
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
        if self.get_symbol_quotes_num() == 0:
            self.add_symbol()

        num_before = self.get_balance_sheet_num()

        for report in reports:
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
        if self.get_symbol_quotes_num() == 0:
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
        if self.get_symbol_quotes_num() == 0:
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

        return(num_before, self.get_earnings_num())

class StockFetcher(RWStockData, BaseFetcher, metaclass=abc.ABCMeta):
    """
        Abstract class to fetch quotes by API wrapper and add them to the database.
    """
    def _fetch_fundamentals_if_none(self, threshold, num_method, add_method, fetch_method, queries=None, pause=0):
        """
            Fetch all the available fundamental reports if data entries do not meet the specified threshold for
            specified interval (+- 500 days).

            Args:
                treshold(int): the minimum required number of quotes in the database.
                num_method(method): method to get the current reports number.
                add_method(method): method to add the reports to the database.
                fetch_method(method): method to fetch the reports.
                queries(list): additional data to get.
                pause(int): pause in seconds before fetching data (needed to avoid failure because of api keys limits).

            Returns:
                array: the fetched reports.
                int: the number of fetched reports.
        """
        initially_connected = self.is_connected()

        if self.is_connected() is False:
            self.db_connect()

        current_num = num_method()

        # Fetch reports if there are less than a threshold number of records in the database
        # for a selected timespan (+- 500 days)
        if current_num < threshold:
            time.sleep(pause)

            num_before, num_after = add_method(fetch_method())
            num = num_after - num_before

            if num < threshold:
                raise FdataError(f"Threshold {threshold} can't be met on specified date/time interval. Decrease the threshold.")
        else:
            num = 0

        rows = self.get_quotes(queries=queries)

        if initially_connected is False:
            self.db_close()

        return (rows, num)

    def fetch_income_statement_if_none(self, threshold, queries=None, pause=0):
        """
            Fetch all the available income statement reports if data entries do not meet the specified threshold for
            specified interval (+- 500 days).

            Args:
                treshold(int): the minimum required number of reports in the database.
                queries(list): additional data to get.
                pause(int): pause in seconds before fetching data (needed to avoid failure because of api keys limits).

            Returns:
                array: the fetched reports.
                int: the number of fetched reports.
        """
        return self._fetch_fundamentals_if_none(threshold=threshold,
                                                num_method=self.get_income_statement_num,
                                                add_method=self.add_income_statement,
                                                fetch_method=self.fetch_income_statement,
                                                queries=queries,
                                                pause=pause)

    def fetch_balance_sheet_if_none(self, threshold, queries=None, pause=0):
        """
            Fetch all the available balance sheet reports if data entries do not meet the specified threshold for
            specified interval (+- 500 days).

            Args:
                treshold(int): the minimum required number of reports in the database.
                queries(list): additional data to get.
                pause(int): pause in seconds before fetching data (needed to avoid failure because of api keys limits).

            Returns:
                array: the fetched reports.
                int: the number of fetched reports.
        """
        return self._fetch_fundamentals_if_none(threshold=threshold,
                                                num_method=self.get_balance_sheet_num,
                                                add_method=self.add_balance_sheet,
                                                fetch_method=self.fetch_balance_sheet,
                                                queries=queries,
                                                pause=pause)

    def fetch_cash_flow_if_none(self, threshold, queries=None, pause=0):
        """
            Fetch all the available cash flow reports if data entries do not meet the specified threshold for
            specified interval (+- 500 days).

            Args:
                treshold(int): the minimum required number of reports in the database.
                queries(list): additional data to get.
                pause(int): pause in seconds before fetching data (needed to avoid failure because of api keys limits).

            Returns:
                array: the fetched reports.
                int: the number of fetched reports.
        """
        return self._fetch_fundamentals_if_none(threshold=threshold,
                                                num_method=self.get_cash_flow_num,
                                                add_method=self.add_cash_flow,
                                                fetch_method=self.fetch_cash_flow,
                                                queries=queries,
                                                pause=pause)

    def fetch_earnings_if_none(self, threshold, queries=None, pause=0):
        """
            Fetch all the available earnings reports if data entries do not meet the specified threshold for
            specified interval (+- 500 days).

            Args:
                treshold(int): the minimum required number of reports in the database.
                queries(list): additional data to get.
                pause(int): pause in seconds before fetching data (needed to avoid failure because of api keys limits).

            Returns:
                array: the fetched reports.
                int: the number of fetched reports.
        """
        return self._fetch_fundamentals_if_none(threshold=threshold,
                                                num_method=self.get_earnings_num,
                                                add_method=self.add_earnings,
                                                fetch_method=self.fetch_earnings,
                                                queries=queries,
                                                pause=pause)

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
