"""AlphaVantage API wrapper.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from datetime import datetime
import pytz

from data import stock

from data.fvalues import Timespans, SecType, Currency
from data.fdata import FdataError

import pandas as pd
import numpy as np

import json

from data.futils import get_dt, get_labelled_ndarray

import settings

class AvSubquery():
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
                            WHERE fiscal_date_ending <= time_stamp
                            AND symbol_id = quotes.symbol_id
                            {self.condition}
                            ORDER BY fiscal_date_ending DESC LIMIT 1) AS {self.title}\n"""

        return subquery

class AVStock(stock.StockFetcher):
    """
        AlphaVantage API wrapper class.
    """
    def __init__(self, **kwargs):
        """Initialize the instance of AVStock class."""
        super().__init__(**kwargs)

        # Default values
        self.source_title = "AlphaVantage"
        self.api_key = settings.AV.api_key
        self.compact = False  # Indicates if a limited number (100) of quotes should be obtained

        self.sectype = SecType.Stock  # TODO LOW Distinguish stock and ETF for AV
        self.currency = Currency.Unknown  # Currencies are not supported yet

        # Cached EOD quotes to get dividends and split data
        self._eod = None
        self._eod_symbol = None

        if settings.AV.plan == settings.AV.Plan.Free:
            self.max_queries = 5
        if settings.AV.plan == settings.AV.Plan.Plan30:
            self.max_queries = 30
        if settings.AV.plan == settings.AV.Plan.Plan75:
            self.max_queries = 75
        if settings.AV.plan == settings.AV.Plan.Plan150:
            self.max_queries = 150
        if settings.AV.plan == settings.AV.Plan.Plan300:
            self.max_queries = 300
        if settings.AV.plan == settings.AV.Plan.Plan600:
            self.max_queries = 600
        if settings.AV.plan == settings.AV.Plan.Plan1200:
            self.max_queries = 1200

        if self.api_key is None:
            raise FdataError("API key is needed for this data source. Get your free API key at alphavantage.co and put it in setting.py")

        # Data related to fundamental tables
        self._fundamental_intervals_tbl = 'av_fundamental_intervals'
        self._income_statement_tbl = 'av_income_statement'
        self._balance_sheet_tbl = 'av_balance_sheet'
        self._cash_flow_tbl = 'av_cash_flow'

    def check_database(self):
        """
            Database create/integrity check method for stock data related tables.
            Checks if the database exists. Otherwise, creates it. Checks if the database has required tables.

            Raises:
                FdataError: sql error happened.
        """
        super().check_database()

        # Check if we need to create a table income_statement
        try:
            check_income_statement = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{self._income_statement_tbl}';"

            self.cur.execute(check_income_statement)
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table '{self._income_statement_tbl}': {e}\n{check_income_statement}") from e

        if len(rows) == 0:
            create_is = f"""CREATE TABLE {self._income_statement_tbl}(
                                av_is_report_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                source_id INTEGER NOT NULL,
                                symbol_id INTEGER NOT NULL,
                                reported_date INTEGER,
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
                raise FdataError(f"Can't execute a query on a table '{self._income_statement_tbl}': {e}\n{create_is}") from e

            # Create index for symbol_id
            create_symbol_date_is_idx = f"CREATE INDEX idx_{self._income_statement_tbl} ON {self._income_statement_tbl}(symbol_id, reported_date);"

            try:
                self.cur.execute(create_symbol_date_is_idx)
            except self.Error as e:
                raise FdataError(f"Can't create index {self._income_statement_tbl}(symbol_id, reported_date): {e}") from e

        # Check if we need to create a table balance_sheet
        try:
            check_balance_sheet = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{self._balance_sheet_tbl}';"

            self.cur.execute(check_balance_sheet)
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table '{self._balance_sheet_tbl}': {e}\n{check_balance_sheet}") from e

        if len(rows) == 0:
            create_bs = f"""CREATE TABLE {self._balance_sheet_tbl}(
                                av_bs_report_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                source_id INTEGER NOT NULL,
                                symbol_id INTEGER NOT NULL,
                                reported_date INTEGER,
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
                raise FdataError(f"Can't execute a query on a table '{self._balance_sheet_tbl}': {e}\n{create_bs}") from e

            # Create index for symbol_id
            create_symbol_date_bs_idx = f"CREATE INDEX idx_{self._balance_sheet_tbl} ON {self._balance_sheet_tbl}(symbol_id, reported_date);"

            try:
                self.cur.execute(create_symbol_date_bs_idx)
            except self.Error as e:
                raise FdataError(f"Can't create index {self._balance_sheet_tbl}(symbol_id, reported_date): {e}") from e

        # Check if we need to create a table cash_flow
        try:
            check_cash_flow = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{self._cash_flow_tbl}';"

            self.cur.execute(check_cash_flow)
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table '{self._cash_flow_tbl}': {e}\n{check_cash_flow}") from e

        # TODO LOW Get rid of tabulations above
        if len(rows) == 0:
            create_cf = f"""CREATE TABLE {self._cash_flow_tbl}(
                                av_cf_report_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                source_id INTEGER NOT NULL,
                                symbol_id INTEGER NOT NULL,
                                reported_date INTEGER,
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
                raise FdataError(f"Can't execute a query on a table '{self._cash_flow_tbl}': {e}\n{create_cf}") from e

            # Create index for symbol_id
            create_symbol_date_cf_idx = f"CREATE INDEX idx_{self._cash_flow_tbl} ON {self._cash_flow_tbl}(symbol_id, reported_date);"

            try:
                self.cur.execute(create_symbol_date_cf_idx)
            except self.Error as e:
                raise FdataError(f"Can't create index {self._cash_flow_tbl}(symbol_id, reported_date): {e}") from e

        # Check if we need to create a table earnings
        try:
            check_earnings = "SELECT name FROM sqlite_master WHERE type='table' AND name='av_earnings';"

            self.cur.execute(check_earnings)
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table 'av_earnings': {e}\n{check_earnings}") from e

        if len(rows) == 0:
            create_earnings = """CREATE TABLE av_earnings(
                                    av_earnings_report_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    source_id INTEGER NOT NULL,
                                    symbol_id INTEGER NOT NULL,
                                    reported_date INTEGER,
                                    reported_period INTEGER NOT NULL,
                                    fiscal_date_ending INTEGER NOT NULL,
                                    reported_eps INTEGER,
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
                raise FdataError(f"Can't execute a query on a table 'av_earnings': {e}\n{create_earnings}") from e

            # Create index for symbol_id
            create_symbol_date_is_idx = "CREATE INDEX idx_av_earnings ON av_earnings(symbol_id, reported_date);"

            try:
                self.cur.execute(create_symbol_date_is_idx)
            except self.Error as e:
                raise FdataError(f"Can't create index av_earnings(symbol_id, reported_date): {e}") from e

        # Check if we need to create table 'fundamental_intervals'
        try:
            check_fundamental_intervals = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{self._fundamental_intervals_tbl}';"

            self.cur.execute(check_fundamental_intervals)
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table '{self._fundamental_intervals_tbl}': {e}\n{check_fundamental_intervals}") from e

        if len(rows) == 0:
            create_fundamental_intervals = f"""CREATE TABLE {self._fundamental_intervals_tbl} (
                                                av_f_interval_id INTEGER PRIMARY KEY AUTOINCREMENT,
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
                raise FdataError(f"Can't create table {self._fundamental_intervals_tbl}: {e}") from e

            # Create indexes for fundamental_intervals
            create_fundamental_intervals_idx = f"CREATE INDEX idx_{self._fundamental_intervals_tbl} ON {self._fundamental_intervals_tbl}(symbol_id, source_id);"

            try:
                self.cur.execute(create_fundamental_intervals_idx)
            except self.Error as e:
                raise FdataError(f"Can't create indexes for {self._fundamental_intervals_tbl} table: {e}") from e

        # TODO LOW Unite it with general fundamentals table
        # Check if we need to create table 'earnings_intervals'
        try:
            check_earnings_intervals = "SELECT name FROM sqlite_master WHERE type='table' AND name='av_earnings_intervals';"

            self.cur.execute(check_earnings_intervals)
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table 'av_earnings_intervals': {e}\n{check_earnings_intervals}") from e

        if len(rows) == 0:
            create_earnings_intervals = """CREATE TABLE av_earnings_intervals (
                                                av_e_interval_id INTEGER PRIMARY KEY AUTOINCREMENT,
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
                raise FdataError(f"Can't create table av_earnings_intervals: {e}") from e

            # Create indexes for earnings_intervals
            create_earnings_intervals_idx = "CREATE INDEX idx_av_earnings_intervals ON av_earnings_intervals(symbol_id, source_id);"

            try:
                self.cur.execute(create_earnings_intervals_idx)
            except self.Error as e:
                raise FdataError(f"Can't create indexes for av_earnings_intervals table: {e}") from e

    ##########################
    # Fundamental data methods
    ##########################

    def get_earnings_num(self):
        """Get the number of earnings reports.

            Returns:
                int: the number of earnings entries in the database.

            Raises:
                FdataError: sql error happened.
        """
        return self._get_data_num('av_earnings')

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
            insert_report = f"""INSERT OR {self._update} INTO {self._income_statement_tbl} (symbol_id,
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

        self._update_intervals('income_statement_max_ts', self._fundamental_intervals_tbl)

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
            insert_report = f"""INSERT OR {self._update} INTO {self._balance_sheet_tbl} (symbol_id,
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

        self._update_intervals('balance_sheet_max_ts', self._fundamental_intervals_tbl)

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
            insert_report = f"""INSERT OR {self._update} INTO {self._cash_flow_tbl} (symbol_id,
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

        self._update_intervals('cash_flow_max_ts', self._fundamental_intervals_tbl)

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
            insert_report = f"""INSERT OR {self._update} INTO av_earnings (symbol_id,
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
                raise FdataError(f"Can't add a record to a table 'av_earnings': {e}\n\nThe query is\n{insert_report}") from e

        self.commit()

        self._update_intervals('earnings_max_ts', 'av_earnings_intervals')

        return(num_before, self.get_earnings_num())

    def get_earnings(self):
        """
            Fetch all the available earnings reports if needed.

            Returns:
                array: the fetched reports.
                int: the number of fetched reports.
        """
        return self._fetch_data_if_none(column='earnings_max_ts',
                                        interval_table='av_earnings_intervals',
                                        data_table='av_earnings',
                                        num_method=self.get_earnings_num,
                                        add_method=self.add_earnings,
                                        fetch_method=self.fetch_earnings)

    ##########################
    # Fetching-related methods
    ##########################

    def get_timespan_str(self):
        """
            Get the timespan.

            Converts universal timespan to AlphaVantage timespan.

            Raises:
                FdataError: incorrect/unsupported timespan requested.

            Returns:
                str: timespan for AV query.
        """
        if self.timespan == Timespans.Minute:
            return '1min'
        elif self.timespan == Timespans.FiveMinutes:
            return '5min'
        elif self.timespan == Timespans.FifteenMinutes:
            return '15min'
        elif self.timespan == Timespans.ThirtyMinutes:
            return '30min'
        elif self.timespan == Timespans.Hour:
            return '60min'
        else:
            raise FdataError(f"Unsupported timespan: {self.timespan.value}")

    # TODO LOW Think if it is ever needed
    def is_intraday(self, timespan=None):
        """
            Determine if the current timespan is intraday.

            Args:
                timespan(Timespan): timespan to override.

            Returns:
                bool: if the current timespan is intraday.
        """
        if timespan is None:
            timespan = self.timespan

        if timespan in (Timespans.Minute,
                        Timespans.FiveMinutes,
                        Timespans.FifteenMinutes,
                        Timespans.ThirtyMinutes,
                        Timespans.Hour):
            return True
        elif timespan == Timespans.Day:
            return False
        else:
            raise FdataError(f"Unsupported timespan: {timespan}")

    def query_and_parse(self, url, timeout=30):
        """
            Query the data source and parse the response.

            Args:
                url(str): the url for a request.
                timeout(int): timeout for the request.

            Returns:
                Parsed data.
        """
        response = self.query_api(url, timeout)

        try:
            json_data = response.json()
        except json.decoder.JSONDecodeError as e:
            raise FdataError(f"Can't parse JSON. Likely API key limit reached: {e}") from e

        return json_data

    def get_intraday_url(self, year, month):
        """
            Get the url for an intraday query.

            Args:
                year(int): The year to get data
                month(int): The month to get data

            Returns(string): url for an intraday query
        """
        output_size = 'compact' if self.compact else 'full'

        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={self.symbol}&interval={self.get_timespan_str()}&outputsize={output_size}&adjusted=false&month={year}-{str(month).zfill(2)}&apikey={self.api_key}'

        return url

    def get_quote_json(self, url, json_key):
        """
            Get quote json data.

            Args:
                url(string): url to get data
                json_key(string); json key to get data.

            Raises:
                FdataError: no data obtained as likely API key limit is reached.

            Returns:
                dictionaries: quotes data and a header
        """
        json_data = self.query_and_parse(url)

        try:
            dict_header = dict(json_data['Meta Data'].items())
            dict_results = dict(sorted(json_data[json_key].items()))
        except KeyError:
            # It is possible that just there is not data yet for the current month
            self.log(f"Can't get data for {self.symbol} using {self.source_title}. Likely API key limit or just no data for the requested period.")
            return (None, None)

        return (dict_results, dict_header)

    def fetch_quotes(self, first_ts=None, last_ts=None):
        """
            The method to fetch quotes.

            Args:
                first_ts(int): overridden first ts to fetch.
                last_ts(int): overridden last ts to fetch.

            Raises:
                FdataError: incorrect API key(limit reached), http error happened, invalid timespan or no data obtained.

            Returns:
                list: quotes data
        """
        quotes_data = []

        # At first, need to set a function depending on a timespan.
        if self.is_intraday():
            json_key = f'Time Series ({self.get_timespan_str()})'

            # Year and month
            if first_ts is None:
                first_date = self.first_date
            else:
                first_date = get_dt(first_ts, pytz.UTC)

            if last_ts is None:
                last_date = self.last_date
            else:
                last_date = get_dt(last_ts, pytz.UTC)

            year = first_date.year
            month = first_date.month

            if year < 2000:
                year = 2000
                month = 1

            url = self.get_intraday_url(year, month)

        else:
            output_size = 'compact' if self.compact else 'full'
            json_key = 'Time Series (Daily)'

            url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={self.symbol}&outputsize={output_size}&apikey={self.api_key}'

        # Get quotes data
        dict_results, dict_header = self.get_quote_json(url, json_key)

        # Get the time zone
        if self.is_intraday():
            tz_str = dict_header['6. Time Zone']
        else:
            tz_str = dict_header['5. Time Zone']

        tz = pytz.timezone(tz_str)

        while self.is_intraday() and (year <= last_date.year and month < last_date.month):
            month += 1

            if month == 13:
                year += 1
                month = 1

            new_dict, _ = self.get_quote_json(self.get_intraday_url(year, month), json_key)

            if new_dict is not None:
                dict_results.update(new_dict)

        datetimes = list(dict_results.keys())

        if len(datetimes) == 0:
            raise FdataError("No data obtained.")

        for dt_str in datetimes:
            try:
                dt = get_dt(dt_str, tz)  # Get UTC-adjusted datetime

                if self.is_intraday() is False:
                    # Keep all non-intraday timestamps at 23:59:59
                    dt = dt.replace(hour=23, minute=59, second=59)
            except ValueError as e:
                raise FdataError(f"Can't parse the datetime {dt_str}: {e}") from e

            # The current quote to process
            quote = dict_results[dt_str]

            quote_dict = {
                'ts': int(datetime.timestamp(dt)),
                'open': quote['1. open'],
                'high': quote['2. high'],
                'low': quote['3. low'],
                'close': quote['4. close'],
                'volume': quote['5. volume'],
                'transactions': 'NULL'
            }

            quotes_data.append(quote_dict)

        return quotes_data

    def _fetch_fundamentals(self, function):
        """
            Fetch stock fundamentals

            Args:
                function(str): the function to use

            Raises:
                FdataError: incorrect API key(limit reached), http error happened or no data obtained.

            Returns:
                list: fundamental data
        """
        url = f'https://www.alphavantage.co/query?function={function}&symbol={self.symbol}&apikey={self.api_key}'

        # Get fundamental data
        json_data = self.query_and_parse(url)

        try:
            annual_reports = pd.json_normalize(json_data['annualReports'])
            quarterly_reports = pd.json_normalize(json_data['quarterlyReports'])
        except KeyError as e:
            raise FdataError(f"Can't parse results. Likely because of API key limit: {e}") from e

        annual_reports['period'] = 'Year'
        quarterly_reports['period'] = 'Quarter'

        # Merge and sort reports
        reports = pd.concat([annual_reports, quarterly_reports], ignore_index=True)
        reports = reports.sort_values(by=['fiscalDateEnding'], ignore_index=True)

        # Delete reported currency
        reports = reports.drop(labels="reportedCurrency", axis=1)

        # Replace string datetime to timestamp
        reports['fiscalDateEnding'] = reports['fiscalDateEnding'].apply(get_dt)
        reports['fiscalDateEnding'] = reports['fiscalDateEnding'].apply(lambda x: int(datetime.timestamp(x)))

        reports['reportedDate'] = np.nan

        # Replace AV "None" to SQL 'NULL'
        reports = reports.replace(['None'], 'NULL')
        # Replave Python None to SQL 'NULL'
        reports = reports.fillna(value='NULL')

        # Convert dataframe to dictionary
        fundamental_results = reports.T.to_dict().values()

        return fundamental_results

    def fetch_income_statement(self):
        """
            Fetches the income statement.

            Raises:
                FdataError: incorrect API key(limit reached), http error happened or no data obtained.

            Returns:
                list: fundamental data
        """
        return self._fetch_fundamentals('INCOME_STATEMENT')

    def fetch_balance_sheet(self):
        """
            Fetches the balance sheet.

            Raises:
                FdataError: incorrect API key(limit reached), http error happened or no data obtained.

            Returns:
                list: fundamental data
        """
        return self._fetch_fundamentals('BALANCE_SHEET')

    def fetch_cash_flow(self):
        """
            Fetches the cash flow.

            Raises:
                FdataError: incorrect API key(limit reached), http error happened or no data obtained.

            Returns:
                list: fundamental data
        """
        return self._fetch_fundamentals('CASH_FLOW')

    # TODO LOW Think if the behavior above is correct.
    # If eventually reportedEPS is None (sometimes it is possible because of API issue), it won't be added to the DB.
    # However, it may be used for reported date estimation for other reports.
    def fetch_earnings(self):
        """
            Fetch stock earnings

            Raises:
                FdataError: incorrect API key(limit reached), http error happened or no data obtained.

            Returns:
                list: earnings data
        """
        url = f'https://www.alphavantage.co/query?function=EARNINGS&symbol={self.symbol}&apikey={self.api_key}'

        # Get earnings data
        json_data = self.query_and_parse(url)

        try:
            annual_earnings = pd.json_normalize(json_data['annualEarnings'])
            quarterly_earnings = pd.json_normalize(json_data['quarterlyEarnings'])
        except KeyError as e:
            raise FdataError(f"Can't parse results. Likely because of API key limit: {e}") from e

        # Convert reported date to UTC-adjusted timestamp
        quarterly_earnings['reportedDate'] = quarterly_earnings['reportedDate'].apply(get_dt)
        quarterly_earnings['reportedDate'] = quarterly_earnings['reportedDate'].apply(lambda x: int(datetime.timestamp(x)))

        # Add reporting date to annual earnings
        quarterly_earnings = quarterly_earnings.set_index('fiscalDateEnding')
        annual_earnings = annual_earnings.set_index('fiscalDateEnding')

        annual_earnings = pd.merge(annual_earnings, quarterly_earnings, left_index=True, right_index=True)
        annual_earnings['reportedEPS'] = annual_earnings['reportedEPS_x']
        annual_earnings = annual_earnings.drop(['estimatedEPS', 'surprise', 'surprisePercentage', 'reportedEPS_y', 'reportedEPS_x'], axis=1)

        annual_earnings = annual_earnings.reset_index()
        quarterly_earnings = quarterly_earnings.reset_index()

        annual_earnings['period'] = 'Year'
        quarterly_earnings['period'] = 'Quarter'

        # Merge and sort earnings reports
        earnings = pd.concat([annual_earnings, quarterly_earnings], ignore_index=True)
        earnings = earnings.sort_values(by=['fiscalDateEnding'], ignore_index=True)

        # Replace string datetime to timestamp
        earnings['fiscalDateEnding'] = earnings['fiscalDateEnding'].apply(get_dt)
        earnings['fiscalDateEnding'] = earnings['fiscalDateEnding'].apply(lambda x: int(datetime.timestamp(x)))

        # Replace AV "None" to SQL 'NULL'
        earnings = earnings.replace(['None'], 'NULL')
        # Replave Python None to SQL 'NULL'
        earnings = earnings.fillna(value='NULL')

        # Convert dataframe to dictionary
        earnings_results = earnings.T.to_dict().values()

        return earnings_results

    def get_recent_data(self, to_cache=False):
        """
            Get delayed quote.

            Args:
                to_cache(bool): indicates if real time data should be cached in a database.

            Raises:
                FdataErorr: markets are closed or network error has happened.

            Returns:
                list: real time data.
        """
        url = f'https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={self.symbol}&apikey={self.api_key}'

        # Get recent quote
        response = self.query_api(url)

        # Get json
        json_data = response.json()

        try:
            quote = json_data['Global Quote']
        except KeyError as e:
            raise FdataError(f"Can't parse data. Maybe API key limit is reached: {e}") from e

        # Use the current time as a timestamp
        dt = datetime.now()
        # Always keep datetimes in UTC time zone!
        dt = dt.replace(tzinfo=pytz.utc)
        ts = int(dt.timestamp())

        result = {'time_stamp': ts,
                  'date_time': get_dt(ts).replace(microsecond=0).isoformat(' '),
                  'opened': quote['02. open'],
                  'high': quote['03. high'],
                  'low': quote['04. low'],
                  'closed': quote['05. price'],
                  'volume': quote['06. volume'],
                  'transactions': None,
                  'adj_close': quote['05. price'],
                  'divs_ex': 0.0,
                  'divs_pay': 0.0,
                  'splits': 1.0
                 }

        result = [result]
        result = get_labelled_ndarray(result)

        return result

    def _get_eod_quotes(self):
        """
            Fetch EOD quotes if needed with dividends/splits data. Return cached data otherwise.

            Returns(list of dict): EOD quotes data.
        """
        if self._eod is None or self._eod_symbol != self.symbol:
            if settings.AV.plan == settings.AV.Plan.Free:
                raise FdataError("Daily adjusted data is not available in the free plan now.")

            json_key = 'Time Series (Daily)'

            url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={self.symbol}&outputsize=full&apikey={self.api_key}'

            # Demo key for testing split/divs fetching without a premium key
            #url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=IBM&outputsize=full&apikey=demo'

            # Get quotes data
            dict_results, dict_header = self.get_quote_json(url, json_key)

            # Get the time zone
            tz_str = dict_header['5. Time Zone']
            tz = pytz.timezone(tz_str)

            datetimes = list(dict_results.keys())

            if len(datetimes) == 0:
                raise FdataError("No data obtained.")

            self._eod = []

            for dt_str in datetimes:
                try:
                    dt = get_dt(dt_str, tz)  # Get UTC-adjusted datetime
                    # Keep all splits/dividend timestamps at 00:00:00
                    dt = dt.replace(hour=00, minute=00, second=00)
                except ValueError as e:
                    raise FdataError(f"Can't parse the datetime {dt_str}: {e}") from e

                # The current quote to process
                quote = dict_results[dt_str]

                quote_dict = {
                    'ts': int(datetime.timestamp(dt)),
                    'divs': quote['7. dividend amount'],
                    'split': quote['8. split coefficient']
                }

                self._eod.append(quote_dict)

        self._eod_symbol = self.symbol

        return self._eod

    def fetch_dividends(self):
        """
            Fetch cash dividends for the specified period.
        """
        quotes = self._get_eod_quotes()

        df = pd.DataFrame(quotes)

        df = df.loc[df['divs'].astype(float) > 0]

        df_result = pd.DataFrame()
        df_result['ex_ts'] = df['ts']
        df_result['amount'] = df['divs']

        # Not used in this data source
        df_result['currency'] = self.currency.value
        df_result['decl_ts'] = 'NULL'
        df_result['record_ts'] = 'NULL'
        df_result['pay_ts'] = 'NULL'

        return df_result.T.to_dict().values()

    def fetch_splits(self):
        """
            Fetch stock splits for the specified period.
        """
        quotes = self._get_eod_quotes()

        df = pd.DataFrame(quotes)

        df = df.loc[df['split'].astype(float) != 1]

        df_result = pd.DataFrame()
        df_result['ts'] = df['ts']
        df_result['split_ratio'] = df['split']

        return df_result.T.to_dict().values()
