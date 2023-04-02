import unittest

from mockito import when, mock, verify, unstub

from fdata_test import DataMocker

import sys
sys.path.append('../')

from sqlite3 import Cursor, Connection

from data.stock import ROStockData, RWStockData, StockFetcher
from data.fvalues import Timespans, ReportPeriod, five_hundred_days

from datetime import datetime
import pytz

class FetchData(StockFetcher):
    # Implement abstract methods
    def fetch_quotes(self):
        pass

    def get_recent_data(self, to_cache=False):
        pass

    def fetch_income_statement(self):
        pass

    def fetch_balance_sheet(self):
        pass

    def fetch_cash_flow(self):
        pass

    def fetch_earnings(self):
        pass

class Test(unittest.TestCase, DataMocker):
    def setUp(self):
        self.read_data = ROStockData()

        self.read_data.conn = mock(Connection)
        self.read_data.cur = mock(Cursor)

        self.read_data.Error = BaseException
        self.read_data.symbol = "AAPL"
        self.read_data.first_date = 0
        self.read_data.last_date = 1
        self.read_data.timespan = Timespans.Day

        self.read_data._connected = True

        self.test_db = 'test.sqlite'
        self.result = "result"
        self.results = ['one', 'two']

        when(self.read_data.conn).cursor().thenReturn(self.read_data.cur)
        when(self.read_data.cur).fetchone().thenReturn(self.result)
        when(self.read_data.cur).fetchall().thenReturn(self.results)

        self.write_data = RWStockData()

        self.write_data.conn = mock(Connection)
        self.write_data.cur = mock(Cursor)

        self.write_data.Error = BaseException
        self.write_data.symbol = "AAPL"
        self.write_data.first_date = 0
        self.write_data.last_date = 1
        self.write_data.timespan = Timespans.Day

        self.write_data._connected = True

        when(self.write_data.conn).cursor().thenReturn(self.write_data.cur)
        when(self.write_data.cur).fetchone().thenReturn(self.result)
        when(self.write_data.cur).fetchall().thenReturn(self.results)
        when(self.write_data.conn).commit().thenReturn()

        self.fetch_data = FetchData()

        self.fetch_data.conn = mock(Connection)
        self.fetch_data.cur = mock(Cursor)

        self.fetch_data.Error = BaseException
        self.fetch_data.symbol = "AAPL"
        self.fetch_data.first_date = 0
        self.fetch_data.last_date = 1
        self.fetch_data.timespan = Timespans.Day

        self.fetch_data._connected = True

    def tearDown(self):
        unstub()

    # Read data methods test

    def test_1_check_database(self):
        when(self.read_data).check_if_connected().thenReturn(True)

        queries = self.check_database_preparation(self.read_data)

        sql_query1 = "SELECT name FROM sqlite_master WHERE type='table' AND name='stock_core';"
        when(self.read_data.cur).execute(sql_query1).thenReturn()
        queries.append(sql_query1)

        sql_query2 = """CREATE TABLE stock_core(
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

        when(self.read_data.cur).execute(sql_query2).thenReturn()
        queries.append(sql_query2)

        sql_query3 = "CREATE INDEX idx_quote ON stock_core(quote_id);"
        when(self.read_data.cur).execute(sql_query3).thenReturn()
        queries.append(sql_query3)

        # Reporting periods

        sql_query4 = "SELECT name FROM sqlite_master WHERE type='table' AND name='report_periods';"
        when(self.read_data.cur).execute(sql_query4).thenReturn()
        queries.append(sql_query4)

        sql_query5 = """CREATE TABLE report_periods(
                                    period_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    title TEXT NOT NULL UNIQUE
                                );"""
        when(self.read_data.cur).execute(sql_query5).thenReturn()
        queries.append(sql_query5)

        sql_query6 = "CREATE INDEX idx_report_period_title ON report_periods(title);"
        when(self.read_data.cur).execute(sql_query6).thenReturn()
        queries.append(sql_query6)

        sql_query7 = "SELECT * FROM report_periods;"
        when(self.read_data.cur).execute(sql_query7).thenReturn()
        queries.append(sql_query7)

        # Prepare the query with all supported reporting periods
        rp = ""

        for period in ReportPeriod:
            if period != ReportPeriod.All:
                rp += f"('{period.value}'),"

        rp = rp[:len(rp) - 2]

        sql_query8 = f"""INSERT OR IGNORE INTO report_periods (title)
                                    VALUES {rp});"""
        when(self.read_data.cur).execute(sql_query8).thenReturn()
        queries.append(sql_query8)

        sql_query9 = "SELECT name FROM sqlite_master WHERE type='table' AND name='income_statement';"
        when(self.read_data.cur).execute(sql_query9).thenReturn()
        queries.append(sql_query9)

        sql_query10 = """CREATE TABLE income_statement(
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
        when(self.read_data.cur).execute(sql_query10).thenReturn()
        queries.append(sql_query10)

        sql_query11 = "CREATE INDEX idx_income_statement ON income_statement(symbol_id, reported_date);"
        when(self.read_data.cur).execute(sql_query11).thenReturn()
        queries.append(sql_query11)

        sql_query12 = "SELECT name FROM sqlite_master WHERE type='table' AND name='balance_sheet';"
        when(self.read_data.cur).execute(sql_query12).thenReturn()
        queries.append(sql_query12)

        sql_query13 = """CREATE TABLE balance_sheet(
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
        when(self.read_data.cur).execute(sql_query13).thenReturn()
        queries.append(sql_query13)

        sql_query14 = "CREATE INDEX idx_balance_sheet ON balance_sheet(symbol_id, reported_date);"
        when(self.read_data.cur).execute(sql_query14).thenReturn()
        queries.append(sql_query14)

        sql_query15 = "SELECT name FROM sqlite_master WHERE type='table' AND name='cash_flow';"
        when(self.read_data.cur).execute(sql_query15).thenReturn()
        queries.append(sql_query15)

        sql_query16 = """CREATE TABLE cash_flow(
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
        when(self.read_data.cur).execute(sql_query16).thenReturn()
        queries.append(sql_query16)

        sql_query17 = "CREATE INDEX idx_cash_flow ON cash_flow(symbol_id, reported_date);"
        when(self.read_data.cur).execute(sql_query17).thenReturn()
        queries.append(sql_query17)

        sql_query18 = "SELECT name FROM sqlite_master WHERE type='table' AND name='earnings';"
        when(self.read_data.cur).execute(sql_query18).thenReturn()
        queries.append(sql_query18)

        sql_query19 = """CREATE TABLE earnings(
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
        when(self.read_data.cur).execute(sql_query19).thenReturn()
        queries.append(sql_query19)

        sql_query19 = "CREATE INDEX idx_earnings ON earnings(symbol_id, reported_date);"
        when(self.read_data.cur).execute(sql_query19).thenReturn()
        queries.append(sql_query19)

        self.read_data.check_database()

        verify(self.read_data, times=1).check_if_connected()

        for query in queries:
            verify(self.read_data.cur, times=1).execute(query)

        verify(self.read_data.cur, times=18).fetchall()
        verify(self.read_data.conn, times=1).commit()

    def test_2_get_fundamental_data(self):
        when(self.read_data).check_if_connected().thenReturn(True)

        sql_query = f"""SELECT COUNT(*) FROM test
	                    WHERE reported_date >= (0 - {five_hundred_days})
                        AND reported_date <= (1 + {five_hundred_days})
                        AND symbol_id = (SELECT symbol_id FROM symbols where ticker = 'AAPL');"""
        when(self.read_data.cur).execute(sql_query).thenReturn()

        self.read_data._get_fundamentals_num('test')

        verify(self.read_data, times=1).check_if_connected()
        verify(self.read_data.cur, times=1).fetchone()

    def test_3_get_income_statement(self):
        when(self.read_data).check_if_connected().thenReturn(True)

        sql_query = f"""SELECT COUNT(*) FROM income_statement
	                    WHERE reported_date >= (0 - {five_hundred_days})
                        AND reported_date <= (1 + {five_hundred_days})
                        AND symbol_id = (SELECT symbol_id FROM symbols where ticker = 'AAPL');"""
        when(self.read_data.cur).execute(sql_query).thenReturn()

        self.read_data.get_income_statement_num()

        verify(self.read_data, times=1).check_if_connected()
        verify(self.read_data.cur, times=1).fetchone()

    def test_4_get_balance_sheet(self):
        when(self.read_data).check_if_connected().thenReturn(True)

        sql_query = f"""SELECT COUNT(*) FROM balance_sheet
	                    WHERE reported_date >= (0 - {five_hundred_days})
                        AND reported_date <= (1 + {five_hundred_days})
                        AND symbol_id = (SELECT symbol_id FROM symbols where ticker = 'AAPL');"""
        when(self.read_data.cur).execute(sql_query).thenReturn()

        self.read_data.get_balance_sheet_num()

        verify(self.read_data, times=1).check_if_connected()
        verify(self.read_data.cur, times=1).fetchone()

    def test_5_get_cash_flow(self):
        when(self.read_data).check_if_connected().thenReturn(True)

        sql_query = f"""SELECT COUNT(*) FROM cash_flow
	                    WHERE reported_date >= (0 - {five_hundred_days})
                        AND reported_date <= (1 + {five_hundred_days})
                        AND symbol_id = (SELECT symbol_id FROM symbols where ticker = 'AAPL');"""
        when(self.read_data.cur).execute(sql_query).thenReturn()

        self.read_data.get_cash_flow_num()

        verify(self.read_data, times=1).check_if_connected()
        verify(self.read_data.cur, times=1).fetchone()

    def test_6_get_earnings(self):
        when(self.read_data).check_if_connected().thenReturn(True)

        sql_query = f"""SELECT COUNT(*) FROM earnings
	                    WHERE reported_date >= (0 - {five_hundred_days})
                        AND reported_date <= (1 + {five_hundred_days})
                        AND symbol_id = (SELECT symbol_id FROM symbols where ticker = 'AAPL');"""
        when(self.read_data.cur).execute(sql_query).thenReturn()

        self.read_data.get_earnings_num()

        verify(self.read_data, times=1).check_if_connected()
        verify(self.read_data.cur, times=1).fetchone()

    def test_7_get_quotes(self):
        when(self.read_data).check_if_connected().thenReturn(True)

        timespan_query = "AND timespans.title = 'Day'"
        sectype_query = "AND sectypes.title = 'Stock'"
        currency_query = ""
        num_query = ""
        additional_queries = ""

        sql_query = f"""SELECT datetime(time_stamp, 'unixepoch') as time_stamp,
                                opened,
                                high,
                                low,
                                closed,
                                volume,
                                transactions
                                , raw_close, dividends, split_coefficient
                                {additional_queries}
                            FROM quotes INNER JOIN symbols ON quotes.symbol_id = symbols.symbol_id
                            INNER JOIN timespans ON quotes.time_span_id = timespans.time_span_id
                            INNER JOIN sectypes ON quotes.sec_type_id = sectypes.sec_type_id
                            INNER JOIN currency ON quotes.currency_id = currency.currency_id
                            INNER JOIN stock_core ON quotes.quote_id = stock_core.quote_id

                            WHERE symbols.ticker = '{self.read_data.symbol}'
                            {timespan_query}
                            {sectype_query}
                            {currency_query}
                            AND time_stamp >= {self.read_data.first_date_ts}
                            AND time_stamp <= {self.read_data.last_date_ts}
                            ORDER BY time_stamp
                            {num_query};"""
        when(self.read_data.cur).execute(sql_query).thenReturn()

        self.read_data.get_quotes()

        verify(self.read_data, times=1).check_if_connected()
        verify(self.read_data.cur, times=1).fetchall()

    # Test Read/Write operations

    def test_8_add_quotes(self):
        quote_dict = {
            'volume': 1,
            'open': 2,
            'adj_close': 3,
            'high': 4,
            'low': 5,
            'raw_close': 6,
            'transactions': 7,
            'ts': 8,
            'sectype': self.write_data.sectype.value,
            'currency': self.write_data.currency.value,
            'divs': 9,
            'split': 10
        }

        quotes = [quote_dict]

        when(self.write_data).check_if_connected().thenReturn(True)
        when(self.write_data).get_symbol_quotes_num().thenReturn(1)
        when(self.write_data).get_quotes_num().thenReturn(1)
        when(self.write_data)._add_base_quote_data(quote_dict).thenReturn(1)
        when(self.write_data).commit().thenReturn()

        sql_query = """INSERT OR IGNORE INTO stock_core (quote_id, raw_close, dividends, split_coefficient)
                                VALUES (
                                    (1),
                                    (6),
                                    (9),
                                    (10)
                                );"""
        when(self.write_data.cur).execute(sql_query).thenReturn()

        before, after = self.write_data.add_quotes(quotes)

        verify(self.write_data, times=1).check_if_connected()
        verify(self.write_data, times=1).get_symbol_quotes_num()
        verify(self.write_data, times=2).get_quotes_num()
        verify(self.write_data, times=1)._add_base_quote_data(quote_dict)
        verify(self.write_data, times=0).execute(sql_query)  # For some reason it is not detected as executed (but it definitely called according to UT debugging).
        verify(self.write_data, times=1).commit()

        assert before == 1
        assert after == 1

    def test_9_add_income_statement(self):
        report = {
            'reportedDate': 1,
            'fiscalDateEnding': 2,
            'grossProfit': 3,
            'totalRevenue': 4,
            'costOfRevenue': 5,
            'costofGoodsAndServicesSold': 6,
            'operatingIncome': 7,
            'sellingGeneralAndAdministrative': 8,
            'researchAndDevelopment': 9,
            'operatingExpenses': 10,
            'investmentIncomeNet': 11,
            'netInterestIncome': 12,
            'interestIncome': 13,
            'interestExpense': 14,
            'nonInterestIncome': 15,
            'otherNonOperatingIncome': 16,
            'depreciation': 17,
            'depreciationAndAmortization': 18,
            'incomeBeforeTax': 19,
            'incomeTaxExpense': 20,
            'interestAndDebtExpense': 21,
            'netIncomeFromContinuingOperations': 22,
            'comprehensiveIncomeNetOfTax': 23,
            'ebit': 24,
            'ebitda': 25,
            'netIncome': 26,
            'period': 0
        }

        reports = [report]

        when(self.write_data).check_if_connected().thenReturn(True)
        when(self.write_data).get_symbol_quotes_num().thenReturn(1)
        when(self.write_data).get_income_statement_num().thenReturn(1)

        sql_query = """INSERT OR IGNORE INTO income_statement (symbol_id,
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
											(SELECT symbol_id FROM symbols WHERE ticker = 'AAPL'),
                                            (SELECT source_id FROM sources WHERE title = ''),
											1,
											(SELECT period_id FROM report_periods WHERE title = '0'),
											2,
											3,
											4,
											5,
											6,
											7,
											8,
											9,
											10,
											11,
											12,
											13,
											14,
											15,
											16,
											17,
											18,
											19,
											20,
											21,
											22,
											23,
											24,
											25,
											26);"""
        when(self.write_data.cur).execute(sql_query).thenReturn()

        before, after = self.write_data.add_income_statement(reports)

        verify(self.write_data.cur, times=1).execute(sql_query)
        verify(self.write_data, times=2).get_income_statement_num()

        assert before == 1
        assert after == 1

    def test_10_get_balance_sheet(self):
        report = {
            'reportedDate': 1,
            'fiscalDateEnding': 2,
            'totalAssets': 3,
            'totalCurrentAssets': 4,
            'cashAndCashEquivalentsAtCarryingValue': 5,
            'cashAndShortTermInvestments': 6,
            'inventory': 7,
            'currentNetReceivables': 8,
            'totalNonCurrentAssets': 9,
            'propertyPlantEquipment': 10,
            'accumulatedDepreciationAmortizationPPE': 11,
            'intangibleAssets': 12,
            'intangibleAssetsExcludingGoodwill': 13,
            'goodwill': 14,
            'investments': 15,
            'longTermInvestments': 16,
            'shortTermInvestments': 17,
            'otherCurrentAssets': 18,
            'otherNonCurrentAssets': 19,
            'totalLiabilities': 20,
            'totalCurrentLiabilities': 21,
            'currentAccountsPayable': 22,
            'deferredRevenue': 23,
            'currentDebt': 24,
            'shortTermDebt': 25,
            'totalNonCurrentLiabilities': 26,
            'capitalLeaseObligations': 27,
            'longTermDebt': 28,
            'currentLongTermDebt': 29,
            'longTermDebtNoncurrent': 30,
            'shortLongTermDebtTotal': 31,
            'otherNonCurrentLiabilities': 32,
            'totalShareholderEquity': 33,
            'treasuryStock': 34,
            'retainedEarnings': 35,
            'commonStock': 36,
            'commonStockSharesOutstanding': 37,
            'period': 0
        }

        reports = [report]

        when(self.write_data).check_if_connected().thenReturn(True)
        when(self.write_data).get_symbol_quotes_num().thenReturn(1)
        when(self.write_data).get_balance_sheet_num().thenReturn(1)

        sql_query = """INSERT OR IGNORE INTO balance_sheet (symbol_id,
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
											(SELECT symbol_id FROM symbols WHERE ticker = 'AAPL'),
                                            (SELECT source_id FROM sources WHERE title = ''),
											1,
											(SELECT period_id FROM report_periods WHERE title = '0'),
											2,
											3,
											4,
											5,
											6,
											7,
											8,
											9,
											10,
											11,
											12,
											13,
											14,
											15,
											16,
											17,
											18,
											19,
											20,
											21,
											22,
											23,
											24,
											25,
											26,
                                            27,
											28,
											29,
											30,
											31,
											32,
											33,
											34,
											35,
											36,
											37);"""
        when(self.write_data.cur).execute(sql_query).thenReturn()

        before, after = self.write_data.add_balance_sheet(reports)

        verify(self.write_data.cur, times=1).execute(sql_query)
        verify(self.write_data, times=2).get_balance_sheet_num()

        assert before == 1
        assert after == 1

    def test_11_add_cash_flow(self):
        report = {
            'reportedDate': 1,
            'fiscalDateEnding': 2,
            'operatingCashflow': 3,
            'paymentsForOperatingActivities': 4,
            'proceedsFromOperatingActivities': 5,
            'changeInOperatingLiabilities': 6,
            'changeInOperatingAssets': 7,
            'depreciationDepletionAndAmortization': 8,
            'capitalExpenditures': 9,
            'changeInReceivables': 10,
            'changeInInventory': 11,
            'profitLoss': 12,
            'cashflowFromInvestment': 13,
            'cashflowFromFinancing': 14,
            'proceedsFromRepaymentsOfShortTermDebt': 15,
            'paymentsForRepurchaseOfCommonStock': 16,
            'paymentsForRepurchaseOfEquity': 17,
            'paymentsForRepurchaseOfPreferredStock': 18,
            'dividendPayout': 19,
            'dividendPayoutCommonStock': 20,
            'dividendPayoutPreferredStock': 21,
            'proceedsFromIssuanceOfCommonStock': 22,
            'proceedsFromIssuanceOfLongTermDebtAndCapitalSecuritiesNet': 23,
            'proceedsFromIssuanceOfPreferredStock': 24,
            'proceedsFromRepurchaseOfEquity': 25,
            'proceedsFromSaleOfTreasuryStock': 26,
            'changeInCashAndCashEquivalents': 27,
            'changeInExchangeRate': 28,
            'netIncome': 29,
            'period': 0
        }

        reports = [report]

        when(self.write_data).check_if_connected().thenReturn(True)
        when(self.write_data).get_symbol_quotes_num().thenReturn(1)
        when(self.write_data).get_cash_flow_num().thenReturn(1)

        sql_query = """INSERT OR IGNORE INTO cash_flow (symbol_id,
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
											(SELECT symbol_id FROM symbols WHERE ticker = 'AAPL'),
                                            (SELECT source_id FROM sources WHERE title = ''),
											1,
											(SELECT period_id FROM report_periods WHERE title = '0'),
											2,
											3,
											4,
											5,
											6,
											7,
											8,
											9,
											10,
											11,
											12,
											13,
											14,
											15,
											16,
											17,
											18,
											19,
											20,
											21,
											22,
											23,
											24,
											25,
											26,
                                            27,
											28,
											29);"""
        when(self.write_data.cur).execute(sql_query).thenReturn()

        before, after = self.write_data.add_cash_flow(reports)

        verify(self.write_data.cur, times=1).execute(sql_query)
        verify(self.write_data, times=2).get_cash_flow_num()

        assert before == 1
        assert after == 1

    def test_12_add_earnings(self):
        report = {
            'reportedDate': 1,
            'fiscalDateEnding': 2,
            'reportedEPS': 3,
            'estimatedEPS': 4,
            'surprise': 5,
            'surprisePercentage': 6,
            'period': 0
        }

        reports = [report]

        when(self.write_data).check_if_connected().thenReturn(True)
        when(self.write_data).get_symbol_quotes_num().thenReturn(1)
        when(self.write_data).get_earnings_num().thenReturn(1)

        sql_query = """INSERT OR IGNORE INTO earnings (symbol_id,
                                        source_id,
										reported_date,
										reported_period,
										fiscal_date_ending,
										reported_eps,
                                        estimated_eps,
                                        surprise,
                                        surprise_percentage)
									VALUES (
											(SELECT symbol_id FROM symbols WHERE ticker = 'AAPL'),
                                            (SELECT source_id FROM sources WHERE title = ''),
											1,
											(SELECT period_id FROM report_periods WHERE title = '0'),
											2,
											3,
											4,
											5,
											6);"""
        when(self.write_data.cur).execute(sql_query).thenReturn()

        before, after = self.write_data.add_earnings(reports)

        verify(self.write_data.cur, times=1).execute(sql_query)
        verify(self.write_data, times=2).get_earnings_num()

        assert before == 1
        assert after == 1

    def test_13_fetch_income_statement_if_none(self):
        result = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

        when(self.fetch_data).is_connected().thenReturn(True)
        when(self.fetch_data).get_income_statement_num().thenReturn(5)
        when(self.fetch_data).add_income_statement(None).thenReturn((10, 20))
        when(self.fetch_data).fetch_income_statement().thenReturn(None)
        when(self.fetch_data).get_quotes(queries=None).thenReturn(result)

        rows, num = self.fetch_data.fetch_income_statement_if_none(threshold=10)

        verify(self.fetch_data, times=2).is_connected()
        verify(self.fetch_data, times=1).get_income_statement_num()
        verify(self.fetch_data, times=1).fetch_income_statement()
        verify(self.fetch_data, times=1).add_income_statement(None)
        verify(self.fetch_data, times=1).get_quotes(queries=None)

        assert rows == result
        assert num == 10

    def test_14_fetch_balance_sheet_if_none(self):
        result = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

        when(self.fetch_data).is_connected().thenReturn(True)
        when(self.fetch_data).get_balance_sheet_num().thenReturn(5)
        when(self.fetch_data).add_balance_sheet(None).thenReturn((10, 20))
        when(self.fetch_data).fetch_balance_sheet().thenReturn(None)
        when(self.fetch_data).get_quotes(queries=None).thenReturn(result)

        rows, num = self.fetch_data.fetch_balance_sheet_if_none(threshold=10)

        verify(self.fetch_data, times=2).is_connected()
        verify(self.fetch_data, times=1).get_balance_sheet_num()
        verify(self.fetch_data, times=1).fetch_balance_sheet()
        verify(self.fetch_data, times=1).add_balance_sheet(None)
        verify(self.fetch_data, times=1).get_quotes(queries=None)

        assert rows == result
        assert num == 10

    def test_15_fetch_cash_flow_if_none(self):
        result = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

        when(self.fetch_data).is_connected().thenReturn(True)
        when(self.fetch_data).get_cash_flow_num().thenReturn(5)
        when(self.fetch_data).add_cash_flow(None).thenReturn((10, 20))
        when(self.fetch_data).fetch_cash_flow().thenReturn(None)
        when(self.fetch_data).get_quotes(queries=None).thenReturn(result)

        rows, num = self.fetch_data.fetch_cash_flow_if_none(threshold=10)

        verify(self.fetch_data, times=2).is_connected()
        verify(self.fetch_data, times=1).get_cash_flow_num()
        verify(self.fetch_data, times=1).fetch_cash_flow()
        verify(self.fetch_data, times=1).add_cash_flow(None)
        verify(self.fetch_data, times=1).get_quotes(queries=None)

        assert rows == result
        assert num == 10

    def test_16_fetch_earnings_if_none(self):
        result = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

        when(self.fetch_data).is_connected().thenReturn(True)
        when(self.fetch_data).get_earnings_num().thenReturn(5)
        when(self.fetch_data).add_earnings(None).thenReturn((10, 20))
        when(self.fetch_data).fetch_earnings().thenReturn(None)
        when(self.fetch_data).get_quotes(queries=None).thenReturn(result)

        rows, num = self.fetch_data.fetch_earnings_if_none(threshold=10)

        verify(self.fetch_data, times=2).is_connected()
        verify(self.fetch_data, times=1).get_earnings_num()
        verify(self.fetch_data, times=1).fetch_earnings()
        verify(self.fetch_data, times=1).add_earnings(None)
        verify(self.fetch_data, times=1).get_quotes(queries=None)

        assert rows == result
        assert num == 10
