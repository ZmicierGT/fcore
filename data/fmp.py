"""FMP (Financial Modeling Prep) API wrapper.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from datetime import datetime, timezone
from dateutil import tz
import calendar
import json

import pandas as pd

import settings

from data import stock
from data.fvalues import StrEnum, SecType, Timespans, Currency, ReportPeriod
from data.fdata import FdataError
from data.futils import get_dt, get_labelled_ndarray

class FMPDataEntries(StrEnum):
    """
        Enum class for FMP dataset entries with intervals tracking.
        The value is the name of the corresponding database table.
    """
    IncomeStatement = 'fmp_income_statement'
    BalanceSheet = 'fmp_balance_sheet'
    CashFlow = 'fmp_cash_flow'
    Capitalization = 'fmp_capitalization'


# Time zones of some popular exchanges for FMP data source
Exchanges = {
    'AMEX':     'America/New_York',
    'ETF':      'America/New_York',  # TODO MID It may be a problem with other regions
    'ASX':      'Australia/Sydney',
    'BSE':      'Asia/Kolkata',
    'EURONEXT': 'Europe/Paris',
    'HKSE':     'Asia/Hong_Kong',
    'JPX':      'Asia/Tokyo',
    'LSE':      'Europe/London',
    'NASDAQ':   'America/New_York',
    'NSE':      'Asia/Kolkata',
    'NYSE':     'America/New_York',
    'OTC':      'America/New_York',
    'PNK':      'America/New_York',
    'SSE':      'Asia/Shanghai',
    'SHH':      'Asia/Shanghai',
    'SHZ':      'Asia/Shanghai',
    'TSX':      'America/Toronto',
    'XETRA':    'Europe/Frankfurt'
}


class FMP(stock.StockData):
    """
        FMP API wrapper class.
    """
    def __init__(self, **kwargs):
        """
            Initialize the FMP wrapper class.
        """
        super().__init__(**kwargs)

        # Default values
        self._source_title = "FMP"

        # API key and other params should be configured in settings.py
        self._settings = settings.FMP()
        self._max_queries = self._settings.queries_per_min

        try:
            self._api_key = self._settings.api_key
        except RuntimeError:
            self._api_key = None
            self._log("Warning! No FMP API-KEY is configured in settings.py")

        self._stock_info_supported = True

        self._annual_report_supported = True
        self._quarter_report_supported = True

        # Data entries (also used as table names) for intervals tracking
        self._income_statement_entry = FMPDataEntries.IncomeStatement
        self._balance_sheet_entry = FMPDataEntries.BalanceSheet
        self._cash_flow_entry = FMPDataEntries.CashFlow

        # FMP-specific extras
        self._cap_entry = FMPDataEntries.Capitalization

    #######################################
    # Helpers
    #######################################

    def _query_and_parse(self, url, historical=False, timeout=30):
        """
            Query the data source and parse the response.

            Args:
                url(str): the url for a request.
                historical(bool): indicates if historical data is fetched (the
                    list is nested under the 'historical' key in that case).
                timeout(int): timeout for the request.

            Returns:
                Parsed data.

            Raises:
                FdataError: network error or no data obtained.
        """
        response = self._query_api(url, timeout=timeout)

        try:
            json_data = response.json()
        except (json.JSONDecodeError, ValueError) as e:
            raise FdataError(f"Can't parse json or no symbol found. Is API call limit reached? {e} URL: {url}") from e

        results = json_data

        if historical:
            try:
                results = json_data['historical']
            except (KeyError, TypeError) as e:
                self._log(f"Can't get the historical data when {url} is requested. Likely API key limit is reached: {e}")
                results = []

        if results is not None and (len(results) == 0 or results == ['Error Message']):
            # TODO MID Hide API keys from the log (here and in other places)
            self._log(f"No data obtained for {self._symbol} using the query {url}")

        return results

    def _get_timespan_str(self):
        """
            Get the timespan string for FMP queries (like '5min' and so on) based
            on the timespan specified in the datasource instance.

            Returns:
                str: timespan string.

            Raises:
                FdataError: incorrect/unsupported timespan requested.
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
            return '1hour'
        elif self.timespan == Timespans.Day:
            return '1d'
        else:
            raise FdataError(f"Requested timespan is not supported by {type(self).__name__}: {self.timespan}")

    ##########################
    # Quotes fetching
    ##########################

    def _fetch_quotes(self, first_ts=None, last_ts=None):
        """
            The method to fetch quotes.

            Args:
                first_ts(int): overridden first ts to fetch.
                last_ts(int): overridden last ts to fetch.

            Returns:
                list: quotes data.

            Raises:
                FdataError: incorrect API key(limit reached), http error happened,
                    invalid timespan or no data obtained.
        """
        # Adjust dates for the exchange time zone for the request.
        first_datetime, last_datetime = self._get_request_datetimes(first_ts, last_ts)

        first_date = first_datetime.date()
        last_date = last_datetime.date()

        # Intraday is not yet supported on the new /stable/ endpoints.
        if self.is_intraday():
            raise FdataError("Intraday timespan is not yet supported for FMP.")

        # New stable EOD endpoint: non-split-adjusted prices. Returns a flat list (not nested
        # under 'historical'). Fields: symbol, date, adjOpen, adjHigh, adjLow, adjClose, volume.
        url = (f"https://financialmodelingprep.com/stable/historical-price-eod/non-split-adjusted"
               f"?symbol={self._symbol}&from={first_date}&to={last_date}&apikey={self._api_key}")

        json_results = self._query_and_parse(url)

        if json_results is not None and (
                len(json_results) == 0 or json_results == ['Error Message'] or 'Error Message' in json_results):
            self._log(f"Unexpected data obtained. May be due the lack of API key or API key limit: {json_results}")
            return []

        quotes = []  # Processed quotes

        for quote in json_results:
            dt = get_dt(quote['date'], self.timezone)

            # No need to add quotes to DB which are outside of the requested interval.
            if dt.date() < first_date:
                break

            # Keep all non-intraday timestamps at 23:59:59.
            dt = dt.replace(hour=23, minute=59, second=59)
            volume = quote['volume']
            # New EOD non-split-adjusted endpoint exposes raw prices under adj* fields.
            open_val = quote['adjOpen']
            high_val = quote['adjHigh']
            low_val = quote['adjLow']
            close_val = quote['adjClose']

            ts = calendar.timegm(dt.utctimetuple())

            quote_dict = {
                'ts': ts,
                'open': open_val,
                'high': high_val,
                'low': low_val,
                'close': close_val,
                'volume': volume,
                'transactions': None,
            }

            quotes.append(quote_dict)

        if len(quotes) == 0:
            raise FdataError(f"No valid quotes obtained for {self._symbol}. The security may be delisted or the symbol is incorrect.")

        return quotes

    def get_recent_data(self, to_cache=False):
        """
            Get pseudo real time data. Used in screening demonstration.

            Args:
                to_cache(bool): indicates if real time data should be cached in a database.

            Returns:
                ndarray: real time data.
        """
        quote_url = f"https://financialmodelingprep.com/stable/quote?symbol={self._symbol}&apikey={self._api_key}"

        json_data = self._query_and_parse(quote_url)

        if not json_data or len(json_data) == 0 or json_data == ['Error Message']:
            raise FdataError(f"No quote data obtained for {self._symbol}")

        quote = json_data[0]

        # The timestamp is returned in UTC, not exchange time zone.
        dt = get_dt(quote['timestamp'], tz.UTC)

        result = {'time_stamp': calendar.timegm(dt.utctimetuple()),
                  'date_time': dt.isoformat(' '),
                  'opened': quote['open'],
                  'high': quote['dayHigh'],
                  'low': quote['dayLow'],
                  'closed': quote['price'],
                  'volume': int(quote['volume']),
                  'transactions': None,
                  'adj_open': quote['open'],
                  'adj_high': quote['dayHigh'],
                  'adj_low': quote['dayLow'],
                  'adj_close': quote['price'],
                  'adj_volume': int(quote['volume']),
                  'divs_ex': 0.0,
                  'divs_pay': 0.0,
                  'splits': 1.0
                 }

        # TODO LOW caching should be implemented

        return get_labelled_ndarray([result])

    ##########################################
    # Dividends and splits fetching
    ##########################################

    def _fetch_dividends(self):
        """
            Fetch the cash dividend data.

            Returns:
                list: dividend entries.

            Raises:
                FdataError: network error or no data obtained.
        """
        url_divs = (f"https://financialmodelingprep.com/stable/dividends?symbol={self._symbol}"
                    f"&apikey={self._api_key}")

        json_results = self._query_and_parse(url_divs)

        divs_data = []

        for div in json_results:
            decl_text = div['declarationDate']
            record_text = div['recordDate']
            pay_text = div['paymentDate']
            ex_text = div['date']

            # Declaration date
            decl_ts = None
            if decl_text != '':
                decl_date = get_dt(decl_text, self.timezone)
                decl_ts = calendar.timegm(decl_date.utctimetuple())

            # Ex-date can't be None
            ex_date = get_dt(ex_text, self.timezone)
            ex_ts = calendar.timegm(ex_date.utctimetuple())

            # Record date
            record_ts = None
            if record_text != '':
                record_date = get_dt(record_text, self.timezone)
                record_ts = calendar.timegm(record_date.utctimetuple())

            # Payment date
            pay_ts = None
            if pay_text != '':
                pay_date = get_dt(pay_text, self.timezone)
                pay_ts = calendar.timegm(pay_date.utctimetuple())

            div_dict = {
                'amount': div['dividend'],
                'decl_ts': decl_ts,
                'ex_ts': ex_ts,
                'record_ts': record_ts,
                'pay_ts': pay_ts,
                'currency': self.currency  # TODO LOW For now it is considered that dividend currency is the same as stock currency
            }

            divs_data.append(div_dict)

        return divs_data

    def _fetch_splits(self):
        """
            Fetch the split data.

            Returns:
                list: split entries.

            Raises:
                FdataError: network error or no data obtained.
        """
        url_splits = (f"https://financialmodelingprep.com/stable/splits?symbol={self._symbol}"
                      f"&apikey={self._api_key}")

        json_results = self._query_and_parse(url_splits)

        splits_data = []

        for split in json_results:
            dt = get_dt(split['date'], self.timezone)
            ts = calendar.timegm(dt.utctimetuple())

            numerator = int(split['numerator'])
            denominator = int(split['denominator'])
            split_ratio = numerator / denominator

            split_dict = {
                'ts': ts,
                'split_ratio': split_ratio,
            }

            splits_data.append(split_dict)

        return splits_data

    ###################
    # Info fetching
    ###################

    def _fetch_info(self):
        """
            Fetch stock related info.

            Returns:
                dict: stock info with fc_sec_type, fc_currency, fc_time_zone keys.

            Raises:
                FdataError: network error or no data obtained (API key limit is possible).
        """
        profile_url = f"https://financialmodelingprep.com/stable/profile?symbol={self._symbol}&apikey={self._api_key}"

        json_data = self._query_and_parse(profile_url)

        # NotExist fallback: no profile returned ([]) or the security is marked as not actively trading.
        not_exist = {'fc_sec_type': SecType.NotExist, 'fc_time_zone': 'UTC', 'fc_currency': Currency.Unknown}

        # A security is considered delisted/non-existent when the profile is empty ([]) or
        # the profile explicitly reports that the security is not actively trading.
        if json_data is None or len(json_data) == 0:
            self._log(f"Empty profile obtained for {self._symbol}. The security may be delisted or the symbol is incorrect. URL: {profile_url}")
            return not_exist

        results = json_data[0]

        if results.get('isActivelyTrading') is False:
            self._log(f"{self._symbol} is returned as not actively trading. The security may be delisted. URL: {profile_url}")
            return not_exist

        try:
            tz_str = Exchanges[results['exchange']]
        except KeyError:
            # Unknown exchange: use New York time zone as a fallback but log a warning
            self._log(f"WARNING: Unknown exchange '{results.get('exchange')}' for {self._symbol}."
                      f" Falling back to 'America/New_York' time zone. URL: {profile_url}")
            # TODO MID Think if we need to have an unknown exchange enum member which is treated as NY time zone
            tz_str = Exchanges['NYSE']

        results['fc_time_zone'] = tz_str

        # Determine the security type. The stable profile exposes isEtf/isFund/isAdr flags.
        if results.get('isEtf'):
            results['fc_sec_type'] = SecType.ETF
        else:
            results['fc_sec_type'] = SecType.Stock

        # Map the currency reported by FMP to the supported set of currencies.
        currency_str = results.get('currency')
        if currency_str is not None:
            try:
                results['fc_currency'] = Currency(currency_str)
            except ValueError:
                results['fc_currency'] = Currency.Unknown
        else:
            results['fc_currency'] = Currency.Unknown

        return results

    ############################################
    # Fundamental data fetching (per-source)
    ############################################

    def _fetch_fundamentals(self, report, reported_period='Year'):
        """
            Fetch stock fundamentals.

            Args:
                report(str): the report endpoint to use (e.g. 'income-statement').
                reported_period(str): the period to fetch (Year or Quarter).

            Returns:
                list: fundamental data.

            Raises:
                FdataError: incorrect API key(limit reached), http error happened or no data obtained.
        """
        url = (f"https://financialmodelingprep.com/stable/{report}?symbol={self._symbol}"
               f"&period={reported_period.lower()}&apikey={self._api_key}")

        json_data = self._query_and_parse(url)

        if isinstance(json_data, dict) and 'Error Message' in json_data:
            raise FdataError(json_data['Error Message'])

        # Avoid pd.json_normalize on an empty list which yields a columnless DataFrame
        # and would KeyError on the field access below. Data adding methods will handle the empty return further.
        if not isinstance(json_data, list) or len(json_data) == 0:
            return []

        reports = pd.json_normalize(json_data)

        reports['reported_period'] = reported_period

        # Replace string datetime with a timestamp.
        reports['time_stamp'] = reports['filingDate'].apply(get_dt)
        reports['time_stamp'] = reports['time_stamp'].apply(lambda x: int(datetime.timestamp(x)))

        reports['fiscalDate'] = reports['date'].apply(get_dt)
        reports['fiscalDate'] = reports['fiscalDate'].apply(lambda x: int(datetime.timestamp(x)))

        # Convert dataframe to a list of dictionaries.
        fundamental_results = list(reports.T.to_dict().values())

        return fundamental_results

    def _fetch_income_statement(self):
        """
            Fetch the income statement (both annual and quarterly reports).

            Returns:
                list: fundamental data.
        """
        return (self._fetch_fundamentals('income-statement', ReportPeriod.Year) +
                self._fetch_fundamentals('income-statement', ReportPeriod.Quarter))

    def _fetch_balance_sheet(self):
        """
            Fetch the balance sheet (both annual and quarterly reports).

            Returns:
                list: fundamental data.
        """
        return (self._fetch_fundamentals('balance-sheet-statement', ReportPeriod.Year) +
                self._fetch_fundamentals('balance-sheet-statement', ReportPeriod.Quarter))

    def _fetch_cash_flow(self):
        """
            Fetch the cash flow (both annual and quarterly reports).

            Returns:
                list: fundamental data.
        """
        return (self._fetch_fundamentals('cash-flow-statement', ReportPeriod.Year) +
                self._fetch_fundamentals('cash-flow-statement', ReportPeriod.Quarter))

    def _add_income_statement(self, reports):
        """
            Add income statement entries to the database.

            Args:
                reports(list of dictionaries): reports entries obtained from the API wrapper.

            Returns:
                (int, int): total number of report entries in DB before and after the operation.

            Raises:
                FdataError: sql error happened.
        """
        self._check_if_connected()

        # Insert new symbols to 'symbols' table (if the symbol does not exist)
        if not self.symbol_exists:
            self._add_symbol()

        num_before = self.get_income_statement_num()

        if not reports:
            self._log(f"No income statement data to add for {self._symbol}. Updating data interval only.")
            self._update_data_interval(self._income_statement_entry)
            return (num_before, num_before)

        insert_report = f"""INSERT INTO {self._income_statement_entry} (symbol_id,
                                    source_id,
                                    reported_period,
                                    time_stamp,
                                    fiscalDate,
                                    revenue,
                                    costOfRevenue,
                                    grossProfit,
                                    grossProfitRatio,
                                    researchAndDevelopmentExpenses,
                                    generalAndAdministrativeExpenses,
                                    sellingAndMarketingExpenses,
                                    sellingGeneralAndAdministrativeExpenses,
                                    otherExpenses,
                                    operatingExpenses,
                                    costAndExpenses,
                                    interestIncome,
                                    interestExpense,
                                    depreciationAndAmortization,
                                    ebitda,
                                    ebitdaratio,
                                    operatingIncome,
                                    operatingIncomeRatio,
                                    totalOtherIncomeExpensesNet,
                                    incomeBeforeTax,
                                    incomeBeforeTaxRatio,
                                    incomeTaxExpense,
                                    netIncome,
                                    netIncomeRatio,
                                    eps,
                                    epsdiluted,
                                    weightedAverageShsOut,
                                    weightedAverageShsOutDil)
                                VALUES (
                                    (SELECT symbol_id FROM symbols WHERE ticker = ?),
                                    (SELECT source_id FROM sources WHERE title = ?),
                                    (SELECT period_id FROM report_periods WHERE title = ?),
                                    ?,  -- time_stamp
                                    ?,  -- fiscalDate
                                    ?,  -- revenue
                                    ?,  -- costOfRevenue
                                    ?,  -- grossProfit
                                    ?,  -- grossProfitRatio
                                    ?,  -- researchAndDevelopmentExpenses
                                    ?,  -- generalAndAdministrativeExpenses
                                    ?,  -- sellingAndMarketingExpenses
                                    ?,  -- sellingGeneralAndAdministrativeExpenses
                                    ?,  -- otherExpenses
                                    ?,  -- operatingExpenses
                                    ?,  -- costAndExpenses
                                    ?,  -- interestIncome
                                    ?,  -- interestExpense
                                    ?,  -- depreciationAndAmortization
                                    ?,  -- ebitda
                                    ?,  -- ebitdaratio
                                    ?,  -- operatingIncome
                                    ?,  -- operatingIncomeRatio
                                    ?,  -- totalOtherIncomeExpensesNet
                                    ?,  -- incomeBeforeTax
                                    ?,  -- incomeBeforeTaxRatio
                                    ?,  -- incomeTaxExpense
                                    ?,  -- netIncome
                                    ?,  -- netIncomeRatio
                                    ?,  -- eps
                                    ?,  -- epsdiluted
                                    ?,  -- weightedAverageShsOut
                                    ?)  -- weightedAverageShsOutDil
                                ON CONFLICT(symbol_id, fiscalDate, reported_period)
                                DO UPDATE SET revenue = excluded.revenue,
                                              costOfRevenue = excluded.costOfRevenue,
                                              grossProfit = excluded.grossProfit,
                                              grossProfitRatio = excluded.grossProfitRatio,
                                              researchAndDevelopmentExpenses = excluded.researchAndDevelopmentExpenses,
                                              generalAndAdministrativeExpenses = excluded.generalAndAdministrativeExpenses,
                                              sellingAndMarketingExpenses = excluded.sellingAndMarketingExpenses,
                                              sellingGeneralAndAdministrativeExpenses = excluded.sellingGeneralAndAdministrativeExpenses,
                                              otherExpenses = excluded.otherExpenses,
                                              operatingExpenses = excluded.operatingExpenses,
                                              costAndExpenses = excluded.costAndExpenses,
                                              interestIncome = excluded.interestIncome,
                                              interestExpense = excluded.interestExpense,
                                              depreciationAndAmortization = excluded.depreciationAndAmortization,
                                              ebitda = excluded.ebitda,
                                              ebitdaratio = excluded.ebitdaratio,
                                              operatingIncome = excluded.operatingIncome,
                                              operatingIncomeRatio = excluded.operatingIncomeRatio,
                                              totalOtherIncomeExpensesNet = excluded.totalOtherIncomeExpensesNet,
                                              incomeBeforeTax = excluded.incomeBeforeTax,
                                              incomeBeforeTaxRatio = excluded.incomeBeforeTaxRatio,
                                              incomeTaxExpense = excluded.incomeTaxExpense,
                                              netIncome = excluded.netIncome,
                                              netIncomeRatio = excluded.netIncomeRatio,
                                              eps = excluded.eps,
                                              epsdiluted = excluded.epsdiluted,
                                              weightedAverageShsOut = excluded.weightedAverageShsOut,
                                              weightedAverageShsOutDil = excluded.weightedAverageShsOutDil;"""

        rows = (
            (self._symbol,
             self._source_title,
             report['reported_period'],
             int(report['time_stamp']),
             int(report['fiscalDate']),
             report.get('revenue'),
             report.get('costOfRevenue'),
             report.get('grossProfit'),
             report.get('grossProfitRatio'),
             report.get('researchAndDevelopmentExpenses'),
             report.get('generalAndAdministrativeExpenses'),
             report.get('sellingAndMarketingExpenses'),
             report.get('sellingGeneralAndAdministrativeExpenses'),
             report.get('otherExpenses'),
             report.get('operatingExpenses'),
             report.get('costAndExpenses'),
             report.get('interestIncome'),
             report.get('interestExpense'),
             report.get('depreciationAndAmortization'),
             report.get('ebitda'),
             report.get('ebitdaratio'),
             report.get('operatingIncome'),
             report.get('operatingIncomeRatio'),
             report.get('totalOtherIncomeExpensesNet'),
             report.get('incomeBeforeTax'),
             report.get('incomeBeforeTaxRatio'),
             report.get('incomeTaxExpense'),
             report.get('netIncome'),
             report.get('netIncomeRatio'),
             report.get('eps'),
             report.get('epsdiluted'),
             report.get('weightedAverageShsOut'),
             report.get('weightedAverageShsOutDil'))
            for report in reports
        )

        try:
            self._cur.executemany(insert_report, rows)
        except self._error as e:
            raise FdataError(f"Can't add a record to a table '{self._income_statement_entry}': {e}\n\nThe query is\n{insert_report}") from e

        self._commit()
        self._update_data_interval(self._income_statement_entry)

        return (num_before, self.get_income_statement_num())

    def _add_balance_sheet(self, reports):
        """
            Add balance sheet entries to the database.

            Args:
                reports(list of dictionaries): reports entries obtained from the API wrapper.

            Returns:
                (int, int): total number of report entries in DB before and after the operation.

            Raises:
                FdataError: sql error happened.
        """
        self._check_if_connected()

        # Insert new symbols to 'symbols' table (if the symbol does not exist)
        if not self.symbol_exists:
            self._add_symbol()

        num_before = self.get_balance_sheet_num()

        if not reports:
            self._log(f"No balance sheet data to add for {self._symbol}. Updating data interval only.")
            self._update_data_interval(self._balance_sheet_entry)
            return (num_before, num_before)

        insert_report = f"""INSERT INTO {self._balance_sheet_entry} (symbol_id,
                                    source_id,
                                    reported_period,
                                    time_stamp,
                                    fiscalDate,
                                    cashAndCashEquivalents,
                                    shortTermInvestments,
                                    cashAndShortTermInvestments,
                                    netReceivables,
                                    inventory,
                                    otherCurrentAssets,
                                    totalCurrentAssets,
                                    propertyPlantEquipmentNet,
                                    goodwill,
                                    intangibleAssets,
                                    goodwillAndIntangibleAssets,
                                    longTermInvestments,
                                    taxAssets,
                                    otherNonCurrentAssets,
                                    totalNonCurrentAssets,
                                    otherAssets,
                                    totalAssets,
                                    accountPayables,
                                    shortTermDebt,
                                    taxPayables,
                                    deferredRevenue,
                                    otherCurrentLiabilities,
                                    totalCurrentLiabilities,
                                    longTermDebt,
                                    deferredRevenueNonCurrent,
                                    deferredTaxLiabilitiesNonCurrent,
                                    otherNonCurrentLiabilities,
                                    totalNonCurrentLiabilities,
                                    otherLiabilities,
                                    capitalLeaseObligations,
                                    totalLiabilities,
                                    preferredStock,
                                    commonStock,
                                    retainedEarnings,
                                    accumulatedOtherComprehensiveIncomeLoss,
                                    othertotalStockholdersEquity,
                                    totalStockholdersEquity,
                                    totalEquity,
                                    totalLiabilitiesAndStockholdersEquity,
                                    minorityInterest,
                                    totalLiabilitiesAndTotalEquity,
                                    totalInvestments,
                                    totalDebt,
                                    netDebt)
                                VALUES (
                                    (SELECT symbol_id FROM symbols WHERE ticker = ?),
                                    (SELECT source_id FROM sources WHERE title = ?),
                                    (SELECT period_id FROM report_periods WHERE title = ?),
                                    ?,  -- time_stamp
                                    ?,  -- fiscalDate
                                    ?,  -- cashAndCashEquivalents
                                    ?,  -- shortTermInvestments
                                    ?,  -- cashAndShortTermInvestments
                                    ?,  -- netReceivables
                                    ?,  -- inventory
                                    ?,  -- otherCurrentAssets
                                    ?,  -- totalCurrentAssets
                                    ?,  -- propertyPlantEquipmentNet
                                    ?,  -- goodwill
                                    ?,  -- intangibleAssets
                                    ?,  -- goodwillAndIntangibleAssets
                                    ?,  -- longTermInvestments
                                    ?,  -- taxAssets
                                    ?,  -- otherNonCurrentAssets
                                    ?,  -- totalNonCurrentAssets
                                    ?,  -- otherAssets
                                    ?,  -- totalAssets
                                    ?,  -- accountPayables
                                    ?,  -- shortTermDebt
                                    ?,  -- taxPayables
                                    ?,  -- deferredRevenue
                                    ?,  -- otherCurrentLiabilities
                                    ?,  -- totalCurrentLiabilities
                                    ?,  -- longTermDebt
                                    ?,  -- deferredRevenueNonCurrent
                                    ?,  -- deferredTaxLiabilitiesNonCurrent
                                    ?,  -- otherNonCurrentLiabilities
                                    ?,  -- totalNonCurrentLiabilities
                                    ?,  -- otherLiabilities
                                    ?,  -- capitalLeaseObligations
                                    ?,  -- totalLiabilities
                                    ?,  -- preferredStock
                                    ?,  -- commonStock
                                    ?,  -- retainedEarnings
                                    ?,  -- accumulatedOtherComprehensiveIncomeLoss
                                    ?,  -- othertotalStockholdersEquity
                                    ?,  -- totalStockholdersEquity
                                    ?,  -- totalEquity
                                    ?,  -- totalLiabilitiesAndStockholdersEquity
                                    ?,  -- minorityInterest
                                    ?,  -- totalLiabilitiesAndTotalEquity
                                    ?,  -- totalInvestments
                                    ?,  -- totalDebt
                                    ?)  -- netDebt
                                ON CONFLICT(symbol_id, fiscalDate, reported_period)
                                DO UPDATE SET cashAndCashEquivalents = excluded.cashAndCashEquivalents,
                                              shortTermInvestments = excluded.shortTermInvestments,
                                              cashAndShortTermInvestments = excluded.cashAndShortTermInvestments,
                                              netReceivables = excluded.netReceivables,
                                              inventory = excluded.inventory,
                                              otherCurrentAssets = excluded.otherCurrentAssets,
                                              totalCurrentAssets = excluded.totalCurrentAssets,
                                              propertyPlantEquipmentNet = excluded.propertyPlantEquipmentNet,
                                              goodwill = excluded.goodwill,
                                              intangibleAssets = excluded.intangibleAssets,
                                              goodwillAndIntangibleAssets = excluded.goodwillAndIntangibleAssets,
                                              longTermInvestments = excluded.longTermInvestments,
                                              taxAssets = excluded.taxAssets,
                                              otherNonCurrentAssets = excluded.otherNonCurrentAssets,
                                              totalNonCurrentAssets = excluded.totalNonCurrentAssets,
                                              otherAssets = excluded.otherAssets,
                                              totalAssets = excluded.totalAssets,
                                              accountPayables = excluded.accountPayables,
                                              shortTermDebt = excluded.shortTermDebt,
                                              taxPayables = excluded.taxPayables,
                                              deferredRevenue = excluded.deferredRevenue,
                                              otherCurrentLiabilities = excluded.otherCurrentLiabilities,
                                              totalCurrentLiabilities = excluded.totalCurrentLiabilities,
                                              longTermDebt = excluded.longTermDebt,
                                              deferredRevenueNonCurrent = excluded.deferredRevenueNonCurrent,
                                              deferredTaxLiabilitiesNonCurrent = excluded.deferredTaxLiabilitiesNonCurrent,
                                              otherNonCurrentLiabilities = excluded.otherNonCurrentLiabilities,
                                              totalNonCurrentLiabilities = excluded.totalNonCurrentLiabilities,
                                              otherLiabilities = excluded.otherLiabilities,
                                              capitalLeaseObligations = excluded.capitalLeaseObligations,
                                              totalLiabilities = excluded.totalLiabilities,
                                              preferredStock = excluded.preferredStock,
                                              commonStock = excluded.commonStock,
                                              retainedEarnings = excluded.retainedEarnings,
                                              accumulatedOtherComprehensiveIncomeLoss = excluded.accumulatedOtherComprehensiveIncomeLoss,
                                              othertotalStockholdersEquity = excluded.othertotalStockholdersEquity,
                                              totalStockholdersEquity = excluded.totalStockholdersEquity,
                                              totalEquity = excluded.totalEquity,
                                              totalLiabilitiesAndStockholdersEquity = excluded.totalLiabilitiesAndStockholdersEquity,
                                              minorityInterest = excluded.minorityInterest,
                                              totalLiabilitiesAndTotalEquity = excluded.totalLiabilitiesAndTotalEquity,
                                              totalInvestments = excluded.totalInvestments,
                                              totalDebt = excluded.totalDebt,
                                              netDebt = excluded.netDebt;"""

        rows = (
            (self._symbol,
             self._source_title,
             report['reported_period'],
             int(report['time_stamp']),
             int(report['fiscalDate']),
             report.get('cashAndCashEquivalents'),
             report.get('shortTermInvestments'),
             report.get('cashAndShortTermInvestments'),
             report.get('netReceivables'),
             report.get('inventory'),
             report.get('otherCurrentAssets'),
             report.get('totalCurrentAssets'),
             report.get('propertyPlantEquipmentNet'),
             report.get('goodwill'),
             report.get('intangibleAssets'),
             report.get('goodwillAndIntangibleAssets'),
             report.get('longTermInvestments'),
             report.get('taxAssets'),
             report.get('otherNonCurrentAssets'),
             report.get('totalNonCurrentAssets'),
             report.get('otherAssets'),
             report.get('totalAssets'),
             report.get('accountPayables'),
             report.get('shortTermDebt'),
             report.get('taxPayables'),
             report.get('deferredRevenue'),
             report.get('otherCurrentLiabilities'),
             report.get('totalCurrentLiabilities'),
             report.get('longTermDebt'),
             report.get('deferredRevenueNonCurrent'),
             report.get('deferredTaxLiabilitiesNonCurrent'),
             report.get('otherNonCurrentLiabilities'),
             report.get('totalNonCurrentLiabilities'),
             report.get('otherLiabilities'),
             report.get('capitalLeaseObligations'),
             report.get('totalLiabilities'),
             report.get('preferredStock'),
             report.get('commonStock'),
             report.get('retainedEarnings'),
             report.get('accumulatedOtherComprehensiveIncomeLoss'),
             report.get('othertotalStockholdersEquity'),
             report.get('totalStockholdersEquity'),
             report.get('totalEquity'),
             report.get('totalLiabilitiesAndStockholdersEquity'),
             report.get('minorityInterest'),
             report.get('totalLiabilitiesAndTotalEquity'),
             report.get('totalInvestments'),
             report.get('totalDebt'),
             report.get('netDebt'))
            for report in reports
        )

        try:
            self._cur.executemany(insert_report, rows)
        except self._error as e:
            raise FdataError(f"Can't add a record to a table '{self._balance_sheet_entry}': {e}\n\nThe query is\n{insert_report}") from e

        self._commit()
        self._update_data_interval(self._balance_sheet_entry)

        return (num_before, self.get_balance_sheet_num())

    def _add_cash_flow(self, reports):
        """
            Add cash flow entries to the database.

            Args:
                reports(list of dictionaries): reports entries obtained from the API wrapper.

            Returns:
                (int, int): total number of report entries in DB before and after the operation.

            Raises:
                FdataError: sql error happened.
        """
        self._check_if_connected()

        # Insert new symbols to 'symbols' table (if the symbol does not exist)
        if not self.symbol_exists:
            self._add_symbol()

        num_before = self.get_cash_flow_num()

        if not reports:
            self._log(f"No cash flow data to add for {self._symbol}. Updating data interval only.")
            self._update_data_interval(self._cash_flow_entry)
            return (num_before, num_before)

        insert_report = f"""INSERT INTO {self._cash_flow_entry} (symbol_id,
                                    source_id,
                                    reported_period,
                                    time_stamp,
                                    fiscalDate,
                                    netIncome,
                                    depreciationAndAmortization,
                                    deferredIncomeTax,
                                    stockBasedCompensation,
                                    changeInWorkingCapital,
                                    accountsReceivables,
                                    inventory,
                                    accountsPayables,
                                    otherWorkingCapital,
                                    otherNonCashItems,
                                    netCashProvidedByOperatingActivities,
                                    investmentsInPropertyPlantAndEquipment,
                                    acquisitionsNet,
                                    purchasesOfInvestments,
                                    salesMaturitiesOfInvestments,
                                    otherInvestingActivites,
                                    netCashUsedForInvestingActivites,
                                    debtRepayment,
                                    commonStockIssued,
                                    commonStockRepurchased,
                                    dividendsPaid,
                                    otherFinancingActivites,
                                    netCashUsedProvidedByFinancingActivities,
                                    effectOfForexChangesOnCash,
                                    netChangeInCash,
                                    cashAtEndOfPeriod,
                                    cashAtBeginningOfPeriod,
                                    operatingCashFlow,
                                    capitalExpenditure,
                                    freeCashFlow)
                                VALUES (
                                    (SELECT symbol_id FROM symbols WHERE ticker = ?),
                                    (SELECT source_id FROM sources WHERE title = ?),
                                    (SELECT period_id FROM report_periods WHERE title = ?),
                                    ?,  -- time_stamp
                                    ?,  -- fiscalDate
                                    ?,  -- netIncome
                                    ?,  -- depreciationAndAmortization
                                    ?,  -- deferredIncomeTax
                                    ?,  -- stockBasedCompensation
                                    ?,  -- changeInWorkingCapital
                                    ?,  -- accountsReceivables
                                    ?,  -- inventory
                                    ?,  -- accountsPayables
                                    ?,  -- otherWorkingCapital
                                    ?,  -- otherNonCashItems
                                    ?,  -- netCashProvidedByOperatingActivities
                                    ?,  -- investmentsInPropertyPlantAndEquipment
                                    ?,  -- acquisitionsNet
                                    ?,  -- purchasesOfInvestments
                                    ?,  -- salesMaturitiesOfInvestments
                                    ?,  -- otherInvestingActivites
                                    ?,  -- netCashUsedForInvestingActivites
                                    ?,  -- debtRepayment
                                    ?,  -- commonStockIssued
                                    ?,  -- commonStockRepurchased
                                    ?,  -- dividendsPaid
                                    ?,  -- otherFinancingActivites
                                    ?,  -- netCashUsedProvidedByFinancingActivities
                                    ?,  -- effectOfForexChangesOnCash
                                    ?,  -- netChangeInCash
                                    ?,  -- cashAtEndOfPeriod
                                    ?,  -- cashAtBeginningOfPeriod
                                    ?,  -- operatingCashFlow
                                    ?,  -- capitalExpenditure
                                    ?)  -- freeCashFlow
                                ON CONFLICT(symbol_id, fiscalDate, reported_period)
                                DO UPDATE SET netIncome = excluded.netIncome,
                                              depreciationAndAmortization = excluded.depreciationAndAmortization,
                                              deferredIncomeTax = excluded.deferredIncomeTax,
                                              stockBasedCompensation = excluded.stockBasedCompensation,
                                              changeInWorkingCapital = excluded.changeInWorkingCapital,
                                              accountsReceivables = excluded.accountsReceivables,
                                              inventory = excluded.inventory,
                                              accountsPayables = excluded.accountsPayables,
                                              otherWorkingCapital = excluded.otherWorkingCapital,
                                              otherNonCashItems = excluded.otherNonCashItems,
                                              netCashProvidedByOperatingActivities = excluded.netCashProvidedByOperatingActivities,
                                              investmentsInPropertyPlantAndEquipment = excluded.investmentsInPropertyPlantAndEquipment,
                                              acquisitionsNet = excluded.acquisitionsNet,
                                              purchasesOfInvestments = excluded.purchasesOfInvestments,
                                              salesMaturitiesOfInvestments = excluded.salesMaturitiesOfInvestments,
                                              otherInvestingActivites = excluded.otherInvestingActivites,
                                              netCashUsedForInvestingActivites = excluded.netCashUsedForInvestingActivites,
                                              debtRepayment = excluded.debtRepayment,
                                              commonStockIssued = excluded.commonStockIssued,
                                              commonStockRepurchased = excluded.commonStockRepurchased,
                                              dividendsPaid = excluded.dividendsPaid,
                                              otherFinancingActivites = excluded.otherFinancingActivites,
                                              netCashUsedProvidedByFinancingActivities = excluded.netCashUsedProvidedByFinancingActivities,
                                              effectOfForexChangesOnCash = excluded.effectOfForexChangesOnCash,
                                              netChangeInCash = excluded.netChangeInCash,
                                              cashAtEndOfPeriod = excluded.cashAtEndOfPeriod,
                                              cashAtBeginningOfPeriod = excluded.cashAtBeginningOfPeriod,
                                              operatingCashFlow = excluded.operatingCashFlow,
                                              capitalExpenditure = excluded.capitalExpenditure,
                                              freeCashFlow = excluded.freeCashFlow;"""

        rows = (
            (self._symbol,
             self._source_title,
             report['reported_period'],
             int(report['time_stamp']),
             int(report['fiscalDate']),
             report.get('netIncome'),
             report.get('depreciationAndAmortization'),
             report.get('deferredIncomeTax'),
             report.get('stockBasedCompensation'),
             report.get('changeInWorkingCapital'),
             report.get('accountsReceivables'),
             report.get('inventory'),
             report.get('accountsPayables'),
             report.get('otherWorkingCapital'),
             report.get('otherNonCashItems'),
             report.get('netCashProvidedByOperatingActivities'),
             report.get('investmentsInPropertyPlantAndEquipment'),
             report.get('acquisitionsNet'),
             report.get('purchasesOfInvestments'),
             report.get('salesMaturitiesOfInvestments'),
             report.get('otherInvestingActivites'),
             report.get('netCashUsedForInvestingActivites'),
             report.get('debtRepayment'),
             report.get('commonStockIssued'),
             report.get('commonStockRepurchased'),
             report.get('dividendsPaid'),
             report.get('otherFinancingActivites'),
             report.get('netCashUsedProvidedByFinancingActivities'),
             report.get('effectOfForexChangesOnCash'),
             report.get('netChangeInCash'),
             report.get('cashAtEndOfPeriod'),
             report.get('cashAtBeginningOfPeriod'),
             report.get('operatingCashFlow'),
             report.get('capitalExpenditure'),
             report.get('freeCashFlow'))
            for report in reports
        )

        try:
            self._cur.executemany(insert_report, rows)
        except self._error as e:
            raise FdataError(f"Can't add a record to a table '{self._cash_flow_entry}': {e}\n\nThe query is\n{insert_report}") from e

        self._commit()
        self._update_data_interval(self._cash_flow_entry)

        return (num_before, self.get_cash_flow_num())

    ###################################################
    # Capitalization data processing (FMP-specific)
    ###################################################

    def get_cap_num(self):
        """Get the number of capitalization data entries.

            Returns:
                int: the number of capitalization data entries.

            Raises:
                FdataError: sql error happened.
        """
        initially_connected = self.is_connected

        if self.is_connected is False:
            self.db_connect()

        try:
            return self._get_data_num(self._cap_entry)
        finally:
            if initially_connected is False:
                self.db_close()

    def _fetch_cap(self, num=100000, first_ts=None, last_ts=None):
        """
            Fetch the capitalization data.

            The stable historical-market-capitalization endpoint does not support
            the 'from'/'to' date range params on the basic plan (they are
            premium-gated). Instead, a single request returns the most recent
            'num' entries. Client-side date filtering is not applied here — all
            available entries are returned and stored; the data_intervals
            mechanism tracks freshness.

            Args:
                num(int): the number of entries to limit the request.
                first_ts(int): overridden first ts to fetch (unused on basic plan).
                last_ts(int): overridden last ts to fetch (unused on basic plan).

            Returns:
                list: capitalization data.
        """
        cap_url = (f"https://financialmodelingprep.com/stable/historical-market-capitalization"
                   f"?symbol={self._symbol}&limit={num}&apikey={self._api_key}")

        return self._query_and_parse(cap_url)

    def _add_cap(self, results):
        """
            Add capitalization data to the database.

            Args:
                results(list): the capitalization data.

            Returns:
                (int, int): total number of entries before and after the operation.

            Raises:
                FdataError: sql error happened.
        """
        self._check_if_connected()

        # Insert new symbols to 'symbols' table (if the symbol does not exist)
        if not self.symbol_exists:
            self._add_symbol()

        num_before = self.get_cap_num()

        if not results:
            self._log(f"No capitalization data to add for {self._symbol}. Updating data interval only.")
            self._update_data_interval(self._cap_entry)
            return (num_before, num_before)

        insert_cap = f"""INSERT INTO {self._cap_entry} (symbol_id,
                                    source_id,
                                    time_stamp,
                                    cap)
                                VALUES (
                                    (SELECT symbol_id FROM symbols WHERE ticker = ?),
                                    (SELECT source_id FROM sources WHERE title = ?),
                                    ?,  -- time_stamp
                                    ?)  -- cap
                                ON CONFLICT(symbol_id, time_stamp)
                                DO UPDATE SET cap = excluded.cap;"""

        rows = []

        for result in results:
            # Need to convert date to a timestamp.
            try:
                dt = get_dt(result['date'], self.timezone).replace(hour=23, minute=59, second=59)
                ts = calendar.timegm(dt.utctimetuple())
            except TypeError as e:
                raise FdataError(f"Unexpected data. API key limit is possible. {e}") from e

            rows.append((self._symbol,
                         self._source_title,
                         ts,
                         result['marketCap']))

        try:
            self._cur.executemany(insert_cap, rows)
        except self._error as e:
            raise FdataError(f"Can't add a record to a table '{self._cap_entry}': {e}\n\nThe query is\n{insert_cap}") from e

        self._commit()
        self._update_data_interval(self._cap_entry)

        return (num_before, self.get_cap_num())

    def get_cap(self):
        """
            Fetch (if needed) the capitalization data.

            Returns:
                int: the number of fetched entries.
        """
        initially_connected = self.is_connected

        if self.is_connected is False:
            self.db_connect()

        try:
            return self._fetch_data_if_none(data_entry=self._cap_entry,
                                            num_method=self.get_cap_num,
                                            add_method=self._add_cap,
                                            fetch_method=self._fetch_cap)
        finally:
            if initially_connected is False:
                self.db_close()

    ###############################
    # Database integrity check
    ###############################

    def _check_database(self):
        """
            Database create/integrity check method for FMP-specific tables.

            Runs inside the BEGIN IMMEDIATE init transaction opened by
            db_connect(); no commits are issued here.

            Raises:
                FdataError: sql error happened.
        """
        super()._check_database()

        # Create table 'fmp_capitalization' if needed.
        create_capitalization = f"""CREATE TABLE IF NOT EXISTS {self._cap_entry}(
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
            self._cur.execute(create_capitalization)
        except self._error as e:
            raise FdataError(f"Can't execute a query on a table '{self._cap_entry}': {e}\n{create_capitalization}") from e

        create_cap_idx = f"CREATE INDEX IF NOT EXISTS idx_{self._cap_entry} ON {self._cap_entry}(symbol_id, time_stamp);"

        try:
            self._cur.execute(create_cap_idx)
        except self._error as e:
            raise FdataError(f"Can't create index {self._cap_entry}(symbol_id, time_stamp): {e}") from e

        # Create trigger to track last modified time.
        create_cap_trigger = f"""CREATE TRIGGER IF NOT EXISTS update_{self._cap_entry}
                                            BEFORE UPDATE
                                                ON {self._cap_entry}
                                        BEGIN
                                            UPDATE {self._cap_entry}
                                            SET modified = strftime('%s', 'now')
                                            WHERE fmp_cap_id = old.fmp_cap_id;
                                        END;"""

        try:
            self._cur.execute(create_cap_trigger)
        except self._error as e:
            raise FdataError(f"Can't create trigger for '{self._cap_entry}': {e}") from e

        # Create table 'fmp_income_statement' if needed.
        create_is = f"""CREATE TABLE IF NOT EXISTS {self._income_statement_entry} (
                            fmp_is_report_id INTEGER PRIMARY KEY AUTOINCREMENT,
                            source_id INTEGER NOT NULL,
                            symbol_id INTEGER NOT NULL,
                            reported_period INTEGER NOT NULL,
                            time_stamp INTEGER NOT NULL,
                            fiscalDate INTEGER,
                            revenue INTEGER,
                            costOfRevenue INTEGER,
                            grossProfit INTEGER,
                            grossProfitRatio REAL,
                            researchAndDevelopmentExpenses INTEGER,
                            generalAndAdministrativeExpenses INTEGER,
                            sellingAndMarketingExpenses INTEGER,
                            sellingGeneralAndAdministrativeExpenses INTEGER,
                            otherExpenses INTEGER,
                            operatingExpenses INTEGER,
                            costAndExpenses INTEGER,
                            interestIncome INTEGER,
                            interestExpense INTEGER,
                            depreciationAndAmortization INTEGER,
                            ebitda INTEGER,
                            ebitdaratio INTEGER,
                            operatingIncome INTEGER,
                            operatingIncomeRatio REAL,
                            totalOtherIncomeExpensesNet INTEGER,
                            incomeBeforeTax INTEGER,
                            incomeBeforeTaxRatio INTEGER,
                            incomeTaxExpense INTEGER,
                            netIncome INTEGER,
                            netIncomeRatio REAL,
                            eps REAL,
                            epsdiluted REAL,
                            weightedAverageShsOut INTEGER,
                            weightedAverageShsOutDil INTEGER,
                            UNIQUE(symbol_id, fiscalDate, reported_period)
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
            self._cur.execute(create_is)
        except self._error as e:
            raise FdataError(f"Can't execute a query on a table '{self._income_statement_entry}': {e}\n{create_is}") from e

        create_is_idx = f"CREATE INDEX IF NOT EXISTS idx_{self._income_statement_entry} ON {self._income_statement_entry}(symbol_id, time_stamp);"

        try:
            self._cur.execute(create_is_idx)
        except self._error as e:
            raise FdataError(f"Can't create index {self._income_statement_entry}(symbol_id, time_stamp): {e}") from e

        # Create table 'fmp_balance_sheet' if needed.
        create_bs = f"""CREATE TABLE IF NOT EXISTS {self._balance_sheet_entry} (
                            fmp_bs_report_id INTEGER PRIMARY KEY AUTOINCREMENT,
                            source_id INTEGER NOT NULL,
                            symbol_id INTEGER NOT NULL,
                            reported_period INTEGER NOT NULL,
                            time_stamp INTEGER NOT NULL,
                            fiscalDate INTEGER,
                            cashAndCashEquivalents INTEGER,
                            shortTermInvestments INTEGER,
                            cashAndShortTermInvestments INTEGER,
                            netReceivables INTEGER,
                            inventory INTEGER,
                            otherCurrentAssets INTEGER,
                            totalCurrentAssets INTEGER,
                            propertyPlantEquipmentNet INTEGER,
                            goodwill INTEGER,
                            intangibleAssets INTEGER,
                            goodwillAndIntangibleAssets INTEGER,
                            longTermInvestments INTEGER,
                            taxAssets INTEGER,
                            otherNonCurrentAssets INTEGER,
                            totalNonCurrentAssets INTEGER,
                            otherAssets INTEGER,
                            totalAssets INTEGER,
                            accountPayables INTEGER,
                            shortTermDebt INTEGER,
                            taxPayables INTEGER,
                            deferredRevenue INTEGER,
                            otherCurrentLiabilities INTEGER,
                            totalCurrentLiabilities INTEGER,
                            longTermDebt INTEGER,
                            deferredRevenueNonCurrent INTEGER,
                            deferredTaxLiabilitiesNonCurrent INTEGER,
                            otherNonCurrentLiabilities INTEGER,
                            totalNonCurrentLiabilities INTEGER,
                            otherLiabilities INTEGER,
                            capitalLeaseObligations INTEGER,
                            totalLiabilities INTEGER,
                            preferredStock INTEGER,
                            commonStock INTEGER,
                            retainedEarnings INTEGER,
                            accumulatedOtherComprehensiveIncomeLoss INTEGER,
                            othertotalStockholdersEquity INTEGER,
                            totalStockholdersEquity INTEGER,
                            totalEquity INTEGER,
                            totalLiabilitiesAndStockholdersEquity INTEGER,
                            minorityInterest INTEGER,
                            totalLiabilitiesAndTotalEquity INTEGER,
                            totalInvestments INTEGER,
                            totalDebt INTEGER,
                            netDebt INTEGER,
                            UNIQUE(symbol_id, fiscalDate, reported_period)
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
            self._cur.execute(create_bs)
        except self._error as e:
            raise FdataError(f"Can't execute a query on a table '{self._balance_sheet_entry}': {e}\n{create_bs}") from e

        create_bs_idx = f"CREATE INDEX IF NOT EXISTS idx_{self._balance_sheet_entry} ON {self._balance_sheet_entry}(symbol_id, time_stamp);"

        try:
            self._cur.execute(create_bs_idx)
        except self._error as e:
            raise FdataError(f"Can't create index {self._balance_sheet_entry}(symbol_id, time_stamp): {e}") from e

        # Create table 'fmp_cash_flow' if needed.
        create_cf = f"""CREATE TABLE IF NOT EXISTS {self._cash_flow_entry} (
                            fmp_cf_report_id INTEGER PRIMARY KEY AUTOINCREMENT,
                            source_id INTEGER NOT NULL,
                            symbol_id INTEGER NOT NULL,
                            reported_period INTEGER NOT NULL,
                            time_stamp INTEGER NOT NULL,
                            fiscalDate INTEGER,
                            netIncome INTEGER,
                            depreciationAndAmortization INTEGER,
                            deferredIncomeTax INTEGER,
                            stockBasedCompensation INTEGER,
                            changeInWorkingCapital INTEGER,
                            accountsReceivables INTEGER,
                            inventory INTEGER,
                            accountsPayables INTEGER,
                            otherWorkingCapital INTEGER,
                            otherNonCashItems INTEGER,
                            netCashProvidedByOperatingActivities INTEGER,
                            investmentsInPropertyPlantAndEquipment INTEGER,
                            acquisitionsNet INTEGER,
                            purchasesOfInvestments INTEGER,
                            salesMaturitiesOfInvestments INTEGER,
                            otherInvestingActivites INTEGER,
                            netCashUsedForInvestingActivites INTEGER,
                            debtRepayment INTEGER,
                            commonStockIssued INTEGER,
                            commonStockRepurchased INTEGER,
                            dividendsPaid INTEGER,
                            otherFinancingActivites INTEGER,
                            netCashUsedProvidedByFinancingActivities INTEGER,
                            effectOfForexChangesOnCash INTEGER,
                            netChangeInCash INTEGER,
                            cashAtEndOfPeriod INTEGER,
                            cashAtBeginningOfPeriod INTEGER,
                            operatingCashFlow INTEGER,
                            capitalExpenditure INTEGER,
                            freeCashFlow INTEGER,
                            UNIQUE(symbol_id, fiscalDate, reported_period)
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
            self._cur.execute(create_cf)
        except self._error as e:
            raise FdataError(f"Can't execute a query on a table '{self._cash_flow_entry}': {e}\n{create_cf}") from e

        create_cf_idx = f"CREATE INDEX IF NOT EXISTS idx_{self._cash_flow_entry} ON {self._cash_flow_entry}(symbol_id, time_stamp);"

        try:
            self._cur.execute(create_cf_idx)
        except self._error as e:
            raise FdataError(f"Can't create index {self._cash_flow_entry}(symbol_id, time_stamp): {e}") from e

        self._register_data_entries(FMPDataEntries)
