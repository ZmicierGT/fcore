"""FMP API wrapper.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from data import stock
from data.fvalues import SecType, Timespans, Exchanges
from data.fdata import FdataError

from data.futils import get_dt, get_labelled_ndarray

import settings

import numpy as np
import math

from datetime import datetime, timedelta
from dateutil import tz
import calendar

import json

# TODO HIGH If any fetching was interrupted (like due to API key limit) the exception (not warning) should be triggered
# as it may lead to incomplete data which won't be re-fetched (as intervals are set already).

# TODO MID Make subquery universal for any data source
class FmpSubquery():
    """
        Class which represents additional subqueries for optional data (fundamentals, global economic, customer data and so on).
    """
    def __init__(self, table, column, condition='', title=None, fill=True):
        """
            Initializes the instance of Subquery class.

            Args:
                table(str): table for subquery.
                column(str): column to obtain.
                condition(str): additional SQL condition for the subquery.
                title(str): optional title for the output column (the same as column name by default)
                fill(bool): Indicates if all rows should have the value. False if only a row with the most
                            suitable data (according to time stamp) should have it.
        """
        self.table = table
        self.column = column
        self.condition = condition
        self.fill = fill

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
        ts_query = ''

        if self.fill is False:
            ts_query = """ AND report_tbl.time_stamp >
                           (SELECT time_stamp FROM quotes qqq WHERE qqq.quote_id > quotes.quote_id ORDER BY qqq.quote_id ASC LIMIT 1)"""

        subquery = f"""(SELECT {self.column}
                            FROM {self.table} report_tbl
                            WHERE report_tbl.time_stamp <= quotes.time_stamp{ts_query}
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

        if settings.FMP.plan == settings.FMP.Plan.Basic:
            self.max_queries = 250
        if settings.AV.plan == settings.FMP.Plan.Starter:
            self.max_queries = 300
        if settings.AV.plan == settings.FMP.Plan.Premium:
            self.max_queries = 750
        if settings.AV.plan == settings.FMP.Plan.Ultimate:
            self.max_queries = 3000

        self._sec_info_supported = True
        self._stock_info_supported = True

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

        # TODO LOW Think if we should alter caching so multiple queries for companies with no reports yet won't be requested
        # Check if we need to create a table for earnings surprises
        try:
            check_surprises = "SELECT name FROM sqlite_master WHERE type='table' AND name='fmp_surprises';"

            self.cur.execute(check_surprises)
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table 'fmp_surprises': {e}\n{check_surprises}") from e

        if len(rows) == 0:
            create_surprises = f"""CREATE TABLE fmp_surprises(
                                    fmp_surp_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    source_id INTEGER NOT NULL,
                                    symbol_id INTEGER NOT NULL,
                                    time_stamp INTEGER NOT NULL,
                                    actualEarning REAL,
                                    estimatedEarning REAL,
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
                self.cur.execute(create_surprises)
            except self.Error as e:
                raise FdataError(f"Can't execute a query on a table 'fmp_surprises': {e}\n{create_surprises}") from e

            # Create index for symbol_id
            create_symbol_date_surprises_idx = "CREATE INDEX idx_fmp_surprises ON fmp_surprises(symbol_id, time_stamp);"

            try:
                self.cur.execute(create_symbol_date_surprises_idx)
            except self.Error as e:
                raise FdataError(f"Can't create index fmp_surprises(symbol_id, time_stamp): {e}") from e

            # Create trigger to last modified time
            create_fmp_surprises_trigger = """CREATE TRIGGER update_fmp_surprises
                                                BEFORE UPDATE
                                                    ON fmp_surprises
                                                BEGIN
                                                    UPDATE fmp_surprises
                                                    SET modified = strftime('%s', 'now')
                                                    WHERE fmp_surp_id = old.fmp_surp_id;
                                                END;"""

            try:
                self.cur.execute(create_fmp_surprises_trigger)
            except self.Error as e:
                raise FdataError(f"Can't create trigger for fmp_surprises: {e}") from e

    ###################################################
    # Methods related to capitalization data processing
    ###################################################

    def get_cap_num(self):
        """Get the number of capitalization data entries.

            Returns:
                int: the number of capitalization data entries.

            Raises:
                FdataError: sql error happened.
        """
        return self._get_data_num('fmp_capitalization')

    def fetch_cap(self, num=1000000, first_ts=None, last_ts=None):
        """
            Fetch the capitalization data.

            Args:
                num(int): the number of days to limit the request.
                first_ts(int): overridden first ts to fetch.
                last_ts(int): overridden last ts to fetch.

            Returns:
                list: capitalization data.
        """
        # Adjust dates for the exchange time zone for the request
        first_date, last_date = self.get_request_dates(first_ts, last_ts)

        earliest_date = last_date

        cap_data = []

        while True:
            cap_url = f"https://financialmodelingprep.com/api/v3/historical-market-capitalization/{self.symbol}?limit={num}&from={first_date}&to={last_date}&apikey={self.api_key}"

            # Get capitalization data
            results = self.query_and_parse(cap_url)

            # Remove the last element as it was re-fetched
            if len(cap_data):
                cap_data.remove(cap_data[-1])

            cap_data += results

            # If we are still getting data, need to check the earliest date to distinguish if we have to continue fetching.

            try:
                last_element = results[-1]
            except KeyError as e:
                raise FdataError(f"Can't fetch data (may be due to API key limit). Url is {cap_url}, erro is {e}")
            earliest_date = get_dt(last_element['date']).date()

            if earliest_date <= first_date or earliest_date == last_date or earliest_date == '1980-12-12' or len(results) < 1000:
                break

            # Need to continue fetching
            last_date = earliest_date

        return cap_data

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
            try:
                dt = get_dt(result['date'], self.get_timezone()).replace(hour=23, minute=59, second=59)
                result['date'] = calendar.timegm(dt.utctimetuple())
            except TypeError as e:
                raise FdataError(f"Unexpected data. API key limit is possible. {e}")

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
            Fetch (if needed) the capitalization data.
        """
        initially_connected = self.is_connected()

        if self.is_connected() is False:
            self.db_connect()

        quote_num = self.get_total_symbol_quotes_num()

        if quote_num == 0:
            raise FdataError("Quotes should be fetched at first before fetching capitalization data.")

        num = self.get_cap_num()

        mod_ts = self.get_last_modified('fmp_capitalization')

        current = min(datetime.now(self.get_timezone()).replace(tzinfo=None), self.last_date.replace(tzinfo=None))

        # Fetch data if no data present or day difference between current/requested data more than 1 day
        if mod_ts is None:
            self.add_cap(self.fetch_cap())
        else:
            days_delta = (current - get_dt(mod_ts)).days

            if self.last_date_ts > mod_ts and days_delta:
                self.add_cap(self.fetch_cap(days_delta + 1))

        new_num = self.get_cap_num()

        if initially_connected is False:
            self.db_close()

        return (new_num - num)

    ######################################################
    # Methods related to earnings surprise data processing
    ######################################################

    def get_surprises_num(self):
        """Get the number of surprises data entries.

            Returns:
                int: the number of surprises data entries.

            Raises:
                FdataError: sql error happened.
        """
        return self._get_data_num('fmp_surprises')

    def fetch_surprises(self, num=None):
        """
            Fetch the surprises data.

            Args:
                num(int): the number of days to limit the request.

            Returns:
                list: surprises data.
        """
        if num is not None:
            surprises_url = f"https://financialmodelingprep.com/api/v3/earnings-surprises/{self.symbol}?limit={num}&apikey={self.api_key}"
        else:
            surprises_url = f"https://financialmodelingprep.com/api/v3/earnings-surprises/{self.symbol}?apikey={self.api_key}"

        # Get the surprises data
        results = self.query_and_parse(surprises_url, timeout=120)

        return results

    def add_surprises(self, results):
        """
            Add surprises data to the database.

            Args:
                results(list): the surprises data

            Returns:
                (int, int): total number of earnings reports before and after the operation.

            Raises:
                FdataError: sql error happened.
        """
        self.check_if_connected()

        # Insert new symbols to 'symbols' table (if the symbol does not exist)
        if self.get_total_symbol_quotes_num() == 0:
            self.add_symbol()

        num_before = self.get_surprises_num()

        for result in results:
            # Need to convert date to a time stamp
            try:
                dt = get_dt(result['date'], self.get_timezone())
                result['date'] = calendar.timegm(dt.utctimetuple())

                if result['actualEarningResult'] == None:
                    result['actualEarningResult'] = 'NULL'

                if result['estimatedEarning'] is None:
                    result['estimatedEarning'] = 'NULL'
            except TypeError as e:
                raise FdataError(f"Unexpected data. API key limit is possible. {e}")

            insert_surprises = f"""INSERT OR {self._update} INTO fmp_surprises (symbol_id,
                                        source_id,
                                        time_stamp,
                                        actualEarning,
                                        estimatedEarning)
                                    VALUES (
                                            (SELECT symbol_id FROM symbols WHERE ticker = '{self.symbol}'),
                                            (SELECT source_id FROM sources WHERE title = '{self.source_title}'),
                                            {result['date']},
                                            {result['actualEarningResult']},
                                            {result['estimatedEarning']});"""

            try:
                self.cur.execute(insert_surprises)
            except self.Error as e:
                raise FdataError(f"Can't add a record to a table 'fmp_surprises': {e}\n\nThe query is\n{insert_surprises}") from e

        self.commit()

        return(num_before, self.get_surprises_num())

    # TODO MID It should use intervals table.
    # TODO MID Do not check surprises for ETF.
    def get_surprises(self):
        """
            Fetch (if needed) the surprises data.
        """
        initially_connected = self.is_connected()

        if self.is_connected() is False:
            self.db_connect()

        quote_num = self.get_total_symbol_quotes_num()

        if quote_num == 0:
            raise FdataError("Quotes should be fetched at first before fetching surprises data.")

        num = self.get_surprises_num()

        mod_ts = self.get_last_modified('fmp_surprises')

        current = min(datetime.now(self.get_timezone()).replace(tzinfo=None), self.last_date.replace(tzinfo=None))

        # TODO LOW Ideally here implementation based on earnings calendar is needed
        # Fetch data if no data present or day difference between current/requested data more than 90 days
        if mod_ts is None:
            self.add_surprises(self.fetch_surprises())
        else:
            last_ts = self.get_last_timestamp('fmp_surprises')

            days_delta = (current - get_dt(last_ts)).days
            days_delta_mod = (current - get_dt(mod_ts)).days

            # TODO LOW It should be done in a better way than just checking for 90 days difference.
            if self.last_date_ts > mod_ts and days_delta >= 90 and days_delta_mod:
                #self.add_surprises(self.fetch_surprises(round(days_delta / 90 + 1)))
                self.add_surprises(self.fetch_surprises())

        new_num = self.get_surprises_num()

        if initially_connected is False:
            self.db_close()

        return (new_num - num)

    #########################
    # Methods to fetch quotes
    #########################

    def query_and_parse(self, url, historical=False, timeout=30):
        """
            Query the data source and parse the response.

            Args:
                url(str): the url for a request.
                historical(bool): indicates if historical data is fetched.
                timeout(int): timeout for the request.

            Returns:
                Parsed data.
        """
        # Get the data
        response = self.query_api(url)

        # Get json
        try:
            json_data = response.json()
        except (json.JSONDecodeError, KeyError) as e:
            self.log(f"Can't parse json or no symbol found. Is API call limit reached? {e} URL: {url}")

        results = json_data

        if historical:
            try:
                results = json_data['historical']
            except KeyError as e:
                self.log(f"Can't get the historical data when {url} is requested. Likely API key limit is reached.")

        if results is not None and (len(results) == 0 or results == ['Error Message']):
            self.log(f"No data obtained for {self.symbol} using the query {url}")

        return results

    def get_timespan_str(self):
        """
            Get timespan string (like '5min' and so on) to query a particular data source based on the timespan specified
            in the datasource instance.

            Returns:
                str: timespan string.
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
        elif self.timespan == Timespans.FourHour:
            return '4hour'
        elif self.timespan == Timespans.Day:
            return '1d'
        else:
            raise FdataError(f"Requested timespan is not supported by {type(self).__name__}: {self.timespan.value}")


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
        # Adjust dates for the exchange time zone for the request
        first_datetime, last_datetime = self.get_request_datetimes(first_ts, last_ts)

        earliest_datetime = last_datetime

        # Parsed quotes data. Lets keep it in the same object because it is very unlikely that it won't fit in the memory.
        quotes_data = []

        self.get_splits()  # Split data is necessary for reverse-adjustment. Can't proceed without split data (it any).
        splits = self.get_db_splits()

        while True:
            first_date = first_datetime.date()
            last_date = last_datetime.date()

            if self.is_intraday():
                url = f"https://financialmodelingprep.com/api/v3/historical-chart/{self.get_timespan_str()}/{self.symbol}?from={first_date}&to={last_date}&apikey={self.api_key}"
            else:
                url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{self.symbol}?from={first_date}&to={last_date}&apikey={self.api_key}"

            historical = not self.is_intraday()
            json_results = self.query_and_parse(url, historical=historical)

            if json_results is not None and (len(json_results) == 0 or json_results == ['Error Message']):
                break

            quotes_data += json_results

            # If we are still getting data, need to check the earliest date to distinguish if we have to
            # continue fetching.
            try:
                first_element = json_results[-1]
            except KeyError as e:
                raise FdataError(f"Incorrect data obtained using URL {url} (is API key limit reached)?\n{e}")

            earliest_datetime = get_dt(first_element['date'])

            if earliest_datetime <= first_datetime or earliest_datetime == last_datetime:
                break

            # Need to continue fetching
            last_datetime = earliest_datetime

        # Process the fetched data

        quotes = []  # Processed quotes

        for quote in quotes_data:
            dt = get_dt(quote['date'], self.get_timezone())

            if self.is_intraday():
                volume = quote['volume']
            else:
                # Keep all non-intraday timestamps at 23:59:59
                dt = dt.replace(hour=23, minute=59, second=59)

                volume = quote['unadjustedVolume']

            # Get the combined split ratio over the available history
            ratio = 1

            ts = calendar.timegm(dt.utctimetuple())

            if splits is not None:
                idx = np.where(ts <= splits['split_date'])
                ratio = math.prod(splits['split_ratio'][idx])

            if ratio == 0:
                ratio = 1

            quote_dict = {
                'ts': ts,
                'open': quote['open'] * ratio,
                'high': quote['high'] * ratio,
                'low': quote['low'] * ratio,
                'close': quote['close'] * ratio,
                'volume': volume / ratio,
                'transactions': 'NULL',
            }

            quotes.append(quote_dict)

        return quotes

    #######################################
    # Methods to fetch dividends and splits
    #######################################
    def fetch_dividends(self):
        """
            Fetch the cash dividend data.
        """
        url_divs = f"https://financialmodelingprep.com/api/v3/historical-price-full/stock_dividend/{self.symbol}?apikey={self.api_key}"

        json_results = None
        json_results = self.query_and_parse(url_divs, historical=True)

        divs_data = []

        for div in json_results:
            decl_text = div['declarationDate']
            record_text = div['recordDate']
            pay_text = div['paymentDate']

            date_text = div['date']

            # Consider that the declaration date was a week before entry date if no data
            if decl_text == '':
                decl_date = get_dt(date_text, self.get_timezone()) - timedelta(days=7)
            else:
                decl_date = get_dt(decl_text, self.get_timezone())

            # Consider that the record date was a week after the entry date if no data
            if record_text == '':
                record_date = get_dt(date_text, self.get_timezone()) + timedelta(days=7)
            else:
                record_date = get_dt(record_text, self.get_timezone())

            # Consider that the payment date was a week after the entry date if no data
            if pay_text == '':
                pay_date = get_dt(date_text, self.get_timezone()) + timedelta(days=30)
            else:
                pay_date = get_dt(pay_text, self.get_timezone())

            ex_date = record_date - timedelta(days=1)  # Record date is one day after the ex date

            decl_ts = calendar.timegm(decl_date.utctimetuple())
            ex_ts = calendar.timegm(ex_date.utctimetuple())
            record_ts = calendar.timegm(record_date.utctimetuple())
            pay_ts = calendar.timegm(pay_date.utctimetuple())

            div_dict = {
                'amount': div['dividend'],
                'decl_ts': decl_ts,
                'ex_ts': ex_ts,
                'record_ts': record_ts,
                'pay_ts': pay_ts,
                'currency': self.get_currency()  # TODO LOW For now it is consider that divident currency is the same as stock currency
            }

            divs_data.append(div_dict)

        return divs_data

    def fetch_splits(self):
        """
            Fetch the split data.
        """
        url_splits = f"https://financialmodelingprep.com/api/v3/historical-price-full/stock_split/{self.symbol}?apikey={self.api_key}"

        json_results = None
        json_results = self.query_and_parse(url_splits, historical=True)

        splits_data = []

        for split in json_results:
            dt = get_dt(split['date'], self.get_timezone())
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

    ###############
    # Other methods
    ###############

    def fetch_info(self):
        """
            Fetch stock related info.

            Returns
                dict: stock info.
        """
        profile_url = f"https://financialmodelingprep.com/api/v3/profile/{self.symbol}?apikey={self.api_key}"

        # Get company profile
        json_data = self.query_and_parse(profile_url)

        try:
            results = json_data[0]
            tz_str = Exchanges[results['exchangeShortName']]
        except (KeyError, IndexError) as e:
            raise FdataError(f"Can't fetch info (API key limit is possible): {e}, url is {profile_url}")

        results['fc_time_zone'] = tz_str
        results['fc_sec_type'] = SecType.Stock

        return results

    def get_recent_data(self, to_cache=False):
        """
            Get the resent quote data.

            The usage of this method should be limited even for screening as data request from DB (and this request is
            not DB related) may involve additional data.
        """
        quote_url = f"https://financialmodelingprep.com/api/v3/quote-order/{self.symbol}?apikey={self.api_key}"

        # Get company profile
        json_data = self.query_and_parse(quote_url)

        quote = json_data[0]

        if len(quote) == 0 or quote == ['Error Message']:
            self.log(f"No quote data obtained for {self.symbol}")

        dt = get_dt(quote['timestamp'], tz.UTC)  # It is returned in UTC, not exchange time zone

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

        return get_labelled_ndarray([result])

    #########################################
    # Methods which are not implemented (yet)
    #########################################

    def fetch_income_statement(self):
        raise FdataError(f"Income statement data is not supported (yet) for the source {type(self).__name__}")

    def fetch_balance_sheet(self):
        raise FdataError(f"Balance sheet data is not supported (yet) for the source {type(self).__name__}")

    def fetch_cash_flow(self):
        raise FdataError(f"Cash flow data is not supported (yet) for the source {type(self).__name__}")

    def add_income_statement(self, reports):
        raise FdataError(f"Adding income statement data is not supported (yet) for the source {type(self).__name__}")

    def add_balance_sheet(self, reports):
        raise FdataError(f"Adding balance sheet data is not supported (yet) for the source {type(self).__name__}")

    def add_cash_flow(self, reports):
        raise FdataError(f"Adding cash flow data is not supported (yet) for the source {type(self).__name__}")
