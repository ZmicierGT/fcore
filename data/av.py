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

        # Cached earnings to estimate reporting dates.
        self.earnings = None
        self.earnings_first_date = None
        self.earnings_last_date = None

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

    def is_intraday(self):
        """
            Determine if the current timespan is intraday.

            Returns:
                bool: if the current timespan is intraday.
        """
        if self.timespan in (Timespans.Minute,
                               Timespans.FiveMinutes,
                               Timespans.FifteenMinutes,
                               Timespans.ThirtyMinutes,
                               Timespans.Hour):
            return True
        elif self.timespan == Timespans.Day:
            return False
        else:
            raise FdataError(f"Unsupported timespan: {self.timespan.value}")

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
            self.log("Can't get data. Likely API key limit or just no data for the requested period.")
            return (None, None)

        return (dict_results, dict_header)

    def fetch_quotes(self):
        """
            The method to fetch quotes.

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
            year = self.first_date.year
            month = self.first_date.month

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

        while self.is_intraday() and (year <= self.last_date.year and month < self.last_date.month):
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
                'transactions': 'NULL',
                'sectype': self.sectype.value,
                'currency': self.currency.value
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
        # Check if earnings data is cached for this interval. If not, fetch it.
        if self.earnings_cached() is False:
            self.fetch_earnings()

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

        # Align data in both reports
        adj_earnings = self.earnings[self.earnings['fiscalDateEnding'].isin(reports['fiscalDateEnding'])]
        adj_earnings = adj_earnings.reset_index(drop=True)

        # Drop exceeding rows. Sometimes they may present due to the broken data obtained through API.
        len_diff = adj_earnings.shape[0] - reports.shape[0]

        if len_diff > 0:
            adj_earnings = adj_earnings.drop(adj_earnings.tail(len_diff).index)
        elif len_diff < 0:
            reports = reports.drop(reports.tail(-abs(len_diff)).index)

        # Add reporting date from earnings
        try:
            reports['reportedDate'] = np.where(reports['fiscalDateEnding'].equals(adj_earnings['fiscalDateEnding']), \
                adj_earnings['reportedDate'], None)
        except ValueError as e:
            raise FdataError(f"Can't align dates of reports. This may be due to the broken data obtained from API: {e}") from e

        # Replace AV "None" to SQL 'NULL'
        reports = reports.replace(['None'], 'NULL')

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
        # Check if earnings are already cached for this interval
        if self.earnings_cached():
            return self.earnings.T.to_dict().values()

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

        self.earnings = earnings
        self.earnings_first_date = self.first_date
        self.earnings_last_date = self.last_date

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
            # if settings.AV.plan == settings.AV.Plan.Free:
            #     raise FdataError("Daily adjusted data is not available in the free plan now.")

            json_key = 'Time Series (Daily)'

            #url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={self.symbol}&outputsize=full&apikey={self.api_key}'

            # Demo key for testing split/divs fetching without a premium key
            url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=IBM&outputsize=full&apikey=demo'

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

    def earnings_cached(self):
        """
            Check if earnings are cached for the current interval.
            Reporting dates present in earnings reports only. They are needed to estimate reporting dates for
            other fundamentals.

            Returns:
                bool: indicates if earnings are cached.
        """
        return self.earnings is not None and self.earnings_first_date == self.first_date \
            and self.earnings_last_date == self.last_date
