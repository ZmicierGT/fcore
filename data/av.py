"""AlphaVantage API wrapper.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

from datetime import datetime
import pytz

import http.client
import urllib.error
import requests

from data import fdata

from data.fvalues import Timespans
from data.fdata import FdataError

import pandas as pd

from data.futils import get_dt

import settings

# TODO LOW Add unit test for this module

class AVStock(fdata.BaseFetchData):
    """
        AlphaVantage API wrapper class.
    """
    def __init__(self, **kwargs):
        """Initialize the instance of AV class."""
        super().__init__(**kwargs)

        # Default values
        self.source_title = "AlphaVantage"
        self.api_key = settings.AV.api_key
        self.compact = True  # Indicates if a limited number (100) of quotes should be obtained

        if self.api_key is None:
            raise FdataError("API key is needed for this data source. Get your free API key at alphavantage.co and put it in setting.py")

    def get_timespan(self):
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
        # Other timespans are obtained with different function and there is no timespan parameter.
        else:
            raise FdataError(f"Unsupported timespan: {self.timespan}")

    def fetch_quotes(self):
        """
            The method to fetch quotes.

            Raises:
                FdataError: incorrect API key(limit reached), http error happened, invalid timespan or no data obtained.

            Returns:
                list: quotes data
        """
        # At first, need to set a function depending on a timespan.
        if self.timespan == Timespans.Day:
            output_size = 'compact' if self.compact else 'full'
            json_key = 'Time Series (Daily)'

            url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={self.symbol}&outputsize={output_size}&apikey={self.api_key}'
        elif self.timespan == Timespans.Week:
            json_key = 'Weekly Adjusted Time Series'

            url = f'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY_ADJUSTED&symbol={self.symbol}&apikey={self.api_key}'
        elif self.timespan == Timespans.Month:
            json_key = 'Monthly Adjusted Time Series'

            url = f'https://www.alphavantage.co/query?function=TIME_SERIES_MONTHLY_ADJUSTED&symbol={self.symbol}&apikey={self.api_key}'
        # All intraday timespans
        elif self.timespan in [Timespans.Minute,
                               Timespans.FiveMinutes,
                               Timespans.FifteenMinutes,
                               Timespans.ThirtyMinutes,
                               Timespans.Hour]:
            output_size = 'compact' if self.compact else 'full'
            json_key = f'Time Series ({self.get_timespan()})'

            url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={self.symbol}&interval={self.get_timespan()}&outputsize={output_size}&apikey={self.api_key}'
        else:
            raise FdataError(f"Unsupported timespan: {self.timespan}")

        # Get quotes data
        try:
            response = requests.get(url, timeout=30)
        except (urllib.error.HTTPError, urllib.error.URLError, http.client.HTTPException) as e:
            raise FdataError(f"Can't fetch quotes: {e}") from e

        json_data = response.json()

        dict_results = json_data[json_key]
        datetimes = list(dict_results.keys())

        if len(datetimes) == 0:
            raise FdataError("No data obtained.")

        quotes_data = []

        for dt_str in datetimes:
            try:
                dt = get_dt(dt_str)  # Get UTC-adjusted datetime
            except ValueError as e:
                raise FdataError(f"Can't parse the datetime {dt_str}: {e}") from e

            # The current quote to process
            quote = dict_results[dt_str]

            quote_dict = {
                'ts': 'NULL',
                'open': quote['1. open'],
                'high': quote['2. high'],
                'low': quote['3. low'],
                'adj_close': 'NULL',
                'raw_close': 'NULL',
                'volume': 'NULL',
                'divs': 'NULL',
                'transactions': 'NULL',
                'split': 'NULL'
            }

            # Set the entries depending if the quote is intraday
            if self.timespan in (Timespans.Day, Timespans.Week, Timespans.Month):
                quote_dict['adj_close'] = quote['5. adjusted close']
                quote_dict['raw_close'] = quote['4. close']
                quote_dict['volume'] = quote['6. volume']
                quote_dict['divs'] = quote['7. dividend amount']
                quote_dict['split'] = quote['8. split coefficient']

                # Keep all non-intraday timestamps at 23:59:59
                dt = dt.replace(hour=23, minute=59, second=59)
            else:
                quote_dict['adj_close'] = quote['4. close']
                quote_dict['volume'] = quote['5. volume']
                quote_dict['raw_close'] = quote['4. close']
                quote_dict['split'] = 1  # Split coefficient is always 1 for intraday

            # Set the timestamp
            quote_dict['ts'] = int(datetime.timestamp(dt))

            quotes_data.append(quote_dict)

        return quotes_data

    def fetch_fundamentals(self, function):
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
        try:
            response = requests.get(url, timeout=30)
        except (urllib.error.HTTPError, urllib.error.URLError, http.client.HTTPException) as e:
            raise FdataError(f"Can't fetch fundamental data: {e}") from e

        json_data = response.json()

        annual_reports = pd.json_normalize(json_data['annualReports'])
        quarterly_reports = pd.json_normalize(json_data['quarterlyReports'])

        annual_reports['period'] = 'Year'
        quarterly_reports['period'] = 'Quarter'

        # Merge and sort reports
        reports = pd.concat([annual_reports, quarterly_reports], ignore_index=True)
        reports = reports.sort_values(by=['fiscalDateEnding'], ignore_index=True)

        # Delete reported currency
        reports = reports.drop(labels="reportedCurrency", axis=1)

        # Replace string datetime to timestamp
        reports['fiscalDateEnding'] = reports['fiscalDateEnding'].apply(lambda x: get_dt(x))
        reports['fiscalDateEnding'] = reports['fiscalDateEnding'].apply(lambda x: int(datetime.timestamp(x)))

        # Convert dataframe to dictionary
        fundamental_results = reports.T.to_dict().values()

        return fundamental_results

    # TODO MID these methods should be abstract in the base class
    def fetch_income_statement(self):
        """
            Fetches the income statement.

            Raises:
                FdataError: incorrect API key(limit reached), http error happened or no data obtained.

            Returns:
                list: fundamental data
        """
        return self.fetch_fundamentals('INCOME_STATEMENT')

    def fetch_balance_sheet(self):
        """
            Fetches the balance sheet.

            Raises:
                FdataError: incorrect API key(limit reached), http error happened or no data obtained.

            Returns:
                list: fundamental data
        """
        return self.fetch_fundamentals('BALANCE_SHEET')

    def fetch_cash_flow(self):
        """
            Fetches the cash flow.

            Raises:
                FdataError: incorrect API key(limit reached), http error happened or no data obtained.

            Returns:
                list: fundamental data
        """
        return self.fetch_fundamentals('CASH_FLOW')

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
        try:
            response = requests.get(url, timeout=30)
        except (urllib.error.HTTPError, urllib.error.URLError, http.client.HTTPException) as e:
            raise FdataError(f"Can't fetch earnings data: {e}") from e

        json_data = response.json()

        annual_earnings = pd.json_normalize(json_data['annualEarnings'])
        quarterly_earnings = pd.json_normalize(json_data['quarterlyEarnings'])

        annual_earnings['period'] = 'Year'
        quarterly_earnings['period'] = 'Quarter'

        # These columns are not available in annual earnings reports
        annual_earnings['reportedDate'] = None
        annual_earnings['estimatedEPS'] = None
        annual_earnings['surprise'] = None
        annual_earnings['surprisePercentage'] = None

        # Convert reported date to UTC-adjusted timestamp
        quarterly_earnings['reportedDate'] = quarterly_earnings['reportedDate'].apply(lambda x: get_dt(x))
        quarterly_earnings['reportedDate'] = quarterly_earnings['reportedDate'].apply(lambda x: int(datetime.timestamp(x)))

        # Merge and sort earnings reports
        earnings = pd.concat([annual_earnings, quarterly_earnings], ignore_index=True)
        earnings = earnings.sort_values(by=['fiscalDateEnding'], ignore_index=True)

        # Replace string datetime to timestamp
        earnings['fiscalDateEnding'] = earnings['fiscalDateEnding'].apply(lambda x: get_dt(x))
        earnings['fiscalDateEnding'] = earnings['fiscalDateEnding'].apply(lambda x: int(datetime.timestamp(x)))

        print(earnings)

        # Convert dataframe to dictionary
        earnings_results = earnings.T.to_dict().values()

        return earnings_results

    # TODO MID This method should be abstract in the base class
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
        try:
            response = requests.get(url, timeout=30)
        except (urllib.error.HTTPError, urllib.error.URLError, http.client.HTTPException) as e:
            raise FdataError(f"Can't fetch earnings data: {e}") from e

        # Get json
        json_data = response.json()
        quote = json_data['Global Quote']

        # Use the current time as a timestamp
        dt = datetime.now()
        # Always keep datetimes in UTC time zone!
        dt = dt.replace(tzinfo=pytz.utc)
        ts = int(dt.timestamp())

        result = [ts,
                  quote['02. open'],
                  quote['03. high'],
                  quote['04. low'],
                  quote['05. price'],  # Consider that Close and AdjClose is the same for intraday timespans
                  quote['05. price'],
                  quote['06. volume'],
                  'NULL',
                  'NULL',
                  'NULL']

        return result
