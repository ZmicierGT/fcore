"""Polygon.IO API wrapper.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from datetime import datetime
import pytz

from dateutil.relativedelta import relativedelta

import json

from termcolor import colored

from data import stock
from data.fdata import FdataError

from data.fvalues import Timespans, SecType, Currency, def_first_date, def_last_date, Sector

from data.futils import get_dt

import settings

# TODO High Check if this data source works correctly, then add data handling test for it and fundamental data support.

# TODO Mid Check if other exchanges/time zones are used in this data source
# Time zones of some popular exchanges for FMP data source
Exchanges = {
    'XNYS':     'America/New_York',
    'XNAS':     'America/New_York',
    'ARCX':     'America/New_York'
}

class Polygon(stock.StockFetcher):
    """
        Polygon.IO wrapper class.
    """
    def __init__(self, **kwargs):
        """
            Initialize Polygon.IO wrapper class.
        """
        print(colored("Warning! This data source needs to be maintained!", "yellow"))
        super().__init__(**kwargs)

        # Default values
        self.source_title = "Polygon.io"
        self.api_key = settings.Polygon.api_key

        # Maximum number of API queries per minute for the subscription plan.
        # Even in the case of payed subscription it is better to keep some limit here as Polygon has
        # some spamming requests protection.
        self.max_queries = 250

        if settings.Polygon.stocks_plan == settings.Polygon.Stocks.Basic:
            self.year_delta = 2
            self.max_queries = 5
        elif settings.Polygon.stocks_plan == settings.Polygon.Stocks.Starter:
            self.year_delta = 5
        elif settings.Polygon.stocks_plan == settings.Polygon.Stocks.Developer:
            self.year_delta = 10
        elif settings.Polygon.stocks_plan == settings.Polygon.Stocks.Advanced:
            self.year_delta = 15
        elif settings.Polygon.stocks_plan == settings.Polygon.Stocks.Commercial:
            self.year_delta = 15

        if self.api_key is None:
            raise FdataError("API key is needed for this data source. Get your free API key at polygon.io and put it in setting.py")

        # IF first/last datetimes are not provided, use the current datetime as the last and current - year_delta as first
        if self.first_date_ts == def_first_date:
            new_first_date = datetime.now() - relativedelta(years=int(self.year_delta))
            new_first_date = new_first_date.replace(tzinfo=pytz.utc)
            new_first_date = new_first_date.replace(hour=0, minute=0, second=0)
            self.first_date = new_first_date

        if self.last_date_ts == def_last_date:
            new_last_date = datetime.now()
            new_last_date = new_last_date.replace(tzinfo=pytz.utc)
            new_last_date = new_last_date.replace(hour=23, minute=59, second=59)
            self.last_date = new_last_date

        self._sec_info_supported = True
        self._stock_info_supported = True

    def get_timespan_str(self):
        """
            Get the timespan for queries.

            Raises:
                FdataError: incorrect/unsupported timespan requested.

            Returns:
                str: timespan for Polygon query.
        """
        if self.timespan in [Timespans.Minute,
                             Timespans.Hour,
                             Timespans.Day]:
            return self.timespan.lower()
        else:
            raise FdataError(f"Requested timespan is not supported by Polygon: {self.timespan.value}")

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

        if timespan in [Timespans.Minute, Timespans.Hour]:
            return True
        elif timespan == Timespans.Day:
            return False
        else:
            raise FdataError(f"Unknown timespan for Polygon: {timespan}")

    # TODO LOW Think if it should be abstract in the base class
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

        json_results = None

        try:
            json_data = json.loads(response.text)
            json_results = json_data['results']
        except (json.JSONDecodeError, KeyError) as e:
            error = e

            try:
                error = json_data['error']
            except (json.JSONDecodeError, KeyError):
                # Not relevant for error reporting
                pass

            self.log(f"Can't parse json or no symbol found. Is API call limit reached? {error} URL: {url}")

        if json_results is not None and len(json_results) == 0:
            self.log(f"No data obtained for {self.symbol}")

        return json_results

    def fetch_quotes(self, first_ts=None, last_ts=None):
        """
            The method to fetch quotes.

            Args:
                first_ts(int): overridden first ts to fetch.
                last_ts(int): overridden last ts to fetch.

            Returns:
                list: quotes data

            Raises:
                FdataError: Network error happened, no data obtained or can't parse json.
        """
        if first_ts is not None:
            first_date = get_dt(first_ts, pytz.UTC).date()
        else:
            first_date = self.first_date.date()

        if last_ts is not None:
            last_date = get_dt(last_ts, pytz.UTC).date()
        else:
            last_date = self.last_date.date()

        # Parsed quotes data. Lets keep it in the same object because it is very unlikely that it won't fit in the memory.
        quotes_data = []

        while True:
            url = f"https://api.polygon.io/v2/aggs/ticker/{self.symbol}/range/1/{self.get_timespan_str()}/{first_date}/{last_date}?adjusted=false&sort=asc&limit=50000&apiKey={self.api_key}"

            json_results = self.query_and_parse(url)

            if json_results is None:
                self.log(f"No data obtained for {self.symbol} using {self.source_title}")

                break

            for j in range(len(json_results)):
                quote = json_results[j]

                # No need in ms
                ts = int(quote['t'] / 1000)

                dt = datetime.utcfromtimestamp(ts)
                dt = dt.replace(tzinfo=pytz.utc)

                # Keep all non-intraday timestamps at 23:59:59
                if self.is_intraday() is False:
                    dt = dt.replace(hour=23, minute=59, second=59)
                    ts = int(datetime.timestamp(dt))

                # Sometimes the number of transactions does not exist in json
                if 'n' not in quote:
                    n = 'NULL'
                else:
                    n = quote['n']

                quote_dict = {
                    'ts': ts,
                    'open': quote['o'],
                    'high': quote['h'],
                    'low': quote['l'],
                    'close': quote['c'],
                    'volume': quote['v'],
                    'transactions': n
                }

                quotes_data.append(quote_dict)

            # Likely the last days are days off so no quotes are obtained
            if first_date == dt.date():
                break

            first_date = dt.date()

            # Enough quotes are obtained
            if first_date > last_date:
                break
            else:
                self.log(f"Continue aggregate fetching quotes for {self.symbol} from {first_date} to {last_date}.")

        return quotes_data

    def fetch_dividends(self):
        """
            Fetch the cash dividend data.
        """
        url_divs = f"https://api.polygon.io/v3/reference/dividends?ticker={self.symbol}&limit=1000&apiKey={self.api_key}"

        json_results = self.query_and_parse(url_divs)

        divs_data = []

        for div in json_results:
            # Note that for some stocks (like DE) some data entries may be missed. Not just having no data but
            # completely missing in json.

            decl_ts = 'NULL'
            record_ts = 'NULL'
            pay_ts = 'NULL'

            if 'declaration_date' in div:
                decl_date = get_dt(div['declaration_date'], pytz.UTC)
                decl_ts = int(datetime.timestamp(decl_date))

            ex_date = get_dt(div['ex_dividend_date'], pytz.UTC)
            ex_ts = int(datetime.timestamp(ex_date))

            if 'record_date' in div:
                record_date = get_dt(div['record_date'], pytz.UTC)
                record_ts = int(datetime.timestamp(record_date))

            if 'pay_date' in div:
                pay_date = get_dt(div['pay_date'], pytz.UTC)
                pay_ts = int(datetime.timestamp(pay_date))

            div_dict = {
                'amount': div['cash_amount'],
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
        url_splits = f"https://api.polygon.io/v3/reference/splits?ticker={self.symbol}&limit=1000&apiKey={self.api_key}"

        json_results = self.query_and_parse(url_splits)

        splits_data = []

        for split in json_results:
            dt = get_dt(split['execution_date'], pytz.UTC)
            ts = int(datetime.timestamp(dt))

            split_to = int(split['split_to'])
            split_from = int(split['split_from'])
            split_ratio = split_to / split_from

            split_dict = {
                'ts': ts,
                'split_ratio': split_ratio,
            }

            splits_data.append(split_dict)

        return splits_data

    def fetch_info(self):
        """
            Fetch stock related info.

            Returns
                dict: stock info.
        """
        profile_url = f"https://api.polygon.io/v3/reference/tickers/{self.symbol}?apiKey={self.api_key}"

        # Get company profile
        results = self.query_and_parse(profile_url)

        try:
            tz_str = Exchanges[results['primary_exchange']]
        except (KeyError, IndexError) as e:
            raise FdataError(f"Can't fetch info (API key limit is possible): {e}, url is {profile_url}")

        results['fc_time_zone'] = tz_str
        results['fc_sec_type'] = SecType.Stock.value
        results['sector'] = Sector.Unknown.value  # TODO MID Convert the provided SIC code to a sector

        return results

    def fetch_income_statement(self):
        raise FdataError(f"Income statement data is not supported (yet) for the source {type(self).__name__}")

    def fetch_balance_sheet(self):
        raise FdataError(f"Balance sheet data is not supported (yet) for the source {type(self).__name__}")

    def fetch_cash_flow(self):
        raise FdataError(f"Cash flow data is not supported (yet) for the source {type(self).__name__}")

    def get_recent_data(self, to_cache=False):
        raise FdataError(f"Real time data is not supported (yet) for the source {type(self).__name__}")

    def get_income_statement_num(self):
        raise FdataError(f"Income statement reports num data is not supported (yet) for the source {type(self).__name__}")

    def get_balance_sheet_num(self):
        raise FdataError(f"Balance sheet reports num data is not supported (yet) for the source {type(self).__name__}")

    def get_cash_flow_num(self):
        raise FdataError(f"Cash flow reports num data is not supported (yet) for the source {type(self).__name__}")

    def add_income_statement(self, reports):
        raise FdataError(f"Adding income statement data is not supported (yet) for the source {type(self).__name__}")

    def add_balance_sheet(self, reports):
        raise FdataError(f"Adding balance sheet data is not supported (yet) for the source {type(self).__name__}")

    def add_cash_flow(self, reports):
        raise FdataError(f"Adding cash flow data is not supported (yet) for the source {type(self).__name__}")
