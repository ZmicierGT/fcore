"""Yahoo Finance wrapper.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from datetime import datetime, timedelta
from dateutil import tz
import calendar

import pandas as pd
import numpy as np

import yfinance as yfin

from data import stock
from data.fvalues import Timespans, SecType, Currency
from data.fdata import FdataError
from data.futils import get_labelled_ndarray, get_dt

import urllib.error
import http.client

class YF(stock.StockFetcher):
    """
        Yahoo Finance wrapper class.
    """
    def __init__(self, **kwargs):
        """
            Initialize Yahoo Finance wrapper class.
        """
        super().__init__(**kwargs)

        # Default values
        self.source_title = "YF"

        self._data = None  # Cached data for splits/divs
        self._data_symbol = self.symbol  # Symbol of cached data

        self._sec_info_supported = True
        self._stock_info_supported = True

        self._earnings_history_tbl = 'yf_earnings_history'

    def get_timespan_str(self):
        """
            Get the timespan for queries.

            Raises:
                FdataError: incorrect/unsupported timespan requested.

            Returns:
                str: timespan for YF query.
        """
        if self.timespan == Timespans.Minute:
            return '1m'
        elif self.timespan == Timespans.TwoMinutes:
            return '2m'
        elif self.timespan == Timespans.FiveMinutes:
            return '5m'
        elif self.timespan == Timespans.FifteenMinutes:
            return '15m'
        elif self.timespan == Timespans.ThirtyMinutes:
            return '30m'
        elif self.timespan == Timespans.Hour:
            return '1h'
        elif self.timespan == Timespans.NinetyMinutes:
            return '90m'
        elif self.timespan == Timespans.Day:
            return '1d'
        else:
            raise FdataError(f"Requested timespan is not supported by YF: {self.timespan.value}")

    # TODO MID Think how to handle a situation that YF fetches the current quote even if period is incomplete
    def fetch_quotes(self, first_ts=None, last_ts=None):
        """
            The method to fetch quotes.

            Args:
                first_ts(int): overridden first ts to fetch.
                last_ts(int): overridden last ts to fetch.

            Returns:
                list: quotes data

            Raises:
                FdataError: network error, no data obtained, can't parse json or the date is incorrect.
        """
        # Adjust dates for the exchange time zone for the request
        first_date, last_date = self.get_request_dates(first_ts, last_ts)

        # Dates should differ or no data obtained
        if (last_date - first_date).days == 0:
            first_date = first_date - timedelta(days=1)

        try:
            data = yfin.download(self.symbol,
                                 interval=self.get_timespan_str(),
                                 start=first_date,
                                 end=last_date,
                                 auto_adjust=False)
        except Exception as e:
            raise FdataError(f"Can't fetch quotes for {self.symbol} from YF: {e}") from e

        length = len(data)

        if length == 0:
            raise FdataError(f"Can not fetch quotes for {self.symbol}. No quotes fetched. The security may be delisted or the symbol is incorrect.")

        pick_ts = np.vectorize(lambda x: calendar.timegm(get_dt(str(x), self.get_timezone()).utctimetuple()))

        data = data.reset_index()

        if self.is_intraday() is False:
            # TODO LOW For simplicity just set time to 23:59:59 without time zone adjustments.
            # For some markets (non-US) timestamps (which are supposed to be UTC-adjusted) may be incorrect.
            data['ts'] = data['Date'].dt.normalize() + timedelta(hours=23, minutes=59, seconds=59)
            # Convert datetime to timestamp: pandas datetime64[us] stores microseconds, so divide by 10^6 to get seconds
            data['ts'] = (data['ts'].astype(np.int64) // 10**6).astype(int)

            # Reverse-adjust the quotes
            splits = self.__fetch_splits()

            for i in range(len(splits)):
                ind = np.searchsorted(data['ts'], [splits['ts'][i] ,], side='right')[0] - 1
                split_ratio = splits['split_ratio'][i]

                data.loc[: ind, 'Open'] = data.loc[:ind, 'Open'][self.symbol] * split_ratio
                data.loc[: ind, 'High'] = data.loc[:ind, 'High'][self.symbol] * split_ratio
                data.loc[: ind, 'Low'] = data.loc[:ind, 'Low'][self.symbol] * split_ratio
                data.loc[: ind, 'Close'] = data.loc[:ind, 'Close'][self.symbol] * split_ratio
                data.loc[: ind, 'Volume'] = round(data.loc[:ind, 'Volume'][self.symbol] / split_ratio)
        else:
            data['ts'] = pick_ts(data['Datetime'])

        # Create a list of dictionaries with quotes
        quotes_data = []

        for ind in range(length):
            open_val = data.iloc[[ind]]['Open'].values[0][0]
            high_val = data.iloc[[ind]]['High'].values[0][0]
            low_val = data.iloc[[ind]]['Low'].values[0][0]
            close_val = data.iloc[[ind]]['Close'].values[0][0]
            volume_val = data.iloc[[ind]]['Volume'].values[0][0]

            # Skip rows with NaN values (can happen when requesting future dates or out-of-scope data)
            if pd.isna(open_val) or pd.isna(high_val) or pd.isna(low_val) or pd.isna(close_val) or pd.isna(volume_val):
                self.log(f"Skipping row {ind} due to NaN values (likely out-of-scope date)")
                continue

            quote_dict = {
                'volume': volume_val,
                'open': open_val,
                'close': close_val,
                'high': high_val,
                'low': low_val,
                'transactions': 'NULL',
                'ts': data.iloc[[ind]]['ts'].values[0]
            }

            quotes_data.append(quote_dict)

        if len(quotes_data) == 0:
            raise FdataError(f"No valid quotes obtained for {self.symbol}. The security may be delisted or the symbol is incorrect.")

        return quotes_data

    # TODI MID For correct screeners work it should correspond the data in the main dataset.
    def get_recent_data(self, to_cache=False):
        """
            Get pseudo real time data. Used in screening demonstration.

            Args:
                to_cache(bool): indicates if real time data should be cached in a database.

            Returns:
                list: real time data.
        """
        data = yfin.download(tickers=self.symbol, period='1d', interval='1m', auto_adjust=False)
        row = data.iloc[-1]
        row = row.droplevel(1)

        dt = data.index[-1].to_pydatetime().replace(tzinfo=None)
        ts = int(datetime.timestamp(dt))

        volume = row['Volume'].astype(int)

        result = {'time_stamp': ts,
                  'date_time': dt.replace(microsecond=0).isoformat(' '),
                  'opened': row['Open'],
                  'high': row['High'],
                  'low': row['Low'],
                  'closed': row['Close'],
                  'volume': volume,
                  'transactions': None,
                  'adj_open': row['Open'],
                  'adj_high': row['High'],
                  'adj_low': row['Low'],
                  'adj_close': row['Close'],
                  'adj_volume': volume,
                  'divs_ex': 0.0,
                  'divs_pay': 0.0,
                  'splits': 1.0
                 }

        # TODO LOW caching should be implemented

        result = [result]
        result = get_labelled_ndarray(result)

        return result

    def get_cached_data(self):
        """
            Gets the cached data for dividends/splits.

            Returns:
                data instance for getting dividends/splits.
        """
        if self._data is None or self.symbol != self._data_symbol:
            self._data = yfin.Ticker(self.symbol)
            self._data.history(period='max')

            self._data_symbol = self.symbol

        return self._data

    def __fetch_splits(self):
        """
            Fetch the split data.

            Return:
                DataFrame: splits data
        """
        data = self.get_cached_data()
        splits = data.splits

        df_result = pd.DataFrame()

        # Handle empty splits (no splits data available)
        if len(splits) == 0:
            return df_result

        # Keep splits at 00:00:00
        df_result['ts'] = splits.keys().tz_convert('UTC').normalize() + timedelta(hours=00, minutes=00, seconds=00)
        # Convert datetime to timestamp: pandas datetime64[ns] stores nanoseconds, so divide by 10^9 to get seconds
        df_result['ts'] = (df_result['ts'].astype(np.int64) // 10**9).astype(int)

        df_result['split_ratio'] = splits.reset_index()['Stock Splits']

        return df_result

    def __fetch_dividends(self):
        """
            Fetch cash dividends for the specified period.

            Note that YF dividend data may be incomplete

            Returns:
                DataFrame: cash dividend data.
        """
        data = self.get_cached_data()
        divs = data.dividends
        splits = self.__fetch_splits()

        df_result = pd.DataFrame()

        # Handle empty dividends (no dividends data available)
        if len(divs) == 0:
            return df_result

        # Keep dividends at 00:00:00
        df_result['ex_ts'] = divs.keys().tz_convert('UTC').normalize() + timedelta(hours=00, minutes=00, seconds=00)
        # Convert datetime to timestamp: pandas datetime64[ns] stores nanoseconds, so divide by 10^9 to get seconds
        df_result['ex_ts'] = (df_result['ex_ts'].astype(np.int64) // 10**9).astype(int)

        df_result['amount'] = divs.reset_index()['Dividends']

        # Not used in this data source
        df_result['currency'] = self.get_currency()
        df_result['decl_ts'] = 'NULL'
        df_result['record_ts'] = 'NULL'
        df_result['pay_ts'] = 'NULL'

        # Reverse-adjust the dividends
        for i in range(len(splits)):
            ind = np.searchsorted(df_result['ex_ts'], [splits['ts'][i] ,], side='right')[0]

            df_result.loc[df_result.index < ind, 'amount'] = df_result.loc[df_result.index < ind, 'amount'] * splits['split_ratio'][i]

        return df_result

    # TODO MID Dividends are adjusted by default!
    def fetch_dividends(self):
        """
            Fetch cash dividends for the specified period.
        """
        return self.__fetch_dividends().T.to_dict().values()

    def fetch_splits(self):
        """
            Fetch the split data.
        """
        return self.__fetch_splits().T.to_dict().values()

    def fetch_info(self):
        """
            Fetch and return the info of the security.

            Returns:
                dict: dictionary with the info
        """
        ticker = yfin.Ticker(self.symbol)

        try:
            info = ticker.info
        except (urllib.error.HTTPError, urllib.error.URLError, http.client.HTTPException) as e:
            raise FdataError(f"Can't fetch info. Likely yfinance needs updating. Invoke pip install yfinance --upgrade: {e}") from e

        info['fc_time_zone'] = info.get('exchangeTimezoneName', 'UTC')
        info['fc_sec_type'] = SecType.Unknown

        sec_type = info.get('quoteType', 'EQUITY')

        if sec_type == 'EQUITY':
            info['fc_sec_type'] = SecType.Stock
        elif sec_type == 'CRYPTOCURRENCY':
            info['fc_sec_type'] = SecType.Crypto
        elif sec_type == 'ETF':
            info['fc_sec_type'] = SecType.ETF

        return info

    def check_database(self):
        """
            Database create/integrity check method for YF-specific tables.

            Raises:
                FdataError: sql error happened.
        """
        super().check_database()

        # Check if we need to create table 'yf_earnings_history'
        try:
            check_earnings_history = "SELECT name FROM sqlite_master WHERE type='table' AND name='yf_earnings_history';"

            self.cur.execute(check_earnings_history)
            rows = self.cur.fetchall()
        except self.Error as e:
            raise FdataError(f"Can't execute a query on a table 'yf_earnings_history': {e}\n{check_earnings_history}") from e

        if len(rows) == 0:
            create_earnings_history = f"""CREATE TABLE yf_earnings_history(
                                    yf_eh_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    source_id INTEGER NOT NULL,
                                    symbol_id INTEGER NOT NULL,
                                    time_stamp INTEGER NOT NULL,
                                    epsActual REAL,
                                    epsEstimate REAL,
                                    epsDifference REAL,
                                    surprisePercent REAL,
                                    modified INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                                    UNIQUE(symbol_id, time_stamp, source_id)
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
                self.cur.execute(create_earnings_history)
            except self.Error as e:
                raise FdataError(f"Can't execute a query on a table 'yf_earnings_history': {e}\n{create_earnings_history}") from e

            # Create index for symbol_id
            create_eh_idx = "CREATE INDEX idx_yf_earnings_history ON yf_earnings_history(symbol_id, time_stamp);"

            try:
                self.cur.execute(create_eh_idx)
            except self.Error as e:
                raise FdataError(f"Can't create index yf_earnings_history(symbol_id, time_stamp): {e}") from e

            # Create trigger to update last modified time
            create_eh_trigger = """CREATE TRIGGER update_yf_earnings_history
                                        BEFORE UPDATE
                                            ON yf_earnings_history
                                    BEGIN
                                        UPDATE yf_earnings_history
                                        SET modified = strftime('%s', 'now')
                                        WHERE yf_eh_id = old.yf_eh_id;
                                    END;"""

            try:
                self.cur.execute(create_eh_trigger)
            except self.Error as e:
                raise FdataError(f"Can't create trigger for yf_earnings_history: {e}") from e

    def get_earnings_history_num(self):
        """Get the number of earnings history entries.

            Returns:
                int: the number of earnings history entries.

            Raises:
                FdataError: sql error happened.
        """
        return self._get_data_num(self._earnings_history_tbl)

    def fetch_earnings_history(self):
        """
            Fetch the earnings history data.

            Returns:
                list: earnings history data.

            Raises:
                FdataError: network error or no data obtained.
        """
        ticker = yfin.Ticker(self.symbol)

        try:
            eh = ticker.earnings_history
        except Exception as e:
            raise FdataError(f"Can't fetch earnings history for {self.symbol} from YF: {e}") from e

        if eh is None or eh.empty:
            self.log(f"No earnings history data obtained for {self.symbol}")
            return []

        eh = eh.reset_index()

        eh_data = []

        for _, row in eh.iterrows():
            quarter = row['quarter']

            dt = quarter.tz_localize('UTC') if quarter.tz is None else quarter.tz_convert('UTC')
            ts = int(calendar.timegm(dt.utctimetuple()))

            eps_actual = row.get('epsActual')
            eps_estimate = row.get('epsEstimate')
            eps_difference = row.get('epsDifference')
            surprise_percent = row.get('surprisePercent')

            def val_or_null(v):
                return 'NULL' if pd.isna(v) else v

            eh_dict = {
                'time_stamp': ts,
                'epsActual': val_or_null(eps_actual),
                'epsEstimate': val_or_null(eps_estimate),
                'epsDifference': val_or_null(eps_difference),
                'surprisePercent': val_or_null(surprise_percent),
            }

            eh_data.append(eh_dict)

        return eh_data

    def add_earnings_history(self, results):
        """
            Add earnings history data to the database.

            Args:
                results(list): the earnings history data.

            Returns:
                (int, int): total number of earnings history entries before and after the operation.

            Raises:
                FdataError: sql error happened.
        """
        self.check_if_connected()

        # Insert new symbols to 'symbols' table (if the symbol does not exist)
        if self.get_total_symbol_quotes_num() == 0:
            self.add_symbol()

        num_before = self.get_earnings_history_num()

        for result in results:
            insert_eh = f"""INSERT OR {self._update} INTO yf_earnings_history (symbol_id,
                                        source_id,
                                        time_stamp,
                                        epsActual,
                                        epsEstimate,
                                        epsDifference,
                                        surprisePercent)
                                    VALUES (
                                            (SELECT symbol_id FROM symbols WHERE ticker = '{self.symbol}'),
                                            (SELECT source_id FROM sources WHERE title = '{self.source_title}'),
                                            {result['time_stamp']},
                                            {result['epsActual']},
                                            {result['epsEstimate']},
                                            {result['epsDifference']},
                                            {result['surprisePercent']});"""

            try:
                self.cur.execute(insert_eh)
            except self.Error as e:
                raise FdataError(f"Can't add a record to a table 'yf_earnings_history': {e}\n\nThe query is\n{insert_eh}") from e

        self.commit()

        return (num_before, self.get_earnings_history_num())

    def get_earnings_history(self):
        """
            Fetch (if needed) the earnings history data.
        """
        initially_connected = self.is_connected()

        if self.is_connected() is False:
            self.db_connect()

        quote_num = self.get_total_symbol_quotes_num()

        if quote_num == 0:
            raise FdataError("Quotes should be fetched at first before fetching earnings history data.")

        num = self.get_earnings_history_num()

        mod_ts = self.get_last_modified(self._earnings_history_tbl)

        current = min(datetime.now().replace(tzinfo=None), self.last_date.replace(tzinfo=None))

        # Fetch data if no data present or day difference between current/requested data more than 90 days
        if mod_ts is None:
            self.add_earnings_history(self.fetch_earnings_history())
        else:
            last_ts = self.get_last_timestamp(self._earnings_history_tbl)

            days_delta = (current - get_dt(last_ts)).days
            days_delta_mod = (current - get_dt(mod_ts)).days

            if self.last_date_ts > mod_ts and days_delta >= 90 and days_delta_mod:
                self.add_earnings_history(self.fetch_earnings_history())

        new_num = self.get_earnings_history_num()

        if initially_connected is False:
            self.db_close()

        return (new_num - num)

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
