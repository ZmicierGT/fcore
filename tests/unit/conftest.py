"""Fixtures for offline YF/FMP unit tests using synthetic data injected at the data source boundary.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
import calendar
import json
from datetime import datetime

from pathlib import Path

import pandas as pd
import pytest

from data import yf
from data import fmp as fmp_module

# TODO HIGH Add cases to check intervals.

SYNTHETIC_DATA_DIR = Path(__file__).resolve().parent.parent / 'yf_synthetic_data'

def load_dated_series(path, timezone):
    """Deserialize a synthetic Date,<value> CSV into a Series (index in the exchange timezone)."""
    df = pd.read_csv(path, index_col=0)
    df.index = pd.to_datetime(df.index, utc=True).tz_convert(timezone).as_unit('s')
    return df.iloc[:, 0]  # column name comes from the CSV header

def load_earnings_history(path):
    """Deserialize a synthetic earnings history CSV into a DataFrame (empty if path is None)."""
    if path is None:
        return pd.DataFrame()
    df = pd.read_csv(path, index_col=0)
    df.index = pd.to_datetime(df.index)
    df.index.name = 'quarter'
    return df

def load_quotes(path):
    """Deserialize a synthetic quotes CSV into a yf.download-style DataFrame (empty if path is None)."""
    if path is None:
        return pd.DataFrame()
    df = pd.read_csv(path, header=[0, 1], index_col=0)
    df.index = pd.to_datetime(df.index)
    df.index.name = 'Date'
    return df

class FakeTicker:
    """Fake yf Ticker returning synthetic data loaded from files (paths may be None)."""

    def __init__(self, info, quotes_path=None, splits_path=None, dividends_path=None, earnings_history_path=None):
        self._info = info
        self._quotes_path = quotes_path
        self._splits_path = splits_path
        self._dividends_path = dividends_path
        self._earnings_history_path = earnings_history_path

        self.info_calls = 0
        self.download_calls = 0
        self.history_calls = 0
        self.eh_calls = 0

    @property
    def info(self):
        self.info_calls += 1
        return dict(self._info)

    def history(self, period="max"):
        self.history_calls += 1
        # TODO HIGH To implement

    def download(self, *args, **kwargs):
        self.download_calls += 1
        return load_quotes(self._quotes_path)

    @property
    def exchange_timezone(self):
        """Exchange timezone from info (America/New_York fallback for non-existent)."""
        if self._info is None or 'exchangeTimezoneName' not in self._info:
            return 'America/New_York'
        return self._info['exchangeTimezoneName']

    @property
    def splits(self):
        if self._splits_path is None:
            return pd.Series(dtype=float)
        return load_dated_series(self._splits_path, self.exchange_timezone)

    @property
    def dividends(self):
        if self._dividends_path is None:
            return pd.Series(dtype=float)
        return load_dated_series(self._dividends_path, self.exchange_timezone)

    @property
    def earnings_history(self):
        self.eh_calls += 1
        return load_earnings_history(self._earnings_history_path)

@pytest.fixture
def make_yf(monkeypatch):
    """Create a real YF (bound to :memory:/tmp DB) with a fake ticker patched in; return (inst, fake)."""
    def _make(ticker=None, db_name=':memory:', symbol='FFFF', **kwargs):
        fake = ticker if ticker is not None else FakeTicker(info={})
        monkeypatch.setattr(yf.yfin, 'Ticker', lambda symbol: fake)
        monkeypatch.setattr(yf.yfin, 'download', fake.download)

        return yf.YF(symbol=symbol, db_name=db_name, verbosity=False, **kwargs), fake
    return _make


###############################################################################
# FMP fixtures (synthetic data injected at the _query_api boundary)
###############################################################################

FMP_SYNTHETIC_DATA_DIR = Path(__file__).resolve().parent.parent / 'fmp_synthetic_data'

def load_fmp_json(name):
    """Deserialize a synthetic FMP JSON fixture file."""
    with open(FMP_SYNTHETIC_DATA_DIR / name, encoding='utf-8') as f:
        return json.load(f)

def eod_ts(date_str):
    """EOD (23:59:59 UTC) timestamp of a YYYY-MM-DD date (as FMP quotes are stored)."""
    dt = datetime.strptime(date_str, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
    return calendar.timegm(dt.utctimetuple())

def fixture_filing_ts(r):
    """Timestamp used by FMP._fetch_fundamentals for a report's time_stamp (based on filingDate)."""
    return int(datetime.timestamp(datetime.strptime(r['filingDate'], '%Y-%m-%d')))

class FakeResponse:
    """Minimal stand-in for requests.Response exposing just json()."""
    def __init__(self, payload):
        self._payload = payload
    def json(self):
        return self._payload

class FakeQueryApi:
    """Fake FMP HTTP boundary: routes FMP request URLs to synthetic JSON payloads.

    Every FMP fetch goes through FMP._query_api; routes are matched by URL substrings.
    All requests are recorded so tests can assert the number/kind of API calls.
    """

    def __init__(self, profile='profile.json', routes=None):
        self._profile = profile
        self._routes = routes or {}
        self.calls = []

    def query(self, url, timeout=30):
        self.calls.append(url)
        return FakeResponse(self._payload(url))

    def _payload(self, url):
        for token, payload in self._routes.items():
            if token in url:
                return payload
        if 'profile' in url:
            return load_fmp_json(self._profile)
        if 'historical-price-eod/non-split-adjusted' in url:
            return load_fmp_json('quotes_non-split-adjusted.json')
        if 'historical-market-capitalization' in url:
            return load_fmp_json('historical-market-capitalization.json')
        if 'dividends' in url:
            return load_fmp_json('dividends.json')
        if 'splits' in url:
            return load_fmp_json('splits.json')
        if 'quote' in url:
            return load_fmp_json('recent_quote.json')
        for base in ('income-statement', 'balance-sheet-statement', 'cash-flow-statement'):
            if base in url:
                period = 'annual' if 'period=year' in url else 'quarterly'
                return load_fmp_json(f'{base}_{period}.json')
        raise AssertionError(f'Unexpected FMP URL requested by tests: {url}')

    def count(self, token):
        """Number of recorded API calls whose URL contains the given token."""
        return sum(1 for url in self.calls if token in url)

@pytest.fixture
def make_fmp(monkeypatch):
    """Create a real FMP (bound to :memory:/tmp DB) with a fake _query_api patched in;
    return (inst, fake)."""
    def _make(symbol='FFFF', db_name=':memory:', profile='profile.json', routes=None, **kwargs):
        fake = FakeQueryApi(profile=profile, routes=routes)
        monkeypatch.setattr(fmp_module.FMP, '_query_api', fake.query)

        return fmp_module.FMP(symbol=symbol, db_name=db_name, verbosity=False, **kwargs), fake
    return _make
