"""Fixtures for offline YF unit tests using synthetic data injected at the yf boundary.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""

from pathlib import Path

import pandas as pd
import pytest

from data import yf

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
