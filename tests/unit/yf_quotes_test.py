"""Unit tests for the YF quotes flow (get(), split reverse-adjustment at store time, and
split/dividend adjustment on read) using real YF instances with synthetic data injected at
the yfinance boundary. Fully offline (:memory: DB). Expected values are obtained from the
synthetic CSVs rather than hardcoded.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
import calendar
import datetime

import pandas as pd
import pytest

from data.fvalues import Dividends, StockQuotes, StockSplits

from conftest import SYNTHETIC_DATA_DIR, FakeTicker, load_dated_series, load_quotes

# TODO HIGH Add a case to test split/dividends on the same date.

EQUITY_INFO = {'quoteType': 'EQUITY', 'symbol': 'FFFF', 'exchangeTimezoneName': 'America/New_York'}
EXCHANGE_TIMEZONE = EQUITY_INFO['exchangeTimezoneName']

QUOTES = SYNTHETIC_DATA_DIR / 'quotes.csv'
SPLITS = SYNTHETIC_DATA_DIR / 'splits.csv'
DIVS = SYNTHETIC_DATA_DIR / 'divs.csv'


def eod_ts(year, month, day):
    """EOD (23:59:59 UTC) timestamp of a date."""
    return calendar.timegm(datetime.datetime(year, month, day, 23, 59, 59,
                                             tzinfo=datetime.timezone.utc).utctimetuple())


def csv_quote_rows(path):
    """Map a date -> (open, high, low, close, volume) from a synthetic quotes CSV."""
    df = load_quotes(path)
    df.columns = df.columns.get_level_values(0)
    rows = {}
    for idx, row in df.iterrows():
        ts = eod_ts(idx.year, idx.month, idx.day)
        rows[ts] = (row['Open'], row['High'], row['Low'], row['Close'], row['Volume'])
    return rows


def series_midnight_ts(series):
    """Convert a tz-aware Series index to UTC-midnight timestamps (as the fetch code does)."""
    return [int(calendar.timegm(idx.tz_convert('UTC').normalize().utctimetuple())) for idx in series.index]


def build_quotes_inst(make_yf, paths=None, **kwargs):
    """Create (inst, fake) patched with synthetic quotes (and optional splits/dividends)."""
    paths = paths or {}
    base = {'first_date': '2020-1-1', 'last_date': '2020-2-1'}
    return make_yf(FakeTicker(info=EQUITY_INFO, quotes_path=QUOTES, **paths), **base, **kwargs)


##############################
# A. Base quotes flow
##############################

def test_quotes_saved(make_yf):
    inst, fake = build_quotes_inst(make_yf)
    inst.db_connect()

    rows = inst.get()
    assert len(rows) == len(csv_quote_rows(QUOTES))
    assert fake.download_calls == 1
    assert inst.get_quotes_num(dt=False) == len(csv_quote_rows(QUOTES))
    inst.db_close()


def test_quotes_values(make_yf):
    inst, _ = build_quotes_inst(make_yf)
    inst.db_connect()

    rows = inst.get()
    expected = csv_quote_rows(QUOTES)

    for row in rows:
        # Daily quotes are stored at EOD 23:59:59
        assert row[StockQuotes.TimeStamp] % 86400 == 86399
        exp = expected[row[StockQuotes.TimeStamp]]
        assert row[StockQuotes.Open] == pytest.approx(exp[0])
        assert row[StockQuotes.High] == pytest.approx(exp[1])
        assert row[StockQuotes.Low] == pytest.approx(exp[2])
        assert row[StockQuotes.Close] == pytest.approx(exp[3])
        assert row[StockQuotes.Volume] == exp[4]
    inst.db_close()


def test_quotes_date_time_format(make_yf):
    inst, _ = build_quotes_inst(make_yf)
    inst.db_connect()

    rows = inst.get()
    assert rows[0][StockQuotes.DateTime] == '2020-01-02 23:59:59'
    assert rows[-1][StockQuotes.DateTime] == '2020-01-31 23:59:59'
    inst.db_close()


def test_quotes_cached(make_yf):
    inst, fake = build_quotes_inst(make_yf)
    inst.db_connect()

    first = inst.get()
    second = inst.get()

    # The covered range is cached: no second download
    assert fake.download_calls == 1
    assert len(first) == len(second) == len(csv_quote_rows(QUOTES))
    assert inst.get_quotes_num(dt=False) == len(csv_quote_rows(QUOTES))
    inst.db_close()


def test_quotes_refetch_upsert(make_yf):
    inst, fake = build_quotes_inst(make_yf, refetch=True)
    inst.db_connect()

    inst.get()
    inst.get()

    # refetch=True forces a re-download; UPSERT keeps a single set of rows
    assert fake.download_calls == 2
    assert inst.get_quotes_num(dt=False) == len(csv_quote_rows(QUOTES))
    inst.db_close()


###################################
# B. Splits & dividends (adjustments)
###################################

def test_splits_stored(make_yf):
    inst, _ = build_quotes_inst(make_yf, paths={'splits_path': SPLITS})
    inst.db_connect()

    inst.get()
    splits = inst._get_db_splits()
    expected_ts = series_midnight_ts(load_dated_series(SPLITS, EXCHANGE_TIMEZONE))

    assert inst.get_split_num() == 1
    assert len(splits) == 1
    assert splits[0][StockSplits.Date] == expected_ts[0]
    assert splits[0][StockSplits.Ratio] == pytest.approx(load_dated_series(SPLITS, EXCHANGE_TIMEZONE).iloc[0])
    inst.db_close()


def test_dividends_stored(make_yf):
    inst, _ = build_quotes_inst(make_yf, paths={'dividends_path': DIVS})
    inst.db_connect()

    inst.get()
    divs = inst._get_db_dividends()
    expected_ts = series_midnight_ts(load_dated_series(DIVS, EXCHANGE_TIMEZONE))

    assert inst.get_dividends_num() == 1
    assert len(divs) == 1
    assert divs[0][Dividends.ExDate] == expected_ts[0]
    assert divs[0][Dividends.Amount] == pytest.approx(load_dated_series(DIVS, EXCHANGE_TIMEZONE).iloc[0])
    # YF never populates declaration/record/payment dates
    assert pd.isna(divs[0][Dividends.DeclDate])
    assert pd.isna(divs[0][Dividends.RecordDate])
    assert pd.isna(divs[0][Dividends.PaymentDate])
    inst.db_close()


def test_quotes_reverse_adjusted_roundtrip(make_yf):
    inst, _ = build_quotes_inst(make_yf, paths={'splits_path': SPLITS, 'dividends_path': DIVS})
    inst.db_connect()

    rows = inst.get()
    expected = csv_quote_rows(QUOTES)
    splits_series = load_dated_series(SPLITS, EXCHANGE_TIMEZONE)
    divs_series = load_dated_series(DIVS, EXCHANGE_TIMEZONE)
    split_ts = series_midnight_ts(splits_series)[0]
    ex_ts = series_midnight_ts(divs_series)[0]
    amount = float(divs_series.iloc[0])

    # Dividend deflator computed from the ex-date row's prices (as done on read)
    ex_ts_eod = ex_ts + 86400 - 1
    ex_open, _, _, ex_close, _ = expected[ex_ts_eod]
    o_ratio = 1 - amount / ex_open
    c_ratio = 1 - amount / ex_close

    for row in rows:
        ts = row[StockQuotes.TimeStamp]
        exp = expected[ts]
        if ts < split_ts:
            # Reverse-adjusted at store (x2) then scale-restored on read: net = csv x dividend deflator
            assert row[StockQuotes.AdjOpen] == pytest.approx(exp[0] * o_ratio)
            assert row[StockQuotes.AdjClose] == pytest.approx(exp[3] * c_ratio)
            assert row[StockQuotes.AdjVolume] == exp[4]
        elif ts < ex_ts:
            # On/after the split but before the ex-date: csv price x dividend deflator
            assert row[StockQuotes.AdjClose] == pytest.approx(exp[3] * c_ratio)
            assert row[StockQuotes.AdjVolume] == exp[4]
        else:
            # At/after the ex-date: no adjustments
            assert row[StockQuotes.AdjOpen] == pytest.approx(exp[0])
            assert row[StockQuotes.AdjClose] == pytest.approx(exp[3])
            assert row[StockQuotes.AdjVolume] == exp[4]
    inst.db_close()


def test_get_flags(make_yf):
    inst, _ = build_quotes_inst(make_yf, paths={'splits_path': SPLITS, 'dividends_path': DIVS})
    inst.db_connect()

    rows = inst.get()
    splits_series = load_dated_series(SPLITS, EXCHANGE_TIMEZONE)
    divs_series = load_dated_series(DIVS, EXCHANGE_TIMEZONE)
    split_ts = series_midnight_ts(splits_series)[0]
    ex_ts = series_midnight_ts(divs_series)[0]
    ratio = splits_series.iloc[0]
    amount = divs_series.iloc[0]

    def row_of(ts):
        for r in rows:
            if r[StockQuotes.TimeStamp] == ts:
                return r
        raise AssertionError(f"no row with ts {ts}")

    # Split flag on the split date; ex-dividend on the ex date; payment on ex+7d (a CSV trading day)
    assert row_of(split_ts + 86400 - 1)[StockQuotes.Splits] == pytest.approx(ratio)
    assert row_of(ex_ts + 86400 - 1)[StockQuotes.ExDividends] == pytest.approx(amount)
    assert row_of(ex_ts + 7 * 86400 + 86400 - 1)[StockQuotes.PayDividends] == pytest.approx(amount)
    inst.db_close()
