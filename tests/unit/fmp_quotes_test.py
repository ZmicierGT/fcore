"""Unit tests for the FMP quotes flow (get(), dividend/split storage and their adjustment
on read) using real FMP instances with synthetic data injected at the _query_api boundary.
Fully offline (:memory: DB). Expected values are derived from the synthetic JSON fixtures.

Unlike YF (which reverse-adjusts at store time), FMP stores raw prices; the read-time
adjustment computes adj_* from the stored prices, dividends and splits.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
import calendar

import numpy as np
import pytest
from dateutil import tz

from data.futils import get_dt
from data.fvalues import Dividends, StockQuotes, StockSplits

from conftest import load_fmp_json, eod_ts, make_fmp

QUOTES_FIXTURE = load_fmp_json('quotes_non-split-adjusted.json')
# The quotes fixture is newest-first
BASE = {'first_date': QUOTES_FIXTURE[-1]['date'], 'last_date': QUOTES_FIXTURE[0]['date']}

def quote_fixture_rows():
    """Map eod_ts -> (open, high, low, close, volume) from the synthetic quotes fixture."""
    rows = {}
    for q in load_fmp_json('quotes_non-split-adjusted.json'):
        rows[eod_ts(q['date'])] = (q['adjOpen'], q['adjHigh'], q['adjLow'], q['adjClose'], q['volume'])
    return rows

def exchange_day_ts(tz, date_str):
    """Timestamp the FMP code stores for a date: midnight in the exchange time zone, as UTC."""
    return calendar.timegm(get_dt(date_str, tz).utctimetuple())

def div_fixture_entries(tz):
    """Synthetic dividends as (ex_ts, amount, decl_ts, record_ts, pay_ts), ascending by ex-date."""
    entries = []
    for d in load_fmp_json('dividends.json'):
        entries.append((exchange_day_ts(tz, d['date']), d['dividend'],
                        exchange_day_ts(tz, d['declarationDate']),
                        exchange_day_ts(tz, d['recordDate']),
                        exchange_day_ts(tz, d['paymentDate'])))
    return sorted(entries, key=lambda e: e[0])

def split_fixture_entries(tz):
    """Synthetic splits as (split_ts, ratio), ascending by split date."""
    entries = [(exchange_day_ts(tz, s['date']), s['numerator'] / s['denominator'])
               for s in load_fmp_json('splits.json')]
    return sorted(entries, key=lambda e: e[0])

def expected_adjusted(ts_list, rows, divs, splits):
    """Replicate stock._get_quotes read-time adjustment on the fixture data."""
    opens = np.array([rows[ts][0] for ts in ts_list], dtype=float)
    closes = np.array([rows[ts][3] for ts in ts_list], dtype=float)
    volumes = np.array([rows[ts][4] for ts in ts_list], dtype=float)

    adj_open = opens.copy()
    adj_close = closes.copy()
    adj_volume = volumes.copy()

    for ex_ts, amount, _, _, _ in divs:
        idx_ex = np.searchsorted(ts_list, [ex_ts], side='right')[0]
        adj_open[:idx_ex] *= 1 - amount / opens[idx_ex]
        adj_close[:idx_ex] *= 1 - amount / closes[idx_ex]

    for split_ts, ratio in splits:
        idx_split = np.searchsorted(ts_list, [split_ts], side='right')[0]
        adj_open[:idx_split] /= ratio
        adj_close[:idx_split] /= ratio
        adj_volume[:idx_split] *= ratio

    return adj_open, adj_close, adj_volume

def build_quotes_inst(make_fmp, **kwargs):
    """Create (inst, fake) with synthetic quotes + dividends + splits covering the whole range."""
    return make_fmp(**BASE, **kwargs)

##############################
# A. Base quotes flow
##############################

def test_quotes_saved(make_fmp):
    inst, fake = build_quotes_inst(make_fmp)
    inst.db_connect()

    rows = inst.get()
    assert len(rows) == len(quote_fixture_rows())
    assert fake.count('historical-price-eod') == 1
    assert inst.get_quotes_num(dt=False) == len(quote_fixture_rows())
    inst.db_close()

def test_quotes_values(make_fmp):
    inst, _ = build_quotes_inst(make_fmp)
    inst.db_connect()

    rows = inst.get()
    expected = quote_fixture_rows()

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

def test_quotes_date_time_format(make_fmp):
    inst, _ = build_quotes_inst(make_fmp)
    inst.db_connect()

    # Daily quotes are stored at EOD 23:59:59; the fixture is newest-first
    rows = inst.get()
    assert rows[0][StockQuotes.DateTime] == f"{QUOTES_FIXTURE[-1]['date']} 23:59:59"
    assert rows[-1][StockQuotes.DateTime] == f"{QUOTES_FIXTURE[0]['date']} 23:59:59"
    inst.db_close()

def test_quotes_cached(make_fmp):
    inst, fake = build_quotes_inst(make_fmp)
    inst.db_connect()

    first = inst.get()
    second = inst.get()

    # The covered range is cached: no second download
    assert fake.count('historical-price-eod') == 1
    assert len(first) == len(second) == len(quote_fixture_rows())
    inst.db_close()

def test_quotes_refetch_upsert(make_fmp):
    inst, fake = build_quotes_inst(make_fmp, refetch=True)
    inst.db_connect()

    inst.get()
    inst.get()

    # refetch=True forces a re-download; UPSERT keeps a single set of rows
    assert fake.count('historical-price-eod') == 2
    assert inst.get_quotes_num(dt=False) == len(quote_fixture_rows())
    inst.db_close()

################################
# B. Dividends & splits storage
################################

def test_dividends_stored(make_fmp):
    inst, _ = build_quotes_inst(make_fmp)
    inst.db_connect()

    inst.get()
    divs = inst._get_db_dividends()
    expected = div_fixture_entries(inst.timezone)

    assert len(divs) == len(expected)
    for row, exp in zip(divs, expected):
        ex_ts, amount, decl_ts, record_ts, pay_ts = exp
        assert row[Dividends.ExDate] == ex_ts
        assert row[Dividends.Amount] == pytest.approx(amount)
        # FMP populates declaration/record/payment dates (unlike YF)
        assert row[Dividends.DeclDate] == decl_ts
        assert row[Dividends.RecordDate] == record_ts
        assert row[Dividends.PaymentDate] == pay_ts
    inst.db_close()

def test_splits_stored(make_fmp):
    inst, _ = build_quotes_inst(make_fmp)
    inst.db_connect()

    inst.get()
    splits = inst._get_db_splits()
    expected = split_fixture_entries(inst.timezone)

    assert len(splits) == len(expected)
    for row, exp in zip(splits, expected):
        split_ts, ratio = exp
        assert row[StockSplits.Date] == split_ts
        assert row[StockSplits.Ratio] == pytest.approx(ratio)
    inst.db_close()

################################
# C. Read-time adjustment & flags
################################

def test_quotes_flags(make_fmp):
    inst, _ = build_quotes_inst(make_fmp)
    inst.db_connect()

    rows = inst.get()
    ts_all = [row[StockQuotes.TimeStamp] for row in rows]

    def row_of(ts):
        for r in rows:
            if r[StockQuotes.TimeStamp] == ts:
                return r
        raise AssertionError(f"no row with ts {ts}")

    # Split flags on the split dates
    for split_ts, ratio in split_fixture_entries(inst.timezone):
        idx = np.searchsorted(ts_all, [split_ts], side='right')[0]
        assert row_of(ts_all[idx])[StockQuotes.Splits] == pytest.approx(ratio)

    # Ex-dividend / payment flags carry the correct per-dividend amount.
    # A payment may fall outside the stored quote window (after the last quote) -
    # the flag is then simply not set, mirroring the read-time behavior.
    for ex_ts, amount, _, _, pay_ts in div_fixture_entries(inst.timezone):
        idx_ex = np.searchsorted(ts_all, [ex_ts], side='right')[0]
        assert row_of(ts_all[idx_ex])[StockQuotes.ExDividends] == pytest.approx(amount)

        idx_pay = np.searchsorted(ts_all, [pay_ts], side='right')[0]
        if idx_pay < len(rows):
            assert row_of(ts_all[idx_pay])[StockQuotes.PayDividends] == pytest.approx(amount)
        else:
            assert pay_ts > ts_all[-1]
    inst.db_close()

def test_quotes_adjusted(make_fmp):
    inst, _ = build_quotes_inst(make_fmp)
    inst.db_connect()

    rows = inst.get()
    ts_all = [row[StockQuotes.TimeStamp] for row in rows]
    expected_o, expected_c, expected_v = expected_adjusted(ts_all, quote_fixture_rows(),
                                                           div_fixture_entries(inst.timezone),
                                                           split_fixture_entries(inst.timezone))

    for i, row in enumerate(rows):
        assert row[StockQuotes.AdjOpen] == pytest.approx(expected_o[i])
        assert row[StockQuotes.AdjClose] == pytest.approx(expected_c[i])
        assert row[StockQuotes.AdjVolume] == pytest.approx(expected_v[i])
    inst.db_close()

############################
# D. Recent quote
############################

def test_recent_quote(make_fmp):
    inst, fake = make_fmp()

    data = inst.get_recent_data()
    q = load_fmp_json('recent_quote.json')[0]
    ts = calendar.timegm(get_dt(q['timestamp'], tz.UTC).utctimetuple())

    assert fake.count('stable/quote') == 1
    assert len(data) == 1

    row = data[0]
    assert row[StockQuotes.TimeStamp] == ts
    assert row[StockQuotes.Open] == pytest.approx(q['open'])
    assert row[StockQuotes.High] == pytest.approx(q['dayHigh'])
    assert row[StockQuotes.Low] == pytest.approx(q['dayLow'])
    assert row[StockQuotes.Close] == pytest.approx(q['price'])
    assert row[StockQuotes.Volume] == q['volume']

    # No adjustments for pseudo real time data: adj columns mirror the raw ones
    assert row[StockQuotes.AdjOpen] == row[StockQuotes.Open]
    assert row[StockQuotes.AdjClose] == row[StockQuotes.Close]
    assert row[StockQuotes.AdjVolume] == row[StockQuotes.Volume]
    assert row[StockQuotes.ExDividends] == 0.0
    assert row[StockQuotes.PayDividends] == 0.0
    assert row[StockQuotes.Splits] == 1.0
