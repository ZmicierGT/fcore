"""Unit tests for the YF outstanding shares flow (get_shares_full, persistence in the
yf_shares_full table and Subquery joins) using real YF instances with synthetic data injected
at the yfinance boundary. Fully offline (:memory: DB).

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
import calendar
import datetime

import pandas as pd

from data.fdata import Subquery
from data.fvalues import StockQuotes
from data.yf import YFDataEntries

from conftest import SYNTHETIC_DATA_DIR, FakeTicker, load_dated_series

EQUITY_INFO = {'quoteType': 'EQUITY', 'symbol': 'FFFF', 'exchangeTimezoneName': 'America/New_York'}
EXCHANGE_TIMEZONE = EQUITY_INFO['exchangeTimezoneName']

QUOTES = SYNTHETIC_DATA_DIR / 'quotes.csv'
SHARES = SYNTHETIC_DATA_DIR / 'shares.csv'


def expected_shares():
    """Load the synthetic shares fixture as {UTC timestamp: shares int} (last per timestamp)."""
    shares = load_dated_series(SHARES, EXCHANGE_TIMEZONE)
    shares = pd.to_numeric(shares, errors='coerce').dropna()
    shares.index = shares.index.tz_convert('UTC')
    shares = shares.groupby(level=0).last().sort_index()

    return {int(calendar.timegm(idx.utctimetuple())): int(round(float(value)))
            for idx, value in shares.items()}


def make_shares_inst(make_yf, shares_path=SHARES, with_quotes=False, **kwargs):
    """Create a connected YF instance populated with synthetic shares (and optional quotes)."""
    ticker = FakeTicker(info=EQUITY_INFO, shares_path=shares_path,
                        quotes_path=QUOTES if with_quotes else None)
    inst, fake = make_yf(ticker, **kwargs)
    inst.db_connect()

    return inst, fake


##############################
# A. Base fetch & persistence
##############################

def test_shares_saved(make_yf):
    inst, fake = make_shares_inst(make_yf)

    assert inst.get_shares_full() == len(expected_shares())
    assert fake.shares_calls == 1
    assert inst.get_shares_full_num() == len(expected_shares())
    inst.db_close()


def test_shares_full_history_requested(make_yf):
    inst, fake = make_shares_inst(make_yf)

    inst.get_shares_full()

    # Full available history is requested, not just the instance date window.
    assert fake.shares_start == datetime.datetime(1970, 1, 1)
    assert fake.shares_end is None
    inst.db_close()


def test_shares_values_and_last_duplicate_wins(make_yf):
    inst, _ = make_shares_inst(make_yf, with_quotes=True)
    expected = expected_shares()

    inst.get()
    inst.get_shares_full()

    rows = inst.get(queries=[Subquery(YFDataEntries.SharesFull.title, 'shares', title='shares')])

    matched = 0
    before_first = 0

    for row in rows:
        ts = row[StockQuotes.TimeStamp]
        applicable = [share_ts for share_ts in expected if share_ts <= ts]

        if applicable:
            exp = expected[max(applicable)]
            assert row['shares'] == exp
            matched += 1
        else:
            assert row['shares'] is None
            before_first += 1

    assert matched > 0, "the shares data was not joined to any quote"
    assert before_first > 0, "quotes before the first shares record must have no shares"
    inst.db_close()


def test_shares_interval_set(make_yf):
    inst, _ = make_shares_inst(make_yf)

    assert inst._get_interval_ts(YFDataEntries.SharesFull.title, is_max=True) is None
    inst.get_shares_full()
    assert inst._get_interval_ts(YFDataEntries.SharesFull.title, is_max=True) is not None
    inst.db_close()


#######################
# B. Caching / refetch
#######################

def test_shares_cached(make_yf):
    inst, fake = make_shares_inst(make_yf)

    inst.get_shares_full()
    inst.get_shares_full()

    assert fake.shares_calls == 1
    assert inst.get_shares_full_num() == len(expected_shares())
    inst.db_close()


def test_shares_refetch_upsert(make_yf):
    inst, fake = make_shares_inst(make_yf, refetch=True)

    inst.get_shares_full()
    inst.get_shares_full()

    assert fake.shares_calls == 2
    assert inst.get_shares_full_num() == len(expected_shares())
    inst.db_close()


#############
# C. Edge cases
#############

def test_shares_empty(make_yf):
    inst, fake = make_shares_inst(make_yf, shares_path=None)

    assert inst.get_shares_full() == 0
    assert fake.shares_calls == 1
    assert inst.get_shares_full_num() == 0

    # The interval marker must prevent repeated fetches for a symbol without shares.
    assert inst.get_shares_full() == 0
    assert fake.shares_calls == 1
    inst.db_close()


def test_shares_remove_symbol_cascade(make_yf):
    inst, _ = make_shares_inst(make_yf)

    inst.get_shares_full()
    assert inst.get_shares_full_num() > 0

    inst.remove_symbol()

    assert inst.get_shares_full_num() == 0
    inst.db_close()
