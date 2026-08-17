"""Unit tests for the YF earnings history flow (get_earnings_history, persistence in the
yf_earnings_history table) using real YF instances with synthetic data injected at the
yf boundary. Fully offline (:memory: DB).

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""

import calendar
import datetime

import pytest

from data.yf import YFDataEntries

from conftest import SYNTHETIC_DATA_DIR, FakeTicker

EQUITY_INFO = {'quoteType': 'EQUITY', 'symbol': 'FFFF', 'exchangeTimezoneName': 'America/New_York'}

EARNINGS_HISTORY = SYNTHETIC_DATA_DIR / 'earnings_history.csv'
# Expected row from synthetic_data/earnings_history.csv (quarter 2020-01-20 as UTC timestamp)
EXPECTED_TS = calendar.timegm(datetime.datetime(2020, 1, 20, tzinfo=datetime.timezone.utc).utctimetuple())
EXPECTED_ROW = (EXPECTED_TS, 1.85, 1.72, 0.13, 0.0756)

def read_eh_rows(inst):
    """Read back the persisted earnings history rows ordered by time_stamp."""
    rows = inst._cur.execute(f"""SELECT time_stamp, epsActual, epsEstimate, epsDifference, surprisePercent
                                 FROM {YFDataEntries.EarningsHistory} ORDER BY time_stamp""").fetchall()
    return rows

##############################
# A. Base fetch & persistence
##############################

def test_earnings_history_saved(make_yf):
    inst, fake = make_yf(FakeTicker(info=EQUITY_INFO, earnings_history_path=EARNINGS_HISTORY))
    inst.db_connect()

    assert inst.get_earnings_history() == 1
    assert fake.eh_calls == 1
    assert inst.get_earnings_history_num() == 1
    inst.db_close()

def test_earnings_history_values(make_yf):
    inst, _ = make_yf(FakeTicker(info=EQUITY_INFO, earnings_history_path=EARNINGS_HISTORY))
    inst.db_connect()

    inst.get_earnings_history()

    rows = read_eh_rows(inst)
    assert len(rows) == 1
    assert rows[0] == pytest.approx(EXPECTED_ROW)
    inst.db_close()

def test_earnings_history_interval_set(make_yf):
    inst, _ = make_yf(FakeTicker(info=EQUITY_INFO, earnings_history_path=EARNINGS_HISTORY))
    inst.db_connect()

    assert inst._get_interval_ts(YFDataEntries.EarningsHistory, is_max=True) is None
    inst.get_earnings_history()
    assert inst._get_interval_ts(YFDataEntries.EarningsHistory, is_max=True) is not None
    inst.db_close()

#######################
# B. Caching / refetch
#######################

def test_earnings_history_cached(make_yf):
    inst, fake = make_yf(FakeTicker(info=EQUITY_INFO, earnings_history_path=EARNINGS_HISTORY))
    inst.db_connect()

    inst.get_earnings_history()
    inst.get_earnings_history()

    # Interval marker prevents the second fetch
    assert fake.eh_calls == 1
    assert inst.get_earnings_history_num() == 1
    inst.db_close()

def test_earnings_history_refetch_upsert(make_yf):
    inst, fake = make_yf(FakeTicker(info=EQUITY_INFO, earnings_history_path=EARNINGS_HISTORY), refetch=True)
    inst.db_connect()

    inst.get_earnings_history()
    inst.get_earnings_history()

    # refetch=True forces a re-fetch, UPSERT keeps a single row
    assert fake.eh_calls == 2
    assert inst.get_earnings_history_num() == 1
    inst.db_close()

#############
# C. Edge case
#############

def test_earnings_history_empty(make_yf):
    # earnings_history_path=None: the fake returns an empty DataFrame (like yfinance does)
    inst, fake = make_yf(FakeTicker(info=EQUITY_INFO))
    inst.db_connect()

    assert inst.get_earnings_history() == 0
    assert fake.eh_calls == 1
    assert inst.get_earnings_history_num() == 0
    inst.db_close()
