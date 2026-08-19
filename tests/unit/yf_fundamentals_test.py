"""Unit tests for the YF earnings history flow (get_earnings_history, persistence in the
yf_earnings_history table) using real YF instances with synthetic data injected at the
yf boundary. Fully offline (:memory: DB).

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""

import calendar
import datetime

import pytest

from data.fdata import Subquery
from data.fvalues import StockQuotes
from data.yf import YFDataEntries

from conftest import SYNTHETIC_DATA_DIR, FakeTicker, load_earnings_history

EQUITY_INFO = {'quoteType': 'EQUITY', 'symbol': 'FFFF', 'exchangeTimezoneName': 'America/New_York'}

QUOTES = SYNTHETIC_DATA_DIR / 'quotes.csv'
EARNINGS_HISTORY = SYNTHETIC_DATA_DIR / 'earnings_history.csv'

# Expected values of the single report in yf_synthetic_data/earnings_history.csv
EH_FIELDS = ['epsActual', 'epsEstimate', 'epsDifference', 'surprisePercent']
EH_RECORD = load_earnings_history(EARNINGS_HISTORY).iloc[0]
# Quarter of the report (2020-01-20) as a UTC timestamp
EXPECTED_TS = calendar.timegm(EH_RECORD.name.to_pydatetime()
                              .replace(tzinfo=datetime.timezone.utc).utctimetuple())

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
    inst, _ = make_yf(FakeTicker(info=EQUITY_INFO, quotes_path=QUOTES, earnings_history_path=EARNINGS_HISTORY),
                      first_date='2020-1-1', last_date='2020-2-1')
    inst.db_connect()

    inst.get_earnings_history()
    rows = inst.get(queries=[Subquery(YFDataEntries.EarningsHistory, f, title=f) for f in EH_FIELDS])

    # Count is checked via the data API too
    assert inst.get_earnings_history_num() == 1

    matched = 0
    for row in rows:
        for f in EH_FIELDS:
            if row[StockQuotes.TimeStamp] >= EXPECTED_TS:
                assert row[f] == pytest.approx(EH_RECORD[f]), f"{f} mismatch"
            else:
                assert row[f] is None, f"{f} must be None before the report"
        matched += row[StockQuotes.TimeStamp] >= EXPECTED_TS

    assert matched > 0, "the earnings report values were not joined to any quote"
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
