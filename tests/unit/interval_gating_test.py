"""Unit tests for the _need_to_update interval-gating logic (data/fdata.py).

Deterministic timestamps are achieved purely through the API: markers are written via
_update_data_interval() with the instance's _current_ts() patched to a chosen value
(the only way to create a stale marker offline), and gaps are probed via
_need_to_update(). No direct SQL is used.

Offline (tmp file DB where scenarios span instances).

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
import calendar
from datetime import datetime

import pytest

from data.fmp import FMPDataEntries
from data.fvalues import Timespans

from conftest import make_fmp

# A deterministic window in the past
FIRST_DATE = '2023-06-01'
LAST_DATE = '2023-06-15'

def _ts(date_str):
    return calendar.timegm(datetime.strptime(date_str, '%Y-%m-%d').utctimetuple())

FIRST_TS = _ts(FIRST_DATE)
LAST_TS = _ts(LAST_DATE)

def make_inst(make_fmp, tmp_path, db_name, **kwargs):
    """Connected FMP instance with a known symbol in a fixed past date window."""
    params = {'first_date': FIRST_DATE, 'last_date': LAST_DATE, **kwargs}
    inst, _ = make_fmp(db_name=db_name, **params)
    inst.db_connect()
    inst.get_info()  # makes sure the symbol/entry registration exists
    return inst

def set_marker(inst, entry, when_ts):
    """Write the data entry marker as if it was fetched at when_ts (max_ts = when_ts)."""
    real = inst._current_ts
    inst._current_ts = lambda adjusted=True: when_ts
    try:
        inst._update_data_interval(entry)
    finally:
        inst._current_ts = real

# NOTE: for quotes the marker min_ts is written as first_date_ts by the library, so the
# 'uncovered start' is expressed from the request side: a wider-window instance on the
# same DB instead of tailored min_ts values.

@pytest.fixture
def tmp_db(tmp_path):
    return str(tmp_path / 'test.sqlite')

##############################
# A. Data entries (fundamentals) gating
##############################

def test_entry_no_marker_needs_update(make_fmp, tmp_db):
    inst = make_inst(make_fmp, None, tmp_db)
    assert inst._need_to_update(FMPDataEntries.IncomeStatement) is True
    inst.db_close()

def test_entry_fresh_marker_no_update(make_fmp, tmp_db):
    inst = make_inst(make_fmp, None, tmp_db)
    set_marker(inst, FMPDataEntries.IncomeStatement, LAST_TS)  # exactly at the window end: not stale
    assert inst._need_to_update(FMPDataEntries.IncomeStatement) is False
    inst.db_close()

def test_entry_same_day_gap_no_update(make_fmp, tmp_db):
    inst = make_inst(make_fmp, None, tmp_db)
    set_marker(inst, FMPDataEntries.IncomeStatement, LAST_TS - 3600)  # within the one-day grace gap
    assert inst._need_to_update(FMPDataEntries.IncomeStatement) is False
    inst.db_close()

def test_entry_stale_marker_needs_update(make_fmp, tmp_db):
    inst = make_inst(make_fmp, None, tmp_db)
    set_marker(inst, FMPDataEntries.IncomeStatement, LAST_TS - 86401)  # more than one day stale
    assert inst._need_to_update(FMPDataEntries.IncomeStatement) is True
    inst.db_close()

def test_entry_marker_beyond_window_no_update(make_fmp, tmp_db):
    inst = make_inst(make_fmp, None, tmp_db)
    set_marker(inst, FMPDataEntries.IncomeStatement, LAST_TS + 86400 * 10)  # fetched later than the window end
    assert inst._need_to_update(FMPDataEntries.IncomeStatement) is False
    inst.db_close()

##############################
# B. Quotes timespan gating
##############################

def test_quotes_no_marker_needs_update(make_fmp, tmp_db):
    inst = make_inst(make_fmp, None, tmp_db)
    assert inst._need_to_update() is True
    inst.db_close()

def test_quotes_covered_range_no_update(make_fmp, tmp_db):
    # Write the marker exactly as a real fetch of [FIRST_DATE, LAST_DATE] would do.
    # _update_data_interval() for quotes caps max_ts at min(now, last_date_ts); by setting
    # an instance with first_date=FIRST_DATE and "now" == LAST_TS a day-fresh marker is made.
    writer = make_inst(make_fmp, None, tmp_db)
    real = writer._current_ts
    writer._current_ts = lambda adjusted=True: LAST_TS + (24 * 3600 - 1)
    try:
        writer._update_data_interval()
    finally:
        writer._current_ts = real

    # Marker now covers [FIRST_TS, LAST_TS]: a same-window instance must not refetch
    assert writer._need_to_update() is False
    assert writer._get_interval_ts(Timespans.Day, is_max=False) == FIRST_TS

    # A narrower same-interior window on the same DB is covered as well
    reader = make_inst(make_fmp, None, tmp_db, first_date='2023-06-05', last_date='2023-06-10')
    assert reader._need_to_update() is False

    writer.db_close()
    reader.db_close()

def test_quotes_uncovered_start_needs_update(make_fmp, tmp_db):
    writer = make_inst(make_fmp, None, tmp_db)
    writer._update_data_interval()  # stored min_ts == FIRST_TS

    # A wider window starting before the stored range must trigger a fetch
    reader = make_inst(make_fmp, None, tmp_db, first_date='2023-05-01')
    assert reader._need_to_update() is True

    writer.db_close()
    reader.db_close()

def test_quotes_uncovered_end_needs_update(make_fmp, tmp_db):
    writer = make_inst(make_fmp, None, tmp_db)
    writer._update_data_interval()  # stored max_ts == LAST_TS (capped by last_date_ts)

    # A wider window ending after the stored range must trigger a fetch
    reader = make_inst(make_fmp, None, tmp_db, last_date='2023-06-20')
    assert reader._need_to_update() is True

    writer.db_close()
    reader.db_close()

##############################
# C. refetch bypass
##############################

@pytest.mark.parametrize('entry', [None, FMPDataEntries.IncomeStatement])
def test_refetch_bypasses_gating(make_fmp, tmp_db, entry):
    writer = make_inst(make_fmp, None, tmp_db)
    writer._update_data_interval(entry)
    writer.db_close()

    refetch_inst = make_inst(make_fmp, None, tmp_db, refetch=True)
    # refetch=True ignores the stored markers entirely
    assert refetch_inst._need_to_update(entry) is True
    refetch_inst.db_close()

##############################
# D. End-to-end: cached fundamentals are not refetched
##############################

def test_income_statement_not_refetched_within_gap(make_fmp):
    inst, fake = make_fmp()
    inst.db_connect()

    inst.get_income_statement()
    calls_after_first = fake.count('income-statement')
    assert calls_after_first == 2  # annual + quarterly

    inst.get_income_statement()
    assert fake.count('income-statement') == calls_after_first
    inst.db_close()
