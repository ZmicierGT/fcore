"""Unit tests for the data_intervals freshness/gating machinery (data/fdata.py):

- marker handling behind _need_to_update() (_update_data_interval/_get_interval_ts)
- _get_modified_ts() reading of markers (quotes TTL)
- interval-based gating of info/fundamentals fetches
- adaptive cadence logic (fresh_days/cadence_days of the data entries)
- drop_symbol_intervals()/drop_datasource_intervals() (manual refetch bypass)

Deterministic marker ages are achieved by patching _current_ts when writing markers
via _update_data_interval(). Dates derived from the fixture data are computed rather
than hardcoded where possible.

Offline (:memory: or tmp-file DBs; fake data sources from conftest).

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
import calendar
from datetime import datetime, timezone

import pytest

from data.fdata import FdataError, CommonDataEntries
from data.fmp import FMPDataEntries
from data.stock import StockDataEntries
from data.fvalues import Timespans, ReportPeriod

from conftest import make_fmp, load_fmp_json

# A deterministic window in the past; all other dates are derived relative to it
FIRST_DATE = '2023-06-01'
LAST_DATE = '2023-06-15'

SEC = 86400

def _ts(date_str):
    return calendar.timegm(datetime.strptime(date_str, '%Y-%m-%d').utctimetuple())

def _datestr(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime('%Y-%m-%d')

FIRST_TS = _ts(FIRST_DATE)
LAST_TS = _ts(LAST_DATE)

# Quotes fixture: window that is actually fetchable offline
QUOTES_FIXTURE = load_fmp_json('quotes_non-split-adjusted.json')  # newest first
Q_FIRST, Q_LAST = QUOTES_FIXTURE[-1]['date'], QUOTES_FIXTURE[0]['date']

# Due maths for the cadence tests are derived from the fixture data itself:
# the latest annual filing date is the base, due = base + cadence_days.
BASE_TS = max(_ts(r['filingDate']) for r in load_fmp_json('income-statement_annual.json'))
DUE_TS = BASE_TS + FMPDataEntries.IncomeStatement.cadence_days * SEC

IS = FMPDataEntries.IncomeStatement

def make_inst(make_fmp, db_name=':memory:', **kwargs):
    """Connected FMP instance with a known symbol (get_info seeds the symbol + info markers)."""
    params = {'first_date': FIRST_DATE, 'last_date': LAST_DATE, **kwargs}
    inst, _ = make_fmp(db_name=db_name, **params)
    inst.db_connect()
    inst.get_info()
    return inst

def set_marker(inst, entry, when_ts):
    """Write the data entry marker as if it was fetched at when_ts (max_ts = when_ts).

    An existing marker is deleted first - the UPSERT keeps the max of the old/new
    max_ts, so overwriting with an older timestamp is not possible otherwise.
    """
    inst.drop_symbol_intervals()

    real = inst._current_ts
    inst._current_ts = lambda adjusted=True: when_ts
    try:
        inst._update_data_interval(entry.title)
    finally:
        inst._current_ts = real

def assert_recent(ts):
    assert isinstance(ts, int)
    assert _ts(datetime.now().strftime('%Y-%m-%d')) - SEC <= ts <= _ts(datetime.now().strftime('%Y-%m-%d')) + 2 * SEC


# NOTE: for quotes the marker min_ts is written as first_date_ts by the library, so
# 'uncovered start/end' cases are expressed from the request side: a wider-window
# instance on the same DB instead of tailored marker bounds.

#############################
# A. Data entries markers gating
#############################

def test_entry_no_marker_needs_update(make_fmp):
    inst = make_inst(make_fmp)
    assert inst._need_to_update(IS) is True
    inst.db_close()

def test_entry_fresh_marker_no_update(make_fmp):
    inst = make_inst(make_fmp)
    set_marker(inst, IS, LAST_TS)  # exactly at the window end: not stale
    assert inst._need_to_update(IS) is False
    inst.db_close()

def test_entry_same_day_gap_no_update(make_fmp):
    inst = make_inst(make_fmp)
    set_marker(inst, IS, LAST_TS - 3600)  # within the grace gap
    assert inst._need_to_update(IS) is False
    inst.db_close()

def test_entry_stale_marker_needs_update(make_fmp):
    inst = make_inst(make_fmp)
    # Stale beyond both cadence (no data rows -> the marker is the due base)
    # and fresh_days: polling fetch is due
    set_marker(inst, IS, LAST_TS - (IS.cadence_days + 1) * SEC)
    assert inst._need_to_update(IS) is True
    inst.db_close()

def test_entry_marker_beyond_window_no_update(make_fmp):
    inst = make_inst(make_fmp)
    set_marker(inst, IS, LAST_TS + 10 * SEC)  # fetched later than the window end
    assert inst._need_to_update(IS) is False
    inst.db_close()

#############################
# B. Quotes timespan gating
#############################

def test_quotes_no_marker_needs_update(make_fmp):
    inst = make_inst(make_fmp)
    assert inst._need_to_update() is True
    inst.db_close()

def test_quotes_covered_range_no_update(make_fmp, shared_mem_db):
    # Write the marker exactly as a real fetch of [FIRST_DATE, LAST_DATE] would do:
    # _update_data_interval() for quotes caps max_ts at min(now, last_date_ts), so
    # writing it one day short of LAST_TS leaves a day-fresh marker.
    writer = make_inst(make_fmp, shared_mem_db)
    real = writer._current_ts
    writer._current_ts = lambda adjusted=True: LAST_TS + (SEC - 1)
    try:
        writer._update_data_interval()
    finally:
        writer._current_ts = real

    # Marker now covers [FIRST_TS, LAST_TS]: a same-window instance must not refetch
    assert writer._need_to_update() is False
    assert writer._get_interval_ts(Timespans.Day, is_max=False) == FIRST_TS

    # A narrower same-interior window on the same DB is covered as well
    reader = make_inst(make_fmp, shared_mem_db,
                       first_date=_datestr(FIRST_TS + 4 * SEC), last_date=_datestr(LAST_TS - 5 * SEC))
    assert reader._need_to_update() is False

    writer.db_close()
    reader.db_close()

def test_quotes_uncovered_start_needs_update(make_fmp, shared_mem_db):
    writer = make_inst(make_fmp, shared_mem_db)
    writer._update_data_interval()  # stored min_ts == FIRST_TS

    # A wider window starting before the stored range must trigger a fetch
    reader = make_inst(make_fmp, shared_mem_db, first_date=_datestr(FIRST_TS - 31 * SEC))
    assert reader._need_to_update() is True

    writer.db_close()
    reader.db_close()

def test_quotes_uncovered_end_needs_update(make_fmp, shared_mem_db):
    writer = make_inst(make_fmp, shared_mem_db)
    writer._update_data_interval()  # stored max_ts == LAST_TS (capped by last_date_ts)

    # A wider window ending after the stored range must trigger a fetch
    reader = make_inst(make_fmp, shared_mem_db, last_date=_datestr(LAST_TS + 5 * SEC))
    assert reader._need_to_update() is True

    writer.db_close()
    reader.db_close()

#############################
# C. refetch bypass
#############################

@pytest.mark.parametrize('entry', [None, IS])
def test_refetch_bypasses_gating(make_fmp, shared_mem_db, entry):
    writer = make_inst(make_fmp, shared_mem_db)
    writer._update_data_interval(None if entry is None else entry.title)

    # refetch_inst must connect before writer disconnects: the shared in-memory
    # cache exists only while at least one connection to it is open.
    refetch_inst = make_inst(make_fmp, shared_mem_db, refetch=True)
    writer.db_close()
    # refetch=True ignores the stored markers entirely
    assert refetch_inst._need_to_update(entry) is True
    refetch_inst.db_close()

def test_update_data_interval_unknown_entry_raises(make_fmp):
    inst = make_inst(make_fmp)
    with pytest.raises(FdataError):
        inst._update_data_interval('no_such_entry')
    inst.db_close()

#############################
# D. _get_modified_ts
#############################

def test_modified_ts_unknown_entry_is_none(make_fmp):
    """An unregistered entry has no interval record (consistent with _get_interval_ts)."""
    inst = make_inst(make_fmp)
    assert inst._get_modified_ts('no_such_entry') is None
    inst.db_close()

def test_modified_ts_none_without_markers(make_fmp):
    inst = make_inst(make_fmp)
    assert inst._get_modified_ts() is None                # quotes interval (current timespan)
    assert inst._get_modified_ts(Timespans.Day) is None
    assert inst._get_modified_ts(IS.title) is None
    inst.db_close()

def test_modified_ts_dataset_marker(make_fmp):
    inst = make_inst(make_fmp)
    assert inst._get_modified_ts(IS.title) is None
    inst._update_data_interval(IS.title)
    assert_recent(inst._get_modified_ts(IS.title))
    inst.db_close()

def test_modified_ts_quotes_marker(make_fmp):
    inst = make_inst(make_fmp, first_date=Q_FIRST, last_date=Q_LAST)
    inst.get()
    ts = inst._get_modified_ts()
    assert_recent(ts)
    assert ts == inst._get_modified_ts(Timespans.Day)
    inst.db_close()

def test_modified_ts_info_markers(make_fmp):
    inst = make_inst(make_fmp)  # get_info() inside wrote the info markers
    assert_recent(inst._get_modified_ts(CommonDataEntries.SecurityInfo.title))
    assert_recent(inst._get_modified_ts(StockDataEntries.StockInfo.title))
    inst.db_close()

#############################
# E. Info gating via intervals
#############################

def test_info_fetched_once_via_marker(make_fmp):
    inst, fake = make_fmp()
    inst.db_connect()
    assert fake.count('profile') == 0
    inst.get_info()
    inst.get_info()
    assert fake.count('profile') == 1

    # Second call is served from memory, so verify marker gating directly
    assert inst._need_to_update(CommonDataEntries.SecurityInfo) is False
    assert inst._need_to_update(StockDataEntries.StockInfo) is False
    inst.db_close()

def test_security_info_cadence(make_fmp):
    entry = CommonDataEntries.SecurityInfo
    # Due (marker + cadence_days) is still ahead -> no update
    inst = make_inst(make_fmp)
    set_marker(inst, entry, LAST_TS - (entry.cadence_days - 10) * SEC)
    assert inst._need_to_update(entry) is False
    inst.db_close()

    # Due passed and the marker is older than fresh_days -> update
    inst = make_inst(make_fmp)
    set_marker(inst, entry, LAST_TS - (entry.cadence_days + 10) * SEC)
    assert inst._need_to_update(entry) is True
    inst.db_close()

#############################
# F. Adaptive cadence for fundamentals
#############################

@pytest.fixture
def inst_with_data(make_fmp):
    """FMP instance with income statement data present and a stale marker.

    The due base is the latest actual filing date from the fixture data;
    the marker is reset to that date - stale relative to the windows below
    (diff > fresh_days), as if the symbol was fetched at filing time.
    """
    inst = make_inst(make_fmp)
    # Load an annual-only batch via the private API: the due base must be deterministic
    # (quarterly fixture filings are newer than BASE_TS and would shift the maths)
    inst._add_income_statement(inst._fetch_fundamentals('income-statement', ReportPeriod.Year))
    set_marker(inst, IS, BASE_TS)
    yield inst
    inst.db_close()

def test_fundamentals_before_due_no_update(inst_with_data):
    # The next report is not due yet (last_ts_adj < base + cadence_days)
    inst_with_data.last_date = _datestr(DUE_TS - 2 * SEC)
    assert inst_with_data._need_to_update(IS) is False

def test_fundamentals_after_due_needs_update(inst_with_data):
    # The next report is due and the marker is older than fresh_days
    inst_with_data.last_date = _datestr(DUE_TS + 6 * SEC)
    assert inst_with_data._need_to_update(IS) is True

def test_fundamentals_after_due_throttled_by_fresh_days(inst_with_data):
    # Marker refreshed an hour ago: even past due, polls are throttled by fresh_days
    inst_with_data.last_date = _datestr(DUE_TS + 6 * SEC)
    set_marker(inst_with_data, IS, _ts(_datestr(DUE_TS + 6 * SEC)) - 3600)
    assert inst_with_data._need_to_update(IS) is False

#############################
# G. End-to-end via the public API
#############################

def test_income_statement_not_refetched_within_gap(make_fmp):
    inst, fake = make_fmp()
    inst.db_connect()

    inst.get_income_statement()
    calls_after_first = fake.count('income-statement')
    assert calls_after_first == 2  # annual + quarterly

    # Data present + fresh marker: no refetch
    inst.get_income_statement()
    assert fake.count('income-statement') == calls_after_first
    assert_recent(inst._get_modified_ts(IS.title))
    inst.db_close()

#############################
# H. Dropping intervals
#############################

def test_drop_symbol_intervals_returns_count(make_fmp):
    inst = make_inst(make_fmp)
    before = inst._get_data_num('data_intervals')
    assert before > 0

    assert inst.drop_symbol_intervals() == before
    assert inst._get_data_num('data_intervals') == 0
    assert inst._need_to_update(CommonDataEntries.SecurityInfo) is True
    inst.db_close()

def test_drop_symbol_intervals_empty_returns_zero(make_fmp):
    inst, _ = make_fmp(first_date=FIRST_DATE, last_date=LAST_DATE)
    inst.db_connect()
    assert inst.drop_symbol_intervals() == 0
    inst.db_close()

def test_drop_symbol_intervals_triggers_info_refetch(make_fmp):
    inst, fake = make_fmp()
    inst.db_connect()
    inst.get_info()
    assert fake.count('profile') == 1

    assert inst.drop_symbol_intervals() > 0
    assert inst._need_to_update(CommonDataEntries.SecurityInfo) is True

    inst.get_info()
    assert fake.count('profile') == 2
    inst.db_close()

def test_drop_symbol_intervals_quotes_need_update(make_fmp):
    inst = make_inst(make_fmp, first_date=Q_FIRST, last_date=Q_LAST)
    inst.get()
    assert inst._need_to_update() is False
    quotes_before = inst.get_quotes_num(dt=False)

    assert inst.drop_symbol_intervals() > 0
    assert inst._need_to_update() is True
    assert inst.get_quotes_num(dt=False) == quotes_before
    inst.db_close()

def test_drop_symbol_intervals_resets_stock_info_cache(make_fmp):
    """drop_* resets the in-memory _stock_info cache: sector is re-read from the DB
    (and stays present in the returned info) after the markers are dropped."""
    inst, _ = make_fmp()
    inst.db_connect()
    sector_before = inst.get_info()['sector']
    assert inst._stock_info is not None

    assert inst.drop_symbol_intervals() > 0
    assert inst._stock_info is None

    assert inst.get_info()['sector'] == sector_before
    inst.db_close()

def test_drop_datasource_intervals_count_and_refetch(make_fmp, shared_mem_db):
    """The drop spans all symbols of the source (not just the instance's symbol)."""
    inst, fake = make_fmp(db_name=shared_mem_db)
    inst.db_connect()
    inst.get_info()
    assert fake.count('profile') == 1

    # Second symbol, same DB and source: make_fmp re-patches _query_api, so the
    # inst's subsequent fetches go through this fake as well.
    other, other_fake = make_fmp(symbol='GGGG', db_name=shared_mem_db)
    other.db_connect()
    other.get_info()
    assert other_fake.count('profile') == 1

    mine = inst._get_data_num('data_intervals')
    total = inst._get_data_num('data_intervals', symbol=False)
    assert mine > 0
    assert total > mine  # the second symbol contributed its markers

    # == total would fail if the DELETE wrongly filtered by symbol_id
    assert inst.drop_datasource_intervals() == total

    assert inst._get_modified_ts(CommonDataEntries.SecurityInfo.title) is None
    assert inst._need_to_update(CommonDataEntries.SecurityInfo) is True
    assert other._need_to_update(CommonDataEntries.SecurityInfo) is True

    inst.get_info()
    assert other_fake.count('profile') == 2

    other.db_close()
    inst.db_close()
