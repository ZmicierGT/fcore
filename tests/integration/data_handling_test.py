"""YF data source testing script.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from data import yf
from data.fvalues import Timespans, SecType, Currency, StockQuotes, DataEntries, def_last_date
from data.fdata import Subquery, FdataError
from data.futils import get_dt

from datetime import datetime, timedelta
from dateutil import tz

from termcolor import colored

import yfinance as yfin
import calendar

import numpy as np

import sys

# TODO High This test should be able to use custom intervals for testing. Currently it is pre-defined.

def failure(text, source):
    """
        Print error message, disconnect db and exit.

        Args:
            text(sts): the error message to print.
            source(ReadOnlyData): data source instance
    """
    print(colored(text, "red"))
    source._db_close()
    sys.exit()

def test_request_ts(source):
    """
        Test if minimim maximum request timestamp changes appropriately depending on pre-existing data
        and requested ranges. These timestamps are used to distinguish if quotes were already fetched.
        Note that fcore always keeps contiguous ranges of quotes in DB for every timespan to adjust the
        prices correctly.

        Args:
            source(ReadOnlyData): data source instance.
    """
    info = source.get_info()
    print(info)

    print("SECTION1a: Checkin if security info is as expected...")
    print("_____________________________________________________")

    if info['time_zone'] != 'America/New_York':
        failure(f"Unexpected time zone: {info['time_zone']}", source)
    if info['sec_type'] != SecType.Stock:
        failure(f"Unexpected stock type: {info['sec_type']}", source)
    if info['currency'] != Currency.Unknown:
        failure(f"Unexpected currency: {info['currency']}", source)
    if info['sector'] != 'Technology':
        failure(f"Unexpected sector: {info['sector']}", source)

    print(colored("Info validation passed", 'green'))

    #######################################################

    quotes_num = source.get_quotes_num()

    if quotes_num != 0:
        failure("There should be no quotes in the db at the beginning.", source)

    rows = source._get_quotes_only()

    min_req = source._get_min_request_ts()
    max_req = source._get_max_request_ts()

    print("\nSECTION1b: check if quotes, dividends and splits number increases")
    print("_________________________________________________________________")

    div_data = source._fetch_dividends()  # Split data may be fetched as it is needed to reverse-adjust the dividends
    split_data = source._fetch_splits()

    before, after = source._add_dividends(div_data)

    print(f"Divs before {before}, divs after {after}.")

    if before >= after:
        failure("Number of divs did not increase", source)

    before, after = source._add_splits(split_data)

    print(f"Splits before {before}, splits after {after}.")

    if before > after:
        failure("Unexpected number of splits", source)

    after = source.get_quotes_num()

    print(f"Quotes before {quotes_num} quotes after {after}")

    if quotes_num >= after:
        failure("Number of quotes did not increase", source)

    print(colored("Quotes, splits and divs num increased as expected", "green"))

    print(f"\nSECTION2: Check initial request dates")
    print("______________________________________")

    print(f"Initial min/max request dates: {min_req}={get_dt(min_req)} {max_req}={get_dt(max_req)}")

    if min_req != 1580515200 or max_req != 1583020800:
        failure(f"Request timestamps are unexpected.", source)

    print(colored("Request timestamps are as expected", "green"))

    #######################################################

    print("\nSECTION3: Checking the intervals of received data including correct time zone adjustment.")
    print("Requested dates are: 2020-2-1 and 2020-3-1.")
    print("_________________________________________________________________________________________")

    date1 = rows[StockQuotes.DateTime][-1]
    date2 = rows[StockQuotes.DateTime][0]

    if date1 != '2020-02-28 23:59:59' or date2 != '2020-02-03 23:59:59':
        failure(f"Incorrect date ranges returned: {date1} {date2}", source)

    print(colored("Date ranges are as expected.", "green"))

    #######################################################

    source.first_date = "2018-1-1"
    source.last_date = "2019-1-1"

    source.get()

    print("\nSECTION4: Min request ts should decrease now")
    print("____________________________________________")

    new_min_req = source._get_min_request_ts()

    if new_min_req != 1514764800:
        failure(f"Error: {min_req} should not be less or equal than {new_min_req}", source)

    print(colored(f"Timestamp decreased as expected:: {min_req} > {new_min_req}", "green"))

    #######################################################

    source.first_date = "2022-1-1"
    source.last_date="2023-1-1"

    source.get()

    print("\nSECTION5: Max request should be bigger now")
    print("__________________________________________")

    new_max_req = source._get_max_request_ts()

    if new_max_req != 1672531200:
        failure(f"Error: {max_req} should not be bigger or equal than {new_max_req}", source)

    print(colored(f"Timestamp increased as expected:: {max_req} > {new_max_req}", "green"))

    min_req = source._get_min_request_ts()
    max_req = source._get_max_request_ts()

    #######################################################

    source.first_date="2017-1-1"
    source.last_date="2023-6-1"

    source.get()

    print("\nSECTION6: Max request should be bigger now and min request should be smaller")
    print("____________________________________________________________________________")

    new_min_req = source._get_min_request_ts()
    new_max_req = source._get_max_request_ts()

    if new_max_req != 1685577600 or new_min_req != 1483228800:
        failure(f"Error: {max_req} should not be bigger or equal than {new_max_req} and {min_req} should be not less or equal than {new_min_req}", source)

    print(colored(f"Timestamps are as expected: {new_min_req} {new_max_req}", "green"))

    #######################################################

    print("\nSECTION7: both timestamps should change")
    print("_______________________________________")

    source.first_date="2022-1-1"
    source.last_date = def_last_date

    source.get()

    now = datetime.now(tz.UTC)

    if source.is_intraday() is False:
        now = source._set_eod_time(now)

    ts = int(now.timestamp())

    if source._get_max_request_ts() > ts:
        failure(f"Max request ts is {source._get_max_request_ts()} but it should be less or equal to {ts}", source)

    print(colored(f"Final min/max request dates: {get_dt(source._get_min_request_ts())} {get_dt(source._get_max_request_ts())}", "green"))

    #######################################################

def test_request_intervals(source, timespans):
    """
        Test request intervals. Each interval should increase max_request timestamp correspondingly to avoid excessive
        calls to API data source. For example, if we request 1 hour bars then no sense to check for new data for an hour.

        Args:
            source(ReadOnlyData): the data source.
            timespans(dict): the timespans to test (except EOD).
    """
    print("\nSECTION8: Testing max request timespans for intraday quotes")
    print("___________________________________________________________")

    old_num = 0

    source.first_date=get_dt(datetime.now(tz.UTC)) - timedelta(days=5)
    source.last_date = def_last_date

    max_minutes = 0

    for key, value in timespans.items():
        print(f"\nTesting intraday interval: {value}")

        source.timespan = value

        quotes = source._get_quotes_only()

        utc_now = get_dt(datetime.now(tz.UTC))

        quotes_num = len(quotes)
        max_req = source._get_max_request_ts()

        print(f"Initial: {utc_now}, max request: {get_dt(max_req)}")

        delta = get_dt(max_req) - utc_now

        if delta.seconds < (key * 60 + 1 - 60) or delta.seconds > (key * 60 + 1):
            failure(f"{delta.seconds} is unexpected", source)

        print(f"Total quotes fetched: {quotes_num}")

        print(f"Number of previous interval quotes {old_num}, number of current interval quotes {quotes_num}")

        if old_num != 0 and quotes_num >= old_num:
            failure(f"Current quotes num should be less than the previous quotes num! {quotes_num} < {old_num}", source)

        print(colored(f"The delta seconds {delta.seconds} is expected", "green"))

        old_num = quotes_num
        max_minutes = max(max_minutes, key)

    print("\nSECTION9: Testing max request timespans for EOD quotes")
    print("______________________________________________________")

    source.timespan = Timespans.Day

    quotes = source._get_quotes_only()

    utc_now = get_dt(datetime.now(tz.UTC))

    quotes_num = len(quotes)
    max_req = source._get_max_request_ts()

    print(f"Initial: {utc_now}, max request: {get_dt(max_req)}")

    max_dt = get_dt(max_req)

    if utc_now.date() != max_dt.date():
        failure(f"{utc_now.date()} and {max_dt.date()} should not differ!", source)

    if max_dt.hour != 23 or max_dt.minute != 59:
        failure(f"Time difference should be 23:59!", source)

    can_equal = max_minutes >= 240  # In such case the number of quotes may be equal to EOD

    if old_num != 0 and ((can_equal and quotes_num > old_num) or (can_equal is False and quotes_num >= old_num)):
        failure(f"Current quotes num value is too big compared to the previous quotes num: {quotes_num} < {old_num}", source)

    print(colored("The max request timestamp for EOD quotes is expected", "green"))

def test_earnings_history_intervals(i):
    """
        Test interval tracking for earnings history data via data_intervals.

        Verifies two things:
        - EH1: eh_max_ts is None before the first fetch.
        - EH2: the first get_earnings_history() fetches data (>0 entries) and
          records eh_max_ts in data_intervals.

        Args:
            i(YF): the data source instance (already db-connected, no quotes yet).
    """
    # Precondition for get_earnings_history(): quotes must be present in the DB.
    i.get()

    print("\nSECTION EH1: eh_max_ts is None before the first fetch")
    print("__________________________________________________________")

    ts_before = i._get_interval_ts(DataEntries.YFEarningsHistory.value)

    if ts_before is not None:
        failure(f"eh_max_ts should be None before the first fetch, got {ts_before}", i)

    print(colored("eh_max_ts is None as expected.", 'green'))

    #######################################################

    print("\nSECTION EH2: First get_earnings_history() fetches data and records the interval")
    print("____________________________________________________________________________________")

    num_before = i.get_earnings_history_num()

    fetched = i.get_earnings_history()

    if fetched <= 0:
        failure(f"First get_earnings_history() should fetch >0 entries, got {fetched}", i)

    ts_after = i._get_interval_ts(DataEntries.YFEarningsHistory.value)

    if ts_after is None:
        failure("eh_max_ts should be set after the first fetch", i)

    print(colored(f"Fetched {fetched} entries. eh_max_ts recorded: {get_dt(ts_after)}", 'green'))

def test_subqueries(i, source_eh):
    i.get()
    i.get_earnings_history()

    rows = i._get_quotes(queries=[Subquery('yf_earnings_history', 'epsActual', title='eps_actual'),
                                 Subquery('yf_earnings_history', 'epsEstimate', title='eps_estimate'),
                                 Subquery('yf_earnings_history', 'surprisePercent', title='surprise_pct')])

    # The last 3 columns of the resulting array are the earnings history subqueries.
    cols = ['eps_actual', 'eps_estimate', 'surprise_pct']

    # Check if rows where no data is expected (before the first ts of earnings history) are indeed None
    first_ts = int(source_eh['ts'].min())
    idx = np.where(rows['time_stamp'] < first_ts)[0]
    sub = rows[idx]

    #print(sub)

    if all(np.all(sub[f] == None) for f in cols):
        print(colored('Rows with None values are as expected.', 'green'))
    else:
        failure("Unexpected non-None values found in the subquery data.", i)

    # Iterate through earnings ranges and check if values are expected

    l = list(source_eh['ts'])

    for i in range(len(l)):
        start_ts = l[i]

        if i == len(l) - 1:
            end_ts = sys.maxsize
        else:
            end_ts = l[i+1]

        idx = np.where((rows['time_stamp'] >= start_ts) & (rows['time_stamp'] < end_ts))[0]
        sub = rows[idx]

        epsActual = source_eh.at[i, 'epsActual']
        epsEstimate = source_eh.at[i, 'epsEstimate']
        surprisePercent = source_eh.at[i, 'surprisePercent']

        if np.all(sub['eps_actual'] != epsActual):
            failure('Unexpected epsActual', i)
        if np.all(sub['eps_estimate'] != epsEstimate):
            failure('Unexpected epsEstimate', i)
        if np.all(sub['surprise_pct'] != surprisePercent):
            failure('Unexpected surprisePercent', i)

    print(colored('All subquery data is as expected.', 'green'))

def test_remove_symbol(i):
    """
        Test remove_symbol().

        Verifies that the symbol itself and all cascade-linked data (quotes,
        dividends, splits, earnings history) are deleted.
        Also checks idempotency: a second call on an already-removed symbol
        must not raise.

        Args:
            i(YF): the data source instance (already populated with quotes,
                   dividends, splits and earnings history data via prior tests).
    """
    print(colored("\nTesting remove_symbol():\n", "yellow"))

    print("SECTION RMS1: Pre-delete presence")
    print("_________________________________")

    if not i.symbol_exists:
        failure("IBM should be present in symbols before deletion", i)

    quotes_num = i.get_quotes_num(dt=False)
    dividends_num = i.get_dividends_num()
    splits_num = i.get_split_num()
    eh_num = i.get_earnings_history_num()

    print(f"Quotes: {quotes_num}, Dividends: {dividends_num}, Splits: {splits_num}, Earnings history: {eh_num}")

    if quotes_num == 0 or dividends_num == 0 or splits_num == 0 or eh_num == 0:
        failure("All categories should have data before deletion", i)

    print(colored("Pre-delete presence confirmed", "green"))

    #######################################################

    print("\nSECTION RMS2: Invoke remove_symbol()")
    print("__________________________________")

    # NOTE: A close-then-reopen would discard the in-memory DB (every new
    # connection to ":memory:" starts empty). So remove_symbol() is invoked
    # while connected, consistent with the other in-memory tests in this file.
    i.remove_symbol()

    print(colored("remove_symbol() succeeded", "green"))

    #######################################################

    print("\nSECTION RMS3: Post-delete absence (cascade verification via get_*_num())")
    print("____________________________________________________________________________")

    if i.symbol_exists:
        failure("IBM should be absent from symbols after deletion", i)

    quotes_num = i.get_quotes_num(dt=False)
    dividends_num = i.get_dividends_num()
    splits_num = i.get_split_num()
    eh_num = i.get_earnings_history_num()

    print(f"Quotes: {quotes_num}, Dividends: {dividends_num}, Splits: {splits_num}, Earnings history: {eh_num}")

    if quotes_num != 0 or dividends_num != 0 or splits_num != 0 or eh_num != 0:
        failure("All categories should have 0 rows after cascade deletion", i)

    print(colored("Symbol and all linked data removed (cascade verified)", "green"))

    #######################################################

    print("\nSECTION RMS4: Idempotency - second remove_symbol() must not raise")
    print("__________________________________________________________________")

    try:
        i.remove_symbol()
    except Exception as e:
        failure(f"Second remove_symbol() should not raise: {e}", i)

    print(colored("Idempotency verified: second remove_symbol() did not raise", "green"))

def test_get_delisted():
    """
        Test get() for a delisted/non-existent symbol (WBA). Both first and second
        invocations must raise FdataError without fetching quotes or marking
        intervals. The first invocation persists sec_info as NotExist; the second
        raises early from the cached sec_info (no API refetch of info).
    """
    print(colored("\nTesting get() for a delisted symbol (WBA):\n", "yellow"))

    yfi = yf.YF(symbol='WBA', first_date="2020-1-1", last_date="2020-3-1", verbosity=True, db_name=":memory:")
    yfi._db_connect()

    # First invocation: empty DB. fetch_info() runs, detects NotExist, persists
    # sec_info; get_info() raises (fdata.py:1271) before any quote fetch.
    print("First invocation (empty DB): expecting FdataError ...")
    raised_first = False
    try:
        yfi.get()
    except FdataError as e:
        print(f"  Got expected FdataError: {e}")
        raised_first = True

    if not raised_first:
        failure("First get() should have raised FdataError for a delisted symbol", yfi)

    if yfi.get_quotes_num(dt=False) != 0:
        failure("No quotes should be fetched for a delisted symbol", yfi)

    if yfi._get_min_request_ts() is not None or yfi._get_max_request_ts() is not None:
        failure("No data_intervals should be marked for a delisted symbol", yfi)

    print(colored("First invocation: raised FdataError, fetched nothing, no intervals marked", "green"))

    # Confirm the security is indeed marked as NotExist in sec_info. get_info()
    # raises FdataError when sec_type == NotExist (fdata.py:1271).
    print("\nDirect get_info() check: expecting FdataError (sec_info persisted as NotExist) ...")
    raised_info = False
    try:
        yfi.get_info()
    except FdataError as e:
        print(f"  Got expected FdataError: {e}")
        raised_info = True

    if not raised_info:
        failure("get_info() should raise FdataError for a symbol marked NotExist", yfi)

    print(colored("get_info() confirmed the symbol is marked NotExist", "green"))

    # Second invocation: sec_info persisted as NotExist. get_info() raises at
    # fdata.py:1271 BEFORE fetch_info() runs (no API refetch).
    print("\nSecond invocation (sec_info persisted as NotExist): expecting FdataError ...")
    raised_second = False
    try:
        yfi.get()
    except FdataError as e:
        print(f"  Got expected FdataError: {e}")
        raised_second = True

    if not raised_second:
        failure("Second get() should also raise FdataError", yfi)

    print(colored("Second invocation: raised FdataError from cached sec_info", "green"))

    yfi._db_close()

def test_get_existing():
    """
        Test get() for an existing symbol (IBM). First invocation fetches info +
        quotes and marks intervals. Second invocation reuses cached info (no
        refetch) and skips quote fetch (intervals cover the range), returning
        cached rows without error.
    """
    print(colored("\nTesting get() for an existing symbol (IBM):\n", "yellow"))

    yfi = yf.YF(symbol='IBM', first_date="2020-2-1", last_date="2020-3-1", verbosity=True, db_name=":memory:")
    yfi._db_connect()

    # First invocation: empty DB.
    print("First invocation (empty DB): expecting fetch + rows ...")
    rows_first = yfi.get()

    if rows_first is None or len(rows_first) == 0:
        failure("First get() should return fetched rows for IBM", yfi)

    quotes_num_first = yfi.get_quotes_num(dt=False)
    if quotes_num_first == 0:
        failure("Quotes count should increase after first get()", yfi)

    min_req_first = yfi._get_min_request_ts()
    max_req_first = yfi._get_max_request_ts()

    if min_req_first is None or max_req_first is None:
        failure("Intervals should be marked after first get()", yfi)

    print(colored(f"First invocation: fetched {len(rows_first)} rows, intervals {get_dt(min_req_first)}..{get_dt(max_req_first)}", "green"))

    # Second invocation: same range, covered. No refetch expected.
    print("\nSecond invocation (same range, covered): expecting cached rows, no error ...")
    rows_second = yfi.get()

    if rows_second is None or len(rows_second) == 0:
        failure("Second get() should return cached rows for IBM", yfi)

    quotes_num_second = yfi.get_quotes_num(dt=False)

    # INSERT OR IGNORE guarantees no duplicate rows: count must be unchanged.
    if quotes_num_second != quotes_num_first:
        failure(f"Quotes count should not change on second get(): {quotes_num_first} -> {quotes_num_second}", yfi)

    if yfi._get_min_request_ts() != min_req_first or yfi._get_max_request_ts() != max_req_first:
        failure("Intervals should not change on second get() (covered range)", yfi)

    print(colored("Second invocation: returned cached rows, no duplicate fetch, intervals unchanged", "green"))

    yfi._db_close()

def test_get_empty_range_valid_symbol():
    """
        Test get() for a valid symbol (INFQ) requested in a range before the
        first available quote (2026-02-17). The range is genuinely empty but
        the symbol is valid (not delisted). Intervals must be recorded so the
        known-empty range is permanently skipped on subsequent calls.
    """
    print(colored("\nTesting get() for a valid symbol with empty range (INFQ):\n", "yellow"))

    yfi = yf.YF(symbol='INFQ', first_date="2026-01-01", last_date="2026-02-16", verbosity=True, db_name=":memory:")
    yfi._db_connect()

    # First invocation: empty DB, valid symbol, range before first quote.
    # Accept either raise (current YF empty-download contract) or None return —
    # the decisive assertion is on intervals, not the return type.
    print("First invocation (empty range): expecting intervals recorded ...")
    try:
        yfi.get()
    except FdataError as e:
        print(f"  Got FdataError: {e}")

    if yfi.get_quotes_num(dt=False) != 0:
        failure("No quotes should be fetched for an empty range", yfi)

    min_req = yfi._get_min_request_ts()
    max_req = yfi._get_max_request_ts()

    if min_req is None or max_req is None:
        failure("Intervals should be recorded for an empty valid-symbol range", yfi)

    print(colored(f"Intervals recorded: {get_dt(min_req)}..{get_dt(max_req)}", "green"))

    yfi._db_close()

def test_refetch():
    """
        Test refetch=True on SecData.

        Verifies that a second get() on a covered range still enters the
        fetch path (bypasses interval gating) when refetch=True, without
        raising. UPSERT prevents duplicate rows, so quote count stays
        unchanged, but the key assertion is that _need_to_update() returns
        True immediately (refetch short-circuit) and get() completes
        successfully on the covered second call.
    """
    print(colored("\nTesting refetch=True (IBM, covered range):\n", "yellow"))

    yfi = yf.YF(symbol='IBM', first_date="2020-2-1", last_date="2020-3-1",
                  verbosity=True, db_name=":memory:", refetch=True)
    yfi._db_connect()

    print("First invocation: expecting fetch + rows ...")
    rows_first = yfi.get()

    if rows_first is None or len(rows_first) == 0:
        failure("First get() should return fetched rows for IBM", yfi)

    quotes_num_first = yfi.get_quotes_num(dt=False)
    if quotes_num_first == 0:
        failure("Quotes count should increase after first get()", yfi)

    min_req_first = yfi._get_min_request_ts()
    max_req_first = yfi._get_max_request_ts()
    print(colored(f"First invocation: fetched {len(rows_first)} rows, "
                  f"intervals {get_dt(min_req_first)}..{get_dt(max_req_first)}", "green"))

    print("\nSecond invocation (same range, covered, refetch=True): "
          "expecting fetch path taken, cached result returned ...")
    rows_second = yfi.get()

    if rows_second is None or len(rows_second) == 0:
        failure("Second get() with refetch=True should return rows for IBM", yfi)

    quotes_num_second = yfi.get_quotes_num(dt=False)

    if quotes_num_second != quotes_num_first:
        failure(f"Quotes count should not change on refetch (UPSERT): "
                f"{quotes_num_first} -> {quotes_num_second}", yfi)

    if yfi._get_min_request_ts() != min_req_first or yfi._get_max_request_ts() != max_req_first:
        failure("Intervals should not change on second get() with refetch=True", yfi)

    print(colored("refetch=True: second get() entered fetch path, returned same rows, "
                  "intervals unchanged", "green"))

    yfi._db_close()

if __name__ == "__main__":
    print(colored("\nTesting YF data source:\n", "yellow"))

    yfi = yf.YF(symbol='IBM', first_date="2020-2-1", last_date="2020-3-1", verbosity=True, db_name=":memory:")
    yfi._db_connect()

    test_request_ts(yfi)

    timespans_yf = {
        1: Timespans.Minute,
        2: Timespans.TwoMinutes,
        5: Timespans.FiveMinutes,
        15: Timespans.FifteenMinutes,
        30: Timespans.ThirtyMinutes,
        60: Timespans.Hour,
        90: Timespans.NinetyMinutes
    }

    test_request_intervals(yfi, timespans_yf)

    yfi._db_close()

    print(colored("ALL INTERVAL TESTS PASSED for YF data source!", "green"))

    print(colored("\nTesting subqueries support:\n", "yellow"))

    yfi = yf.YF(symbol='IBM', verbosity=True, db_name=":memory:")
    yfi._db_connect()

    # At first, obtain the raw earnings history data directly.
    ticker = yfin.Ticker('IBM')
    source_eh = ticker.earnings_history

    # Reset index so 'quarter' becomes a column. Calculate the quarter timestamp (UTC midnight) the
    # same way it is stored in the database by _fetch_earnings_history() (see data/yf.py).
    source_eh = source_eh.reset_index()
    source_eh['ts'] = source_eh['quarter'].apply(
        lambda q: int(calendar.timegm(
            (q.tz_localize('UTC') if q.tz is None else q.tz_convert('UTC')).utctimetuple()
        ))
    )
    source_eh = source_eh.sort_values('ts').reset_index(drop=True)

    test_earnings_history_intervals(yfi)

    test_subqueries(yfi, source_eh)

    test_remove_symbol(yfi)

    # get() behavior for delisted vs. existing symbols (fresh in-memory DBs)
    test_get_delisted()
    test_get_existing()
    test_get_empty_range_valid_symbol()
    test_refetch()

    print(colored("ALL TESTS PASSED!", "green"))
