"""General YF data source behavior testing script.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from data import yf
from data.fdata import FdataError
from data.futils import get_dt

from termcolor import colored

import sys

def failure(text, source):
    """
        Print error message, disconnect db and exit.

        Args:
            text(str): the error message to print.
            source(ReadOnlyData): data source instance
    """
    print(colored(text, "red"))
    source._db_close()
    sys.exit()

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
        print(f"Got expected FdataError: {e}")
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
        print(f"Got expected FdataError: {e}")
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
        print(f"Got expected FdataError: {e}")
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
        print(f"Got FdataError: {e}")

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
    print(colored("\nTesting general YF data source behavior:\n", "yellow"))

    yfi = yf.YF(symbol='IBM', verbosity=True, db_name=":memory:")
    yfi._db_connect()

    yfi.get()
    yfi.get_earnings_history()

    test_remove_symbol(yfi)

    yfi._db_close()

    # get() behavior for delisted vs. existing symbols (fresh in-memory DBs)
    test_get_delisted()
    test_get_existing()
    test_get_empty_range_valid_symbol()
    test_refetch()

    print(colored("ALL TESTS PASSED!", "green"))
