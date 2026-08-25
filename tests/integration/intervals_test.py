"""YF data source intervals testing script.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from data import yf
from data.fvalues import Timespans, SecType, Currency, StockQuotes, def_last_date
from data.futils import get_dt

from datetime import datetime, timedelta
from dateutil import tz

from data import lg

import sys

# TODO MID This test should be able to use custom intervals for testing. Currently it is pre-defined.

def failure(text, source):
    """
        Print error message, disconnect db and exit.

        Args:
            text(sts): the error message to print.
            source(ReadOnlyData): data source instance
    """
    lg.error(text)
    source.db_close()
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

    lg.success("Info validation passed")

    #######################################################

    quotes_num = source.get_quotes_num()
    divs_num = source.get_dividends_num()
    splits_num = source.get_split_num()

    if quotes_num != 0:
        failure("There should be no quotes in the db at the beginning.", source)

    rows = source.get()

    min_req = source._get_min_request_ts()
    max_req = source._get_max_request_ts()

    print("\nSECTION1b: check if quotes, dividends and splits number increases")
    print("_________________________________________________________________")

    divs_after = source.get_dividends_num()
    splits_after = source.get_split_num()

    print(f"Divs before {divs_num}, divs after {divs_after}.")

    if divs_num >= divs_after:
        failure("Number of divs did not increase", source)

    print(f"Splits before {splits_num}, splits after {splits_after}.")

    if splits_num > splits_after:
        failure("Unexpected number of splits", source)

    after = source.get_quotes_num()

    print(f"Quotes before {quotes_num} quotes after {after}")

    if quotes_num >= after:
        failure("Number of quotes did not increase", source)

    lg.success("Quotes, splits and divs num increased as expected")

    print(f"\nSECTION2: Check initial request dates")
    print("______________________________________")

    print(f"Initial min/max request dates: {min_req}={get_dt(min_req)} {max_req}={get_dt(max_req)}")

    if min_req != 1580515200 or max_req != 1583020800:
        failure(f"Request timestamps are unexpected.", source)

    lg.success("Request timestamps are as expected")

    #######################################################

    print("\nSECTION3: Checking the intervals of received data including correct time zone adjustment.")
    print("Requested dates are: 2020-2-1 and 2020-3-1.")
    print("_________________________________________________________________________________________")

    date1 = rows[StockQuotes.DateTime][-1]
    date2 = rows[StockQuotes.DateTime][0]

    if date1 != '2020-02-28 23:59:59' or date2 != '2020-02-03 23:59:59':
        failure(f"Incorrect date ranges returned: {date1} {date2}", source)

    lg.success("Date ranges are as expected.")

    #######################################################

    source.first_date = "2018-1-1"
    source.last_date = "2019-1-1"

    source.get()

    print("\nSECTION4: Min request ts should decrease now")
    print("____________________________________________")

    new_min_req = source._get_min_request_ts()

    if new_min_req != 1514764800:
        failure(f"Error: {min_req} should not be less or equal than {new_min_req}", source)

    lg.success(f"Timestamp decreased as expected:: {min_req} > {new_min_req}")

    #######################################################

    source.first_date = "2022-1-1"
    source.last_date="2023-1-1"

    source.get()

    print("\nSECTION5: Max request should be bigger now")
    print("__________________________________________")

    new_max_req = source._get_max_request_ts()

    if new_max_req != 1672531200:
        failure(f"Error: {max_req} should not be bigger or equal than {new_max_req}", source)

    lg.success(f"Timestamp increased as expected:: {max_req} > {new_max_req}")

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

    lg.success(f"Timestamps are as expected: {new_min_req} {new_max_req}")

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

    lg.success(f"Final min/max request dates: {get_dt(source._get_min_request_ts())} {get_dt(source._get_max_request_ts())}")

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

        source._timespan = value

        quotes = source.get()

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

        lg.success(f"The delta seconds {delta.seconds} is expected")

        old_num = quotes_num
        max_minutes = max(max_minutes, key)

    print("\nSECTION9: Testing max request timespans for EOD quotes")
    print("______________________________________________________")

    source._timespan = Timespans.Day

    quotes = source.get()

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

    lg.success("The max request timestamp for EOD quotes is expected")

def test_earnings_history_intervals(i):
    """
        Test interval tracking for earnings history data via data_intervals.

        Verifies two things:
        - EH1: eh_max_ts is None before the first fetch.
        - EH2: the first get_earnings_history() fetches data (>0 entries) and
          records eh_max_ts in data_intervals.

        Args:
            i(YF): the data source instance (already db-connected and populated
                   with quotes, dividends and splits via the prior tests in
                   this file).
    """
    # Precondition for get_earnings_history(): quotes must be present in the DB.
    i.get()

    print("\nSECTION EH1: eh_max_ts is None before the first fetch")
    print("__________________________________________________________")

    ts_before = i._get_interval_ts(yf.YFDataEntries.EarningsHistory.title)

    if ts_before is not None:
        failure(f"eh_max_ts should be None before the first fetch, got {ts_before}", i)

    lg.success("eh_max_ts is None as expected.")

    #######################################################

    print("\nSECTION EH2: First get_earnings_history() fetches data and records the interval")
    print("____________________________________________________________________________________")

    num_before = i.get_earnings_history_num()

    fetched = i.get_earnings_history()

    if fetched <= 0:
        failure(f"First get_earnings_history() should fetch >0 entries, got {fetched}", i)

    ts_after = i._get_interval_ts(yf.YFDataEntries.EarningsHistory.title)

    if ts_after is None:
        failure("eh_max_ts should be set after the first fetch", i)

    lg.success(f"Fetched {fetched} entries. eh_max_ts recorded: {get_dt(ts_after)}")


if __name__ == "__main__":
    lg.highlight("\nTesting YF data source intervals:\n")

    yfi = yf.YF(symbol='IBM', first_date="2020-2-1", last_date="2020-3-1", verbosity=True, db_name=":memory:")
    yfi.db_connect()

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

    test_earnings_history_intervals(yfi)

    yfi.db_close()

    lg.success("ALL INTERVAL TESTS PASSED for YF data source!")
