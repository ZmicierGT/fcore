"""YF data source testing script.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from data import yf, fmp
from data.fvalues import Timespans, SecType, Currency, StockQuotes, def_last_date
from data.futils import get_dt

from datetime import datetime, timedelta
from dateutil import tz

from termcolor import colored

import sys

def failure(text, source):
    """
        Print error message, disconnect db and exit.

        Args:
            text(sts): the error message to print.
            source(ReadOnlyData): data source instance
    """
    print(colored(text, "red"))
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

    quotes_num = source.get_symbol_quotes_num()

    if quotes_num != 0:
        failure("There should be no quotes in the db at the beginning.", source)

    rows = source.get_quotes_only()

    min_req = source.get_min_request_ts()
    max_req = source.get_max_request_ts()

    print("\nSECTION1b: check if quotes, dividends and splits number increases")
    print("_________________________________________________________________")

    div_data = source.fetch_dividends()  # Split data may be fetched as it is needed to reverse-adjust the dividends
    split_data = source.fetch_splits()

    before, after = source.add_dividends(div_data)

    print(f"Divs before {before}, divs after {after}.")

    if before >= after:
        failure("Number of divs did not increase", source)

    before, after = source.add_splits(split_data)

    print(f"Splits before {before}, splits after {after}.")

    if before > after:
        failure("Unexpected number of splits", source)

    after = source.get_symbol_quotes_num()

    print(f"Quotes before {quotes_num} quotes after {after}")

    if before >= after:
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
    source.laast_date = "2019-1-1"

    source.get()

    print("\nSECTION4: Min request ts should decrease now")
    print("____________________________________________")

    new_min_req = source.get_min_request_ts()

    if new_min_req != 1514764800:
        failure(f"Error: {min_req} should not be less or equal than {new_min_req}", source)

    print(colored(f"Timestamp decreased as expected:: {min_req} > {new_min_req}", "green"))

    #######################################################

    source.first_date = "2022-1-1"
    source.last_date="2023-1-1"

    source.get()

    print("\nSECTION5: Max request should be bigger now")
    print("__________________________________________")

    new_max_req = source.get_max_request_ts()

    if new_max_req != 1672531200:
        failure(f"Error: {max_req} should not be bigger or equal than {new_max_req}", source)

    print(colored(f"Timestamp increased as expected:: {max_req} > {new_max_req}", "green"))

    min_req = source.get_min_request_ts()
    max_req = source.get_max_request_ts()

    #######################################################

    source.first_date="2017-1-1"
    source.last_date="2023-6-1"

    source.get()

    print("\nSECTION6: Max request should be bigger now and min request should be smaller")
    print("____________________________________________________________________________")

    new_min_req = source.get_min_request_ts()
    new_max_req = source.get_max_request_ts()

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
        now = source.set_eod_time(now)

    ts = int(now.timestamp())

    if source.get_max_request_ts() > ts:
        failure(f"Max request ts is {source.get_max_request_ts()} but it should be less or equal to {ts}", source)

    print(colored(f"Final min/max request dates: {get_dt(source.get_min_request_ts())} {get_dt(source.get_max_request_ts())}", "green"))

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

        quotes = source.get_quotes_only()

        utc_now = get_dt(datetime.now(tz.UTC))

        quotes_num = len(quotes)
        max_req = source.get_max_request_ts()

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

    quotes = source.get_quotes_only()

    utc_now = get_dt(datetime.now(tz.UTC))

    quotes_num = len(quotes)
    max_req = source.get_max_request_ts()

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

if __name__ == "__main__":
    print(colored("\nTesting YF data source:\n", "yellow"))

    yfi = yf.YF(symbol='IBM', first_date="2020-2-1", last_date="2020-3-1", verbosity=True)
    yfi.db_name = ":memory:"
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

    yfi.db_close()

    print(colored("ALL TESTS PASSED for YF data source!", "green"))

    #################################################################

    print(colored("\nTesting FMP data source:\n", "yellow"))

    fmpi = fmp.FmpStock(symbol='IBM', first_date="2020-2-1", last_date="2020-3-1", verbosity=True)
    fmpi.db_name = ":memory:"
    fmpi.db_connect()

    test_request_ts(fmpi)

    timespans_fmp = {
        1: Timespans.Minute,
        5: Timespans.FiveMinutes,
        15: Timespans.FifteenMinutes,
        30: Timespans.ThirtyMinutes,
        60: Timespans.Hour,
        240: Timespans.FourHour
    }

    test_request_intervals(fmpi, timespans_fmp)

    fmpi.db_close()

    print(colored("ALL TESTS PASSED for FMP data source!", "green"))

    print(colored("ALL TESTS PASSED for all data sources!", "green"))
