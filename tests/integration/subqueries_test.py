"""Testing subqueries support for multiple data sources.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from data import yf, fmp
from data.fdata import Subquery
from data.futils import get_dt
from data.fvalues import def_last_date

from termcolor import colored

from datetime import datetime, timedelta
from dateutil import tz

import os
import sys
import tempfile

import calendar

import numpy as np

def failure(text):
    """
        Print error message and exit.

        Args:
            text(str): the error message to print.
    """
    print(colored(text, "red"))
    sys.exit()

def check_cap_values(rows, expected_ts, expected_cap, test_name):
    """
        Check the cap values obtained by a subquery against the expected ones.

        Rows before the first cap timestamp must be None and each cap range
        must contain the expected values (compare with Section 2 of the test).

        Args:
            rows: quotes with the cap subquery data.
            expected_ts(list): timestamps of the cap ranges.
            expected_cap(list): expected cap values for the ranges.
            test_name(str): subquery description to use in error messages.
    """
    # Check if rows where no data is expected (before the first cap
    # timestamp) are indeed None.
    first_ts = expected_ts[0]
    idx = np.where(rows['time_stamp'] < first_ts)[0]
    sub = rows[idx]

    if len(sub) > 0 and np.all(sub['cap'] == None) is False:
        failure(f"{test_name}: unexpected non-None values found before the first cap timestamp.")

    # Iterate through cap ranges and check if the values are expected.
    for i in range(len(expected_ts)):
        start_ts = expected_ts[i]

        if i == len(expected_ts) - 1:
            end_ts = sys.maxsize
        else:
            end_ts = expected_ts[i + 1]

        idx = np.where((rows['time_stamp'] >= start_ts) & (rows['time_stamp'] < end_ts))[0]
        sub = rows[idx]

        if len(sub) > 0 and np.all(sub['cap'] == expected_cap[i]) is False:
            failure(f"{test_name}: unexpected cap value at ts {get_dt(start_ts)}: expected {expected_cap[i]}")

def test_cross_source_subqueries():
    """
        Test that data from one data source (FMP) can be used with subqueries
        along with quotes from another data source (YF).

        A temporary database file is used and deleted afterwards.
    """
    print(colored("\nTesting subqueries support for multiple data sources:\n", "yellow"))

    fd, db_path = tempfile.mkstemp(suffix='.sqlite')
    os.close(fd)

    yfi = None
    fmpi = None

    try:
        # NOTE: The free FMP plan provides only the most recent capitalization
        # data (~3 months), so the date range must overlap the recent period.
        first_date = get_dt(datetime.now(tz.UTC)) - timedelta(days=120)
        last_date = def_last_date

        # Instance 1: FMP. Connect at first so that the source-specific tables
        # (like fmp_capitalization) exist in the database before the YF quotes
        # are fetched with the subqueries.
        fmpi = fmp.FMP(symbol='AAPL', first_date=first_date, last_date=last_date,
                       verbosity=True, db_name=db_path)
        fmpi.db_connect()

        print("SECTION 1: Fetching YF quotes with a cap subquery (no cap data yet)")
        print("___________________________________________________________________")

        # Instance 2: YF. A separate connection to the same database file
        # (both connections are open in parallel).
        yfi = yf.YF(symbol='AAPL', first_date=first_date, last_date=last_date,
                    verbosity=True, db_name=db_path)
        yfi.db_connect()

        if yfi.is_connected is False or fmpi.is_connected is False:
            failure("Both database connections should be open in parallel.")

        # The public get() accepts subqueries. Fetch the quotes and obtain the
        # cap subquery data in a single call. The cap data is not fetched yet,
        # so all cap values must be None.
        rows = yfi.get(queries=[Subquery(fmp.FMPDataEntries.Capitalization, 'cap', title='cap')])

        if rows is None or len(rows) == 0:
            failure(f"No quotes returned by get() for AAPL: {rows}")

        print(f"Fetched {len(rows)} quotes for AAPL.")

        if np.all(rows['cap'] == None) is False:
            failure("Cap values should be None before the cap data is fetched.")

        print(colored("Cap values are None before the cap data is fetched.", 'green'))

        print("\nSECTION 2: Fetching FMP cap data separately (no quotes fetched)")
        print("_______________________________________________________________")

        # Fetch the cap data. No quotes are fetched for the FMP instance -
        # the capitalization data is fetched separately.
        fetched = fmpi.get_cap()

        if fetched <= 0:
            failure(f"No cap entries were fetched by get_cap(): {fetched}")

        print(f"Fetched {fetched} cap entries for AAPL without fetching quotes.")

        print(colored("Cap data was fetched separately without quotes.", 'green'))

        print("\nSECTION 3: Checking the cap values obtained by the subqueries")
        print("_______________________________________________________________")

        # Obtain the ground truth data using the FMP API.
        expected_raw = fmpi._fetch_cap()

        if len(expected_raw) == 0:
            failure("No cap entries obtained from the FMP API.")

        timezone = fmpi._get_timezone()

        expected = []

        for entry in expected_raw:
            dt = get_dt(entry['date'], timezone).replace(hour=23, minute=59, second=59)
            expected.append((calendar.timegm(dt.utctimetuple()), entry['marketCap']))

        expected.sort()  # Handle the situation is eventually the API returned the data in unexpected order

        expected_ts = [entry[0] for entry in expected]
        expected_cap = [entry[1] for entry in expected]

        # Get the quotes again with the cap subquery. The cap data is now
        # present so the subquery must return the expected values.
        rows = yfi.get(queries=[Subquery(fmp.FMPDataEntries.Capitalization, 'cap', title='cap')])

        if rows is None or len(rows) == 0:
            failure(f"No quotes returned by get() for AAPL: {rows}")

        check_cap_values(rows, expected_ts, expected_cap, "cap subquery")

        print(colored("All subquery data is as expected.", 'green'))

        print("\nSECTION 4: Checking the cap values obtained with the symbol argument")
        print("_____________________________________________________________________")

        rows = yfi.get(queries=[Subquery(fmp.FMPDataEntries.Capitalization, 'cap', title='cap', symbol='AAPL')])

        if rows is None or len(rows) == 0:
            failure(f"No quotes returned by get() with the symbol argument for AAPL: {rows}")

        check_cap_values(rows, expected_ts, expected_cap, "symbol subquery")

        print(colored('All symbol-subquery data is as expected.', 'green'))
    finally:
        # Close the database connections and delete the temporary database
        # file including the WAL journal sidecar files (if any).
        for source in (fmpi, yfi):
            if source is not None and source.is_connected:
                source.db_close()

        for path in (db_path, db_path + '-wal', db_path + '-shm'):
            if os.path.exists(path):
                os.remove(path)

if __name__ == "__main__":
    print(colored("Testing subqueries support:", "yellow"))

    test_cross_source_subqueries()

    print(colored("ALL TESTS PASSED!", "green"))
