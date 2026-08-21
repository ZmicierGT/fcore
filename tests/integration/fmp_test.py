"""FMP data source endpoints testing script.

Each test verifies that the number of entries in the database has increased after the corresponding fetch is performed.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from data import fmp
from data.fdata import FdataError, Subquery
from data.futils import get_dt
from data.fvalues import def_last_date
from data.stock import report_quarter, report_year

from data import lg

from datetime import datetime, timedelta
from dateutil import tz

import sys

import numpy as np

def failure(text, source):
    """
        Print error message, disconnect db and exit.

        Args:
            text(str): the error message to print.
            source(ReadOnlyData): data source instance
    """
    lg.error(text)
    source.db_close()
    sys.exit()

def test_info(source):
    """
        Test the profile/info endpoint.

        Args:
            source(FMP): FMP data source instance.
    """
    print("SECTION1: Testing security info (profile endpoint)...")
    print("_____________________________________________________")

    num_before = source._get_data_num('sec_info')

    info = source.get_info()

    num_after = source._get_data_num('sec_info')

    if num_after <= num_before:
        failure("The number of sec_info entries did not increase after get_info() call.", source)
    if 'time_zone' not in info or 'sec_type' not in info or 'currency' not in info:
        failure(f"Unexpected info obtained: {info}", source)

    lg.plain(f"Info obtained: {info}")
    lg.success("Info endpoint passed")

def test_quotes_dividends_splits(source):
    """
        Test EOD quotes, cash dividends and stock splits endpoints (all are triggered by get()).

        Args:
            source(FMP): FMP data source instance.
    """
    print("\nSECTION2: Testing EOD quotes, dividends and splits endpoints (get())...")
    print("______________________________________________________________________")

    quotes_before = source.get_quotes_num()
    divs_before = source.get_dividends_num()
    splits_before = source.get_split_num()

    quotes = source.get()

    quotes_after = source.get_quotes_num()
    divs_after = source.get_dividends_num()
    splits_after = source.get_split_num()

    if quotes_after <= quotes_before:
        failure(f"The number of quotes did not increase after get() call: {quotes_before} -> {quotes_after}", source)
    if divs_after <= divs_before:
        failure(f"The number of dividends did not increase after get() call: {divs_before} -> {divs_after}", source)
    if splits_after <= splits_before:
        failure(f"The number of splits did not increase after get() call: {splits_before} -> {splits_after}", source)
    if len(quotes) == 0:
        failure("No quotes returned by get().", source)

    lg.plain(f"Quotes obtained: {quotes_after} (before: {quotes_before})")
    lg.plain(f"Dividends obtained: {divs_after} (before: {divs_before})")
    lg.plain(f"Splits obtained: {splits_after} (before: {splits_before})")
    lg.success("Quotes/dividends/splits endpoints passed")

def test_cap(source):
    """
        Test the market capitalization endpoint.

        Args:
            source(FMP): FMP data source instance.
    """
    print("\nSECTION3: Testing market capitalization endpoint (get_cap())...")
    print("_______________________________________________________________")

    num_before = source.get_cap_num()

    source.get_cap()

    num_after = source.get_cap_num()

    if num_after <= num_before:
        failure(f"The number of capitalization entries did not increase after get_cap() call: {num_before} -> {num_after}", source)

    lg.plain(f"Capitalization entries obtained: {num_after} (before: {num_before})")
    lg.success("Capitalization endpoint passed")

def test_income_statement(source):
    """
        Test the income statement endpoint.

        Args:
            source(FMP): FMP data source instance.
    """
    print("\nSECTION4: Testing income statement endpoint (get_income_statement())...")
    print("______________________________________________________________________")

    num_before = source.get_income_statement_num()

    source.get_income_statement()

    num_after = source.get_income_statement_num()

    if num_after <= num_before:
        failure(f"The number of income statement entries did not increase after get_income_statement() call: {num_before} -> {num_after}", source)

    # Verify both annual and quarterly reports were fetched and stored by
    # joining the fundamentals table to the cached quotes via subqueries. The
    # report_quarter / report_year helper conditions filter the same table
    # (aliased as 'report_tbl' by Subquery.generate()) to the matching period.
    quarter_subquery = Subquery(fmp.FMPDataEntries.IncomeStatement, 'revenue',
                                condition=report_quarter, title='revenue_quarter')
    annual_subquery = Subquery(fmp.FMPDataEntries.IncomeStatement, 'revenue',
                               condition=report_year, title='revenue_annual')

    rows = source.get(queries=[quarter_subquery, annual_subquery])

    if rows is None or len(rows) == 0:
        failure("No quotes returned by get() with the income statement subqueries.", source)

    if 'revenue_quarter' not in rows.dtype.names or 'revenue_annual' not in rows.dtype.names:
        failure(f"Expected revenue_quarter/revenue_annual columns in subquery result: {rows.dtype.names}", source)

    quarter_mask = rows['revenue_quarter'] != None
    annual_mask = rows['revenue_annual'] != None
    quarter_values = rows['revenue_quarter'][quarter_mask]
    annual_values = rows['revenue_annual'][annual_mask]

    if len(quarter_values) == 0:
        failure("No quarterly revenue values obtained via the report_quarter subquery.", source)
    if len(annual_values) == 0:
        failure("No annual revenue values obtained via the report_year subquery.", source)

    # Both periods returned non-None on the same quote rows: they must differ
    # somewhere (quarterly revenue != full-year revenue for the same report date).
    both_mask = quarter_mask & annual_mask
    if not np.any(rows['revenue_quarter'][both_mask] != rows['revenue_annual'][both_mask]):
        failure("Quarterly and annual revenue values are identical; conditions did not filter to different periods.", source)

    # quarter_num + annual_num == total: each distinct revenue value corresponds to one report
    # (AAPL revenues are distinct across reports), so the number of distinct
    # values per period equals the number of reports of that period.
    quarter_num = len(np.unique(quarter_values))
    annual_num = len(np.unique(annual_values))

    if quarter_num == 0:
        failure("No distinct quarterly revenue values found.", source)
    if annual_num == 0:
        failure("No distinct annual revenue values found.", source)
    if quarter_num + annual_num > num_after:
        failure(f"Quarter({quarter_num}) + Annual({annual_num}) > total({num_after}) in {fmp.FMPDataEntries.IncomeStatement}", source)

    lg.plain(f"Income statement periods: quarter={quarter_num} annual={annual_num} total={num_after}")

    lg.plain(f"Income statement entries obtained: {num_after} (before: {num_before})")
    lg.success("Income statement endpoint passed")

def test_balance_sheet(source):
    """
        Test the balance sheet endpoint.

        Args:
            source(FMP): FMP data source instance.
    """
    print("\nSECTION5: Testing balance sheet endpoint (get_balance_sheet())...")
    print("________________________________________________________________")

    num_before = source.get_balance_sheet_num()

    source.get_balance_sheet()

    num_after = source.get_balance_sheet_num()

    if num_after <= num_before:
        failure(f"The number of balance sheet entries did not increase after get_balance_sheet() call: {num_before} -> {num_after}", source)

    lg.plain(f"Balance sheet entries obtained: {num_after} (before: {num_before})")
    lg.success("Balance sheet endpoint passed")

def test_cash_flow(source):
    """
        Test the cash flow statement endpoint.

        Args:
            source(FMP): FMP data source instance.
    """
    print("\nSECTION6: Testing cash flow statement endpoint (get_cash_flow())...")
    print("___________________________________________________________________")

    num_before = source.get_cash_flow_num()

    source.get_cash_flow()

    num_after = source.get_cash_flow_num()

    if num_after <= num_before:
        failure(f"The number of cash flow entries did not increase after get_cash_flow() call: {num_before} -> {num_after}", source)

    lg.plain(f"Cash flow entries obtained: {num_after} (before: {num_before})")
    lg.success("Cash flow statement endpoint passed")

def test_recent_data(source):
    """
        Test the recent quote endpoint (screening).

        Note that the recent quote data is not cached in the database, so only
        the returned data is verified.

        Args:
            source(FMP): FMP data source instance.
    """
    print("\nSECTION7: Testing recent quote endpoint (get_recent_data())...")
    print("______________________________________________________________")

    data = source.get_recent_data()

    if len(data) == 0:
        failure("No recent quote data obtained.", source)

    lg.plain(f"Recent quote obtained for {data[0]['date_time']}: {data[0]['closed']}")
    lg.success("Recent quote endpoint passed")

def test_get_delisted():
    lg.highlight("\nTesting get() for a delisted symbol (WBA):\n")

    now = datetime.now(tz.UTC)
    first_date = now - timedelta(days=60)
    last_date = now - timedelta(days=30)

    fmpi = fmp.FMP(symbol='WBA', first_date=first_date, last_date=last_date, verbosity=True, db_name=":memory:")
    fmpi.db_connect()

    print("First invocation (empty DB): expecting FdataError ...")
    raised_first = False
    try:
        fmpi.get()
    except FdataError as e:
        print(f"Got expected FdataError: {e}")
        raised_first = True

    if not raised_first:
        failure("First get() should have raised FdataError for a delisted symbol", fmpi)

    if fmpi.get_quotes_num(dt=False) != 0:
        failure("No quotes should be fetched for a delisted symbol", fmpi)

    if fmpi._get_min_request_ts() is not None or fmpi._get_max_request_ts() is not None:
        failure("No data_intervals should be marked for a delisted symbol", fmpi)

    lg.success("First invocation: raised FdataError, fetched nothing, no intervals marked")

    # Confirm the security is indeed marked as NotExist in sec_info. get_info()
    # raises FdataError when sec_type == NotExist.
    print("\nDirect get_info() check: expecting FdataError (sec_info persisted as NotExist) ...")
    raised_info = False
    try:
        fmpi.get_info()
    except FdataError as e:
        print(f"  Got expected FdataError: {e}")
        raised_info = True

    if not raised_info:
        failure("get_info() should raise FdataError for a symbol marked NotExist", fmpi)

    lg.success("get_info() confirmed the symbol is marked NotExist")

    # Second invocation: sec_info persisted as NotExist. get_info() raises before
    # fetch_info() runs (no API refetch).
    print("\nSecond invocation (sec_info persisted as NotExist): expecting FdataError ...")
    raised_second = False
    try:
        fmpi.get()
    except FdataError as e:
        print(f"Got expected FdataError: {e}")
        raised_second = True

    if not raised_second:
        failure("Second get() should also raise FdataError", fmpi)

    lg.success("Second invocation: raised FdataError from cached sec_info")

    fmpi.db_close()

def test_get_empty_range_valid_symbol():
    lg.highlight("\nTesting get() for a valid symbol with empty range:\n")

    fmpi = fmp.FMP(symbol='HOOD', first_date="2020-01-01", last_date="2020-02-16", verbosity=True, db_name=":memory:")
    fmpi.db_connect()

    print("First invocation (empty range): expecting intervals recorded ...")
    try:
        fmpi.get()
    except FdataError as e:
        print(f"  Got FdataError: {e}")

    if fmpi.get_quotes_num(dt=False) != 0:
        failure("No quotes should be fetched for an empty range", fmpi)

    min_req = fmpi._get_min_request_ts()
    max_req = fmpi._get_max_request_ts()

    if min_req is None or max_req is None:
        failure("Intervals should be recorded for an empty valid-symbol range", fmpi)

    lg.success(f"Intervals recorded: {get_dt(min_req)}..{get_dt(max_req)}")

    fmpi.db_close()

def test_get_non_existing():
    lg.highlight("\nTesting get() for a non-existing symbol (FFFF):\n")

    now = datetime.now(tz.UTC)
    first_date = now - timedelta(days=60)
    last_date = now - timedelta(days=30)

    fmpi = fmp.FMP(symbol='FFFF', first_date=first_date, last_date=last_date, verbosity=True, db_name=":memory:")
    fmpi.db_connect()

    print("First invocation (empty DB): expecting FdataError ...")
    raised_first = False
    try:
        fmpi.get()
    except FdataError as e:
        print(f"Got expected FdataError: {e}")
        raised_first = True

    if not raised_first:
        failure("First get() should have raised FdataError for a non-existing symbol", fmpi)

    if fmpi.get_quotes_num(dt=False) != 0:
        failure("No quotes should be fetched for a non-existing symbol", fmpi)

    if fmpi._get_min_request_ts() is not None or fmpi._get_max_request_ts() is not None:
        failure("No data_intervals should be marked for a non-existing symbol", fmpi)

    lg.success("First invocation: raised FdataError, fetched nothing, no intervals marked")

    # Confirm the security is indeed marked as NotExist in sec_info. get_info()
    # raises FdataError when sec_type == NotExist.
    print("\nDirect get_info() check: expecting FdataError (sec_info persisted as NotExist) ...")
    raised_info = False
    try:
        fmpi.get_info()
    except FdataError as e:
        print(f"  Got expected FdataError: {e}")
        raised_info = True

    if not raised_info:
        failure("get_info() should raise FdataError for a symbol marked NotExist", fmpi)

    lg.success("get_info() confirmed the symbol is marked NotExist")

    # Second invocation: sec_info persisted as NotExist. get_info() raises before
    # fetch_info() runs (no API refetch).
    print("\nSecond invocation (sec_info persisted as NotExist): expecting FdataError ...")
    raised_second = False
    try:
        fmpi.get()
    except FdataError as e:
        print(f"Got expected FdataError: {e}")
        raised_second = True

    if not raised_second:
        failure("Second get() should also raise FdataError", fmpi)

    lg.success("Second invocation: raised FdataError from cached sec_info")

    fmpi.db_close()

if __name__ == "__main__":
    lg.highlight("\nTesting FMP data source endpoints:\n")

    last_date = def_last_date
    first_date = get_dt(datetime.now(tz.UTC)) - timedelta(days=365*2)

    fmpi = fmp.FMP(symbol='AAPL', first_date=first_date, last_date=last_date, verbosity=True, db_name=":memory:")
    fmpi.db_connect()

    if fmpi._api_key is None:
        lg.warning("No FMP API key is configured. Set the FMP_API_KEY environment variable or configure the key in settings.py.")

    test_info(fmpi)

    test_quotes_dividends_splits(fmpi)

    test_cap(fmpi)

    test_income_statement(fmpi)

    test_balance_sheet(fmpi)

    test_cash_flow(fmpi)

    test_recent_data(fmpi)

    fmpi.db_close()

    # get() behavior for delisted, non-existing and empty-range valid symbols (fresh in-memory DBs)
    test_get_delisted()
    test_get_non_existing()
    test_get_empty_range_valid_symbol()

    lg.success("ALL TESTS PASSED!")
