"""FMP data source endpoints testing script.

Each test verifies that the number of entries in the database has increased after the corresponding fetch is performed.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from data import fmp

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

    print(colored(f"Info obtained: {info}", "yellow"))
    print(colored("Info endpoint passed", 'green'))

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

    print(colored(f"Quotes obtained: {quotes_after} (before: {quotes_before})", "yellow"))
    print(colored(f"Dividends obtained: {divs_after} (before: {divs_before})", "yellow"))
    print(colored(f"Splits obtained: {splits_after} (before: {splits_before})", "yellow"))
    print(colored("Quotes/dividends/splits endpoints passed", 'green'))

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

    print(colored(f"Capitalization entries obtained: {num_after} (before: {num_before})", "yellow"))
    print(colored("Capitalization endpoint passed", 'green'))

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

    print(colored(f"Income statement entries obtained: {num_after} (before: {num_before})", "yellow"))
    print(colored("Income statement endpoint passed", 'green'))

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

    print(colored(f"Balance sheet entries obtained: {num_after} (before: {num_before})", "yellow"))
    print(colored("Balance sheet endpoint passed", 'green'))

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

    print(colored(f"Cash flow entries obtained: {num_after} (before: {num_before})", "yellow"))
    print(colored("Cash flow statement endpoint passed", 'green'))

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

    print(colored(f"Recent quote obtained for {data[0]['date_time']}: {data[0]['closed']}", "yellow"))
    print(colored("Recent quote endpoint passed", 'green'))

if __name__ == "__main__":
    print(colored("\nTesting FMP data source endpoints:\n", "yellow"))

    fmpi = fmp.FMP(symbol='AAPL', first_date="2020-2-1", last_date="2024-3-1", verbosity=True, db_name=":memory:")
    fmpi._db_connect()

    if fmpi._api_key is None:
        print(colored("Warning! No FMP API key is configured. Set the FMP_API_KEY environment variable or configure the key in settings.py.", "yellow"))

    test_info(fmpi)

    test_quotes_dividends_splits(fmpi)

    test_cap(fmpi)

    test_income_statement(fmpi)

    test_balance_sheet(fmpi)

    test_cash_flow(fmpi)

    test_recent_data(fmpi)

    fmpi._db_close()

    print(colored("ALL TESTS PASSED!", "green"))
