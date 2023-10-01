"""Demonstration of Buy and Hold strategy backtesting.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""

from backtest.bh import BuyAndHold
from backtest.base import BackTestError
from backtest.stock import StockData
from backtest.reporting import Report

from data.fdata import FdataError
from data.yf import YF

import sys

threshold = 500  # Quotes num threshold for the test

min_width = 2500  # Minimum width for charting
height = 250  # Height of each subchart in reporting

import numpy as np
np.warnings.filterwarnings('error', category=np.VisibleDeprecationWarning)

if __name__ == "__main__":
    # Get quotes
    try:
        # Fetch quotes if there are less than a threshold number of records in the database for the specified timespan.
        source = YF(symbol="SPY", first_date="2020-10-01", last_date="2022-11-1")
        # TODO MID Consider fetching dividends also
        rows, num = source.fetch_if_none(threshold)
    except FdataError as e:
        sys.exit(e)

    length = len(rows)

    if num > 0:
        print(f"Fetched {num} quotes for {source.symbol}. Total number of quotes used is {length}.")
    else:
        print(f"No need to fetch quotes for {source.symbol}. There are {length} quotes in the database and it is >= the threshold level of {threshold}.")

    quotes = StockData(rows=rows,
                          title=source.symbol,
                          spread=0.1
                         )

    bh = BuyAndHold(
        data=[quotes],
        commission=2.5,
        periodic_deposit=500,
        deposit_interval=30,
        inflation=2.5,
        initial_deposit=10000
    )

    try:
        bh.calculate()
    except BackTestError as e:
        sys.exit(f"Can't perform backtesting calculation: {e}")

    results = bh.get_results()

    #################
    # Create a report
    #################

    report = Report(data=results, width=max(length, min_width))

    # Add a chart with quotes
    fig_quotes = report.add_quotes_chart(title=f"BuyAndHold Example Testing for {source.symbol}")

    # Add a chart to represent portfolio performance
    fig_portf = report.add_portfolio_chart(height=height)

    # Add chart a with expenses
    report.add_expenses_chart(height=height)

    # Add annotations with strategy results
    report.add_annotations(title="B&H Strategy performance/expenses:")

    # Show image
    new_file = report.show_image()
    print(f"{new_file} is written.")
