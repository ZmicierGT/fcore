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

min_width = 2500  # Minimum width for charting
height = 250  # Height of each subchart in reporting

symbol = 'RSPT'

if __name__ == "__main__":
    # Get quotes
    try:
        warning = "WARNING! Using yfinance data for the demonstration.\n" +\
                  "Always keep yfinance up to date ( pip install yfinance --upgrade ) and use quotes obtained from this " +\
                  "datasource only for demonstation purposes!\n"
        print(warning)

        rows = YF(symbol=symbol, first_date="2020-10-01", last_date="2023-11-1").get()
    except FdataError as e:
        sys.exit(e)

    length = len(rows)

    print(f"The total number of quotes used for {symbol} is {length}.\n")

    quotes = StockData(rows=rows,
                       title=symbol,
                       spread=0.1)

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
    fig_quotes = report.add_quotes_chart(title=f"BuyAndHold Example Testing for {symbol}")

    # Add a chart to represent portfolio performance
    fig_portf = report.add_portfolio_chart(height=height)

    # Add chart a with expenses
    report.add_expenses_chart(height=height)

    # Add annotations with strategy results
    report.add_annotations(title="B&H Strategy performance/expenses:")

    # Show image
    new_file = report.show_image()
    print(f"{new_file} is written.")
