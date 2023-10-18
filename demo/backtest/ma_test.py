"""Demonstration of MA/price cross strategy backtesting.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from backtest.ma import MA
from backtest.base import BackTestError
from backtest.stock import StockData
from backtest.bh import BuyAndHold
from backtest.reporting import Report

from data.fdata import FdataError
from data.yf import YF

import plotly.graph_objects as go

import sys

period = 50  # Period used in strategy

min_width = 2500 # Minimum width for charting
height = 250  # Height of each subchart in reporting

symbol = 'NKE'

if __name__ == "__main__":
    # Get quotes
    try:
        warning = "WARNING! Using yfinance data for the demonstration.\n" +\
                  "Always keep yfinance up to date ( pip install yfinance --upgrade ) and use quotes obtained from this " +\
                  "datasource only for demonstation purposes!\n"
        print(warning)

        rows = YF(symbol=symbol, first_date="2015-06-01", last_date="2016-06-1").get()
    except FdataError as e:
        sys.exit(e)

    length = len(rows)

    print(f"The total number of quotes used for {symbol} is {length}.\n")

    quotes = StockData(rows=rows,
                          title=symbol,
                          margin_rec=0.4,
                          margin_req=0.7,
                          spread=0.1,
                          margin_fee=1,
                          trend_change_period=2,
                          trend_change_percent=2
                         )

    ma = MA(
        data=[quotes],
        commission=2.5,
        initial_deposit=10000,
        periodic_deposit=500,
        deposit_interval=30,
        inflation=2.5,
        period=period,
        margin_rec=0.9,
        margin_req=1,
        verbosity=False,
    )

    # Buy and Hold to compare

    quotes_bh = StockData(rows=rows,
                             title=symbol,
                             spread=0.1,
                            )

    bh = BuyAndHold(
        data=[quotes_bh],
        commission=2.5,
        initial_deposit=10000,
        periodic_deposit=500,
        deposit_interval=30,
        inflation=2.5,
        offset=period
    )

    try:
        ma.calculate()
        bh.calculate()
    except BackTestError as e:
        sys.exit(f"Can't perform backtesting calculation: {e}")

    results_bh = bh.get_results()
    results = ma.get_results()

    #################
    # Create a report
    #################

    report = Report(data=results, width=max(length, min_width), margin=True)

    # Add a chart with quotes
    fig_quotes = report.add_quotes_chart(title=f"MA/Quote Cross Backtesting Example for {symbol}")

    # Append MA values to the quotes chart
    fig_quotes.add_trace(go.Scatter(x=results.DateTime, y=results.Symbols[0].Tech[0], mode='lines', name="MA", line=dict(color="green")))

    # Add a chart to represent portfolio performance
    fig_portf = report.add_portfolio_chart(height=height)

    # Append B&H comparison to the portfolio chart
    fig_portf.add_trace(go.Scatter(x=results.DateTime, y=results_bh.TotalValue, mode='lines', name="Total Value Buy and Hold", line=dict(color="#32CD32")))

    # Add chart a with expenses
    report.add_expenses_chart(height=height)

    # Add annotations with strategy results
    report.add_annotations()

    # Show image
    new_file = report.show_image()
    print(f"{new_file} is written.")
