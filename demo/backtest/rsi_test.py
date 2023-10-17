"""Demo of RSI EOD multi symbol strategy.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from backtest.rsi import RSI
from backtest.bh import BuyAndHold
from backtest.base import BackTestError
from backtest.stock import StockData
from backtest.reporting import Report

from data.fdata import FdataError
from data.yf import YF

import plotly.graph_objects as go
from plotly import subplots

from itertools import repeat

import sys

first_date = "2020-10-01"  # First date to fetch quotes
last_date = "2021-10-01"  # The last date to fetch quotes

symbol1 = 'MMM'
symbol2 = 'AXP'

symbols = [[symbol1, 245, 4],
           [symbol2, 187, 6]]

period = 14
support = 30
resistance = 70

min_width = 2500  # Minimum width for charting
height = 250  # Height of each subchart in reporting

if __name__ == "__main__":
    # Array for the fetched data for all symbols
    allrows = []

    warning = "WARNING! Using yfinance data for the demonstration.\n" +\
                "Always keep yfinance up to date ( pip install yfinance --upgrade ) and use quotes obtained from this " +\
                "datasource only for demonstation purposes!\n"
    print(warning)

    for symbol, threshold_divs, threshold_splits in symbols:
        try:
            source = YF(symbol=symbol, first_date=first_date, last_date=last_date)
            rows = source.fetch_stock_data_if_none(threshold_divs, threshold_splits)
        except FdataError as e:
            sys.exit(e)

        print(f"The total number of quotes used for {source.symbol} is {len(rows)}.\n")

        allrows.append(rows)

    data_a = StockData(rows=allrows[0],
                          title=symbol1,
                          spread=0.1
                         )

    data_b = StockData(rows=allrows[1],
                          title=symbol2,
                          spread=0.1
                         )

    quotes = [data_a, data_b]

    rsi = RSI(data=quotes,
              commission=2.5,
              periodic_deposit=500,
              deposit_interval=30,
              inflation=2.5,
              initial_deposit=10000,
              period=period,
              support=support,
              resistance=resistance
             )

    bh_a = BuyAndHold(
        data=[data_a],
        commission=2.5,
        initial_deposit=10000,
        periodic_deposit=500,
        deposit_interval=30,
        inflation=2.5,
        offset=period
    )

    bh_b = BuyAndHold(
        data=[data_b],
        commission=2.5,
        initial_deposit=10000,
        periodic_deposit=500,
        deposit_interval=30,
        inflation=2.5,
        offset=period
    )

    try:
        rsi.calculate()
        bh_a.calculate()
        bh_b.calculate()
    except BackTestError as e:
        sys.exit(f"Can't perform backtesting calculation: {e}")

    results = rsi.get_results()
    results_bh_a = bh_a.get_results()
    results_bh_b = bh_b.get_results()

    # Support and resistance for RSI
    length = len(allrows[0])
    support_arr = []
    resistance_arr = []

    support_arr.extend(repeat(support, length))
    resistance_arr.extend(repeat(resistance, length))

    #################
    # Create a report
    #################

    # TODO LOW Consider adding B&H text statistics at the bottom
    report = Report(data=results, width=max(length, min_width), margin=True)

    # Add charts for used symbols
    report.add_quotes_chart(title=f"RSI Multi Example Testing for {symbol1} and {symbol2}", height=250)
    report.add_quotes_chart(index=1, height=height)

    # Add a custom chart with RSI values
    rsi_fig = subplots.make_subplots()

    rsi_fig.add_trace(go.Scatter(x=results.DateTime, y=results.Symbols[0].Tech[0], mode='lines', name=f"RSI {symbol1}"))
    rsi_fig.add_trace(go.Scatter(x=results.DateTime, y=results.Symbols[1].Tech[0], mode='lines', name=f"RSI {symbol2}"))

    # Add support and resistance lines to the second chart
    rsi_fig.add_trace(go.Scatter(x=results.DateTime, y=support_arr, mode='lines', name="Support"))
    rsi_fig.add_trace(go.Scatter(x=results.DateTime, y=resistance_arr, mode='lines', name="Resistance"))

    report.add_custom_chart(rsi_fig, height=height)

    # Add a chart to represent portfolio performance
    fig_portf = report.add_portfolio_chart(height=height)

    # Add B&H performance comparison to the portfolio chart
    fig_portf.add_trace(go.Scatter(x=results.DateTime, y=results_bh_a.TotalValue, mode='lines', name=f"B&H {symbol1}"))
    fig_portf.add_trace(go.Scatter(x=results.DateTime, y=results_bh_b.TotalValue, mode='lines', name=f"B&H {symbol2}"))

    # Add a chart with expenses
    report.add_expenses_chart(height=height)

    # Add annotations with strategy results
    report.add_annotations()

    # Show image
    new_file = report.show_image()
    print(f"{new_file} is written.")
