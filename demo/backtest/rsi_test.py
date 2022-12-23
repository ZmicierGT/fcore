"""Demo of RSI EOD multi symbol strategy.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

from backtest.rsi import RSI
from backtest.bh import BuyAndHold
from backtest.base import BackTestError
from backtest.stock import StockData
from backtest.reporting import Report

from data.fdata import FdataError
from data.yf import YFQuery, YF

import plotly.graph_objects as go
from plotly import subplots

from itertools import repeat

import sys

threshold = 250  # Quotes num threshold for the test
first_date = "2020-10-01"  # First date to fetch quotes
last_date = "2021-10-01"  # The last date to fetch quotes

symbols = ['SPY', 'AAPL']

period = 14
support = 30
resistance = 70

min_width = 2500  # Minimum width for charting
height = 250  # Height of each subchart in reporting

if __name__ == "__main__":
    # Array for the fetched data for all symbols
    allrows = []

    for symbol in symbols:
        try:
            # Fetch quotes if there are less than a threshold number of records in the database for a day (default) timespan
            query = YFQuery(symbol=symbol, first_date=first_date, last_date=last_date)
            rows, num = YF(query).fetch_if_none(threshold)
        except FdataError as e:
            sys.exit(e)

        if num > 0:
            print(f"Fetched {num} quotes for {query.symbol}. Total number of quotes used is {len(rows)}.")
        else:
            print(f"No need to fetch quotes for {query.symbol}. There are {len(rows)} quotes in the database and it is >= the threshold level of {threshold}.")

        allrows.append(rows)

    data_a = StockData(rows=allrows[0],
                          title=symbols[0],
                          spread=0.1,
                          use_yield=1.5,
                          yield_interval=90
                         )

    data_b = StockData(rows=allrows[1],
                          title=symbols[1],
                          spread=0.1,
                          use_yield=1.5,
                          yield_interval=90
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

    report = Report(data=results, width=max(length, min_width), margin=True)

    # Add charts for used symbols
    report.add_quotes_chart(title=f"RSI Multi Example Testing for {symbols[0]} and {symbols[1]}", height=250)
    report.add_quotes_chart(index=1, height=height)

    # Add a custom chart with RSI values
    rsi_fig = subplots.make_subplots()

    rsi_fig.add_trace(go.Scatter(x=results.DateTime, y=results.Symbols[0].Tech[0], mode='lines', name=f"RSI {symbols[0]}"))
    rsi_fig.add_trace(go.Scatter(x=results.DateTime, y=results.Symbols[1].Tech[0], mode='lines', name=f"RSI {symbols[1]}"))

    # Add support and resistance lines to the second chart
    rsi_fig.add_trace(go.Scatter(x=results.DateTime, y=support_arr, mode='lines', name="Support"))
    rsi_fig.add_trace(go.Scatter(x=results.DateTime, y=resistance_arr, mode='lines', name="Resistance"))

    report.add_custom_chart(rsi_fig, height=height)

    # Add a chart to represent portfolio performance
    fig_portf = report.add_portfolio_chart(height=height)

    # Add B&H performance comparison to the portfolio chart
    fig_portf.add_trace(go.Scatter(x=results.DateTime, y=results_bh_a.TotalValue, mode='lines', name=f"B&H {symbols[0]}"))
    fig_portf.add_trace(go.Scatter(x=results.DateTime, y=results_bh_b.TotalValue, mode='lines', name=f"B&H {symbols[1]}"))

    # Add a chart with expenses
    report.add_expenses_chart(height=height)

    # Add annotations with strategy results
    report.add_annotations()

    # Show image
    new_file = report.show_image()
    print(f"{new_file} is written.")
