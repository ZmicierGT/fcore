"""Demo of intraday RSI multi symbol strategy.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

from backtest.rsi import RSI
from backtest.base import BackTestError
from backtest.stock import StockData
from backtest.reporting import Report

from data.fdata import FdataError
from data.fvalues import Timespans
from data.yf import YFQuery, YF

import plotly.graph_objects as go
from plotly import subplots

from itertools import repeat

from datetime import datetime, timedelta

import sys

threshold = 1000  # Quotes num threshold for the test

symbols = ['MSFT', 'AAPL']

period = 14
support = 30
resistance = 70

min_width = 2500  # Minimum width for charting
height = 250  # Height of each subchart in reporting

if __name__ == "__main__":
    print("Using YF as the data source for demonstration purposes only! Please note that the data is delayed (especially volume)")
    print("and exceptions due to network errors may happen.")

    # Array for the fetched data for all symbols
    allrows = []

    # As YF is used as the default data source, the data should be withing the last 30 days. Use the last week as the interval.
    # TODO Currently the start and end data processing may be different based on data source and database (included or not included). Needs to be adjusted.
    now = datetime.now()
    then = now - timedelta(days=7)

    print("At least 1000 quotes for each symbol need to be fetched for the last week.")

    for symbol in symbols:
        try:
            # Fetch quotes if there are less than a threshold number of records in the database for a day (default) timespan.
            query = YFQuery(symbol=symbol, first_date=then, last_date=now, timespan=Timespans.Intraday)
            rows, num = YF(query).fetch_if_none(threshold)
        except FdataError as e:
            sys.exit(e)

        if num > 0:
            print(f"Fetched {num} quotes for {query.symbol}. Total number of quotes used is {len(rows)}.")
        else:
            # Please note that data providers and database may tread the date differently. For example, YF in query fetches the data for the end day
            # but sql database will fetch the data TILL the last day.
            print(f"No need to fetch quotes for {query.symbol}. There are {len(rows)} quotes in the database and it is >= the threshold level of {threshold}.")

        allrows.append(rows)

    # RSI strategy calculation

    data_a = StockData(rows=allrows[0],
                          title=symbols[0],
                          spread=0.1,
                          margin_rec=0.4,
                          margin_req=0.7,
                         )

    data_b = StockData(rows=allrows[1],
                          title=symbols[1],
                          spread=0.1,
                          margin_rec=0.4,
                          margin_req=0.7,
                         )

    quotes = [data_a, data_b]

    rsi = RSI(data=quotes,
              commission=2.5,
              initial_deposit=10000,
              margin_rec=0.9,
              margin_req=1,
              period=period,
              support=support,
              resistance=resistance,
              to_short=True
             )

    try:
        rsi.calculate()
    except BackTestError as e:
        sys.exit(f"Can't perform backtesting calculation: {e}")

    results = rsi.get_results()

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

    # Add a chart with expenses
    report.add_expenses_chart(height=height)

    # Add annotations with strategy results
    report.add_annotations()

    # Show image
    new_file = report.show_image()
    print(f"{new_file} is written.")
