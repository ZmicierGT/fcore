"""Demo of intraday RSI multi symbol strategy.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from backtest.rsi import RSI
from backtest.base import BackTestError
from backtest.stock import StockData
from backtest.reporting import Report

from data.fdata import FdataError
from data.fvalues import Timespans
from data.yf import YF

import plotly.graph_objects as go
from plotly import subplots

from itertools import repeat

from datetime import datetime, timedelta
import pytz

import sys

symbols = ['MSFT', 'AAPL']

period = 14
support = 30
resistance = 70

min_width = 2500  # Minimum width for charting
height = 250  # Height of each subchart in reporting

if __name__ == "__main__":
    print("Using YF as the data source for demonstration purposes only! Please note that the data is delayed (especially volume)")
    print("and exceptions due to network errors may happen.\n")

    # Array for the fetched data for all symbols
    allrows = []

    # As YF is used as the default data source, the data should be withing the last 30 days. Use the last week as the interval.
    # TODO LOW Currently the start and end data processing may be different based on data source and database (included or not included). Needs to be adjusted.
    # TODO MID Think what to do if we can't maintain a continuous request withing 7 days
    then = datetime.now().replace(tzinfo=pytz.utc) - timedelta(days=5)

    warning = "WARNING! Using yfinance data for the demonstration.\n" +\
                "Always keep yfinance up to date ( pip install yfinance --upgrade ) and use quotes obtained from this " +\
                "datasource only for demonstation purposes!\n"
    print(warning)

    for symbol in symbols:
        try:
            # TODO MID Check why update warning is displayed
            rows = YF(symbol=symbol, first_date=then, timespan=Timespans.Minute, verbosity=True).get()
        except FdataError as e:
            sys.exit(e)

        print(f"The total number of quotes used for {symbol} is {len(rows)}.\n")

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

    rsi_fig.add_trace(go.Scatter(x=results.DateTime, y=rsi.exec(0).get_vals()['rsi'], mode='lines', name=f"RSI {symbols[0]}"))
    rsi_fig.add_trace(go.Scatter(x=results.DateTime, y=rsi.exec(1).get_vals()['rsi'], mode='lines', name=f"RSI {symbols[1]}"))

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
