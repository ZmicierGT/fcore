"""Demo of intraday RSI multi symbol strategy.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

from backtest.rsi import RSITest
from backtest.bh import BuyAndHold

from backtest.base import BackTestError
from backtest.stock import StockData

from data.futils import check_datetime
from data.futils import standard_margin_chart

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from data.fdata import Query, ReadOnlyData
from data.futils import write_image
from data.fdata import FdataError

from data.fvalues import Timespans

from itertools import repeat

import sys

if __name__ == "__main__":

    ##################
    # First symbol
    ##################

    # Fetch the quotes at first:
    # python polygon_cli.py -s MSFT -t Intraday -f 2022-07-11 -l 2022-07-13
    # python polygon_cli.py -s AAPL -t Intraday -f 2022-07-11 -l 2022-07-13

    query_a = Query()
    query_a.symbol = "MSFT"
    query_a.timespan = Timespans.Intraday
    query_a.db_connect()

    # Implement that only syncronised quotes should be used in multi-symbol backtesting. Every quote which does not match other should be removed from testing
    query_a.first_date = check_datetime("2022-07-11 14:30:00")[1]
    query_a.last_date = check_datetime("2022-07-11 21:00:00")[1]

    data_a = ReadOnlyData(query_a)

    try:
        rows_a = data_a.get_quotes()
        query_a.db_close()
    except FdataError as e:
        print(e)
        sys.exit(2)

    length_a = len(rows_a)

    print(f"Obtained {length_a} rows for {query_a.symbol}.")

    ##################
    # Secondary symbol
    ##################

    query_b = Query()
    query_b.symbol = "AAPL"
    query_b.timespan = Timespans.Intraday
    query_b.db_connect()

    query_b.first_date = check_datetime("2022-07-11 14:30:00")[1]
    query_b.last_date = check_datetime("2022-07-11 21:00:00")[1]

    data_b = ReadOnlyData(query_b)

    try:
        rows_b = data_b.get_quotes()
        query_b.db_close()
    except FdataError as e:
        print(e)
        sys.exit(2)

    length_b = len(rows_b)

    print(f"Obtained {length_b} rows for {query_b.symbol}.")

    if length_a == 0 or length_b == 0:
        print(f"Make sure that the symbols are fetched and present in the {query_a.db_name} database.")
        sys.exit(2)

    data_a = StockData(rows=rows_a,
                          title=query_a.symbol,
                          spread=0.1,
                          margin_rec=0.4,
                          margin_req=0.7,
                         )

    data_b = StockData(rows=rows_b,
                          title=query_b.symbol,
                          spread=0.1,
                          margin_rec=0.4,
                          margin_req=0.7,
                         )

    quotes = [data_a, data_b]

    period = 14
    support = 30
    resistance = 70

    rsi = RSITest(data=quotes,
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
        print(f"Can't perform backtesting calculation: {e}")
        sys.exit(2)

    results = rsi.get_results()

    # Support and resistance for RSI
    support_arr = []
    resistance_arr = []

    support_arr.extend(repeat(support, length_a))
    resistance_arr.extend(repeat(resistance, length_a))

    ##################
    # Build the charts
    ##################

    # Create a custom figure
    fig = make_subplots(rows=4, cols=1, shared_xaxes=True, row_width=[0.25, 0.25, 0.25, 0.25],
                        specs=[[{"secondary_y": True}],
                            [{"secondary_y": False}],
                            [{"secondary_y": True}],
                            [{"secondary_y": False}]])

    # Create a standard chart
    standard_margin_chart(results, fig=fig, title=f"RSI Multi Example Testing for {query_a.symbol} and {query_b.symbol}")

    # Add RSI values to the second chart
    fig.add_trace(go.Scatter(x=results.DateTime, y=results.Symbols[0].Tech, mode='lines', name=f"RSI {query_a.symbol}"), row=2, col=1)
    fig.add_trace(go.Scatter(x=results.DateTime, y=results.Symbols[1].Tech, mode='lines', name=f"RSI {query_b.symbol}"), row=2, col=1)

    # Add support and resistance lines to the second chart
    fig.add_trace(go.Scatter(x=results.DateTime, y=support_arr, mode='lines', name="Support"), row=2, col=1)
    fig.add_trace(go.Scatter(x=results.DateTime, y=resistance_arr, mode='lines', name="Resistance"), row=2, col=1)

    ######################
    # Write the chart
    ######################

    new_file = write_image(fig)

    print(f"{new_file} is written.")
