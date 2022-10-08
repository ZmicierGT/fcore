"""Demo of RSI EOD multi symbol strategy.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

from backtest.rsi import RSITest
from backtest.bh import BuyAndHold

from backtest.base import BackTestError
from backtest.stock import StockData

from data.futils import check_date
from data.futils import standard_chart

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from data.fdata import Query, ReadOnlyData
from data.futils import write_image
from data.fdata import FdataError

from itertools import repeat

import sys

if __name__ == "__main__":

    ##################
    # First symbol
    ##################

    query_a = Query()
    query_a.symbol = "SPY"
    query_a.db_connect()

    query_a.first_date = check_date("2020-10-01")[1]
    query_a.last_date = check_date("2021-10-01")[1]

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
    query_b.first_date = check_date("2020-10-01")[1]
    query_b.last_date = check_date("2021-10-01")[1]
    query_b.db_connect()

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
                          use_yield=1.5,
                          yield_interval=90
                         )

    data_b = StockData(rows=rows_b,
                          title=query_b.symbol,
                          spread=0.1,
                          use_yield=1.5,
                          yield_interval=90
                         )

    quotes = [data_a, data_b]

    period = 14
    support = 30
    resistance = 70

    rsi = RSITest(data=quotes,
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
        print(f"Can't perform backtesting calculation: {e}")
        sys.exit(2)

    results = rsi.get_results()
    results_bh_a = bh_a.get_results()
    results_bh_b = bh_b.get_results()

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
    standard_chart(results, fig=fig, title=f"RSI Multi Example Testing for {query_a.symbol} and {query_b.symbol}")

    # Add RSI values to the second chart
    fig.add_trace(go.Scatter(x=results.DateTime, y=results.Symbols[0].Tech, mode='lines', name=f"RSI {query_a.symbol}"), row=2, col=1)
    fig.add_trace(go.Scatter(x=results.DateTime, y=results.Symbols[1].Tech, mode='lines', name=f"RSI {query_b.symbol}"), row=2, col=1)

    # Add support and resistance lines to the second chart
    fig.add_trace(go.Scatter(x=results.DateTime, y=support_arr, mode='lines', name="Support"), row=2, col=1)
    fig.add_trace(go.Scatter(x=results.DateTime, y=resistance_arr, mode='lines', name="Resistance"), row=2, col=1)

    # Add B&H comparison for both symbols to the third chart
    fig.add_trace(go.Scatter(x=results.DateTime, y=results_bh_a.TotalValue, mode='lines', name=f"B&H {query_a.symbol}"), row=3, col=1)
    fig.add_trace(go.Scatter(x=results.DateTime, y=results_bh_b.TotalValue, mode='lines', name=f"B&H {query_b.symbol}"), row=3, col=1)

    ######################
    # Write the chart
    ######################

    new_file = write_image(fig)

    print(f"{new_file} is written.")
