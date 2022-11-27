"""Demo of RSI EOD multi symbol strategy.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

from backtest.rsi import RSI
from backtest.bh import BuyAndHold

from backtest.base import BackTestError
from backtest.stock import StockData

from data.futils import standard_chart

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from data.futils import write_image
from data.fdata import FdataError

from data.yf import YFError, YFQuery, YF

from itertools import repeat

import sys

threshold = 250  # Quotes num threshold for the test
first_date = "2020-10-01"  # First date to fetch quotes
last_date = "2021-10-01"  # The last date to fetch quotes

symbols = ['SPY', 'AAPL']

period = 14
support = 30
resistance = 70

if __name__ == "__main__":
    # Array for the fetched data for all symbols
    allrows = []

    for symbol in symbols:
        try:
            # Fetch quotes if there are less than a threshold number of records in the database for a day (default) timespan
            query = YFQuery(symbol=symbol, first_date=first_date, last_date=last_date)
            rows, num = YF(query).fetch_if_none(threshold)
        except (YFError, FdataError) as e:
            print(e)
            sys.exit(2)

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
        print(f"Can't perform backtesting calculation: {e}")
        sys.exit(2)

    results = rsi.get_results()
    results_bh_a = bh_a.get_results()
    results_bh_b = bh_b.get_results()

    # Support and resistance for RSI
    length = len(allrows[0])
    support_arr = []
    resistance_arr = []

    support_arr.extend(repeat(support, length))
    resistance_arr.extend(repeat(resistance, length))

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
    standard_chart(results, fig=fig, title=f"RSI Multi Example Testing for {symbols[0]} and {symbols[1]}")

    # Add RSI values to the second chart
    fig.add_trace(go.Scatter(x=results.DateTime, y=results.Symbols[0].Tech[0], mode='lines', name=f"RSI {symbols[0]}"), row=2, col=1)
    fig.add_trace(go.Scatter(x=results.DateTime, y=results.Symbols[1].Tech[0], mode='lines', name=f"RSI {symbols[1]}"), row=2, col=1)

    # Add support and resistance lines to the second chart
    fig.add_trace(go.Scatter(x=results.DateTime, y=support_arr, mode='lines', name="Support"), row=2, col=1)
    fig.add_trace(go.Scatter(x=results.DateTime, y=resistance_arr, mode='lines', name="Resistance"), row=2, col=1)

    # Add B&H comparison for both symbols to the third chart
    fig.add_trace(go.Scatter(x=results.DateTime, y=results_bh_a.TotalValue, mode='lines', name=f"B&H {symbols[0]}"), row=3, col=1)
    fig.add_trace(go.Scatter(x=results.DateTime, y=results_bh_b.TotalValue, mode='lines', name=f"B&H {symbols[1]}"), row=3, col=1)

    ######################
    # Write the chart
    ######################

    new_file = write_image(fig)

    print(f"{new_file} is written.")
