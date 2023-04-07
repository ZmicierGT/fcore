"""MACD demonstration using pandas_ta.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from data.futils import update_layout
from data.futils import show_image

from data.fdata import FdataError
from data.fvalues import Quotes

from data.yf import YF

import sys

import pandas as pd
import pandas_ta as ta

slow_period = 26
fast_period = 12
signal_period = 9

threshold = 525  # Quotes number threshold for calculation

if __name__ == "__main__":
    # Get quotes
    try:
        # Fetch quotes if there are less than a threshold number of records in the database for the specified timespan.
        source = YF(symbol="SPY", first_date="2020-10-01", last_date="2022-11-1")
        rows, num = source.fetch_if_none(threshold)
    except FdataError as e:
        sys.exit(e)

    length = len(rows)

    if num > 0:
        print(f"Fetched {num} quotes for {source.symbol}. Total number of quotes used is {length}.")
    else:
        print(f"No need to fetch quotes for {source.symbol}. There are {length} quotes in the database and it is >= the threshold level of {threshold}.")

    # Calculate MACD
    #df = pd.DataFrame(rows)

    dates = [row[Quotes.DateTime] for row in rows]
    price = [row[Quotes.AdjClose] for row in rows]

    # Please note that the warning below is caused by pandas_ta issue.
    macd = ta.macd(pd.Series(price), fast_period, slow_period, signal_period)

    macd_values = macd.iloc[:,0]
    histogram = macd.iloc[:,1]
    signal_values = macd.iloc[:,2]

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_width=[0.3, 0.7],
                        specs=[[{"secondary_y": True}],
                            [{"secondary_y": True}]])

    fig.add_trace(
        go.Scatter(x=dates, y=price, name="AdjClose"),
        secondary_y=False,
    )

    fig.append_trace(go.Scatter(x=dates, y=macd_values, mode='lines', name="MACD"), row=2, col=1)
    fig.append_trace(go.Scatter(x=dates, y=signal_values, mode='lines', name="Signal"), row=2, col=1)
    fig.add_trace(go.Scatter(x=dates, y=histogram, fill='tozeroy', name="Histogram"), row=2, col=1, secondary_y=True)

    ######################
    # Write the chart
    ######################

    update_layout(fig, f"MACD example chart for {source.symbol}", length)

    new_file = show_image(fig)

    print(f"{new_file} is written.")
