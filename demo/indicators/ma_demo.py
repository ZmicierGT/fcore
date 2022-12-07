"""Moving average demonstration using pandas_ta.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from data.futils import update_layout
from data.futils import show_image

from data.fdata import FdataError
from data.fvalues import Quotes

from data.yf import YFError, YFQuery, YF

import sys

import pandas as pd
import pandas_ta as ta

threshold = 525  # Quotes number threshold for calculation
period = 50

if __name__ == "__main__":
    # Get quotes
    try:
        # Fetch quotes if there are less than a threshold number of records in the database for the specified timespan.
        query = YFQuery(symbol="SPY", first_date="2020-10-01", last_date="2022-11-1")
        rows, num = YF(query).fetch_if_none(threshold)
    except (YFError, FdataError) as e:
        print(e)
        sys.exit(2)

    length = len(rows)

    if num > 0:
        print(f"Fetched {num} quotes for {query.symbol}. Total number of quotes used is {length}.")
    else:
        print(f"No need to fetch quotes for {query.symbol}. There are {length} quotes in the database and it is >= the threshold level of {threshold}.")

    # Calculate MA
    df = pd.DataFrame(rows)

    sma = ta.sma(df[Quotes.AdjClose], length = period)
    ema = ta.ema(df[Quotes.AdjClose], length = period)

    dates = [row[Quotes.DateTime] for row in rows]
    price = [row[Quotes.AdjClose] for row in rows]

    fig = make_subplots(specs=[[{"secondary_y": False}]])

    fig.add_trace(
        go.Scatter(x=dates, y=price, name="AdjClose"),
        secondary_y=False,
    )

    fig.add_trace(
        go.Scatter(x=dates, y=sma, name="SMA"),
        secondary_y=False,
    )

    fig.add_trace(
        go.Scatter(x=dates, y=ema, name="EMA"),
        secondary_y=False,
    )

    ######################
    # Write the chart
    ######################

    update_layout(fig, f"SMA/EMA example chart for {query.symbol}", length)

    fig.update_yaxes(title_text="<b>Price</b>", secondary_y=False)
    fig.update_yaxes(title_text="<b>SMA/EMA</b>", secondary_y=False)

    new_file = show_image(fig)

    print(f"{new_file} is written.")
