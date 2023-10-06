"""Moving average demonstration using pandas_ta. It demontrates a basic usage of a third party indicator using
   Fcore's data management API.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from data.futils import update_layout
from data.futils import show_image

from data.fdata import FdataError
from data.fvalues import StockQuotes

from data.yf import YF

import sys

import pandas as pd
import pandas_ta as ta

threshold = 525  # Quotes number threshold for calculation
period = 50

if __name__ == "__main__":
    # Get quotes
    try:
        # Fetch quotes if there are less than a threshold number of records in the database for the specified timespan.
        warning = "WARNING! Using yfinance data for the demonstration.\n" +\
                  "Always keep yfinance up to date ( pip install yfinance --upgrade ) and use quotes obtained from this " +\
                  "datasource only for demonstation purposes!\n"
        print(warning)

        source = YF(symbol="SPY", first_date="2020-10-01", last_date="2022-11-1")
        rows, num = source.fetch_if_none(threshold)
    except FdataError as e:
        sys.exit(e)

    length = len(rows)

    if num > 0:
        print(f"Fetched {num} quotes for {source.symbol}. Total number of quotes used is {length}.")
    else:
        print(f"No need to fetch quotes for {source.symbol}. There are {length} quotes in the database and it is >= the threshold level of {threshold}.")

    # Calculate MA
    df = pd.DataFrame(rows)

    sma = ta.sma(df[StockQuotes.AdjClose], length = period)
    ema = ta.ema(df[StockQuotes.AdjClose], length = period)

    dates = rows[StockQuotes.DateTime]
    price = rows[StockQuotes.AdjClose]

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

    update_layout(fig, f"SMA/EMA example chart for {source.symbol}", length)

    fig.update_yaxes(title_text="<b>Price</b>", secondary_y=False)
    fig.update_yaxes(title_text="<b>SMA/EMA</b>", secondary_y=False)

    new_file = show_image(fig)

    print(f"{new_file} is written.")
