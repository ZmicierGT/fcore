"""RSI demonstation using pandas_ta.

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
from itertools import repeat

import pandas as pd
import pandas_ta as ta

period = 14
upper_band = 70
lower_band = 30

if __name__ == "__main__":
    # Get quotes
    try:
        warning = "WARNING! Using yfinance data for the demonstration.\n" +\
                  "Always keep yfinance up to date ( pip install yfinance --upgrade ) and use quotes obtained from this " +\
                  "datasource only for demonstation purposes!\n"
        print(warning)

        source = YF(symbol="SPY", first_date="2020-10-01", last_date="2022-11-1")
        rows = source.fetch_if_none()
    except FdataError as e:
        sys.exit(e)

    length = len(rows)

    print(f"The total number of quotes used for {source.symbol} is {length}.\n")

    # RSI calculation
    df = pd.DataFrame(rows)
    rsi = ta.rsi(df[StockQuotes.AdjClose], length = 14)

    dates = rows[StockQuotes.DateTime]
    price = rows[StockQuotes.AdjClose]

    length = len(rows)

    higher_indication = list(repeat(upper_band, length))
    lower_indication = list(repeat(lower_band, length))

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_width=[0.3, 0.7],
                        specs=[[{"secondary_y": False}],
                            [{"secondary_y": False}]])

    fig.add_trace(
        go.Scatter(x=dates, y=price, name="AdjClose"),
        secondary_y=False,
    )

    fig.append_trace(go.Scatter(x=dates, y=higher_indication, mode='lines', name="Higher Band"), row=2, col=1)
    fig.append_trace(go.Scatter(x=dates, y=lower_indication, mode='lines', name="Lower Band"), row=2, col=1)
    fig.append_trace(go.Scatter(x=dates, y=rsi, mode='lines', name="RSI"), row=2, col=1)

    ######################
    # Write the chart
    ######################

    update_layout(fig, f"RSI example chart for {source.symbol}", length)

    new_file = show_image(fig)

    print(f"{new_file} is written.")
