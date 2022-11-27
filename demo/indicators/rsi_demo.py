"""RSI demonstation using pandas_ta.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from data.futils import update_layout
from data.futils import write_image

from data.fdata import FdataError
from data.fvalues import Quotes

from data.yf import YFError, YFQuery, YF

import sys
from itertools import repeat

import pandas as pd
import pandas_ta as ta

period = 14
upper_band = 70
lower_band = 30

threshold = 525  # Quotes number threshold for calculation

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

    # RSI calculation
    df = pd.DataFrame(rows)
    rsi = ta.rsi(df[Quotes.AdjClose], length = 14)

    dates = [row[Quotes.DateTime] for row in rows]
    price = [row[Quotes.AdjClose] for row in rows]

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

    update_layout(fig, f"RSI example chart for {query.symbol}", length)

    new_file = write_image(fig)

    print(f"{new_file} is written.")
