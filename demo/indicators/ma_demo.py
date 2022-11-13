"""Moving average demonstration using pandas_ta.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from data.futils import update_layout
from data.futils import write_image
from data.futils import check_date

from data.fdata import Query, ReadOnlyData
from data.fdata import FdataError
from data.fvalues import Rows

import sys

import pandas as pd
import pandas_ta as ta

if __name__ == "__main__":
    query = Query()
    query.symbol = "SPY"
    query.first_date = check_date("2020-10-01")[1]
    query.db_connect()

    data = ReadOnlyData(query)

    try:
        rows = data.get_quotes()
        query.db_close()
    except FdataError as e:
        print(e)
        sys.exit(2)

    print(f"Obtained {len(rows)} rows.")

    if len(rows) == 0:
        print(f"Make sure that the symbol {query.symbol} is fetched and presents in the {query.db_name} database.")
        sys.exit(2)

    length = len(rows)

    df = pd.DataFrame(rows)

    sma = ta.sma(df[Rows.AdjClose], length = 50)
    ema = ta.ema(df[Rows.AdjClose], length = 50)

    dates = [row[Rows.DateTime] for row in rows]
    price = [row[Rows.AdjClose] for row in rows]

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(
        go.Scatter(x=dates, y=price, name="AdjClose"),
        secondary_y=False,
    )

    fig.add_trace(
        go.Scatter(x=dates, y=sma, name="SMA"),
        secondary_y=True,
    )

    fig.add_trace(
        go.Scatter(x=dates, y=ema, name="EMA"),
        secondary_y=True,
    )

    ######################
    # Write the chart
    ######################

    update_layout(fig, f"SMA/EMA example chart for {query.symbol}", length)

    fig.update_yaxes(title_text="<b>Price</b>", secondary_y=False)
    fig.update_yaxes(title_text="<b>SMA/EMA</b>", secondary_y=True)

    new_file = write_image(fig)

    print(f"{new_file} is written.")
