"""MACD demonstration using pandas_ta.

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

    length = len(rows)

    print(f"Obtained {length} rows.")

    if length == 0:
        print(f"Make sure that the symbol {query.symbol} is fetched and presents in the {query.db_name} database.")
        sys.exit(2)

    slow_period = 26
    fast_period = 12
    signal_period = 9

    df = pd.DataFrame(rows)

    macd = ta.macd(df[Rows.AdjClose], fast_period, slow_period, signal_period)

    dates = [row[Rows.DateTime] for row in rows]
    price = [row[Rows.AdjClose] for row in rows]

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

    update_layout(fig, f"MACD example chart for {query.symbol}", length)

    new_file = write_image(fig)

    print(f"{new_file} is written.")
