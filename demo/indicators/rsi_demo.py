"""RSI demonstation using pandas_ta.

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
from itertools import repeat

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
        print(f"Make sure that the symbol {query.symbol} is fetched and present in the {query.db_name} databases.")
        sys.exit(2)

    period = 14
    upper_band = 70
    lower_band = 30

    df = pd.DataFrame(rows)
    rsi = ta.rsi(df[Rows.AdjClose], length = 14)

    dates = [row[Rows.DateTime] for row in rows]
    price = [row[Rows.AdjClose] for row in rows]

    length = len(rows)

    higher_indication = list(repeat(upper_band, length))
    lower_indication = list(repeat(lower_band, length))

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_width=[0.3, 0.7],
                        specs=[[{"secondary_y": True}],
                            [{"secondary_y": True}]])

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
