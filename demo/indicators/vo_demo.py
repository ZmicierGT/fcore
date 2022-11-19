"""Volume Occillator demonstration.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

from indicators.vo import VO
from indicators.vo import VORows

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from data.futils import update_layout
from data.futils import write_image
from data.futils import check_date

from data.fdata import Query, ReadOnlyData
from data.fdata import FdataError
from data.fvalues import Rows
from indicators.base import IndicatorError

import sys

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

    long_period = 28
    short_period = 14

    vo = VO(long_period, short_period, rows, Rows.Volume)

    try:
        vo.calculate()
    except IndicatorError as e:
        print(f"Can't calculate VO: {e}")
        sys.exit(2)

    results = vo.get_results()
    length = len(results)

    dates = [row[Rows.DateTime] for row in rows]
    price = [row[Rows.AdjClose] for row in rows]

    vo_values = [row[VORows.Value] for row in results]
    long_sma = [row[VORows.LongSMAValue] for row in results]
    short_sma = [row[VORows.ShortSMAValue] for row in results]
    volume = [row[Rows.Volume] for row in rows]

    fig = make_subplots(rows=3, cols=1, shared_xaxes=True, row_width=[0.2, 0.2, 0.6],
                        specs=[[{"secondary_y": False}],
                            [{"secondary_y": True}],
                            [{"secondary_y": False}]])

    fig.add_trace(
        go.Scatter(x=dates, y=price, name="AdjClose"),
        secondary_y=False,
    )

    fig.append_trace(go.Scatter(x=dates, y=long_sma, mode='lines', name=f"{long_period} SMA"), row=2, col=1)
    fig.append_trace(go.Scatter(x=dates, y=short_sma, mode='lines', name=f"{short_period} SMA"), row=2, col=1)
    fig.add_trace(go.Scatter(x=dates, y=vo_values, fill='tozeroy', name="VO"), row=2, col=1, secondary_y=True)

    fig.append_trace(go.Scatter(x=dates, y=volume, mode='lines', name="Volume"), row=3, col=1)  

    ######################
    # Write the chart
    ######################

    update_layout(fig, f"VO example chart for {query.symbol}", length)

    new_file = write_image(fig)

    print(f"{new_file} is written.")
