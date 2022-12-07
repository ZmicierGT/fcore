"""Volume Occillator demonstration.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

from indicators.vo import VO
from indicators.vo import VOData

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from data.futils import update_layout
from data.futils import show_image

from data.fdata import FdataError
from data.fvalues import Quotes
from indicators.base import IndicatorError

from data.yf import YFError, YFQuery, YF

import sys

long_period = 28
short_period = 14

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

    # VO calculation
    vo = VO(long_period, short_period, rows, Quotes.Volume)

    try:
        vo.calculate()
    except IndicatorError as e:
        print(f"Can't calculate VO: {e}")
        sys.exit(2)

    results = vo.get_results()
    length = len(results)

    dates = [row[Quotes.DateTime] for row in rows]
    price = [row[Quotes.AdjClose] for row in rows]

    vo_values = [row[VOData.Value] for row in results]
    long_sma = [row[VOData.LongSMAValue] for row in results]
    short_sma = [row[VOData.ShortSMAValue] for row in results]
    volume = [row[Quotes.Volume] for row in rows]

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

    new_file = show_image(fig)

    print(f"{new_file} is written.")
