"""Volume Occillator demonstration.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""

from tools.vo import VO
from tools.vo import VOData

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from data.futils import update_layout
from data.futils import show_image

from data.fdata import FdataError
from data.fvalues import StockQuotes
from tools.base import ToolError

from data.yf import YF

import sys

long_period = 28
short_period = 14

threshold = 525  # Quotes number threshold for calculation

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

    # VO calculation
    vo = VO(long_period, short_period, rows, StockQuotes.Volume)

    try:
        vo.calculate()
    except ToolError as e:
        sys.exit(f"Can't calculate VO: {e}")

    results = vo.get_results()
    length = len(results)

    dates = [row[StockQuotes.DateTime] for row in rows]
    price = [row[StockQuotes.AdjClose] for row in rows]

    vo_values = [row[VOData.Value] for row in results]
    long_sma = [row[VOData.LongSMAValue] for row in results]
    short_sma = [row[VOData.ShortSMAValue] for row in results]
    volume = [row[StockQuotes.Volume] for row in rows]

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

    update_layout(fig, f"VO example chart for {source.symbol}", length)

    new_file = show_image(fig)

    print(f"{new_file} is written.")
