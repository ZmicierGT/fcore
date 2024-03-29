"""Volume Occillator demonstration. It demonstrates a usage of a basic custom tool.

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
symbol = 'SPY'

if __name__ == "__main__":
    # Get quotes
    try:
        warning = "WARNING! Using yfinance data for the demonstration.\n" +\
                  "Always keep yfinance up to date ( pip install yfinance --upgrade ) and use quotes obtained from this " +\
                  "datasource only for demonstation purposes!\n"
        print(warning)

        rows = YF(symbol=symbol, first_date="2020-10-01", last_date="2022-11-1").get()
    except FdataError as e:
        sys.exit(e)

    length = len(rows)

    print(f"The total number of quotes used for {symbol} is {length}.\n")

    # VO calculation
    vo = VO(long_period, short_period, rows, StockQuotes.Volume)

    try:
        vo.calculate()
    except ToolError as e:
        sys.exit(f"Can't calculate VO: {e}")

    results = vo.get_results()
    length = len(results)

    dates = rows[StockQuotes.DateTime]
    price = rows[StockQuotes.AdjClose]
    volume = rows[StockQuotes.Volume]

    vo_values = results[VOData.Value]
    long_sma = results[VOData.LongSMAValue]
    short_sma = results[VOData.ShortSMAValue]

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

    fig.append_trace(go.Scatter(x=dates, y=volume, fill='tozeroy', name="Volume"), row=3, col=1)

    ######################
    # Write the chart
    ######################

    update_layout(fig, f"VO example chart for {symbol}", length)

    new_file = show_image(fig)

    print(f"{new_file} is written.")
