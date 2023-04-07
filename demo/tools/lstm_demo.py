"""Demonstration of using AI data tool.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

from data.futils import update_layout

from tools.lstm import LSTM
from tools.lstm import LSTMData

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from data.futils import show_image
from data.fdata import FdataError
from data.fvalues import Quotes
from tools.base import ToolError

from data.yf import YF

import sys

period = 60
threshold = 546  # Quotes number threshold for calculation

if __name__ == "__main__":
    # Get quotes
    try:
        # Fetch quotes if there are less than a threshold number of records in the database for the specified timespan.
        source = YF(symbol="SPY", first_date="2020-09-01", last_date="2022-11-1")
        rows, num = source.fetch_if_none(threshold)
    except FdataError as e:
        sys.exit(e)

    length = len(rows)

    if num > 0:
        print(f"Fetched {num} quotes for {source.symbol}. Total number of quotes used is {length}.")
    else:
        print(f"No need to fetch quotes for {source.symbol}. There are {length} quotes in the database and it is >= the threshold level of {threshold}.")

    # Calculate LSTM
    lstm = LSTM(rows, period, Quotes.AdjClose, 'models/LSTM_1')

    try:
        lstm.calculate()
    except ToolError as e:
        sys.exit(f"Can't calculate LSTM. Likely you need to train the model at first by launching lstm_learn.py. {e}")

    results = lstm.get_results()
    length = len(results)

    # Prepare the chart

    dates = [row[Quotes.DateTime] for row in rows]
    quotes = [row[Quotes.AdjClose] for row in rows]

    difference = [row[LSTMData.Difference] for row in results]
    lstm_values = [row[LSTMData.Value] for row in results]
    volume = [row[Quotes.Volume] for row in rows]

    fig = make_subplots(rows=3, cols=1, shared_xaxes=True, row_width=[0.2, 0.2, 0.6],
                        specs=[[{"secondary_y": False}],
                            [{"secondary_y": False}],
                            [{"secondary_y": False}]])

    fig.add_trace(
        go.Scatter(x=dates, y=quotes, name="AdjClose"), row=1, col=1, secondary_y=False,
    )

    fig.add_trace(
        go.Scatter(x=dates, y=lstm_values, name="LSTM"), row=1, col=1, secondary_y=False,
    )

    fig.add_trace(go.Scatter(x=dates, y=difference, fill='tozeroy', name="LSTM Difference"), row=2, col=1, secondary_y=False)
    fig.add_trace(go.Scatter(x=dates, y=volume, mode='lines', name="Volume"), row=3, col=1, secondary_y=False)  

    ######################
    # Write the chart
    ######################

    update_layout(fig, f"LSTM example chart for {source.symbol}", length)

    new_file = show_image(fig)

    print(f"{new_file} is written.")
