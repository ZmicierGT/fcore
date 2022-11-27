"""Demonstration of using AI as a technical indicator.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

from data.futils import update_layout

from indicators.lstm import LSTM
from indicators.lstm import LSTMData

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from data.futils import write_image
from data.fdata import FdataError
from data.fvalues import Quotes
from indicators.base import IndicatorError

from data.yf import YFError, YFQuery, YF

import sys

period = 60
threshold = 546  # Quotes number threshold for calculation

if __name__ == "__main__":
    # Get quotes
    try:
        # Fetch quotes if there are less than a threshold number of records in the database for the specified timespan.
        query = YFQuery(symbol="SPY", first_date="2020-09-01", last_date="2022-11-1")
        rows, num = YF(query).fetch_if_none(threshold)
    except (YFError, FdataError) as e:
        print(e)
        sys.exit(2)

    length = len(rows)

    if num > 0:
        print(f"Fetched {num} quotes for {query.symbol}. Total number of quotes used is {length}.")
    else:
        print(f"No need to fetch quotes for {query.symbol}. There are {length} quotes in the database and it is >= the threshold level of {threshold}.")

    # Calculate LSTM
    lstm = LSTM(rows, period, Quotes.AdjClose, 'models/LSTM_1')

    try:
        lstm.calculate()
    except IndicatorError as e:
        print(f"Can't calculate LSTM. Likely you need to train the model at first by launching lstm_learn.py. {e}")
        sys.exit(2)

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

    update_layout(fig, f"LSTM example chart for {query.symbol}", length)

    new_file = write_image(fig)

    print(f"{new_file} is written.")
