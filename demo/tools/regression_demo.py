"""Demonstration of using Regression AI data tool.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
import torch

import numpy as np

from data.futils import update_layout

from tools.regression import Regression, RegressionData, LSTM

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from data.futils import show_image
from data.fdata import FdataError
from data.fvalues import Quotes
from tools.base import ToolError

from data.yf import YF

import sys

window_size = 20  # Sliding window size
forecast_size = 10  # Number of periods to forecast
output_size = 1  # Number of features to forecast

threshold = 756  # Quotes number threshold for calculation

if __name__ == "__main__":
    # Get quotes
    try:
        # Fetch quotes if there are less than a threshold number of records in the database for the specified timespan.
        source = YF(symbol="SPY", first_date="2020-01-01", last_date="2023-01-01")
        rows, num = source.fetch_if_none(threshold)
    except FdataError as e:
        sys.exit(e)

    length = len(rows)

    if num > 0:
        print(f"Fetched {num} quotes for {source.symbol}. Total number of quotes used is {length}.")
    else:
        print(f"No need to fetch quotes for {source.symbol}. There are {length} quotes in the database and it is >= the threshold level of {threshold}.")

    # Calculate LSTM
    data = RegressionData(rows,  # TODO HIGH Add utils function to convert it to a labelled numpy array
                          window_size=window_size,
                          forecast_size=forecast_size,
                          in_features=[Quotes.AdjClose, Quotes.Volume],
                          output_size=output_size,
                         )

    model = LSTM(data=data)
    loss = torch.nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=0.01)

    reg = Regression(model=model,
                     loss=loss,
                     optimizer=optimizer
                    )

    try:
        reg.calculate()
        est_data = reg.get_results()
    except ToolError as e:
        sys.exit(f"Can't calculate/forecast LSTM: {e}")

    # Shift test data by windows size + test data length
    forecasted = np.empty((data.get_test_size() - forecast_size, output_size))
    forecasted[:] = np.nan

    # start the chart from the last known point
    forecasted[-1] = rows[len(rows) - forecast_size - 1][Quotes.AdjClose]

    forecasted = np.append(forecasted, est_data, axis = 0)

    # Prepare the chart

    dates = [row[Quotes.DateTime] for row in rows][data.get_train_size():]
    quotes = [row[Quotes.AdjClose] for row in rows][data.get_train_size():]
    volume = [row[Quotes.Volume] for row in rows][data.get_train_size():]
    lstm_quotes = [row[0] for row in forecasted]

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_width=[0.4, 0.6],
                        specs=[[{"secondary_y": False}],
                            [{"secondary_y": False}]])

    fig.add_trace(
        go.Scatter(x=dates, y=quotes, name="AdjClose"), row=1, col=1, secondary_y=False,
    )

    fig.add_trace(
        go.Scatter(x=dates, y=lstm_quotes, name="LSTM"), row=1, col=1, secondary_y=False,
    )

    fig.add_trace(go.Scatter(x=dates, y=volume, mode='lines', name="Volume"), row=2, col=1, secondary_y=False)

    # Write the chart

    update_layout(fig, f"LSTM example chart for {source.symbol}", data.get_test_size())

    new_file = show_image(fig)

    print(f"{new_file} is written.")
