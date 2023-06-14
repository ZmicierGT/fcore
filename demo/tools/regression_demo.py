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

from data.futils import show_image, get_labelled_ndarray
from data.fdata import FdataError
from data.fvalues import Quotes
from tools.base import ToolError

from data.yf import YF

from time import perf_counter

import sys

window_size = 10  # Sliding window size
forecast_size = 5  # Number of periods to forecast
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

    # Optionally convert 2d list to a labelled ndarray.
    rows = get_labelled_ndarray(rows)

    # Split data to different datasets to demonstrate learning/forecasting in several stages.
    min_len = window_size + forecast_size
    split_len = len(rows) - min_len * 2

    rows1 = rows[:split_len]  # First batch of data for learning
    rows2 = rows[split_len:len(rows) - forecast_size]  # Next batch of data for learning. Remaining data won't ever be used in the model.

    # Calculate LSTM
    data = RegressionData(rows1,
                          window_size=window_size,
                          forecast_size=forecast_size,
                          in_features=[Quotes.AdjClose, Quotes.Volume],
                          output_size=output_size
                         )

    model = LSTM(data=data)
    loss = torch.nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=0.01)

    reg = Regression(model=model,
                     loss=loss,
                     optimizer=optimizer
                    )

    try:
        print("\nTraining using the initial (big) dataset: ")
        reg.calculate()

        print("\nTraining using the additional (small) dataset: ")
        reg.set_verbosity(False)
        before = perf_counter()

        data.auto_train = True  # Automatically continue training if enough data arrives
        result = data.append_data(rows2, epochs=60)

        if result is not None:
            loss, rmse = result

            total = (perf_counter() - before) * 1000
            print(f"Training took {round(total, 4)} ms, final loss is {round(loss, 5)}, rmse is {round(rmse, 4)}.\n")

        # Perform the forecasting
        before_forecast = perf_counter()
        est_data = reg.get_results()
        total_forecast = (perf_counter() - before_forecast) * 1000

        print(f"Forecasting took in total: {round(total_forecast, 4)} ms.\n")
    except ToolError as e:
        sys.exit(f"Can't calculate/forecast LSTM: {e}")

    # Shift test data by windows size + test data length
    forecasted = np.empty((window_size, output_size))
    forecasted[:] = np.nan

    # start the chart from the last known point
    forecasted[-1] = rows[len(rows) - forecast_size - 1][Quotes.AdjClose]

    forecasted = np.append(forecasted, est_data, axis=0)

    # Prepare the chart

    rows = rows[len(rows) - window_size - forecast_size:]

    dates = [row[Quotes.DateTime] for row in rows]
    quotes = [row[Quotes.AdjClose] for row in rows]
    volume = [row[Quotes.Volume] for row in rows]

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

    update_layout(fig, f"LSTM example chart for {source.symbol}", window_size + forecast_size)

    new_file = show_image(fig)

    print(f"{new_file} is written.")
