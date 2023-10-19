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
from data.fvalues import StockQuotes
from tools.base import ToolError

from data.yf import YF

from time import perf_counter

import sys

window_size = 100  # Sliding window size
forecast_size = 50  # Number of periods to forecast
test_length = 150  # Length of data to perform forecasting. Make sure that the last forecast_size is never seen during learning.
output_size = 1  # Number of features to forecast

epochs = 1000

symbol = 'SPY'

if __name__ == "__main__":
    # Get quotes
    try:
        warning = "WARNING! Using yfinance data for the demonstration.\n" +\
                  "Always keep yfinance up to date ( pip install yfinance --upgrade ) and use quotes obtained from this " +\
                  "datasource only for demonstation purposes!\n"
        print(warning)

        rows = YF(symbol=symbol, first_date="2005-11-01", last_date="2008-11-01").get()
    except FdataError as e:
        sys.exit(e)

    length = len(rows)

    print(f"The total number of quotes used for {symbol} is {length}.\n")

    # Split data to different datasets to demonstrate learning/forecasting in several stages.
    split_len = len(rows) - test_length - forecast_size

    rows1 = rows[:split_len]  # First batch of data for learning
    rows2 = rows[split_len:len(rows) - forecast_size]  # Next batch of data for learning. Remaining data won't ever be used in the model.

    # Calculate LSTM
    data = RegressionData(rows1,
                          epochs=epochs,
                          window_size=window_size,
                          forecast_size=forecast_size,
                          in_features=[StockQuotes.AdjClose, StockQuotes.Volume],
                          output_size=output_size,
                          test_length=test_length
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
        result = data.append_data(rows2, epochs=30)

        if result is not None:
            loss, rmse = result

            total = (perf_counter() - before) * 1000
            print(f"Training took {round(total, 4)} ms, final loss is {round(loss, 6)}, rmse is {round(rmse, 4)}.\n")
        else:
            print(f"The training was not triggered. Maybe length {len(rows2)} of the additional dataset was too small?\n")

        ######################################################################
        # Test various ways of serialization. Not used in the demo by default.
        ######################################################################

        # model_file = 'model.pt'
        # reg_file = 'reg.fc'

        # torch.save(model, model_file)
        # model = torch.load(model_file)
        # model.eval()

        # import pickle
        # file = open(reg_file, 'wb')
        # pickle.dump(reg, file)
        # file.close()

        # file = open(reg_file, 'rb')
        # reg = pickle.load(file)
        # file.close()

        # import os
        # os.remove(model_file)
        # os.remove(reg_file)

        # Perform the forecasting
        before_forecast = perf_counter()
        est_data = reg.get_results()
        total_forecast = (perf_counter() - before_forecast) * 1000

        print(f"Forecasting took in total: {round(total_forecast, 4)} ms.\n")
    except ToolError as e:
        sys.exit(f"Can't calculate/forecast LSTM: {e}")

    # Shift test data by windows size + test data length
    forecasted = np.empty((test_length, output_size))
    forecasted[:] = np.nan

    # start the chart from the last known point
    forecasted[-1] = rows[len(rows) - forecast_size - 1][StockQuotes.AdjClose]

    forecasted = np.append(forecasted, est_data, axis=0)

    # Prepare the chart

    rows = rows[len(rows) - test_length - forecast_size:]

    dates = rows[StockQuotes.DateTime]
    quotes = rows[StockQuotes.AdjClose]
    volume = rows[StockQuotes.Volume]

    lstm_quotes = forecasted[:, 0]

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_width=[0.4, 0.6],
                        specs=[[{"secondary_y": False}],
                            [{"secondary_y": False}]])

    fig.add_trace(
        go.Scatter(x=dates, y=quotes, name="AdjClose"), row=1, col=1, secondary_y=False,
    )

    fig.add_trace(
        go.Scatter(x=dates, y=lstm_quotes, name="LSTM"), row=1, col=1, secondary_y=False,
    )

    fig.add_trace(go.Scatter(x=dates, y=volume, fill='tozeroy', name="Volume"), row=2, col=1, secondary_y=False)

    # Write the chart

    update_layout(fig, f"LSTM example chart for {symbol}", test_length + forecast_size)

    new_file = show_image(fig)

    print(f"{new_file} is written.")
