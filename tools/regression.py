"""Regression AI data tool implementation.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
import numpy as np

import torch
import torch.nn as nn

from sklearn.preprocessing import StandardScaler

from tools.base import BaseTool
from tools.base import ToolError

# TODO MID Implement a basic screener which uses regression forecast with periodic retraining of the models.
class LSTM(nn.Module):
    """
        Class to represent an LSTM model.
    """
    def __init__(self, data, hidden_size=2, num_layers=1):
        """
            Initialize the instance of LSTM model.

            Args:
                hidden_size(int): the number of features in the hidden state.
                num_layes(int): the number of recurrent layers.
        """
        super().__init__()

        self.data = data

        self.lstm = nn.LSTM(input_size=data.input_size, hidden_size=hidden_size, num_layers=num_layers, batch_first=True)
        self.linear = nn.Linear(in_features=data.input_size, out_features=data.output_size)

    def rows(self):
        """
            Get the raw data for calculations.

            Returns:
                list: raw data for calculations.
        """
        return self.data.get_data()

    def forward(self, x):
        x, _ = self.lstm(x)
        x = self.linear(x)
        x = x[:, self.data.window_size - self.data.forecast_size:, :]  # Trim results to the forecast size

        return x

class RegressionData():
    """
        The class to represent the data used in regression learning/forecasting.
    """
    def __init__(self,
                 rows,
                 window_size,
                 forecast_size,
                 in_features=None,
                 output_size=1,
                 epochs=1000,
                 auto_train=False,
                 train_threshold=None):
        """
            Initialized the data used in regression calculations.

            Args:
                rows(list): data for calculation.
                window_size(int): sliding window size.
                forecast_size(int): number or periods to be forecasted.
                in_features(list): features for model training (like [Quotes.AdjClose, Quotes.Volume]). All available if None.
                out_features_num(int): number of out features (the first num of features in in_features).
                epochs(int): number of epochs.
                auto_train(bool): indicates if a training should continue automatically when new data has arrived (window_size + forecast_size).
                train_threshold(int): threshold value of new data arrived to perform the additional training
        """
        if window_size <= 0 or forecast_size <= 0:
            raise ToolError(f"Sliding window size {window_size} of forecast size {forecast_size} should be bigger than 0.")

        if forecast_size > window_size:
            raise ToolError(f"Sliding window size {window_size} is less that forecast size {forecast_size}.")

        self.window_size = window_size
        self.forecast_size = forecast_size

        if in_features is None:
            self.input_size = len(rows[0])
        else:
            self.input_size = len(in_features)

        self.in_features = in_features

        self.output_size = output_size

        if output_size > self.input_size:
            raise ToolError(f"The requested number of out_features {output_size} is bigger than the number of in_features {self.input_size}.")

        # TODO MID prevent rows from being serialized
        self._rows = None
        self.set_data(rows)

        min_len = window_size + forecast_size
        if len(rows) < min_len:
            raise ToolError(f"Number on input rows is {len(rows)} but at least {min_len} rows are required.")

        self.epochs = None
        self.set_epochs(epochs)

        self.auto_train = auto_train
        self.train_counter = 0  # Counter of appended rows to start training automatically

        # Set the train threshold
        self.train_threshold = None
        min_train_threhold = window_size + forecast_size

        if train_threshold is None:
            self.train_threshold = min_train_threhold
        else:
            if train_threshold < min_train_threhold:
                raise ToolError(f"Minimum train threshold is {min_train_threhold} but {train_threshold} is specified.")

            self.train_threshold = train_threshold

        self.reg = None  # Parent Regression instance

    def set_epochs(self, epochs):
        """
            Set the number of epochs for the current cycle of learning.

            Args:
                epochs(int): the number of epochs.
        """
        if epochs <= 0:
            raise(f"Epochs {epochs} can't be <= 0.")

        self.epochs = epochs

    def set_data(self, rows, epochs=None):
        """
            Set the new data.

            Args:
                rows(list): the new data to set
                epochs(int): the new number of epochs.
        """
        if self.window_size > len(rows):
            raise ToolError(f"Sliding window size {self.window_size} is bigger than the total data provided {len(rows)}.")

        self._rows = rows

        if epochs is not None:
            self.set_epochs(epochs)

    def append_data(self, rows, epochs=None):
        """
            Append the rows of data to the main dataset. Used with streaming quotes.
            This method will invoke automatic training if enough data comes and auto_train flag is on.

            Args:
                rows(list): data to append to the main dataset.
                epochs(int): new number of epochs. As append data is normally not called standalone for learning purposes, use with caution.
        """
        if isinstance(self._rows, list):
            self._rows.extend(rows)
        elif isinstance(self._rows, np.ndarray):
            # TODO MID Implement limit for stored data
            self._rows = np.append(self._rows, np.array(rows), axis=0)

        if epochs is not None:
            self.set_epochs(epochs)

        if self.auto_train:
            self.train_counter += len(rows)

            if self.train_counter >= self.train_threshold:
                if self.reg is None:
                    raise ToolError("Can't perform auto calculation because data instance was not assigned to any Regression instance.")

                calculation_length = self.train_counter
                self.train_counter = 0

                return self.reg.calculate(calculation_length)

    def get_data(self):
        """
            Get the raw data used for calculations.

            Returns:
                list: the raw data
        """
        return self._rows

class Regression(BaseTool):
    """
        Regression class impementation.
    """
    def __init__(self,
                 model,
                 loss=None,
                 optimizer=None,
                 verbosity=True,
                 offset=None):
        """
            Initialize regression implementation class.

            Args:
                model(nn.Module): instance for learning/forecasting.
                loss(torch.nn.modules.loss): loss function.
                optimizer(torch.optim): optimizer.
                verbosity(bool): indicates if additional output is needed (loss, rmse).
                offset(int): offset for calculation.
        """
        super().__init__(self, verbosity=verbosity, offset=offset)

        if model.training and optimizer is None:
            raise ToolError("Optimizer should be specified if model is not trained.")

        if model.training and loss is None:
            raise ToolError("Loss function instance should be specified if learning is not performed yet.")
        
        self._model = model
        self._loss = loss
        self._optimizer = optimizer

        self._model.data.reg = self

    def get_results(self):
        """
            Get the forecasting results.

            Raises:
                ToolError: calculation is not performed.

            list: results of the calculation.
        """
        if self._model.training:
            raise ToolError("Can't get forecasting results as the calculation is not performed.")

        # Prepare the data for forecasting
        length = len(self._model.rows())
        testing_data = self._model.rows()[length - self._model.data.window_size:]

        if self._model.data.in_features is not None:
            arr = np.zeros((self._model.data.window_size + self._model.data.forecast_size, self._model.data.input_size))

            for i in range(self._model.data.input_size):
                feature = self._model.data.in_features[i]
                arr[:, i] = [row[feature] for row in testing_data] + [0] * self._model.data.forecast_size
        else:
            zeros = np.zeros((self._model.data.forecast_size, self._model.data.input_size))
            arr = np.append(testing_data, zeros, axis=0)

        # Scale data to make an estimation
        sc_test = StandardScaler()
        data = sc_test.fit_transform(arr)

        # Scaler for resulting data
        sc_result = StandardScaler()
        sc_result.fit_transform(arr[:, :self._model.data.output_size])

        x, _ = self.get_sliding_windows(data)
        x = torch.Tensor(x)

        forecast_data = self._model(x).data.numpy()[-1]

        self._results = sc_result.inverse_transform(forecast_data)

        return super().get_results()

    def get_model(self):
        """
            Get the model instance.

            Returns:
                nn.Module: model used in the data tool.
        """
        return self._model

    def get_sliding_windows(self, data):
        x = []
        y = []

        for i in range(len(data) - self._model.data.window_size - self._model.data.forecast_size + 1):
            temp_x = data[i:i + self._model.data.window_size]
            temp_y = data[i + self._model.data.forecast_size: i + self._model.data.window_size + self._model.data.forecast_size]
            x.append(temp_x)
            y.append(temp_y)

        return np.array(x), np.array(y)

    def calculate(self, num=None):
        """
            Perform the calculation based on the provided data and model.

            Args:
                num(int): the number of rows use in learning.

            Returns:
                (float, float): final loss/rmse.
        """
        # Prepare the data for learning
        if num is None:
            training_data = self._model.rows()
        else:
            length = len(self._model.rows())
            training_data = self._model.rows()[length - num:]

        if self._model.data.in_features is not None:
            arr = np.zeros((len(training_data), self._model.data.input_size))

            for i in range(self._model.data.input_size):
                feature = self._model.data.in_features[i]
                arr[:, i] = [row[feature] for row in training_data]
        else:
            arr = self._model.data.get_data()

        # Scale data for learning
        sc_learn = StandardScaler()
        data = sc_learn.fit_transform(arr)

        x, y = self.get_sliding_windows(data)

        x = torch.Tensor(x)
        y = torch.Tensor(y[:, :self._model.data.forecast_size, :self._model.data.output_size])

        # Train the model
        for epoch in range(self._model.data.epochs + 1):
            result = self._model(x)
            self._optimizer.zero_grad()

            # Loss
            loss_fn = self._loss(result, y)
            loss_fn.backward()

            # RMSE
            rmse = np.sqrt(loss_fn.detach().numpy())
            
            self._optimizer.step()

            if self._verbosity and (epoch % (int(self._model.data.epochs / 10))) == 0:
                print("Epoch: %d, loss: %1.5f, RMSE %.4f" % (epoch, loss_fn.item(), rmse))

        self._model.eval()

        return (float(loss_fn.item()), float(rmse))
