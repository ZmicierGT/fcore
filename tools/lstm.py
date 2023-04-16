"""LSTM AI price estimation calculation. Compares the actual price with LSTM predicted data and provides the difference as a result.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""

from tools.base import BaseTool
from tools.base import ToolError

from enum import IntEnum

from keras.models import load_model

import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import numpy as np

class LSTMData(IntEnum):
    """
        Enum to represent LSTM results.
    """
    Difference = 0
    Value = 1

class LSTM(BaseTool):
    """
        LSTM impementation.
    """
    def __init__(self, rows, period, row_val, model_name, offset=None):
        """
            Initialize LSTM implementation class.

            Args:
                rows(list): quotes for calculation.
                period(int): timesteps used in learning.
                row_val(int): number of row with data to use in calculation.
                model_name(str): path to the model to use in calculation.
                offset(int): offset for calculation.
        """
        super().__init__(rows)

        self.__row_val = row_val
        self.__period = period
        self.__model_name = model_name

    def calculate(self):
        """
            Perform the calculation based on the provided data and model.

            Raises:
                ToolError: can't load the model.
        """
        # Load model
        try:
            model = load_model(self.__model_name)
        except OSError as e:
            raise ToolError(f"Can't load model {self.__model_name}: {e}") from e

        length = len(self._rows)

        # Prepare the DataFrame to work with the model

        close = [row[self.__row_val] for row in self._rows]

        df = pd.DataFrame()
        df['Close'] = close

        # Scale the DataFrame

        scaler = MinMaxScaler(feature_range=(0,1))
        df = scaler.fit_transform(np.array(df).reshape(-1,1))

        # Prepare the data for analysis

        x = []
        y = []

        for i in range(length-self.__period):
            a = df[i:(i+self.__period), 0]
            x.append(a)
            y.append(df[i + self.__period, 0])

        x = np.array(x)
        y = np.array(y)
        x = x.reshape(x.shape[0], x.shape[1])

        # Make predictions based on the model

        predictions = model.predict(x)
        predictions = scaler.inverse_transform(predictions)

        # Generate results

        for row in self._rows:
            index = self._rows.index(row)

            prediction = None
            difference = None
            value = row[self.__row_val]

            if index > self.__period:
                prediction = predictions[index - self.__period][0]
                difference = value - prediction

            result = [difference, prediction]

            self._results.append(result)
