"""Demonstration of a training of a LSTM model for stock price estimation.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

from data.futils import write_model
from data.fdata import FdataError
from data.fvalues import Quotes

from data.yf import YFError, YFQuery, YF

import sys

#import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import numpy as np
import pandas as pd

from keras.layers import Dense
from keras.layers import Dropout
from keras.layers import LSTM
from keras.models import Sequential

timesteps = 60
dropout = 0.3  # Add dropouts to decrease overfitting

threshold = 2500  # Quotes number threshold for learning

if __name__ == "__main__":
    name = "LSTM"

    # Get data for learning

    try:
        # Fetch quotes if there are less than a threshold number of records in the database for the specified timespan.
        query = YFQuery(symbol="SPY", first_date="2010-08-30", last_date="2020-08-30")
        rows, num = YF(query).fetch_if_none(threshold)
    except (YFError, FdataError) as e:
        print(e)
        sys.exit(2)

    length = len(rows)

    if num > 0:
        print(f"Fetched {num} quotes for {query.symbol}. Total number of quotes used is {length}.")
    else:
        print(f"No need to fetch quotes for {query.symbol}. There are {length} quotes in the database and it is beyond the threshold level of {threshold}.")

    # Prepare a DataFrame of close prices

    close = [row[Quotes.AdjClose] for row in rows]

    df = pd.DataFrame()
    df['Close'] = close

    # Scale the DataFrame

    scaler = MinMaxScaler(feature_range=(0,1))
    df = scaler.fit_transform(np.array(df).reshape(-1,1))

    # Prepare the data structure for learning

    x = []
    y = []

    for i in range(length-timesteps-1):
        a = df[i:(i+timesteps), 0]
        x.append(a)
        y.append(df[i + timesteps, 0])

    x = np.array(x)
    y = np.array(y)
    x = x.reshape(x.shape[0], x.shape[1])

    model = Sequential()
    model.add(LSTM(units=50, return_sequences=True, input_shape=(x.shape[1], 1)))
    model.add(Dropout(dropout))
    model.add(LSTM(units=50, return_sequences=True))
    model.add(Dropout(dropout))
    model.add(LSTM(units=50, return_sequences=True))
    model.add(Dropout(dropout))
    model.add(LSTM(units=50))
    model.add(Dropout(dropout))
    model.add(Dense(units=1))
    model.compile(optimizer='adam', loss='mean_squared_error')
    model.fit(x, y, epochs=100, batch_size=32)

    # Save the model

    new_dir = write_model(name, model)

    print(f"{new_dir} is written.")
