"""Demonstration of a training of a LSTM model for stock price estimation.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

from data.futils import check_date
from data.futils import write_model
from data.fdata import Query, ReadOnlyData
from data.fdata import FdataError
from data.fvalues import Rows

import sys

#import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import numpy as np
import pandas as pd

from keras.layers import Dense
from keras.layers import Dropout
from keras.layers import LSTM
from keras.models import Sequential

if __name__ == "__main__":
    name = "LSTM"

    # Get data for learning

    query = Query()
    query.symbol = "SPY"
    query.db_connect()

    query.first_date = check_date("2018-01-01")[1]
    query.last_date = check_date("2021-01-01")[1]

    data = ReadOnlyData(query)

    try:
        rows = data.get_quotes()
        query.db_close()
    except FdataError as e:
        print(e)
        sys.exit(2)

    length = len(rows)

    print(f"Obtained {length} rows.")

    if length == 0:
        print(f"Make sure that the symbol {query.symbol} is fetched and present in the {query.db_name} databases.")
        sys.exit(2)

    # Prepare a DataFrame of close prices

    close = [row[Rows.AdjClose] for row in rows]

    df = pd.DataFrame()
    df['Close'] = close

    # Scale the DataFrame

    scaler = MinMaxScaler(feature_range=(0,1))
    df = scaler.fit_transform(np.array(df).reshape(-1,1))

    # Prepare the data structure for learning

    timesteps = 60

    x = []
    y = []

    for i in range(length-timesteps-1):
        a = df[i:(i+timesteps), 0]
        x.append(a)
        y.append(df[i + timesteps, 0])

    x = np.array(x)
    y = np.array(y)
    x = x.reshape(x.shape[0], x.shape[1])

    # Train the model and add dropouts to decrease overfitting
    dropout = 0.3

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
