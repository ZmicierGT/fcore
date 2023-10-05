"""Regression AI screener implementation.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from screener.base import BaseScr
from screener.base import ScrError

from data.fvalues import StockQuotes

from tools.regression import Regression, RegressionData, LSTM

import torch

class RegScr(BaseScr):
    """
        Regression AI screener implementation class.
    """
    def __init__(self,
                 window_size,
                 epochs,
                 forecast_size,
                 test_length,
                 max_rows,
                 **kwargs):
        """
            Initialize regression AI screener class.

            Args:
                window_size(int): Sliding window size
                epochs(int): number of epochs.
                forecast_size(int): Size of periods to forecast. Decision will be taken based on the last predicted period.
                test_length(int): number of periods to perform the test. Be sure that the last forecast_size elements
                                  vere not used while learning. By default (the minimum value) is window_size + forecast_size
                max_rows(int): The maxumum number of rows to store. Used to prevent excessive dataset growth.
        """
        super().__init__(**kwargs)

        self._window_size = window_size
        self._epochs = epochs
        self._forecast_size = forecast_size
        self._test_length = test_length
        self._max_rows = max_rows

    def calculate(self):
        """
            Perform calculation for regression AI demo.

            Raises:
                ScrError: not enough data to make a calculation.
        """
        self._results = [] 

        for symbol in self.get_symbols():
            rows = symbol.get_data(self.get_period(), self.get_init_status())

            if self.get_init_status() is False:
                min_len = self._window_size + self._forecast_size

                if len(rows) < min_len:
                    raise ScrError(f"Not enough quotes: {len(rows)} < {min_len}")

                # Need to initialize regression instances for each symbol
                data = RegressionData(rows=rows,
                                      window_size=self._window_size,
                                      epochs=self._epochs,
                                      forecast_size=self._forecast_size,
                                      in_features=[StockQuotes.AdjClose, StockQuotes.Volume],
                                      output_size=1,
                                      test_length=self._test_length,
                                      auto_train=True,
                                      max_rows=self._max_rows)

                model = LSTM(data=data)
                loss = torch.nn.MSELoss()
                optimizer = torch.optim.Adam(model.parameters(), lr=0.01)

                symbol.reg = Regression(model=model,
                                        loss=loss,
                                        optimizer=optimizer,
                                        verbosity=self._verbosity)

                # Perform the initial learning
                self.log(f"Perform initial model training for {symbol.get_title()}")
                symbol.reg.calculate()

                symbol.reg._model.data.set_epochs(30)  # Set less epochs for appending learning
            else:
                # Here we add the whole obtained data as the period is one. If the period is bigger,
                # we need to add the last row only.
                symbol.reg._model.data.append_data(rows=rows)

            signal_buy = False
            signal_sell = False

            # Perform a forecasting
            est_data = symbol.reg.get_results()

            current = rows[StockQuotes.Close][-1]
            forecasted = est_data[-1][0]

            if current < forecasted:
                signal_buy = True

            if current > forecasted:
                signal_sell = True

            result = [symbol.get_title(),
                      symbol.get_max_datetime(),
                      symbol.get_quotes_num(),
                      [current, forecasted],
                      [signal_buy, signal_sell]]

            self._results.append(result)
