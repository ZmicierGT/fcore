"""Moving average vs. price cross backtesting strategy with fake signals distinguished by AI.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

from backtest.ma import MA
from backtest.base import BackTestError

from indicators.ma_classifier import MAClassifier
from indicators.base import IndicatorError

import pandas as pd
import numpy as np

class MAClassification(MA):
    """
        Moving average vs. price cross backtesting strategy implementation when signal is checked by AI.

        If MA goes above the price, it is a signal to buy. Otherwise, it is a signal to sell.
        AI determines if signals are true or false.
    """
    def __init__(self,
                 model_buy=None,
                 model_sell=None,
                 classifier=None,
                 **kwargs):
        """
            Initializes the MA Cross stragegy implementation with signal validity estimation.

            Args:
                model_buy(sklearn model): model to check buy signals.
                model_sell(sklearn model): model to check sell signals.
                classifier(MAClassifier): classifier for estimations.
                period(int): period of MA.
                is_simple(bool): indicates if SMA(True) or EMA(False) will be used.

            Raises:
                BackTestError: provided arguments are incorrect or no data for learning/estimation.
        """
        super().__init__(**kwargs)

        if ((model_buy == None) or (model_sell == None)) and (classifier == None):
            raise BackTestError("Either models or classifier object should be provided.")

        if classifier == None:
            self._model_buy = model_buy
            self._model_sell = model_sell
        else:
            self._model_buy, self._model_sell = classifier.get_models()

        self._ma_cls = classifier

    def do_tech_calculation(self, ex):
        """
            Perform technical calculation and model training for the strategy.

            Args:
                ex(BackTestOperations): Operations instance class.
        """
        # Initialize classifier if the instance is not provided.
        if self._ma_cls == None:
            self._ma_cls = MAClassifier(period=self._period,
                                        model_buy=self._model_buy,
                                        model_sell=self._model_sell,
                                        is_simple=self.is_simple())

        self._ma_cls.set_data(self.get_main_data().get_rows())

        try:
            self._ma_cls.calculate()
        except IndicatorError as e:
            raise BackTestError(e) from e

        # Set MA values used by base testing class. Add empty values at the beginning or the column.
        ma = pd.DataFrame([np.nan] * self._period)
        ex.append_tech(ma[0].append(self._ma_cls.get_results()['ma'], ignore_index=True))

        # Skip data when no MA is calculated.
        self.set_offset(self.get_offset() + self._period)

    def classifier(self):
        """
            Get the MA Classifier instance.

            Returns:
                MAClassifier: the instance used in calculations.
        """
        return self._ma_cls

    def signal_buy(self):
        """
            Determines if a signal to buy is true.

            Returns:
                True if the buy signal is true, False otherwise.
        """
        dt_str = self.exec().get_datetime_str()
        df = self.classifier().get_results()
        row = df.loc[df['dt'] == dt_str]

        if row.empty == False and row.iloc[0]['buy-signal'] == True:
            return True

        return False

    def signal_sell(self):
        """
            Determines if a signal to sell is true.

            Returns:
                True if the sell signal is true, False otherwise.
        """
        dt_str = self.exec().get_datetime_str()
        df = self.classifier().get_results()
        row = df.loc[df['dt'] == dt_str]

        if row.empty == False and row.iloc[0]['sell-signal'] == True:
            return True

        return False

    def any_signal(self):
        """
            Indicates if buy/sell signal was considered as true.

            Returns:
                True/False depending on signal verification.
        """

        return self.signal_buy() or self.signal_sell()
