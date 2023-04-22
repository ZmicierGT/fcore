"""Classifier of MA/Quote cross signals (true/false) according to the trained model.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from tools.base import ToolError
from tools.classifier import Classifier

from data.fvalues import Quotes

import pandas as pd
import pandas_ta as ta

import numpy as np

class MAClassifier(Classifier):
    """
        MA/Price signals classifier (true/false) impementation.
    """
    def __init__(self,
                 period,
                 is_simple=True,
                 **kwargs
                ):
        """
            Initialize MA Classifier implementation class.

            Args:
                period(int): long period for MA calculation (must match the period used for model training).
                is_simple(bool): indicates if SMA or EMA is used.
                rows(list): quotes for estimation.
                model_buy(): model to estimate buy signals.
                model_sell(): model to estimate sell signals.
                data_to_learn([array]) data to train the models. Either models or data to learn need to be specified.
                is_simple(bool): indicated is SMA or EMA should be used (must match the MA type used for model training).
                true_ratio(float): ratio when signal is considered as true in cycle_num. For example, if true_ratio is 0.03 and cycle_num is 5,
                                then the signal will be considered as true if there was a 0.03 change in ma/quote ratio in the following 5 cycles
                                after getting the signal from MA.
                cycle_num(int): number of cycles to reach to true_ratio to consider that the signal is true.
                algorithm(Algorithm): algorithm used for learning (from Algorithm enum).
                classify(bool): indicates if classification should be performed.
                probabilities(bool): determines if probabilities should be calculated.
                offset(int): offset for calculation.

            Raises:
                ToolError: No model provided to make the estimation.
        """
        super().__init__(**kwargs)

        if self._use_buy is False or self._use_sell is False:
            raise ToolError("Both buy and sell signals are required by this tool.")

        self._period = period
        self._is_simple = is_simple

    def prepare(self, rows=None):
        """
            Get the DataFrame for learning/estimation.

            Returns:
                DataFrame: data ready for learning/estimation
        """
        # DataFrame for the current symbol
        if rows == None:
            df = pd.DataFrame(self._rows)
        else:
            df = pd.DataFrame(rows)

        # Calculate moving average
        if self._is_simple:
            ma = ta.sma(df[Quotes.AdjClose], length = self._period)
        else:
            ma = ta.ema(df[Quotes.AdjClose], length = self._period)

        # Calculate PVO
        pvo = ta.pvo(df[Quotes.Volume])

        df['ma'] = ma
        df['pvo'] = pvo.iloc[:, 0]
        df['diff'] = ((df[Quotes.AdjClose] - df['ma']) / df[Quotes.AdjClose])
        df['hilo-diff'] = ((df[Quotes.High] - df[Quotes.Low]) / df[Quotes.High])

        self._data_to_est = ['pvo', 'diff', 'hilo-diff']  # Columns to make estimations
        self._data_to_report = self._data_to_est + ['ma']  # Columns for reporting

        # Get rid of the values where MA is not calculated because they are useless for learning.
        df = df[self._period-1:]
        df = df.reset_index().drop(['index'], axis=1)

        # Fill nan values (if any) with mean values
        if df[self._data_to_est].isnull().values.any():
            for key in self._data_to_est:
                df[key].fillna(value=df[key].mean(), inplace=True)

        return df

    def find_buy_signals(self, df):
        """
            Find buy signals in the data.

            Args:
                df(DataFrame): data to find signals.
        """
        curr_trend = df['diff'] > 0
        prev_trend = df['diff'].shift() > 0

        df['buy'] = np.where(curr_trend & (prev_trend == False) & (df.index != 0), 1, 0)

    def find_sell_signals(self, df):
        """
            Find sell signals in the data.

            Args:
                df(DataFrame): data to find signals.
        """
        curr_trend = df['diff'] > 0
        prev_trend = df['diff'].shift() > 0

        df['sell'] = np.where((curr_trend == False) & prev_trend & (df.index != 0), 1, 0)

    def get_buy_condition(self, df):
        """
            Get buy condiiton to check signals.

            Args:
                df(DataFrame): data with signals to check.

            Returns:
                TimeSeries: signals
        """
        return df['diff'].shift(-abs(self._cycle_num)) >= self._true_ratio

    def get_sell_condition(self, df):
        """
            Get sell condiiton to check signals.

            Args:
                df(DataFrame): data with signals to check.

            Returns:
                TimeSeries: signals
        """
        return df['diff'].shift(-abs(self._cycle_num)) <= -abs(self._true_ratio)
