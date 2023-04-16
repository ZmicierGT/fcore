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

    def calculate(self):
        """
            Perform the calculation based on the provided data.

            Raises:
                ToolError: no data for test provided.
        """
        if self._rows == None:
            raise ToolError("No data for testing provided.")

        if self._classify is False:
            raise ToolError("Classification is disabled but it is required by this tool.")

        # Check if we need to train the model at first
        if self._model_buy == None or self._model_sell == None:
            self.learn()

        # DataFrame for the current symbol
        df = self.get_df()

        # Find signals which are needed to check

        curr_trend = df['diff'] > 0
        prev_trend = df['diff'].shift() > 0

        df['buy'] = np.where(curr_trend & (prev_trend == False) & (df.index != 0), 1, np.nan)
        df['sell'] = np.where((curr_trend == False) & prev_trend & (df.index != 0), 1, np.nan)

        #########################################
        # Make estimations according to the model
        #########################################

        # Create separate DataFrames for buy and sell estimation
        df_buy = df[df['buy'] == 1]
        df_sell = df[df['sell'] == 1]

        results = pd.DataFrame()
        results['dt'] = df[Quotes.DateTime]

        results[self._data_to_report] = df[self._data_to_report]

        # Estimate if signals are true. Classification is always used and both signals are always used in this tool.
        self._results_buy_est = self._model_buy.predict(df_buy[self._data_to_est])
        self._results_sell_est = self._model_sell.predict(df_sell[self._data_to_est])

        it_buy = iter(self._results_buy_est)
        it_sell = iter(self._results_sell_est)

        results['buy-signal'] = np.array(map(lambda r : next(it_buy) if r == 1 else np.nan, df['buy']))
        results['sell-signal'] = np.array(map(lambda r : next(it_sell) if r == 1 else np.nan, df['sell']))

        # Probabilities calculation is optional
        if self._probability:
            buy_prob = self._model_buy.predict_proba(df_buy[self._data_to_est])
            sell_prob = self._model_sell.predict_proba(df_sell[self._data_to_est])

            results['buy-prob'] = [row[1] for row in buy_prob]
            results['sell-prob'] = [row[1] for row in sell_prob]

        self._results = results

    def get_df(self, rows=None):
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

    def add_buy_signals(self, df_source):
        """
            Add buy signals to the DataFrame.

            Args:
                df_source(DataFrame): the initial data
        """
        curr_trend = df_source['diff'] > 0
        prev_trend = df_source['diff'].shift() > 0

        buy_condition = df_source['diff'].shift(-abs(self._cycle_num)) >= self._true_ratio

        df = pd.DataFrame()

        # TODO MID Need to think of a faster way to do it
        df['buy-true'] = np.where(curr_trend & (prev_trend == False) & buy_condition & (df_source.index != 0), 1, np.nan)
        df['buy-false'] = np.where(curr_trend & (prev_trend == False) & (buy_condition == False) & (df_source.index != 0), 1, np.nan)

        df[self._data_to_est] = df_source[self._data_to_est]

        # Get rid of rows without signals
        df = df.dropna(subset=['buy-true', 'buy-false'], thresh=1)

        return df

    def add_sell_signals(self, df_source):
        """
            Add sell signals to the DataFrame.

            Args:
                df_source(DataFrame): the initial data
        """
        curr_trend = df_source['diff'] > 0
        prev_trend = df_source['diff'].shift() > 0

        sell_condition = df_source['diff'].shift(-abs(self._cycle_num)) <= -abs(self._true_ratio)

        df = pd.DataFrame()

        df['sell-true'] = np.where((curr_trend == False) & prev_trend & sell_condition & (df_source.index != 0), 1, np.nan)
        df['sell-false'] = np.where((curr_trend == False) & prev_trend & (sell_condition == False) & (df_source.index != 0), 1, np.nan)

        df[self._data_to_est] = df_source[self._data_to_est]

        # Get rid of rows without signals
        df = df.dropna(subset=['sell-true', 'sell-false'], thresh=1)

        return df
