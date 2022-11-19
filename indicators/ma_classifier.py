"""Classifier of MA/Quote cross signals (true/false) according to the trained model.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

from indicators.base import IndicatorError
from indicators.classifier import Classifier

from data.fvalues import Rows

import pandas as pd
import pandas_ta as ta

import numpy as np

from sklearn.metrics import accuracy_score

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
                period(int): long period for MA calculation (must match the period used for model calculation).
                rows(list): quotes for calculation.
                model_buy(): model to estimate buy signals.
                model_sell(): model to estimate sell signals.
                data_to_learn([array]) data to train the models. Either models or data to learn need to be specified.
                is_simple(bool): indicated is SMA or EMA should be used (must match the MA type used for model calculation).
                true_ratio(float): ratio when signal is considered as true in cycle_num. For example, if true_ratio is 0.03 and cycle_num is 5,
                                then the signal will be considered as true if there was a 0.03 change in ma/quote ratio in the following 5 cycles
                                after getting the signal from MA.
                cycle_num(int): number of cycles to reach to true_ratio to consider that the signal is true.
                algorithm(Algorithm): algorithm used for learning (from Algorithm enum).
                offset(int): offset for calculation.

            Raises:
                IndicatorError: No model provided to make the estimation.
        """
        super().__init__(**kwargs)

        self._period = period
        self._is_simple = is_simple

    def calculate(self):
        """
            Perform the calculation based on the provided data.

            Raises:
                IndicatorError: no data for test provided.
        """
        if self._rows == None:
            raise IndicatorError("No data for testing provided.")

        # Check if we need to train the model at first
        if self._model_buy == None or self._model_sell == None:
            self.learn()

        # DataFrame for the current symbol
        df = self.get_df()

        # Find signals which are needed to check

        curr_trend = df['diff'] > 0
        prev_trend = df['diff'].shift() > 0

        buy = np.where(curr_trend & (prev_trend == False) & (df.index != 0), 1, np.nan)
        sell = np.where((curr_trend == False) & prev_trend & (df.index != 0), 1, np.nan)
        df['buy'] = buy
        df['sell'] = sell

        #########################################
        # Make estimations according to the model
        #########################################

        # Create separate DataFrames for buy and sell estimation
        df_buy = df[df['buy'] == 1]
        df_sell = df[df['sell'] == 1]

        # Estimate if signals are true
        self._results_buy_est = self._model_buy.predict(df_buy[['pvo', 'diff']])
        self._results_sell_est = self._model_sell.predict(df_sell[['pvo', 'diff']])

        it_buy = iter(self._results_buy_est)
        it_sell = iter(self._results_sell_est)

        df['buy-signal'] = np.array(map(lambda r : next(it_buy) if r == 1 else np.nan, df['buy']))
        df['sell-signal'] = np.array(map(lambda r : next(it_sell) if r == 1 else np.nan, df['sell']))

        results = pd.DataFrame()
        results['dt'] = df[Rows.DateTime]
        results['ma'] = df['ma']
        results['pvo'] = df['pvo']
        results['buy-signal'] = df['buy-signal']
        results['sell-signal'] = df['sell-signal']

        self._results = results

    def get_df(self, rows=None):
        """
            Get the DataFrame for learning/estimation based on the initial DataFrame.

            Returns:
                DataFrame: data ready for learning/estimation
        """
        # DataFrame for the current symbol
        if rows == None:
            df = pd.DataFrame(self._rows)
        else:
            df = pd.DataFrame(rows)

        # Calculate moving average for each DataFrame
        ma = self.get_ma(df)

        # Calculate PVO
        pvo = ta.pvo(df[Rows.Volume])

        df['ma'] = ma
        df['pvo'] = pvo.iloc[:, 0]
        df['diff'] = ((df[Rows.AdjClose] - df['ma']) / df[Rows.AdjClose])
        df['hilo-diff'] = (df[Rows.High] - df[Rows.Low] / df[Rows.High])

        # Get rid of the values where MA is not calculated because they are useless for learning.
        df = df[self._period-1:]
        df = df.reset_index().drop(['index'], axis=1)

        # Fill nan values (if any) with mean values
        df['pvo'].fillna(value=df['pvo'].mean(), inplace=True)
        df['diff'].fillna(value=df['diff'].mean(), inplace=True)
        df['hilo-diff'].fillna(value=df['diff'].mean(), inplace=True)

        return df

    def get_ma(self, df):
        """
            Get the moving average values.

            Args:
                df(DataFrame): data for calculation

            Returns:
                DataFrame: calculated MA
        """
        if self._is_simple:
            ma = ta.sma(df[Rows.AdjClose], length = self._period)
        else:
            ma = ta.ema(df[Rows.AdjClose], length = self._period)

        return ma

    def add_signals(self, df):
        """
            Add buy-sell signals to the DataFrame.

            Args:
                df(DataFrame): the initial data
        """
        curr_trend = df['diff'] > 0
        prev_trend = df['diff'].shift() > 0
        buy_cycle = df['diff'].shift(-abs(self._cycle_num)) >= self._true_ratio
        sell_cycle = df['diff'].shift(-abs(self._cycle_num)) <= -abs(self._true_ratio)

        buy_true = np.where(curr_trend & (prev_trend == False) & buy_cycle & (df.index != 0), 1, np.nan)
        buy_false = np.where(curr_trend & (prev_trend == False) & (buy_cycle == False) & (df.index != 0), 1, np.nan)
        sell_true = np.where((curr_trend == False) & prev_trend & sell_cycle & (df.index != 0), 1, np.nan)
        sell_false = np.where((curr_trend == False) & prev_trend & (sell_cycle == False) & (df.index != 0), 1, np.nan)
        df['buy-true'] = buy_true
        df['buy-false'] = buy_false
        df['sell-true'] = sell_true
        df['sell-false'] = sell_false

        # Get rid of rows without signals
        df = df[['buy-true', 'buy-false', 'sell-true', 'sell-false']].dropna(thresh=1)

    def learn(self):
        """
            Perform model training.
        """
        df_main = pd.DataFrame()

        for rows in self._data_to_learn:
            # DataFrame for the current symbol
            df = self.get_df(rows)

            # Distintuish true and false trade signals
            self.add_signals(df)

            # Append current symbol's calculation to the main DataFrame

            dfi = pd.DataFrame()
            dfi['pvo'] = df['pvo']
            dfi['diff'] = df['diff']
            dfi['buy-true'] = df['buy-true']
            dfi['buy-false'] = df['buy-false']
            dfi['sell-true'] = df['sell-true']
            dfi['sell-false'] = df['sell-false']
            dfi['hilo-diff'] = df['hilo-diff']

            df_main = df_main.append(dfi)

        results_buy, results_sell = self.get_buy_sell_results(df_main)

        # Create separate DataFrames for buy and sell learning
        df_buy = df_main[(df_main['buy-true'] == 1) | (df_main['buy-false'] == 1)]
        df_sell = df_main[(df_main['sell-true'] == 1) | (df_main['sell-false'] == 1)]

        # Train the model

        buy_learn, sell_learn = self.get_learning_instances()

        self._model_buy = buy_learn.fit(df_buy[['pvo', 'diff']], results_buy)
        self._model_sell = sell_learn.fit(df_sell[['pvo', 'diff']], results_sell)

        # Check accuracy of learning
        est_buy_train = self._model_buy.predict(df_buy[['pvo', 'diff']])
        est_sell_train = self._model_sell.predict(df_sell[['pvo', 'diff']])

        self._accuracy_buy_learn = accuracy_score(results_buy, est_buy_train)
        self._accuracy_sell_learn = accuracy_score(results_sell, est_sell_train)
        self._total_accuracy_learn = (self._accuracy_buy_learn * len(results_buy) + self._accuracy_sell_learn * len(results_sell)) / (len(results_buy) + len(results_sell))
