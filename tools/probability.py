"""Module with the base class for growth probability tool.

The value returned by pobability tool indicates the chance that the security will grow in the next cycles.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""
from tools.base import ToolError
from tools.classifier import Classifier

from data.fvalues import Quotes

import pandas as pd
import pandas_ta as ta

import numpy as np

from sklearn.metrics import accuracy_score, f1_score

class Probability(Classifier):
    """
        Base security growth probability impementation.
    """
    def __init__(self,
                 period_long=30,
                 period_short=15,
                 is_simple=True,
                 **kwargs):
        """
            Initialize probability class.

            Args:
                period_long(int): MA period to compare with a short period
                period_short: MA period to compare with a long period
                is_simple(bool): indicates if SMAs or EMAs are used.
                rows(list): quotes to make an estimation.
                model_buy(): model to estimate buy signals.
                model_sell(): model to estimate sell signals.
                data_to_learn([array]) data to train the models. Either models or data to learn need to be specified.
                true_ratio(float): ratio when signal is considered as true in cycle_num. For example, if true_ratio is 0.03 and cycle_num is 5,
                                then the signal will be considered as true if there was a 3% change in quote in the following 5 cycles
                                after getting the signal.
                cycle_num(int): number of cycles to reach to true_ratio to consider that the signal is true.
                algorithm(Algorithm): algorithm used for learning (from Algorithm enum).
                offset(int): offset for calculation.

            Raises:
                ToolError: No model provided to make the estimation.
        """
        super().__init__(**kwargs)

        if period_long <= period_short:
            raise ToolError(f"Long MA period should be bigger than short period: {period_long} > {period_short}")

        self._period_long = period_long
        self._period_short = period_short
        self._is_simple = is_simple

    def calculate(self):
        """
            Perform the calculation based on the provided data.

            Raises:
                ToolError: no data for test provided.
        """
        if self._rows == None:
            raise ToolError("No data for testing provided.")

        # Check if we need to train the model at first
        if self._model_buy == None or self._model_sell == None:
            self.learn()

        # DataFrame for the current symbol
        df = self.get_df()

        #########################################
        # Make estimations according to the model
        #########################################

        data_to_est = df[['pvo', 'diff', 'ma-diff']]

        self._results_buy_est = self._model_buy.predict(data_to_est)
        self._results_sell_est = self._model_sell.predict(data_to_est)

        buy_prob = self._model_buy.predict_proba(data_to_est)
        sell_prob = self._model_sell.predict_proba(data_to_est)

        results = pd.DataFrame()
        results['dt'] = df[Quotes.DateTime]
        results['ma-long'] = df['ma-long']
        results['ma-short'] = df['ma-short']
        results['pvo'] = df['pvo']
        results['diff'] = df['diff']
        results['ma-diff'] = df['ma-diff']
        results['buy-signal'] = self._results_buy_est
        results['sell-signal'] = self._results_sell_est
        results['buy-prob'] = [row[1] for row in buy_prob]
        results['sell-prob'] = [row[1] for row in sell_prob]

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

        # Calculate moving averages
        if self._is_simple:
            ma_long = ta.sma(df[Quotes.AdjClose], length = self._period_long)
            ma_short = ta.sma(df[Quotes.AdjClose], length = self._period_short)
        else:
            ma_long = ta.ema(df[Quotes.AdjClose], length = self._period_long)
            ma_short = ta.ema(df[Quotes.AdjClose], length = self._period_short)

        # Calculate PVO
        pvo = ta.pvo(df[Quotes.Volume])

        df['pvo'] = pvo.iloc[:, 0]
        df['diff'] = ((df[Quotes.AdjClose] - ma_long) / df[Quotes.AdjClose])
        df['ma-diff'] = ((ma_long - ma_short) / ma_long)
        df['hilo-diff'] = ((df[Quotes.High] - df[Quotes.Low]) / df[Quotes.High])
        df['ma-long'] = ma_long
        df['ma-short'] = ma_short

        # Get rid of the values where MA is not calculated because they are useless for learning.
        df = df[self._period_long-1:]
        df = df.reset_index().drop(['index'], axis=1)

        # Fill nan values (if any) with mean values
        df['pvo'].fillna(value=df['pvo'].mean(), inplace=True)
        df['diff'].fillna(value=df['diff'].mean(), inplace=True)
        df['ma-diff'].fillna(value=df['ma-diff'].mean(), inplace=True)
        df['hilo-diff'].fillna(value=df['hilo-diff'].mean(), inplace=True)

        return df

    def add_signals(self, df):
        """
            Add buy-sell signals to the DataFrame.

            Args:
                df(DataFrame): the initial data
        """
        curr_quote = df[Quotes.AdjClose]
        next_quote = df[Quotes.AdjClose].shift(-abs(self._cycle_num))

        buy_cycle = (next_quote - curr_quote) / curr_quote >= self._true_ratio
        sell_cycle = (curr_quote - next_quote) / next_quote >= self._true_ratio

        buy_true = np.where(buy_cycle & (df.index != 0), 1, np.nan)
        buy_false = np.where((buy_cycle == False) & (df.index != 0), 1, np.nan)

        sell_true = np.where(sell_cycle & (df.index != 0), 1, np.nan)
        sell_false = np.where((sell_cycle == False) & (df.index != 0), 1, np.nan)

        df['buy-true'] = buy_true
        df['buy-false'] = buy_false
        df['sell-true'] = sell_true
        df['sell-false'] = sell_false

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
            dfi['ma-diff'] = df['ma-diff']

            dfi['buy-true'] = df['buy-true']
            dfi['buy-false'] = df['buy-false']
            dfi['sell-true'] = df['sell-true']
            dfi['sell-false'] = df['sell-false']

            df_main = pd.concat([df_main, dfi], ignore_index=True)

        results_buy, results_sell = self.get_buy_sell_results(df_main)

        # Create separate DataFrames for buy and sell learning
        df_buy = df_main[(df_main['buy-true'] == 1) | (df_main['buy-false'] == 1)]
        df_sell = df_main[(df_main['sell-true'] == 1) | (df_main['sell-false'] == 1)]

        # Replace nans with mean values
        means_buy = df_buy.mean()  # TODO MID Check if it needs to be removed
        means_sell = df_sell.mean()

        df_buy = df_buy.fillna(means_buy)
        df_sell = df_sell.fillna(means_sell)

        # Train the model

        buy_learn, sell_learn = self.get_learning_instances()

        data_buy = df_buy[['pvo', 'diff', 'ma-diff']]
        data_sell = df_sell[['pvo', 'diff', 'ma-diff']]

        self._model_buy = buy_learn.fit(data_buy, results_buy)
        self._model_sell = sell_learn.fit(data_sell, results_sell)

        # Check accuracy of learning
        est_buy_train = self._model_buy.predict(data_buy)
        est_sell_train = self._model_sell.predict(data_sell)

        self._accuracy_buy_learn = accuracy_score(results_buy, est_buy_train)
        self._accuracy_sell_learn = accuracy_score(results_sell, est_sell_train)
        self._total_accuracy_learn = (self._accuracy_buy_learn * len(results_buy) + self._accuracy_sell_learn * len(results_sell)) / (len(results_buy) + len(results_sell))

        self._f1_buy_learn = f1_score(results_buy, est_buy_train)
        self._f1_sell_learn = f1_score(results_sell, est_sell_train)
        self._total_f1_learn = (self._f1_buy_learn * len(results_buy) + self._f1_sell_learn * len(results_sell)) / (len(results_buy) + len(results_sell))
