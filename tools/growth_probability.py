"""Module with the base class for growth probability tool.

The value returned by pobability tool indicates the chance that the security will grow in the next cycles.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from tools.base import ToolError
from tools.classifier import Classifier

from data.fvalues import Quotes

import pandas as pd
import pandas_ta as ta

import numpy as np

# TODO MID need to think if we can specify columns which are used in learning in data_to_learn and avoid calculations inside the tool itself
class Probability(Classifier):
    """
        Base security growth probability impementation.
    """
    def __init__(self,
                 period_long=30,
                 period_short=15,
                 is_simple=True,
                 probability=True,
                 classify=False,
                 use_sell=False,
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
                classify(bool): indicates if classification should be performed.
                probability(bool): determines if probabilities should be calculated.
                offset(int): offset for calculation.

            Raises:
                ToolError: No model provided to make the estimation.
        """
        super().__init__(**kwargs, probability=probability, use_sell=use_sell, classify=classify)

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

        if self._probability is False:
            raise ToolError("Probabilities calculalation is disabled but it is required by this tool.")

        # Check if we need to train the model at first
        if self.need_buy(self._model_buy is None) or self.need_sell(self._model_sell is None):
            self.learn()

        # DataFrame for the current symbol
        df = self.get_df()

        #########################################
        # Make estimations according to the model
        #########################################

        results = pd.DataFrame()
        results['dt'] = df[Quotes.DateTime]

        results[self._data_to_report] = df[self._data_to_report]

        # Classification is optional
        if self._classify:
            if self._use_buy:
                self._results_buy_est = self._model_buy.predict(df[self._data_to_est])
                results['buy-signal'] = self._results_buy_est

            if self._use_sell:
                self._results_sell_est = self._model_sell.predict(df[self._data_to_est])
                results['sell-signal'] = self._results_sell_est

        # Probabilities are always calculated
        if self._use_buy:
            buy_prob = self._model_buy.predict_proba(df[self._data_to_est])
            results['buy-prob'] = [row[1] for row in buy_prob]

        if self._use_sell:
            sell_prob = self._model_sell.predict_proba(df[self._data_to_est])
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

        # Calculate moving averages
        if self._is_simple:
            ma_long = ta.sma(df[Quotes.AdjClose], length = self._period_long)
            ma_short = ta.sma(df[Quotes.AdjClose], length = self._period_short)
        else:
            ma_long = ta.ema(df[Quotes.AdjClose], length = self._period_long)
            ma_short = ta.ema(df[Quotes.AdjClose], length = self._period_short)

        # Calculate PVO
        pvo = ta.pvo(df[Quotes.Volume])

        # Prepare data for estimation
        df['pvo'] = pvo.iloc[:, 0]
        df['ma-diff'] = ((ma_long - ma_short) / ma_long)
        df['hilo-diff'] = ((df[Quotes.High] - df[Quotes.Low]) / df[Quotes.High])
        df['ma-long'] = ma_long
        df['ma-short'] = ma_short

        self._data_to_est = ['pvo', 'ma-diff', 'hilo-diff']  # Columns to make estimations
        self._data_to_report = self._data_to_est + ['ma-long', 'ma-short']  # Columns for reporting

        # Get rid of the values where MA is not calculated because they are useless for learning.
        df = df[self._period_long-1:]
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
        curr_quote = df_source[Quotes.AdjClose]
        next_quote = df_source[Quotes.AdjClose].shift(-abs(self._cycle_num))

        buy_condition = (next_quote - curr_quote) / curr_quote >= self._true_ratio

        df = pd.DataFrame()

        df['buy-true'] = np.where(buy_condition & (df_source.index != 0), 1, np.nan)
        df['buy-false'] = np.where((buy_condition == False) & (df.index != 0), 1, np.nan)

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
        curr_quote = df_source[Quotes.AdjClose]
        next_quote = df_source[Quotes.AdjClose].shift(-abs(self._cycle_num))

        sell_condition = (curr_quote - next_quote) / next_quote >= self._true_ratio

        df = pd.DataFrame()

        df['sell-true'] = np.where(sell_condition & (df_source.index != 0), 1, np.nan)
        df['sell-false'] = np.where((sell_condition == False) & (df.index != 0), 1, np.nan)

        df[self._data_to_est] = df_source[self._data_to_est]

        # Get rid of rows without signals
        df = df.dropna(subset=['sell-true', 'sell-false'], thresh=1)

        return df
