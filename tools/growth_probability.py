"""Module with the base class for growth probability tool.

The value returned by pobability tool indicates the chance that the security will grow in the next cycles.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from tools.base import ToolError
from tools.classifier import Classifier

from data.fvalues import StockQuotes  # TODO Low think if we should make it universal (not just stock-related)

import pandas as pd
import pandas_ta as ta

class Probability(Classifier):
    """
        Growth probability impementation.
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

    def prepare(self, rows=None):
        """
            Get the DataFrame for learning/estimation.

            Returns:
                DataFrame: data ready for learning/estimation
        """
        # Create the dataframe based on provided/pre-defined data
        if rows is None:
            df = pd.DataFrame(self._rows)
        else:
            df = pd.DataFrame(rows)

        # Calculate moving averages
        if self._is_simple:
            ma_long = ta.sma(df[StockQuotes.AdjClose], length = self._period_long)
            ma_short = ta.sma(df[StockQuotes.AdjClose], length = self._period_short)
        else:
            ma_long = ta.ema(df[StockQuotes.AdjClose], length = self._period_long)
            ma_short = ta.ema(df[StockQuotes.AdjClose], length = self._period_short)

        # Calculate PVO
        pvo = ta.pvo(df[StockQuotes.Volume])

        # Prepare data for estimation
        df['pvo'] = pvo.iloc[:, 0]
        df['ma-diff'] = ((ma_long - ma_short) / ma_long)
        df['hilo-diff'] = ((df[StockQuotes.High] - df[StockQuotes.Low]) / df[StockQuotes.High])
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

    def get_buy_condition(self, df):
        """
            Get buy condiiton to check signals.

            Args:
                df(DataFrame): data with signals to check.

            Returns:
                TimeSeries: signals
        """
        curr_quote = df[StockQuotes.AdjClose]
        next_quote = df[StockQuotes.AdjClose].shift(-abs(self._cycle_num))

        return (next_quote - curr_quote) / curr_quote >= self._true_ratio

    def get_sell_condition(self, df):
        """
            Get sell condiiton to check signals.

            Args:
                df(DataFrame): data with signals to check.

            Returns:
                TimeSeries: signals
        """
        curr_quote = df[StockQuotes.AdjClose]
        next_quote = df[StockQuotes.AdjClose].shift(-abs(self._cycle_num))

        return (curr_quote - next_quote) / next_quote >= self._true_ratio
