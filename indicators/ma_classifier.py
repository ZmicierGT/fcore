"""Classifier of MA/Quote cross signals (true/false) according to the trained model.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

from indicators.base import BaseIndicator
from indicators.base import IndicatorError

from data.fvalues import Rows

from enum import IntEnum

import pandas as pd
import pandas_ta as ta

import numpy as np

from enum import IntEnum

from sklearn.linear_model import LogisticRegression
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
from sklearn.neighbors import KNeighborsClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.tree import DecisionTreeClassifier
from sklearn.svm import SVC

from sklearn.metrics import accuracy_score

class MAClassifierRows(IntEnum):
    """
        Enum to represent MA Classifier results.
    """
    Value = 0
    Quote = 1
    MA = 2

class Algorithm(IntEnum):
    """Enum with the supported algorithms."""
    LR = 0
    LDA = 1
    KNC = 2
    GaussianNB = 3
    DTC = 4
    SVC = 5

class MAClassifier(BaseIndicator):
    """
        MA/Price signals classifier (true/false) impementation.
    """
    def __init__(self,
                 period,
                 rows,
                 row_val,
                 model_buy=None,
                 model_sell=None,
                 data_to_learn=None,
                 is_simple=True,
                 true_ratio=0,
                 cycle_num=2,
                 algorithm=Algorithm.GaussianNB,
                 offset=None):
        """
            Initialize PDO implementation class.

            Args:
                period(int): long period for MA calculation (must match the period used for model calculation).
                rows(list): quotes for calculation.
                row_val(int): number of row with data to use in calculation.
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
        super().__init__(rows)

        # Some type of model (path to serialized models or objects to models) must present.
        if (model_buy == None or model_sell == None) and data_to_learn == None:
            raise IndicatorError("No models or data to learn provided to make the estimation.")

        self.__row_val = row_val
        self.__period = period
        self.__model_buy = model_buy
        self.__model_sell = model_sell
        self.__data_to_learn = data_to_learn
        self.__is_simple = is_simple
        self.__cycle_num = cycle_num
        self.__true_ratio = true_ratio
        self.__algorithm = algorithm

        # Accuracy values are set in a case if there was a calculation
        self.__accuracy_buy_learn = None
        self.__accuracy_sell_learn = None
        self.__total_accuracy_learn = None

    def calculate(self):
        """
            Perform the calculation based on the provided data.
        """
        # Check if we need to train the model at first
        if self.__model_buy == None or self.__model_sell == None:
            self.learn()

        # DataFrame for the current symbol
        df = self.get_df()

        # Find signals which are needed to check
        df['buy'] = (df['diff'].shift(-1) < 0) & (df['diff'] > 0)
        df['sell'] = (df['diff'].shift(-1) > 0) & (df['diff'] < 0)

        # Make estimations according to the model

        # Create separate DataFrames for buy and sell estimation
        df_buy = df[df['buy']]
        df_sell = df[df['sell']]

        # Estimate if signals are true
        self.__results_buy_est = self.__model_buy.predict(df_buy[['pvo', 'diff']])
        self.__results_sell_est = self.__model_sell.predict(df_sell[['pvo', 'diff']])

        # DataFrames with buy/sell signals for the strategy
        df_buy_signals = pd.DataFrame()
        df_buy_signals['dt'] = df_buy[Rows.DateTime]
        df_buy_signals['signal'] = self.__results_buy_est.astype('bool')

        df_sell_signals = pd.DataFrame()
        df_sell_signals['dt'] = df_sell[Rows.DateTime]
        df_sell_signals['signal'] = self.__results_sell_est.astype('bool')

        self._results = [df_buy_signals, df_sell_signals]

    def check_est_precision(self):
        """
            Check precision of price estimation. The function compares the actual results with the estimated ones.

            Raises:
                IndicatorError: the calculation is not performed.

            Returns:
                float: buy accuracy
                float: sell accuracy
                float: cumulative accuracy
        """
        if len(self._results) == 0:
            raise IndicatorError("The calculation is not performed.")

        df = self.get_df()
        self.add_signals(df)
        results_buy, results_sell = self.get_buy_sell_results(df)

        # If we use the indicator on live data, we still may not know the outcome of the latest occurrences yes.
        # Then we need to trim the arrays to calculate estimation correctly.

        results_buy_est = self.__results_buy_est
        results_sell_est = self.__results_sell_est

        if len(results_buy) != len(results_buy_est) or len(results_sell) != len(results_sell_est):
            results_buy_est = results_buy_est[:len(results_buy)]
            results_sell_est = results_sell_est[:len(results_sell)]

        # Check the accuracy
        accuracy_buy = accuracy_score(results_buy, results_buy_est)
        accuracy_sell = accuracy_score(results_sell, results_sell_est)

        total_accuracy = (accuracy_buy * len(results_buy) + accuracy_sell * len(results_sell)) / (len(results_buy) + len(results_sell))

        np.set_printoptions(threshold=np.inf)
        print(results_buy)
        print(results_sell)

        print(results_buy_est)
        print(results_sell_est)

        return (accuracy_buy, accuracy_sell, total_accuracy)

    def get_df(self, rows=None):
        """
            Get the DataFrame for learning/predictions based on the initial DataFrame.

            Returns:
                DataFrame: data ready for learning/predictions
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
        df['pvo'] = pvo.iloc[:, 1]
        df['diff'] = ((df[Rows.AdjClose] - df['ma']) / df[Rows.AdjClose])
        df['hilo-diff'] = (df[Rows.High] - df[Rows.Low] / df[Rows.High])

        # Get rid of the values where MA is not calculated because they are useless for learning.
        df = df[self.__period-1:]

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
        if self.__is_simple:
            ma = ta.sma(df[Rows.AdjClose], length = self.__period)
        else:
            ma = ta.ema(df[Rows.AdjClose], length = self.__period)

        return ma

    def add_signals(self, df):
        """
            Add buy-sell signals to the DataFrame.

            Args:
                df(DataFrame): the initial data
        """
        df['buy-true'] = (df['diff'].shift(-1) < 0) &\
                            (df['diff'] > 0) &\
                            (df['diff'].shift(self.__cycle_num) >= self.__true_ratio)

        df['buy-false'] = (df['diff'].shift(-1) < 0) &\
                            (df['diff'] > 0) &\
                            (df['diff'].shift(self.__cycle_num) < self.__true_ratio)

        df['sell-true'] = (df['diff'].shift(-1) > 0) &\
                            (df['diff'] < 0) &\
                            (df['diff'].shift(self.__cycle_num) <= -abs(self.__true_ratio))

        df['sell-false'] = (df['diff'].shift(-1) > 0) &\
                            (df['diff'] < 0) &\
                            (df['diff'].shift(self.__cycle_num) > -abs(self.__true_ratio))

    def get_buy_sell_results(self, df):
        """
            Get buy/sell signals as numpy array.

            Args:
                df(DataFrame): the initial data

            Returns:
                results_buy(numpy array): buy signals data
                results_sell(numpy array): sell signals data
        """
        # Create a results numpy array with 4 signals
        results_buy = np.where(df['buy-true'] == True, 1, np.nan)
        results_buy = np.where(df['buy-false'] == True, 0, results_buy)
        results_sell = np.where(df['sell-true'] == True, 1, np.nan)
        results_sell = np.where(df['sell-false'] == True, 0, results_sell)

        # Get rid of NaNs in np arrays
        results_buy = results_buy[~np.isnan(results_buy)]
        results_sell = results_sell[~np.isnan(results_sell)]

        return (results_buy, results_sell)

    def learn(self):
        """
            Perform model training.
        """
        # DataFrame for learning
        df_main = pd.DataFrame(columns=['pvo', 'diff'])

        for rows in self.__data_to_learn:
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
        df_buy = df_main[df_main['buy-true'] | df_main['buy-false']]
        df_sell = df_main[df_main['sell-true'] | df_main['sell-false']]

        # Train the model
        if self.__algorithm == Algorithm.LR:
            buy_learn = LogisticRegression()
            sell_learn = LogisticRegression()
        elif self.__algorithm == Algorithm.LDA:
            buy_learn = LinearDiscriminantAnalysis()
            sell_learn = LinearDiscriminantAnalysis()
        elif self.__algorithm == Algorithm.KNC:
            buy_learn = KNeighborsClassifier()
            sell_learn = KNeighborsClassifier()
        elif self.__algorithm == Algorithm.GaussianNB:
            buy_learn = GaussianNB()
            sell_learn = GaussianNB()
        elif self.__algorithm == Algorithm.DTC:
            buy_learn = DecisionTreeClassifier()
            sell_learn = DecisionTreeClassifier()
        elif self.__algorithm == Algorithm.SVC:
            buy_learn = SVC()
            sell_learn = SVC()

        self.__model_buy = buy_learn.fit(df_buy[['pvo', 'diff']], results_buy)
        self.__model_sell = sell_learn.fit(df_sell[['pvo', 'diff']], results_sell)

        # Check accuracy of learning
        pred_buy_train = self.__model_buy.predict(df_buy[['pvo', 'diff']])
        pred_sell_train = self.__model_sell.predict(df_sell[['pvo', 'diff']])

        self.__accuracy_buy_learn = accuracy_score(results_buy, pred_buy_train)
        self.__accuracy_sell_learn = accuracy_score(results_sell, pred_sell_train)

        self.__total_accuracy_learn = (self.__accuracy_buy_learn * len(results_buy) + self.__accuracy_sell_learn * len(results_sell)) / (len(results_buy) + len(results_sell))

    def get_learn_accuracy(self):
        """
            Get accuracies for learning. Learning should be performed at first to get a rational results. It does not work with pre-defined model.

            Raises:
                IndicatorError: the learning wasn't performed.

            Returns:
                float: buy accuracy
                float: sell accuracy
                float: cumulative accuracy
        """
        if self.__accuracy_buy_learn == None or self.__accuracy_sell_learn == None or self.__total_accuracy_learn == None:
            raise IndicatorError("The learning wasn't performed.")

        return (self.__accuracy_buy_learn, self.__accuracy_sell_learn, self.__total_accuracy_learn)
