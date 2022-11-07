"""Moving average vs. price cross backtesting strategy with fake signals distinguished by AI.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

from backtest.ma import MA
from backtest.base import BackTestError

from data.fvalues import Rows

from data.futils import get_datetime

import pandas as pd
import pandas_ta as ta
import numpy as np

from sklearn.svm import SVC
from sklearn.naive_bayes import GaussianNB
from sklearn.metrics import accuracy_score

from enum import IntEnum

class Algorithm(IntEnum):
    """Enum with the supported algorithms."""
    GaussianNB = 0
    SVC = 1

class MACls(MA):
    """
        Moving average vs. price cross backtesting strategy implementation when signal is checked by AI.

        If MA goes above the price, it is a signal to buy. Otherwise, it is a signal to sell.
        AI determines if signals are true or false.
    """
    def __init__(self,
                 true_ratio=1.01,
                 cycle_num=2,
                 learn_test_ratio=0.8,
                 algorithm=Algorithm.GaussianNB,
                 **kwargs):
        super().__init__(**kwargs)
        """
            Initializes the MA Cross stragegy implementation.

            Args:
                true_ratio(float): ratio when signal is considered as true in cycle_num. For example, if true_ratio is 1.03 and cycle_num is 5,
                                   then the signal will be considered as true if there was a 3% change in ma/quote ratio in the following 5 cycles
                                   after getting the signal from MA.
                cycle_num(int):    number of cycles to reach to true_ratio to consider that the signal is true.
                learn_test_ratio(float): ratio to split dataset into learn/test parts. For example, if the ratio is 0.8, 80% of data will be used
                                         for learning, 20% for testing.
                algorithm(Algorithm): algorithm used for learning (from Algorithm enum).

            Raises:
                BackTestError: provided arguments are incorrect.
        """
        if true_ratio <= 1:
            raise BackTestError(f"true_ratio can't be <= 1. The provided value is {true_ratio}")
        self._true_ratio = true_ratio

        if cycle_num < 0:
            raise BackTestError(f"cycle_num can't be negative. The provided value is {cycle_num}")
        self._cycle_num = cycle_num

        if learn_test_ratio < 0:
            raise BackTestError(f"learn_test_ratio can't be less than 0. The provided value is {learn_test_ratio}")
        self._learn_test_ratio = learn_test_ratio

        self._algorithm = algorithm

        # Calculated buy and sell signals
        self._df_buy_signals = None
        self._df_sell_signals = None

        # Accuracy for calculations
        self._accuracy_buy_train = None
        self._accuracy_buy_test = None

        self._accuracy_sell_train = None
        self._accuracy_sell_test = None

        # Actual and predicted signals:
        self._actual_buy_signals = None
        self._pred_buy_signals = None

        self._actual_sell_signals = None
        self._pred_sell_signals = None   

    def do_tech_calculation(self, ex):
        """
            Perform technical calculation and model training for the strategy.

            Args:
                ex(BackTestOperations): Operations instance class.
        """
        # Calculate MA values
        super().do_tech_calculation(ex)

        # Create a DataFrame with the initial data
        df_initial = pd.DataFrame(self.get_main_data().get_rows())

        # Add MA column to the DataFrame
        df_initial['ma'] = ex.get_values()

        # Calculate PVO
        pvo = ta.pvo(df_initial[Rows.Volume])

        # Get rid of the values where MA is not calculated because they are useless for learning.
        df_initial = df_initial[self._period-1:]

        # DataFrame for learning
        df = pd.DataFrame()
        df['dt'] = df_initial[Rows.DateTime]
        df['pvo'] = pvo.iloc[:, 1]

        # Get price-ma difference
        #df['diff'] = (df_initial[Rows.AdjClose] - df_initial['ma'])
        df['diff'] = ((df_initial[Rows.AdjClose] - df_initial['ma']) / df_initial[Rows.AdjClose])

        # Fill nan values (if any) with mean values
        df['pvo'].fillna(value=df['pvo'].mean(), inplace=True)
        df['diff'].fillna(value=df['diff'].mean(), inplace=True)

        # Distintuish true and false trade signals
        df['buy-true'] = (df['diff'].shift(-1) < 0) &\
                         (df['diff'] > 0) &\
                         (df['diff'].shift(self._cycle_num) / df['diff'] >= self._true_ratio)

        df['buy-false'] = (df['diff'].shift(-1) < 0) &\
                          (df['diff'] > 0) &\
                          (df['diff'].shift(self._cycle_num) / df['diff'] < self._true_ratio)

        df['sell-true'] = (df['diff'].shift(-1) > 0) &\
                          (df['diff'] < 0) &\
                          (df['diff'] / df['diff'].shift(self._cycle_num) >= self._true_ratio)

        df['sell-false'] = (df['diff'].shift(-1) > 0) &\
                           (df['diff'] < 0) &\
                           (df['diff'] / df['diff'].shift(self._cycle_num) < self._true_ratio)

        # Create a results numpy array with 4 signals
        results_buy = np.where(df['buy-true'] == True, 0, np.nan)
        results_buy = np.where(df['buy-false'] == True, 1, results_buy)
        results_sell = np.where(df['sell-true'] == True, 0, np.nan)
        results_sell = np.where(df['sell-false'] == True, 1, results_sell)

        # Get rid of NaNs in np arrays
        results_buy = results_buy[~np.isnan(results_buy)]
        results_sell = results_sell[~np.isnan(results_sell)]

        # Create separate DataFrames for buy and sell learning
        df_buy = df[df['buy-true'] | df['buy-false']]
        df_sell = df[df['sell-true'] | df['sell-false']]

        # Split the arrays to learn/test
        split_buy = int(self._learn_test_ratio*len(df_buy))
        split_sell = int(self._learn_test_ratio*len(df_sell))

        # Buy signals data to learn
        learn_buy_data = df_buy[:split_buy]
        learn_buy_results = results_buy[:split_buy]

        # Buy signals data to test
        test_buy_data = df_buy[split_buy:]
        self._actual_buy_signals = results_buy[split_buy:]

        # Sell signals data to learn
        learn_sell_data = df_sell[:split_sell]
        learn_sell_results = results_sell[:split_sell]

        # Sell signals data to test
        test_sell_data = df_sell[split_sell:]
        self._actual_sell_signals = results_sell[split_sell:]

        # Train the model
        if self._algorithm == Algorithm.GaussianNB:
            buy_learn = GaussianNB()
            sell_learn = GaussianNB()
        elif self._algorithm == Algorithm.SVC:
            buy_learn = SVC()
            sell_learn = SVC()

        pred_buy = buy_learn.fit(learn_buy_data[['diff', 'pvo']], learn_buy_results)
        pred_sell = sell_learn.fit(learn_sell_data[['diff', 'pvo']], learn_sell_results)

        # Predict values
        pred_buy_train = pred_buy.predict(learn_buy_data[['diff', 'pvo']])
        pred_sell_train = pred_sell.predict(learn_sell_data[['diff', 'pvo']])

        self._pred_buy_signals = pred_buy.predict(test_buy_data[['diff', 'pvo']])
        self._pred_sell_signals = pred_sell.predict(test_sell_data[['diff', 'pvo']])

        # Check accuracy of learning/testing
        self._accuracy_buy_train = accuracy_score(learn_buy_results, pred_buy_train)
        self._accuracy_buy_test = accuracy_score(self._actual_buy_signals, self._pred_buy_signals)

        self._accuracy_sell_train = accuracy_score(learn_sell_results, pred_sell_train)
        self._accuracy_sell_test = accuracy_score(self._actual_sell_signals, self._pred_sell_signals)

        # DataFrames with buy/sell signals for the strategy
        self._df_buy_signals = pd.DataFrame()
        self._df_buy_signals['dt'] = test_buy_data['dt']
        self._df_buy_signals['signal'] = self._pred_buy_signals.astype('bool')

        self._df_sell_signals = pd.DataFrame()
        self._df_sell_signals['dt'] = test_sell_data['dt']
        self._df_sell_signals['signal'] = self._pred_sell_signals.astype('bool')

        # Trim values from the calculation where no AI generated data present
        datetimes = [row[Rows.DateTime] for row in self.get_main_data().get_rows()]
        dt_row = datetimes.index(str(self.get_min_dt()))
        self._offset = max(dt_row, self._period)

    def get_buy_accuracy(self):
        """
            Get accuracy for determining buy signals.

            Returns:
                float: train data accuracy.
                float: test data accuracy.
        """
        return (self._accuracy_buy_train, self._accuracy_buy_test)

    def get_sell_accuracy(self):
        """
            Get accuracy for determining sell signals.

            Returns:
                float: train data accuracy.
                float: test data accuracy.
        """
        return (self._accuracy_sell_train, self._accuracy_sell_test)

    def get_buy_signals(self):
        """
            Get buy signals.

            Returns:
                numpy array: actual buy signals
                numpy array: predicted buy signals
        """
        return (self._actual_buy_signals, self._pred_buy_signals)

    def get_sell_signals(self):
        """
            Get sell signals.

            Returns:
                numpy array: actual sell signals
                numpy array: predicted sell signals
        """
        return (self._actual_sell_signals, self._pred_sell_signals)

    def get_min_dt(self):
        """
            Get the minimum datetime where AI generated data presents.

            Returns:
                datetime: the earliers available datetime with AI-generated data.
        """
        min_buy_dt = get_datetime(self._df_buy_signals['dt'].iloc[0])
        min_sell_dt = get_datetime(self._df_sell_signals['dt'].iloc[0])

        return min(min_buy_dt, min_sell_dt)

    def skip_criteria(self, index):
        """
            Check if the current cycle should be skipped.

            The cycle should be skipped if the MA is not calculated yet or there are no AI generated data.

            Args:
                index(int): index of the current cycle.
        """
        return self.exec().get_datetime() < self.get_min_dt() or index < self._period

    def signal_buy(self):
        """
            Determines if a signal to buy is true.

            Returns:
                True if the buy signal is true, False otherwise.
        """
        dt_str = self.exec().get_datetime_str()

        row = self._df_buy_signals.loc[self._df_buy_signals['dt'] == dt_str]

        if row.empty == False and row.iloc[0]['signal'] == True:
            return True

        return False

    def signal_sell(self):
        """
            Determines if a signal to sell is true.

            Returns:
                True if the sell signal is true, False otherwise.
        """
        dt_str = self.exec().get_datetime_str()

        row = self._df_sell_signals.loc[self._df_sell_signals['dt'] == dt_str]

        if row.empty == False and row.iloc[0]['signal'] == True:
            return True

        return False

    def signal(self):
        """
            Indicates if buy/sell signal was considered as true.

            Returns:
                True/False depending on signal verification.
        """

        return self.signal_buy() or self.signal_sell()
