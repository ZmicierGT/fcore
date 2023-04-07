"""Module with the base class for custom classifier 'AI-indicator'.

Classifier is an extension to a technical indicator/oscillator when signal is checked to be true/false by AI.
All the signals are classified to 4 groups then: buy-true, buy-false, sell-true, sell-false.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

import abc

from tools.base import BaseTool
from tools.base import ToolError

from enum import IntEnum

import pandas as pd
import numpy as np

from sklearn.linear_model import LogisticRegression
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
from sklearn.neighbors import KNeighborsClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.tree import DecisionTreeClassifier
from sklearn.svm import SVC

from sklearn.metrics import accuracy_score, f1_score

# TODO LOW Add the ability to pass the learning instance as an argument
class Algorithm(IntEnum):
    """Enum with the supported algorithms."""
    LR = 0
    LDA = 1
    KNC = 2
    GaussianNB = 3
    DTC = 4
    SVC = 5

class Classifier(BaseTool):
    """
        Base signals classifier (true/false) impementation.
    """
    def __init__(self,
                 rows=None,
                 model_buy=None,
                 model_sell=None,
                 data_to_learn=None,
                 true_ratio=0,
                 cycle_num=2,
                 algorithm=Algorithm.GaussianNB
                ):
        """
            Initialize PDO implementation class.

            Args:
                rows(list): quotes for calculation.
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
        super().__init__(rows)

        # Some type of model (path to serialized models or objects to models) must present.
        if (model_buy == None or model_sell == None) and data_to_learn == None:
            raise ToolError("No models or data to learn provided to make the estimation.")

        self._model_buy = model_buy
        self._model_sell = model_sell
        self._data_to_learn = data_to_learn
        self._cycle_num = cycle_num
        self._true_ratio = true_ratio
        self._algorithm = algorithm

        # Accuracy values are set in a case if there was a calculation
        self._accuracy_buy_learn = None
        self._accuracy_sell_learn = None
        self._total_accuracy_learn = None

        # f1 score values are set in a case if there was a calculation
        self._f1_buy_learn = None
        self._f1_sell_learn = None
        self._total_f1_learn = None

        self._results_buy_est = None
        self._results_sell_est = None

    def get_est_accuracy(self):
        """
            Get precision of signal estimation. The function compares the actual results with the estimated ones.

            Raises:
                ToolError: the calculation is not performed.

            Returns:
                float: buy accuracy
                float: sell accuracy
                float: cumulative accuracy
        """
        if len(self._results) == 0:
            raise ToolError("The calculation is not performed.")

        results_buy_actual, results_buy_est, results_sell_actual, results_sell_est = self.get_signals_to_compare()

        # Get the accuracy
        accuracy_buy = accuracy_score(results_buy_actual, results_buy_est)
        accuracy_sell = accuracy_score(results_sell_actual, results_sell_est)

        total_accuracy = (accuracy_buy * len(results_buy_actual) + accuracy_sell * len(results_sell_actual)) / (len(results_buy_actual) + len(results_sell_actual))

        return (accuracy_buy, accuracy_sell, total_accuracy)

    def get_est_f1(self):
        """
            Get f1 score of signal estimation.

            Raises:
                ToolError: the calculation is not performed.

            Returns:
                float: buy f1 score
                float: sell f1 score
                float: cumulative f1 score
        """
        if len(self._results) == 0:
            raise ToolError("The calculation is not performed.")

        results_buy_actual, results_buy_est, results_sell_actual, results_sell_est = self.get_signals_to_compare()

        # Get the f1 score
        f1_buy = f1_score(results_buy_actual, results_buy_est)
        f1_sell = f1_score(results_sell_actual, results_sell_est)

        total_f1 = (f1_buy * len(results_buy_actual) + f1_sell * len(results_sell_actual)) / (len(results_buy_actual) + len(results_sell_actual))

        return (f1_buy, f1_sell, total_f1)

    def set_data(self, data):
        """
            Set data for calculation

            Args:
                data(BackTestData): data for estimation.
        """
        self._rows = data

    def get_signals_to_compare(self):
        """
            Get buy/sell actual and estimated signals.
            In the case of the streaming calculation, there may be less actual values than than estimated.
            In such case the exceeding estimated results will be trimmed.

            Raises:
                ToolError: calculation is not performed.

            Returns:
                numpy.array: adjusted actual signals to buy
                numpy.array: estimated signals to buy
                numpy.array: adjusted actual signals to sell
                numpy.array: estimated signals to sell
        """
        if len(self._results) == 0:
            raise ToolError("The calculation is not performed.")

        df = self.get_df()
        self.add_signals(df)
        results_buy_actual, results_sell_actual = self.get_buy_sell_results(df)

        results_buy_est = self._results_buy_est
        results_sell_est = self._results_sell_est

        if len(results_buy_actual) != len(results_buy_est) or len(results_sell_actual) != len(results_sell_est):
            results_buy_est = results_buy_est[:len(results_buy_actual)]
            results_sell_est = results_sell_est[:len(results_sell_actual)]

        return (results_buy_actual, results_buy_est, results_sell_actual, results_sell_est)

    def get_df_signals_to_compare(self):
        """
            Get buy/sell signals to compare as a dataframe.

            Returns:
                DataFrame: actual and estimated signals to compare.
        """
        results_buy_actual, results_buy_est, results_sell_actual, results_sell_est = self.get_signals_to_compare()

        signals = pd.DataFrame()
        signals.index = range(0, max(len(results_buy_est), len(results_sell_est)))

        signals['buy-actual'] = pd.Series(results_buy_actual.astype('bool'))
        signals['buy-est'] = pd.Series(results_buy_est.astype('bool'))
        signals['sell-actual'] = pd.Series(results_sell_actual.astype('bool'))
        signals['sell-est'] = pd.Series(results_sell_est.astype('bool'))

        return signals

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
        results_buy = np.where(df['buy-true'] == 1, 1, np.nan)
        results_buy = np.where(df['buy-false'] == 1, 0, results_buy)
        results_sell = np.where(df['sell-true'] == 1, 1, np.nan)
        results_sell = np.where(df['sell-false'] == 1, 0, results_sell)

        # Get rid of NaNs in np arrays
        results_buy = results_buy[~np.isnan(results_buy)]
        results_sell = results_sell[~np.isnan(results_sell)]

        return (results_buy, results_sell)

    def get_learning_instances(self):
        """
            Get instances for learning based of the chosen algorithm.

            Retunrs:
                buy_learn: instance to learn buy signals.
                sell_learn: instance to learn sell signals.
        """
        if self._algorithm == Algorithm.LR:
            buy_learn = LogisticRegression(class_weight='balanced')
            sell_learn = LogisticRegression(class_weight='balanced')
        elif self._algorithm == Algorithm.LDA:
            buy_learn = LinearDiscriminantAnalysis()
            sell_learn = LinearDiscriminantAnalysis()
        elif self._algorithm == Algorithm.KNC:
            buy_learn = KNeighborsClassifier()
            sell_learn = KNeighborsClassifier()
        elif self._algorithm == Algorithm.GaussianNB:
            buy_learn = GaussianNB()
            sell_learn = GaussianNB()
        elif self._algorithm == Algorithm.DTC:
            buy_learn = DecisionTreeClassifier(class_weight='balanced')
            sell_learn = DecisionTreeClassifier(class_weight='balanced')
        elif self._algorithm == Algorithm.SVC:
            buy_learn = SVC(class_weight='balanced')
            sell_learn = SVC(class_weight='balanced')

        return (buy_learn, sell_learn)

    def get_learn_accuracy(self):
        """
            Get accuracies for learning. Learning should be performed at first to get a rational results. It does not work with pre-defined model.

            Raises:
                ToolError: the learning wasn't performed.

            Returns:
                float: buy accuracy
                float: sell accuracy
                float: cumulative accuracy
        """
        if self._accuracy_buy_learn == None or self._accuracy_sell_learn == None or self._total_accuracy_learn == None:
            raise ToolError("The learning wasn't performed.")

        return (self._accuracy_buy_learn, self._accuracy_sell_learn, self._total_accuracy_learn)

    def get_learn_f1(self):
        """
            Get f1 ratio for learning. Learning should be performed at first to get a rational results. It does not work with pre-defined model.

            Raises:
                ToolError: the learning wasn't performed.

            Returns:
                float: buy f1 ratio
                float: sell f1 ratio
                float: cumulative f1 ratio
        """
        if self._f1_buy_learn == None or self._f1_sell_learn == None or self._total_f1_learn == None:
            raise ToolError("The learning wasn't performed.")

        return (self._f1_buy_learn, self._f1_sell_learn, self._total_f1_learn)

    def get_models(self):
        """
            Get models for buy/sell signals.

            Raises:
                ToolError: learning was not performed.

            Returns:
                model_buy: model to check buy signals
                model_sell: model to check sell signals
        """
        if self._model_buy == None or self._model_sell == None:
            raise ToolError("The learning was not performed.")

        return (self._model_buy, self._model_sell)

    ##################
    # Abstract methods
    ##################

    @abc.abstractmethod
    def get_df(self):
        """
            Get the DataFrame for learning/estimation based on the initial DataFrame.

            Returns:
                DataFrame: data ready for learning/estimation
        """

    @abc.abstractmethod
    def add_signals(self, df):
        """
            Add buy-sell signals to the DataFrame.

            Args:
                df(DataFrame): the initial data
        """
