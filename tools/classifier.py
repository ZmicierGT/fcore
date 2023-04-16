"""Module with the base class for 'classifiers' AI-tools.

Classifiers help to estimate if obtained market signals are True/False.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
import abc

from tools.base import BaseTool
from tools.base import ToolError

from data.fvalues import Algorithm

import pandas as pd
import numpy as np

from sklearn.linear_model import LogisticRegression
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
from sklearn.neighbors import KNeighborsClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.tree import DecisionTreeClassifier
from sklearn.svm import SVC

from sklearn.metrics import accuracy_score, f1_score

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
                 algorithm=Algorithm.GaussianNB,
                 classify=True,
                 probability=False,
                 use_buy=True,
                 use_sell=True
                ):
        """
            Initialize classifier class.

            Args:
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
                probability(bool): indicates if probabilities should be calculated.
                use_buy(bool): use buy signals in calculations.
                use_sell(bool): use sell signals in calculations.
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

        self._results_buy_est = None
        self._results_sell_est = None

        self._classify = classify
        self._probability = probability

        if use_buy is False and use_sell is False:
            raise ToolError("At least one type of signals should be enabled.")

        self._use_buy = use_buy
        self._use_sell = use_sell

        # Data used for learning (needed for metrics if requested)
        self._data_buy_learn = None
        self._data_sell_learn = None

        # Results to learn
        self._results_buy_learn = None
        self._results_sell_learn = None

        # Indicates if accuracy check was performed already
        self._est_buy_learn = None
        self._est_sell_learn = None

        self._data_to_est = None  # Columns to make estimations
        self._data_to_report = None  # Columns for reporting

    def get_buy_signals_to_compare(self):
        """
            Get actual and estimated buy signals.
            In the case of the streaming calculation, there may be less actual values than than estimated.
            In such case the exceeding estimated results will be trimmed.

            Raises:
                ToolError: calculation is not performed.

            Returns:
                numpy.array: adjusted actual signals to buy
                numpy.array: estimated signals to buy
        """

    def get_sell_signals_to_compare(self):
        """
            Get actual and estimated sell signals.
            In the case of the streaming calculation, there may be less actual values than than estimated.
            In such case the exceeding estimated results will be trimmed.

            Raises:
                ToolError: calculation is not performed.

            Returns:
                numpy.array: adjusted actual signals to sell
                numpy.array: estimated signals to sell
        """

    def get_buy_results(self, df):
        """
            Get buy signals as numpy array.

            Args:
                df(DataFrame): the initial data

            Returns:
                numpy array: buy signals data
        """
        # Create a results numpy array with 4 signals
        results_buy = np.where(df['buy-true'] == 1, 1, np.nan)
        results_buy = np.where(df['buy-false'] == 1, 0, results_buy)

        # Get rid of NaNs in np array
        results_buy = results_buy[~np.isnan(results_buy)]

        return results_buy

    def get_sell_results(self, df):
        """
            Get sell signals as numpy array.

            Args:
                df(DataFrame): the initial data

            Returns:
                numpy array: sell signals data
        """
        # Create a results numpy array with 4 signals
        results_sell = np.where(df['sell-true'] == 1, 1, np.nan)
        results_sell = np.where(df['sell-false'] == 1, 0, results_sell)

        # Get rid of NaNs in np array
        results_sell = results_sell[~np.isnan(results_sell)]

        return results_sell

    def get_buy_model(self):
        """
            Get model for buy signals estimation.

            Raises:
                ToolError: learning was not performed.

            Returns:
                model_buy: model to check buy signals
        """
        if self._model_buy is None:
            raise ToolError("Buy model was not trained.")

        return self._model_buy

    def get_sell_model(self):
        """
            Get model for sell signals estimation.

            Raises:
                ToolError: learning was not performed.

            Returns:
                model_sell: model to check sell signals
        """
        if self._model_buy is None:
            raise ToolError("Sell model was not trained.")

        return self._model_sell

    def need_buy(self, condition=True):
        """
            Indicates if buy model is needed based on the condition and self._use_buy

            For example, if the conditon is `self._model_buy is None` and we need to check for the need of buy signals,
            then the following call should be invoked: `self.need_buy(self._model_buy is None)`

            Args:
                condition(bool): the condition to check

            Returns:
                bool: the whole condition value
        """
        return condition and self._use_buy

    def need_sell(self, condition=True):
        """
            Indicates if buy model is needed based on the condition and self._use_sell

            For example, if the conditon is `self._model_sell is None` and we need to check for the need of sell signals,
            then the following call should be invoked: `self.need_sell(self._model_sell is None)`

            Args:
                condition(bool): the condition to check

            Returns:
                bool: the whole condition value
        """
        return condition and self._use_sell

    #################################################
    # Common methods for both buy and sell operations
    #################################################

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
        if len(self._results) == 0 or self._classify is False:
            raise ToolError("The calculation is not performed or no classification requested.")

        df = self.get_df()

        results_buy_actual = None
        results_buy_est = None
        results_sell_actual = None
        results_sell_est = None

        if self._use_buy:
            df_buy = self.add_buy_signals(df)

            results_buy_actual = self.get_buy_results(df_buy)
            results_buy_est = self._results_buy_est

            if len(results_buy_actual) != len(results_buy_est):
                results_buy_est = results_buy_est[:len(results_buy_actual)]

        if self._use_sell:
            df_sell = self.add_sell_signals(df)

            results_sell_actual = self.get_sell_results(df_sell)
            results_sell_est = self._results_sell_est

            if len(results_sell_actual) != len(results_sell_est):
                results_sell_est = results_sell_est[:len(results_sell_actual)]

        return (results_buy_actual, results_buy_est, results_sell_actual, results_sell_est)

    def get_df_signals_to_compare(self):
        """
            Get buy/sell signals to compare as a dataframe.

            The method is usually used just to print a dataframe with symbols for debugging purposes so it is common
            for both buy and sell signals.

            Returns:
                DataFrame: actual and estimated signals to compare.
        """
        len_buy = 0
        len_sell = 0

        results_buy_est, results_buy_actual, results_sell_est, results_sell_actual = self.get_signals_to_compare()

        if results_buy_est is not None:
            len_buy = len(results_buy_est)

        if results_sell_est is not None:
            len_sell = len(results_sell_est)

        signals = pd.DataFrame()
        signals.index = range(0, max(len_buy, len_sell))

        if self._use_buy:
            signals['buy-actual'] = pd.Series(results_buy_actual.astype('bool'))
            signals['buy-est'] = pd.Series(results_buy_est.astype('bool'))

        if self._use_sell:
            signals['sell-actual'] = pd.Series(results_sell_actual.astype('bool'))
            signals['sell-est'] = pd.Series(results_sell_est.astype('bool'))

        return signals

    def get_learning_instance(self):
        """
            Get instance for learning based of the chosen algorithm.

            Retunrs:
                instance to train a model.
        """
        if self._algorithm == Algorithm.LR:
            learn = LogisticRegression(class_weight='balanced')
        elif self._algorithm == Algorithm.LDA:
            learn = LinearDiscriminantAnalysis()
        elif self._algorithm == Algorithm.KNC:
            learn = KNeighborsClassifier()
        elif self._algorithm == Algorithm.GaussianNB:
            learn = GaussianNB()
        elif self._algorithm == Algorithm.DTC:
            learn = DecisionTreeClassifier(class_weight='balanced')
        elif self._algorithm == Algorithm.SVC:
            learn = SVC(class_weight='balanced')

        return learn

    def prepare(self):
        """
            Prepare the data for learning.

            Returns:
                df_buy(DataFrame): data for training buy signals distinguishing.
                df_sell(DataFrame): data for training sell signals distinguishing.
        """
        df_buy = pd.DataFrame()
        df_sell = pd.DataFrame()

        for rows in self._data_to_learn:
            # DataFrame for the current symbol
            df = self.get_df(rows)

            if self._use_buy:
                df_buy = pd.concat([df_buy, self.add_buy_signals(df)], ignore_index=True)

            if self._use_sell:
                df_sell = pd.concat([df_sell, self.add_sell_signals(df)], ignore_index=True)

        return (df_buy, df_sell)

    def learn(self):
        """
            Perform model(s) learning.
        """
        # Create the list of columns to exclude
        cols_to_exclude = []

        if self._use_buy:
            cols_to_exclude.extend(['buy-true', 'buy-false'])

        if self._use_sell:
            cols_to_exclude.extend(['sell-true', 'sell-false'])

        df_buy, df_sell = self.prepare()

        if self._use_buy:
            # Create a results numpy array with buy signals
            self._results_buy_learn = self.get_buy_results(df_buy)

            # Create a DataFrame without signals for learning
            self._data_buy_learn = df_buy.drop(['buy-true', 'buy-false'], axis=1)

            # Fill nan values with mean values (if any)
            if self._data_buy_learn.isnull().values.any():
                means_buy = self._data_buy_learn.mean()
                self._data_buy_learn = self._data_buy_learn.fillna(means_buy)

            buy_learn = self.get_learning_instance()            

            # Train buy model
            if len(self._data_buy_learn) == 0 or len(self._results_buy_learn) == 0:
                raise ToolError("No buy signals for learning")

            self._model_buy = buy_learn.fit(self._data_buy_learn, self._results_buy_learn)

        if self._use_sell:
            # Create a results numpy array with sell signals
            self._results_sell_learn = self.get_sell_results(df_sell)

            # Create a DataFrame without signals for learning
            self._data_sell_learn = df_sell.drop(['sell-true', 'sell-false'], axis=1)

            # Fill nan values with mean values (if any)
            if self._data_sell_learn.isnull().values.any():
                means_sell = self._data_sell_learn.mean()
                self._data_sell_learn = self._data_sell_learn.fillna(means_sell)

            sell_learn = self.get_learning_instance()

            # Train sell model
            if len(self._data_sell_learn) == 0 or len(self._results_sell_learn) == 0:
                raise ToolError("No sell signals for learning")

            self._model_sell = sell_learn.fit(self._data_sell_learn, self._results_sell_learn)

    ######################
    # Metrics
    ######################

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
        if (self._data_buy_learn is None and self._results_buy_learn is None) and \
           (self._data_sell_learn is None and self._results_sell_learn is None):
            raise ToolError("Can't calculate accurary as the learning wasn't performed or classification was not requested.")

        accuracy_buy_learn = None
        accuracy_sell_learn = None

        # Check if accuracy/f1 was checked before
        if self._use_buy:
            if self._est_buy_learn is None:
                self._est_buy_learn = self._model_buy.predict(self._data_buy_learn)

            accuracy_buy_learn = accuracy_score(self._results_buy_learn, self._est_buy_learn)
            total_accuracy_learn = accuracy_buy_learn

        if self._use_sell:
            if self._est_sell_learn is None:
                self._est_sell_learn = self._model_sell.predict(self._data_sell_learn)

            accuracy_sell_learn = accuracy_score(self._results_sell_learn, self._est_sell_learn)
            total_accuracy_learn = accuracy_sell_learn

        if self._use_buy and self._use_sell:
            total_accuracy_learn = (accuracy_buy_learn * len(self._results_buy_learn) + \
                                    accuracy_sell_learn * len(self._results_sell_learn)) / \
                                    (len(self._results_buy_learn) + len(self._results_sell_learn))

        return (accuracy_buy_learn, accuracy_sell_learn, total_accuracy_learn)

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
        if (self._data_buy_learn is None and self._results_buy_learn is None) and \
           (self._data_sell_learn is None and self._results_sell_learn is None):
            raise ToolError("Can't calculate f1 as the learning wasn't performed or classification was not requested.")

        f1_buy_learn = None
        f1_sell_learn = None

        # Check if accuracy/f1 was checked before
        if self._use_buy:
            if self._est_buy_learn is None:
                self._est_buy_learn = self._model_buy.predict(self._data_buy_learn)

            f1_buy_learn = f1_score(self._results_buy_learn, self._est_buy_learn)
            total_f1_learn = f1_buy_learn

        if self._use_sell:
            if self._est_sell_learn is None:
                self._est_sell_learn = self._model_sell.predict(self._data_sell_learn)

            f1_sell_learn = f1_score(self._results_sell_learn, self._est_sell_learn)
            total_f1_learn = f1_sell_learn

        if self._use_buy and self._use_sell:
            total_f1_learn = (f1_buy_learn * len(self._results_buy_learn) + \
                                    f1_sell_learn * len(self._results_sell_learn)) / \
                                    (len(self._results_buy_learn) + len(self._results_sell_learn))

        return (f1_buy_learn, f1_sell_learn, total_f1_learn)

    def get_est_accuracy(self):
        """
            Get the cumulative precision of signal estimation. The function compares the actual results with the estimated ones.

            Raises:
                ToolError: the calculation is not performed.

            Returns:
                float: buy accuracy
                float: sell accuracy
                float: cumulative accuracy
        """
        accuracy_buy = None
        accuracy_sell = None

        if self._model_buy is False and self._model_sell is False:
            raise ToolError("At least one model should be trained with requested classification to get estimation accuracy.")

        if len(self._results) == 0:
            raise ToolError("The calculation should be performed to get accuracy score.")

        results_buy_actual, results_buy_est, results_sell_actual, results_sell_est = self.get_signals_to_compare()

        # Get the accuracy score
        if self._use_buy:
            accuracy_buy = accuracy_score(results_buy_actual, results_buy_est)
            total_accuracy = accuracy_buy

        if self._use_sell:
            accuracy_sell = accuracy_score(results_sell_actual, results_sell_est)
            total_accuracy = accuracy_sell

        if self._use_buy and self._use_sell:
            total_accuracy = (accuracy_buy * len(results_buy_actual) + accuracy_sell * len(results_sell_actual)) / \
                             (len(results_buy_actual) + len(results_sell_actual))

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
        f1_buy = None
        f1_sell = None

        if self._model_buy is False and self._model_sell is False:
            raise ToolError("At least one model should be trained with requested classification to get estimation f1.")

        if len(self._results) == 0:
            raise ToolError("The calculation should be performed to get f1 score.")

        results_buy_actual, results_buy_est, results_sell_actual, results_sell_est = self.get_signals_to_compare()

        # Get the f1 score
        if self._use_buy:
            f1_buy = f1_score(results_buy_actual, results_buy_est)
            total_f1 = f1_buy

        if self._use_sell:
            f1_sell = f1_score(results_sell_actual, results_sell_est)
            total_f1 = f1_sell

        if self._use_buy and self._use_sell:
            total_f1 = (f1_buy * len(results_buy_actual) + f1_sell * len(results_sell_actual)) / \
                             (len(results_buy_actual) + len(results_sell_actual))

        return (f1_buy, f1_sell, total_f1)

    ##################
    # Abstract methods
    ##################

    @abc.abstractmethod
    def get_df(self, rows=None):
        """
            Get the DataFrame for learning/estimation based on the initial DataFrame.

            Args:
                rows(list): data to use. Data for estimation is used if not specified.

            Returns:
                DataFrame: data ready for learning/estimation
        """

    @abc.abstractmethod
    def add_buy_signals(self, df):
        """
            Add buy signals to the DataFrame.

            If buy signals are not used in a derived tool, override this method to raise a ToolError.

            Args:
                df(DataFrame): the initial data

            Raises:
                ToolError: the method is not used in a derived tool.
        """

    @abc.abstractmethod
    def add_sell_signals(self, df):
        """
            Add sell signals to the DataFrame.

            If buy signals are not used in a derived tool, override this method to raise a ToolError.

            Args:
                df(DataFrame): the initial data

            Raises:
                ToolError: the method is not used in a derived tool.
        """

    @abc.abstractmethod
    def calculate(self):
        """
            Perform a calculation.
        """
