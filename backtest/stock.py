"""Backtesting classes related to stock backtesting.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

from backtest.base import BackTestData
from backtest.base import BackTestOperations
from backtest.base import BackTestError

from data.fvalues import Rows

from enum import IntEnum

class StockData(BackTestData):
    """
        The class represents stock data for backtesting.
    """
    def __init__(self,
                 use_yield=0,
                 yield_interval=0,
                 **kwargs):
        super().__init__(**kwargs)
        """
            Initializes the stock data class.

            Args:
                use_yield(float): the yield which should be used for calculation. If the value i 0, then incoming yield from
                    the database will be used (if any).
                yield_interval(int): interval in days in which the yield comes. Use 0 if you want to use yield values
                    from the database.

            Raises:
                BackTestError: incorrect values in arguments.
        """
        # Annual dividend/coupon yield in percent. Overrides the yield in dataset if != 0
        if use_yield < 0 or use_yield > 100:
            raise BackTestError(f"use_yield can't be less than 0% or more than 100%. Specified value is {use_yield}")
        self._use_yield = use_yield

        # Yield interval (days). It is relevant only if use_yield is set
        if yield_interval < 0:
            raise BackTestError(f"yield_interval can't be less than 0. Specified value is {yield_interval}")
        self._yield_interval = yield_interval

    def create_exec(self, caller):
        """
            Create StockOperations instance based on BackTestData instance.
            StockData is a container for data used for calculation and the usage of every instance of this class is thread safe.
            Several StockOperations may be associated with a single StockData. StockOperations class is not thread safe
            and it represents an operations performed on a certain symbol in the portfolio.

            Args:
                StockData: backtesting data class

            Returns:
                StockOperations: instance for performing operations for a particular symbol in the portfolio.
        """
        return StockOperations(data=self, caller=caller)

    def get_use_yield(self):
        """
            Get the pre-defined yield.

            Returns:
                float: pre-defined yield value.
        """
        return self._use_yield

    def get_yield_interval(self):
        """
            Get the pre-defined yield interval.

            Returns:
                float: pre-defined yield interval value.
        """        
        return self._yield_interval

    def add_yield_counter(self, days_delta):
        """
            Increase the counter till the next yield.

            Args:
                days_delta(int): the days to add to the yield counter
        """
        self._yield_counter += days_delta

class StockOperations(BackTestOperations):
    """
        The class represents operations performed on a stock.
    """
    def __init__(self, **kwargs):
        """
            Initializes the stock operations instance.
        """
        super().__init__(**kwargs)

        # Counter till dividend yield
        self._yield_counter = 0

    #############################################################
    # General functions with calculations for a particular symbol
    #############################################################

    def apply_days_counter(self, days_delta):
        """
            Increase the counter till the next yield.

            Args:
                days_delta(int): the days to add to the yield counter
        """
        self._yield_counter += days_delta

    def get_current_yield(self):
        """
            Get the current yield (pre-defined of from the database).

            Returns:
                float: the yield incoming in the current counter.
        """
        current_yield = 0

        # Check if pre-defined yield is incoming
        if self.data().get_use_yield() != 0 and self.data().get_yield_interval() <= self._yield_counter and self.get_max_positions() > 0:
            current_yield = self.get_max_positions() * self.get_quote() * self.data().get_use_yield() / 100 / (240 / self.data().get_yield_interval())
            self._yield_counter = 0

        # Check if we have dividends today according to the dataset
        if self.data().get_use_yield() == 0 and self.data().get_rows()[self.get_caller_index()][Rows.Dividends] != None and self.get_max_positions() > 0:
            current_yield = self.data().get_rows()[self.get_caller_index()][Rows.Dividends] * self.get_max_positions()

        return current_yield

    def apply_other_balance_changes(self):
        """
            Apply the current yield to the portfolio.
        """
        current_yield = self.get_current_yield()

        if current_yield != 0:
            if self.is_long():
                self.get_caller().add_other_profit(current_yield)
            else:
                self.get_caller().add_other_expense(current_yield)
