"""Backtesting classes related to stock backtesting.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""

from backtest.base import BackTestData
from backtest.base import BackTestOperations
from backtest.base import BackTestError

from data.fvalues import StockQuotes

class StockData(BackTestData):
    """
        The class represents stock data for backtesting.
    """
    def __init__(self,
                 **kwargs):
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
        super().__init__(**kwargs)

        # the default close price column to make calculations (StockQuote.AdjClose for the stock security type).
        self._close = StockQuotes.AdjClose

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
    # General methods with calculations for a particular symbol
    #############################################################

    def get_current_yield(self):
        """
            Get the current yield.

            Returns:
                float: the yield incoming in the current cycle.
        """
        current_yield = 0

        # Check if we have dividends today according to the dataset
        if self.data().get_rows()[self.get_caller_index()][StockQuotes.PayDividends][0] != None and self.get_max_positions() > 0:
            current_yield = self.data().get_rows()[self.get_caller_index()][StockQuotes.PayDividends][0] * self.get_max_positions()

        return current_yield

    # TODO HIGH Implement it
    def check_for_split(self):
        """
            Check for a stock split and apply split to the portfolio if any.
        """
        ratio = self.data().get_rows()[self.get_caller_index()][StockQuotes.Splits][0]

        if ratio != 1 and self.get_caller_index() != 0:
            pass

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

        self.check_for_split()
