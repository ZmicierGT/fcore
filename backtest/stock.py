"""Backtesting classes related to stock backtesting.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from backtest.base import BackTestData
from backtest.base import BackTestOperations

from data.fvalues import StockQuotes

from itertools import repeat

class StockData(BackTestData):
    """
        The class represents stock data for backtesting.
    """
    def __init__(self,
                 **kwargs):
        """
            Initializes the stock data class.
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

class StockOperations(BackTestOperations):
    """
        The class represents operations performed on a stock.
    """
    def __init__(self, **kwargs):
        """
            Initializes the stock operations instance.
        """
        super().__init__(**kwargs)

        # Number of positions to get dividends at payment date
        self._yield_positions = 0

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

        # Check if we have opened long positions at ex_date
        if self.data().get_rows()[self.get_caller_index()][StockQuotes.ExDividends] != None and self._long_positions > 0:
            self._yield_positions = self._long_positions

        # Calculate dividends to pay for long positions which were opened at ex_date
        if self.data().get_rows()[self.get_caller_index()][StockQuotes.PayDividends] != None and self._yield_positions > 0:
            current_yield = self.data().get_rows()[self.get_caller_index()][StockQuotes.PayDividends] * self._yield_positions
            self._yield_positions = 0

        # Calculate dividends for short positions to get payed to a borrower
        if self.data().get_rows()[self.get_caller_index()][StockQuotes.ExDividends] != None and self._short_positions > 0:
            current_yield = self.data().get_rows()[self.get_caller_index()][StockQuotes.ExDividends] * self._short_positions

        return current_yield

    def check_for_split(self):
        """
            Check for a stock split and apply split to the portfolio if any.
        """
        ratio = self.data().get_rows()[self.get_caller_index()][StockQuotes.Splits]
        old_close = self.data().get_rows()[self.get_caller_index() - 1][StockQuotes.Close]

        if ratio != 1 and self.get_caller_index() != 0:
            if self.is_long():
                margin_positions = self._long_positions - self._long_positions_cash
                self._long_positions_cash *= ratio

                excess = self._long_positions_cash - round(self._long_positions_cash)

                # Add excess cash to the cash balance (in case of any decimal parts of share number)
                if excess != 0:
                    self._long_positions_cash = round(self._long_positions_cash)
                    self.get_caller().add_cash(excess * self.get_close())
                else:
                    self._long_positions_cash = int(self._long_positions_cash)  # Get rid of possible .0

                # In the case of margin positions, calculate total margin used and readjust all the margin portfolio.
                # The adjustment is implemented as a comission and spread free closure of all margin positions with
                # immediate spread and comission free reopening of new positions withing the previous margin
                # buying power.
                if margin_positions != 0:
                    buying_power = margin_positions * old_close

                    delta = 0

                    # Close all the margin positions (commission and spread free)
                    for _ in range(margin_positions):
                        delta += old_close - self._portfolio.pop()

                    self.get_caller().add_cash(delta)

                    # Open (spread and commission free) new long margin positions withing
                    # the previous margin buying power limit
                    new_margin_positions = round(buying_power / self.get_buy_price())

                    self._portfolio = []
                    self._portfolio.extend(repeat(self.get_buy_price(), new_margin_positions))

                    self._long_positions = self._long_positions_cash + new_margin_positions
            else:
                # Handling short positions
                if self._short_positions != 0:
                    buying_power = self._short_positions * old_close

                delta = 0

                # Close (commission and spread fee) all short positions
                for _ in range(self._short_positions):
                    delta += self._portfolio.pop() - old_close

                self.get_caller().add_cash(delta)

                # Open (spread and commission free) new short positions withing the previously available margin
                new_short_positions = round(buying_power / self.get_buy_price())

                self._portfolio = []
                self._portfolio.extend(repeat(self.get_sell_price(), new_short_positions))

                self._short_positions = new_short_positions

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
