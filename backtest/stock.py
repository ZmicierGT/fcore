"""Backtesting classes related to stock backtesting.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from backtest.base import BackTestData, BackTestOperations, BackTestError
from data.fvalues import StockQuotes, Weighted, sector_titles

from math import inf

class StockData(BackTestData):
    """
        The class represents stock data for backtesting.
    """
    def __init__(self,
                 div_tax=0,
                 **kwargs):
        """
            Initializes the stock data class.

            Args:
                div_tax(float): dividend tax
        """
        super().__init__(**kwargs)

        # the default close price column to make calculations (StockQuote.AdjClose for the stock security type).
        self._close = StockQuotes.AdjClose

        if div_tax < 0 or div_tax >= 100:
            raise BackTestError(f"Dividend tax can't be less than 0 or >= 100. {div_tax} is specified.")
        self.div_tax = div_tax

    ############
    # Properties
    ############

    @property
    def sector(self):
        """
            Get the stock sector.

            Returns:
                str: the sector of the stock.
        """
        sector = None

        if self._info is not None and 'sector' in self._info:
            sector = self._info['sector']

        if sector not in sector_titles:
            sector = None

        return sector

    ###############
    # Methods
    ###############

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

        # The future incoming yield
        self._future_yield = 0

    ############
    # Properties
    ############

    @property
    def div_tax(self):
        """
            Get the dividend tax.

            Returns:
                float: the dividend tax
        """
        return self.data.div_tax

    #############################################################
    # General methods with calculations for a particular symbol
    #############################################################

    def get_current_yield(self):
        """
            Get the current yield.

            Returns:
                float: the yield incoming in the current cycle.
        """
        idx = self.get_index()

        if idx is None:
            return

        current_yield = 0

        # Check if we have opened long positions at ex_date
        if self.data.rows[idx][StockQuotes.ExDividends] != 0 and self._long_positions > 0:
            self._future_yield = self._long_positions * self.data.rows[idx][StockQuotes.ExDividends]

        # Calculate dividends to pay for long positions which were opened at ex_date
        if self.data.rows[idx][StockQuotes.PayDividends] != 0 and self._future_yield > 0:
            current_yield = self._future_yield
            self._future_yield = 0

        if current_yield:
            self.get_caller()._lg.highlight(f"At {self.get_datetime_str()} incoming yield for {self.data.title} is {current_yield}")

        return current_yield

    def check_for_split(self):
        """
            Check for a stock split and apply split to the portfolio if any.
        """
        idx = self.get_index()

        if idx is None:
            return

        if self.get_long_positions() == 0:
            return

        ratio = self.data.rows[idx][StockQuotes.Splits]
        old_close = self.data.rows[idx - 1][StockQuotes.Close]

        if ratio != 1 and idx != 0:
            long_before = self.get_long_positions()

            if self.is_long():
                # TODO LOW Think if excessive cash should be treated as profit or loss (depending on the price of opening the position)
                excess = self._long_positions * ratio - round(self._long_positions * ratio)

                # Add excess cash to the cash balance (in case of any decimal parts of share number)
                if excess != 0:
                    self.get_caller().add_cash(excess * self.get_close())

                self._long_positions = int(round(self._long_positions * ratio))

            self.get_caller()._lg.highlight(f"At {self.get_datetime_str()} New positions after split of {self.data.title} "
                                  f"for {self.data.title}: {self.get_long_positions()} "
                                  f"Positions before split: {long_before}")

    def apply_other_balance_changes(self):
        """
            Apply the current yield to the portfolio.
        """
        self.check_for_split()

        current_yield = self.get_current_yield()

        # TODO HIGH no need to perform yield deducting any more
        if current_yield != 0:
            if current_yield > 0:
                txt = 'added'

                if self.div_tax:
                    tax = current_yield * self.div_tax / 100
                    self.get_caller().add_other_expense(tax)

                    current_yield = current_yield - tax

                self.get_caller().add_other_profit(current_yield)
                self._total_profit += current_yield
            else:
                txt = 'deducted'

                self.get_caller().add_other_expense(current_yield)

            log = f"At {self.get_datetime_str()} {txt} {current_yield} dividends for {self.data.title}. The cash balance is {round(self.get_caller().get_cash(), 2)}."
            self.get_caller()._lg.plain(log)

    def get_total_value(self):
        """
            Get the total value of positions opened for the particular symbol.

            Returns:
                float: the total value of the all opened positions.
        """
        total_value = super().get_total_value()
        total_value += self._future_yield

        return total_value
