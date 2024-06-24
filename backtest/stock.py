"""Backtesting classes related to stock backtesting.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from backtest.base import BackTestData, BackTestOperations, BackTestError
from data.fvalues import StockQuotes, Weighted, sector_titles

from itertools import repeat
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
        return self.data().div_tax

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
        if self.data().get_rows()[idx][StockQuotes.ExDividends] != 0 and self._long_positions > 0:
            self._future_yield = self._long_positions * self.data().get_rows()[idx][StockQuotes.ExDividends]

        # Calculate dividends to pay for long positions which were opened at ex_date
        if self.data().get_rows()[idx][StockQuotes.PayDividends] != 0 and self._future_yield > 0:
            current_yield = self._future_yield
            self._future_yield = 0

        # Calculate dividends for short positions to get payed to a borrower
        if self.data().get_rows()[idx][StockQuotes.ExDividends] != 0 and self._short_positions > 0:
            current_yield = -abs(self.data().get_rows()[idx][StockQuotes.ExDividends] * self._short_positions)

        # TODO MID Check why it is commented
        # if current_yield:
        #     self.get_caller().log(f"At {self.get_datetime_str()} incoming yield for {self.title} - {current_yield}")

        return current_yield

    def check_for_split(self):
        """
            Check for a stock split and apply split to the portfolio if any.
        """
        idx = self.get_index()

        if idx is None:
            return

        if self.get_long_positions() == 0 and self._short_positions == 0:
            return

        ratio = self.data().get_rows()[idx][StockQuotes.Splits]
        old_close = self.data().get_rows()[idx - 1][StockQuotes.Close]

        if ratio != 1 and idx != 0:
            long_before = self.get_long_positions()
            long_cash_before = self._long_positions_cash
            short_before = self._short_positions

            if self.is_long():
                margin_positions = self._long_positions - self._long_positions_cash
                self._long_positions_cash *= ratio

                # TODO LOW Think if excessive cash should be treated as profit or loss (depending on the price of opening the position)
                excess = self._long_positions_cash - round(self._long_positions_cash)

                # Add excess cash to the cash balance (in case of any decimal parts of share number)
                if excess != 0:
                    self._long_positions_cash = round(self._long_positions_cash)
                    self.get_caller().add_cash(excess * self.get_close())

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
                    self._long_positions = self._long_positions_cash
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

            self.get_caller().log(f"At {self.get_datetime_str()} New positions after split of {self.title} "
                                  f"(total long / cash long / short) for {self.title}: "
                                  f"{self.get_long_positions()} / {self._long_positions_cash} / {self._short_positions} "
                                  f"Positions before split: {long_before} {long_cash_before} {short_before}")

    def apply_other_balance_changes(self):
        """
            Apply the current yield to the portfolio.
        """
        self.check_for_split()

        current_yield = self.get_current_yield()

        if current_yield != 0:
            if current_yield > 0:
                txt = 'Added'

                if self.div_tax:
                    tax = current_yield * self.div_tax / 100
                    self.get_caller().add_other_expense(tax)

                    current_yield = current_yield - tax

                self.get_caller().add_other_profit(current_yield)
                self._total_profit += current_yield
            else:
                txt = 'Deducted'

                self.get_caller().add_other_expense(current_yield)

            log = f"{txt} {current_yield} dividends for {self.title}. The cash balance is {round(self.get_caller().get_cash(), 2)}."
            self.get_caller().log(log)

    def get_total_value(self):
        """
            Get the total value of positions opened for the particular symbol.

            Returns:
                float: the total value of the all opened positions.
        """
        total_value = super().get_total_value()
        total_value += self._future_yield

        return total_value
