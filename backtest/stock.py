"""Backtesting classes related to stock backtesting.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from backtest.base import BackTestData, BackTestOperations, BackTestError
from data.fvalues import StockQuotes, Weighted

from itertools import repeat
from math import inf

# These are helper functions which are non class members. Calling standalone functions is preferred than
# creating a derived class for BackTest for every security type because each backtest may include different
# security types in the future.

sector_titles = ['Technology', 'Financial Services', 'Healthcare', 'Consumer Cyclical', 'Industrials', \
           'Communication Services', 'Consumer Defensive', 'Energy', 'Basic Materials', 'Real Estate', 'Utilities']

def get_sector_values(all_exec):
    """
        Calculate value for each sector

        Args:
            all_exec(list of StockOperations): list of all execs to calculate the value

        Returns:
            dict: sectors with values
    """
    sectors_dict = {key: None for key in sector_titles}

    for ex in all_exec:
        if ex.get_index():
            if ex.sector == 'Unknown':
                continue

            if sectors_dict[ex.sector] is None:
                sectors_dict[ex.sector] = 0

            sectors_dict[ex.sector] += ex.get_total_value()

    return sectors_dict

def get_min_sector_value(sectors_dict, nonzero=False):
    """
        Get the minimum value of the sector (among those which present in datasets).

        Args:
            sectors_dict(dict): dictionary with value per sector.
            nonzero(bool): if zero values should be processed

        Returns:
            float: the minimum sector value (including 0)
    """
    if nonzero:
        key = min(sectors_dict, key=lambda x: sectors_dict[x] or inf)
    else:
        key = min(sectors_dict, key = lambda x: sectors_dict[x] if sectors_dict[x] != None else inf)

    return sectors_dict[key]

def get_min_sector_keys(sectors_dict, nonzero=False):
    """
        Get the list of sector keys with minimum values

        Args:
            sectors_dict(dict): dictionary with value per sector
            nonzero(bool): if zero values should be processed

        Returns:
            list: the list of sector titles with minimum value
    """
    min_value = get_min_sector_value(sectors_dict, nonzero)

    return [key for key, value in sectors_dict.items() if value == min_value]

def get_max_sector_value(sectors_dict):
    """
        Get the maximum value of the sector (among those which present in datasets).

        Args:
            sectors_dict(dict): dictionary with value per sector.

        Returns:
            float: the maximum sector value
    """
    key = max(sectors_dict, key = lambda x: sectors_dict[x] if sectors_dict[x] != None else -1)

    return sectors_dict[key]

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

        if self.get_caller().get_weighted() == Weighted.Cap and 'cap' not in self.data().get_rows().dtype.names:
            raise BackTestError(f"No 'cap' column in dataset for {self.data().get_title()} but it is required by the weighting method.")

        # The future incoming yield
        self._future_yield = 0

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
        sector = ''

        if self.data()._info is not None and 'sector' in self.data()._info:
            sector = self.data()._info['sector']

        if sector not in sector_titles:
            return 'Unknown'

        return sector

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
            current_yield = self.data().get_rows()[idx][StockQuotes.ExDividends] * self._short_positions

        # if current_yield:
        #     self.get_caller().log(f"At {self.get_datetime_str()} incoming yield for {self.data().get_title()} - {current_yield}")

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

            self.get_caller().log(f"At {self.get_datetime_str()} New positions after split of {self.data().get_title()} "
                                  f"(total long / cash long / short) for {self.data().get_title()}: "
                                  f"{self.get_long_positions()} / {self._long_positions_cash} / {self._short_positions} "
                                  f"Positions before split: {long_before} {long_cash_before} {short_before}")

    def apply_other_balance_changes(self):
        """
            Apply the current yield to the portfolio.
        """
        current_yield = self.get_current_yield()

        if current_yield != 0:
            if self.is_long():
                self.get_caller().add_other_profit(current_yield)
                self._total_profit += current_yield

                log = f"Added {current_yield} dividends for {self.data().get_title()}. The cash balance is {round(self.get_caller().get_cash(), 2)}."
                self.get_caller().log(log)
            else:
                self.get_caller().add_other_profit(-abs(current_yield))
                self._total_profit -= current_yield

                log = f"Deducted {current_yield} dividends for {self.data().get_title()}. The cash balance is {round(self.get_caller().get_cash(), 2)}."
                self.get_caller().log(log)

        self.check_for_split()

    def get_total_value(self):
        """
            Get the total value of positions opened for the particular symbol.

            Returns:
                float: the total value of the all opened positions.
        """
        total_value = super().get_total_value()
        total_value += self._future_yield

        return total_value
