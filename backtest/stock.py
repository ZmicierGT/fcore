"""Backtesting classes related to stock backtesting.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from backtest.base import BackTestData, BackTestOperations, BackTestError, ChartingBar, SecState
from data.fvalues import StockQuotes, sector_titles

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

        # For stocks, the charting bar is based on adjusted quotes (used for reporting/charting
        # and indicator calculation only - never for order execution or valuation).
        # TODO LOW: adj_* columns are back-adjusted using dividends/splits from the rest of the window (future data
        #  for each bar), so indicator signals computed on them deviate from real-time observables.
        #  Think of using raw prices with dividends-as-cash instead.
        self._charting = ChartingBar(StockQuotes.AdjOpen, StockQuotes.AdjHigh, StockQuotes.AdjLow, StockQuotes.AdjClose)

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

    def process_yield(self):
        """
            Get the current yield.

            Returns:
                float: the yield incoming in the current cycle.
        """
        idx = self.get_index()

        if idx is None:
            return

        incoming_yield = 0

        # Check if we have opened long positions at ex_date
        if self.data.rows[idx][StockQuotes.ExDividends] != 0 and self._long_positions > 0:
            self._future_yield = self._long_positions * self.data.rows[idx][StockQuotes.ExDividends]

        # Calculate dividends to pay for long positions which were opened at ex_date
        if self.data.rows[idx][StockQuotes.PayDividends] != 0 and self._future_yield > 0:
            incoming_yield = self._future_yield
            self._future_yield = 0

        if incoming_yield:
            self.caller._lg.highlight(f"At {self.get_datetime_str()} incoming yield for {self.data.title} is {incoming_yield}")

        return incoming_yield

    def check_for_split(self):
        """
            Check for a stock split and apply split to the portfolio if any.
        """
        idx = self.get_index()

        if idx is None:
            return

        if self.long_positions == 0:
            return

        ratio = self.data.rows[idx][StockQuotes.Splits]

        if ratio != 1 and idx != 0:
            long_before = self.long_positions

            if self.is_long():
                entitled = self._long_positions * ratio
                # Round down to avoid crediting shares the holder is not entitled to
                new_positions = int(entitled + 1e-9)  # TODO Low Think if we need 1e-9
                fractional = entitled - new_positions

                if fractional < 1e-9:
                    fractional = 0

                if len(self._portfolio_cash) != self._long_positions:
                    raise BackTestError(f"Cost basis lots out of sync with positions for {self.data.title}: "
                                        f"{len(self._portfolio_cash)} != {self._long_positions}")

                old_prices = self._portfolio_cash
                price_idx = 0
                offset = 0.0

                def consume(length):
                    nonlocal price_idx, offset

                    cost = 0.0

                    while length > 1e-12 and price_idx < len(old_prices):
                        take = min(length, 1.0 - offset)
                        cost += take * old_prices[price_idx]
                        offset += take
                        length -= take

                        if offset >= 1.0 - 1e-12:
                            price_idx += 1
                            offset = 0.0

                    return cost

                if fractional:
                    # Cash the fractional part using the oldest cost basis (FIFO)
                    cash = fractional * self.open
                    cash_basis = consume(fractional / ratio)
                    self.caller.add_cash(cash)

                    self._total_profit += cash - cash_basis

                    self.caller._lg.plain(f"At {self.get_datetime_str()} added {round(cash, 2)} of cash for the fractional "
                                          f"share of {self.data.title} at split. The cash balance is {round(self.caller.cash, 2)}.")

                # Slice remaining costs into post-split shares in FIFO order
                new_prices = []
                chunk = 1.0 / ratio if new_positions else 0

                for i in range(new_positions):
                    if i == new_positions - 1:
                        # Consume the remainder to avoid floating-point leftovers
                        cost = 0.0

                        while price_idx < len(old_prices):
                            cost += (1.0 - offset) * old_prices[price_idx]
                            price_idx += 1
                            offset = 0.0

                        new_prices.append(cost)
                    else:
                        new_prices.append(consume(chunk))

                self._portfolio_cash = new_prices
                self._long_positions = new_positions

            self.caller._lg.highlight(f"At {self.get_datetime_str()} new positions after split of {self.data.title}: "
                                  f"{self.long_positions} "
                                  f"Positions before split: {long_before}")

    def _book_yield(self, current_yield):
        """
            Apply the yield to the cash balance and statistics.

            Args:
                current_yield(float): the yield to apply.
        """
        # TODO HIGH no need to perform yield deducting any more
        if not current_yield:
            return

        if current_yield > 0:
            txt = 'added'

            if self.div_tax:
                tax = current_yield * self.div_tax / 100
                self.caller.add_other_expense(tax)

                current_yield = current_yield - tax

            self.caller.add_other_profit(current_yield)
            self._total_profit += current_yield
        else:
            txt = 'deducted'

            self.caller.add_other_expense(current_yield)

        # The main dataset date is used as there may be no fresh bar (e.g. when paying at delisting).
        log = f"At {self.caller.exec().get_datetime_str()} {txt} {current_yield} dividends for {self.data.title}. The cash balance is {round(self.caller.cash, 2)}."
        self.caller._lg.plain(log)

    def apply_other_balance_changes(self):
        """
            Apply the current yield to the portfolio.
        """
        # Delisted and skipped securities keep their (worthless) positions: no splits or dividends
        # of the ticker which may have reappeared are applied to them.
        if self._state is not SecState.Active:
            return

        self.check_for_split()

        self._book_yield(self.process_yield())

    def _on_delisting(self):
        """
            Pay out the yield still owed to the holder at the moment the security disappears.
        """
        if self._future_yield:
            yield_paid = self._future_yield

            self._future_yield = 0
            self._book_yield(yield_paid)

    @property
    def total_value(self):
        """
            Get the total value of positions opened for the particular symbol.

            Returns:
                float: the total value of the all opened positions.
        """
        total_value = super().total_value
        total_value += self._future_yield

        return total_value
