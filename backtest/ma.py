"""Moving average vs. price cross backtesting strategy.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

from backtest.base import BackTest
from backtest.base import BackTestError
from backtest.base import BackTestEvent

from data.fvalues import Quotes

import pandas as pd
import pandas_ta as ta

class MA(BackTest):
    """
        Moving average vs. price cross backtesting strategy implementation.

        If MA goes above the price, it is a signal to buy. Otherwise, it is a signal to sell.
    """
    def __init__(self,
                 period,
                 is_simple=True,
                 **kwargs):
        super().__init__(**kwargs)
        """
            Initializes the MA Cross stragegy implementation.

            Args:
                period(int): period for moving average.
                is_simple(bool): indicates if SMA or EMA should be used.

            Raises:
                BackTestError: the period is too small.
        """

        # Indicates if the backtesting should use SMA or EMA
        self.__is_simple = is_simple

        # Period for MA calculation
        if period < 0:
            raise BackTestError(f"period can't be less than 0. Specified value is {period}")
        self._period = period

    def is_simple(self):
        """
            Gets the MA type flag.

            Retuns:
                bool: True if SMA, EMA otherwise.
        """
        return self.__is_simple

    def skip_criteria(self, index):
        """
            Check if the current cycle should be skipped.

            The cycle should be skipped if the MA is not calculated yet.

            Args:
                index(int): index of the current cycle.
        """
        return index < self._period

    def is_uptrend(self):
        """
            Check if the current trend is uptrend according to the strategy.

            Returns:
                True if uptrend, False otherwise.
        """
        return self.exec().get_tech_val() <= self.exec().get_close()

    def do_tech_calculation(self, ex):
        """
            Perform technical calculation for the strategy.

            Args:
                ex(BackTestOperations): Operations instance class.
        """
        df = pd.DataFrame(ex.data().get_rows())

        if self.__is_simple:
            ex.append_tech(ta.sma(df[Quotes.AdjClose], length = self._period))
        else:
            ex.append_tech(ta.ema(df[Quotes.AdjClose], length = self._period))

        # Skip data when no MA is calculated.
        self.set_offset(self.get_offset() + self._period)

    def do_calculation(self):
        """
            Perform strategy calculation.

            Raises:
                BackTestError: not enough data for calculation.
        """
        rows = self.get_main_data().get_rows()
        length = len(rows)

        if length < self._period:
            raise BackTestError(f"Not enough data to calculate a period: {length} < {self._period}")

        ######################################
        # Perform the global calculation setup
        ######################################
        self.setup()

        ############################################################
        # Iterate through all rows and calculate the required values
        ############################################################

        for row in rows:

            ####################################################################################################
            # Setup cycle calculations if current cycle shouldn't be skipped (because of offset or lack of data)
            ####################################################################################################

            if self.do_cycle(rows.index(row)) == False:
                continue

            ############################################################################
            # Check if we need to close the positions because the trend changed recently
            ############################################################################

            if self.exec().get_max_positions() and self.exec().trend_changed(self.is_uptrend()) or self.any_signal():
                self.exec().close_all()

            ########################
            # Open positions
            ########################

            # Depending on trend, open a long / short position(s) if we have enough cash / margin
            if self.is_uptrend() == True and self.exec().get_short_positions() == 0:
                self.exec().open_long_max()
 
            if self.is_uptrend() == False and self.exec().get_max_positions() == 0 and self.exec().get_long_positions() == 0:
                self.exec().open_short_max()

            ##############################
            # Teardown the cycle
            ##############################

            self.tear_down()
