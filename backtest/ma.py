"""Moving average vs. price cross backtesting strategy.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from backtest.base import BackTest
from backtest.base import BackTestError

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
        """
            Initializes the MA Cross stragegy implementation.

            Args:
                period(int): period for moving average.
                is_simple(bool): indicates if SMA or EMA should be used.

            Raises:
                BackTestError: the period is too small.
        """
        super().__init__(**kwargs)

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
        return self.exec().get_val()['ma'] <= self.exec().get_close(True)

    def do_tech_calculation(self, ex):
        """
            Perform technical calculation for the strategy.

            Args:
                ex(BackTestOperations): Operations instance class.
        """
        df = pd.DataFrame(ex.data().get_rows())

        if self.__is_simple:
            ma = ta.sma(df[ex.data().close], length = self._period)
        else:
            ma = ta.ema(df[ex.data().close], length = self._period)

        # Append data to the calculations dataset
        ex.add_col(name='ma', data=ma, dtype=float)

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

            if self.do_cycle(row) == False:
                continue

            ############################################################################
            # Check if we need to close the positions because the trend changed recently
            ############################################################################
            if self.exec().get_max_positions():
                if self.exec().trend_changed(self.is_uptrend()):
                    self.exec().close_all()
                elif self.is_uptrend() != self.exec().is_long():
                    if (self.is_uptrend() and self.signal_buy()) or (self.is_uptrend() is False and self.signal_sell()):
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
