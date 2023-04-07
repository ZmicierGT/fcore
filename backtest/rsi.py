"""RSI strategy implementation.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

from backtest.base import BackTest
from backtest.base import BackTestError

from data.fvalues import Quotes

import pandas as pd
import pandas_ta as ta

class RSI(BackTest):
    """
        RSI backtesting strategy implementation.

        If RSI crosses support line from behind, it is a signal to buy. If RSI crosses the resistance line from above,
        it is signal to sell. Multiple symbols may be used.
    """
    def __init__(self,
                 period=14,
                 support=30,
                 resistance=70,
                 to_short=False,
                 **kwargs):
        """
            Initializes RSI backtesting strategy implementation.

            Args:
                period(int): period for RSI calculation.
                support(int): RSI support value.
                resistance(int): RSI resistance value.
                to_short(bool): Indicates if a short position should be opened when we get a signal to sell.

            Raises:
                BackTestError: incorrect arguments values.
        """
        super().__init__(**kwargs)

        # Period for RSI calculation
        if period < 0:
            raise BackTestError(f"period can't be less than 0. Specified value is {period}")
        self._period = period

        # Support and resistance values for the strategy
        if support < 0 or support > 99:
            raise BackTestError(f"support can't be less than 0 or more than 99. Specified value is {support}")
        self.__support = support

        if resistance < 1 or resistance > 100:
            raise BackTestError(f"resistance can't be less than 1 or more than 100. Specified value is {resistance}")
        self.__resistance = resistance

        if support >= resistance:
            raise BackTestError(f"Support can't be more or equal than resistance: {support} >= {resistance}")

        # Indicates if we should open short positions when price goes below resistance
        self.__to_short = to_short

        # Expect multi symbol data
        self._is_multi = True

    def skip_criteria(self, index):
        """
            Check if the current cycle should be skipped.

            The cycle should be skipped if the MA is not calculated yet.

            Args:
                index(int): index of the current cycle.
        """
        return index < self._period

    def do_tech_calculation(self, ex):
        """
            Perform technical calculation for the strategy.

            Args:
                ex(BackTestOperations): Operations instance class.
        """
        df = pd.DataFrame(ex.data().get_rows())

        ex.append_calc_data(ta.rsi(df[Quotes.AdjClose], length = self._period))

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

            #################################################################################
            # Check if we need/can to open a long position of the symbol with the lowest RSI
            #################################################################################

            min_ex = None
            max_ex = None

            open_short = False

            for ex in self.all_exec():
                if max_ex == None or ex.get_calc_data_val() > max_ex.get_calc_data_val():
                    max_ex = ex

                if min_ex == None or ex.get_calc_data_val() < min_ex.get_calc_data_val():
                    min_ex = ex

            if (
                max_ex.get_calc_data_val(offset=1) != None and
                max_ex.get_calc_data_val(offset=1) > self.__resistance and
                max_ex.get_calc_data_val() < self.__resistance
               ):

                max_ex.close_all_long()

                if self.__to_short:
                    open_short = True

            if (
                min_ex.get_calc_data_val(offset=1) != None and
                min_ex.get_calc_data_val(offset=1) < self.__support and
                min_ex.get_calc_data_val() > self.__support
               ):

                min_ex.close_all_short()
                min_ex.open_long_max()

            # Long position is a priority so open short afterwards
            if open_short:
                max_ex.open_short_max()

            ##############################
            # Teardown the cycle
            ##############################

            self.tear_down()
