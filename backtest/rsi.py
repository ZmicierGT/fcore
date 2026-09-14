"""RSI strategy implementation.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from backtest.base import BackTest
from backtest.base import BackTestError

from data.futils import add_column

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
                 **kwargs):
        """
            Initializes RSI backtesting strategy implementation.

            Args:
                period(int): period for RSI calculation.
                support(int): RSI support value.
                resistance(int): RSI resistance value.

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

    def skip_criteria(self, index):
        """
            Check if the current cycle should be skipped.

            The cycle should be skipped if the RSI (of the previous bar, compared with the bar
            before it) is not calculated yet.

            Args:
                index(int): index of the current cycle.
        """
        return index < self._period + 2

    def do_tech_calculation(self, ex):
        """
            Perform technical calculation for the strategy.

            Args:
                ex(BackTestOperations): Operations instance class.
        """
        df = pd.DataFrame(ex.data.rows)
        rsi = ta.rsi(df[ex.data.charting.close], length=self._period)

        # Append data to the calculations dataset
        ex.add_col(name='rsi', data=rsi, dtype=float)

    def do_calculation(self):
        """
            Perform strategy calculation.

            Raises:
                BackTestError: not enough data for calculation.
        """

        ######################################
        # Perform the global calculation setup
        ######################################

        # TODO LOW In setup() the initial data for multi-symbol strategy may be altered.
        # Consider any length-related operations to be performed only after setup (including other demos)
        self.setup()

        length = len(self.get_main_data().rows)

        # TODO MID This check may be put to setup
        if length < self._period:
            raise BackTestError(f"Not enough data to calculate a period: {length} < {self._period}")

        ############################################################
        # Iterate through all rows and calculate the required values
        ############################################################

        for row in self.get_main_data().rows:

            ####################################################################################################
            # Setup cycle calculations if current cycle shouldn't be skipped (because of offset or lack of data)
            ####################################################################################################

            if self.do_cycle(row) == False:
                continue

            #################################################################################
            # Check if we need/can to open a long position of the symbol with the lowest RSI
            #################################################################################

            min_ex = None
            max_ex = None
            min_val = None
            max_val = None

            # Rank symbols by the RSI of the latest completed bar (the same bar the crossing
            # checks use) — the current bar's RSI is not known at execution time (the Open).
            for ex in self.all_exec():
                val_row = ex.get_val(offset=-1)
                val = val_row['rsi'] if val_row is not None and pd.notna(val_row['rsi']) else None

                if val is None:
                    continue

                if max_val is None or val > max_val:
                    max_ex, max_val = ex, val

                if min_val is None or val < min_val:
                    min_ex, min_val = ex, val

            # Check the row for None before subscripting: get_val() returns None for
            # negative local indexes and for timestamps missing in the symbol's data.
            if max_ex is not None:
                prev = max_ex.get_val(offset=-2)

                if (prev is not None and pd.notna(prev['rsi']) and
                    prev['rsi'] > self.__resistance and max_val < self.__resistance):
                    max_ex.close_all_long()

            if min_ex is not None:
                prev = min_ex.get_val(offset=-2)

                if (prev is not None and pd.notna(prev['rsi']) and
                    prev['rsi'] < self.__support and min_val > self.__support):
                    min_ex.open_long_max()

            ##############################
            # Teardown the cycle
            ##############################

            self.tear_down()
