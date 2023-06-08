"""Volume Oscillator implementation.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""

from tools.base import BaseTool
from tools.base import ToolError

from enum import IntEnum

import pandas as pd
import pandas_ta as ta

# TODO LOW Switch to labelled numpy array
class VOData(IntEnum):
    """
        Enum to represent Volume Oscillator results.
    """
    Value = 0
    LongSMAValue = 1
    ShortSMAValue = 2

class VO(BaseTool):
    """
        Volume Oscillator impementation.
    """
    def __init__(self, long_period, short_period, rows, row_val, offset=None):
        """
            Initialize Volume Oscillator implementation class.

            Args:
                long_period(int): long period for VO calculation.
                short_period(int): short period for VO calculation.
                rows(list): quotes for calculation.
                row_val(int): number of row with data to use in calculation.
                offset(int): offset for calculation.
        """
        super().__init__(rows)

        self.__row_val = row_val

        self.__short_period = short_period
        self.__long_period = long_period

        self.__short_sma = None
        self.__long_sma = None

    def calculate(self):
        """
            Perform the calculation based on the provided data.

            Raises:
                ToolError: incorrect periods provided or moving average misalignment.
        """
        period_difference = self.__long_period - self.__short_period

        if period_difference <= 0:
            raise ToolError(f"{self.__long_period} must be bigger than {self.__short_period}")

        # Calculated SMAs for VO

        if self.__long_sma == None or self.__short_sma == None:
            if self.__long_sma != self.__short_sma:
                raise ToolError("If short or long ema is not set, another should not be set as well.")

        df = pd.DataFrame(self._rows)

        self.__long_sma = ta.sma(df[self.__row_val], length = self.__long_period)
        self.__short_sma = ta.sma(df[self.__row_val], length = self.__short_period)

        length = len(self.__long_sma)
        length_short = len(self.__short_sma)

        if length_short != length:
            raise ToolError(f"Long/Short EMAs results lengths are incorrect: {length_short} != {length}")

        # Calculate VO

        self._results = []

        # TODO LOW it should be rewritten to numpy processing when switched to labelled numpy array
        for i in range (0, length):
            long_value = self.__long_sma[i]
            short_value = self.__short_sma[i]

            if short_value == None or long_value == None:
                result = [None, long_value, short_value]
            else:
                result = [short_value - long_value, long_value, short_value]

            self._results.append(result)
