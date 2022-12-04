"""RSI strategy screener implementations.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

from screener.base import BaseScr
from screener.base import ScrError

import pandas as pd
import pandas_ta as ta

from data.fvalues import Quotes

class RsiScr(BaseScr):
    """
        RSI strategy screener class.

        If RSI crosses support line from behind, it is a signal to buy. If RSI crosses the resistance line from above,
        it is signal to sell. Multiple symbols may be used.
    """
    def __init__(self,
                 support=30,
                 resistance=70,
                 ** kwargs):
        """
            Initialize RSI strategy class.

            Args:
                support(int): RSI support value.
                resistance(int): RSI resistance value.

            Raises:
                ScrErrorr: incorrect arguments.
        """

        super().__init__(**kwargs)

        # Support and resistance values for the strategy
        if support < 0 or support > 99:
            raise ScrError(f"support can't be less than 0 or more than 99. Specified value is {support}")
        self.__support = support

        if resistance < 1 or resistance > 100:
            raise ScrError(f"resistance can't be less than 1 or more than 100. Specified value is {resistance}")
        self.__resistance = resistance

        if support >= resistance:
            raise ScrError(f"Support can't be more or equal than resistance: {support} >= {resistance}")

        # Previous RSI values
        self.__prevs = []

    def calculate(self):
        """
            Perform calculation for RSI strategy.

            Raises:
                ScrError: not enough data to make a calculation.
        """
        self._results = []

        for symbol in self.get_symbols():
            self.__prevs.append(None)

        for symbol in self.get_symbols():
            rows = symbol.get_data(self.get_period() + 1)

            if len(rows) <= self.get_period():
                raise ScrError(f"Quotes length should be more than the period + 2: {len(rows)} <= {self.get_period()}")

            df = pd.DataFrame(rows)
            rsi = ta.rsi(df[Quotes.AdjClose], length = self.get_period())

            current = rsi.iloc[-1]

            # Get previous RSI value (if any)
            index = self.get_symbols().index(symbol)
            prev = self.__prevs[index]

            signal_buy = False
            signal_sell = False

            if prev != None and prev < self.__support and current > self.__support:
                signal_buy = True

            if prev != None and prev > self.__resistance and current < self.__resistance:
                signal_sell = True 

            result = [symbol.get_title(),
                      symbol.get_max_datetime(),
                      symbol.get_quotes_num(),
                      [prev, current],
                      [signal_buy, signal_sell]]

            self._results.append(result)
            self.__prevs[index] = current
