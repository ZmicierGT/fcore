"""Base class for screener implementations.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from data import fvalues
from data.fvalues import Quotes
from data.fdata import FdataError
from data.futils import Log

from datetime import datetime
from datetime import timedelta

from enum import IntEnum

import abc

import numpy as np

# Exception class for screener errors
class ScrError(Exception):
    """
        Screening exception class.
    """

class ScrResult(IntEnum):
    """
        Enumeration with screening results.
    """
    Title = 0
    LastDatetime = 1
    QuotesNum = 2
    # Depending on strategy, the Values and Signals may be not just a single value but a list (or another data type)
    Values = 3
    Signals = 4

# Base class for screener implementation

class BaseScr(metaclass=abc.ABCMeta):
    """
        Base screener implementation.

        The screening is performed on-demand: a single screen() call fetches the data
        (including the latest quote) and makes a conclusion.
    """
    def __init__(self,
                 symbols,
                 period,
                 timespan=fvalues.Timespans.Day,
                 init_days=120,
                 verbosity=True):
        """
            Initialize screener class instance.

            Args:
                symbols(list of dictionaries): symbols to use in screening.
                period(int): minimum period for calculation.
                timespan(fvalues.Timespans): timespan used in screening.
                init_days(int): the number of days of history to get the data.
                verbosity(bool): verbosity flag.

            Raises:
                ScrError: incorrect arguments provided.
        """
        self._verbosity = verbosity
        self._lg = Log(verbosity=verbosity)

        if period <= 0:
            raise ScrError(f"Period should not be <= 0: {period}")
        self.__period = period

        if timespan not in fvalues.Timespans:
            raise ScrError(f"Unknown timespan: {timespan}")
        self.__timespan = timespan

        self.__symbols = []

        for symbol in symbols:
            data = ScrData(symbol['Title'], symbol['Source'], self, init_days)
            self.__symbols.append(data)

        # Results of the calculation
        self._results = None

    def get_symbols(self):
        """
            Get symbols used in screening.

            Returns:
                symbols(list of dictionaries): symbols to used in screening.
        """
        return self.__symbols

    def get_period(self):
        """
            Get the period.

            Returns:
                int: the period used in screening.
        """
        return self.__period

    def get_timespan(self):
        """
            Get the timespan used in screening.

            Return:
                fvalues.Timespans: the timespan used in screening.
        """
        return self.__timespan

    # TODO LOW Think if nogil multithreading has a sense here
    def screen(self):
        """
            Fetch the data (including the latest quote for each symbol) and perform the calculation.

            Returns:
                list: results of the calculation.

            Raises:
                ScrError: can't fetch quotes.
        """
        # Fetch the data for each symbol
        for symbol in self.get_symbols():
            symbol.fetch_data()

        # Perform the calculation
        self.calculate()

        return self.get_results()

    @abc.abstractmethod
    def calculate(self):
        """
            Abstract method to perform the calculation.
        """

    def get_results(self):
        """
            Get the results of the calculation.

            Returns:
                list: results of the calculation.
        """
        return self._results

class ScrData():
    """
        Base class for screener data.
    """
    def __init__(self, title, source, caller=None, init_days=120):
        """
            Initialize screening data class.

            Args:
                title(str): title of the used symbol.
                source(str): source of the symbol.
                caller(BaseScr): instance of the class which creates the current instance.
                init_days(int): the number of days of history to get the data.
        """
        if title == "":
            raise ScrError("Title should not be empty.")
        self.__title = title

        if source.timespan != caller.get_timespan():
            raise ScrError(f"Timespan of {title} ({source.timespan}) doesn't match the screener timespan ({caller.get_timespan()})")

        self.__source = source
        self.__caller = caller

        self.__max_datetime = None
        self.__quotes_num = None

        # Data used in calculations
        self._data = None

        # Number of days of history to get the data
        if init_days <= 0:
            raise ScrError("The number of days of history can't be <= 0.")

        self.__init_days = init_days

    def get_caller(self):
        """
            Get the caller's instance.

            Returns:
                BaseScr: the caller's instance.
        """
        return self.__caller

    def get_data(self, period):
        """
            Get the latest data for screening.

            Args:
                period(int): number of entries to get.

            Returns:
                list: list with quotes for the screening.
        """
        return self._data[len(self._data) - period:]

    def get_title(self):
        """
            Get the title of the corresponding symbol.

            Returns:
                str: the title of the corresponding symbol.
        """
        return self.__title

    def get_max_datetime(self):
        """
            Get max datetime for a symbol.

            Returns:
                str: max datetime string for a symbol.
        """
        return self.__max_datetime

    def get_quotes_num(self):
        """
            Get quotes number for a symbol.

            Returns:
                int: quotes number for a symbol.
        """
        return self.__quotes_num

    def get_source(self):
        """
            Get the source of a symbol.

            Returns:
                int: the source of a symbol.
        """
        return self.__source

    def get_init_days(self):
        """
            Return the number of days of history to get the data.

            Returns:
                int: the number of days of history to get the data.
        """
        return self.__init_days

    def fetch_data(self):
        """
            Get historical quotes along with the latest quote (not cached).

            Raises:
                ScrError: can't fetch quotes.
        """
        last_date = datetime.now()
        self.get_source().first_date = last_date - timedelta(days=self.get_init_days())
        self.get_source().last_date = last_date

        try:
            data = self.get_source().get()
        except FdataError as e:
            raise ScrError(e) from e

        self.__quotes_num = self.get_source().get_quotes_num(timespan=True, dt=False)

        # Append the latest quote, but only if it is newer than the history
        # (the recent quote may refer to an already-cached day, e.g. on weekends)
        recent = self.get_source().get_recent_data()

        if len(data) == 0 or recent[-1][Quotes.TimeStamp] > data[-1][Quotes.TimeStamp]:
            self.__max_datetime = recent[-1][Quotes.DateTime]
            self.__quotes_num += len(recent)
            self._data = np.append(data, recent)
        else:
            self.__max_datetime = data[-1][Quotes.DateTime]
            self._data = data
