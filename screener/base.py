"""Base class for screener implementations.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

from data import fvalues
from data.fdata import FdataError

from datetime import datetime
from datetime import timedelta
from datetime import timezone

from time import sleep

from enum import IntEnum

import abc

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
    """
    def __init__(self, symbols, period, interval, timespan):
        """
            Initialize screener class instance.

            Args:
                symbols(list of dictionaries): symbols to use in screening.
                period(int): minimum period for calculation.
                interval(int): interval in seconds between each iteration.
                timespan(fvalues.Timespans): timespan used in screening.

            Raises:
                ScrError: incorrect arguments provided.
        """
        if period <= 0:
            raise ScrError(f"Period should not be <= 0: {period}")
        self.__period = period

        if interval <= 0:
            raise ScrError(f"Interval should not be <= 0: {interval}")
        self.__interval = interval

        self.__symbols = []

        for symbol in symbols:
            data = ScrData(symbol['Title'], symbol['Source'], self)
            self.__symbols.append(data)

        if timespan not in set(item.value for item in fvalues.Timespans):
            raise ScrError(f"Unknown timespan: {timespan}")
        self.__timespan = timespan

        # Set counter till the next update
        self.set_datetime()

        # Indicates if initial data is checked
        self.__init_status = False

        # Results of cycle calculation
        self._results = None

        # Datetime of the iteration.
        self.__dt = datetime.now(timezone.utc)

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

    def get_datetime(self):
        """
            Get the datetime of the last iteration.

            Returns:
                DateTime: datetime of the last iteration.
        """
        return self.__dt

    def set_datetime(self):
        """
            Set the datetime of the iteration.
        """
        self.__dt = datetime.now(timezone.utc)

    def get_interval(self):
        """
            Get the interval of the iteration.

            Returns:
                int: interval of the iteration.
        """
        return self.__interval

    def get_timespan(self):
        """
            Get the timespan used in screening.

            Return:
                fvalues.Timespans: the timespan used in screening.
        """
        return self.__timespan

    def get_init_status(self):
        """
            Indicates if the initial data was fetched.

            Returns:
                True if the initial data was fetched, false otherwise.
        """
        return self.__init_status

    def __set_init_status(self):
        self.__init_status = True

    def do_cycle(self):
        """
            Fetch the latest quotes and perform the calculation.

            Raises:
                ScrError: can't fetch quotes.
        """
        delta = datetime.now(timezone.utc) - self.get_datetime()

        if delta.seconds < self.get_interval():
            sleep(self.get_interval() - delta.seconds)

        # Create data source object for each symbol
        for symbol in self.get_symbols():
            # Connect to the database
            symbol.get_source().symbol = symbol.get_title()
            symbol.get_source().timespan = self.get_timespan()

            # Check if initial data was initialized
            if self.get_init_status() == False:
                symbol.get_source().db_connect()
                symbol.get_initial_data()
                symbol.get_source().db_close()

        self.__set_init_status()

        self.set_datetime()

        # Perform the calculation
        self.calculate()

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
    def __init__(self, title, source, caller=None):
        """
            Initialize screening data class.

            Args:
                title(str): title of the used symbol.
                source(str): source of the symbol.
                caller(BaseScr): instance of the class which creates the current instance.
        """
        if title == "":
            raise ScrError("Title should not be empty.")
        self.__title = title

        self.__source = source
        self.__caller = caller

        self.__max_datetime = None
        self.__quotes_num = None

        # Data used in calculations
        self._data = None

    def get_caller(self):
        """
            Get the caller's instance.

            Returns:
                BaseScr: the caller's instance.
        """
        return self.__caller

    def get_data(self, period):
        """
            Get data for screening.

            Args:
                period(int): number of entries to get.

            Returns:
                list: list with quotes for the screening.
        """
        self.get_source().db_connect()

        data = self.get_source().get_rt_data()
        self.__max_datetime = self.get_source().get_max_datetime()
        self.__quotes_num = self.get_source().get_symbol_quotes_num()

        self.get_source().db_close()

        self._data.append(data)

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

    def get_initial_data(self):
        """
            Get initial data for the calculation.

            Raises:
                ScrError: can't fetch quotes.
        """
        # Get yesterday to fetch current quotes
        yesterday = datetime.now() - timedelta(days=1)

        self.get_source().first_date = yesterday
        self.get_source().last_date = yesterday + timedelta(days=2)

        try:
            self.get_source().insert_quotes(self.get_source().fetch_quotes())
            data = self.get_source().get_quotes()
        except FdataError as e:
            raise ScrError(e) from e

        self._data = data
