"""Module with the base class for all custom data-tools.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from data.futils import logger

import abc

class BaseTool(metaclass=abc.ABCMeta):
    """
        Base custom data-tool class. For majority of technical indicators calculation you may use pandas_ta
    """
    def __init__(self, rows, verbosity=False, offset=None):
        """
            Initialize the custom data-tool class.

            Args:
                rows(list): quotes from the database.
                verbosity(bool): indicates if additional logs should be displayed.
                offset(int): offset to perform a calculation in rows.
        """
        # Data to make a calculation
        self._rows = rows
        # Result of the calculation. 
        self._results = []

        self._verbosity = verbosity
        self._offset = offset

    # Update the data for calculation
    def update(self, rows, offset=None):
        """
            Update the data for calculation. Used in streaming quotes.

            Args:
                rows(list): quotes from the database.
                offset(int): offset to perform a calculation in rows.            
        """
        self._rows = rows
        self._offset = offset

    @abc.abstractmethod
    def calculate(self):
        """
            Abstract method to perform the tech calculation.
        """

    def get_results(self):
        """
            Abstract method to get results of the calculation.
        """
        return self._results

    def set_results(self, results):
        """
            Sets the results of the calculation. Needed for future streaming implementation.

            Args:
                results(list): results of the calculation.
        """
        self._results = results

    def set_data(self, data):
        """
            Set data for calculation

            Args:
                data(list): data for calculation.
        """
        self._rows = data

    def set_verbosity(self, flag):
        """
            Set the verbosity flag.

            Args:
                flag(bool): indicates if additional output should be printed.
        """
        self._verbosity = flag

    # TODO LOW Think if we need a class which is a base for all other base classes
    # and which handles verbosity settings, logging, likely pre-initialization checks and
    # basic data management.
    def log(self, message):
        """
            Display a logging message depending on verbotisy flag.

            Args:
                message(str): the message to display.
        """
        logger(self._verbosity, message)

class ToolError(Exception):
    pass
