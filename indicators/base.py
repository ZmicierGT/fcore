"""Module with the base class for all custom indicators/oscillators/AI models.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

import abc

class BaseIndicator(metaclass=abc.ABCMeta):
    """
        Base custom indicator class. Used in the case if there is no (or we do not want) to use indicators from
        libraries like pandas_ta.
    """
    def __init__(self, rows, offset=None):
        """
            Initialize the custom indicator class.

            Args:
                rows(list): quotes from the database.
                offset(int): offset to perform a calculation in rows.
        """
        # Private data to make a calculation
        self._rows = rows
        # Result of the calculation. 
        self._results = []
        # Offset
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
        pass

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

class IndicatorError(Exception):
    pass
