"""Module with the base class for all custom data-tools.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from data.futils import Log

import abc

# TODO HIGH Do we need tools? If still need to simplify complex calculations, maybe it worth to add appropriate demos?

def causal_fill(df, columns):
    """
        Fill NaN values causally (without using future data).

        Each NaN is replaced with the expanding mean of the past-and-current values of
        its column. The leading all-NaN prefix (where even the expanding mean is not
        available yet) is filled with 0.0 as the neutral 'no data yet' value.

        Args:
            df(DataFrame): dataframe to fill (modified in place).
            columns(list): columns to fill.

        Returns:
            DataFrame: the filled dataframe.
    """
    for key in columns:
        df[key] = df[key].fillna(df[key].expanding().mean())
        df[key] = df[key].fillna(0)

    return df

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
        self._lg = Log(verbosity=verbosity)
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
        self._lg.set_verbosity(flag)

class ToolError(Exception):
    pass
