"""Module related to database connections.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
import sqlite3
from sqlite3 import Error

import abc

# Exception class for general database errors
class FdatabaseError(Exception):
    """
        Database exception class.
    """

class DBConn(metaclass=abc.ABCMeta):
    """
        Class to represent a database connection.
    """
    def __init__(self, db_name):
        """
            Initialize the database connection.

            Args:
                db_name(str): database name/path used to establish the connection.
            """
        self._conn = None
        self._cur = None

        # Type of exception for db queries
        self._error = None

        self._db_name = db_name

    @property
    def conn(self):
        """Get the database connection."""
        return self._conn

    @property
    def cur(self):
        """Get the database cursor."""
        return self._cur

    @property
    def error(self):
        """Get the exception type for db queries."""
        return self._error

    @property
    def db_name(self):
        """Get the database name."""
        return self._db_name

    # Abstract method to connect to db
    @abc.abstractmethod
    def db_connect(self):
        """
            Abstract method to connect to the database. Needs to be overloader for a particular database type.
        """

    # Abstract method to close db connection
    @abc.abstractmethod
    def db_close(self):
        """
            Abstract method to disconnect from the database. Needs to be overloader for a particular database type.
        """

class SQLiteConn(DBConn):
    # Connect to the database
    def db_connect(self):
        """
            Connect to SQLite database.

            Raises:
                FdatabaseError: Can't connect to a database.
        """
        try:
            self._conn = sqlite3.connect(self._db_name)
        except Error as e:
            raise FdatabaseError(f"An error has happened when trying to connect to a {self._db_name}: {e}") from e

        # Set the row factory
        self._conn.row_factory = sqlite3.Row

        self._cur = self._conn.cursor()
        self._error = Error

        # Enable foreign keys
        try:
            self._cur.execute("PRAGMA foreign_keys=on;")
        except self._error as e:
            raise FdatabaseError(f"Can't enable foreign keys: {e}") from e

    # Close the connection
    def db_close(self):
        self._cur.close()
        self._conn.close()
