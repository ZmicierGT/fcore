"""Module related to database connections.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

import sqlite3
from sqlite3 import Error

import abc

# Exception class for general database errors
class FdatabaseError(Exception):
    """
        Database exception class.
    """
    pass

class DBConn():
    """
        Class to represent a database connection.
    """
    def __init__(self, query):
        """
            Initialize the database connection.

            Args:
                query(Query): query to initialize the database connection.
        """
        self.conn = None
        self.cur = None

        self.query = query

    # Abstract method to connect to db
    @abc.abstractmethod
    def db_connect(self):
        """
            Abstract method to connect to the database. Needs to be overloader for a particular database type.
        """
        pass

    # Abstract method to close db connection
    @abc.abstractmethod
    def db_close(self):
        """
            Abstract method to disconnect from the database. Needs to be overloader for a particular database type.
        """
        pass

class SQLiteConn(DBConn, metaclass=abc.ABCMeta):
    # Connect to the database
    def db_connect(self):
        """
            Connect to SQLite database.

            Raises:
                FdatabaseError: Can't connect to a database.
        """
        try:
            self.query.conn = sqlite3.connect(self.query.db_name)
        except Error as e:
            raise FdatabaseError(f"An error has happened when trying to connect to a {self.query.db_name}: {e}") from e

        self.query.cur = self.query.conn.cursor()
        self.query.Error = Error

    # Close the connection
    def db_close(self):
        self.query.cur.close()
        self.query.conn.close()
