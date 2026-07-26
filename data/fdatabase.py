"""Module related to database connections.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
import sqlite3
from sqlite3 import Error

import abc

# TODO HIGH Analyze and possibly refactor all current logging (by adding log entries of different colors)
import warnings

# Minimum SQLite version required for ON CONFLICT(...) DO UPDATE (UPSERT),
# introduced in SQLite 3.24.0 (2018-06-04).
_MIN_SQLITE_VERSION = (3, 24, 0)

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
            self._conn = sqlite3.connect(self._db_name, timeout=30)
        except Error as e:
            raise FdatabaseError(f"An error has happened when trying to connect to a {self._db_name}: {e}") from e

        # Verify the underlying SQLite library supports the required features
        if sqlite3.sqlite_version_info < _MIN_SQLITE_VERSION:
            raise FdatabaseError(
                f"SQLite version {sqlite3.sqlite_version} is too old. "
                f"Required SQLite >= {'.'.join(str(v) for v in _MIN_SQLITE_VERSION)}."
            )

        # Set the row factory
        self._conn.row_factory = sqlite3.Row

        self._cur = self._conn.cursor()
        self._error = Error

        # Use WAL so writers don't block readers (and vice versa) during init.
        # WAL silently falls back to the default journal mode when unsupported
        # (e.g. in-memory DBs, read-only or network filesystems) — SQLite does
        # not raise, it just returns the actual mode, so inspect the result.
        try:
            row = self._cur.execute("PRAGMA journal_mode=WAL;").fetchone()
            mode = row[0].lower() if row else None
        except self._error:
            mode = None

        if mode != "wal" and self._db_name != ":memory:":
            warnings.warn(
                f"WAL journal mode could not be set on '{self._db_name}' "
                f"(got '{mode}'). Concurrent readers/writers may block each other.",
                RuntimeWarning,
                stacklevel=2,
            )

        # Wait up to 30s for a locked DB instead of failing immediately. This
        # is the safety net that lets concurrent initializations serialize
        # cleanly under BEGIN IMMEDIATE.
        try:
            self._cur.execute("PRAGMA busy_timeout=30000;")
        except self._error as e:
            raise FdatabaseError(f"Can't set busy_timeout: {e}") from e

        # Enable foreign keys
        try:
            self._cur.execute("PRAGMA foreign_keys=on;")
        except self._error as e:
            raise FdatabaseError(f"Can't enable foreign keys: {e}") from e

        # Disable recursive triggers so self-referencing BEFORE UPDATE
        # triggers (e.g. auto-bumping `modified` columns on sec_info /
        # stock_info) do not recurse infinitely. This is the SQLite default,
        # but set it explicitly to make the assumption future-proof.
        try:
            self._cur.execute("PRAGMA recursive_triggers=OFF;")
        except self._error as e:
            raise FdatabaseError(f"Can't disable recursive triggers: {e}") from e

        # TODO MID It will be safer to disable qutocommits in the future as well

    # Close the connection
    def db_close(self):
        self._cur.close()
        self._conn.close()
