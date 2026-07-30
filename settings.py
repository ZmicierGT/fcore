"""Module with settings for various data sources.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
import os

from data.fvalues import DbTypes

class Quotes():
    """
        Settings for the default quotes storage.
    """
    db_name = 'data.sqlite'
    db_type = DbTypes.SQLite

class DataSource():
    """
        Base class for data source settings. Provides common getters for API key
        and queries-per-minute rate limiting.
    """
    _api_key = None
    _api_key_var = None
    _queries_per_min = None

    @property
    def api_key(self):
        """
            Returns the API key: the direct value if set, otherwise the value of
            the environment variable named by `api_key_var`. Raises
            RuntimeError if neither is available.
        """
        if self._api_key is not None:
            return self._api_key
        if self._api_key_var is not None:
            value = os.environ.get(self._api_key_var)
            if value is not None:
                return value
        raise RuntimeError("API key is not configured.")

    @property
    def queries_per_min(self):
        """
            Returns the configured API queries-per-minute rate limit.
        """
        return self._queries_per_min

class Massive(DataSource):
    """
        Default settings for the Massive data source.
    """
    _api_key = None
    _api_key_var = 'MASSIVE_API_KEY'
    _queries_per_min = 5  # Free/basic plan default rate
