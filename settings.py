"""Module with settings for various data sources.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from enum import Enum

from data.fvalues import DbTypes

class Quotes():
    """
        Settings for the default quotes storage.
    """
    db_name = 'data.sqlite'
    db_type = DbTypes.SQLite

# Settings for derivative data sources. They'll be applied after the settings above.


