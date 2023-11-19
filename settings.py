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

class Polygon():
    """
        Default settings for Polygon.IO data source.
    """
    class Stocks(Enum):
        """
            Enumeration for stocks subscription plans.
        """
        Basic = 0
        Starter = 1
        Developer = 2
        Advanced = 3
        Commercial = 4

    stocks_plan = Stocks.Basic  # Subscription plan
    api_key = None  # Get your free api key at polygon.io

class AV():
    """
        Default settings for AlphaVantage data source.
    """
    class Plan(Enum):
        """
            Enumeration for subscription plans
        """
        Free = 0
        Plan30 = 1
        Plan75 = 2
        Plan150 = 3
        Plan300 = 4
        Plan600 = 5
        Plan1200 = 6

    plan = Plan.Free  # Subscription plan
    api_key = None  # Get your free api key at alphavantage.co

class Finnhub():
    """
        Default settings for Finnhub data source.
    """
    api_key = None  # Get your free api key at finnhub.io

class FMP():
    """
        Default settings for FMP data source.
    """
    class Plan(Enum):
        """
            Enumeration for subscription plans
        """
        Basic = 0
        Starter = 1
        Premium = 2
        Ultimate = 3

    plan = Plan.Basic
    api_key = None
