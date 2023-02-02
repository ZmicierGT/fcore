"""Module with settings for various data sources.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

class Quotes():
    """
        Settings for the default quotes storage.
    """
    db_name = 'data.sqlite'
    db_type = 'sqlite'

# Settings for derivative data sources. They'll be applied after the settings above.

class Polygon():
    """
        Default settings for Polygon.IO data source.
    """
    api_key = None  # Get your free api key at polygon.io
    year_delta = 2  # For a free account you can fetch historical quotes for up to 2 years.

class AV():
    """
        Default settings for AlphaVantage data source.
    """
    api_key = None  # Get your free api key at alphavantage.co

class Finnhub():
    """
        Default settings for Finnhub data source.
    """
    api_key = None  # Get your free api key at finnhub.io
