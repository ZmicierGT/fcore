"""Polygon.IO API wrapper.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

from datetime import datetime
import pytz

from dateutil.relativedelta import relativedelta

import http.client
import urllib.error
import requests

import json

from data import fdata
from data.fdata import FdataError

from data.fvalues import Timespans, def_first_date, def_last_date

# Provides parameters for the query to Polygon.IO
class PolygonQuery(fdata.Query):
    """
        Polygon.IO query class.
    """
    def __init__(self, **kwargs):
        """
            Initialize Polygon.IO query class.
        """
        super().__init__(**kwargs)

        # Default values
        self.source_title = "Polygon.io"
        self.year_delta = "2"
        self.api_key = "get_your_free_api_key_at_polygon.io"

        # IF first/last datetimes are not provided, use the current datetime as the last and current - year_delta as first
        if self.first_date_ts == def_first_date:
            new_first_date = datetime.now() - relativedelta(years=int(self.year_delta))
            new_first_date = new_first_date.replace(tzinfo=pytz.utc)
            new_first_date = new_first_date.replace(hour=0, minute=0, second=0)
            self.first_date = new_first_date

        if self.last_date_ts == def_last_date:
            new_last_date = datetime.now()
            new_last_date = new_last_date.replace(tzinfo=pytz.utc)
            new_last_date = new_last_date.replace(hour=23, minute=59, second=59)
            self.last_date = new_last_date

    def get_timespan(self):
        """
            Get the timespan for the query.

            No need to convert the default timespan to Polygon.IO timespan because they are the same.
        """
        if self.timespan == Timespans.Intraday:
            return 'minute'
        else:
            return self.timespan.lower()

class Polygon(fdata.BaseFetchData):
    """
        Poligon.IO API wrapper class.
    """
    def __init__(self, query):
        """Initialize the instance of Polygon class."""
        super().__init__(query)

    def fetch_quotes(self):
        """
            The method to fetch quotes.

            Returns:
                list: quotes data

            Raises:
                FdataError: Network error happened, no data obtained or can't parse json.
        """
        first_date = self.query.first_date.date()
        last_date = self.query.last_date.date()

        url = f"https://api.polygon.io/v2/aggs/ticker/{self.query.symbol}/range/1/{self.query.get_timespan()}/{first_date}/{last_date}?adjusted=true&sort=asc&limit=50000&apiKey={self.query.api_key}"

        try:
            response = requests.get(url, timeout=30)
        except (urllib.error.HTTPError, urllib.error.URLError, http.client.HTTPException) as e:
            raise FdataError(f"Can't fetch quotes: {e}") from e
        
        try:
            json_data = json.loads(response.text)
            json_results = json_data['results']
        except (json.JSONDecodeError, KeyError) as e:
            error = e

            try:
                error = json_results = json_data['error']
            except (json.JSONDecodeError, KeyError) as e:
                # Not relevant for error reporting
                pass

            raise FdataError(f"Can't parse json or no symbol found. {error}") from e

        if len(json_results) == 0:
            raise FdataError("No data obtained.")

        for row in json_results:
            # No need in ms
            ts = int(row['t'] / 1000)
            # Keep all non-intraday timestamps at 23:59:59
            if self.query.timespan in (Timespans.Day, Timespans.Week, Timespans.Month, Timespans.Year):
                dt = datetime.utcfromtimestamp(ts)
                dt = dt.replace(tzinfo=pytz.utc)
                dt = dt.replace(hour=23, minute=59, second=59)
                ts = int(datetime.timestamp(dt))
            row['t'] = ts
            row['d'] = "NULL"
            row['cl'] = "NULL"

        return json_results
