"""Polygon.IO API wrapper.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

from datetime import datetime
from datetime import date
import pytz

from dateutil.relativedelta import relativedelta

import http.client
import urllib.error
import requests

import json

from data import fdata

from data.fvalues import Timespans

# Provides parameters for the query to Polygon.IO
class PolygonQuery(fdata.Query):
    """
        Polygon.IO query class.
    """
    def __init__(self):
        """
            Initialize Polygon.IO query class.
        """
        super().__init__()

        # Default values
        self.source_title = "Polygon.io"
        self.timespan = Timespans.Day
        self.year_delta = "2"
        self.api_key = "get_your_free_api_key_at_polygon.io"
        self.first_date = date.today() - relativedelta(years=int(self.year_delta))
        self.last_date = date.today()

    def get_timespan(self):
        """
            Get the timespan for the query.

            No need to convert the default timespan to Polygon.IO timespan because they are the same.
        """
        return self.timespan

class PolygonError(Exception):
    """
        Polygon.IO exception class.
    """
    pass

class Polygon(fdata.BaseFetchData):
    """
        Poligon.IO API wrapper class.
    """
    def fetch_quotes(self):
        """
            The method to fetch quotes.

            Returns:
                list: quotes data

            Raises:
                PolygonError: Network error happened, no data obtained or can't parse json.
        """
        request_timespan = self.query.timespan

        if request_timespan == Timespans.Intraday:
            request_timespan = "minute"

        url = f"https://api.polygon.io/v2/aggs/ticker/{self.query.symbol}/range/1/{request_timespan.lower()}/{self.query.first_date}/{self.query.last_date}?adjusted=true&sort=asc&limit=50000&apiKey={self.query.api_key}"

        try:
            response = requests.get(url)
        except (urllib.error.HTTPError, urllib.error.URLError, http.client.HTTPException) as e:
            raise PolygonError(f"Can't fetch quotes: {e}") from e
        
        try:
            json_data = json.loads(response.text)
            json_results = json_data['results']
        except (json.JSONDecodeError, KeyError) as e:
            raise PolygonError(f"Can't parse json or no symbol found: {e}") from e

        if len(json_results) == 0:
            raise PolygonError("No data obtained.")

        for row in json_results:
            # No need in ms
            ts = row['t'] / 1000
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
