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
        if self.first_date == def_first_date:
            self.first_date = datetime.now() - relativedelta(years=int(self.year_delta))
            self.first_date = self.first_date.replace(tzinfo=pytz.utc)
            self.first_date = self.first_date.replace(hour=0, minute=0, second=0)
            self.first_date = int(datetime.timestamp(self.first_date))

        if self.last_date == def_last_date:
            self.last_date = datetime.now()
            self.last_date = self.last_date.replace(tzinfo=pytz.utc)
            self.last_date = self.last_date.replace(hour=23, minute=59, second=59)
            self.last_date = int(datetime.timestamp(self.last_date))

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
        first_date = datetime.utcfromtimestamp(self.query.first_date)
        first_date.replace(tzinfo=pytz.utc)
        first_date = first_date.date()

        last_date = datetime.utcfromtimestamp(self.query.last_date)
        last_date.replace(tzinfo=pytz.utc)
        last_date = last_date.date()

        url = f"https://api.polygon.io/v2/aggs/ticker/{self.query.symbol}/range/1/{self.query.get_timespan()}/{first_date}/{last_date}?adjusted=true&sort=asc&limit=50000&apiKey={self.query.api_key}"

        try:
            response = requests.get(url, timeout=30)
        except (urllib.error.HTTPError, urllib.error.URLError, http.client.HTTPException) as e:
            raise FDataError(f"Can't fetch quotes: {e}") from e
        
        try:
            json_data = json.loads(response.text)
            json_results = json_data['results']
        except (json.JSONDecodeError, KeyError) as e:
            raise FDataError(f"Can't parse json or no symbol found. Maybe API key is missing? {e}") from e

        if len(json_results) == 0:
            raise FdataError("No data obtained.")

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
