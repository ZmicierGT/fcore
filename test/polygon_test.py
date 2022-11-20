import unittest
from unittest import mock

import sys
sys.path.append('../')

import requests

import json

from data import polygon

class Test(unittest.TestCase):
    def test_0_check_arg_parser(self):
        query = polygon.PolygonQuery()
        query.first_date = "2020-06-16"
        query.last_date = "2022-06-16"
        polygon_obj = polygon.Polygon(query)

        # Mocking
        requests.get = mock.MagicMock()
        json.loads = mock.MagicMock()

        try:
            polygon_obj.fetch_quotes()
        except polygon.PolygonError as e:
            # This is expected
            pass

        requests.get.assert_called_once_with('https://api.polygon.io/v2/aggs/ticker//range/1/day/2020-06-16/2022-06-16?adjusted=true&sort=asc&limit=50000&apiKey=get_your_free_api_key_at_polygon.io')
        json.loads.assert_called_once()
