import unittest
from unittest import mock

import sys
sys.path.append('../')

import requests

import json

from data import polygon
from data.fdata import FdataError

import settings

class Test(unittest.TestCase):
    def test_0_check_arg_parser(self):
        settings.Polygon.api_key = 'test'

        source = polygon.Polygon()
        source.first_date = "2020-06-16"
        source.last_date = "2022-06-16"

        # Mocking
        requests.get = mock.MagicMock()
        json.loads = mock.MagicMock()

        try:
            source.fetch_quotes()
        except FdataError as e:
            # This is expected
            pass

        requests.get.assert_called_once_with('https://api.polygon.io/v2/aggs/ticker//range/1/day/2020-06-16/2022-06-16?adjusted=true&sort=asc&limit=50000&apiKey=test', timeout=30)
        json.loads.assert_called_once()
