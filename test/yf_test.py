import unittest
from unittest import mock

import sys
sys.path.append('../')

import requests

import urllib

from data import yf
from data.fdata import FdataError

class Test(unittest.TestCase):
    def test_0_check_arg_parser(self):
        query = yf.YFQuery()
        yf_obj = yf.YF(query)

        # Mocking
        urllib.request.urlopen = mock.MagicMock()

        try:
            yf_obj.fetch_quotes()
        except FdataError as e:
            # This is expected
            pass

        divs_url = 'https://query1.finance.yahoo.com/v7/finance/download/?period1=-2147483648&period2=9999999999&interval=1d&events=div&includeAdjustedClose=true'
        quotes_url = 'https://query1.finance.yahoo.com/v7/finance/download/?period1=-2147483648&period2=9999999999&interval=1d&events=history&includeAdjustedClose=true'

        urllib.request.urlopen.assert_has_calls([
            mock.call(quotes_url),
            mock.call().read(),
            mock.call().read().decode('utf8'),
            mock.call().read().decode().splitlines(),
            mock.call().read().decode().splitlines().__getitem__(slice(1, None, None)),
            mock.call(divs_url),
            mock.call().read(),
            mock.call().read().decode('utf8'),
            mock.call().read().decode().splitlines(),
            mock.call().read().decode().splitlines().__getitem__(slice(1, None, None)),
            mock.call().read().decode().splitlines().__getitem__().__iter__(),
            mock.call().read().decode().splitlines().__getitem__().__iter__()
        ])
