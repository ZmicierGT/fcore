"""Unit tests for storing quotes from several data sources for the same symbol (mixed YF/FMP data in one DB).

Fully offline (tmp file DB).

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
import calendar
import datetime

import pytest

from data.fvalues import StockQuotes

from yf_quotes_test import csv_quote_rows, build_quotes_inst, QUOTES

FIRST_DATE = '2020-1-1'
LAST_DATE = '2020-2-1'

# Overlapping FMP quotes with clearly distinguishable prices (YF fixture prices differ)
FMP_QUOTES = [
    {'symbol': 'FFFF', 'date': '2020-01-02', 'adjOpen': 190.0, 'adjHigh': 195.0, 'adjLow': 185.0,
     'adjClose': 201.0, 'volume': 100},
    {'symbol': 'FFFF', 'date': '2020-01-03', 'adjOpen': 290.0, 'adjHigh': 295.0, 'adjLow': 285.0,
     'adjClose': 202.0, 'volume': 200},
]

OVERLAP_TS = calendar.timegm(datetime.datetime(2020, 1, 2, 23, 59, 59).utctimetuple())

def _fmp_routes():
    return {'historical-price-eod/non-split-adjusted': FMP_QUOTES, 'dividends': [], 'splits': []}

@pytest.fixture
def both_sources(make_yf, make_fmp, tmp_path):
    """Fetched YF and FMP instances on one shared DB. Returns (yf_inst, fmp_inst)."""
    db = str(tmp_path / 'test.sqlite')

    yf_inst, _ = build_quotes_inst(make_yf, db_name=db)
    yf_inst.get()

    fmp_inst, _ = make_fmp(db_name=db, first_date=FIRST_DATE, last_date=LAST_DATE,
                           routes=_fmp_routes())
    fmp_inst.get()

    return yf_inst, fmp_inst

def test_quotes_stored_for_both_sources(both_sources):
    yf_inst, fmp_inst = both_sources

    assert fmp_inst.get_quotes_num(dt=False) == len(FMP_QUOTES)
    assert yf_inst.get_quotes_num(dt=False) > len(FMP_QUOTES)

def test_per_source_reads_are_isolated(both_sources):
    yf_inst, fmp_inst = both_sources

    expected_yf_close = csv_quote_rows(QUOTES)[OVERLAP_TS][3]

    yf_rows = {row[StockQuotes.TimeStamp]: row for row in yf_inst.get()}
    fmp_rows = {row[StockQuotes.TimeStamp]: row for row in fmp_inst.get()}

    # The overlapping timestamp is stored in each source's rows
    assert OVERLAP_TS in yf_rows
    assert OVERLAP_TS in fmp_rows

    # No cross-contamination on the shared timestamp: each source reads only its own rows
    assert fmp_rows[OVERLAP_TS][StockQuotes.Close] == FMP_QUOTES[0]['adjClose']
    assert yf_rows[OVERLAP_TS][StockQuotes.Close] == expected_yf_close
    assert yf_rows[OVERLAP_TS][StockQuotes.Close] != fmp_rows[OVERLAP_TS][StockQuotes.Close]

def test_refetch_updates_only_own_source(both_sources, make_yf):
    _, fmp_inst = both_sources

    fmp_num_before = fmp_inst.get_quotes_num(dt=False)

    # YF refetch: its rows are upserted, FMP rows must stay untouched
    yf_refetch, _ = build_quotes_inst(make_yf, db_name=fmp_inst._db_name, refetch=True)
    yf_refetch.get()

    assert yf_refetch.get_quotes_num(dt=False) > 0
    assert fmp_inst.get_quotes_num(dt=False) == fmp_num_before

def test_remove_symbol_cascades_all_sources(both_sources):
    yf_inst, fmp_inst = both_sources

    assert yf_inst.get_quotes_num(dt=False) > 0
    assert fmp_inst.get_quotes_num(dt=False) > 0

    yf_inst.remove_symbol()

    assert yf_inst.get_quotes_num(dt=False) == 0
    assert fmp_inst.get_quotes_num(dt=False) == 0
