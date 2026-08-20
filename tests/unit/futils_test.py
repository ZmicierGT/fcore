"""Unit tests for data/futils.py (pure helpers, fully offline).

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
import calendar
from datetime import datetime

import pytest
from dateutil import tz

from data.futils import get_dt, get_ts_from_str, get_labelled_ndarray, add_column, trim_time, gui_available, thread_available
from data.fvalues import Quotes

##############################
# A. get_dt / get_ts_from_str
##############################

def test_get_dt_from_epoch():
    assert get_dt(0) == datetime(1970, 1, 1)

def test_get_dt_from_timestamp():
    ts = calendar.timegm(datetime(2023, 6, 15, 12, 30).utctimetuple())
    assert get_dt(ts) == datetime(2023, 6, 15, 12, 30)

def test_get_dt_from_negative_timestamp():
    # Pre-epoch values take the timedelta path
    assert get_dt(-86400) == datetime(1969, 12, 31)

def test_get_dt_from_naive_string():
    # Naive strings are interpreted in the provided timezone (UTC by default)
    assert get_dt('2023-06-15 12:30:00') == datetime(2023, 6, 15, 12, 30)

def test_get_dt_from_string_with_timezone():
    dt_ny = get_dt('2023-06-15 12:00', tz.gettz('America/New_York'))
    assert dt_ny == datetime(2023, 6, 15, 16, 0)  # 12:00 EDT = 16:00 UTC

def test_get_dt_from_datetime():
    # A naive datetime is interpreted in the provided timezone (UTC by default)
    assert get_dt(datetime(2023, 6, 15, 12, 30)) == datetime(2023, 6, 15, 12, 30)

    # An aware datetime is converted to UTC
    aware = datetime(2023, 6, 15, 12, 0, tzinfo=tz.UTC)
    assert get_dt(aware) == datetime(2023, 6, 15, 12, 0)

@pytest.mark.parametrize('value', [1.5, None, []])
def test_get_dt_unknown_type_raises(value):
    with pytest.raises(ValueError, match='Unknown type'):
        get_dt(value)

def test_get_dt_overflow_raises():
    with pytest.raises(ValueError, match='Too big/small timestamp'):
        get_dt(2**62)

def test_get_ts_from_str_roundtrip():
    ts = calendar.timegm(datetime(2023, 6, 15, 12, 30).utctimetuple())
    assert get_ts_from_str('2023-06-15 12:30:00') == ts

##############################
# B. get_labelled_ndarray
##############################

def _rows(values):
    """Build dict rows (compatible with sqlite3.Row usage inside get_labelled_ndarray)."""
    keys = ['time_stamp', 'volume', 'comment']
    return [dict(zip(keys, v)) for v in values]

def test_labelled_ndarray_keeps_dtypes():
    rows = _rows([(100, 5, 'a'), (200, 7, 'b')])
    arr = get_labelled_ndarray(rows)

    assert arr['time_stamp'].tolist() == [100, 200]
    assert arr['volume'].tolist() == [5, 7]
    assert arr['comment'].tolist() == ['a', 'b']
    assert arr.dtype['comment'] == 'object'

@pytest.mark.parametrize('column',
                         ['transactions', 'declaration_date', 'record_date', 'payment_date'])
def test_labelled_ndarray_nullable_columns_object(column):
    # These columns must become object dtype even when the values look numeric (may contain None)
    arr = get_labelled_ndarray([{column: 123}])
    assert arr.dtype[column] == 'object'
    assert arr[column][0] == 123

def test_labelled_ndarray_empty_raises():
    with pytest.raises(ValueError, match='length is 0'):
        get_labelled_ndarray([])

##############################
# C. add_column
##############################

def test_add_column_default():
    arr = get_labelled_ndarray([{'a': 1}, {'a': 2}])
    result = add_column(arr, 'b', dtype=float)

    assert result['a'].tolist() == [1, 2]
    assert result['b'].tolist() == [0.0, 0.0]
    assert result.dtype['b'] == float

def test_add_column_custom_default():
    arr = get_labelled_ndarray([{'a': 1}])
    result = add_column(arr, 'b', dtype=int, default=42)

    assert result['b'].tolist() == [42]
    assert result.dtype['b'] == int

##############################
# D. trim_time (start/end are interpreted as UTC)
##############################

def _quotes(timestamps):
    return get_labelled_ndarray([{Quotes.TimeStamp: t} for t in timestamps])

def _ts_utc(hour, minute):
    return calendar.timegm(datetime(2023, 6, 15, hour, minute).utctimetuple())

def test_trim_time_start_and_end():
    arr = _quotes([_ts_utc(12, 0), _ts_utc(14, 0), _ts_utc(18, 0), _ts_utc(22, 0)])

    result = trim_time(arr, start='13:30', end='21:00')

    assert result[Quotes.TimeStamp].tolist() == [_ts_utc(14, 0), _ts_utc(18, 0)]

def test_trim_time_start_only():
    arr = _quotes([_ts_utc(12, 0), _ts_utc(14, 0)])
    result = trim_time(arr, start='13:30')

    assert result[Quotes.TimeStamp].tolist() == [_ts_utc(14, 0)]

def test_trim_time_end_only():
    arr = _quotes([_ts_utc(12, 0), _ts_utc(22, 0)])
    result = trim_time(arr, end='21:00')

    assert result[Quotes.TimeStamp].tolist() == [_ts_utc(12, 0)]

def test_trim_time_end_filter_on_empty_start_result():
    # The guard must keep the end-filter out of an empty dataset
    arr = _quotes([_ts_utc(10, 0)])
    result = trim_time(arr, start='12:00', end='21:00')

    assert len(result) == 0

##############################
# E. Environment helpers
##############################

@pytest.mark.parametrize('var', ['SSH_CONNECTION', 'SSH_TTY', 'SSH_CLIENT'])
def test_gui_not_available_over_ssh(monkeypatch, var):
    monkeypatch.setenv(var, 'x')
    assert gui_available() is False

def test_thread_not_available_on_single_cpu(monkeypatch):
    import multiprocessing
    monkeypatch.setattr(multiprocessing, 'cpu_count', lambda: 1)
    assert thread_available() is False
