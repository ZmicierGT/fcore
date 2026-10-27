"""Synthetic quote builders for backtesting unit tests.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from datetime import date, datetime, time, timedelta, timezone

import numpy as np

from backtest.base import BackTestData
from backtest.stock import StockData

QUOTE_DTYPE = np.dtype([
    ('time_stamp', np.int64),
    ('date_time', object),
    ('opened', np.float64),
    ('high', np.float64),
    ('low', np.float64),
    ('closed', np.float64),
    ('volume', np.float64),
    ('transactions', object),
    ('adj_open', np.float64),
    ('adj_high', np.float64),
    ('adj_low', np.float64),
    ('adj_close', np.float64),
    ('adj_volume', np.float64),
    ('divs_ex', np.float64),
    ('divs_pay', np.float64),
    ('splits', np.float64),
])


def daily(start, count):
    """Return a list of consecutive dates starting at start."""
    if isinstance(start, str):
        start = date.fromisoformat(start)

    return [start + timedelta(days=i) for i in range(count)]


def intraday(start, days, times):
    """Return datetimes for each day and intraday time combination."""
    if isinstance(start, datetime):
        start_date = start.date()
    elif isinstance(start, date):
        start_date = start
    else:
        start_date = date.fromisoformat(str(start)[:10])

    parsed_times = []

    for value in times:
        if isinstance(value, time):
            parsed_times.append(value)
        else:
            parts = [int(part) for part in str(value).split(':')]
            parsed_times.append(time(*parts))

    return [datetime.combine(start_date + timedelta(days=day), bar_time)
            for day in range(days)
            for bar_time in sorted(parsed_times)]


def _as_datetime(value):
    """Convert a date/string/datetime into a UTC datetime (daily dates use EOD)."""
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, date):
        dt = datetime.combine(value, time(23, 59, 59))
    else:
        text = str(value)
        if len(text) == 10:
            dt = datetime.combine(date.fromisoformat(text), time(23, 59, 59))
        else:
            dt = datetime.fromisoformat(text.replace('Z', '+00:00'))

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def _expand(value, length, name):
    """Expand a scalar or validate a per-bar sequence."""
    if isinstance(value, (list, tuple, np.ndarray)):
        if len(value) != length:
            raise ValueError(f"{name} has {len(value)} values while {length} bars are expected.")
        return list(value)

    return [value] * length


def make_quotes(date_times,
                opens,
                highs=None,
                lows=None,
                closes=None,
                volumes=1000.0,
                transactions=None,
                divs_ex=0.0,
                divs_pay=0.0,
                splits=1.0,
                adj_open=None,
                adj_high=None,
                adj_low=None,
                adj_close=None,
                adj_volume=None):
    """Build a sorted synthetic StockQuotes-compatible ndarray.

    Missing high/low/close values are derived from the open by adding/subtracting 1 and adding 0.5.
    Adjusted values default to the raw values. Event values may be scalars or per-bar sequences.
    """
    length = len(date_times)

    if length == 0:
        raise ValueError("At least one bar is required.")

    opens = _expand(opens, length, 'opens')
    closes = _expand(opens if closes is None else closes, length, 'closes')
    highs = _expand([max(o, c) + 1 for o, c in zip(opens, closes)] if highs is None else highs, length, 'highs')
    lows = _expand([min(o, c) - 1 for o, c in zip(opens, closes)] if lows is None else lows, length, 'lows')
    volumes = _expand(volumes, length, 'volumes')
    transactions = _expand(transactions, length, 'transactions')
    divs_ex = _expand(divs_ex, length, 'divs_ex')
    divs_pay = _expand(divs_pay, length, 'divs_pay')
    splits = _expand(splits, length, 'splits')

    adj_open = _expand(opens if adj_open is None else adj_open, length, 'adj_open')
    adj_high = _expand(highs if adj_high is None else adj_high, length, 'adj_high')
    adj_low = _expand(lows if adj_low is None else adj_low, length, 'adj_low')
    adj_close = _expand(closes if adj_close is None else adj_close, length, 'adj_close')
    adj_volume = _expand(volumes if adj_volume is None else adj_volume, length, 'adj_volume')

    dts = [_as_datetime(value) for value in date_times]

    if any(dts[i] > dts[i + 1] for i in range(length - 1)):
        raise ValueError("Quotes must be sorted in ascending date order.")

    rows = []

    for i, dt in enumerate(dts):
        rows.append((
            int(dt.timestamp()),
            dt.strftime('%Y-%m-%d %H:%M:%S'),
            float(opens[i]),
            float(highs[i]),
            float(lows[i]),
            float(closes[i]),
            float(volumes[i]),
            transactions[i],
            float(adj_open[i]),
            float(adj_high[i]),
            float(adj_low[i]),
            float(adj_close[i]),
            float(adj_volume[i]),
            float(divs_ex[i]),
            float(divs_pay[i]),
            float(splits[i]),
        ))

    return np.array(rows, dtype=QUOTE_DTYPE)


def make_data(date_times,
              opens,
              *,
              title='TEST',
              spread=0.0,
              lot=1,
              stock=False,
              div_tax=0.0,
              **quote_kwargs):
    """Build a BackTestData or StockData instance from synthetic quotes."""
    rows = make_quotes(date_times, opens, **quote_kwargs)

    if stock:
        return StockData(rows=rows, title=title, spread=spread, lot=lot, div_tax=div_tax)

    return BackTestData(rows=rows, title=title, spread=spread, lot=lot)
