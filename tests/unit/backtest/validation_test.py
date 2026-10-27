"""Validation and lifecycle-error tests for the backtesting API.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
import numpy as np
import pytest

from backtest.base import BackTestData, BackTestError
from backtest.stock import StockData
from scripted import ScriptedBackTest
from synthetic import QUOTE_DTYPE, daily, make_data, make_quotes


@pytest.mark.parametrize('kwargs', [
    {'commission': -1},
    {'commission_percent': -1},
    {'commission_percent': 101},
    {'commission_security': -1},
    {'initial_deposit': -1},
    {'periodic_deposit': -1},
    {'deposit_interval': -1},
    {'inflation': -1},
    {'inflation': 101},
    {'weighted': object()},
    {'offset': -1},
    {'timeout': -1},
    {'open_deviation': 3, 'close_deviation': 2},
    {'grouping_attr': 'sector'},
    {'grouping_shares': {'US': 0.5}},
])
def test_backtest_constructor_rejects_invalid_arguments(kwargs):
    """Case 18: invalid engine arguments raise BackTestError."""
    data = make_data(['2020-01-01'], [100])

    with pytest.raises(BackTestError):
        ScriptedBackTest(data=[data], **kwargs)


@pytest.mark.parametrize('kwargs', [
    {'spread': -1},
    {'spread': 101},
    {'trend_change_period': -1},
    {'trend_change_percent': -1},
    {'lot': 0},
    {'lot': 1.5},
])
def test_backtest_data_rejects_invalid_arguments(kwargs):
    """Case 18: invalid quote-container arguments raise BackTestError."""
    rows = make_quotes(['2020-01-01'], [100])

    with pytest.raises(BackTestError):
        BackTestData(rows=rows, **kwargs)


def test_backtest_data_rejects_empty_rows():
    """Case 18: an empty quote array is rejected."""
    with pytest.raises(BackTestError):
        BackTestData(rows=np.array([], dtype=QUOTE_DTYPE))


def test_stock_data_rejects_invalid_dividend_tax():
    """Case 18: dividend tax must be in [0, 100)."""
    rows = make_quotes(['2020-01-01'], [100])

    for div_tax in (-1, 100):
        with pytest.raises(BackTestError):
            StockData(rows=rows, div_tax=div_tax)


def test_results_before_calculation_are_rejected():
    """Case 18: results cannot be read before calculate()."""
    data = make_data(['2020-01-01'], [100])
    backtest = ScriptedBackTest(data=[data], initial_deposit=1000)

    with pytest.raises(BackTestError):
        _ = backtest.results


def test_repeated_setup_is_rejected():
    """Case 18: setup() can run only once."""
    data = make_data(['2020-01-01'], [100])
    backtest = ScriptedBackTest(data=[data], initial_deposit=1000)
    backtest.setup()

    with pytest.raises(BackTestError):
        backtest.setup()


def test_do_cycle_requires_setup():
    """Case 18: do_cycle() cannot run before setup()."""
    data = make_data(['2020-01-01'], [100])
    backtest = ScriptedBackTest(data=[data], initial_deposit=1000)

    with pytest.raises(BackTestError):
        backtest.do_cycle(0)


def test_cycle_lifecycle_errors_are_rejected():
    """Case 18: a cycle cannot be opened twice or torn down while inactive."""
    data = make_data(daily('2020-01-01', 2), [100, 101])
    backtest = ScriptedBackTest(data=[data], initial_deposit=1000)
    backtest.setup()
    backtest.do_cycle(0)

    with pytest.raises(BackTestError):
        backtest.do_cycle(1)

    fresh = ScriptedBackTest(data=[make_data(['2020-01-01'], [100])], initial_deposit=1000)

    with pytest.raises(BackTestError):
        fresh.tear_down()
