"""Weighted-portfolio and composition tests for the backtesting engine.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
import pytest

from backtest.weigher import EqualWeigher
from helpers import run_scripted, symbol
from synthetic import daily, make_data


def test_equal_weigher_balances_second_symbol():
    """Case 17: an underweight symbol is bought, its size clamped by available cash.

    The EqualWeigher target is (500 * 2 - 0) / 100 = 10 shares, but only 500
    cash remains after the first buy, so the position is clamped to 5 shares.
    """
    dates = daily('2020-01-01', 3)
    first = make_data(dates, [100] * 3, title='AAA')
    second = make_data(dates, [100] * 3, title='BBB')
    weigher = EqualWeigher()

    backtest, results = run_scripted(
        [first, second],
        actions={
            0: lambda bt: bt.all_exec[0].buy(num=5, exact=True),
            1: lambda bt: bt.all_exec[1].buy(),
        },
        initial_deposit=1000,
        weighted=weigher,
    )

    assert results.Cash == pytest.approx([500, 0, 0])
    assert results.TotalValue == pytest.approx([1000, 1000, 1000])
    assert results.TotalTrades == [1, 2, 2]
    assert symbol(results, 0).LongPositions == [5, 5, 5]
    assert symbol(results, 1).LongPositions == [0, 5, 5]

    # After both positions are open, EqualWeigher gives them the same weight.
    assert backtest.mean_weight == pytest.approx(500)
    assert backtest.all_exec[0].weight == pytest.approx(500)
    assert backtest.all_exec[1].weight == pytest.approx(500)


def test_composition_change_sells_excluded_symbol():
    """Case 17: composition changes with dates and exits are sold."""
    dates = daily('2020-01-01', 3)
    first = make_data(dates, [100] * 3, title='AAA')
    second = make_data(dates, [100] * 3, title='BBB')
    seen_compositions = {}

    def open_both(bt):
        bt.all_exec[0].buy(num=5, exact=True)
        bt.all_exec[1].buy(num=5, exact=True)

    def record_composition(bt):
        seen_compositions[1] = list(bt.composition)

    def drop_excluded(bt):
        seen_compositions[2] = list(bt.composition)

        for ex in bt.all_exec:
            if ex.data.title not in bt.composition and ex.long_positions:
                ex.sell()

    _, results = run_scripted(
        [first, second],
        actions={0: open_both, 1: record_composition, 2: drop_excluded},
        initial_deposit=1000,
        compositions={
            '2020-01-01': ['AAA', 'BBB'],
            '2020-01-03': ['AAA'],
        },
    )

    assert seen_compositions[1] == ['AAA', 'BBB']
    assert seen_compositions[2] == ['AAA']

    assert symbol(results, 0).LongPositions == [5, 5, 5]
    assert symbol(results, 1).LongPositions == [5, 5, 0]
    assert symbol(results, 1).TradesNo == [1, 1, 2]
    assert results.Cash == pytest.approx([0, 0, 500])
    assert results.TotalValue == pytest.approx([1000, 1000, 1000])
    assert results.TotalTrades == [2, 2, 3]
