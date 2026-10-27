"""Multi-symbol portfolio tests for the backtesting engine.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
import pytest

from helpers import run_scripted, symbol
from synthetic import daily, make_data


def test_multi_symbol_positions_and_portfolio_value():
    """Case 12: symbols keep independent positions while sharing portfolio cash."""
    dates = daily('2020-01-01', 3)
    first = make_data(dates, [100, 110, 120], title='AAA')
    second = make_data(dates, [50, 55, 60], title='BBB')

    def open_both(bt):
        bt.all_exec[0].buy(num=5, exact=True)
        bt.all_exec[1].buy(num=10, exact=True)

    def close_first(bt):
        bt.all_exec[0].sell(num=5)

    _, results = run_scripted(
        [first, second],
        actions={0: open_both, 1: close_first},
        initial_deposit=1000,
    )

    assert len(results.Symbols) == 2
    assert results.Symbols[0].Title == 'AAA'
    assert results.Symbols[1].Title == 'BBB'

    assert results.Cash == pytest.approx([0, 550, 550])
    assert results.TotalValue == pytest.approx([1000, 1100, 1150])
    assert results.TotalTrades == [2, 3, 3]
    assert results.TotalExpenses == pytest.approx([0, 0, 0])

    first_result = symbol(results, 0)
    second_result = symbol(results, 1)

    assert first_result.Open == pytest.approx([100, 110, 120])
    assert first_result.LongPositions == [5, 0, 0]
    assert first_result.TradesNo == [1, 2, 2]

    assert second_result.Open == pytest.approx([50, 55, 60])
    assert second_result.LongPositions == [10, 10, 10]
    assert second_result.TradesNo == [1, 1, 1]

    stats = results.get_statistics()

    assert stats.invested == pytest.approx(1000)
    assert stats.total_value == pytest.approx(1150)
    assert stats.total_trades == 3
