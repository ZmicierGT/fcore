"""Order sizing and limit-order tests for the backtesting engine.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
import pytest

from backtest.base import BackTestError
from helpers import run_backtest, run_scripted, symbol
from scripted import ScriptedBackTest
from synthetic import daily, make_data


def test_order_sizing_lot_exact_and_empty_sell():
    """Case 10: automatic sizing respects lot, exact and affordability rules."""
    data = make_data(daily('2020-01-01', 3), [100] * 3, title='LOT', lot=100)

    _, results = run_scripted(
        [data],
        actions={0: lambda bt: bt.exec().buy()},
        initial_deposit=25000,
    )

    assert symbol(results).LongPositions[0] == 200
    assert results.Cash[0] == pytest.approx(5000)
    assert results.TotalTrades[0] == 1

    exact_data = make_data(['2020-01-01'], [100], title='EXACT')
    exact_backtest = ScriptedBackTest(
        data=[exact_data],
        actions={0: lambda bt: bt.exec().buy(num=100, exact=True)},
        initial_deposit=1000,
    )

    with pytest.raises(BackTestError):
        run_backtest(exact_backtest)

    clamp_data = make_data(['2020-01-01'], [100], title='CLAMP')
    _, clamp_results = run_scripted(
        [clamp_data],
        actions={0: lambda bt: bt.exec().buy(num=100, exact=False)},
        initial_deposit=1000,
    )

    assert symbol(clamp_results).LongPositions[0] == 10
    assert clamp_results.Cash[0] == pytest.approx(0)
    assert clamp_results.TotalTrades[0] == 1

    empty_data = make_data(['2020-01-01'], [100], title='NO_OP')
    _, empty_results = run_scripted(
        [empty_data],
        actions={0: [
            lambda bt: bt.exec().buy(num=0),
            lambda bt: bt.exec().buy(num=-5),
            lambda bt: bt.exec().sell(num=5),
        ]},
        initial_deposit=1000,
    )

    assert symbol(empty_results).LongPositions[0] == 0
    assert empty_results.Cash[0] == pytest.approx(1000)
    assert empty_results.TotalTrades[0] == 0


def test_limit_order_immediate_fill():
    """Case 11: a marketable limit order fills at its limit on the placement bar."""
    data = make_data(['2020-01-01'], [100], closes=[99], highs=[101], lows=[98],
                     title='LIMIT_NOW', spread=2.0)

    _, results = run_scripted(
        [data],
        actions={0: lambda bt: bt.exec().buy(num=5, exact=True, limit=100)},
        initial_deposit=1000,
    )

    assert results.Cash[-1] == pytest.approx(500)
    assert results.SpreadExpense[-1] == pytest.approx(0)
    assert results.TotalTrades[-1] == 1
    assert symbol(results).LongPositions[-1] == 5


def test_limit_order_resting_gap_and_touch_fills():
    """Case 11: resting orders fill at the gapped open or at the touched limit."""
    gap_data = make_data(daily('2020-01-01', 2), [100, 90], highs=[106, 95],
                         lows=[99, 85], closes=[105, 92], title='LIMIT_GAP')

    _, gap_results = run_scripted(
        [gap_data],
        actions={0: lambda bt: bt.exec().buy(num=5, exact=True, limit=95)},
        initial_deposit=1000,
    )

    assert gap_results.Cash[-1] == pytest.approx(550)
    assert gap_results.SpreadExpense[-1] == pytest.approx(0)
    assert gap_results.TotalTrades[-1] == 1
    assert symbol(gap_results).LongPositions[-1] == 5

    touch_data = make_data(daily('2020-01-01', 2), [100, 100], highs=[106, 101],
                           lows=[99, 90], closes=[105, 100], title='LIMIT_TOUCH')

    _, touch_results = run_scripted(
        [touch_data],
        actions={0: lambda bt: bt.exec().buy(num=5, exact=True, limit=95)},
        initial_deposit=1000,
    )

    assert touch_results.Cash[-1] == pytest.approx(525)
    assert touch_results.SpreadExpense[-1] == pytest.approx(0)
    assert touch_results.TotalTrades[-1] == 1
    assert symbol(touch_results).LongPositions[-1] == 5


def test_limit_order_sell_touch_and_expiry():
    """Case 11: sell limits fill on a high touch and unfilled orders expire."""
    sell_data = make_data(daily('2020-01-01', 3), [100, 100, 102],
                          highs=[101, 101, 106], lows=[99, 98, 101],
                          closes=[100, 99, 104], title='LIMIT_SELL')

    _, sell_results = run_scripted(
        [sell_data],
        actions={
            0: lambda bt: bt.exec().buy(num=5, exact=True),
            1: lambda bt: bt.exec().sell(num=5, limit=105),
        },
        initial_deposit=1000,
    )

    assert sell_results.Cash[-1] == pytest.approx(1025)
    assert sell_results.SpreadExpense[-1] == pytest.approx(0)
    assert sell_results.TotalTrades[-1] == 2
    assert symbol(sell_results).LongPositions[-1] == 0

    expiry_data = make_data(daily('2020-01-01', 4), [100] * 4,
                            highs=[101] * 4, lows=[96] * 4, closes=[100] * 4,
                            title='LIMIT_EXPIRY')

    _, expiry_results = run_scripted(
        [expiry_data],
        actions={0: lambda bt: bt.exec().buy(num=5, exact=True, limit=95, limit_validity=2)},
        initial_deposit=1000,
    )

    assert expiry_results.Cash[-1] == pytest.approx(1000)
    assert expiry_results.TotalTrades[-1] == 0
    assert symbol(expiry_results).LongPositions[-1] == 0
