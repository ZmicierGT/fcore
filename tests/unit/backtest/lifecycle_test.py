"""Skipped-cycle and offset semantics tests for the backtesting engine.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
import pytest

from helpers import run_scripted
from synthetic import daily, make_data


def test_skipped_cycles_do_not_advance_state_or_run_actions():
    """Case 16: offset and strategy skips emit empty rows without side effects."""
    data = make_data(daily('2020-01-01', 6), [100] * 6, title='SKIP')
    rate = 10 / 100 / 365.25
    ran = []

    def record(index):
        return lambda bt: ran.append(index)

    _, results = run_scripted(
        [data],
        actions={0: record(0), 2: record(2), 3: record(3), 5: record(5)},
        skip=lambda index: index == 3,
        offset=2,
        initial_deposit=1000,
        periodic_deposit=100,
        deposit_interval=1,
        cash_interest=10,
    )

    assert ran == [2, 5]
    assert results.TotalTrades == [0] * 6
    assert results.DateTime == [f'2020-01-0{i} 23:59:59' for i in range(1, 7)]

    for index in (0, 1, 3):
        assert results.TotalValue[index] is None
        assert results.Deposits[index] is None
        assert results.Cash[index] is None
        assert results.OtherProfit[index] is None
        assert results.TotalExpenses[index] is None

    # The first calculated cycle has no elapsed days yet, so no deposit is made.
    assert results.Deposits[2] == pytest.approx(1000)
    assert results.Cash[2] == pytest.approx(1000 * (1 + rate))
    assert results.OtherProfit[2] == pytest.approx(1000 * rate)

    # The skipped day does not advance prev_dt/deposit counters. At index 4 two calendar
    # days have passed since index 2, so two daily deposits are made in one cycle.
    assert results.Deposits[4] == pytest.approx(1200)
    cash_before_interest_4 = 1000 * (1 + rate) + 200
    assert results.Cash[4] == pytest.approx(cash_before_interest_4 * (1 + 2 * rate))
    assert results.OtherProfit[4] == pytest.approx(1000 * rate + cash_before_interest_4 * rate * 2)

    assert results.Deposits[5] == pytest.approx(1300)
    cash_before_interest_5 = results.Cash[4] + 100
    assert results.Cash[5] == pytest.approx(cash_before_interest_5 * (1 + rate))
    assert results.OtherProfit[5] == pytest.approx(results.OtherProfit[4] + cash_before_interest_5 * rate)

    stats = results.get_statistics()

    assert stats.invested == pytest.approx(1300)
    assert stats.total_value == pytest.approx(results.TotalValue[-1])
    assert stats.total_trades == 0
