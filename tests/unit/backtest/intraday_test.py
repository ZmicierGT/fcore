"""Intraday cycle-vs-day semantics tests for the backtesting engine.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
import pytest

from backtest.base import SecState
from helpers import run_scripted, symbol
from synthetic import intraday, make_data

THREE_BARS = ['09:30:00', '10:30:00', '15:30:00']


def test_intraday_result_rows_are_per_bar():
    """Intraday case 1: each bar is a separate calculation cycle."""
    dates = intraday('2020-01-01', 2, ['09:30:00', '10:30:00'])
    data = make_data(dates, [100, 101, 102, 103], title='BARS')

    _, results = run_scripted(
        [data],
        actions={
            0: lambda bt: bt.exec().buy(num=1, exact=True),
            1: lambda bt: bt.exec().buy(num=1, exact=True),
            2: lambda bt: bt.exec().sell(num=1),
        },
        initial_deposit=1000,
    )

    assert results.DateTime == [
        '2020-01-01 09:30:00',
        '2020-01-01 10:30:00',
        '2020-01-02 09:30:00',
        '2020-01-02 10:30:00',
    ]
    assert results.TotalTrades == [1, 2, 3, 3]
    assert symbol(results).LongPositions == [1, 2, 1, 1]
    assert symbol(results).TradesNo == [1, 2, 3, 3]
    assert results.Cash[-1] == pytest.approx(901)
    assert results.TotalValue[-1] == pytest.approx(1004)


def test_intraday_deposits_use_calendar_dates():
    """Intraday case 2: deposits do not happen once per bar."""
    dates = intraday('2020-01-01', 3, THREE_BARS)
    data = make_data(dates, [10] * len(dates), title='INTRADAY_DEPOSITS')

    _, results = run_scripted(
        [data],
        initial_deposit=0,
        periodic_deposit=100,
        deposit_interval=1,
    )

    assert results.Deposits == pytest.approx([0, 0, 0, 100, 100, 100, 200, 200, 200])
    assert results.Cash == pytest.approx([0, 0, 0, 100, 100, 100, 200, 200, 200])
    assert results.TotalValue == pytest.approx([0, 0, 0, 100, 100, 100, 200, 200, 200])


def test_intraday_cash_interest_across_overnight_gap():
    """Intraday case 3: the overnight session gap counts as one calendar day."""
    dates = intraday('2020-01-01', 2, ['09:30:00', '15:30:00'])
    data = make_data(dates, [100] * len(dates), title='INTRADAY_INTEREST')
    rate = 10 / 100 / 365.25

    _, results = run_scripted([data], initial_deposit=10000, cash_interest=10)

    cash_after_first = 10000 * (1 + rate)
    cash_after_next_date = cash_after_first * (1 + rate)

    assert results.OtherProfit[0] == pytest.approx(10000 * rate)
    assert results.Cash[0] == pytest.approx(cash_after_first)
    assert results.Cash[1] == pytest.approx(cash_after_first)
    assert results.OtherProfit[2] == pytest.approx(10000 * rate + cash_after_first * rate)
    assert results.Cash[2] == pytest.approx(cash_after_next_date)
    assert results.Cash[3] == pytest.approx(cash_after_next_date)
    assert results.OtherProfit[3] == pytest.approx(results.OtherProfit[2])


def test_intraday_limit_order_validity_uses_calendar_dates():
    """Intraday case 4: limit orders age per date, not per bar."""
    dates = intraday('2020-01-01', 3, THREE_BARS)
    data = make_data(dates, [100] * len(dates),
                     highs=[101] * len(dates),
                     lows=[96] * len(dates),
                     closes=[100] * len(dates),
                     title='INTRADAY_LIMIT')
    seen = {}

    def record_day2(bt):
        seen['day2'] = bt.exec().is_limit

    def record_day3(bt):
        seen['day3'] = bt.exec().is_limit

    backtest, results = run_scripted(
        [data],
        actions={
            0: lambda bt: bt.exec().buy(num=5, exact=True, limit=95, limit_validity=1),
            3: record_day2,
            6: record_day3,
        },
        initial_deposit=1000,
    )

    assert seen == {'day2': True, 'day3': False}
    assert backtest.all_exec[0].is_limit is False
    assert results.TotalTrades == [0] * len(dates)
    assert results.Cash[-1] == pytest.approx(1000)
    assert symbol(results).LongPositions[-1] == 0


def test_intraday_delisting_uses_calendar_dates():
    """Intraday case 5: same-day bars do not make a symbol stale."""
    first = make_data(intraday('2020-01-01', 1, ['09:30:00', '15:30:00']),
                      [100, 100], title='AAA')
    second = make_data(intraday('2020-01-01', 10, THREE_BARS),
                       [50] * 30, title='BBB')
    seen = {}

    def record_day8(bt):
        seen['day8'] = bt.all_exec[0].state

    def record_day9(bt):
        seen['day9'] = bt.all_exec[0].state

    backtest, results = run_scripted(
        [first, second],
        actions={
            0: lambda bt: bt.all_exec[0].buy(num=5, exact=True),
            23: record_day8,
            24: record_day9,
        },
        initial_deposit=1000,
    )

    assert seen['day8'] is SecState.Active
    assert seen['day9'] is SecState.Delisted
    assert backtest.all_exec[0].state is SecState.Delisted
    assert backtest.all_exec[0].long_positions == 5
    assert results.TotalValue[23] == pytest.approx(1000)
    assert results.OtherExpense[23] == pytest.approx(0)
    assert results.TotalValue[24] == pytest.approx(500)
    assert results.OtherExpense[24] == pytest.approx(500)
    assert results.TotalValue[-1] == pytest.approx(500)
    assert results.TotalTrades[-1] == 1


def test_intraday_year_boundary_inflation_and_deposits():
    """Intraday case 6: inflation applies on the date-based year change."""
    dates = (intraday('2020-12-31', 1, ['09:30:00', '15:30:00'])
             + intraday('2021-01-01', 1, ['09:30:00', '15:30:00'])
             + intraday('2021-01-02', 1, ['09:30:00']))
    data = make_data(dates, [10] * len(dates), title='INTRADAY_INFLATION')

    backtest, results = run_scripted(
        [data],
        initial_deposit=0,
        periodic_deposit=100,
        deposit_interval=1,
        inflation=10,
    )

    assert results.Deposits == pytest.approx([0, 0, 110, 110, 220])
    assert results.Cash == pytest.approx([0, 0, 110, 110, 220])
    assert backtest._periodic_deposit == pytest.approx(110)
    assert backtest._year == 2021


def test_intraday_skipped_cycles_do_not_advance_counters():
    """Intraday case 7: skipped bars do not advance days, deposits or interest."""
    dates = intraday('2020-01-01', 2, THREE_BARS)
    data = make_data(dates, [10] * len(dates), title='INTRADAY_SKIP')
    rate = 10 / 100 / 365.25
    ran = []

    def record(index):
        return lambda bt: ran.append(index)

    _, results = run_scripted(
        [data],
        actions={index: record(index) for index in range(len(dates))},
        skip=lambda index: index == 2,
        offset=1,
        initial_deposit=1000,
        periodic_deposit=100,
        deposit_interval=1,
        cash_interest=10,
    )

    assert ran == [1, 3, 4, 5]

    for index in (0, 2):
        assert results.TotalValue[index] is None
        assert results.Deposits[index] is None
        assert results.Cash[index] is None

    cash_after_first = 1000 * (1 + rate)
    cash_before_interest_day2 = cash_after_first + 100
    cash_after_day2 = cash_before_interest_day2 * (1 + rate)

    assert results.Deposits[1] == pytest.approx(1000)
    assert results.Cash[1] == pytest.approx(cash_after_first)
    assert results.Deposits[3] == pytest.approx(1100)
    assert results.Cash[3] == pytest.approx(cash_after_day2)
    assert results.Cash[4] == pytest.approx(cash_after_day2)
    assert results.Cash[5] == pytest.approx(cash_after_day2)
    assert results.TotalTrades == [0] * len(dates)


def test_intraday_midnight_crossing_counts_as_calendar_day():
    """Intraday case 8: crossing midnight advances the day counters."""
    data = make_data(['2020-01-01 23:00:00', '2020-01-02 01:00:00'],
                     [100, 100], title='MIDNIGHT')
    rate = 10 / 100 / 365.25

    _, results = run_scripted(
        [data],
        initial_deposit=1000,
        periodic_deposit=100,
        deposit_interval=1,
        cash_interest=10,
    )

    cash_after_first = 1000 * (1 + rate)
    cash_before_interest_next = cash_after_first + 100
    cash_after_next = cash_before_interest_next * (1 + rate)

    assert results.Deposits == pytest.approx([1000, 1100])
    assert results.Cash[0] == pytest.approx(cash_after_first)
    assert results.Cash[1] == pytest.approx(cash_after_next)
    assert results.OtherProfit[1] == pytest.approx(1000 * rate + cash_before_interest_next * rate)
