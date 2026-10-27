"""Deposit, inflation and cash-interest tests for the backtesting engine.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
import pytest

from helpers import run_scripted, symbol
from synthetic import daily, make_data


def test_periodic_deposits_and_same_cycle_investment():
    """Case 5: deposits happen on calendar intervals and are investable immediately."""
    data = make_data(daily('2020-01-01', 100), [10] * 100, title='DEPOSITS')

    def invest_all(bt):
        bt.exec().buy()

    _, results = run_scripted(
        [data],
        actions={30: invest_all, 60: invest_all, 90: invest_all},
        initial_deposit=0,
        periodic_deposit=100,
        deposit_interval=30,
    )

    assert results.DateTime[29] == '2020-01-30 23:59:59'
    assert results.DateTime[30] == '2020-01-31 23:59:59'
    assert results.DateTime[60] == '2020-03-01 23:59:59'
    assert results.DateTime[90] == '2020-03-31 23:59:59'

    assert results.Deposits[29] == pytest.approx(0)
    assert results.Deposits[30] == pytest.approx(100)
    assert results.Deposits[59] == pytest.approx(100)
    assert results.Deposits[60] == pytest.approx(200)
    assert results.Deposits[89] == pytest.approx(200)
    assert results.Deposits[90] == pytest.approx(300)

    assert results.Cash[29] == pytest.approx(0)
    assert results.Cash[30] == pytest.approx(0)
    assert results.Cash[60] == pytest.approx(0)
    assert results.Cash[90] == pytest.approx(0)
    assert symbol(results).LongPositions[90] == 30
    assert results.TotalValue[90] == pytest.approx(300)

    # A date gap longer than two intervals must compensate both deposits in one cycle.
    gap_dates = daily('2020-01-01', 10) + daily('2020-03-10', 10)
    gap_data = make_data(gap_dates, [10] * 20, title='GAP')

    _, gap_results = run_scripted(
        [gap_data],
        actions={10: invest_all},
        initial_deposit=0,
        periodic_deposit=100,
        deposit_interval=30,
    )

    assert gap_results.Deposits[9] == pytest.approx(0)
    assert gap_results.Deposits[10] == pytest.approx(200)
    assert gap_results.Cash[10] == pytest.approx(0)
    assert symbol(gap_results).LongPositions[10] == 20
    assert gap_results.TotalValue[10] == pytest.approx(200)


def test_inflation_adjusted_periodic_deposits():
    """Case 6: inflation compounds yearly and does not adjust the initial deposit."""
    dates = ['2020-01-01', '2020-12-31', '2021-01-01', '2021-12-31', '2022-01-01', '2022-12-31']
    data = make_data(dates, [10] * len(dates), title='INFLATION')

    backtest, results = run_scripted(
        [data],
        initial_deposit=1000,
        periodic_deposit=100,
        deposit_interval=365,
        inflation=10,
    )

    assert results.Deposits == pytest.approx([1000, 1100, 1100, 1210, 1210, 1331])
    assert results.Cash == pytest.approx([1000, 1100, 1100, 1210, 1210, 1331])
    assert results.TotalValue == pytest.approx([1000, 1100, 1100, 1210, 1210, 1331])
    assert results.TotalTrades == [0] * len(dates)

    assert backtest.initial_deposit == pytest.approx(1000)
    assert backtest._periodic_deposit == pytest.approx(121)
    assert backtest._year == 2022


def test_cash_interest_positive_negative_and_zero_day_gap():
    """Case 7: interest accrues per calendar day and can be negative."""
    rate = 10 / 100 / 365.25

    data = make_data(['2020-01-01', '2020-01-03'], [100, 100], title='INTEREST')
    _, results = run_scripted([data], initial_deposit=10000, cash_interest=10)

    cash_after_first = 10000 * (1 + rate)
    cash_after_second = cash_after_first * (1 + rate * 2)

    assert results.OtherProfit[0] == pytest.approx(10000 * rate)
    assert results.Cash[0] == pytest.approx(cash_after_first)
    assert results.OtherProfit[1] == pytest.approx(cash_after_second - 10000)
    assert results.Cash[1] == pytest.approx(cash_after_second)
    assert results.TotalValue[1] == pytest.approx(cash_after_second)

    negative_data = make_data(['2020-01-01'], [100], title='NEG_INTEREST')
    _, negative_results = run_scripted([negative_data], initial_deposit=10000, cash_interest=-10)

    assert negative_results.OtherProfit[0] == pytest.approx(-10000 * rate)
    assert negative_results.Cash[0] == pytest.approx(10000 * (1 - rate))
    assert negative_results.TotalValue[0] == pytest.approx(10000 * (1 - rate))

    intraday_data = make_data(['2020-01-01 10:00:00', '2020-01-01 15:00:00'],
                              [100, 100], title='INTRADAY')
    _, intraday_results = run_scripted([intraday_data], initial_deposit=10000, cash_interest=10)

    assert intraday_results.OtherProfit[0] == pytest.approx(10000 * rate)
    assert intraday_results.Cash[0] == pytest.approx(cash_after_first)
    assert intraday_results.Cash[1] == pytest.approx(cash_after_first)
    assert intraday_results.OtherProfit[1] == pytest.approx(10000 * rate)
