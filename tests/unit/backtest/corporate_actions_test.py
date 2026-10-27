"""Corporate-action tests: dividends, splits and delisting.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
import pytest

from backtest.base import SecState
from helpers import run_scripted, symbol
from synthetic import daily, make_data


def test_dividends_pending_payment_tax_and_no_position():
    """Case 13: dividends are accrued on ex-date, paid later and taxed."""
    dates = daily('2020-01-01', 5)
    data = make_data(dates, [100] * 5,
                     stock=True,
                     div_tax=10,
                     divs_ex=[0, 1, 0, 0, 0],
                     divs_pay=[0, 0, 0, 1, 0],
                     title='DIV')

    _, results = run_scripted(
        [data],
        actions={0: lambda bt: bt.exec().buy(num=10, exact=True)},
        initial_deposit=1000,
    )

    # Ex-date: 10 shares * 1.0 is pending, included in total value but not in cash.
    assert results.Cash[1] == pytest.approx(0)
    assert results.OtherProfit[1] == pytest.approx(0)
    assert results.TotalValue[1] == pytest.approx(1010)
    assert results.TotalTrades[1] == 1

    # Payment date: 10.0 gross - 1.0 tax = 9.0 credited to cash.
    assert results.Cash[3] == pytest.approx(9)
    assert results.OtherProfit[3] == pytest.approx(9)
    assert results.OtherExpense[3] == pytest.approx(1)
    assert results.TotalExpenses[3] == pytest.approx(1)
    assert results.TotalValue[3] == pytest.approx(1009)

    stats = results.get_statistics()

    assert stats.other_profit == pytest.approx(9)
    assert stats.other_expense == pytest.approx(1)
    assert stats.total_expenses == pytest.approx(1)
    assert stats.profit == pytest.approx(0.9)

    # Without a position the same events must not change anything.
    _, empty_results = run_scripted(
        [make_data(dates, [100] * 5,
                   stock=True,
                   div_tax=10,
                   divs_ex=[0, 1, 0, 0, 0],
                   divs_pay=[0, 0, 0, 1, 0],
                   title='NO_DIV')],
        initial_deposit=1000,
    )

    assert empty_results.Cash == pytest.approx([1000] * 5)
    assert empty_results.TotalValue == pytest.approx([1000] * 5)
    assert empty_results.OtherProfit == pytest.approx([0] * 5)
    assert empty_results.OtherExpense == pytest.approx([0] * 5)


def test_stock_split_positions_fractional_cash_and_cost_basis():
    """Case 14: a split multiplies positions, cashes fractions and preserves value."""
    split_price = 100 / 1.5
    data = make_data(daily('2020-01-01', 3), [100, split_price, 80],
                     stock=True,
                     splits=[1.0, 1.5, 1.0],
                     title='SPLIT')

    backtest, results = run_scripted(
        [data],
        actions={
            0: lambda bt: bt.exec().buy(num=5, exact=True),
            2: lambda bt: bt.exec().sell(),
        },
        initial_deposit=1000,
    )

    # 5 shares split 1.5 -> 7 shares + 0.5 fractional share cashed at the split price.
    assert symbol(results).LongPositions[1] == 7
    assert results.Cash[1] == pytest.approx(500 + 0.5 * split_price)
    assert results.TotalValue[1] == pytest.approx(1000)
    assert results.TotalTrades[1] == 1

    # Sale of 7 post-split shares at 80.
    assert symbol(results).LongPositions[2] == 0
    assert symbol(results).TradesNo[2] == 2
    assert results.Cash[2] == pytest.approx(500 + 0.5 * split_price + 7 * 80)
    assert results.TotalValue[2] == pytest.approx(500 + 0.5 * split_price + 7 * 80)
    assert backtest.all_exec[0].total_profit == pytest.approx(7 * 80 + 0.5 * split_price - 5 * 100)
    assert results.get_statistics().profit == pytest.approx(9.3333333333)


def test_delisting_writes_off_positions_and_disables_trading():
    """Case 15: stale quotes delist a symbol; a reappearing ticker becomes skipped."""
    first = make_data(['2020-01-01', '2020-01-02'], [100, 100], title='AAA')
    second = make_data(daily('2020-01-01', 15), [50] * 15, title='BBB')

    def try_trade_after_delisting(bt):
        assert bt.all_exec[0].state is SecState.Delisted
        bt.all_exec[0].buy(num=1, exact=True)

    backtest, results = run_scripted(
        [first, second],
        actions={0: lambda bt: bt.all_exec[0].buy(num=5, exact=True),
                 10: try_trade_after_delisting},
        initial_deposit=1000,
    )

    assert backtest.all_exec[0].state is SecState.Delisted
    assert backtest.all_exec[0].long_positions == 5
    assert results.TotalValue[9] == pytest.approx(500)
    assert results.TotalValue[-1] == pytest.approx(500)
    assert results.OtherExpense[9] == pytest.approx(500)
    assert results.TotalExpenses[-1] == pytest.approx(500)
    assert results.TotalTrades[-1] == 1

    # Same ticker reappears later: it must be skipped and remain untradeable.
    reappeared = make_data(['2020-01-01', '2020-01-02', '2020-01-21'], [100, 100, 100], title='AAA')
    second_long = make_data(daily('2020-01-01', 25), [50] * 25, title='BBB')

    reappeared_bt, reappeared_results = run_scripted(
        [reappeared, second_long],
        actions={0: lambda bt: bt.all_exec[0].buy(num=5, exact=True)},
        initial_deposit=1000,
    )

    assert reappeared_bt.all_exec[0].state is SecState.Skipped
    assert reappeared_bt.all_exec[0].long_positions == 5
    assert reappeared_results.TotalValue[-1] == pytest.approx(500)
    assert reappeared_results.TotalTrades[-1] == 1
