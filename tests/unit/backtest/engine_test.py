"""Core accounting and valuation tests for the backtesting engine.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
import pytest

from helpers import run_scripted, symbol
from synthetic import daily, make_data


def test_baseline_accounting_and_result_rows():
    """Case 1: no trades should leave deposits, cash and value unchanged."""
    data = make_data(daily('2020-01-01', 5), [100, 101, 102, 103, 104], title='BASE')
    _, results = run_scripted([data], initial_deposit=10000)

    assert results.DateTime == [f'2020-01-0{i} 23:59:59' for i in range(1, 6)]
    assert results.Deposits == pytest.approx([10000] * 5)
    assert results.Cash == pytest.approx([10000] * 5)
    assert results.TotalValue == pytest.approx([10000] * 5)
    assert results.OtherProfit == pytest.approx([0] * 5)
    assert results.CommissionExpense == pytest.approx([0] * 5)
    assert results.SpreadExpense == pytest.approx([0] * 5)
    assert results.OtherExpense == pytest.approx([0] * 5)
    assert results.TotalExpenses == pytest.approx([0] * 5)
    assert results.TotalTrades == [0] * 5

    stats = results.get_statistics()

    assert stats.invested == pytest.approx(10000)
    assert stats.total_value == pytest.approx(10000)
    assert stats.profit == pytest.approx(0)
    assert stats.total_trades == 0


def test_market_buy_sizing_commission_and_valuation():
    """Case 2: an all-in market buy uses the open price and floors the size."""
    data = make_data(daily('2020-01-01', 3), [100, 110, 120], title='BUY')
    backtest, results = run_scripted(
        [data],
        actions={0: lambda bt: bt.exec().buy()},
        initial_deposit=10000,
        commission=5,
    )

    # max size = floor((10000 - 5) / 100) = 99 shares
    assert results.Cash[0] == pytest.approx(95)
    assert results.TotalValue[0] == pytest.approx(9995)
    assert results.CommissionExpense[0] == pytest.approx(5)
    assert results.TotalExpenses[0] == pytest.approx(5)
    assert results.TotalTrades[0] == 1

    assert symbol(results).LongPositions[0] == 99
    assert symbol(results).TradesNo[0] == 1
    assert backtest.all_exec[0].long_positions == 99

    # 95 cash + 99 * 110 at the next open
    assert results.TotalValue[1] == pytest.approx(10985)
    assert results.Deposits[-1] == pytest.approx(10000)

    stats = results.get_statistics()

    assert stats.invested == pytest.approx(10000)
    assert stats.total_value == pytest.approx(11975)
    assert stats.profit == pytest.approx(19.75)
    assert stats.commission_expense == pytest.approx(5)
    assert stats.total_trades == 1


def test_market_sell_fifo_profit_and_clamping():
    """Case 3: sells consume lots FIFO and cannot exceed the open position."""
    data = make_data(daily('2020-01-01', 5), [100, 100, 50, 50, 80], title='SELL')

    def buy_first(bt):
        bt.exec().buy(num=10, exact=True)

    def buy_second(bt):
        bt.exec().buy(num=20, exact=True)

    def sell_more_than_held(bt):
        bt.exec().sell(num=100)

    backtest, results = run_scripted(
        [data],
        actions={0: buy_first, 2: buy_second, 4: sell_more_than_held},
        initial_deposit=10000,
    )

    # 10 * 100 + 20 * 50 = 2000 spent, then 30 * 80 = 2400 received
    assert results.Cash[-1] == pytest.approx(10400)
    assert results.TotalValue[-1] == pytest.approx(10400)
    assert results.TotalTrades[-1] == 3
    assert symbol(results).LongPositions[-1] == 0
    assert symbol(results).TradesNo[-1] == 3

    # FIFO realized profit: proceeds minus the original cost basis
    assert backtest.all_exec[0].total_profit == pytest.approx(400)
    assert results.get_statistics().profit == pytest.approx(4.0)


def test_spread_execution_and_expense_accounting():
    """Case 4: market orders execute around the spread."""
    data = make_data(daily('2020-01-01', 3), [100, 120, 80], title='SPREAD', spread=2.0)

    def buy(bt):
        bt.exec().buy(num=10, exact=True)

    def sell(bt):
        bt.exec().sell()

    backtest, results = run_scripted(
        [data],
        actions={0: buy, 1: sell},
        initial_deposit=10000,
    )

    # Buy at 100 + 1% = 101; sell at 120 - 1% = 118.8
    assert results.Cash[0] == pytest.approx(8990)
    assert results.SpreadExpense[0] == pytest.approx(10)
    assert results.TotalValue[0] == pytest.approx(9980)

    assert results.Cash[1] == pytest.approx(10178)
    assert results.SpreadExpense[1] == pytest.approx(22)
    assert results.TotalValue[1] == pytest.approx(10178)
    assert results.CommissionExpense[-1] == pytest.approx(0)
    assert results.TotalExpenses[-1] == pytest.approx(22)
    assert backtest.all_exec[0].total_profit == pytest.approx(178)


def test_fee_aggregation_and_expense_statistics():
    """Case 8: flat, percent and per-security commissions are aggregated correctly."""
    data = make_data(daily('2020-01-01', 2), [100, 120], title='FEES')

    backtest, results = run_scripted(
        [data],
        actions={
            0: lambda bt: bt.exec().buy(num=10, exact=True),
            1: lambda bt: bt.exec().sell(),
        },
        initial_deposit=10000,
        commission=5,
        commission_percent=1,
        commission_security=0.5,
    )

    # buy: (100 * 1% + 0.5) * 10 + 5 = 20
    assert results.CommissionExpense[0] == pytest.approx(20)
    assert results.Cash[0] == pytest.approx(8980)

    # sell: (120 * 1% + 0.5) * 10 + 5 = 22
    assert results.CommissionExpense[1] == pytest.approx(42)
    assert results.Cash[1] == pytest.approx(10158)

    assert results.SpreadExpense[-1] == pytest.approx(0)
    assert results.OtherExpense[-1] == pytest.approx(0)
    assert results.TotalExpenses[-1] == pytest.approx(42)
    assert results.TotalTrades[-1] == 2
    assert backtest.all_exec[0].long_positions == 0

    stats = results.get_statistics()

    assert stats.total_expenses == pytest.approx(42)
    assert stats.commission_expense == pytest.approx(42)
    assert stats.profit == pytest.approx(1.58)


def test_valuation_and_unrealized_profit():
    """Case 9: portfolio value follows the sell price without affecting expenses."""
    data = make_data(daily('2020-01-01', 3), [100, 120, 80], title='VALUE')

    _, results = run_scripted(
        [data],
        actions={0: lambda bt: bt.exec().buy(num=10, exact=True)},
        initial_deposit=1000,
    )

    assert results.Cash == pytest.approx([0, 0, 0])
    assert results.TotalValue == pytest.approx([1000, 1200, 800])
    assert results.OtherProfit == pytest.approx([0, 0, 0])
    assert results.TotalExpenses == pytest.approx([0, 0, 0])
    assert symbol(results).LongPositions == [10, 10, 10]

    stats = results.get_statistics()

    assert stats.total_value == pytest.approx(800)
    assert stats.profit == pytest.approx(-20)
