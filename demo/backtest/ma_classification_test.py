"""Demonstration of MA/price cross strategy combined with AI estimation of fake signals.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

from backtest.ma_classification import MACls

from backtest.base import BackTestError
from backtest.stock import StockData

from data.futils import check_date
from data.futils import standard_margin_chart

import plotly.graph_objects as go

from data.fdata import Query, ReadOnlyData
from data.futils import write_image
from data.fdata import FdataError

import sys

if __name__ == "__main__":
    query = Query()

    query.symbol = "SPY"
    query.db_connect()

    query.first_date = check_date("2000-1-1")[1]
    query.last_date = check_date("2022-8-1")[1]

    data = ReadOnlyData(query)

    try:
        rows = data.get_quotes()
        query.db_close()
    except FdataError as e:
        print(e)
        sys.exit(2)

    length = len(rows)

    print(f"Obtained {length} rows.")

    if length == 0:
        print(f"Make sure that the symbol {query.symbol} is fetched and presents in the {query.db_name} database.")
        sys.exit(2)

    period = 50

    quotes = StockData(rows=rows,
                          title=query.symbol,
                          margin_rec=0.4,
                          margin_req=0.7,
                          spread=0.1,
                          margin_fee=1,
                          trend_change_period=1,
                          trend_change_percent=3
                         )

    ma = MACls(
        data=[quotes],
        commission=2.5,
        initial_deposit=10000,
        periodic_deposit=500,
        deposit_interval=30,
        inflation=2.5,
        period=period,
        margin_rec=0.9,
        margin_req=1,
    )

    try:
        ma.calculate()
    except BackTestError as e:
        print(f"Can't perform backtesting calculation: {e}")
        sys.exit(2)

    results = ma.get_results()

    accuracy_buy_train, accuracy_buy_test = ma.get_buy_accuracy()
    accuracy_sell_train, accuracy_sell_test = ma.get_sell_accuracy()

    print('Buy train Accuracy:{: .2f}%'.format(accuracy_buy_train * 100))
    print('Buy test Accuracy:{: .2f}%'.format(accuracy_buy_test * 100))
    print('Sell train Accuracy:{: .2f}%'.format(accuracy_sell_train * 100))
    print('Sell test Accuracy:{: .2f}%'.format(accuracy_sell_test * 100))

    actual_buy_signals, pred_buy_signals = ma.get_buy_signals()
    actual_sell_signals, pred_sell_signals = ma.get_sell_signals() 

    # Initial/generated signals
    print(f"\nBuy signals: {actual_buy_signals}")
    print(f"Predicted buy signals: {pred_buy_signals}")
    print(f"Sell signals: {actual_sell_signals}")
    print(f"Predicted sell signals: {pred_sell_signals}")

    ##################
    # Build the charts
    ##################

    fig = standard_margin_chart(results, title=f"MA/Quote Cross + AI Backtesting Example for {query.symbol}")

    # Append MA values to the main chart
    fig.add_trace(go.Scatter(x=results.DateTime, y=results.Symbols[0].Tech, mode='lines', name="MA"), secondary_y=False)

    ######################
    # Write the chart
    ######################

    new_file = write_image(fig)

    print(f"{new_file} is written.")
