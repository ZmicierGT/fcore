"""Demonstration of MA/price cross strategy combined with AI estimation of fake signals.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

from backtest.ma_classification import MACls
from backtest.ma_classification import Algorithm
from backtest.ma_classification import MA

from backtest.base import BackTestError
from backtest.stock import StockData

from data.futils import check_date
from data.futils import check_datetime
from data.futils import standard_margin_chart

import plotly.graph_objects as go
from plotly.subplots import make_subplots

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

    change_period = 2
    change_percent = 2

    quotes_cls = StockData(rows=rows,
                          title=query.symbol,
                          margin_rec=0.4,
                          margin_req=0.7,
                          spread=0.1,
                          margin_fee=1,
                          trend_change_period=change_period,
                          trend_change_percent=change_percent
                         )

    ma_cls = MACls(data=[quotes_cls],
                    commission=2.5,
                    initial_deposit=10000,
                    periodic_deposit=500,
                    deposit_interval=30,
                    inflation=2.5,
                    period=period,
                    margin_rec=0.9,
                    margin_req=1,
                    algorithm=Algorithm.KNC,
                    cycle_num=2,
                    true_ratio=0.01
                   )

    try:
        ma_cls.calculate()
        results_cls = ma_cls.get_results()
    except BackTestError as e:
        print(f"Can't perform backtesting calculation: {e}")
        sys.exit(2)

    accuracy_buy_train, accuracy_buy_test = ma_cls.get_buy_accuracy()
    accuracy_sell_train, accuracy_sell_test = ma_cls.get_sell_accuracy()

    print('Buy train Accuracy:{: .2f}%'.format(accuracy_buy_train * 100))
    print('Buy test Accuracy:{: .2f}%'.format(accuracy_buy_test * 100))
    print('Sell train Accuracy:{: .2f}%'.format(accuracy_sell_train * 100))
    print('Sell test Accuracy:{: .2f}%'.format(accuracy_sell_test * 100))

    actual_buy_signals, pred_buy_signals = ma_cls.get_buy_signals()
    actual_sell_signals, pred_sell_signals = ma_cls.get_sell_signals() 

    # Initial/generated signals
    print(f"\nBuy signals: {actual_buy_signals}")
    print(f"Predicted buy signals: {pred_buy_signals}")
    print(f"Sell signals: {actual_sell_signals}")
    print(f"Predicted sell signals: {pred_sell_signals}")

    # Get data for comparision

    query.first_date = check_datetime(ma_cls.get_min_ma_dt())[1]
    query.db_connect()

    data_cmp = ReadOnlyData(query)

    try:
        rows = data_cmp.get_quotes()
        query.db_close()
    except FdataError as e:
        print(e)
        sys.exit(2)

    length_cmp = len(rows)

    print(f"Obtained {length} rows for comparison.")

    quotes_cmp = StockData(rows=rows,
                          title=query.symbol,
                          margin_rec=0.4,
                          margin_req=0.7,
                          spread=0.1,
                          margin_fee=1,
                          trend_change_period=change_period,
                          trend_change_percent=change_percent
                         )

    # Create the 'regular' MA-Cross result for comparison
    ma = MA(data=[quotes_cmp],
            commission=2.5,
            initial_deposit=10000,
            periodic_deposit=500,
            deposit_interval=30,
            inflation=2.5,
            period=period,
            margin_rec=0.9,
            margin_req=1
            )

    try:
        ma.calculate()
        results = ma.get_results()
    except BackTestError as e:
        print(f"Can't perform backtesting calculation: {e}")
        sys.exit(2)

    ##################
    # Build the charts
    ##################

    # Create a custom figure
    fig = make_subplots(rows=4, cols=1, shared_xaxes=True, row_width=[0.25, 0.25, 0.25, 0.25],
                        specs=[[{"secondary_y": True}],
                            [{"secondary_y": False}],
                            [{"secondary_y": True}],
                            [{"secondary_y": False}]])

    standard_margin_chart(results_cls, title=f"MA/Quote Cross + AI Backtesting Example for {query.symbol}", fig=fig)

    # Append MA values to the main chart
    fig.add_trace(go.Scatter(x=results_cls.DateTime, y=results_cls.Symbols[0].Tech, mode='lines', name="MA"), secondary_y=False)

    # Add strategy comparison to the second chart

    fig.add_trace(go.Scatter(x=results.DateTime, y=results.Symbols[0].Quote, mode='lines', name=f'Quotes {results.Symbols[0].Title[0]}'), row=2, col=1)
    fig.add_trace(go.Scatter(x=results.DateTime, y=results.Symbols[0].TradePriceLong, mode='markers', name=f'Long Trades {results.Symbols[0].Title[0]}'), row=2, col=1)
    fig.add_trace(go.Scatter(x=results.DateTime, y=results.Symbols[0].TradePriceShort, mode='markers', name=f'Short Trades {results.Symbols[0].Title[0]}'), row=2, col=1)
    fig.add_trace(go.Scatter(x=results.DateTime, y=results.Symbols[0].TradePriceMargin, mode='markers', name=f'Margin Req Trades {results.Symbols[0].Title[0]}'), row=2, col=1)

    # Append MA values to the second chart
    fig.add_trace(go.Scatter(x=results.DateTime, y=results_cls.Symbols[0].Tech, mode='lines', name="MA"), row=2, col=1)

    # Add second strategy results for comparison
    fig.add_trace(go.Scatter(x=results.DateTime, y=results.TotalValue, mode='lines', name=f"MA Cross {query.symbol}"), row=3, col=1)

    ######################
    # Write the chart
    ######################

    new_file = write_image(fig)

    print(f"{new_file} is written.")
