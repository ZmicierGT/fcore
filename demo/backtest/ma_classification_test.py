"""Demonstration of MA/price cross strategy combined with AI estimation of fake signals.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

from indicators.classifier import Algorithm
from indicators.ma_classifier import MAClassifier

from backtest.ma_classification import MAClassification
from backtest.ma import MA

from backtest.base import BackTestError
from backtest.stock import StockData

from indicators.base import IndicatorError

from data.futils import check_date
from data.futils import standard_margin_chart

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from data.fdata import Query, ReadOnlyData
from data.futils import write_image
from data.fdata import FdataError

import sys

# Variables for testing
symbol = 'SPY'
period = 50  # Period for MA calculation
change_period = 2  # Number of cycles to consider the trend as changed if there was no signal
change_percent = 2  # Change of price in percent to consider the trend as changed if there was no signal

true_ratio = 0.004  # Ratio of ma/quote change to consider it as a true signal. It should be achieved withing cycles_num to be considered as true.
cycle_num = 2  # Number of cycles to wait for the true_ratio value. If true_ratio is not reached withing these cycles, the signal is considered as false.
algorithm = Algorithm.LDA  # The default algorithm to use

if __name__ == "__main__":
    # Get a separate data for learning and testing.

    query_learn = Query()
    query_test = Query()

    query_learn.symbol = symbol
    query_learn.db_connect()

    query_test.symbol = symbol
    query_test.db_connect()

    query_learn.first_date = check_date("2000-1-1")[1]
    query_learn.last_date = check_date("2020-8-1")[1]

    query_test.first_date = check_date("2020-1-1")[1]
    query_test.last_date = check_date("2022-8-1")[1]

    data_learn = ReadOnlyData(query_learn)
    data_test = ReadOnlyData(query_test)

    try:
        rows_learn = data_learn.get_quotes()
        rows = data_test.get_quotes()

        query_learn.db_close()
        query_test.db_close()
    except FdataError as e:
        print(e)
        sys.exit(2)

    length_learn = len(rows_learn)
    length = len(rows)

    print(f"Obtained {length_learn} rows for learning and {length} rows for testing.")

    if length_learn == 0 or length == 0:
        print(f"Make sure that the symbol {symbol} is fetched and presents in the {query_learn.db_name} database.")
        sys.exit(2)

    # Train the models

    classifier = MAClassifier(period,
                              data_to_learn=[rows_learn],
                              true_ratio=true_ratio,
                              cycle_num=cycle_num,
                              algorithm=algorithm)

    try:
        classifier.learn()
        accuracy_buy_learn, accuracy_sell_learn, total_accuracy_learn = classifier.get_learn_accuracy()
    except IndicatorError as e:
        print(f"Can't train MA classification models: {e}")
        sys.exit(2)

    print('\nBuy train Accuracy:{: .2f}%'.format(accuracy_buy_learn * 100))
    print('Sell train Accuracy:{: .2f}%'.format(accuracy_sell_learn * 100))
    print('Total train Accuracy:{: .2f}%'.format(total_accuracy_learn * 100))

    # Perform a backtest

    quotes = StockData(rows=rows,
                          title=symbol,
                          margin_rec=0.4,
                          margin_req=0.7,
                          spread=0.1,
                          margin_fee=1,
                          trend_change_period=change_period,
                          trend_change_percent=change_percent
                         )

    try:
        classification = MAClassification(data=[quotes],
                                          commission=2.5,
                                          initial_deposit=10000,
                                          periodic_deposit=500,
                                          deposit_interval=30,
                                          inflation=2.5,
                                          period=period,
                                          margin_rec=0.9,
                                          margin_req=1,
                                          classifier=classifier
                                        )

        classification.calculate()
        results_cls = classification.get_results()
        accuracy_buy_est, accuracy_sell_est, total_accuracy_est = classifier.check_est_precision()
    except BackTestError as e:
        print(f"Can't perform backtesting: {e}")
        sys.exit(2)

    print('\nBuy estimation Accuracy:{: .2f}%'.format(accuracy_buy_est * 100))
    print('Sell estimation Accuracy:{: .2f}%'.format(accuracy_sell_est * 100))
    print('Total estimation Accuracy:{: .2f}%'.format(total_accuracy_est * 100))

    print(f"\nThe actual/estimated signals:\n{classifier.get_df_signals_to_compare().to_string()}\n")

    # Compare with regular MA-cross strategy

    # Create the 'regular' MA-Cross result for comparison
    ma = MA(data=[quotes],
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

    standard_margin_chart(results_cls, title=f"MA/Quote Cross + AI Backtesting Example for {symbol}", fig=fig)

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
    fig.add_trace(go.Scatter(x=results.DateTime, y=results.TotalValue, mode='lines', name=f"MA Cross {symbol}"), row=3, col=1)

    ######################
    # Write the chart
    ######################

    new_file = write_image(fig)

    print(f"{new_file} is written.")
