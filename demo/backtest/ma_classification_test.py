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

from data.futils import standard_margin_chart

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from data.futils import write_image
from data.fdata import FdataError

from data.yf import YFError, YFQuery, YF

import sys

# Variables for testing
symbol = 'SPY'
period = 50  # Period for MA calculation
change_period = 2  # Number of cycles to consider the trend as changed if there was no signal
change_percent = 2  # Change of price in percent to consider the trend as changed if there was no signal

true_ratio = 0.004  # Ratio of ma/quote change to consider it as a true signal. It should be achieved withing cycles_num to be considered as true.
cycle_num = 2  # Number of cycles to wait for the true_ratio value. If true_ratio is not reached withing these cycles, the signal is considered as false.
algorithm = Algorithm.LDA  # The default algorithm to use

threshold_learn = 5178  # Quotes num threshold for the learning
threshold_test = 567  # Quotes num threshold for the test

if __name__ == "__main__":
    # Get a separate data for learning and testing.

    # Get quotes for learning
    try:
        # Fetch quotes if there are less than a threshold number of records in the database for the specified timespan.
        query = YFQuery(symbol="SPY", first_date="2000-1-1", last_date="2020-8-1")
        rows_learn, num = YF(query).fetch_if_none(threshold_learn)
    except (YFError, FdataError) as e:
        print(e)
        sys.exit(2)

    length_learn = len(rows_learn)

    if num > 0:
        print(f"Fetched {num} quotes for {query.symbol}. Total number of quotes used is {length_learn}.")
    else:
        print(f"No need to fetch quotes for {query.symbol}. There are {length_learn} quotes in the database and it is >= the threshold level of {threshold_learn}.")

    # Get quotes for testing
    try:
        # Fetch quotes if there are less than a threshold number of records in the database for the specified timespan.
        query = YFQuery(symbol="SPY", first_date="2020-8-2", last_date="2022-11-1")
        rows, num = YF(query).fetch_if_none(threshold_test)
    except (YFError, FdataError) as e:
        print(e)
        sys.exit(2)

    length_test = len(rows)

    if num > 0:
        print(f"Fetched {num} quotes for {query.symbol}. Total number of quotes used is {length_test}.")
    else:
        print(f"No need to fetch quotes for {query.symbol}. There are {length_test} quotes in the database and it is >= the threshold level of {threshold_test}.")

    # Train the models

    classifier = MAClassifier(period,
                              data_to_learn=[rows_learn],
                              true_ratio=true_ratio,
                              cycle_num=cycle_num,
                              algorithm=algorithm)

    try:
        classifier.learn()
        accuracy_buy_learn, accuracy_sell_learn, total_accuracy_learn = classifier.get_learn_accuracy()
        f1_buy_learn, f1_sell_learn, total_f1_learn = classifier.get_learn_f1()
    except IndicatorError as e:
        print(f"Can't train MA classification models: {e}")
        sys.exit(2)

    print('\nBuy train accuracy:{: .2f}%'.format(accuracy_buy_learn * 100))
    print('Sell train accuracy:{: .2f}%'.format(accuracy_sell_learn * 100))
    print('Total train accuracy:{: .2f}%'.format(total_accuracy_learn * 100))

    print(f"\nBuy train f1 score: {round(f1_buy_learn, 4)}")
    print(f"Sell train f1 score: {round(f1_sell_learn, 4)}")
    print(f"Total train f1 score: {round(total_f1_learn, 4)}")

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
        accuracy_buy_est, accuracy_sell_est, total_accuracy_est = classifier.get_est_accuracy()
        f1_buy_est, f1_sell_est, total_f1_est = classifier.get_est_f1()
    except BackTestError as e:
        print(f"Can't perform backtesting: {e}")
        sys.exit(2)

    print('\nBuy estimation accuracy:{: .2f}%'.format(accuracy_buy_est * 100))
    print('Sell estimation accuracy:{: .2f}%'.format(accuracy_sell_est * 100))
    print('Total estimation accuracy:{: .2f}%'.format(total_accuracy_est * 100))

    print(f"\nBuy estimation f1 score: {round(f1_buy_est, 4)}")
    print(f"Sell estimation f1 score: {round(f1_sell_est, 4)}")
    print(f"Total estimation f1 score: {round(total_f1_est, 4)}")

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
