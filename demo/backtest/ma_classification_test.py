"""Demonstration of MA/price cross strategy combined with AI estimation of fake signals.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
# TODO LOW Remove gaps between docstrings and imports
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
from tools.ma_classifier import MAClassifier

from backtest.ma_classification import MAClassification
from backtest.ma import MA
from backtest.base import BackTestError
from backtest.stock import StockData
from backtest.reporting import Report

from tools.base import ToolError

from data.fdata import FdataError
from data.yf import YF

import plotly.graph_objects as go

import sys

# Variables for testing
symbol = 'SPY'
period = 50  # Period for MA calculation
change_period = 2  # Number of cycles to consider the trend as changed if there was no signal
change_percent = 2  # Change of price in percent to consider the trend as changed if there was no signal

true_ratio = 0.004  # Ratio of ma/quote change to consider it as a true signal. It should be achieved withing cycles_num to be considered as true.
cycle_num = 2  # Number of cycles to wait for the true_ratio value. If true_ratio is not reached withing these cycles, the signal is considered as false.

threshold_divs_learn = 124
threshold_divs_test = 8
threshold_splits = 0

min_width = 2500 # Minimum width for charting
height = 250  # Height of each subchart in reporting

if __name__ == "__main__":
    # Get a separate data for learning and testing.

    # Get quotes for learning
    try:
        warning = "WARNING! Using yfinance data for the demonstration.\n" +\
                  "Always keep yfinance up to date ( pip install yfinance --upgrade ) and use quotes obtained from this " +\
                  "datasource only for demonstation purposes!\n"
        print(warning)

        source = YF(symbol=symbol, first_date="2000-1-1", last_date="2021-1-1")
        rows_learn, num = source.fetch_stock_data_if_none(threshold_divs_learn, threshold_splits)
    except FdataError as e:
        sys.exit(e)

    length_learn = len(rows_learn)

    if num > 0:
        print(f"Fetched {num} quotes for {source.symbol}. Total number of quotes used is {length_learn}.")

    # Get quotes for testing
    try:
        source = YF(symbol=symbol, first_date="2021-1-2", last_date="2023-4-1")
        rows, num = source.fetch_stock_data_if_none(threshold_divs_test, threshold_splits)
    except FdataError as e:
        sys.exit(e)

    length_test = len(rows)

    if num > 0:
        print(f"Fetched {num} quotes for {source.symbol}. Total number of quotes used is {length_test}.")

    # Train the models

    classifier = MAClassifier(period,
                              data_to_learn=[rows_learn],
                              true_ratio=true_ratio,
                              cycle_num=cycle_num,
                              model_buy=LinearDiscriminantAnalysis(),
                              model_sell=LinearDiscriminantAnalysis())

    try:
        classifier.learn()
        accuracy_buy_learn, accuracy_sell_learn, total_accuracy_learn = classifier.get_learn_accuracy()
        f1_buy_learn, f1_sell_learn, total_f1_learn = classifier.get_learn_f1()
    except ToolError as e:
        sys.exit(f"Can't train MA classification models: {e}")

    print('\nBuy train accuracy:{: .2f}%'.format(accuracy_buy_learn * 100))
    print('Sell train accuracy:{: .2f}%'.format(accuracy_sell_learn * 100))
    print('Total train accuracy:{: .2f}%'.format(total_accuracy_learn * 100))

    print(f"\nBuy train f1 score: {round(f1_buy_learn, 4)}")
    print(f"Sell train f1 score: {round(f1_sell_learn, 4)}")
    print(f"Total train f1 score: {round(total_f1_learn, 4)}")

    # Perform a backtest

    quotes = StockData(rows=rows,
                          title=symbol,
                          spread=0.1,
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
        sys.exit(f"Can't perform backtesting: {e}")

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
        results_cmp = ma.get_results()
    except BackTestError as e:
        sys.exit(f"Can't perform backtesting calculation: {e}")

    #################
    # Create a report
    #################

    report = Report(data=results_cls, width=max(length_test, min_width), margin=True)

    # Add a chart with quotes
    fig_quotes = report.add_quotes_chart(title="MA/Quote Cross + AI Backtesting Example")

    # Append MA values to the quotes chart
    fig_quotes.add_trace(go.Scatter(x=results_cls.DateTime, y=results_cls.Symbols[0].Tech[0], mode='lines', name="MA", line=dict(color="green")))

    # Add strategy comparison to the second chart
    fig_cmp = report.add_quotes_chart(title="Regular MA/Quote cross Example for Comparison", data=results_cmp, height=height)

    # Append MA values to the comparison chart
    fig_cmp.add_trace(go.Scatter(x=results_cmp.DateTime, y=results_cmp.Symbols[0].Tech[0], mode='lines', name="MA", line=dict(color="green")))

    # Add a chart to represent portfolio performance
    fig_portf = report.add_portfolio_chart(height=height)

    # Add second strategy results for comparison
    fig_portf.add_trace(go.Scatter(x=results_cmp.DateTime, y=results_cmp.TotalValue, mode='lines', name="MA Cross Results"))

    # Add chart a with expenses
    report.add_expenses_chart(height=height)

    # Add annotations with strategy results
    report.add_annotations(title="MA Classifier performance:")
    report.add_annotations(data=results_cmp, title="Regular MA/Price Crossover performance:")

    # Show image
    new_file = report.show_image()
    print(f"{new_file} is written.")
