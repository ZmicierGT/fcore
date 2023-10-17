"""Minimalistic demonstration of MA Classifier.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from data.fvalues import Algorithm
from tools.ma_classifier import MAClassifier

from backtest.ma_classification import MAClassification
from backtest.ma import MA
from backtest.stock import StockData
from backtest.reporting import Report

from data.yf import YF

import plotly.graph_objects as go

period = 50  # SMA period

min_width = 2500 # Minimum width for reporting
height = 250  # Height of each subchart in reporting

# Get data for training/testing. All the data will be cached in a database without the need of further fetching
rows_learn = YF(symbol='SPY', first_date="2000-1-1", last_date="2021-1-1").fetch_stock_data_if_none(124, 0)
rows_test = YF(symbol='SPY', first_date="2021-1-2", last_date="2023-4-1").fetch_stock_data_if_none(12, 0)

# Train the model
classifier = MAClassifier(period=period,  # SMA Period
                          data_to_learn=[rows_learn],  # Raw quote data to train the model
                          true_ratio=0.004,  # Ratio when signal is considered as true in cycle_num.
                                             # For example, if true_ratio is 0.03 and cycle_num is 5,
                                             # then the signal will be considered as true if there was a 3% change in
                                             # quote in the following 5 cycles after getting the signal.
                          cycle_num=2,  # Nuber of cycles to reach true_ratio to consider the signal as true.
                          algorithm=Algorithm.LDA)  # Classification algorithm to use.

classifier.learn()

# Perform the backtest

# Define backtesting parameters for SPY
quotes = StockData(rows=rows_test,  # Raw quote data
                   title='SPY',
                   spread=0.1,  # Expected spread
                   trend_change_period=2,  # Num of trade cycles (Days) when a stable trend is considered as changed
                   trend_change_percent=2  # Change in percent to consider the trend as changed immediately
                  )

# Parameters for backtesting
params = {
    'data': [quotes],
    'commission': 2.5,
    'initial_deposit': 10000,
    'periodic_deposit': 500,
    'deposit_interval': 30,
    'inflation': 2.5,
    'period': period,
    'margin_rec': 0.9,  # Use some margin (required and recommended) to test shorting.
    'margin_req': 1
}

# Perform backtest using AI classification of signals
classification = MAClassification(**params, classifier=classifier)

classification.calculate()  # It starts the calculation in a separate thread which allows you to make a parralel computations
                            # if you use a Pyhon interpreter without GIL.

# Regular strategy (without classifying signals) for comparison
ma = MA(**params)

ma.calculate()

results_cls = classification.get_results()  # Wait till calculation finishes and return the results.
results_cmp = ma.get_results()

# Generate a report with performance comparison
report = Report(data=results_cls, width=max(length_test, min_width), margin=True)

fig_quotes = report.add_quotes_chart(title="MA/Quote Cross + AI Backtesting Example")
fig_quotes.add_trace(go.Scatter(x=results_cls.DateTime, y=results_cls.Symbols[0].Tech[0], mode='lines', name="MA", line=dict(color="green")))

fig_cmp = report.add_quotes_chart(title="Regular MA/Quote cross Example for Comparison", data=results_cmp, height=height)
fig_cmp.add_trace(go.Scatter(x=results_cmp.DateTime, y=results_cmp.Symbols[0].Tech[0], mode='lines', name="MA", line=dict(color="green")))

fig_portf = report.add_portfolio_chart(height=height)
fig_portf.add_trace(go.Scatter(x=results_cmp.DateTime, y=results_cmp.TotalValue, mode='lines', name="MA Cross Results"))

report.add_expenses_chart(height=height)

# Add annotations with strategy results
report.add_annotations(title="MA Classifier performance:")
report.add_annotations(data=results_cmp, title="Regular MA/Price Crossover performance:")

# Show image
report.show_image()
