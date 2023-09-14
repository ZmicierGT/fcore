"""Demonstration of a growth probability algorithm.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from data.yf import YF
from data.fdata import FdataError

from data.fvalues import StockQuotes

from tools.growth_probability import Probability
from data.fvalues import Algorithm

from tools.base import ToolError

from data.futils import update_layout
from data.futils import show_image

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from time import perf_counter

import sys

# Parameters for learning
true_ratio = 0.004  # Ratio of ma/quote change to consider it as a true signal. It should be achieved withing cycles_num to be considered as true.
cycle_num = 2  # Number of cycles to wait for the true_ratio value. If true_ratio is not reached withing these cycles, the signal is considered as false.
algorithm = Algorithm.KNC  # The default algorithm to use
period_long = 50  # Long period for MA calculation
period_short = 25  # Short period for MA calculation
symbol = 'SPY'  # Symbol to make estimations

first_date = "2020-11-1"  # First date to fetch quotes (for testing only)
last_date = "2022-11-1"  # The last date to fetch quotes
def_threshold = 15314  # The default quotes num required for the calculation for each symbol
test_threshold = 500  # Minimum threshold value for testing

# For learning we may use the previous quotes of the same stock or use quotes of other stocks if the used indicators are percent/ratio based.
# In this case, DJIA stocks are used to train the models.

# DJIA composition [symbol, quotes_threshold]. More quotes will be fetched if the threshold is not met.
symbols = [['MMM', def_threshold],
           ['AXP', 12715],
           ['AMGN', 9926],
           ['AAPL', 10058],
           ['BA', def_threshold],
           ['CAT', def_threshold],
           ['CVX', def_threshold],
           ['CSCO', 8240],
           ['KO', def_threshold],
           ['DIS', def_threshold],
           ['DOW', 913],
           ['GS', 5914],
           ['HD', 10366],
           ['HON', def_threshold],
           ['IBM', def_threshold],
           ['INTC', 10749],
           ['JNJ', def_threshold],
           ['JPM', 10749],
           ['MCD', 14179],
           ['MRK', def_threshold],
           ['MSFT', 9235],
           ['NKE', 10569],
           ['PG', def_threshold],
           ['CRM', 4623],
           ['TRV', 11842],
           ['UNH', 9588],
           ['VZ', 9817],
           ['V', 3682],
           ['WBA', 10749],
           ['WMT', 12655]]

if __name__ == "__main__":
    # Array for the fetched data for all symbols
    allrows = []

    print("Fetchig the required quotes for model training. Press CTRL-C and restart if it stucks.")

    for symbol_learn, threshold in symbols:
        try:
            # Fetch quotes if there are less than a threshold number of records in the database for a day (default) timespan
            source = YF(symbol=symbol_learn, last_date=last_date)
            rows, num = source.fetch_if_none(threshold)
        except FdataError as e:
            sys.exit(e)

        if num > 0:
            print(f"Fetched {num} quotes for {source.symbol}. Total number of quotes used is {len(rows)}.")
        else:
            print(f"No need to fetch quotes for {source.symbol}. There are {len(rows)} quotes in the database and it is >= the threshold level of {threshold}.")

        allrows.append(rows)

    # Get quotes for estimations
    try:
        # Fetch quotes if there are less than a threshold number of records in the database for a day (default) timespan
        source = YF(symbol=symbol, first_date=first_date, last_date=last_date)
        est_rows, num = source.fetch_if_none(test_threshold)
    except FdataError as e:
        sys.exit(e)

    length = len(est_rows)

    if num > 0:
        print(f"Fetched {num} quotes for {source.symbol}. Total number of quotes used is {length}.")
    else:
        print(f"No need to fetch quotes for {source.symbol}. There are {length} quotes in the database and it is >= the threshold level of {test_threshold}.")

    #################################
    # Train the model and get results
    #################################

    prob = Probability(period_long=period_long,
                       period_short=period_short,
                       rows=est_rows,
                       data_to_learn=allrows,
                       true_ratio=true_ratio,
                       cycle_num=cycle_num,
                       algorithm=algorithm,
                       classify=True  # Needed for metrics only
                       )

    try:
        before = perf_counter()
        prob.learn()
        print(f"Total time for learning: {(perf_counter() - before) * 1000}ms")

        before = perf_counter()
        prob.calculate()
        print(f"Total time for estimaiton: {(perf_counter() - before) * 1000}ms")

        accuracy_buy_learn, _, _ = prob.get_learn_accuracy()
        f1_buy_learn, _, _ = prob.get_learn_f1()
        accuracy_buy_est, _, _ = prob.get_est_accuracy()
        f1_buy_est, _, _ = prob.get_est_f1()
    except ToolError as e:
        sys.exit(f"Can't perform calculation: {e}")

    print('\nBuy train accuracy:{: .2f}%'.format(accuracy_buy_learn * 100))
    print(f"Buy train f1 score: {round(f1_buy_learn, 4)}")

    print('\nBuy estimation accuracy:{: .2f}%'.format(accuracy_buy_est * 100))
    print(f"Buy estimation f1 score: {round(f1_buy_est, 4)}\n")

    #################
    # Build the chart
    #################

    df = prob.get_results()
    df['quote'] = [row[StockQuotes.AdjClose] for row in est_rows][period_long-1:]
    df['volume'] = [row[StockQuotes.Volume] for row in est_rows][period_long-1:]

    # Create figure

    fig = make_subplots(rows=4, cols=1, shared_xaxes=True, row_width=[0.2, 0.2, 0.2, 0.4],
                        specs=[[{"secondary_y": False}],
                            [{"secondary_y": False}],
                            [{"secondary_y": False}],
                            [{"secondary_y": False}]])

    fig.add_trace(
        go.Scatter(x=df['dt'], y=df['quote'], name="AdjClose"),
        secondary_y=False,
    )

    fig.add_trace(
        go.Scatter(x=df['dt'], y=df['ma-long'], name="Long MA"),
        secondary_y=False,
    )

    fig.add_trace(
        go.Scatter(x=df['dt'], y=df['ma-short'], name="Short MA"),
        secondary_y=False,
    )

    # Add probabilities chart
    fig.add_trace(go.Scatter(x=df['dt'], y=df['buy-prob'], fill='tozeroy', name="Growth Probability"), row=2, col=1)

    # Add percentage volume oscillator chart
    fig.add_trace(go.Scatter(x=df['dt'], y=df['pvo'], fill='tozeroy', name="PVO"), row=3, col=1)

    # Add volume chart
    fig.add_trace(go.Scatter(x=df['dt'], y=df['volume'], fill='tozeroy', name="Volume"), row=4, col=1)

    ######################
    # Write the chart
    ######################

    update_layout(fig, f"Probabilities example chart for {source.symbol}", length)

    new_file = show_image(fig)

    print(f"{new_file} is written.")
