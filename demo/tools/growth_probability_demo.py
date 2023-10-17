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
threshold_divs_test = 8
threshold_splits_test = 0

# For learning we may use the previous quotes of the same stock or use quotes of other stocks if the used indicators are percent/ratio based.
# In this case, DJIA stocks are used to train the models.

# DJIA composition [symbol, quotes_threshold]. More quotes will be fetched if the threshold is not met.
symbols = [['MMM', 245, 4],
           ['AXP', 187, 6],
           ['AMGN', 49, 5],
           ['AAPL', 80, 5],
           ['BA', 228, 8],
           ['CAT', 198, 5],
           ['CVX', 217, 5],
           ['CSCO', 50, 9],
           ['KO', 245, 8],
           ['DIS', 124, 8],
           ['DOW', 18, 0],
           ['GS', 98, 0],
           ['HD', 145, 13],
           ['HON', 246, 9],
           ['IBM', 245, 8],
           ['INTC', 124, 8],
           ['JNJ', 247, 7],
           ['JPM', 159, 4],
           ['MCD', 167, 9],
           ['MRK', 243, 7],
           ['MSFT', 79, 9],
           ['NKE', 145, 6],
           ['PG', 248, 6],
           ['CRM', 0, 1],
           ['TRV', 146, 2],
           ['UNH', 75, 5],
           ['VZ', 157, 6],
           ['V', 61, 1],
           ['WBA', 153, 7],
           ['WMT', 197, 9]]

if __name__ == "__main__":
    warning = "WARNING! Using yfinance data for the demonstration.\n" +\
                "Always keep yfinance up to date ( pip install yfinance --upgrade ) and use quotes obtained from this " +\
                "datasource only for demonstation purposes!\n"
    print(warning)

    # Array for the fetched data for all symbols
    allrows = []

    print("Fetchig the required quotes for model training. Press CTRL-C and restart if it stucks.")

    for symbol_learn, divs_threshold, splits_threshold in symbols:
        try:
            print(f"Checking if quotes for {symbol_learn} is already fetched...")

            source = YF(symbol=symbol_learn, last_date=last_date)
            rows = source.fetch_stock_data_if_none(divs_threshold, splits_threshold)
        except FdataError as e:
            sys.exit(e)

        print(f"The total number of quotes used for {source.symbol} is {len(rows)}.\n")

        allrows.append(rows)

    # Get quotes for estimations
    try:
        print(f"\nFetching quotes for {symbol} to validate the model...")

        source = YF(symbol=symbol, first_date=first_date, last_date=last_date, verbosity=True)
        est_rows = source.fetch_stock_data_if_none(threshold_divs_test, threshold_splits_test)
    except FdataError as e:
        sys.exit(e)

    length = len(est_rows)

    print(f"The total number of quotes used for {source.symbol} is {length}.\n")

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
    df['quote'] = est_rows[StockQuotes.AdjClose][period_long-1:]
    df['volume'] = est_rows[StockQuotes.Volume][period_long-1:]

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
