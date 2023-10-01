"""Demonstration of learning a model for MA/price cross strategy combined with AI estimation of false signals.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""

from data.yf import YF
from data.fdata import FdataError

from data.fvalues import StockQuotes

from tools.ma_classifier import MAClassifier
from data.fvalues import Algorithm

from tools.base import ToolError

from data.futils import update_layout
from data.futils import show_image

import plotly.graph_objects as go
from plotly.subplots import make_subplots

import sys

# Parameters for learning
true_ratio = 0.004  # Ratio of ma/quote change to consider it as a true signal. It should be achieved withing cycles_num to be considered as true.
cycle_num = 2  # Number of cycles to wait for the true_ratio value. If true_ratio is not reached withing these cycles, the signal is considered as false.
algorithm = Algorithm.KNC  # The default algorithm to use
period = 50  # Period for MA calculation
symbol = 'SPY'  # Symbol to make estimations

first_date = "2020-11-1"  # First date to fetch quotes (for testing only)
last_date = "2022-11-1"  # The last date to fetch quotes
def_threshold = 15314  # The default quotes num required for the calculation for each symbol
test_threshold = 500  # Minimum threshold value for testing

# For learning we may use the previous quotes of the same stock or use quotes of other stocks if the used indicators are percent/ratio based.
# In this case, DJIA stocks are used to train the models.

# DJIA composition [symbol, quotes_threshold]. More quotes will be fetched if the threshold is not met.
symbols = [['MMM', def_threshold, 245, 4],  # TODO HIGH Determine why open price is sometimes 0
           ['AXP', 12715, 187, 6],
           ['AMGN', 9926, 49, 5],
           ['AAPL', 10058, 80, 5],
           ['BA', def_threshold, 228, 8],
           ['CAT', def_threshold, 198, 5],
           ['CVX', def_threshold, 217, 5],
           ['CSCO', 8240, 50, 9],
           ['KO', def_threshold, 245, 8],
           ['DIS', def_threshold, 124, 8],
           ['DOW', 913, 18, 0],
           ['GS', 5914, 98, 0],
           ['HD', 10366, 145, 13],
           ['HON', def_threshold, 246, 9],
           ['IBM', def_threshold, 245, 8],
           ['INTC', 10749, 124, 8],
           ['JNJ', def_threshold, 247, 7],
           ['JPM', 10749, 159, 4],
           ['MCD', 14179, 167, 9],
           ['MRK', def_threshold, 243, 7],
           ['MSFT', 9235, 79, 9],
           ['NKE', 10569, 145, 6],
           ['PG', def_threshold, 248, 6],
           ['CRM', 4623, 0, 1],
           ['TRV', 11842, 146, 2],
           ['UNH', 9588, 75, 5],
           ['VZ', 9817, 157, 6],
           ['V', 3682, 61, 1],
           ['WBA', 10749, 153, 7],
           ['WMT', 12655, 197, 9]]

if __name__ == "__main__":
    # Array for the fetched data for all symbols
    allrows = []

    print("Fetchig the required quotes for model training. Press CTRL-C and restart if it stucks.")

    for symbol_learn, threshold, divs_threshold, splits_threshold in symbols:
        try:
            # Fetch quotes if there are less than a threshold number of records in the database for a day (default) timespan
            source = YF(symbol=symbol_learn, last_date=last_date)
            rows, num = source.fetch_stock_data_if_none(threshold, divs_threshold, splits_threshold)
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

    ma_cls = MAClassifier(period=period,
                          rows=est_rows,
                          data_to_learn=allrows,
                          true_ratio=true_ratio,
                          cycle_num=cycle_num,
                          algorithm=algorithm)

    try:
        ma_cls.calculate()
        accuracy_buy_learn, accuracy_sell_learn, total_accuracy_learn = ma_cls.get_learn_accuracy()
        f1_buy_learn, f1_sell_learn, total_f1_learn = ma_cls.get_learn_f1()
        accuracy_buy_est, accuracy_sell_est, total_accuracy_est = ma_cls.get_est_accuracy()
        f1_buy_est, f1_sell_est, total_f1_est = ma_cls.get_est_f1()
    except ToolError as e:
        sys.exit(f"Can't calculate MA Classifier: {e}")

    print('\nBuy train accuracy:{: .2f}%'.format(accuracy_buy_learn * 100))
    print('Sell train accuracy:{: .2f}%'.format(accuracy_sell_learn * 100))
    print('Total train accuracy:{: .2f}%'.format(total_accuracy_learn * 100))

    print(f"\nBuy train f1 score: {round(f1_buy_learn, 4)}")
    print(f"Sell train f1 score: {round(f1_sell_learn, 4)}")
    print(f"Total train f1 score: {round(total_f1_learn, 4)}")

    print('\nBuy estimation accuracy:{: .2f}%'.format(accuracy_buy_est * 100))
    print('Sell estimation accuracy:{: .2f}%'.format(accuracy_sell_est * 100))
    print('Total estimation accuracy:{: .2f}%'.format(total_accuracy_est * 100))

    print(f"\nBuy estimation f1 score: {round(f1_buy_est, 4)}")
    print(f"Sell estimation f1 score: {round(f1_sell_est, 4)}")
    print(f"Total estimation f1 score: {round(total_f1_est, 4)}")

    print(f"\nThe actual/estimated signals:\n{ma_cls.get_df_signals_to_compare().to_string()}\n")

    #################
    # Build the chart
    #################

    df = ma_cls.get_results()
    df['quote'] = [row[StockQuotes.AdjClose] for row in est_rows][period-1:]
    df['volume'] = [row[StockQuotes.Volume] for row in est_rows][period-1:]

    buy_quotes = df.loc[df['buy-signal'] == 1]
    sell_quotes = df.loc[df['sell-signal'] == 1]

    # Create figure

    fig = make_subplots(rows=3, cols=1, shared_xaxes=True, row_width=[0.2, 0.2, 0.6],
                        specs=[[{"secondary_y": False}],
                            [{"secondary_y": False}],
                            [{"secondary_y": False}]])

    fig.add_trace(
        go.Scatter(x=df['dt'], y=df['quote'], name="AdjClose"),
        secondary_y=False,
    )

    fig.add_trace(
        go.Scatter(x=df['dt'], y=df['ma'], name="MA"),
        secondary_y=False,
    )

    # Add buy/sell signals to the chart

    fig.add_trace(go.Scatter(x=buy_quotes['dt'],
                             y=buy_quotes['quote'],
                             mode='markers',
                             name='Buy Signals',
                             marker=dict(size=12, symbol="arrow-up", color='green', line_color="midnightblue", line_width=2)),
                  row=1, col=1)

    fig.add_trace(go.Scatter(x=sell_quotes['dt'],
                             y=sell_quotes['quote'],
                             mode='markers',
                             name='Sell Signals',
                             marker=dict(size=12, symbol="arrow-down", color='red', line_color="midnightblue", line_width=2)),
                  row=1, col=1)

    # Add percentage volume oscillator chart
    fig.add_trace(go.Scatter(x=df['dt'], y=df['pvo'], fill='tozeroy', name="PVO"), row=2, col=1)

    # Add volume chart
    fig.add_trace(go.Scatter(x=df['dt'], y=df['volume'], fill='tozeroy', name="Volume"), row=3, col=1)

    ######################
    # Write the chart
    ######################

    update_layout(fig, f"MA Classifier example chart for {source.symbol}", length)

    new_file = show_image(fig)

    print(f"{new_file} is written.")
