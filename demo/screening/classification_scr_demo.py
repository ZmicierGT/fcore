"""Demonstration of a classification AI screener.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from screener.classification_scr import ClsScr
from screener.base import ScrResult

from data.fvalues import Timespans

from data.yf import YF

from data.fdata import FdataError

from tools.growth_probability import Probability
from data.fvalues import Algorithm

from tools.base import ToolError

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
            source = YF(symbol=symbol_learn, last_date=last_date)
            rows = source.fetch_stock_data_if_none(divs_threshold, splits_threshold)
        except FdataError as e:
            sys.exit(e)

        print(f"The total number of quotes used for {source.symbol} is {len(rows)}.\n")

        allrows.append(rows)

    # Train the models
    base_prob = Probability(period_long=period_long,
                            period_short=period_short,
                            rows=None,
                            data_to_learn=allrows,
                            true_ratio=true_ratio,
                            cycle_num=cycle_num,
                            algorithm=algorithm,
                            use_sell=True,
                            classify=True)

    try:
        base_prob.learn()

        model_buy = base_prob.get_buy_model()
        model_sell = base_prob.get_sell_model()

        accuracy_buy_learn, accuracy_sell_learn, _ = base_prob.get_learn_accuracy()
        f1_buy_learn, f1_sell_learn, _ = base_prob.get_learn_f1()
    except ToolError as e:
        sys.exit(f"Can't perform calculation: {e}")

    print('\nBuy train accuracy:{: .2f}%'.format(accuracy_buy_learn * 100))
    print(f"Buy train f1 score: {round(f1_buy_learn, 4)}")

    print('\nSell train accuracy:{: .2f}%'.format(accuracy_sell_learn * 100))
    print(f"Sell train f1 score: {round(f1_sell_learn, 4)}")

    # Perform screening

    source_btc = YF()
    source_ltc = YF()

    # Despite having a model trained using stock quotes, lets use crypto to make estimations as crypto quotes change 24/7
    btc = {'Title': 'BTC-USD', 'Source': source_btc}
    ltc = {'Title': 'LTC-USD', 'Source': source_ltc}

    interval = 60

    scr = ClsScr(symbols=[btc, ltc],
                 period=period_long,
                 period_short=period_short,
                 true_ratio=true_ratio,
                 cycle_num=cycle_num,
                 algorithm=algorithm,
                 model_buy=model_buy,
                 model_sell=model_sell,
                 interval=interval,
                 timespan=Timespans.Minute)

    print("\nPlease note that the data is delayed (especially volume) and exceptions due to network errors may happen.\n")
    print(f"Press CTRL+C to cancel screening. The interval is {interval} seconds.")

    while True:
        scr.do_cycle()

        results = scr.get_results()

        print("--------------------------------------------------------------")

        for i in range(2):
            print(f"Symbol: {results[i][ScrResult.Title]}")
            print(f"Latest update:    {results[i][ScrResult.LastDatetime]}")
            print(f"Cached quotes:    {results[i][ScrResult.QuotesNum]}")
            print(f"Buy weight:       {results[i][ScrResult.Values][0]}")
            print(f"Sell weight:      {results[i][ScrResult.Values][1]}")
            print(f"Signal to buy:    {results[i][ScrResult.Signals][0]}")
            print(f"Signal to sell:   {results[i][ScrResult.Signals][1]}\n")
