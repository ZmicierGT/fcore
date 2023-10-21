"""Demonstration of a classification AI screener.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from screener.classification_scr import ClsScr
from screener.base import ScrResult

from data.fvalues import Timespans, djia

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

if __name__ == "__main__":
    warning = "WARNING! This screener is just an example and do not treat the obtained signals as an investment advice.\n" +\
                "Always keep yfinance up to date ( pip install yfinance --upgrade ) and use quotes obtained from this " +\
                "datasource only for demonstation purposes!\n"
    print(warning)

    # Array for the fetched data for all symbols
    allrows = []

    print("Fetchig the required quotes for model training. Press CTRL-C and restart if it stucks.")

    for symbol_learn in djia:
        try:
            rows = YF(symbol=symbol_learn, last_date=last_date).get()
        except FdataError as e:
            sys.exit(e)

        print(f"The total number of quotes used for {symbol_learn} is {len(rows)}.\n")

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
