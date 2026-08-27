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
true_ratio = 0.01  # Ratio of ma/quote change to consider it as a true signal. It should be achieved withing cycles_num to be considered as true.
cycle_num = 2  # Number of cycles to wait for the true_ratio value. If true_ratio is not reached withing these cycles, the signal is considered as false.
algorithm = Algorithm.KNC  # The default algorithm to use
period_long = 50  # Long period for MA calculation
period_short = 25  # Short period for MA calculation

first_date = "2020-11-1"  # First date to fetch quotes for learning
last_date = "2022-11-1"  # The last date to fetch quotes for learning

if __name__ == "__main__":
    warning = "WARNING! This screener is just an example and do not treat the obtained signals as an investment advice.\n" +\
                "Always keep yfinance up to date ( pip install yfinance --upgrade ) and use quotes obtained from this " +\
                "datasource only for demonstation purposes!\n"
    print(warning)

    # Array for the fetched data for all symbols
    allrows = []

    print("Fetchig the required quotes for model training. Press CTRL-C and restart if it stucks.")

    for symbol_learn in ['AAPL', 'MSFT']:
        try:
            rows = YF(symbol=symbol_learn,
                      first_date=first_date,
                      last_date=last_date).get()
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

    source_aapl = YF(symbol='AAPL')
    source_msft = YF(symbol='MSFT')

    aapl = {'Title': 'AAPL', 'Source': source_aapl}
    msft = {'Title': 'MSFT', 'Source': source_msft}

    scr = ClsScr(symbols=[aapl, msft],
                 period=period_long,
                 period_short=period_short,
                 true_ratio=true_ratio,
                 cycle_num=cycle_num,
                 algorithm=algorithm,
                 model_buy=model_buy,
                 model_sell=model_sell,
                 timespan=Timespans.Day)

    print("\nPlease note that the data is delayed (especially volume) and exceptions due to network errors may happen.\n")

    results = scr.screen()

    print("--------------------------------------------------------------")

    for result in results:
        print(f"Symbol:           {result[ScrResult.Title]}")
        print(f"Latest update:    {result[ScrResult.LastDatetime]}")
        print(f"Cached quotes:    {result[ScrResult.QuotesNum]}")
        print(f"Buy weight:       {result[ScrResult.Values][0]}")
        print(f"Sell weight:      {result[ScrResult.Values][1]}")
        print(f"Signal to buy:    {result[ScrResult.Signals][0]}")
        print(f"Signal to sell:   {result[ScrResult.Signals][1]}\n")
