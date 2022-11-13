"""Demonstration of learning a model for MA/price cross strategy combined with AI estimation of false signals.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

from data.yf import YFQuery, YFError, YF
from data.fdata import FdataError

from data.fvalues import Rows

from data.futils import check_date

from indicators.ma_classifier import MAClassifier
from indicators.ma_classifier import Algorithm 

from indicators.base import IndicatorError

# Parameters for learning
true_ratio = 0.01  # Ratio of ma/quote change to consider it as a true signal. It should be achieved withing cycles_num to be considered as true.
cycle_num = 2  # Number of cycles to wait for the true_ratio value. If true_ratio is not reached withing these cycles, the signal is considered as false.
algorithm = Algorithm.KNC  # The default algorithm to use
period = 50  # Period for MA calculation

def_threshold = 15314  # The default quotes num required for the calculation for each symbol

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
           ['WMT', 12655],
           ['DIA', 6238]]

if __name__ == "__main__":
    # Array for the fetched data for all symbols
    allrows = []

    print("Fetchig the required quotes for model calculation. Press CTRL-C and restart if it stucks.")

    for symbol, threshold in symbols:
        query = YFQuery()
        query.symbol = symbol
        query.last_date = check_date("2022-11-1")[1]

        data = YF(query)

        try:
            query.db_connect()

            current_num = data.get_symbol_quotes_num()

            # Fetch quotes if there are less than a threshold number of records in the database for a day (default) timespan
            if current_num < threshold:
                num_before, num_after = data.check_and_fetch()
                print(f"Fetched {num_after-num_before} quotes for {symbol}.")
            else:
                print(f"No need to fetch quotes for {symbol}. There are {current_num} quotes in the database and it is beyond the threshold level of {threshold}.")

            rows = data.get_quotes()
            query.db_close()
        except (YFError, FdataError) as e:
            print(e)
            sys.exit(2)

        allrows.append(rows)
    
    # Excude DIA from learning
    dia = allrows.pop()

    print(len(allrows))

    # Train the model

    ma_cls = MAClassifier(period,
                          dia,
                          Rows.AdjClose,
                          data_to_learn=allrows,
                          true_ratio=true_ratio,
                          cycle_num=cycle_num,
                          algorithm=algorithm)

    try:
        ma_cls.calculate()
        accuracy_buy_learn, accuracy_sell_learn, total_accuracy_learn = ma_cls.get_learn_accuracy()
        accuracy_buy_est, accuracy_sell_est, total_accuracy_est = ma_cls.check_est_precision()
    except IndicatorError as e:
        print(f"Can't calculate MA Classifier: {e}")
        sys.exit(2)

    print('Buy train Accuracy:{: .2f}%'.format(accuracy_buy_learn * 100))
    print('Sell train Accuracy:{: .2f}%'.format(accuracy_sell_learn * 100))
    print('Total train Accuracy:{: .2f}%'.format(total_accuracy_learn * 100))

    print('\nBuy estimation Accuracy:{: .2f}%'.format(accuracy_buy_est * 100))
    print('Sell estimation Accuracy:{: .2f}%'.format(accuracy_sell_est * 100))
    print('Total estimation Accuracy:{: .2f}%'.format(total_accuracy_est * 100))