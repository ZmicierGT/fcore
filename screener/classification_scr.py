"""Classification AI screener implementation.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from screener.base import BaseScr

from tools.growth_probability import Probability

class ClsScr(BaseScr):
    """
        Classification AI screener implementation class.
    """
    def __init__(self,
                 period_short,
                 true_ratio,
                 cycle_num,
                 algorithm,
                 model_buy=None,
                 model_sell=None,
                 data_to_learn=None,
                 is_simple=True,
                 **kwargs):
        """
            Initialize the instance of classification AI screener.

            Args:
                period(int): MA period to compare with a short period
                period_short: MA period to compare with a long period
                true_ratio(float): ratio when signal is considered as true in cycle_num. For example, if true_ratio is 0.03 and cycle_num is 5,
                                then the signal will be considered as true if there was a 3% change in quote in the following 5 cycles
                                after getting the signal.
                cycle_num(int): number of cycles to reach to true_ratio to consider that the signal is true.
                algorithm(Algorithm): algorithm used for learning (from Algorithm enum).
                model_buy(): model to estimate buy signals.
                model_sell(): model to estimate sell signals.
                data_to_learn([array]) data to train the models. Either models or data to learn need to be specified.
                is_simple(bool): indicates if SMAs or EMAs are used.
        """
        super().__init__(**kwargs)

        self._period_short = period_short
        self._true_ratio = true_ratio
        self._cycle_num = cycle_num
        self._algorithm = algorithm
        self._model_buy = model_buy
        self._model_sell = model_sell
        self._data_to_learn = data_to_learn
        self._is_simple = is_simple

    def calculate(self):
        """
            Perform calculation for classification AI demo.

            Raises:
                ScrError: not enough data to make a calculation.
        """
        self._results = [] 

        for symbol in self.get_symbols():
            rows = symbol.get_data(self.get_period(), self.get_init_status())

            if self.get_init_status() is False:
                # Need to initialize classification instances for each symbol
                symbol.prob = Probability(period_long=self.get_period(),
                                          period_short=self._period_short,
                                          rows=rows,
                                          data_to_learn=self._data_to_learn,
                                          model_buy=self._model_buy,
                                          model_sell=self._model_sell,
                                          true_ratio=self._true_ratio,
                                          cycle_num=self._cycle_num,
                                          algorithm=self._algorithm,
                                          use_sell=True)
            else:
                symbol.prob.set_data(rows)

            signal_buy = False
            signal_sell = False

            # Perform a classification
            symbol.prob.calculate()
            df = symbol.prob.get_results()

            buy_prob = df['buy-prob'].iloc[-1]
            sell_prob = df['sell-prob'].iloc[-1]

            if buy_prob >= 0.8 and sell_prob <= 0.2:
                signal_buy = True

            if sell_prob >= 0.8 and buy_prob <= 0.2:
                signal_sell = True

            result = [symbol.get_title(),
                      symbol.get_max_datetime(),
                      symbol.get_quotes_num(),
                      [buy_prob, sell_prob],
                      [signal_buy, signal_sell]]

            self._results.append(result)
