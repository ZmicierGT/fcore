"""Minimalistic implementation and demonstration of growth probability tool.

The value returned by pobability tool indicates the chance that the security will grow in the next cycles.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from tools.classifier import Classifier

from data.fvalues import Quotes, Algorithm
from data.yf import YF
from data.futils import update_layout, show_image

import pandas as pd
import pandas_ta as ta

import plotly.graph_objects as go
from plotly.subplots import make_subplots

class Probability(Classifier):
    """Minimalistic growth probability impementation."""
    def __init__(self, period_long=30, period_short=15, **kwargs):
        """Initializes minimalistic grows probability instance."""
        super().__init__(**kwargs)

        self._period_long = period_long  # Long SMA period for growth estimation.
        self._period_short = period_short  # Short SMA period for growth estimation.

        self._probability = True  # Calculate probabilities in the tool
        self._classify = False  # No need to perform classification
        self._use_sell = False  # Sell signals are not used by this tool

    def prepare(self, rows=None):
        """Prepare the DataFrame for learning/estimation."""
        df = pd.DataFrame(self._rows) if rows is None else pd.DataFrame(rows)  # Create the dataframe base on provided data

        # Calculate required technical indicators
        ma_long = ta.sma(df[Quotes.AdjClose], length = self._period_long)  # Long SMA
        ma_short = ta.sma(df[Quotes.AdjClose], length = self._period_short)  # Short SMA
        pvo = ta.pvo(df[Quotes.Volume])  # Percentage volume oscillator

        # Prepare data for learning/estimation
        df['pvo'] = pvo.iloc[:, 0]
        df['ma-long'] = ma_long
        df['ma-short'] = ma_short
        df['quote'] = df[Quotes.AdjClose]
        df['ma-diff'] = ((ma_long - ma_short) / ma_long)  # Ratio of difference between long and short SMAs
        df['hilo-diff'] = ((df[Quotes.High] - df[Quotes.Low]) / df[Quotes.High])  # Ratio of difference between High and Low

        self._data_to_est = ['pvo', 'ma-diff', 'hilo-diff']  # Columns to learn/estimate
        self._data_to_report = self._data_to_est + ['ma-long', 'ma-short', 'quote']  # Columns for reporting

        # Get rid of the values where MA is not calculated because they are useless for learning.
        return df[self._period_long-1:].reset_index().drop(['index'], axis=1)

    def get_buy_condition(self, df):
        """Get buy condiiton to check signals."""
        curr_quote = df[Quotes.AdjClose]
        next_quote = df[Quotes.AdjClose].shift(-abs(self._cycle_num))

        return (next_quote - curr_quote) / curr_quote >= self._true_ratio

###############
# Demonstration
###############

threshold_learn, threshold_test = 5284, 565  # Quotes num thresholds for the learning and testing
period_long, period_short = (50, 25)  # Periods for SMAs

# Get data for training/testing a model with the number of quotes >= threshold
# All the data will be cached in a database without the need of further fetching
rows_learn, length_learn = YF(symbol='SPY', first_date="2000-1-1", last_date="2021-1-1").fetch_if_none(threshold_learn)
rows_test, length_test = YF(symbol='SPY', first_date="2021-1-2", last_date="2023-4-1").fetch_if_none(threshold_test)

prob = Probability(period_long=period_long,
                   period_short=period_short,
                   rows=rows_test,
                   data_to_learn=[rows_learn],
                   true_ratio=0.004,  # Ratio when signal is considered as true in cycle_num.
                                      # For example, if true_ratio is 0.03 and cycle_num is 5,
                                      # then the signal will be considered as true if there was a 3% change in
                                      # quote in the following 5 cycles after getting the signal.
                   cycle_num=2,  # Nuber of cycles to reach true_ratio to consider the signal as true.
                   algorithm=Algorithm.KNC)

prob.learn()
prob.calculate()
df = prob.get_results()

# Buind the report
fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_width=[0.3, 0.7],
                    specs=[[{"secondary_y": False}],
                            [{"secondary_y": False}]])

fig.add_trace(go.Scatter(x=df['dt'], y=df['quote'], name="AdjClose"), secondary_y=False)
fig.add_trace(go.Scatter(x=df['dt'], y=df['ma-long'], name="Long MA"), secondary_y=False)
fig.add_trace(go.Scatter(x=df['dt'], y=df['ma-short'], name="Short MA"), secondary_y=False)

# Add probabilities chart
fig.add_trace(go.Scatter(x=df['dt'], y=df['buy-prob'], fill='tozeroy', name="Growth Probability"), row=2, col=1)

update_layout(fig, "Probabilities Example Chart", len(rows_test))
show_image(fig)
