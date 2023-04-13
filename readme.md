# Fcore is an AI framework for financial markets backtesting and screening.

### **Fcore** is based on the following principles:
- AI is becoming the essential (and hardly avoidable) part of a financial markets analysis.
- For successful AI-appliances, we need a lot of data and the data should be well structurized and quickly accessible.
- Analysing financial markets has a lot of common features. That is why a data analyzing API should be used to reduce a routine work.
- All data processing techniques should be treated equally and follow the same interface.
- Backtesting is treated as an another AI metric and it should be very sensitive to a 'minor' details.

### Based on these principles (and not only), **Fcore** is capable of:
- Obtaining data from various sources (AlphaVantage, Polygon, Yahoo Finance, Finnhub) and storing it in an unified way without any need to worry about parsing data, altering the results and so on. Currently around 100 data parameters are supported. If you need an additional source, API allows to quickly write an extension to obtain and parse the required data.
- Providing an API to ease the development of AI-strategies for financial markets analysis. The main focus is on these approaches: classifying strategy signals or security growth/decline forecast as True/False with optional probabilities calculation (already implemented) and estimation of the future prices as a regression problem (partially implemented as LSTM-demo).
- Utilizing the power of the 'classical' technical and fundamental analyses combined with the modern AI-approach. As there is no difference in data processing techniques, data provided by AI may treated as a custom 'AI-indicator'.
- Providing its own backtesting engine which takes into account a lot of expenses related to an actual trade/investment. It handles various margin-specific fees and inflation as well. Obtained results must be very close to actual results which you could get on a real market using the analysed strategy.
- Using multiple financial securities in one backtesting or real time screening strategy (sure, single instument strategies are available as well).
- Providing API for reporting.

These features allow you to develop your AI-based market strategies much faster than when using a 'bare'-approach.

Please note that Fcore is a tool which helps you to easily implement and test your own financial strategies but it does not provide any 'out of the box' solutions. Consider all the provided strategies as programming examples which help you to implement your own strategies.

# Quick Start

Here is a basic example (within 100 LoC) of 200 day SMA strategy implementation when true/false signals are distinguished by AI.

```python
from data.fvalues import Algorithm
from tools.ma_classifier import MAClassifier

from backtest.ma_classification import MAClassification
from backtest.ma import MA
from backtest.stock import StockData
from backtest.reporting import Report

from data.yf import YF

import plotly.graph_objects as go

threshold_learn = 5284  # Quotes num threshold for the learning
threshold_test = 565  # Quotes num threshold for the test

period = 50  # SMA period

min_width = 2500 # Minimum width for reporting
height = 250  # Height of each subchart in reporting

# Get data for training/testing a model with the number of quotes >= threshold
# All the data will be cached in a database without the need of further fetching
rows_learn, length_learn = YF(symbol='SPY', first_date="2000-1-1", last_date="2021-1-1").fetch_if_none(threshold_learn)
rows_test, length_test = YF(symbol='SPY', first_date="2021-1-2", last_date="2023-4-1").fetch_if_none(threshold_test)

# Train the model
classifier = MAClassifier(period=period,  # SMA Period
                          data_to_learn=[rows_learn],  # Raw quote data to train the model
                          true_ratio=0.004,  # Ratio when signal is considered as true in cycle_num.
                                             # For example, if  true_ratio is 0.03 and cycle_num is 5,
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
new_file = report.show_image()
```

This strategy ([backtest/ma_classification.py](backtest/ma_classification.py)) trains a model for MA-Classifier ([tools/ma_classifier.py](tools/ma_classifier.py)) AI tool which uses percentage volume oscillator data to distinguish true/false signals. Please note that in real appliances hundreds of data parameters may be used to train a model. Here we see that AI helped to better distinguish some signals of the strategy and decrease the loses from -23.53% to -12%. To run this strategy with additional metrics output invoke **python -m demo.backtest.ma_classification_test** (source in [demo/backtest/ma_classification_test.py](demo/backtest/ma_classification_test.py)) Here is the basic quick start example - [quick_start.py](quick_start.py)

It is the report generated by the script above:
![Backtesting Report](report.png "Backtesting Report")

You may fetch/obtain another data (like fundamentals) in the following way:
```python
from data import av
from data.fvalues import Timespans

avi = av.AVStock(symbol='IBM', timespan=Timespans.Day)
avi.compact = False

quotes = avi.fetch_quotes()
earnings = avi.fetch_earnings()
cf = avi.fetch_cash_flow()
bs = avi.fetch_balance_sheet()
ins = avi.fetch_income_statement()

avi.db_connect()

before, after = avi.add_quotes(quotes)
avi.add_earnings(earnings)
avi.add_cash_flow(cf)
avi.add_balance_sheet(bs)
avi.add_income_statement(ins)

rows = avi.get_quotes(queries=[('earnings', 'reported_date'), ('earnings', 'reported_eps'), ('cash_flow', 'operating_cashflow')])
avi.db_close()

print(dict(rows[-1]))

print(f"Number of quotes in DB before the operation {before}, after {after}")
```

# Other Examples

The examples above is only a little part of what Fcore is capable. The following examples illustrates the wider usage of the framework.

Use the following command line tools to manage quotes.

- [quotes.py](quotes.py) - general quotes manager
- [yf_cli.py](yf_cli.py) (CLI tool which uses *Yahoo Finance* wrapper in [data/yf.py](data/yf.py))
- [polygon_cli.py](polygon_cli.py) (CLI tool which uses *Polygon.IO* API wrapper in [data/polygon.py](data/yf.py))

Non-CLI API wrappers:
- [data/av.py](data/av.py) (API wrapper for *Alpha Vantage* stock data).
- [data/fh.py](data/fh.py) (API wrapper for *Finnhub* real time quote data).

Examples of custom data processing tools which are relied on AI:
- [tools/lstm.py](tools/lstm.py) - LSTM AI implementation for financial analysis (**python -m demo.ai.lstm_learn** for learning demonstation and **python -m demo.tools.lstm_demo** for price estimation demonstation)
- [tools/ma_classifier.py](tools/ma_classifier.py) 'AI-indicator' where MA/price crossover signals are determined by AI if they are true/false. (**python -m demo.tools.ma_classifier_demo** for demonstration)
- [tools/probability.py](tools/probability.py) AI trend estimator based on probabilistic classification. (**python -m demo.tools.growth_probability_demo** for demonstration)

Examples of a screening strategy:
- [screener/rsi_scr.py](screener/rsi_scr.py) - RSI strategy screener (**python -m demo.screening.rsi_scr_demo** for a demonstation)

Examples of backtesting strategies:
- [backtest/bh.py](backtest/bh.py) - Simple backtesting strategy with periodic investments adjusted to inflation (**python -m demo.backtest.bh_test**)
- [backtest/ma.py](backtest/ma.py) - MA crossover strategy implementation (**python -m demo.backtest.ma_test**)
- [backtest/rsi.py](backtest/rsi.py) - RSI stragegy multi-security demo. See **python -m demo.backtest.rsi_test** for an EOD test and **python demo.backtest.rsi_intraday_test** for an intraday demonstation.
- [backtest/ma_classification.py](backtest/ma_classification.py) - MA/price crossower strategy where true/false signals are determined by AI. (**python -m demo.backtest.ma_classification_test**)

Note that the tools and backtesting demos create an image with the result of a calculation located in *images* folder. AI learn demonstrations create a model subfolder in *models* folder.

# Additional Details

To keep everything working, please keep all the dependencies up to date. Especially the dependencies which are related to data sources (like yfinance).

Currently the project is in the active development stage and is not promoted anywhere yet. However, if you found it and feel interested, sure you are welcome to observe the development process or contribute to the project. The 'general idea' of the project will remain the same but APIs still may change.

The repository uses two branches: 'main' and 'devel'. The 'main' branch is usually stable but as a verification process is not fully established yet, sometimes bugs may be committed there. The 'devel' branch is used for 'intermediate' development commits and it is not intended to be stable or even working. See TODO's (followed by priority) in the code for what is going to be implemented/fixed in the future.

Overall, the next development steps are (planned to finish in Autumn 2023):
- ~~Continue to adopt the project to use various data sources and store the fundamental data in a unified way. Data sources are Yahoo Finance (for demonstration purposes only), Polygon.IO, AlphaVantahe and Finnhub.io~~ (already implemented).
- Continue to implement the API for AI-based analysis of financial markets. Exact description should be written how to use this API.
- Further improvements in performance and reliability. Scripts for pre-commit validation to the 'main' branch should be written.

All fetched quotes are cached in a database (sqlite by default). Data-related settings (like api-keys) are stored in [settings.py](settings.py) file.

Use *python -m unittest discover -s test -p '*_test.py'* to run unit tests (data components are tested now).

**Fcore** is adopted to [nogil-3.9.10](https://github.com/colesbury/nogil) python interpreter. Use it with **nogil** to benefit from parallel computing.

Fcore is distributes on an 'AS IS' basis using a custom source available [License](license.md). The author is not responsible for any losses caused by using the project.

In case of any questions, please feel free to contact me by email - zmiciergt at icloud dot com
