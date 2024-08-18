# Fcore Is an AI Framework for Financial Markets Analysis.

# WARNING!!! Dependency pandas_ta does not work with numpy 2.0 If you use numpy 2.0, use this [workaround](https://github.com/twopirllc/pandas-ta/issues/799)!!!

### With the help of it, you can easily perform the following actions:

- Obtain data from various sources (AlphaVantage, Polygon, Yahoo Finance, Finnhub) and store it in an unified way.
- Use an API to ease the development of AI-strategies for financial markets analysis.
- Utilize the power of the 'classical' technical and fundamental analyses combined with the modern AI-approach.
- Use the own backtesting engine which takes into account a lot of issues related to an actual trade/investment and supports strategies involving multiple securities.
- Perform a real-time screening of the market using the screening API.
- Generate reports.

### Here is a simplified diagram of how Fcore is designed:

![Diagram](fc.png "Diagram")

# Quick Start

Here are some basic examples of how to use Fcore. Please note that all the provided examples are a kind of 'Hello World's' and are not intended to be a real market strategies.

**The latest version of yfinance library is required to run these examples. Always update yfinace to the latest version using 'pip install yfinance --upgrade'**

## Data Management

Fcore supports simultaneous usage of varios data sources. For example, you may obtain quotes using one data source and use another for fundamental data. The data will be cached in a database and also requests to sources which involve maximum number of queries per minute will be automatically delayed to avoid data source errors. Make sure to add your API keys to the [settings.py](settings.py) file at first.

```python
# Fetch quotes if needed. Otherwise just take them from a database.
yf.YF(symbol='IBM', first_date="2017-1-1", last_date="2018-1-1").get()  # Use one source for quotes

avi = av.AVStock(symbol='IBM')  # Use another source for fundamentals
avi.get_cash_flow()

# Get combined data (quotes + fundamentals) in one query
quotes = avi.get_quotes(queries=[av.AvSubquery('av_cash_flow', 'operating_cashflow', condition=report_year, title='annual_cashflow')])
```

Fcore uses labelled numpy arrays as the main data containers as they are memory efficient and fast. You can get the obtained columns in a such way: *quotes['annual_cashflow']*

Invoke **python -m quickstart.min_data_management** to run the full example.

## Tools

Fcore has a Tools API to ease a data processing routine work. A tools may just perform some basic calculations (like technical indicators). However, it may also be used for automating complex machine learning tasks including incremental learning and traing a model based on datasets which do not fit in a memory.

AI API is divided into two parts: The Classification API which allows to classify nearly every market event and The Regression API. As a basic example of using the Classification API you may classify if it is a good time to open a long/short position.

You need to inherit a *Classification* class and override at least two methods: *prepare* for data structures preparation and *get_buy_condition / get_sell_condition* for establishing signals. For example, here we are trying to estimate if the security will grow in the nearest N trading cycles (depending on a time span) based on the current fast and slow moving averages and volatility.

```python
class Probability(Classifier):
    def prepare(self, rows=None):
        """Data structures preparation"""
        # Create the dataframe based on the provided data
        df = pd.DataFrame(self._rows) if rows is None else pd.DataFrame(rows)

        # Calculate required technical indicators and other data
        ma_long = ta.sma(self._rows[StockQuotes.AdjClose], length=self._period_long)  # Long SMA
        ma_short = ta.sma(self._rows[StockQuotes.AdjClose], length=self._period_short)  # Short SMA
        pvo = ta.pvo(self._rows[StockQuotes.Volume])  # Percentage volume oscillator
        hilo = ((self._rows[StockQuotes.High] - self._rows[StockQuotes.Low]) / self._rows[StockQuotes.High])  # Hi/Lo difference ratio

        self._data_to_est = ['pvo', 'ma-diff', 'hilo-diff']  # Columns to learn/estimate
        self._data_to_report = self._data_to_est + ['ma-long', 'ma-short', 'quote']  # Columns for reporting

        # Get rid of the values where MA is not calculated
        return df[self._period_long-1:].reset_index().drop(['index'], axis=1)

    def get_buy_condition(self, df):
        """Get buy conditon to check signals."""
        curr_quote = df[StockQuotes.AdjClose]
        next_quote = df[StockQuotes.AdjClose].shift(-abs(self._cycle_num))

        return (next_quote - curr_quote) / curr_quote >= self._true_ratio
```

Using such basic tool we can train a model which will estimate if a security is most likely to grow in the following N trading cycles and give probabilities of the potential growth. The tool is used in this way:

```python
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
```

Invoke **python -m quickstart.min_growth_probability** to run the example.

It is the graphical representation of the data processing (current quote and growth probability in the particular moment):
![Growth Probability Report](probability.png "Growth Probability Report")

## Backtesting

Fcore provides an API for backtesting and reporting. Backtests are focused to be as close to the real market actions as possible with taking into account various commissions and fees and also using a multiple symbols in one strategy (up to covering the whole market).

Using Classification API you can easily classify nearly every event which happens on a market. For example, you can take a basic 'SMA vs Price' technical analysis strategy but then distinguished by AI if its signals are true or false. Then you can perform a backtest of such strategy and generate a report.

```python
# Get data for training/testing. All the data will be cached in a database without the need of further fetching
rows_learn = YF(symbol='SPY', first_date="2000-1-1", last_date="2021-1-1").get()
rows_test = YF(symbol='SPY', first_date="2021-1-2", last_date="2023-4-1").get()

# Create and train the model
classifier = MAClassifier(period=period,  # SMA Period
                          data_to_learn=[rows_learn],  # Raw quote data to train the model
                          true_ratio=0.004,  # Ratio when signal is considered as true in cycle_num.
                                             # For example, if true_ratio is 0.03 and cycle_num is 5,
                                             # then the signal will be considered as true if there was a 3% change in
                                             # quote in the following 5 cycles after getting the signal.
                          cycle_num=2,  # Nuber of cycles to reach true_ratio to consider the signal as true.
                          algorithm=Algorithm.LDA)  # Classification algorithm to use.

classifier.learn()

# Data instance for backtesting
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
    'margin_rec': 0.9,
    'margin_req': 1
}

# Perform backtest using AI classification of signals with the help of the model trained above
classification = MAClassification(**params, classifier=classifier)

classification.calculate()  # It starts the calculation in a separate thread which allows you to make a parralel computations
                            # if you use a Pyhon interpreter without GIL.
```

Invoke **python -m quickstart.min_ma_classification** to run the example.

It is the report generated by the script above:
![Backtesting Report](ma_classification.png "Backtesting Report")

Here we see that AI helped to better distinguish signals of the strategy and decreased the loses.

# Other Examples

The examples above are only a little part of what Fcore is capable. The following examples illustrates the wider usage of the framework.

Use the following tools to manage quotes and obtain data.

- *Yahoo Finance* wrapper - [data/yf.py](data/yf.py))
- *Polygon.IO* API wrapper - [data/polygon.py](data/polygon.py))
- API wrapper for *Alpha Vantage* stock data - [data/av.py](data/av.py)
- API wrapper for *Finnhub* real time data - [data/fh.py](data/fh.py)
- API wrapper for *Financial Modeling Prep* data - [data/fmp.py](data/fmp.py)

### Examples of custom data processing tools which are relied on AI
- [tools/regression.py](tools/regression.py) - Regression API implementation for financial analysis (**python -m demo.tools.regression_demo** for a demonstration using LSTM algorithm, [source of the demo](demo/tools/regression_demo.py) )
- [tools/ma_classifier.py](tools/ma_classifier.py) AI tool where MA/price crossover signals are determined by AI if they are true/false. (**python -m demo.tools.ma_classifier_demo** for demonstration, [source of the demo](demo/tools/ma_classifier_demo.py))
- [tools/growth_probability.py](tools/growth_probability.py) AI trend estimator based on probabilistic classification. (**python -m demo.tools.growth_probability_demo** for demonstration, [source of the demo](demo/tools/growth_probability_demo.py))

### Screening demos
- [screener/rsi_scr.py](screener/rsi_scr.py) - RSI strategy screener (**python -m demo.screening.rsi_scr_demo** for a demonstation, [source of the demo](demo/screening/rsi_scr_demo.py))
- [screener/regression_scr.py](screener/rsi_scr.py) - Regression AI screener (**python -m demo.screening.regression_scr_demo** for a demonstation, [source of the demo](demo/screening/regression_scr_demo.py))
- [screener/classification_scr.py](screener/classification_scr.py) - Classification AI screener (**python -m demo.screening.classification_scr_demo** for a demonstation, [source of the demo](demo/screening/classification_scr_demo.py))

### Examples of backtesting strategies with portfolio management
- [cap_weight_test.py](demo/backtest/cap_weight_test.py) - Market cap weighted portfolio demo. (**python -m demo.backtest.cap_weight_test**)
- [eql_test.py](demo/backtest/eql_test.py) - Equal sector weight portfolio demo (similar to EQL ETF). Note that here you may 'reconstruct' the ETF even prior its inception date. (**python -m demo.backtest.eql_test**)
- [djia_test.py](demo/backtest/djia_test.py) - Demo to assemble from stocks the price-weighted portfolio which corresponds the DJIA index. (**python -m demo.backtest.djia_test**)
- [grouping_test.py](demo/backtest/grouping_test.py) - Demo to test grouping in a portfolio. Grouping allows you to create a group for a particular asset type (international or domestic stock, particular sectors, bonds etc.). Each group has a pre-defined size which impacts position sizing. (**python -m demo.backtest.grouping_test**)

### Other examples of backtesting strategies
- [backtest/bh.py](backtest/bh.py) - Simple backtesting strategy with periodic investments adjusted to inflation (**python -m demo.backtest.bh_test**, [source of the demo](demo/backtest/bh_test.py))
- [backtest/ma.py](backtest/ma.py) - MA crossover strategy implementation (**python -m demo.backtest.ma_test**, [source of the demo](demo/backtest/ma_test.py))
- [backtest/rsi.py](backtest/rsi.py) - RSI stragegy multi-security demo. See **python -m demo.backtest.rsi_test** for an EOD test and **python demo.backtest.rsi_intraday_test** for an intraday demonstation. [Source of the EOD demo](demo/backtest/rsi_test.py), [source of the intraday demo](demo/backtest/rsi_intraday_test.py).
- [backtest/ma_classification.py](backtest/ma_classification.py) - MA/price crossower strategy where true/false signals are determined by AI. (**python -m demo.backtest.ma_classification_test**, [source of the demo](demo/backtest/ma_classification_test.py))

Note that the tools and backtesting demos create an image with the result of a calculation located in *images* folder and open the image in the default image viewer.

# Additional Details

To keep everything working, please keep all the dependencies up to date. Especially the dependencies which are related to data sources (like yfinance).

Despite beging feature complete, currently Fcore is still in the active development stage as there is still work on low priority issues and performance improvement.

The project is not promoted anywhere yet. However, if you found it and feel interested, sure you are welcome to observe the development process or contribute to the project. The 'general idea' of Fcore will remain the same but APIs still may change.

The repository uses two branches: 'main' and 'devel'. The 'main' branch is supposed to be stable (however, pre-commit validation still needs to be established). The 'devel' branch is used for 'intermediate' development commits and it is not intended to be stable or even working. See TODO's (followed by priority) in the code for what is going to be implemented/fixed in the future.

All fetched quotes are cached in a database (sqlite by default). Data-related settings (like api-keys) are stored in [settings.py](settings.py) file.

Fcore is distributes on an 'AS IS' basis using a custom source available [License](license.md). The author is not responsible for any losses caused by using the project.

Please note that Fcore is a tool which helps you to easily implement and test your own financial strategies but it does not provide any 'out of the box' solutions. Consider all the provided demos as programming examples which help you to implement your own strategies.

In case of any questions, please feel free to contact me by email - zmiciergt at icloud dot com
