##### Fcore is a source available (see the [License](license.md)) AI framework for financial markets backtesting and screening. It is currently in an active developlement state.

**Fcore** is based on the following principles:
- AI is becoming the essential (and hardly avoibale) part of financial analysis.
- For successful AI-appliances, we need a lot of data and the data should be well stucturized and quickly accessible.
- Analyzing financial markets has a lot of common features. That is why a data analyzing API should be used to reduce a routine work.
- All data processing techniques should be treated equally. There is no difference between a technical indicator data, AI generated data or any other data manupulation (like P/E calculation or estimation of a date of a next report). All these manipulations should follow the same interface.

Based on these principles (and not only), **Fcore** is capable of:
- Obtaining data from various sources (AlphaVantage, Polygon, Yahoo Finance, Finnhub) and storing it in an unified way without any need to worry about parsing the data, altering the results, handling timestamp compatibility issues and so on. Currently around 100 data parameters are supported. If you need an additional source, API allows to quickly write an extension to obtain and parse the required data.
- Providing an API to ease the development of AI-strategies for financial analysis. The main focus is on these 3 approaches: classifying TA-signals (true/false) with the help of AI (already implemented as classifiers), analyzing data to estimate the future prices as a regression problem (partially implemented as LSTM-demos), estimating a future market moves as a probabilistic classification (to be implemented).
- Utilizing the power of the 'classical' technical and fundamental analyses combined with the modern AI-approach. As there is no difference in data processing techniques, data provided by AI may treated as a custom 'AI-indicator'.
- Using multiple financial instruments in one backtesting or real time screening strategy (sure, single instument strategies are available as well).
- Backtesting is treated as an another AI-metric and it takes into account a lot of expenses related to an actual trade/investment. Fcore's backtesting engine handles various margin-specific fees and inflation as well. Obtained results must be very close to actual results which you could get on a real market using the analyzed strategy.

These features allow you to develop your AI-based market strategies much faster than when using a 'bare'-approach.

Please note that Fcore is a tool which helps you to easily implement and test your own financial strategies but it does not provide any 'out of the box' solutions. Consider all the provided strategies as programming examples which help you to implement your own strategies.

To keep everything working, please keep all the depencies up to date. Especially the dependencies which are related to data sources (like yfinance).

Currently the project is in the active development stage and is not promoted anywhere yet. However, if you found it and feel interested, sure you are welcome to observe the development process or contribute to the project. The 'general idea' of the project will remain the same but APIs still may change.

The reporitory uses two branches: 'main' and 'devel'. The 'main' branch is usually stable but as a verification process is not fully established yet, sometimes bugs may be committed there. The 'devel' branch is used for 'intermediate' development commits and it is not indended to be stable or even working. See TODO's (followed by priority) in the code for what is going to be implemented/fixed in the future.

Overall, the next development steps are (planned to finish in Autumn 2023):
- ~~Continue to adopt the project to use various data sources and store the fundamental data in a unified way. Data sources are Yahoo Finance (for demonstration purposes only), Polygon.IO, AlphaVantahe and Finnhub.io~~ (already implemented).
- Continue to implement the API for AI-based analysis of financial markets. Exact description should be written how to use this API.
- Further improvements in performance and reliability. Scripts for pre-commit validation to the 'main' branch should be written.

All fetched quotes are cached in a database (sqlite by default). Data-related settings (like api-keys) are stored in [settings.py](settings.py) file.

Use the following command line tools to manage the quotes (sure the API for managing quotes may be used directly from the python scripts as well). Data management API allows you to easily create wrappers for any other data sources.

- [quotes.py](quotes.py) - general quotes manager
- [yf_cli.py](yf_cli.py) (CLI tool which uses *Yahoo Finance* wrapper in [data/yf.py](data/yf.py))
- [polygon_cli.py](polygon_cli.py) (CLI tool which uses *Polygon.IO* API wrapper in [data/polygon.py](data/yf.py))
- [data/av.py](data/av.py) (API wrapper for *Alpha Vantage* stock data).
- [data/fh.py](data/fh.py) (API wrapper for *Finnhub* real time quote data).

Examples of custom data processing tools (in majority of cases just use pandas_ta for technical analysis calculations):
- [tools/vo.py](tools/vo.py) - Volume Oscillator implementation (*python -m demo.tools.vo_demo* for demonstration)
- [tools/lstm.py](tools/lstm.py) - LSTM AI implementation for financial analysis (*python -m demo.ai.lstm_learn* for learning demonstation and *python -m demo.tools.lstm_demo* for price estimation demonstation)
- [tools/ma_classifier.py](tools/ma_classifier.py) 'AI-indicator' where MA/price crossover signals are determined by AI if they are true/false. (*python -m demo.tools.ma_classifier_demo* for demonstration)

Examples of a screening strategy:
- [screener/rsi_scr.py](screener/rsi_scr.py) - RSI strategy screener (*python -m demo.screening.rsi_scr_demo* for a demonstation)

Examples of backtesting strategies:
- [backtest/bh.py](backtest/bh.py) - Simple backtesting strategy with periodic investments adjusted to inflation (*python -m demo.backtest.bh_test*)
- [backtest/ma.py](backtest/ma.py) - MA crossover strategy implementation (*python -m demo.backtest.ma_test*)
- [backtest/rsi.py](backtest/rsi.py) - RSI stragegy multi-instrument demo. See *python -m demo.backtest.rsi_test* for an EOD test and *python demo.backtest.rsi_intraday_test* for an intraday demonstation.
- [backtest/ma_classification.py](backtest/ma_classification.py) - MA/price crossower strategy where true/false signals are determined by AI. (*python -m demo.backtest.ma_classification_test*)

Note that the tools and backtesting demos create an image with the result of a calculation located in *images* folder. AI learn demonstrations create a model subfolder in *models* folder.

Use *python -m unittest discover -s test -p '*_test.py'* to run unit tests (data components are tested now).

**Fcore** is adopted to [nogil-3.9.10](https://github.com/colesbury/nogil) python interpreter. Use it with **nogil** to benefit from parallel computing.

Fcore is distributes on an 'AS IS' basis. The author is not responsible for any losses caused by using the project.

In case of any questions, please feel free to contact me by email - zmiciergt at icloud dot com
