##### Fcore is a source available (see the [License](license.md)) framework for financial markets backtesting and screening. It utilizes the power of the classical technical analysis and the modern artificial intelligence approach. Custom AI-based 'indicators' may be used along with a regular TA in your backtesting/screening strategies.

**Fcore** is capable of:
- Usign various financial API's in an unified way without any need to worry about fetching quotes, storing them, parsing and altering the results, managing timespamps compatibility issues of different sources and so on.
- Using multiple financial instruments in one backtesting or real time screening strategy (sure, single instument strategies are available as well).
- Utilizing the power of the classical technical analysis combined with the modern AI approach. Data, provided by AI, may used as a custom 'indicator'.
- Taking into account a lot of expenses related to the actual trade/investment. Fcore handles various margin-specific fees and inflation as well. Obtained results must be very close to an actual results which you may get on a real account using the analyzed strategy.

Please note that Fcore is a tool which helps you to easily implement and test your own financial strategies but does not provide any 'out of the box' investment solutions. Consider all the provided strategies as a programming examples which help you to implement your own strategies.  

All fetched quotes are cached in a database (sqlite by default). Use the following command line tools to manage the quotes (sure the API for managing quotes may be used directly from your python scripts as well). Data management API allows you to easily create wrappers for any other data sources.

- [quotes.py](quotes.py) - general quotes manager
- [yf_cli.py](yf_cli.py) (CLI tool which uses *Yahoo Finance* wrapper in [data/yf.py](data/yf.py))
- [polygon_cli.py](polygon_cli.py) (CLI tool which uses *Polygon.IO* API wrapper in [data/polygon.py](data/yf.py))
- [data/av.py](data/av.py) (API wrapper for *Alpha Vantage* - no CLI tool yet).

Examples of custom indicators (in majority of cases just use pandas_ta):
- [indicators/vo.py](indicators/vo.py) - Volume Oscillator implementation (*python -m demo.indicators.vo_demo* for demonstration)
- [indicators/lstm.py](indicators/lstm.py) - LSTM AI implementation for financial analysis (*python -m demo.ai.lstm_learn* for learning demonstation and *python -m demo.indicators.lstm_demo* for price estimation demonstation)
- [indicators/ma_classifier.py](indicators/ma_classifier.py) 'AI indicator' where MA/price crossover signals are determined by AI if they are true/false. (*python -m demo.indicators.ma_classifier_demo* for demonstration)

Examples of a screening strategy:
- [screener/rsi_scr.py](screener/rsi_scr.py) - RSI strategy screener (*python -m demo.screening.rsi_scr_demo* for a demonstation)

Examples of backtesting strategies:
- [backtest/bh.py](backtest/bh.py) - Simple backtesting strategy with periodic investments adjusted to inflation (*python -m demo.backtest.bh_test*)
- [backtest/ma.py](backtest/ma.py) - MA crossover strategy implementation (*python -m demo.backtest.ma_test*)
- [backtest/rsi.py](backtest/rsi.py) - RSI stragegy multi-instrument demo. See *python -m demo.backtest.rsi_test* for an EOD test and *python demo.backtest.rsi_intraday_test* for an intraday demonstation.
- [backtest/ma_classification.py](backtest/ma_classification.py) - MA/price crossower strategy where true/false signals are determined by AI. (*python -m demo.backtest.ma_classification_test*)

Note that the indicator and backtesting demos create an image with the result of a calculation located in *images* folder. AI learn demonstrations create a model subfolder in *models* folder.

Use *python -m unittest discover -s test -p '*_test.py'* to run unit tests (data components are tested now).

**Fcore** is adopted to [nogil-3.9.10](https://github.com/colesbury/nogil) python interpreter. Use it with **nogil** to benefit from parallel computing.

Fcore is distributes on an 'AS IS' basis. The author is not responsible for any losses caused by using the project.

In case of any questions, please feel free to contact me by email - zmiciergt at icloud dot com
