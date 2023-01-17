##### Fcore is a source available (see the [License](license.md)) framework for financial markets backtesting and screening. It utilizes the power of the classical technical analysis and the modern artificial intelligence approach. Custom AI-based 'indicators' may be used along with a regular TA in your backtesting/screening strategies.

**Fcore** is capable of:
- Usign various financial API's in an unified way without any need to worry about fetching quotes, storing them, parsing and altering the results, managing timespamps compatibility issues of different sources and so on.
- Using multiple financial instruments in one backtesting or real time screening strategy (sure, single instument strategies are available as well).
- Utilizing the power of the classical technical analysis combined with the modern AI approach. Data, provided by AI, may used as a custom 'indicator'.
- Taking into account a lot of expenses related to the actual trade/investment. Fcore handles various margin-specific fees and inflation as well. Obtained results must be very close to an actual results which you may get on a real account using the analyzed strategy.

Please note that Fcore is a tool which helps you to easily implement and test your own financial strategies but it does not provide any 'out of the box' investment solutions. Consider all the provided strategies as a programming examples which help you to implement your own strategies.

Currently the project is in the active development stage and is not promoted anywhere yet. However, if you found it and feel interested, sure you are welcome to observe the development process or contribute to the project. The reporitory uses two branches: 'main' and 'devel'. The 'main' branch is usually stable but as a verification process is not fully established yet and sometimes bugs may be committed there. The 'devel' branch is used for 'intermediate' development commits and it is not indended to be stable or even working. The 'general idea' of the project will remain the same but interfaces still may change.

Overall, the next development steps are (planned to finish in Autumn 2023):
- Continue to adopt the project to use various data sources and store the data in a unified way. Data sources are Yahoo Finance for demonstration purposes because it is free and does not require any registration or obtaining API-keys, Polygon.IO, AlphaVantahe and Finnhub.io.
- Continue to implement the API for AI-based analysis of financial markets. The main focus is on these 3 approaches: classifying TA-signals (true/false) with the help of AI (already implemented as a 'classifier's), analyzing data to estimate the future prices as a regression problem (partially implemented as LSTM-demos), estimating a future market moves as a probabilistic classification. Also a detailed description/documentation will be writted how to use this API.
- Further improvements in performance and reliability. Scripts for general pre-commit validation of the 'main' branch should be written.

Some clarifications regarding the terminology used in the project and its 'philosophy'. Any prices are hardly possible to predict and that is why the word 'estimation' is used instead of 'prediction' (which is traditionally used in the area of AI/ML). Regarding backtesting, it is considered as a kind of a metric (not ideal, but the best available) to estimate the 'quality' of the trained model(s). Even if you get a very good result on precision score or f1 metrics, the actual results may be poor on non-standard market behavior (like March 2020) and only several mistakes may lead to significant loses. Also all factors related to investment should be taken into account. For example, ignoring some fees related to margin which seem 'not significant' may mislead you and a bad strategy will show an outstanding result during testing. That is why any possible expenses should be handled very carefully in backtests.

All fetched quotes are cached in a database (sqlite by default). Use the following command line tools to manage the quotes (sure the API for managing quotes may be used directly from the python scripts as well). Data management API allows you to easily create wrappers for any other data sources.

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
