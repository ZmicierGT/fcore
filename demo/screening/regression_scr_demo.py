"""Demonstration of a regression AI screener.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from screener.regression_scr import RegScr
from screener.base import ScrResult

from data.fvalues import Timespans

from data import yf

if __name__ == "__main__":
    warning = "WARNING! Using yfinance data for the demonstration.\n" +\
                "Always keep yfinance up to date ( pip install yfinance --upgrade ) and use quotes obtained from this " +\
                "datasource only for demonstation purposes!\n"
    print(warning)

    source_btc = yf.YF()
    source_ltc = yf.YF()

    btc = {'Title': 'BTC-USD', 'Source': source_btc}
    ltc = {'Title': 'LTC-USD', 'Source': source_ltc}

    # Max rows stored along with Regression instance. Used to prevent too huge dataset in memory due to incoming quotes.
    max_rows = 1000
    interval = 60  # Interval to update quotes (in seconds)

    window_size = 10  # Sliding window size
    forecast_size = 5  # Number of periods to forecast
    test_length = 100  # Length of data to perform forecasting.
    epochs=1000

    scr = RegScr(symbols=[btc, ltc],
                 max_rows=max_rows,
                 interval=interval,
                 window_size=window_size,
                 forecast_size=forecast_size,
                 test_length=test_length,
                 epochs=epochs,
                 timespan=Timespans.Minute,
                 period=test_length)

    print("Please note that the data is delayed (especially volume) and exceptions due to network errors may happen.\n")
    print(f"Press CTRL+C to cancel screening. The interval is {interval} seconds.")

    while True:
        scr.do_cycle()

        results = scr.get_results()

        print("--------------------------------------------------------------")

        for i in range(2):
            print(f"Symbol: {results[i][ScrResult.Title]}")
            print(f"Latest update:    {results[i][ScrResult.LastDatetime]}")
            print(f"Cached quotes:    {results[i][ScrResult.QuotesNum]}")
            print(f"Current price:    {results[i][ScrResult.Values][0]}")
            print(f"Forecasted price: {results[i][ScrResult.Values][1]}")
            print(f"Signal to buy:    {results[i][ScrResult.Signals][0]}")
            print(f"Signal to sell:   {results[i][ScrResult.Signals][1]}\n")
