"""Demonstration of a regression AI screener.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from screener.regression_scr import RegScr
from screener.base import ScrResult

from data.fvalues import Timespans

from data import yf

if __name__ == "__main__":
    warning = "WARNING! This screener is just an example and do not treat the obtained signals as an investment advice.\n" +\
                "Always keep yfinance up to date ( pip install yfinance --upgrade ) and use quotes obtained from this " +\
                "datasource only for demonstation purposes!\n"
    print(warning)

    source_aapl = yf.YF(symbol='AAPL')
    source_msft = yf.YF(symbol='MSFT')

    aapl = {'Title': 'AAPL', 'Source': source_aapl}
    msft = {'Title': 'MSFT', 'Source': source_msft}

    # Max rows stored along with Regression instance. Used to prevent too huge dataset in memory.
    max_rows = 1000

    window_size = 10  # Sliding window size
    forecast_size = 5  # Number of periods to forecast
    test_length = 100  # Length of data to perform forecasting.
    epochs = 1000

    scr = RegScr(symbols=[aapl, msft],
                 max_rows=max_rows,
                 window_size=window_size,
                 forecast_size=forecast_size,
                 test_length=test_length,
                 epochs=epochs,
                 timespan=Timespans.Day,
                 init_days=250,
                 period=test_length)

    print("Please note that the data is delayed (especially volume) and exceptions due to network errors may happen.\n")

    results = scr.screen()

    print("--------------------------------------------------------------")

    for result in results:
        print(f"Symbol: {result[ScrResult.Title]}")
        print(f"Latest update:    {result[ScrResult.LastDatetime]}")
        print(f"Cached quotes:    {result[ScrResult.QuotesNum]}")
        print(f"Current price:    {result[ScrResult.Values][0]}")
        print(f"Forecasted price: {result[ScrResult.Values][1]}")
        print(f"Signal to buy:    {result[ScrResult.Signals][0]}")
        print(f"Signal to sell:   {result[ScrResult.Signals][1]}\n")
