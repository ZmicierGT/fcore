"""Demonstration of RSI screener.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from screener.rsi_scr import RsiScr
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

    # Minimum period for calculation
    period = 14

    support = 30
    resistance = 70

    scr = RsiScr(symbols=[aapl, msft],
                 period=period,
                 support=support,
                 resistance=resistance,
                 timespan=Timespans.Day)

    print("Please note that the data is delayed (especially volume) and exceptions due to network errors may happen.\n")

    results = scr.screen()

    print("--------------------------------------------------------------")

    for result in results:
        print(f"Symbol: {result[ScrResult.Title]}")
        print(f"Latest update:    {result[ScrResult.LastDatetime]}")
        print(f"Cached quotes:    {result[ScrResult.QuotesNum]}")
        print(f"Previous RSI val: {result[ScrResult.Values][0]}")
        print(f"Current RSI val:  {result[ScrResult.Values][1]}")
        print(f"Signal to buy:    {result[ScrResult.Signals][0]}")
        print(f"Signal to sell:   {result[ScrResult.Signals][1]}\n")
