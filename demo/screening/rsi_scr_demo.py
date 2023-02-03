"""Demonstration of RSI screener.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

from screener.rsi_scr import RsiScr
from screener.base import ScrResult

from data.fvalues import Timespans

from data import yf

if __name__ == "__main__":
    source_btc = yf.YF()
    source_ltc = yf.YF()

    btc = {'Title': 'BTC-USD', 'Source': source_btc}
    ltc = {'Title': 'LTC-USD', 'Source': source_ltc}

    # Minimum period for calculation
    period = 14
    # Interval to update quotes (in seconds)
    interval = 60

    support = 30
    resistance = 70

    scr = RsiScr(symbols=[btc, ltc],
                 period=period,
                 interval=interval,
                 support=support,
                 resistance=resistance,
                 timespan=Timespans.Intraday)

    print("Using YF as the data source for demonstration purposes only! Please note that the data is delayed (especially volume)")
    print("and exceptions due to network errors may happen.\n")

    print(f"Press CTRL+C to cancel screening. The interval is {interval} seconds.")

    while True:
        scr.do_cycle()

        results = scr.get_results()

        print("--------------------------------------------------------------")

        for i in range(2):
            print(f"Symbol: {results[i][ScrResult.Title]}")
            print(f"Latest update:    {results[i][ScrResult.LastDatetime]}")
            print(f"Cached quotes:    {results[i][ScrResult.QuotesNum]}")
            print(f"Previous RSI val: {results[i][ScrResult.Values][0]}")
            print(f"Current RSI val:  {results[i][ScrResult.Values][1]}")
            print(f"Signal to buy:    {results[i][ScrResult.Signals][0]}")
            print(f"Signal to sell:   {results[i][ScrResult.Signals][1]}\n")
