"""Demonstration of RSI screener.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

from screener.rsi_scr import RsiScr
from screener.base import ScrResult

from data.fvalues import Timespans

from data import av

if __name__ == "__main__":
    # Please note that free AlphaVantage keys do not support live quotes any more.

    query_btc = av.AVQuery()
    # As intraday cryptocurrency has been disabled for free account, daily is used just for demonstration purposes.
    # For actual purposes switch to CryptoIntraday if you have a subscription.
    # TODO better to rewrite this demo to use yahoo finance data.
    query_btc.type = av.AVType.CryptoDaily
    source_btc = av.AV(query_btc)

    query_ltc = av.AVQuery()
    # Same as above ^^
    query_ltc.type = av.AVType.CryptoDaily
    source_ltc = av.AV(query_ltc)

    btc = {'Title': 'BTC', 'Source': source_btc}
    ltc = {'Title': 'LTC', 'Source': source_ltc}

    # Minimum period for calculation
    period = 14
    # Interval to update quotes (in seconds)
    interval = 120

    support = 30
    resistance = 70

    scr = RsiScr(symbols=[btc, ltc],
                 period=period,
                 interval=interval,
                 support=support,
                 resistance=resistance,
                 timespan=Timespans.Intraday)

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
