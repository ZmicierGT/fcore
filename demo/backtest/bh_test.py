"""Demonstration of Buy and Hold strategy backtesting.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

from backtest.bh import BuyAndHold

from backtest.base import BackTestError
from backtest.stock import StockData

from data.futils import check_date
from data.futils import standard_chart

from data.fdata import Query, ReadOnlyData
from data.futils import write_image
from data.fdata import FdataError

import sys

if __name__ == "__main__":
    query = Query()
    query.symbol = "SPY"
    query.db_connect()

    query.first_date = check_date("2020-10-01")[1]

    data = ReadOnlyData(query)

    try:
        rows = data.get_quotes()
        query.db_close()
    except FdataError as e:
        print(e)
        sys.exit(2)

    length = len(rows)

    print(f"Obtained {length} rows.")

    if length == 0:
        print(f"Make sure that the symbol {query.symbol} is fetched and presents in the {query.db_name} database.")
        sys.exit(2)

    quotes = StockData(rows=rows,
                          title=query.symbol,
                          spread=0.1,
                          use_yield=1.5,
                          yield_interval=90
                         )

    bh = BuyAndHold(
        data=[quotes],
        commission=2.5,
        periodic_deposit=500,
        deposit_interval=30,
        inflation=2.5,
        initial_deposit=10000
    )

    try:
        bh.calculate()
    except BackTestError as e:
        print(f"Can't perform backtesting calculation: {e}")
        sys.exit(2)

    results = bh.get_results()

    ##################
    # Build the charts
    ##################

    fig = standard_chart(results, title=f"BuyAndHold Example Testing for {query.symbol}")

    ######################
    # Write the chart
    ######################

    new_file = write_image(fig)

    print(f"{new_file} is written.")
