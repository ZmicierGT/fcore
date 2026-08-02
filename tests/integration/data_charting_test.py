"""Data charting testing script for YF and FMP data sources.

The purpose of this script is to fetch data from both data sources and display
the corresponding charts on the same image to compare the data visually.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
import plotly.graph_objects as go

from termcolor import colored

from data.futils import update_layout
from data.futils import show_image

from data.fdata import FdataError
from data.fvalues import StockQuotes

from data.yf import YF
from data.fmp import FMP

import sys

symbol = 'AAPL'

def get_quotes(data_source_class, source_title):
    """
        Fetch the quotes from the specified data source.

        Args:
            data_source_class(class): data source class to use.
            source_title(str): title of the data source.

        Returns:
            ndarray: fetched quote entries.
    """
    print(colored(f"\nFetching data from the {source_title} data source:\n", "yellow"))

    source = data_source_class(symbol=symbol, first_date="2020-2-1", last_date="2024-3-1", verbosity=True, db_name=":memory:")
    source._db_connect()
    rows = source.get()
    print(f"{source._source_title} splits: {source._get_db_splits()}")
    source._db_close()

    print(f"The total number of quotes used for {symbol} is {len(rows)}.\n")

    return rows

def display_chart(rows_yf, rows_fmp):
    """
        Display the chart with the adj close quotes of both data sources.

        Args:
            rows_yf(ndarray): fetched quote entries from the YF data source.
            rows_fmp(ndarray): fetched quote entries from the FMP data source.
    """
    len_yf = len(rows_yf)
    len_fmp = len(rows_fmp)

    if len_yf != len_fmp:
        print(colored(f"Warning: Number of rows for each data source does not match: {len_yf} vs {len_fmp}\n", "yellow"))

    length = max(len_yf, len_fmp)

    dates = rows_yf[StockQuotes.DateTime]

    price_yf = rows_yf[StockQuotes.AdjClose]
    price_fmp = rows_fmp[StockQuotes.AdjClose]

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(x=dates, y=price_yf, name="YF AdjClose"),
    )

    fig.add_trace(
        go.Scatter(x=dates, y=price_fmp, name="FMP AdjClose"),
    )

    ######################
    # Write the chart
    ######################

    update_layout(fig, f"YF and FMP data source charts for {symbol}", length)

    fig.update_yaxes(title_text="<b>Price</b>")

    show_image(fig)

if __name__ == "__main__":
    try:
        rows_yf = get_quotes(YF, "YF")
        rows_fmp = get_quotes(FMP, "FMP")

        display_chart(rows_yf, rows_fmp)
    except FdataError as e:
        sys.exit(e)

    print(colored("Data for charting obtained successfully. Check if both charts match.", "green"))
