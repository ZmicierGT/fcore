"""Demonstration of using AI as a technical indicator.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

from data.futils import check_date
from data.futils import update_layout

from indicators.lstm import LSTM
from indicators.lstm import LSTMRows

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from data.fdata import Query, ReadOnlyData
from data.futils import write_image
from data.fdata import FdataError
from data.fvalues import Rows
from indicators.base import IndicatorError

import sys

if __name__ == "__main__":
    query = Query()
    query.symbol = "SPY"
    query.db_connect()

    query.first_date = check_date("2020-09-01")[1]

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
        print(f"Make sure that the symbol {query.symbol} is fetched and present in the {query.db_name} databases.")
        sys.exit(2)

    period = 60

    lstm = LSTM(rows, period, Rows.AdjClose, 'models/LSTM_1')

    try:
        lstm.calculate()
    except IndicatorError as e:
        print(f"Can't calculate LSTM: {e}")
        sys.exit(2)

    results = lstm.get_results()
    length = len(results)

    # Prepare the chart

    dates = [row[Rows.DateTime] for row in rows]
    quotes = [row[Rows.AdjClose] for row in rows]

    difference = [row[LSTMRows.Difference] for row in results]
    lstm_values = [row[LSTMRows.Value] for row in results]
    volume = [row[Rows.Volume] for row in rows]

    fig = make_subplots(rows=3, cols=1, shared_xaxes=True, row_width=[0.2, 0.2, 0.6],
                        specs=[[{"secondary_y": True}],
                            [{"secondary_y": True}],
                            [{"secondary_y": True}]])  

    fig.add_trace(
        go.Scatter(x=dates, y=quotes, name="AdjClose"), row=1, col=1, secondary_y=False,
    )

    fig.add_trace(
        go.Scatter(x=dates, y=lstm_values, name="LSTM"), row=1, col=1, secondary_y=False,
    )

    fig.add_trace(go.Scatter(x=dates, y=difference, fill='tozeroy', name="LSTM Difference"), row=2, col=1, secondary_y=False)
    fig.add_trace(go.Scatter(x=dates, y=volume, mode='lines', name="Volume"), row=3, col=1, secondary_y=False)  

    ######################
    # Write the chart
    ######################

    update_layout(fig, f"LSTM example chart for {query.symbol}", length)

    new_file = write_image(fig)

    print(f"{new_file} is written.")
