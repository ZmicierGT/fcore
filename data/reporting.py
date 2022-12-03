"""Module with reporting classes.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

import plotly.graph_objects as go
from plotly import subplots

from PIL import Image

from enum import IntEnum

import copy

import numpy as np

import io

class ChartType(IntEnum):
    Line = 0
    Candle = 1
    Ohlc = 2

class ReportsError(Exception):
    """Exception class for reporting."""

class Report():
    """The reporting class."""
    def __init__(self, width, margin=False):
        """Initializes the instance of reporting class.
        
            Args:
                margin(bool): indicates if margin related data should be used.
                width(int): the width of the chart.
        """

        # Generate margin-related data
        self._margin = margin

        # The width of the chart
        if width <= 0:
            raise ReportsError(f"Invalid width specified: {width}. The width should be > 0.")
        self._width = width

        # The container of subcharts. Standalone charts are preferred than plotly subcharts. 
        self._charts = []

    def adjust_trades(self, data):
        """
            Set trade related data to None if there was no trades this day. It helps with chart creation.

            Args:
                data(BTData): instance with backtesting results.
        """
        alt_data = copy.deepcopy(data)
        for i in range(len(data.Symbols)):
            for j in range(len(data.TotalTrades)):
                price_long = data.Symbols[i].TradePriceLong[j]
                price_short = data.Symbols[i].TradePriceShort[j]
                price_margin = data.Symbols[i].TradePriceMargin[j]

                if np.isnan(price_long) and np.isnan(price_short) and np.isnan(price_margin):
                    alt_data.Symbols[i].TradesNo = (j, None)
                    alt_data.TotalTrades = (j, None)

        return alt_data

    def get_charts_num(self, fig):
        """
            Get the number of subcharts in the fugure.

            Args:
                fig(go.Figure): figure to get the number of subcharts.

            Returns:
                int: the number of subcharts in the figure.
        """
        num = 0

        for keyword in fig.layout:
            if keyword.startswith('xaxis'):
                num += 1

        return num

    def update_layout(self, fig, title, height=600):
        """
            Update layout for a chart.

            Args:
                fig(go.Figure): figure to update the layout.
                title(str): title of the chart.
                height(int): height of each subchart (if any).
        """
        # Top offset depends on if we have a chart title (which requires more space)
        top = 70
        if title is None:
            top = 30

        fig.update_layout(
            title_text=title,
            autosize=False,
            width=self._width,
            height=height * self.get_charts_num(fig),
            legend_x=0,
            margin=dict(
                l=50,
                r=50,
                b=0,
                t=top,
                pad=4,
            ),
            legend=dict(
                bordercolor="Black",
                borderwidth=2
            ),
            paper_bgcolor="LightSteelBlue")

    def add_quotes_chart(self, data, index=0, title=None, chart_type=ChartType.Line, fig=None, height=600):
        """Add a quotes chart with price, trades and dates/time to the chart list.
        
            Args:
                data(BtData): data to build the chart.
                index(int): symbol's index to build the chart.
                title(str): the title of the chart.
                chart_type(ChartType): chart type (line, candle, bar)
                fig(go.figure): custom figure to use.
                height(int): the height of the chart image.

            Returns:
                go.figure: created figure.
        """
        # The symbol to use
        symbol = data.Symbols[index]

        if fig is None:
            # Create the default figure
            fig = subplots.make_subplots(subplot_titles=[symbol.Title])

        if chart_type == ChartType.Line:
            fig.add_trace(go.Scatter(x=data.DateTime,
                                     y=symbol.Close,
                                     mode='lines',
                                     name='Quotes'))
        elif chart_type == ChartType.Candle:
            fig.add_trace(go.Candlestick(x=data.DateTime,
                                         open=symbol.Open,
                                         close=symbol.Close,
                                         high=symbol.High,
                                         low=symbol.Low,
                                         name='Quotes'))
            
            fig.update_layout(xaxis_rangeslider_visible=False)
        elif chart_type == ChartType.Ohlc:
            fig.add_trace(go.Ohlc(x=data.DateTime,
                                  open=symbol.Open,
                                  close=symbol.Close,
                                  high=symbol.High,
                                  low=symbol.Low,
                                  name='Quotes'))

            fig.update_layout(xaxis_rangeslider_visible=False)

        fig.add_trace(go.Scatter(x=data.DateTime,
                                 y=symbol.TradePriceLong,
                                 mode='markers',
                                 marker=dict(color='orange'),
                      name='Trades'))

        if self._margin is True:
            fig.add_trace(go.Scatter(x=data.DateTime,
                                     y=symbol.TradePriceShort,
                                     mode='markers',
                                     marker=dict(color='brown'),
                          name='Short Trades'))

            fig.add_trace(go.Scatter(x=data.DateTime,
                                        y=symbol.TradePriceMargin,
                                        mode='markers',
                                        name='Margin Req Trades'))

        self.update_layout(fig=fig, title=title, height=height)

        # Workaround to handle plotly whitespace bug when adding markers.
        fig.update_layout(xaxis={"range":[data.DateTime[0], data.DateTime[-1]]})
        self._charts.append(fig)

        return fig

    def add_expenses_chart(self, data, title=None, fig=None, height=600):
        """Add an expenses chart to the charts list.
        
            Args:
                data(BtData): data to build the chart.
                title(str): the title of the chart.
                fig(go.figure): custom figure to use.
                height(int): the height of the chart image.

            Returns:
                go.figure: created figure.
        """
        if fig is None:
            # Create the default figure
            fig = go.Figure()

            fig.add_trace(go.Scatter(x=data.DateTime, y=data.TotalExpenses, mode='lines', name="Expenses"))
            fig.add_trace(go.Scatter(x=data.DateTime, y=data.CommissionExpense, mode='lines', name="Commission"))
            fig.add_trace(go.Scatter(x=data.DateTime, y=data.SpreadExpense, mode='lines', name="Spread"))

            if self._margin is True:
                fig.add_trace(go.Scatter(x=data.DateTime, y=data.DebtExpense, mode='lines', name="Margin Expenses"))
                fig.add_trace(go.Scatter(x=data.DateTime, y=data.OtherExpense, mode='lines', name="Yield Expenses"))

        self.update_layout(fig=fig, title=title, height=height)
        self._charts.append(fig)

        return fig

    def add_portfolio_chart(self, data, title=None, fig=None, height=600):
        """Add a chart with portfolio performance.
        
            Args:
                data(BtData): data to build the chart.
                title(str): the title of the chart.
                fig(go.figure): custom figure to use.
                height(int): the height of the chart image.

            Returns:
                go.figure: created figure.
        """
        if fig is None:
            # Create the default figure
            fig = go.Figure()

        fig.add_trace(go.Scatter(x=data.DateTime, y=data.TotalValue, mode='lines', name="Total Value"))
        fig.add_trace(go.Scatter(x=data.DateTime, y=data.Deposits, mode='lines', name="Deposits"))
        fig.add_trace(go.Scatter(x=data.DateTime, y=data.OtherProfit, mode='lines', name="Dividends"))

        self.update_layout(fig=fig, title=title, height=height)
        self._charts.append(fig)

        return fig

    def add_custom_chart(self, fig, title=None, height=600):
        """
            Add custom chart to the charts list.

            Args:
                fig(go.figure): the custom chart
                title(str): title of the custom chart
                height(int): the height of the custom chart
        """
        self.update_layout(fig=fig, title=title, height=height)
        self._charts.append(fig)

        return fig        

    def combine_charts(self):
        """
            Get the combined byteimage (PNG) of all charts.
            The custom mechanism of combining charts is preferred over plotly subcharts because it looks better and more flexible.

            Raises:
                ReportsError: no charts are generated yet.

            Returns:
                byteimage(PNG): the combined byte image of all charts.
        """
        if len(self._charts) == 0:
            raise ReportsError("No charts are generated yet.")

        # Get the width of the resulting image
        width = self._charts[0].layout.width

        images = []
        height = 0

        # Iterate through all the charts to get byteimages and calculate sizes
        for fig in self._charts:
            images.append(fig.to_image(format="png"))
            height += fig.layout.height

        # Create the resulting image
        result = Image.new('RGB', (width, height))

        # The variable to store the current vertical image position
        cursor = 0

        # Iterate through images to append them one after another
        for image in images:
            img = Image.open(io.BytesIO(image))
            result.paste(img, (0, cursor))
            cursor += img.height

        result.show()
