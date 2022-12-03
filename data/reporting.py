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
                height(int): height of each subchart.
        """
        fig.update_layout(
            title_text=title,
            autosize=False,
            width=self._width,
            height=height * self.get_charts_num(fig),
            #legend_tracegroupgap=600,
            legend_x=0,
            margin=dict(
                l=50,
                r=0,
                b=0,
                t=70,
                pad=4,
            ),
            legend=dict(
                bordercolor="Black",
                borderwidth=2
            ),
            paper_bgcolor="LightSteelBlue")

    def add_quotes_chart(self, data, index=0, title=None, chart_type=ChartType.Line, fig=None, height=600):
        """Create a quotes charts with price, trades and dates/time.
        
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
            fig = subplots.make_subplots(rows=1,
                                         cols=1,
                                         subplot_titles=[symbol.Title],
                                         shared_xaxes=True,
                                         vertical_spacing=0,
                                         horizontal_spacing=0,
                                         specs=[[{"secondary_y": True}]])

        if chart_type == ChartType.Line:
            fig.add_trace(go.Scatter(x=data.DateTime,
                                        y=symbol.Close,
                                        mode='lines',
                                        name='Quotes'),
                            row=1, col=1,
                            secondary_y=False)
        elif chart_type == ChartType.Candle:
            fig.add_trace(go.Candlestick(x=data.DateTime,
                                            open=symbol.Open,
                                            close=symbol.Close,
                                            high=symbol.High,
                                            low=symbol.Low,
                                            name='Quotes'),
                            row=1, col=1,
                            secondary_y=False)
            
            fig.update_layout(xaxis_rangeslider_visible=False)
        elif chart_type == ChartType.Ohlc:
            fig.add_trace(go.Ohlc(x=data.DateTime,
                                    open=symbol.Open,
                                    close=symbol.Close,
                                    high=symbol.High,
                                    low=symbol.Low,
                                    name='Quotes'),
                            row=1, col=1,
                            secondary_y=False)

            fig.update_layout(xaxis_rangeslider_visible=False)

        fig.add_trace(go.Scatter(x=data.DateTime,
                                    y=symbol.TradePriceLong,
                                    mode='markers',
                                    marker=dict(color='orange'),
                                    name='Trades'),
                        row=1, col=1,
                        secondary_y=False)

        if self._margin is True:
            fig.add_trace(go.Scatter(x=data.DateTime,
                                        y=symbol.TradePriceShort,
                                        mode='markers',
                                        marker=dict(color='brown'),
                            name='Short Trades'),
                            row=1, col=1, secondary_y=False)

            fig.add_trace(go.Scatter(x=data.DateTime,
                                        y=symbol.TradePriceMargin,
                                        mode='markers',
                                        name='Margin Req Trades'),
                            row=1, col=1,
                            secondary_y=False)

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

        # Get the width of the resulting image (and remove excessive margin bug)
        width = self._charts[0].layout.width - 100

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
        result.save("test.png")

    def MainCharts(self, data, title=None, chart_type=ChartType.Line, fig=None):
        """Create a main charts (for each symbol) with price and dates/time.
        
            Args:
                data(BtData): data to build chart.
                title(str): the title of the chart.
                chart_type(ChartType): chart type (line, candle, bar)
                fig(go.figure): custom figure to use.

            Returns:
                go.figure: created figure.
        """
        # The number of symbols used in the data instance
        sym_num = len(data.Symbols)

        if fig is None:
            # Ratio of each subchart size
            chart_size = 1 / sym_num

            # Create the figure.
            row_width = [chart_size] * sym_num
            specs = [[{"secondary_y": True}]] * sym_num

            # Get titles for subplots
            titles = []
            for symbol in data.Symbols:
                titles.append(symbol.Title)

            # Create the default figure
            fig = subplots.make_subplots(rows=sym_num,
                                         cols=1,
                                         subplot_titles=titles,
                                         shared_xaxes=False,
                                         vertical_spacing=0.05,
                                         row_width=row_width,
                                         specs=specs)

        # Variable to iterate through subcharts
        row_num = 0

        for symbol in data.Symbols:
            row_num += 1

            if chart_type == ChartType.Line:
                fig.add_trace(go.Scatter(x=data.DateTime,
                                         y=symbol.Close,
                                         mode='lines',
                                         name=f'Quotes {symbol.Title}',
                                         legendgroup = symbol.Title),
                              row=row_num, col=1,
                              secondary_y=False)
            elif chart_type == ChartType.Candle:
                fig.add_trace(go.Candlestick(x=data.DateTime,
                                             open=symbol.Open,
                                             close=symbol.Close,
                                             high=symbol.High,
                                             low=symbol.Low,
                                             name=f'Quotes {symbol.Title}',
                                             legendgroup = symbol.Title),
                              row=row_num, col=1,
                              secondary_y=False)
                
                fig.update_layout(xaxis_rangeslider_visible=False)
            elif chart_type == ChartType.Ohlc:
                fig.add_trace(go.Ohlc(x=data.DateTime,
                                      open=symbol.Open,
                                      close=symbol.Close,
                                      high=symbol.High,
                                      low=symbol.Low,
                                      name=f'Quotes {symbol.Title}',
                                      legendgroup = symbol.Title),
                              row=row_num, col=1,
                              secondary_y=False)

                fig.update_layout(xaxis_rangeslider_visible=False)

            fig.add_trace(go.Scatter(x=data.DateTime,
                                     y=symbol.TradePriceLong,
                                     mode='markers',
                                     marker=dict(color='DarkSlateGrey'),
                                     name=f'Trades {symbol.Title}',
                                     legendgroup = symbol.Title),
                          row=row_num, col=1,
                          secondary_y=False)

            if self._margin is True:
                fig.add_trace(go.Scatter(x=data.DateTime,
                                         y=symbol.TradePriceShort,
                                         mode='markers',
                                         marker=dict(color='burlywood'),
                                         legendgroup = symbol.Title,
                                         name=f'Short Trades {symbol.Title}'),
                              row=row_num, col=1, secondary_y=False)

                fig.add_trace(go.Scatter(x=data.DateTime,
                                         y=symbol.TradePriceMargin,
                                         mode='markers',
                                         name=f'Margin Req Trades {symbol.Title}',
                                         legendgroup = symbol.Title),
                              row=row_num, col=1,
                              secondary_y=False)

        self.update_layout(fig=fig, title=title)

        self._charts.append(fig)

        return fig
