"""Module with general functions which are not related to any class.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

import configparser

from datetime import datetime
import pytz

import plotly.graph_objects as go
from plotly import subplots

import os
from os.path import exists
import glob

from data import fvalues

import threading
import multiprocessing
import time

from data.fvalues import Quotes

import numpy as np

def check_date(date):
    """
        Check if the date is valid.

        Args:
            str: date - string with the date to check and format.

        Returns:
            is_correct(bool): indicates if the date is correct.
            ts(int): timestamp.
    """
    is_correct = None
    dt = None
    ts = 0

    try:
        dt = datetime.strptime(date, '%Y-%m-%d')

        # Keep all timestamps UTC adjusted
        dt = dt.replace(tzinfo=pytz.utc)
        ts = int(datetime.timestamp(dt))

        is_correct = True
    except ValueError:
        is_correct = False

    return is_correct, ts

def check_datetime(dt_str):
    """
        Check if the datetime is valid.

        Args:
            str: dt_str - string with the datetime to check and format.

        Returns:
            is_correct(bool): indicates if the date is correct.
            ts(int): timestamp.
    """
    if len(dt_str) <= 10:
        return check_date(dt_str)

    is_correct = None
    dt = None
    ts = 0

    try:
        dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')

        # Keep all timestamps UTC adjusted
        dt = dt.replace(tzinfo=pytz.utc)
        ts = int(datetime.timestamp(dt))

        is_correct = True
    except ValueError:
        is_correct = False

    return is_correct, ts

def get_datetime(dt_str):
    """
        Create datetime from a string.

        Args:
            dt_str(str): string with a date.

        Raises:
            ValueError: the datatime string is not correct.

        Returns:
            datetime from the input string.
    """
    try:
        dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
    except ValueError as e:
        raise ValueError(f"The date {dt_str} is incorrect: {e}") from e

    return dt

# Get values from ini.file
def parse_config(query):
    """
        Parse configuration file to fill query object.

        Returns:
            Query: Query instance with updated properties according to the config file content.
    """
    ini_file = "settings.ini"

    config_parser = configparser.ConfigParser()

    # If for any reason config can't be read, use standard values
    try:
        config_parser.read(ini_file)
        settings = config_parser[query.source_title]

        query.db_name = settings['db_name']
        query.db_type = settings['db_type']
    except Exception:
        # Using default values from 'query' instance if configuration can't be read
        # Other db setting are supposed to be in the different sections (when needed)
        if not config_parser.has_section(query.source_title):
            config_parser.add_section(query.source_title)
            config_parser.set(query.source_title, "db_name", query.db_name)
            config_parser.set(query.source_title, "db_type", query.db_type)

        # Save configuration for future use
        with open(ini_file, 'w') as config_file:
            config_parser.write(config_file)
        config_file.close()

    return query

# Writes chart image to a file
def write_image(fig):
    """
        Write plotly figure to a disk.

        Args:
            fig(go.Figure): plotly figure to write.

        Returns:
            str: new file name.

        Raises:
            RuntimeError: can't generate a filename.
    """
    img_dir = "images"

    if exists(img_dir) == False:
        os.mkdir(img_dir)

    os.chdir(img_dir)
    files = glob.glob("fig_*.png")

    files.sort(key=lambda x: int(x.partition('_')[2].partition('.')[0]))

    if len(files) == 0:
        last_file = 0
    else:
        last_file = files[-1]
        last_file = last_file.replace('.png', '').replace('fig_', '')
    
    try:
        new_counter = int(last_file) + 1
    except ValueError as e:
        raise RuntimeError(f"Can't generate new filename. {last_file} has a broken filename pattern.") from e

    new_file = "fig_" + f"{new_counter}" + ".png"

    fig.write_image(new_file)

    return new_file

# Writes AI model to a directory
def write_model(name, model):
    """
        Write keras model to a disk.

        Args:
            model(keras.Model): keras model to write.

        Returns:
            str: new file name.

        Raises:
            RuntimeError: can't generate a directory name.
    """
    model_dir = "models"

    if exists(model_dir) == False:
        os.mkdir(model_dir)

    os.chdir(model_dir)
    directories = glob.glob(f"{name}_*")

    directories.sort(key=lambda x: int(x.partition('_')[2].partition('.')[0]))

    if len(directories) == 0:
        last_dir = 0
    else:
        last_dir = directories[-1]
        last_dir = last_dir.replace(f'{name}_', '')
    
    try:
        new_counter = int(last_dir) + 1
    except ValueError as e:
        raise RuntimeError(f"Can't generate new directory name. {last_dir} has a broken filename pattern.") from e

    new_dir = f"{name}_" + f"{new_counter}"

    model.save(new_dir)

    return new_dir

def build_chart(rows):
    """
        Build a basic line chart and write it to a disk.

        Args:
            rows(list): list with quotes to build chart.

        Returns:
            str: new file name.
    """
    date = [row[fvalues.Quotes.DateTime] for row in rows]
    close = [row[fvalues.Quotes.AdjClose] for row in rows]

    fig = go.Figure([go.Scatter(x=date, y=close)])

    fig.update_layout(
        autosize=False,
        width=1500,
        height=900,
        margin=dict(
            l=50,
            r=50,
            b=100,
            t=100,
            pad=4
        ),
        paper_bgcolor="LightSteelBlue",)

    return write_image(fig)

def get_dt_offset(rows, dt):
    """
        Get datetime offset in the list.

        Args:
            rows(list): list with quotes.
            dt(dtr): datetime string.

        Returns:
            int: offset in the list where datetime is found.

        Raises:
            RuntimeError: datetime not found.
    """
    dts = [row[Quotes.DateTime] for row in rows]

    if len(dt) == 10:
        dt += ' 23:59:59'

    for row in dts:
        if dt <= row:
            return dts.index(row)

    raise RuntimeError(f"Can't find the datetime <= {dt} in the list.")

#################################
# Functions related to reporting.
#################################

def update_layout(fig, title, length):
    """
        Update layout for a chart.

        Args:
            fig(go.Figure): figure to update the layout.
            title(str): title of the chart.
            length(int): length of the resulting list.
    """
    def_width = 2500
    width = max(def_width, length)

    fig.update_layout(
        title_text=title,
        autosize=False,
        width=width,
        height=1000,
        margin=dict(
            l=50,
            r=50,
            b=200,
            t=100,
            pad=4
        ),
        paper_bgcolor="LightSteelBlue")

def adjust_trades(results):
    """
        Set trade related data to None if there was no trades this day. It helps with chart creation.

        Args:
            results(BTData): instance with backtesting results.
    """
    for i in range(len(results.Symbols)):
        for j in range(len(results.TotalTrades)):
            price_long = results.Symbols[i].TradePriceLong[j]
            price_short = results.Symbols[i].TradePriceShort[j]
            price_margin = results.Symbols[i].TradePriceMargin[j]

            if np.isnan(price_long) and np.isnan(price_short) and np.isnan(price_margin):
                results.Symbols[i].TradesNo = (j, None)
                results.TotalTrades = (j, None)

def main_chart(results, fig):
    """
        Generate main chart for a regular (non-margin) strategy.

        Args:
            results(BTData): results of the calculation.
            fig(go.Figure): plotly figure to display results.
    """
    for i in range(len(results.Symbols)):
        secondary_y = False
        j = i + 1
        if j != 1 and j % 2 == 0:
            secondary_y = True

        num = round(i/2) + 1

        fig.add_trace(go.Scatter(x=results.DateTime, y=results.Symbols[i].Close, mode='lines', name=f'Quotes {results.Symbols[i].Title}'), row=num, col=1, secondary_y=secondary_y)
        fig.add_trace(go.Scatter(x=results.DateTime, y=results.Symbols[i].TradePriceLong, mode='markers', name=f'Trades {results.Symbols[i].Title}'), row=num, col=1, secondary_y=secondary_y)

def main_margin_chart(results, fig):
    """
        Generate main chart for a margin strategy.

        Args:
            results(BTData): results of the calculation.
            fig(go.Figure): plotly figure to display results.
    """
    for i in range(len(results.Symbols)):
        secondary_y = False
        j = i + 1
        if j != 1 and j % 2 == 0:
            secondary_y = True

        num = round(i/2) + 1

        fig.add_trace(go.Scatter(x=results.DateTime, y=results.Symbols[i].Close, mode='lines', name=f'Quotes {results.Symbols[i].Title}'), row=num, col=1, secondary_y=secondary_y)
        fig.add_trace(go.Scatter(x=results.DateTime, y=results.Symbols[i].TradePriceLong, mode='markers', name=f'Long Trades {results.Symbols[i].Title}'), row=num, col=1, secondary_y=secondary_y)
        fig.add_trace(go.Scatter(x=results.DateTime, y=results.Symbols[i].TradePriceShort, mode='markers', name=f'Short Trades {results.Symbols[i].Title}'), row=num, col=1, secondary_y=secondary_y)
        fig.add_trace(go.Scatter(x=results.DateTime, y=results.Symbols[i].TradePriceMargin, mode='markers', name=f'Margin Req Trades {results.Symbols[i].Title}'), row=num, col=1, secondary_y=secondary_y)

def value_chart(results, fig):
    """
        Generate value subchart.

        Args:
            results(BTData): results of the calculation.
            fig(go.Figure): plotly figure to display results.
    """
    num = get_charts_num(fig) - 1

    fig.add_trace(go.Scatter(x=results.DateTime, y=results.TotalValue, mode='lines', name="Total Value"), row=num, col=1)
    fig.add_trace(go.Scatter(x=results.DateTime, y=results.Deposits, mode='lines', name="Deposits"), row=num, col=1)
    fig.add_trace(go.Scatter(x=results.DateTime, y=results.OtherProfit, mode='lines', name="Dividends"), row=num, col=1)

def expenses_chart(results, fig):
    """
        Generate expenses chart for a regular (non-margin) strategy.

        Args:
            results(BTData): results of the calculation.
            fig(go.Figure): plotly figure to display results.
    """
    num = get_charts_num(fig)

    fig.add_trace(go.Scatter(x=results.DateTime, y=results.TotalExpenses, mode='lines', name="Expenses"), row=num, col=1)
    fig.add_trace(go.Scatter(x=results.DateTime, y=results.CommissionExpense, mode='lines', name="Commission"), row=num, col=1)
    fig.add_trace(go.Scatter(x=results.DateTime, y=results.SpreadExpense, mode='lines', name="Spread"), row=num, col=1)

def expenses_margin_chart(results, fig):
    """
        Generate expenses chart for a margin strategy.

        Args:
            results(BTData): results of the calculation.
            fig(go.Figure): plotly figure to display results.
    """
    num = get_charts_num(fig)

    expenses_chart(results, fig)

    fig.add_trace(go.Scatter(x=results.DateTime, y=results.DebtExpense, mode='lines', name="Margin Expenses"), row=num, col=1)
    fig.add_trace(go.Scatter(x=results.DateTime, y=results.OtherExpense, mode='lines', name="Yield Expenses"), row=num, col=1)

def get_charts_num(fig):
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

def create_standard_fig():
    """
        Create a standard figure for a basic strategy.

        Returns:
            go.Figure: a figure for a basic strategy.
    """
    return subplots.make_subplots(rows=3, cols=1, shared_xaxes=True, row_width=[0.3, 0.3, 0.4],
                                  specs=[[{"secondary_y": True}],
                                         [{"secondary_y": True}],
                                         [{"secondary_y": False}]])

def standard_chart(results, title='', fig=None):
    """
        Generate a standard chart for non-margin strategy.

        Args:
            results(BTData): calculation results.
            title(str): title for the chart.
            fig(go.Figure): figure to display results.

        Returns:
            go.Figure: figure to display results.
    """
    fig = prepare_chart(results, title, fig)

    main_chart(results, fig)
    value_chart(results, fig)
    expenses_chart(results, fig)

    return fig

def standard_margin_chart(results, title='', fig=None):
    """
        Generate a standard chart for a margin strategy.

        Args:
            results(BTData): calculation results.
            title(str): title for the chart.
            fig(go.Figure): figure to display results.

        Returns:
            go.Figure: figure to display results.
    """
    fig = prepare_chart(results, title, fig)

    main_margin_chart(results, fig)
    value_chart(results, fig)
    expenses_margin_chart(results, fig)

    return fig

def prepare_chart(results, title, fig):
    """
        Prepare the chart to display results.

        Args:
            results(BTData): calculation results.
            title(str): title for the chart.
            fig(go.Figure): figure to display results.

        Returns:
            go.Figure: figure to display results.
    """
    if fig == None:
        fig = create_standard_fig()

    update_layout(fig, title, len(results.DateTime))

    add_annotations(results, fig)

    # No need to display trades number if there was no trade this day
    adjust_trades(results)

    return fig

def add_annotations(results, fig):
    """
        Add annotations to the chart.

        Args:
            results(BTData): calculation results.
            fig(go.Figure): figure to display results.
    """
    height = fig.layout['height']

    invested = results.Deposits[-1]
    final_value = results.TotalValue[-1]
    profit = final_value / invested * 100 - 100

    invested     = f"Invested in-total: {round(invested, 2)}"
    value        = f"Total value: {round(final_value, 2)}"
    profit       = f"Profit: {round(profit, 2)}%"
    yield_profit = f"Yield profit: {round(results.OtherProfit[-1], 2)}"
    total_trades = f"Total trades: {results.TotalTrades[-1]}"

    expenses = f"Total expenses: {round(results.TotalExpenses[-1], 2)}"
    comm_expense = f"Commission expense: {round(results.CommissionExpense[-1], 2)}"
    spread_expense = f"Spread expense: {round(results.SpreadExpense[-1], 2)}"
    debt_expense = f"Debt expense: {round(results.DebtExpense[-1], 2)}"
    yield_expense = f"Yield expense: {round(results.OtherExpense[-1], 2)}"

    top_margin  = 0 - 1 / (height / 90)
    text_margin = 0 - 1 / (height / 30)

    fig.add_annotation(dict(font=dict(color='black',size=17), x=0, y=top_margin, showarrow=False, text=invested, xref="paper", yref="paper"))
    fig.add_annotation(dict(font=dict(color='black',size=17), x=0, y=top_margin+text_margin, showarrow=False, text=value, xref="paper", yref="paper"))
    fig.add_annotation(dict(font=dict(color='black',size=17), x=0, y=top_margin+text_margin*2, showarrow=False, text=profit, xref="paper", yref="paper"))
    fig.add_annotation(dict(font=dict(color='black',size=17), x=0, y=top_margin+text_margin*3, showarrow=False, text=yield_profit, xref="paper", yref="paper"))
    fig.add_annotation(dict(font=dict(color='black',size=17), x=0, y=top_margin+text_margin*4, showarrow=False, text=total_trades, xref="paper", yref="paper"))

    fig.add_annotation(dict(font=dict(color='black',size=17), x=0.25, y=top_margin, showarrow=False, text=expenses, xref="paper", yref="paper"))
    fig.add_annotation(dict(font=dict(color='black',size=17), x=0.25, y=top_margin+text_margin, showarrow=False, text=comm_expense, xref="paper", yref="paper"))
    fig.add_annotation(dict(font=dict(color='black',size=17), x=0.25, y=top_margin+text_margin*2, showarrow=False, text=spread_expense, xref="paper", yref="paper"))
    fig.add_annotation(dict(font=dict(color='black',size=17), x=0.25, y=top_margin+text_margin*3, showarrow=False, text=debt_expense, xref="paper", yref="paper"))
    fig.add_annotation(dict(font=dict(color='black',size=17), x=0.25, y=top_margin+text_margin*4, showarrow=False, text=yield_expense, xref="paper", yref="paper"))

# The project is intended to be used with GIL-free Python interpreters (like nogil-3.9.10). However, it is fully compatible with regular
# CPython but in such case there won't be any benefit related to parallel computing.
# multiprocessing.pool.Threadpool does not work well with nogil, concurrent.futures.ThreadPoolExecutor is too buggy in 3.9 and 3.10.
# That is why these thread pool implementations are not used.

def thread_available(timeout=0, gap=0.05):
    """
        Check if hardware thread available for calculations.

        Args:
            timeout(int): timeout in seconds to wait for a free thread.
            gap(float): gap for each timeout iteration.

        Returns:
            True if thread is available, False otherwise.
    """
    if multiprocessing.cpu_count() == 1:
        return False

    while threading.active_count() >= multiprocessing.cpu_count() and timeout > 0:
        time.sleep(gap)
        timeout -= gap

    if timeout > 0:
        return False

    return True
