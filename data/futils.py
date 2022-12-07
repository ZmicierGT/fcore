"""Module with general functions which are not related to any class.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

import configparser

from datetime import datetime
import pytz

import plotly.graph_objects as go

import os
from os.path import exists
import glob

from data import fvalues

import threading
import multiprocessing
import time
import platform
import subprocess

from data.fvalues import Quotes

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

def write_image(img):
    """
        Write plotly figure to a disk.

        Args:
            img(PIL.Image or go.Figure): Image to write.

        Returns:
            str: new file name.

        Raises:
            RuntimeError: can't generate a filename.
    """
    new_file = gen_image_path()

    if type(img).__name__ == "Figure":
        img.write_image(new_file)
    elif type(img).__name__ == "Image":
        img.save(new_file)
    else:
        raise RuntimeError(f"Unsupported image type: {type(img).__name__}")

    return new_file

def gen_image_path():
    """
        Generate a next sequential filename for an image.

        Returns:
            str: new image path.
    """
    img_dir = "images"

    if exists(img_dir) == False:
        os.mkdir(img_dir)

    files = glob.glob(os.path.join(img_dir, "fig_*.png"))

    files.sort(key=lambda x: int(x.partition('_')[2].partition('.')[0]))

    if len(files) == 0:
        last_file = 0
    else:
        last_file = files[-1]
        last_file = last_file.replace('.png', '').replace(os.path.join(img_dir, 'fig_'), '')
    
    try:
        new_counter = int(last_file) + 1
    except ValueError as e:
        raise RuntimeError(f"Can't generate new filename. {last_file} has a broken filename pattern.") from e

    new_file = os.path.join(img_dir, "fig_") + f"{new_counter}" + ".png"

    return new_file

def open_image(image_path):
    """
        Open image in the default image-viewer.

        Args:
            path(str): path to the opened image.
    """
    # Open image file in the default viewer.
    if platform.system() == 'Darwin':  # macOS
        subprocess.call(('open', image_path))
    elif platform.system() == 'Windows':
        os.startfile(image_path)
    else:  # Linux
        subprocess.call(('xdg-open', image_path))

def show_image(fig):
    """
        Write the image and open it in the system default image viewer.

        Returns:
            str: path to the image
    """
    image_path = write_image(fig)
    open_image(image_path)

    return image_path

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
