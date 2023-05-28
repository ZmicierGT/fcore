"""Module with general functions which are not related to any class.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from datetime import datetime, timedelta
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

def get_dt(value):
    """
        Get datetime from one of provided types: datetime, string, timestamp.

        Args:
            value(datetime, str, int): value to get datetime from.

        Raises:
            ValueError: unknown type or can't generate a datetime.

        Return:
            datetime: datetime obtained from the provided value.
    """
    # Timestamp
    if isinstance(value, int):
        try:
            if value < 0:
                dt = datetime(1970, 1, 1) + timedelta(seconds=value)
            else:
                dt = datetime.utcfromtimestamp(value)
        except (OverflowError, OSError) as e:
            raise ValueError(f"Too big/small timestamp value: {e}") from e
    # String
    elif isinstance(value, str):
        if len(value) <= 10:
            dt = datetime.strptime(value, '%Y-%m-%d')
        elif len(value) > 10 and len(value) <= 16:
            dt = datetime.strptime(value, '%Y-%m-%d %H:%M')
        else:
            dt = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
    # DateTime
    elif isinstance(value, datetime):
        dt = value
    # Unknown type
    else:
        raise ValueError(f"Unknown type provided for datetime: {type(value).__name__}")

    # Always keep datetimes in UTC time zone!
    dt = dt.replace(tzinfo=pytz.utc)

    return dt

def get_ts_from_str(value):
    """
        Get timestamp from datetime or datetime string representation.

        Args:
            value(datetime, str): datetime or datetime string representation.

        Raises:
            ValueError, OSError: incorrect representation provided.

        Returns:
            int: timestamp.
    """
    return int(get_dt(value).timestamp())

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
