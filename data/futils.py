"""Module with general functions which are not related to any class.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from data.fvalues import Quotes

from datetime import datetime, timedelta
from dateutil.parser import parse
from dateutil import tz

import os
from os.path import exists
import glob

import numpy as np

import threading
import multiprocessing
import time
import platform
import subprocess

def get_dt(value, timezone=tz.UTC):
    """
        Get datetime from one of provided types: datetime, string, timestamp.

        Args:
            value(datetime, str, int): value to get datetime from.
            timezone: the initial time zone.

        Raises:
            ValueError: unknown type or can't generate a datetime.

        Return:
            datetime: datetime obtained from the provided value.
    """
    # Timestamp
    if isinstance(value, int) or isinstance(value, np.int64):
        try:
            if value < 0:
                dt = datetime(1970, 1, 1) + timedelta(seconds=value)
            else:
                dt = datetime.utcfromtimestamp(value)
        except (OverflowError, OSError) as e:
            raise ValueError(f"Too big/small timestamp value: {e}") from e
    # String
    elif isinstance(value, str):
        dt = parse(value)
    # DateTime
    elif isinstance(value, datetime):
        dt = value
    # Unknown type
    else:
        raise ValueError(f"Unknown type provided for datetime: {type(value).__name__}")

    # Set the time zone
    dt = dt.replace(tzinfo=timezone)

    # UTC adjust the datetime
    dt = dt.astimezone(tz.UTC)

    # Remove time zone from UTC adjusted datetime
    dt = dt.replace(tzinfo=None)

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

def get_labelled_ndarray(rows):
    """
        Take a 2D list of sqlite3.Row and convert it to a labelled ndarray.
        Then it is possible to address this array like arr['date_time'].

        Regular list is preferred as it is fast for iterating and it is memory efficient but in some cases
        numpy arrays may be preferred (for example, if a lot of column-wise operations are supposed).

        Args:
            rows(list(sqlite3.Row)): the data to convert.

        Returns:
            ndarray: labelled ndarray.
    """
    # At first, get all column names and types
    key_types = []

    if len(rows) == 0:
        raise ValueError("Source data length is 0.")

    for key, value in dict(rows[0]).items():
        # Set Transactions dtype to object as not every data source have it.
        if isinstance(value, str) or key == 'transactions':  # TODO MID Maybe float for transactions?
            key_type = 'object'
        else:
            key_type = np.array([value]).dtype

        key_types.append((key, key_type))

    dtypes = np.dtype(key_types)

    # Create tuples of each row
    data = [tuple(row[name] for name in dtypes.names) for row in rows]

    return np.array(data, dtypes)

def add_column(rows, name, dtype=object, default=0.0):
    """
        Add column(s) to the labelled numpy array.

        Note that in the case of huge arrays sometimes this operation may be slow. In such case you may get the
        column in advance during data query. For example: source.get(columns='0.0 AS test')

        Args:
            rows(ndarray): the initial array.
            name(str): name of the column.
            dtype(numpy.dtype): dtype of the column.
            default: the default value

        Returns:
            The altered array with added column(s).
    """
    dt = rows.dtype.descr
    dt.append((name, dtype))
    dt = np.dtype(dt)

    col = (default, )

    rows = rows.astype(dtype=object)
    rows = [x + col for x in rows]

    rows = np.array(rows, dtype=dt)

    return rows

def delete_row(self, data, row_num):
    """
        Deletes a row from data.

        Args:
            data(ndarray): dataset to delete a row
            row_num(int): row number
    """
    try:
        self._rows = np.delete(data, row_num, 0)
    except IndexError as e:
        raise KeyError(f"Can not delete row {row_num} as it does not exist") from e

    return data

def trim_time(data, start=None, end=None):
    """
        Trim the time which is out of the time windows.
        For example, if start='13:30' and end='21:00' then all quotes which are outside of this window will be deleted
        from the dataset.
    """
    pick_time = np.vectorize(lambda x: datetime.utcfromtimestamp(x).time())

    if start is not None:
        start_time = datetime.strptime(start, '%H:%M').replace(tzinfo=tz.UTC).time()
        data = data[np.where(pick_time(data[Quotes.TimeStamp]) >= start_time)]

    if end is not None and len(data):
        end_time = datetime.strptime(end, '%H:%M').replace(tzinfo=tz.UTC).time()
        data = data[np.where(pick_time(data[Quotes.TimeStamp]) <= end_time)]

    return data

def logger(verbosity, message):
    """
        Depending on a verbosity flag, display a logging message.

        Args:
            verbosity(bool): verbosity flag.
            message(str): message.
    """
    if verbosity:
        print(message)

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
