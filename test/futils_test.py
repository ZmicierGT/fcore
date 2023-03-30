import unittest
#from unittest import mock
from mockito import when, when2, mock, verify, unstub, ANY

import sys
sys.path.append('../')

import configparser

import plotly.graph_objects as go
from plotly.subplots import make_subplots
from plotly import subplots

import os
import platform
import subprocess
import glob

from data import futils, fdata, fvalues

from keras.models import Sequential

import threading
import multiprocessing

from backtest.base import BTData
from backtest.base import BTSymbol

from data.fdata import FdataError

from datetime import datetime

import numpy as np

from PIL import Image

layout = {
            'height': 2000,
            'template': '...',
            'xaxis': {'anchor': 'y', 'domain': [0.0, 1.0], 'matches': 'x3', 'showticklabels': False},
            'xaxis2': {'anchor': 'y2', 'domain': [0.0, 1.0], 'matches': 'x3', 'showticklabels': False},
            'xaxis3': {'anchor': 'y3', 'domain': [0.0, 1.0]},
            'yaxis': {'anchor': 'x', 'domain': [0.6799999999999999, 1.0]},
            'yaxis2': {'anchor': 'x2', 'domain': [0.33999999999999997, 0.58]},
            'yaxis3': {'anchor': 'x3', 'domain': [0.0, 0.24]}
         }

def create_results():
    results = BTData()

    results.Data = np.array([['1', 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
                             ['2', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                             ['3', 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
                            ])

    symbol = BTSymbol()

    symbol.Data = np.array([[100, 2, 3, 4, 5, 6, 7, 8],
                            [200, 3, 4, 5, 6, 7, 8, 9],
                            [300, 4, 5, 6, 7, 8, 9, 10]
                           ])

    results.Symbols = [symbol]

    symbol.Title = ["Test_Title"]

    return results

class Test(unittest.TestCase):
    def tearDown(self):
        unstub()

    def test_0_get_dt(self):
        date1 = "1800-01-01"  # Passes
        date2 = "1900-01-01"  # Passes
        date3 = "2000-01-01"  # Passes
        date4 = "2040-01-01"  # Passes

        date5 = "2020-01-40"  # Fails

        datetime1 = "2020-01-20 12:55"  # Passes
        datetime2 = "2022-10-10 23:59:59"  # Passes
        datetime3 = "2022-10-10 23:59:79"  # Fails

        ts1 = 1  # Passes
        ts2 = 999999999999999999999  # Fails
        ts3 = -999999999999999999999  # Fails

        dt1 = futils.get_dt(date1)
        dt2 = futils.get_dt(date2)
        dt3 = futils.get_dt(date3)
        dt4 = futils.get_dt(date4)

        self.assertRaises(ValueError, futils.get_dt, date5)

        dt6 = futils.get_dt(datetime1)
        dt7 = futils.get_dt(datetime2)
        self.assertRaises(ValueError, futils.get_dt, datetime3)

        dt8 = futils.get_dt(ts1)
        self.assertRaises(ValueError, futils.get_dt, ts2)
        self.assertRaises(ValueError, futils.get_dt, ts3)

        self.assertEqual(int(dt1.timestamp()), -5364662400)
        self.assertEqual(int(dt2.timestamp()), -2208988800)
        self.assertEqual(int(dt3.timestamp()), 946684800)
        self.assertEqual(int(dt4.timestamp()), 2208988800)

        self.assertEqual(str(dt6), '2020-01-20 12:55:00+00:00')
        self.assertEqual(str(dt7), '2022-10-10 23:59:59+00:00')

        self.assertEqual(int(dt8.timestamp()), 1)

    def test_1_check_get_ts_from_str(self):
        date1 = "1800-01-01"  # Passes
        date2 = "2040-01-01"  # Passes
        date3 = "2020-01-40"  # Fails

        datetime1 = "2020-01-20 12:55"  # Passes
        datetime2 = "2022-10-10 23:59:59"  # Passes
        datetime3 = "2022-10-10 23:59:79"  # Fails

        ts1 = futils.get_ts_from_str(date1)
        ts2 = futils.get_ts_from_str(date2)
        self.assertRaises(ValueError, futils.get_ts_from_str, date3)

        ts3 = futils.get_ts_from_str(datetime1)
        ts4 = futils.get_ts_from_str(datetime2)
        self.assertRaises(ValueError, futils.get_ts_from_str, datetime3)

        self.assertEqual(ts1, -5364662400)
        self.assertEqual(ts2, 2208988800)
        self.assertEqual(ts3, 1579524900)
        self.assertEqual(ts4, 1665446399)

    def test_10_write_image(self):
        img1 = go.Figure()
        img2 = Image.new('RGB', (50, 50))
        img3 = "unsupported"

        image_path = "/home/user/Pictures/1.png"

        self.assertRaises(RuntimeError, futils.write_image, img3)

        when(futils).gen_image_path().thenReturn(image_path)

        when(img1).write_image(image_path).thenReturn()
        when(img2).save(image_path).thenReturn()

        result1 = futils.write_image(img1)

        assert image_path == result1

        verify(img1, times=1).write_image(image_path)

        result2 = futils.write_image(img2)

        verify(img2, times=1).save(image_path)

        assert image_path == result2

    def test_4_gen_image_path(self):
        img_dir = "images"
        file_mask = "fig_*.png"
        expected_file = os.path.join(img_dir, "fig_1.png")
        files = []

        when(futils).exists(img_dir).thenReturn(True)
        when(glob).glob(os.path.join(img_dir, file_mask)).thenReturn(files)

        new_file = futils.gen_image_path()

        verify(futils, times=1).exists(img_dir)
        verify(glob, times=1).glob(os.path.join(img_dir, file_mask))

        assert new_file == expected_file

    def test_11_open_image(self):
        image_path = "/home/user/Pictures/1.png"

        if platform.system() == 'Darwin':  # macOS
            when(subprocess).call(('open', image_path)).thenReturn()
        elif platform.system() == 'Windows':
            when(os).startfile(image_path).thenReturn()
        else:  # Linux
            when(subprocess).call(('xdg-open', image_path)).thenReturn()

        futils.open_image(image_path)

        if platform.system() == 'Darwin':  # macOS
            verify(subprocess, times=1).call(('open', image_path))
        elif platform.system() == 'Windows':
            verify(os, times=1).startfile(image_path)
        else:  # Linux
            verify(subprocess, times=1).call(('xdg-open', image_path))

    def test_12_show_image(self):
        image_path = "/home/user/Pictures/1.png"
        fig = go.Figure()

        when(futils).write_image(fig).thenReturn(image_path)
        when(futils).open_image(image_path).thenReturn()

        result = futils.show_image(fig)

        verify(futils, times=1).write_image(fig)
        verify(futils, times=1).open_image(image_path)

        assert image_path == result

    def test_5_write_model(self):
        model_dir = "models"
        name = "LSTM"
        file_mask = f"{name}_*"
        expected_file = "LSTM_1"
        files = []

        model = mock(Sequential())

        when(futils).exists(model_dir).thenReturn(True)
        when(os).chdir(model_dir).thenReturn()
        when(glob).glob(file_mask).thenReturn(files)
        when(model).save(expected_file).thenReturn()

        new_file = futils.write_model(name, model)

        verify(futils, times=1).exists(model_dir)
        verify(os, times=1).chdir(model_dir)
        verify(glob, times=1).glob(file_mask)
        verify(model).save(expected_file)

        assert new_file == expected_file

    def test_6_build_chart(self):
        expected_file = "fig_1.png"

        test_list = [
            {
                0:1,
                4:2
            },
            {
                0:3,
                4:4
            },
            {
                0:5,
                4:6
            }
        ]

        fig_arg = [go.Scatter({
                        'x': [1, 3, 5], 'y': [2, 4, 6]
                  })]

        update_args = dict(autosize=False,
            width=1500,
            height=900,
            margin=dict(
                l=50,
                r=50,
                b=100,
                t=100,
                pad=4
            ),
            paper_bgcolor="LightSteelBlue")

        fig = go.Figure(fig_arg)

        when(futils.go).Figure(fig_arg).thenReturn(fig)
        when(fig).update_layout(**update_args).thenReturn()
        when(futils).write_image(fig).thenReturn(expected_file)

        new_file = futils.build_chart(test_list)

        assert new_file == expected_file

    def test_7_get_dt_offset(self):
        test_rows = [['1986-12-30 23:59:59'],
                     ['1986-12-31 23:59:59'],
                     ['1987-01-02 23:59:59']]

        dt_offset = futils.get_dt_offset(test_rows, '1986-12-31 23:59:59')

        assert dt_offset == 1

    def test_8_update_layout(self):
        fig = mock(go.Figure())

        update_args = dict(
            title_text='test',
            autosize=False,
            width=2500,
            height=1000,
            margin=dict(
                l=50,
                r=50,
                b=200,
                t=100,
                pad=4
            ),
            paper_bgcolor="LightSteelBlue")

        when(fig).update_layout(**update_args).thenReturn()

        futils.update_layout(fig, 'test', 100)

        verify(fig).update_layout(**update_args)

##########################
# Multithreading functions
##########################

    def test_9_thread_available(self):
        when(multiprocessing).cpu_count().thenReturn(4)
        when(threading).active_count().thenReturn(2)

        flag = futils.thread_available()

        verify(multiprocessing, times=2).cpu_count()
        verify(threading).active_count()

        assert flag == True
