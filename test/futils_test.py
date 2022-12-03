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
from os import path
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

    def test_0_check_date(self):
        date1 = "1800-01-01" # Passes
        date2 = "1900-01-01" # Passes
        date3 = "2000-01-01" # Passes
        date4 = "2040-01-01" # Passes

        date5 = "2020-01-40" # Fails
        date6 = "2020-01-40 12:55" # Fails

        res1, ts1 = futils.check_date(date1)
        res2, ts2 = futils.check_date(date2)
        res3, ts3 = futils.check_date(date3)
        res4, ts4 = futils.check_date(date4)

        res5, ts5 = futils.check_date(date5)
        res6, ts6 = futils.check_date(date6)

        self.assertTrue(res1)
        self.assertTrue(res2)
        self.assertTrue(res3)
        self.assertTrue(res4)

        self.assertEqual(ts1, -5364662400)
        self.assertEqual(ts2, -2208988800)
        self.assertEqual(ts3, 946684800)
        self.assertEqual(ts4, 2208988800)

        self.assertFalse(res5)
        self.assertFalse(res6)

        self.assertEqual(ts5, 0)
        self.assertEqual(ts6, 0)

    def test_20_check_datetime(self):
        datetime1 = "2022-10-10 23:59:59" # Passes
        datetime2 = "2022-10-10 23:59:79" # Fails
        datetime3 = "2022-40-10 23:59:59" # Fails
        datetime4 = "2000-01-01" # Passes

        res1, ts1 = futils.check_datetime(datetime1)
        res2, ts2 = futils.check_datetime(datetime2)
        res3, ts3 = futils.check_datetime(datetime3)
        res4, ts4 = futils.check_datetime(datetime4)

        self.assertTrue(res1)
        self.assertFalse(res2)
        self.assertFalse(res3)
        self.assertTrue(res4)

        self.assertEqual(ts2, 0)
        self.assertEqual(ts3, 0)

        self.assertEqual(ts1, 1665446399)
        self.assertEqual(ts4, 946684800)

    def test_21_check_get_datetime(self):
        dt_str1 = "2022-10-10 23:59:59" # Passes
        dt_str2 = "2022-10-10 23:59:79" # Fails
        dt_str3 = "2022-40-10 23:59:59" # Fails
        dt_str4 = "2000-01-01" # Fails

        dt1 = futils.get_datetime(dt_str1)

        self.assertRaises(ValueError, futils.get_datetime, dt_str2)
        self.assertRaises(ValueError, futils.get_datetime, dt_str3)
        self.assertRaises(ValueError, futils.get_datetime, dt_str4)

        self.assertEqual(dt1, datetime(2022, 10, 10, 23, 59, 59))

    def test_1_check_parse_config(self):
        parser = mock(configparser.ConfigParser)
        ini_file = "settings.ini"

        query = fdata.Query()
        query.source_title = "test"

        expected_db_name = 'test.sqlite'
        expected_db_type = 'sqlite'

        settings = {
            'db_name': expected_db_name,
            'db_type': expected_db_type
        }

        when(configparser).ConfigParser().thenReturn(parser)
        when(parser).read(ini_file).thenReturn()
        when(parser).__getitem__(query.source_title).thenReturn(settings)

        futils.parse_config(query)

        verify(parser, times=1).read(ini_file)

        assert query.db_name == expected_db_name
        assert query.db_type == expected_db_type

    def test_2_write_image(self):
        img_dir = "images"
        file_mask = "fig_*.png"
        expected_file = "fig_1.png"
        files = []

        fig = mock(go.Figure())

        when(futils).exists(img_dir).thenReturn(True)
        when(os).chdir(img_dir).thenReturn()
        when(glob).glob(file_mask).thenReturn(files)
        when(fig).write_image(expected_file).thenReturn()

        new_file = futils.write_image(fig)

        verify(futils, times=1).exists(img_dir)
        verify(os, times=1).chdir(img_dir)
        verify(glob, times=1).glob(file_mask)
        verify(fig).write_image(expected_file)

        assert new_file == expected_file

    def test_4_write_model(self):
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

    def test_3_build_chart(self):
        expected_file = "fig_1.png"

        test_list = [
            {
                3:1,
                9:2
            },
            {
                3:3,
                9:4
            },
            {
                3:5,
                9:6
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

    def test_5_get_dt_offset(self):
        test_rows = [['AAPL', None, 'YF', '1986-12-30 23:59:59', 'Day', 0.180804, 0.185268, 0.180246, None, 0.183036, 148153600, None, None, None],
                     ['AAPL', None, 'YF', '1986-12-31 23:59:59', 'Day', 0.183036, 0.18471, 0.180246, None, 0.180804, 132563200, None, None, None],
                     ['AAPL', None, 'YF', '1987-01-02 23:59:59', 'Day', 0.180246, 0.183594, 0.179129, None, 0.182478, 120870400, None, None, None]]

        dt_offset = futils.get_dt_offset(test_rows, '1986-12-31 23:59:59')

        assert dt_offset == 1

##########################################
# Functions related to backtest reporting.
##########################################

    def test_6_update_layout(self):
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

    def test_7_adjust_trades(self):
        data = BTData()
        symbol = BTSymbol()

        data.Symbols = [symbol]

        # Lets fill the arrays in two different ways.
        data.append([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])
        data.append([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2])
        data.append([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2])
        data.append([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3])

        data.Symbols[0].Data = np.array([[0, None, 100, 0, 0, 0, 0, 1],
                                         [0, 200, None, 0, 0, 0, 0, 2],
                                         [0, None, None, None, 0, 0, 0, 2],
                                         [0, 300, None, 0, 0, 0, 0, 4],
                                        ], dtype='object')


        futils.adjust_trades(data)

        assert data.TotalTrades[0] == 1
        assert data.Symbols[0].TradesNo[0] == 1

        assert data.TotalTrades[1] == 2
        assert data.Symbols[0].TradesNo[1] == 2

        assert np.isnan(data.TotalTrades[2]) == True
        assert np.isnan(data.Symbols[0].TradesNo[2]) == True

        assert data.TotalTrades[3] == 3
        assert data.Symbols[0].TradesNo[3] == 4

    def test_8_main_chart(self):
        results = create_results()

        fig = mock(go.Figure())

        first_args = go.Scatter(x=results.DateTime, y=results.Symbols[0].Close, mode='lines', name=f'Quotes {results.Symbols[0].Title}')
        second_args = go.Scatter(x=results.DateTime, y=results.Symbols[0].TradePriceLong, mode='markers', name=f'Trades {results.Symbols[0].Title}')

        num_main = 1

        when(fig).add_trace(first_args, row=num_main, col=1, secondary_y=False).thenReturn()
        when(fig).add_trace(second_args, row=num_main, col=1, secondary_y=False).thenReturn()

        futils.main_chart(results, fig)

        verify(fig).add_trace(first_args, row=num_main, col=1, secondary_y=False)
        verify(fig).add_trace(second_args, row=num_main, col=1, secondary_y=False)

    def test_9_main_margin_chart(self):
        results = create_results()

        fig = mock(go.Figure())

        first_args = go.Scatter(x=results.DateTime, y=results.Symbols[0].Close, mode='lines', name=f'Quotes {results.Symbols[0].Title}')
        second_args = go.Scatter(x=results.DateTime, y=results.Symbols[0].TradePriceLong, mode='markers', name=f'Long Trades {results.Symbols[0].Title}')
        third_args = go.Scatter(x=results.DateTime, y=results.Symbols[0].TradePriceShort, mode='markers', name=f'Short Trades {results.Symbols[0].Title}')
        fourth_args = go.Scatter(x=results.DateTime, y=results.Symbols[0].TradePriceMargin, mode='markers', name=f'Margin Req Trades {results.Symbols[0].Title}')

        when(fig).add_trace(first_args, row=1, col=1, secondary_y=False).thenReturn()
        when(fig).add_trace(second_args, row=1, col=1, secondary_y=False).thenReturn()
        when(fig).add_trace(third_args, row=1, col=1, secondary_y=False).thenReturn()
        when(fig).add_trace(fourth_args, row=1, col=1, secondary_y=False).thenReturn()

        futils.main_margin_chart(results, fig)

        verify(fig).add_trace(first_args, row=1, col=1, secondary_y=False)
        verify(fig).add_trace(second_args, row=1, col=1, secondary_y=False)
        verify(fig).add_trace(third_args, row=1, col=1, secondary_y=False)
        verify(fig).add_trace(fourth_args, row=1, col=1, secondary_y=False)

    def test_10_value_chart(self):
        results = create_results()

        fig = mock(go.Figure())

        num = 2

        first_args = go.Scatter(x=results.DateTime, y=results.TotalValue, mode='lines', name="Total Value")
        second_args = go.Scatter(x=results.DateTime, y=results.Deposits, mode='lines', name="Deposits")
        third_args = go.Scatter(x=results.DateTime, y=results.OtherProfit, mode='lines', name="Dividends")

        fig.layout = layout

        when(fig).add_trace(first_args, row=num, col=1).thenReturn()
        when(fig).add_trace(second_args, row=num, col=1).thenReturn()
        when(fig).add_trace(third_args, row=num, col=1).thenReturn()

        futils.value_chart(results, fig)

        verify(fig).add_trace(first_args, row=num, col=1)
        verify(fig).add_trace(second_args, row=num, col=1)
        verify(fig).add_trace(third_args, row=num, col=1)

    def test_11_expenses_chart(self):
        results = create_results()

        fig = mock(go.Figure())

        num = 3

        first_args = go.Scatter(x=results.DateTime, y=results.TotalExpenses, mode='lines', name="Expenses")
        second_args = go.Scatter(x=results.DateTime, y=results.CommissionExpense, mode='lines', name="Commission")
        third_args = go.Scatter(x=results.DateTime, y=results.SpreadExpense, mode='lines', name="Spread")

        fig.layout = layout

        when(fig).add_trace(first_args, row=num, col=1).thenReturn()
        when(fig).add_trace(second_args, row=num, col=1).thenReturn()
        when(fig).add_trace(third_args, row=num, col=1).thenReturn()

        futils.expenses_chart(results, fig)

        verify(fig).add_trace(first_args, row=num, col=1)
        verify(fig).add_trace(second_args, row=num, col=1)
        verify(fig).add_trace(third_args, row=num, col=1)

    def test_12_expenses_margin_chart(self):
        results = create_results()

        fig = mock(go.Figure())

        num = 3

        first_args = go.Scatter(x=results.DateTime, y=results.TotalExpenses, mode='lines', name="Expenses")
        second_args = go.Scatter(x=results.DateTime, y=results.CommissionExpense, mode='lines', name="Commission")
        third_args = go.Scatter(x=results.DateTime, y=results.SpreadExpense, mode='lines', name="Spread")
        fourth_args = go.Scatter(x=results.DateTime, y=results.DebtExpense, mode='lines', name="Margin Expenses")
        fifth_args = go.Scatter(x=results.DateTime, y=results.OtherExpense, mode='lines', name="Yield Expenses")

        fig.layout = layout

        when(fig).add_trace(first_args, row=num, col=1).thenReturn()
        when(fig).add_trace(second_args, row=num, col=1).thenReturn()
        when(fig).add_trace(third_args, row=num, col=1).thenReturn()
        when(fig).add_trace(fourth_args, row=num, col=1).thenReturn()
        when(fig).add_trace(fifth_args, row=num, col=1).thenReturn()

        futils.expenses_margin_chart(results, fig)

        verify(fig).add_trace(first_args, row=num, col=1)
        verify(fig).add_trace(second_args, row=num, col=1)
        verify(fig).add_trace(third_args, row=num, col=1)
        verify(fig).add_trace(fourth_args, row=num, col=1)
        verify(fig).add_trace(fifth_args, row=num, col=1)

    def test_13_get_charts_num(self):
        fig = mock(go.Figure())

        fig.layout = layout

        assert futils.get_charts_num(fig) == 3

    def test_14_create_standard_fig(self):
        args = dict(rows=3, cols=1, shared_xaxes=True, row_width=[0.3, 0.3, 0.4],
                         specs=[[{"secondary_y": True}],
                                [{"secondary_y": True}],
                                [{"secondary_y": False}]])

        when(subplots).make_subplots(**args).thenReturn()

        futils.create_standard_fig()

        verify(subplots).make_subplots(**args)

    def test_15_standard_chart(self):
        results = create_results()
        title = 'Test_Title'
        fig = mock(go.Figure())

        update_args = dict(
            title_text=title,
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
        when(fig).add_annotation(ANY).thenReturn()

        first_main_args = go.Scatter(x=results.DateTime, y=results.Symbols[0].Close, mode='lines', name=f'Quotes {results.Symbols[0].Title}')
        second_main_args = go.Scatter(x=results.DateTime, y=results.Symbols[0].TradePriceLong, mode='markers', name=f'Trades {results.Symbols[0].Title}')

        num_main = 1

        when(fig).add_trace(first_main_args, row=num_main, col=1, secondary_y=False).thenReturn()
        when(fig).add_trace(second_main_args, row=num_main, col=1, secondary_y=False).thenReturn()

        first_val_args = go.Scatter(x=results.DateTime, y=results.TotalValue, mode='lines', name="Total Value")
        second_val_args = go.Scatter(x=results.DateTime, y=results.Deposits, mode='lines', name="Deposits")
        third_val_args = go.Scatter(x=results.DateTime, y=results.OtherProfit, mode='lines', name="Dividends")

        num_val = 2

        when(fig).add_trace(first_val_args, row=num_val, col=1).thenReturn()
        when(fig).add_trace(second_val_args, row=num_val, col=1).thenReturn()
        when(fig).add_trace(third_val_args, row=num_val, col=1).thenReturn()

        num_exp = 3

        first_exp_args = go.Scatter(x=results.DateTime, y=results.TotalExpenses, mode='lines', name="Expenses")
        second_exp_args = go.Scatter(x=results.DateTime, y=results.CommissionExpense, mode='lines', name="Commission")
        third_exp_args = go.Scatter(x=results.DateTime, y=results.SpreadExpense, mode='lines', name="Spread")

        when(fig).add_trace(first_exp_args, row=num_exp, col=1).thenReturn()
        when(fig).add_trace(second_exp_args, row=num_exp, col=1).thenReturn()
        when(fig).add_trace(third_exp_args, row=num_exp, col=1).thenReturn()

        fig.layout = layout

        futils.standard_chart(results, title, fig)

        verify(fig).add_trace(first_main_args, row=num_main, col=1, secondary_y=False)
        verify(fig).add_trace(second_main_args, row=num_main, col=1, secondary_y=False)

        verify(fig).add_trace(first_val_args, row=num_val, col=1)
        verify(fig).add_trace(second_val_args, row=num_val, col=1)
        verify(fig).add_trace(third_val_args, row=num_val, col=1)

        verify(fig).add_trace(first_exp_args, row=num_exp, col=1)
        verify(fig).add_trace(second_exp_args, row=num_exp, col=1)
        verify(fig).add_trace(third_exp_args, row=num_exp, col=1)

    def test_16_standard_margin_chart(self):
        results = create_results()
        title = 'Test_Title'
        fig = mock(go.Figure())

        update_args = dict(
            title_text=title,
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
        when(fig).add_annotation(ANY).thenReturn()

        first_main_args = go.Scatter(x=results.DateTime, y=results.Symbols[0].Close, mode='lines', name=f'Quotes {results.Symbols[0].Title}')
        second_main_args = go.Scatter(x=results.DateTime, y=results.Symbols[0].TradePriceLong, mode='markers', name=f'Long Trades {results.Symbols[0].Title}')
        third_main_args = go.Scatter(x=results.DateTime, y=results.Symbols[0].TradePriceShort, mode='markers', name=f'Short Trades {results.Symbols[0].Title}')
        fourth_main_args = go.Scatter(x=results.DateTime, y=results.Symbols[0].TradePriceMargin, mode='markers', name=f'Margin Req Trades {results.Symbols[0].Title}')

        num_main = 1

        when(fig).add_trace(first_main_args, row=num_main, col=1, secondary_y=False).thenReturn()
        when(fig).add_trace(second_main_args, row=num_main, col=1, secondary_y=False).thenReturn()
        when(fig).add_trace(third_main_args, row=num_main, col=1, secondary_y=False).thenReturn()
        when(fig).add_trace(fourth_main_args, row=num_main, col=1, secondary_y=False).thenReturn()

        first_val_args = go.Scatter(x=results.DateTime, y=results.TotalValue, mode='lines', name="Total Value")
        second_val_args = go.Scatter(x=results.DateTime, y=results.Deposits, mode='lines', name="Deposits")
        third_val_args = go.Scatter(x=results.DateTime, y=results.OtherProfit, mode='lines', name="Dividends")

        num_val = 2

        when(fig).add_trace(first_val_args, row=num_val, col=1).thenReturn()
        when(fig).add_trace(second_val_args, row=num_val, col=1).thenReturn()
        when(fig).add_trace(third_val_args, row=num_val, col=1).thenReturn()

        num_exp = 3

        first_exp_args = go.Scatter(x=results.DateTime, y=results.TotalExpenses, mode='lines', name="Expenses")
        second_exp_args = go.Scatter(x=results.DateTime, y=results.CommissionExpense, mode='lines', name="Commission")
        third_exp_args = go.Scatter(x=results.DateTime, y=results.SpreadExpense, mode='lines', name="Spread")
        fourth_exp_args = go.Scatter(x=results.DateTime, y=results.DebtExpense, mode='lines', name="Margin Expenses")
        fifth_exp_args = go.Scatter(x=results.DateTime, y=results.OtherExpense, mode='lines', name="Yield Expenses")

        when(fig).add_trace(first_exp_args, row=num_exp, col=1).thenReturn()
        when(fig).add_trace(second_exp_args, row=num_exp, col=1).thenReturn()
        when(fig).add_trace(third_exp_args, row=num_exp, col=1).thenReturn()
        when(fig).add_trace(fourth_exp_args, row=num_exp, col=1).thenReturn()
        when(fig).add_trace(fifth_exp_args, row=num_exp, col=1).thenReturn()

        fig.layout = layout

        futils.standard_margin_chart(results, title, fig)

        verify(fig).add_trace(first_main_args, row=num_main, col=1, secondary_y=False)
        verify(fig).add_trace(second_main_args, row=num_main, col=1, secondary_y=False)
        verify(fig).add_trace(third_main_args, row=num_main, col=1, secondary_y=False)
        verify(fig).add_trace(fourth_main_args, row=num_main, col=1, secondary_y=False)

        verify(fig).add_trace(first_val_args, row=num_val, col=1)
        verify(fig).add_trace(second_val_args, row=num_val, col=1)
        verify(fig).add_trace(third_val_args, row=num_val, col=1)

        verify(fig).add_trace(first_exp_args, row=num_exp, col=1)
        verify(fig).add_trace(second_exp_args, row=num_exp, col=1)
        verify(fig).add_trace(third_exp_args, row=num_exp, col=1)
        verify(fig).add_trace(fourth_exp_args, row=num_exp, col=1)
        verify(fig).add_trace(fifth_exp_args, row=num_exp, col=1)

    def test_17_prepare_chart(self):
        results = create_results()
        title = 'Test_Title'
        fig = mock(go.Figure())

        update_args = dict(
            title_text=title,
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
        when(fig).add_annotation(ANY).thenReturn()

        fig.layout = layout

        futils.prepare_chart(results, title, fig)

    def test_18_add_annotations(self):
        results = create_results()
        fig = mock(go.Figure())

        fig.layout = layout

        height = fig.layout['height']

        top_margin  = 0 - 1 / (height / 90)
        text_margin = 0 - 1 / (height / 30)

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

        first_main_arg = dict(font=dict(color='black',size=17), x=0, y=top_margin, showarrow=False, text=invested, xref="paper", yref="paper")
        second_main_arg = dict(font=dict(color='black',size=17), x=0, y=top_margin+text_margin, showarrow=False, text=value, xref="paper", yref="paper")
        third_main_arg = dict(font=dict(color='black',size=17), x=0, y=top_margin+text_margin*2, showarrow=False, text=profit, xref="paper", yref="paper")
        fourth_main_arg = dict(font=dict(color='black',size=17), x=0, y=top_margin+text_margin*3, showarrow=False, text=yield_profit, xref="paper", yref="paper")
        fifth_main_arg = dict(font=dict(color='black',size=17), x=0, y=top_margin+text_margin*4, showarrow=False, text=total_trades, xref="paper", yref="paper")

        first_exp_arg = dict(font=dict(color='black',size=17), x=0.25, y=top_margin, showarrow=False, text=expenses, xref="paper", yref="paper")
        second_exp_arg = dict(font=dict(color='black',size=17), x=0.25, y=top_margin+text_margin, showarrow=False, text=comm_expense, xref="paper", yref="paper")
        third_exp_arg = dict(font=dict(color='black',size=17), x=0.25, y=top_margin+text_margin*2, showarrow=False, text=spread_expense, xref="paper", yref="paper")
        fourth_exp_arg = dict(font=dict(color='black',size=17), x=0.25, y=top_margin+text_margin*3, showarrow=False, text=debt_expense, xref="paper", yref="paper")
        fifth_exp_arg = dict(font=dict(color='black',size=17), x=0.25, y=top_margin+text_margin*4, showarrow=False, text=yield_expense, xref="paper", yref="paper")

        when(fig).add_annotation(first_main_arg).thenReturn()
        when(fig).add_annotation(second_main_arg).thenReturn()
        when(fig).add_annotation(third_main_arg).thenReturn()
        when(fig).add_annotation(fourth_main_arg).thenReturn()
        when(fig).add_annotation(fifth_main_arg).thenReturn()

        when(fig).add_annotation(first_exp_arg).thenReturn()
        when(fig).add_annotation(second_exp_arg).thenReturn()
        when(fig).add_annotation(third_exp_arg).thenReturn()
        when(fig).add_annotation(fourth_exp_arg).thenReturn()
        when(fig).add_annotation(fifth_exp_arg).thenReturn()

        futils.add_annotations(results, fig)

        verify(fig).add_annotation(first_main_arg)
        verify(fig).add_annotation(second_main_arg)
        verify(fig).add_annotation(third_main_arg)
        verify(fig).add_annotation(fourth_main_arg)
        verify(fig).add_annotation(fifth_main_arg)

        verify(fig).add_annotation(first_exp_arg)
        verify(fig).add_annotation(second_exp_arg)
        verify(fig).add_annotation(third_exp_arg)
        verify(fig).add_annotation(fourth_exp_arg)
        verify(fig).add_annotation(fifth_exp_arg)

##########################
# Multithreading functions
##########################

    def test_19_thread_available(self):
        when(multiprocessing).cpu_count().thenReturn(4)
        when(threading).active_count().thenReturn(2)

        flag = futils.thread_available()

        verify(multiprocessing, times=2).cpu_count()
        verify(threading).active_count()

        assert flag == True
