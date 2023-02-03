import unittest
from mockito import when, mock, verify, unstub

import sys
sys.path.append('../')

import sqlite3
from sqlite3 import Cursor, Connection

from data import fdata, fdatabase

class Test(unittest.TestCase):
    def setUp(self):
        self.source = fdata.ReadOnlyData()
        self.db = fdatabase.SQLiteConn(self.source)

        self.source.conn = mock(Connection)
        self.source.cur = mock(Cursor)

    def tearDown(self):
        unstub()

    def test_0_check_sqlite_connect(self):
        when(sqlite3).connect(self.source.db_name).thenReturn(self.source.conn)
        when(self.source.conn).cursor().thenReturn(self.source.cur)

        self.db.db_connect()

        verify(sqlite3, times=1).connect(self.source.db_name)
        verify(self.source.conn, times=1).cursor()
