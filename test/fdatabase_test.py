import unittest
from mockito import when, mock, verify, unstub

import sys
sys.path.append('../')

import sqlite3
from sqlite3 import Cursor, Connection

from data import fdata, fdatabase

class Test(unittest.TestCase):
    def setUp(self):
        self.query = fdata.Query()
        self.db = fdatabase.SQLiteConn(self.query)

        self.query.conn = mock(Connection)
        self.query.cur = mock(Cursor)

    def tearDown(self):
        unstub()

    def test_0_check_sqlite_connect(self):
        when(sqlite3).connect(self.query.db_name).thenReturn(self.query.conn)
        when(self.query.conn).cursor().thenReturn(self.query.cur)

        self.db.db_connect()

        verify(sqlite3, times=1).connect(self.query.db_name)
        verify(self.query.conn, times=1).cursor()
