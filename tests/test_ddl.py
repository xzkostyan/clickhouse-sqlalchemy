from sqlalchemy import Column, exc
from sqlalchemy.sql.ddl import CreateTable

from src import types, engines
from src.ddl import DropTable
from src.schema import Table
from src.compat import unicode

from .testcase import BaseTestCase


class DDLTestCase(BaseTestCase):

    def test_create_table(self):
        table = Table('t1', self.metadata(),
                      Column('x', types.Int32, primary_key=True),
                      Column('y', types.String),
                      Column('z', types.String(10)),
                      engines.Memory())

        # No NOT NULL. And any PKS.
        expected = ('CREATE TABLE t1 (x Int32, y String, z FixedString(10)) '
                    'ENGINE = Memory')
        self.assertEqual(self.compile(CreateTable(table)), expected)

    def test_create_table_without_engine(self):
        no_engine_table = Table('t1', self.metadata(),
                                Column('x', types.Int32, primary_key=True),
                                Column('y', types.String))

        with self.assertRaises(exc.CompileError) as ex:
            self.compile(CreateTable(no_engine_table))

        self.assertEqual(unicode(ex.exception), "No engine for table 't1'")

    def test_drop_table(self):
        table = Table('t1', self.metadata(),
                      Column('x', types.Int32, primary_key=True))

        query = self.compile(DropTable(table))
        self.assertEqual(query, "DROP TABLE t1")

        query = self.compile(DropTable(table, if_exists=True))
        self.assertEqual(query, "DROP TABLE IF EXISTS t1")


class EnginesTestCase(DDLTestCase):

    def test_text_engine_columns(self):
        table = Table('t1', self.metadata(),
                      Column('date', types.Date, primary_key=True),
                      Column('x', types.Int32),
                      Column('y', types.String),
                      engines.MergeTree('date', ('date', 'x')))

        query = self.compile(CreateTable(table))
        expected = ('CREATE TABLE t1 (date Date, x Int32, y String) '
                    'ENGINE = MergeTree(date, (date, x), 8192)')
        self.assertEqual(query, expected)
