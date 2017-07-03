from six import text_type
from sqlalchemy import Column, exc
from sqlalchemy.sql.ddl import CreateTable

from src import types, engines
from src.ddl import DropTable
from src.schema import Table
from tests.testcase import BaseTestCase


class DDLTestCase(BaseTestCase):
    def compile(self, clause, **kwargs):
        return self.strip_spaces.sub('', text_type(self._compile(clause)))

    def test_create_table(self):
        table = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            Column('y', types.String),
            Column('z', types.String(10)),
            engines.Memory()
        )

        # No NOT NULL. And any PKS.
        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 (x Int32, y String, z FixedString(10)) '
            'ENGINE = Memory'
        )

    def test_create_table_nested_types(self):
        table = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            Column('y', types.Array(types.String)),
            engines.Memory()
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 '
            '(x Int32, y Array(String)) '
            'ENGINE = Memory'
        )

        table = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            Column('y', types.Array(types.Array(types.String))),
            engines.Memory()
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 '
            '(x Int32, y Array(Array(String))) '
            'ENGINE = Memory'
        )

        table = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            Column('y', types.Array(types.Array(types.String))),
            engines.Memory()
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 '
            '(x Int32, y Array(Array(String))) '
            'ENGINE = Memory'
        )

    def test_create_table_without_engine(self):
        no_engine_table = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            Column('y', types.String)
        )

        with self.assertRaises(exc.CompileError) as ex:
            self.compile(CreateTable(no_engine_table))

        self.assertEqual(text_type(ex.exception), "No engine for table 't1'")

    def test_drop_table(self):
        table = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True)
        )

        self.assertEqual(
            self.compile(DropTable(table)),
            'DROP TABLE t1'
        )
        self.assertEqual(
            self.compile(DropTable(table, if_exists=True)),
            'DROP TABLE IF EXISTS t1'
        )


class EnginesTestCase(DDLTestCase):
    def test_text_engine_columns(self):
        table = Table(
            't1', self.metadata(),
            Column('date', types.Date, primary_key=True),
            Column('x', types.Int32),
            Column('y', types.String),
            engines.MergeTree('date', ('date', 'x')),
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 (date Date, x Int32, y String) '
            'ENGINE = MergeTree(date, (date, x), 8192)'
        )
