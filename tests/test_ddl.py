from sqlalchemy import Column
from sqlalchemy.sql.ddl import CreateTable

from src import types, engines, Table
from src.sql.ddl import DropTable
from tests.testcase import BaseTestCase
from tests.session import mocked_engine


class DDLTestCase(BaseTestCase):
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

    def test_create_table_nullable(self):
        table = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            Column('y', types.Nullable(types.String)),
            Column('z', types.Nullable(types.String(10))),
            engines.Memory()
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 '
            '(x Int32, y Nullable(String), z Nullable(FixedString(10))) '
            'ENGINE = Memory'
        )

    def test_create_table_nested_nullable(self):
        table = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            Column('y', types.Array(types.Nullable(types.String))),
            engines.Memory()
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 '
            '(x Int32, y Array(Nullable(String))) '
            'ENGINE = Memory'
        )

    def test_create_table_nullable_nested_nullable(self):
        table = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            Column('y', types.Nullable(
                types.Array(types.Nullable(types.String)))
            ),
            engines.Memory()
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 '
            '(x Int32, y Nullable(Array(Nullable(String)))) '
            'ENGINE = Memory'
        )

    def test_drop_table_clause(self):
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

    def test_table_drop(self):
        table = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True)
        )

        with mocked_engine() as engine:
            table.drop(if_exists=True)
            engine.assert_sql(['DROP TABLE IF EXISTS t1'])
