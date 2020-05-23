from decimal import Decimal

from sqlalchemy import Column, Numeric
from sqlalchemy.sql.ddl import CreateTable

from clickhouse_sqlalchemy import types, engines, Table
from clickhouse_sqlalchemy.exceptions import DatabaseException
from tests.testcase import (
    BaseTestCase, CompilationTestCase,
    HttpSessionTestCase, NativeSessionTestCase
)


class NumericCompilationTestCase(CompilationTestCase):
    table = Table(
        'test', CompilationTestCase.metadata(),
        Column('x', Numeric(10, 2)),
        engines.Memory()
    )

    def test_create_table(self):
        self.assertEqual(
            self.compile(CreateTable(self.table)),
            'CREATE TABLE test (x Decimal(10, 2)) ENGINE = Memory'
        )

    def test_create_table_decimal_symlink(self):
        table = Table(
            'test', CompilationTestCase.metadata(),
            Column('x', types.Decimal(10, 2)),
            engines.Memory()
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE test (x Decimal(10, 2)) ENGINE = Memory'
        )


class NumericTestCase(BaseTestCase):
    table = Table(
        'test', BaseTestCase.metadata(),
        Column('x', Numeric(10, 2)),
        engines.Memory()
    )

    def test_select_insert(self):
        x = Decimal('12345678.12')

        with self.create_table(self.table):
            self.session.execute(self.table.insert(), [{'x': x}])
            self.assertEqual(self.session.query(self.table.c.x).scalar(), x)

    def test_insert_overflow(self):
        value = Decimal('12345678901234567890.1234567890')

        with self.create_table(self.table):
            with self.assertRaises(DatabaseException) as ex:
                self.session.execute(self.table.insert(), [{'x': value}])

            # 'Too many digits' is written in the CH response;
            # 'out of range' is raised from `struct` within
            # `clickhouse_driver`,
            # before the query is sent to CH.
            self.assertTrue(
                'out of range' in str(ex.exception) or
                'Too many digits' in str(ex.exception))


class NumericNativeTestCase(NativeSessionTestCase):
    table = Table(
        'test', NativeSessionTestCase.metadata(),
        Column('x', Numeric(10, 2)),
        engines.Memory()
    )

    def test_insert_truncate(self):
        value = Decimal('123.129999')
        expected = Decimal('123.12')

        with self.create_table(self.table):
            self.session.execute(self.table.insert(), [{'x': value}])
            self.assertEqual(self.session.query(self.table.c.x).scalar(),
                             expected)


class NumericHttpTestCase(HttpSessionTestCase):
    table = Table(
        'test', HttpSessionTestCase.metadata(),
        Column('x', Numeric(10, 2)),
        engines.Memory()
    )

    def test_insert_truncate(self):
        value = Decimal('123.129999')

        with self.create_table(self.table):
            with self.assertRaises(DatabaseException) as ex:
                self.session.execute(self.table.insert(), [{'x': value}])
            self.assertIn('value is too small', str(ex.exception))
