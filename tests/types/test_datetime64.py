from sqlalchemy import Column
from sqlalchemy.sql.ddl import CreateTable

from clickhouse_sqlalchemy import types, engines, Table
from tests.testcase import CompilationTestCase


class DateTime64CompilationTestCase(CompilationTestCase):
    table = Table(
        'test', CompilationTestCase.metadata(),
        Column('x', types.DateTime64, primary_key=True),
        engines.Memory()
    )

    def test_create_table(self):
        self.assertEqual(
            self.compile(CreateTable(self.table)),
            'CREATE TABLE test (x DateTime64(3)) ENGINE = Memory'
        )


class DateTime64CompilationTestCasePrecision(CompilationTestCase):
    table = Table(
        'test', CompilationTestCase.metadata(),
        Column('x', types.DateTime64(4), primary_key=True),
        engines.Memory()
    )

    def test_create_table_precision(self):
        self.assertEqual(
            self.compile(CreateTable(self.table)),
            'CREATE TABLE test (x DateTime64(4)) ENGINE = Memory'
        )


class DateTime64CompilationTestCaseTimezone(CompilationTestCase):
    table = Table(
        'test', CompilationTestCase.metadata(),
        Column(
            'x', types.DateTime64(4, 'Pacific/Honolulu'), primary_key=True,
        ),
        engines.Memory()
    )

    def test_create_table_precision(self):
        self.assertEqual(
            self.compile(CreateTable(self.table)),
            'CREATE TABLE test ('
            'x DateTime64(4, \'Pacific/Honolulu\')'
            ') ENGINE = Memory'
        )
