from sqlalchemy import Column, Boolean
from sqlalchemy.sql.ddl import CreateTable

from clickhouse_sqlalchemy import engines, Table
from tests.testcase import CompilationTestCase


class BooleanCompilationTestCase(CompilationTestCase):
    table = Table(
        'test', CompilationTestCase.metadata(),
        Column('x', Boolean),
        engines.Memory()
    )

    def test_create_table(self):
        self.assertEqual(
            self.compile(CreateTable(self.table)),
            'CREATE TABLE test (x UInt8) ENGINE = Memory'
        )

    def test_literals(self):
        query = self.session.query(self.table.c.x).filter(self.table.c.x)
        self.assertEqual(
            self.compile(query),
            'SELECT test.x AS test_x FROM test WHERE test.x = 1'
        )
