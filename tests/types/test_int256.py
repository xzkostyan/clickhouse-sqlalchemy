from sqlalchemy import Column
from sqlalchemy.sql.ddl import CreateTable

from clickhouse_sqlalchemy import types, engines, Table
from tests.testcase import BaseTestCase, CompilationTestCase
from tests.util import with_native_and_http_sessions


class Int256CompilationTestCase(CompilationTestCase):
    required_server_version = (21, 6, 0)

    table = Table(
        'test', CompilationTestCase.metadata(),
        Column('x', types.Int256),
        Column('y', types.UInt256),
        engines.Memory()
    )

    def test_create_table(self):
        self.assertEqual(
            self.compile(CreateTable(self.table)),
            'CREATE TABLE test (x Int256, y UInt256) ENGINE = Memory'
        )


@with_native_and_http_sessions
class Int256TestCase(BaseTestCase):
    required_server_version = (21, 6, 0)

    table = Table(
        'test', BaseTestCase.metadata(),
        Column('x', types.Int256),
        Column('y', types.UInt256),
        engines.Memory()
    )

    def test_select_insert(self):
        x = -2 ** 255
        y = 2 ** 256 - 1

        with self.create_table(self.table):
            self.session.execute(self.table.insert(), [{'x': x, 'y': y}])
            self.assertEqual(self.session.query(self.table.c.x).scalar(), x)
            self.assertEqual(self.session.query(self.table.c.y).scalar(), y)
