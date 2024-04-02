from datetime import datetime

from sqlalchemy import Column
from sqlalchemy.sql.ddl import CreateTable

from clickhouse_sqlalchemy import types, engines, Table
from tests.testcase import BaseTestCase, CompilationTestCase
from tests.util import with_native_and_http_sessions


class DateTimeCompilationTestCase(CompilationTestCase):
    def test_create_table(self):
        table = Table(
            'test', CompilationTestCase.metadata(),
            Column('x', types.Date32, primary_key=True),
            engines.Memory()
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE test (x Date32) ENGINE = Memory'
        )

@with_native_and_http_sessions
class DateTimeTestCase(BaseTestCase):
    table = Table(
        'test', BaseTestCase.metadata(),
        Column('x', types.Date32, primary_key=True),
        engines.Memory()
    )

    def test_select_insert(self):
        dt = datetime(2018, 1, 1, 0, 0)

        with self.create_table(self.table):
            self.session.execute(self.table.insert(), [{'x': dt}])
            self.assertEqual(self.session.query(self.table.c.x).scalar(), dt)
