import datetime

from sqlalchemy import Column
from sqlalchemy.sql.ddl import CreateTable

from clickhouse_sqlalchemy import types, engines, Table
from tests.testcase import BaseTestCase, CompilationTestCase
from tests.util import with_native_and_http_sessions


class Date32CompilationTestCase(CompilationTestCase):
    required_server_version = (21, 9, 0)

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
class Date32TestCase(BaseTestCase):
    required_server_version = (21, 9, 0)

    table = Table(
        'test', BaseTestCase.metadata(),
        Column('x', types.Date32, primary_key=True),
        engines.Memory()
    )

    def test_select_insert(self):
        # Use a date before epoch to validate dates before epoch can be stored.
        date = datetime.date(1920, 1, 1)
        with self.create_table(self.table):
            self.session.execute(self.table.insert(), [{'x': date}])
            result = self.session.execute(self.table.select()).scalar()
            if isinstance(result, datetime.date):
                self.assertEqual(result, date)
            else:
                self.assertEqual(result, date.isoformat())
