from datetime import datetime

from sqlalchemy import Column
from sqlalchemy.sql.ddl import CreateTable

from clickhouse_sqlalchemy import types, engines, Table
from tests.testcase import BaseTestCase, BaseAsynchTestCase, \
    CompilationTestCase
from tests.util import with_native_and_http_sessions, run_async


class DateTimeCompilationTestCase(CompilationTestCase):
    table = Table(
        'test', CompilationTestCase.metadata(),
        Column('x', types.DateTime, primary_key=True),
        engines.Memory()
    )

    def test_create_table(self):
        self.assertEqual(
            self.compile(CreateTable(self.table)),
            'CREATE TABLE test (x DateTime) ENGINE = Memory'
        )


@with_native_and_http_sessions
class DateTimeTestCase(BaseTestCase):
    table = Table(
        'test', BaseTestCase.metadata(),
        Column('x', types.DateTime, primary_key=True),
        engines.Memory()
    )

    def test_select_insert(self):
        dt = datetime(2018, 1, 1, 15, 20)

        with self.create_table(self.table):
            self.session.execute(self.table.insert(), [{'x': dt}])
            self.assertEqual(self.session.query(self.table.c.x).scalar(), dt)


class DateTimeAsynchTestCase(BaseAsynchTestCase):
    table = Table(
        'test', BaseAsynchTestCase.metadata(),
        Column('x', types.DateTime, primary_key=True),
        engines.Memory(),
    )

    @run_async
    async def test_select_insert(self):
        dt = datetime(2018, 1, 1, 15, 20)

        async with self.create_table(self.table):
            await self.session.execute(self.table.insert(), [{'x': dt}])

            def _run(session):
                return session.query(self.table.c.x).scalar()

            self.assertEqual(await self.session.run_sync(_run), dt)
