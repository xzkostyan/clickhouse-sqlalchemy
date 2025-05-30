import enum

from sqlalchemy import Column
from sqlalchemy.sql.ddl import CreateTable

from clickhouse_sqlalchemy import types, engines, Table
from tests.testcase import BaseTestCase, CompilationTestCase
from tests.util import with_native_and_http_sessions


class _TestEnum(enum.IntEnum):
    First = 1
    Second = 2


class Enum16CompilationTestCase(CompilationTestCase):
    def test_create_table(self):
        table = Table(
            "test",
            BaseTestCase.metadata(),
            Column("x", types.Enum16(_TestEnum)),
            engines.Memory(),
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            "CREATE TABLE test (x Enum16('First' = 1, 'Second' = 2)) "
            "ENGINE = Memory",
        )


@with_native_and_http_sessions
class Enum8TestCase(BaseTestCase):
    table = Table(
        "test",
        BaseTestCase.metadata(),
        Column("x", types.Enum16(_TestEnum)),
        engines.Memory(),
    )

    def test_select_insert(self):
        value = _TestEnum.First
        with self.create_table(self.table):
            self.session.execute(self.table.insert(), [{"x": value}])
            self.assertEqual(self.session.query(self.table.c.x).scalar(), value)
