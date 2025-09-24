import datetime

from sqlalchemy import Column, text
from sqlalchemy.sql.ddl import CreateTable

from clickhouse_sqlalchemy import Table, engines, types
from tests.testcase import BaseTestCase, CompilationTestCase
from tests.util import with_native_and_http_sessions


class TimeCompilationTestCase(CompilationTestCase):
    def test_create_table(self):
        if self.server_version < (25, 6, 0):
            self.skipTest("Time types require ClickHouse 25.6+")
        table = Table(
            "test",
            CompilationTestCase.metadata(),
            Column("x", types.Time, primary_key=True),
            engines.Memory(),
        )
        assert (
            self.compile(CreateTable(table))
            == "CREATE TABLE test (x Time) ENGINE = Memory"
        )


@with_native_and_http_sessions
class TimeRuntimeTestCase(BaseTestCase):
    def test_select_insert(self):
        if self.server_version < (25, 6, 0):
            self.skipTest("Time types require ClickHouse 25.6+")

        # Native driver doesn't support Time type yet
        if self.session.bind.driver == "native":
            self.skipTest("Native driver doesn't support Time type yet")

        time_val = datetime.time(15, 20, 30)
        table_name = "test_time_runtime"

        with self.session.bind.connect() as conn:
            try:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                conn.execute(
                    text(
                        f"CREATE TABLE {table_name} (x Time) ENGINE = Memory SETTINGS enable_time_time64_type = 1"
                    )
                )
                conn.execute(
                    text(f"INSERT INTO {table_name} (x) VALUES ('{time_val}')")
                )
                result = conn.execute(text(f"SELECT x FROM {table_name}")).scalar()
                assert result == time_val
            finally:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
