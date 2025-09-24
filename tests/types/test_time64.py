import datetime

import pytest
from sqlalchemy import Column, text
from sqlalchemy.sql.ddl import CreateTable

from clickhouse_sqlalchemy import Table, engines, types
from tests.testcase import BaseTestCase, CompilationTestCase
from tests.util import with_native_and_http_sessions


class Time64CompilationTestCase(CompilationTestCase):
    def test_create_table(self):
        if self.server_version < (25, 6, 0):
            self.skipTest("Time types require ClickHouse 25.6+")
        table = Table(
            "test",
            CompilationTestCase.metadata(),
            Column("x", types.Time64, primary_key=True),
            engines.Memory(),
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            "CREATE TABLE test (x Time64(3)) ENGINE = Memory",
        )


class Time64CompilationTestCasePrecision(CompilationTestCase):
    def test_create_table_with_precision(self):
        if self.server_version < (25, 6, 0):
            self.skipTest("Time types require ClickHouse 25.6+")
        table = Table(
            "test",
            CompilationTestCase.metadata(),
            Column("x", types.Time64(6), primary_key=True),
            engines.Memory(),
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            "CREATE TABLE test (x Time64(6)) ENGINE = Memory",
        )

    def test_create_table_with_bad_precision(self):
        if self.server_version < (25, 6, 0):
            self.skipTest("Time types require ClickHouse 25.6+")
        table = Table(
            "test",
            CompilationTestCase.metadata(),
            Column("x", types.Time64(7), primary_key=True),
            engines.Memory(),
        )

        with pytest.raises(ValueError, match="Invalid precision value"):
            self.compile(CreateTable(table))

    def test_create_table_with_empty_precision_defaults_to_3(self):
        if self.server_version < (25, 6, 0):
            self.skipTest("Time types require ClickHouse 25.6+")
        table = Table(
            "test",
            CompilationTestCase.metadata(),
            Column("x", types.Time64, primary_key=True),
            engines.Memory(),
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            "CREATE TABLE test (x Time64(3)) ENGINE = Memory",
        )


@with_native_and_http_sessions
class Time64TestCase(BaseTestCase):
    def test_select_insert(self):
        if self.server_version < (25, 6, 0):
            self.skipTest("Time types require ClickHouse 25.6+")

        # Native driver doesn't support Time64 type yet
        if self.session.bind.driver == "native":
            self.skipTest("Native driver doesn't support Time64 type yet")

        time_val = datetime.time(15, 20, 30, 123000)
        table_name = "test_time64_runtime"

        with self.session.bind.connect() as conn:
            try:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                conn.execute(
                    text(
                        f"""
                        CREATE TABLE {table_name} (x Time64(3)) ENGINE = Memory
                        SETTINGS enable_time_time64_type = 1
                        """
                    )
                )
                conn.execute(
                    text(f"INSERT INTO {table_name} (x) VALUES ('{time_val}')")
                )
                result = conn.execute(
                    text(f"SELECT x FROM {table_name}")
                ).scalar()
                self.assertEqual(result, time_val)

            finally:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
