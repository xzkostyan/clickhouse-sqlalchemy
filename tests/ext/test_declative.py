from sqlalchemy import Column
from sqlalchemy.sql.ddl import CreateTable

from clickhouse_sqlalchemy import types, engines, get_declarative_base
from tests.testcase import BaseTestCase


class DeclarativeTestCase(BaseTestCase):
    def test_create_table(self):
        base = get_declarative_base()

        class TestTable(base):
            x = Column(types.Int32, primary_key=True)
            y = Column(types.String)

            __table_args__ = (
                engines.Memory(),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            'CREATE TABLE test_table (x Int32, y String) ENGINE = Memory'
        )

    def test_create_table_custom_name(self):
        base = get_declarative_base()

        class TestTable(base):
            __tablename__ = 'testtable'

            x = Column(types.Int32, primary_key=True)
            y = Column(types.String)

            __table_args__ = (
                engines.Memory(),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            'CREATE TABLE testtable (x Int32, y String) ENGINE = Memory'
        )
