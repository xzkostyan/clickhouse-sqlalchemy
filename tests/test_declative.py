from six import text_type
from sqlalchemy import Column, func
from sqlalchemy.sql.ddl import CreateTable

from src.declarative import get_declarative_base
from src import types, engines
from tests.testcase import BaseTestCase


class DeclarativeTestCase(BaseTestCase):
    def compile(self, clause, **kwargs):
        return self.strip_spaces.sub('', text_type(self._compile(clause)))

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


class EnginesDeclarativeTestCase(DeclarativeTestCase):
    def test_text_engine_columns(self):
        base = get_declarative_base()

        class TestTable(base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)
            y = Column(types.String)

            __table_args__ = (
                engines.MergeTree('date', ('date', 'x')),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            'CREATE TABLE test_table (date Date, x Int32, y String) '
            'ENGINE = MergeTree(date, (date, x), 8192)'
        )

    def test_func_engine_columns(self):
        base = get_declarative_base()

        class TestTable(base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)
            y = Column(types.String)

            __table_args__ = (
                engines.MergeTree('date', ('date', func.intHash32(x)),
                                  sampling=func.intHash32(x)),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            'CREATE TABLE test_table (date Date, x Int32, y String) '
            'ENGINE = MergeTree('
            'date, intHash32(x), (date, intHash32(x)), 8192'
            ')'
        )

    def test_index_granularity(self):
        base = get_declarative_base()

        class TestTable(base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)
            y = Column(types.String)

            __table_args__ = (
                engines.MergeTree(date, (date, x), index_granularity=4096),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            'CREATE TABLE test_table (date Date, x Int32, y String) '
            'ENGINE = MergeTree(date, (date, x), 4096)'
        )
