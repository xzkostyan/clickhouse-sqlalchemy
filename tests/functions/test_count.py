from sqlalchemy import Column, func

from clickhouse_sqlalchemy import types, Table
from tests.testcase import (
    BaseAbstractTestCase, HttpSessionTestCase, NativeSessionTestCase,
)


class CountTestCaseBase(BaseAbstractTestCase):
    def create_table(self):
        metadata = self.metadata()

        return Table(
            't1', metadata,
            Column('x', types.Int32, primary_key=True)
        )

    def test_count(self):
        table = self.create_table()

        self.assertEqual(
            self.compile(self.session.query(func.count(table.c.x))),
            'SELECT count(t1.x) AS count_1 FROM t1'
        )

    def test_count_distinct(self):
        table = self.create_table()
        query = self.session.query(func.count(func.distinct(table.c.x)))
        self.assertEqual(
            self.compile(query),
            'SELECT count(distinct(t1.x)) AS count_1 FROM t1'
        )

    def test_count_no_column_specified(self):
        table = self.create_table()
        query = self.session.query(func.count()).select_from(table)
        self.assertEqual(
            self.compile(query),
            'SELECT count(*) AS count_1 FROM t1'
        )


class CountHttpTestCase(CountTestCaseBase, HttpSessionTestCase):
    """ ... """


class CountNativeTestCase(CountTestCaseBase, NativeSessionTestCase):
    """ ... """
