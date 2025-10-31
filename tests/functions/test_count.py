from sqlalchemy import Column, func

from clickhouse_sqlalchemy import types, Table
from tests.testcase import CompilationTestCase


class CountTestCaseBase(CompilationTestCase):
    table = Table(
        't1', CompilationTestCase.metadata(),
        Column('x', types.Int32, primary_key=True)
    )

    def test_count(self):
        self.assertEqual(
            self.compile(self.session.query(func.count(self.table.c.x))),
            'SELECT count(t1.x) AS count_1 FROM t1'
        )

    def test_count_distinct(self):
        query = self.session.query(func.count(func.distinct(self.table.c.x)))
        self.assertEqual(
            self.compile(query),
            'SELECT count(distinct(t1.x)) AS count_1 FROM t1'
        )

    def test_count_no_column_specified(self):
        query = self.session.query(func.count()).select_from(self.table)
        self.assertEqual(
            self.compile(query),
            'SELECT count(*) AS count_1 FROM t1'
        )
