from sqlalchemy import Column, func

from tests.session import session
from src import types
from src.schema import Table
from tests.testcase import BaseTestCase


class CountTestCase(BaseTestCase):
    def create_table(self):
        metadata = self.metadata()

        return Table(
            't1', metadata,
            Column('x', types.Int32, primary_key=True)
        )

    def test_count(self):
        table = self.create_table()

        self.assertEqual(
            self.compile(session.query(func.count(table.c.x))),
            'SELECT count(x) AS count_1 FROM t1'
        )

    def test_count_distinct(self):
        table = self.create_table()

        self.assertEqual(
            self.compile(session.query(func.count(func.distinct(table.c.x)))),
            'SELECT count(distinct(x)) AS count_1 FROM t1'
        )
