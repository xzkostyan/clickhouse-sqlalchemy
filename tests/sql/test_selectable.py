from sqlalchemy import Column

from src import types, select, Table
from tests.testcase import BaseTestCase


class SelectTestCase(BaseTestCase):
    def create_table(self):
        metadata = self.metadata()

        return Table(
            't1', metadata,
            Column('x', types.Int32, primary_key=True)
        )

    def test_group_by_with_totals(self):
        table = self.create_table()

        query = select([table.c.x]).group_by(table.c.x)
        self.assertEqual(
            self.compile(query),
            'SELECT x FROM t1 GROUP BY x'
        )

        query = select([table.c.x]).group_by(table.c.x).with_totals()
        self.assertEqual(
            self.compile(query),
            'SELECT x FROM t1 GROUP BY x WITH TOTALS'
        )

    def test_sample(self):
        table = self.create_table()

        query = select([table.c.x]).sample(0.1).group_by(table.c.x)
        self.assertEqual(
            self.compile(query),
            'SELECT x FROM t1 SAMPLE %(param_1)s GROUP BY x'
        )

        self.assertEqual(
            self.compile(query, literal_binds=True),
            'SELECT x FROM t1 SAMPLE 0.1 GROUP BY x'
        )
