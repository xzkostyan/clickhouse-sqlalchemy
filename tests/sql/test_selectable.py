from sqlalchemy import Column, and_

from clickhouse_sqlalchemy import types, select, Table
from tests.testcase import BaseTestCase


class SelectTestCase(BaseTestCase):
    def create_table(self, *columns):
        metadata = self.metadata()
        columns = columns or [
            Column('x', types.Int32, primary_key=True)
        ]

        return Table(
            't1', metadata,
            *columns
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

    def test_nested_type(self):
        table = self.create_table(
            Column('x', types.Int32, primary_key=True),
            Column('parent', types.Nested(
                Column('child1', types.Int32),
                Column('child2', types.String),
            ))
        )

        query = select(
            [table.c.parent.child1]
        ).where(
            and_(
                table.c.parent.child1 == [1, 2],
                table.c.parent.child2 == ['foo', 'bar']
            )
        )
        self.assertEqual(
            self.compile(query, literal_binds=True),
            'SELECT parent.child1 FROM t1 '
            'WHERE parent.child1 = [1, 2] '
            'AND parent.child2 = [\'foo\', \'bar\']'
        )
