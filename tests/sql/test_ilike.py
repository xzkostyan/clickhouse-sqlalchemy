from sqlalchemy import Column
from clickhouse_sqlalchemy import types, Table

from tests.testcase import BaseTestCase


class ILike(BaseTestCase):
    table = Table(
        't1',
        BaseTestCase.metadata(),
        Column('x', types.Int32, primary_key=True),
        Column('y', types.String)
    )

    def test_ilike(self):
        query = (
            self.session.query(self.table.c.x)
            .where(self.table.c.y.ilike('y'))
        )

        self.assertEqual(
            self.compile(query, literal_binds=True),
            "SELECT t1.x AS t1_x FROM t1 WHERE t1.y ILIKE 'y'"
        )

    def test_not_ilike(self):
        query = (
            self.session.query(self.table.c.x)
            .where(self.table.c.y.not_ilike('y'))
        )

        self.assertEqual(
            self.compile(query, literal_binds=True),
            "SELECT t1.x AS t1_x FROM t1 WHERE t1.y NOT ILIKE 'y'"
        )
