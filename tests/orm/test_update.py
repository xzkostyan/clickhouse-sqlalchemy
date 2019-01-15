from sqlalchemy import Column, update

from src import types, Table
from tests.testcase import BaseTestCase


class UpdateTestCase(BaseTestCase):
    def create_table(self):
        metadata = self.metadata()

        return Table(
            't1', metadata,
            Column('x', types.Int32, primary_key=True),
            Column('y', types.Int32, primary_key=True)
        )

    def test_update(self):
        table = self.create_table()
        query = table.update().values(x=3).where(table.c.x.in_([1, 2]))
        self.assertEqual(
            self.compile(query),
            'ALTER TABLE t1 UPDATE x=%(x)s WHERE x IN (%(x_1)s, %(x_2)s)'
        )
        query = update(table).values(x=3).where(table.c.x.in_([1, 2]))
        self.assertEqual(
            self.compile(query),
            'ALTER TABLE t1 UPDATE x=%(x)s WHERE x IN (%(x_1)s, %(x_2)s)'
        )

    def test_update_from_own_field(self):
        table = self.create_table()
        query = table.update().values(x=table.c.y).where(table.c.x.in_([1, 2]))
        self.assertEqual(
            self.compile(query),
            'ALTER TABLE t1 UPDATE x=y WHERE x IN (%(x_1)s, %(x_2)s)'
        )
