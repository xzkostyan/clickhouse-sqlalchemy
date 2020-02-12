from sqlalchemy import Column

from clickhouse_sqlalchemy import types, Table
from tests.testcase import BaseTestCase


class UpdateTestCase(BaseTestCase):
    def test_update(self):
        metadata = self.metadata()

        t1 = Table(
            't1', metadata,
            Column('x', types.Int32, primary_key=True)
        )

        query = t1.update().where(t1.c.x == 25).values(x=5)
        statement = self.compile(query, literal_binds=True)
        self.assertEqual(statement, 'ALTER TABLE t1 UPDATE x=5 WHERE x = 25')
