from sqlalchemy import Column

from clickhouse_sqlalchemy import types, Table
# from tests.session import session
from tests.testcase import BaseTestCase


class DeleteTestCase(BaseTestCase):
    def test_delete(self):
        metadata = self.metadata()

        t1 = Table(
            't1', metadata,
            Column('x', types.Int32, primary_key=True)
        )

        query = t1.delete().where(t1.c.x == 25)
        statement = self.compile(query, literal_binds=True)
        self.assertEqual(statement, 'ALTER TABLE t1 DELETE WHERE x = 25')
