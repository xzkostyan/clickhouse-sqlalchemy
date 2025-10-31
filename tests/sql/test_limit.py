from sqlalchemy import Column, exc

from clickhouse_sqlalchemy import types, Table
from tests.testcase import BaseTestCase
from tests.util import with_native_and_http_sessions


@with_native_and_http_sessions
class LimitTestCase(BaseTestCase):
    table = Table(
        't1', BaseTestCase.metadata(),
        Column('x', types.Int32, primary_key=True)
    )

    def test_limit(self):
        query = self.session.query(self.table.c.x).limit(10)
        self.assertEqual(
            self.compile(query, literal_binds=True),
            'SELECT t1.x AS t1_x FROM t1  LIMIT 10'
        )

    def test_limit_with_offset(self):
        query = self.session.query(self.table.c.x).limit(10).offset(5)
        self.assertEqual(
            self.compile(query, literal_binds=True),
            'SELECT t1.x AS t1_x FROM t1  LIMIT 5, 10'
        )

        query = self.session.query(self.table.c.x).offset(5).limit(10)
        self.assertEqual(
            self.compile(query, literal_binds=True),
            'SELECT t1.x AS t1_x FROM t1  LIMIT 5, 10'
        )

    def test_offset_without_limit(self):
        with self.assertRaises(exc.CompileError) as ex:
            query = self.session.query(self.table.c.x).offset(5)
            self.compile(query, literal_binds=True)

        self.assertEqual(
            str(ex.exception),
            'OFFSET without LIMIT is not supported'
        )
