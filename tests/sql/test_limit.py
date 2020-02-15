from sqlalchemy import Column, exc

from clickhouse_sqlalchemy import types, Table
from tests.testcase import (
    BaseAbstractTestCase, HttpSessionTestCase, NativeSessionTestCase,
)


class LimitTestCase(BaseAbstractTestCase):
    def create_table(self):
        metadata = self.metadata()

        return Table(
            't1', metadata,
            Column('x', types.Int32, primary_key=True)
        )

    def test_limit(self):
        table = self.create_table()

        query = self.session.query(table.c.x).limit(10)
        self.assertEqual(
            self.compile(query, literal_binds=True),
            'SELECT x AS t1_x FROM t1  LIMIT 10'
        )

    def test_limit_with_offset(self):
        table = self.create_table()

        query = self.session.query(table.c.x).limit(10).offset(5)
        self.assertEqual(
            self.compile(query, literal_binds=True),
            'SELECT x AS t1_x FROM t1  LIMIT 5, 10'
        )

        query = self.session.query(table.c.x).offset(5).limit(10)
        self.assertEqual(
            self.compile(query, literal_binds=True),
            'SELECT x AS t1_x FROM t1  LIMIT 5, 10'
        )

    def test_offset_without_limit(self):
        table = self.create_table()

        with self.assertRaises(exc.CompileError) as ex:
            query = self.session.query(table.c.x).offset(5)
            self.compile(query, literal_binds=True)

        self.assertEqual(
            str(ex.exception),
            'OFFSET without LIMIT is not supported'
        )


class LimitHttpTestCase(LimitTestCase, HttpSessionTestCase):
    """ Limit over a HTTP session """


class LimitNativeTestCase(LimitTestCase, NativeSessionTestCase):
    """ Limit over a native protocol session """
