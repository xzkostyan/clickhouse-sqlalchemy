from sqlalchemy import Column, exc, update, func

from clickhouse_sqlalchemy import types, Table, engines
from tests.testcase import NativeSessionTestCase
from tests.util import mock_object_attr


class UpdateTestCase(NativeSessionTestCase):
    def test_update(self):
        t1 = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            engines.MergeTree('x', order_by=('x', ))
        )

        query = t1.update().where(t1.c.x == 25).values(x=5)
        statement = self.compile(query, literal_binds=True)
        self.assertEqual(statement, 'ALTER TABLE t1 UPDATE x=5 WHERE x = 25')

        query = update(t1).where(t1.c.x == 25).values(x=5)
        statement = self.compile(query, literal_binds=True)
        self.assertEqual(statement, 'ALTER TABLE t1 UPDATE x=5 WHERE x = 25')

    def test_update_without_where(self):
        t1 = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            engines.MergeTree('x', order_by=('x', ))
        )

        query = t1.update().values(x=5)
        with self.assertRaises(exc.CompileError) as ex:
            self.compile(query, literal_binds=True)

        self.assertEqual(str(ex.exception), 'WHERE clause is required')

        query = update(t1).values(x=5)
        with self.assertRaises(exc.CompileError) as ex:
            self.compile(query, literal_binds=True)

        self.assertEqual(str(ex.exception), 'WHERE clause is required')

    def test_update_unsupported(self):
        t1 = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            engines.MergeTree(order_by=(func.tuple(), ))
        )
        t1.drop(if_exists=True)
        t1.create()

        with self.assertRaises(exc.CompileError) as ex:
            dialect = self.session.bind.dialect
            with mock_object_attr(dialect, 'supports_update', False):
                self.session.execute(
                    t1.update().where(t1.c.x == 25).values(x=5)
                )

        self.assertEqual(
            str(ex.exception),
            'ALTER UPDATE is not supported by this server version'
        )

        with self.assertRaises(exc.CompileError) as ex:
            dialect = self.session.bind.dialect
            with mock_object_attr(dialect, 'supports_update', False):
                self.session.execute(
                    update(t1).where(t1.c.x == 25).values(x=5)
                )

        self.assertEqual(
            str(ex.exception),
            'ALTER UPDATE is not supported by this server version'
        )
