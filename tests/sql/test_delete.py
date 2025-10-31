from sqlalchemy import Column, exc, delete

from clickhouse_sqlalchemy import types, Table, engines
from tests.testcase import NativeSessionTestCase
from tests.util import mock_object_attr


class DeleteTestCase(NativeSessionTestCase):
    def test_delete(self):
        t1 = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            engines.MergeTree('x', order_by=('x', ))
        )

        query = t1.delete().where(t1.c.x == 25)
        statement = self.compile(query, literal_binds=True)
        self.assertEqual(statement, 'ALTER TABLE t1 DELETE WHERE x = 25')

        query = delete(t1).where(t1.c.x == 25)
        statement = self.compile(query, literal_binds=True)
        self.assertEqual(statement, 'ALTER TABLE t1 DELETE WHERE x = 25')

    def test_delete_without_where(self):
        t1 = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            engines.MergeTree('x', order_by=('x', ))
        )

        query = t1.delete()
        with self.assertRaises(exc.CompileError) as ex:
            self.compile(query, literal_binds=True)

        self.assertEqual(str(ex.exception), 'WHERE clause is required')

        query = delete(t1)
        with self.assertRaises(exc.CompileError) as ex:
            self.compile(query, literal_binds=True)

        self.assertEqual(str(ex.exception), 'WHERE clause is required')

    def test_delete_unsupported(self):
        t1 = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            engines.MergeTree('x', order_by=('x', ))
        )
        t1.drop(bind=self.session.bind, if_exists=True)
        t1.create(bind=self.session.bind)

        with self.assertRaises(exc.CompileError) as ex:
            dialect = self.session.bind.dialect
            with mock_object_attr(dialect, 'supports_delete', False):
                self.session.execute(t1.delete().where(t1.c.x == 25))

        self.assertEqual(
            str(ex.exception),
            'ALTER DELETE is not supported by this server version'
        )

        with self.assertRaises(exc.CompileError) as ex:
            dialect = self.session.bind.dialect
            with mock_object_attr(dialect, 'supports_delete', False):
                self.session.execute(delete(t1).where(t1.c.x == 25))

        self.assertEqual(
            str(ex.exception),
            'ALTER DELETE is not supported by this server version'
        )
