from sqlalchemy import Column, func

from clickhouse_sqlalchemy import engines, types, Table
from clickhouse_sqlalchemy.exceptions import DatabaseException
from tests.testcase import NativeSessionTestCase


class NativeInsertTestCase(NativeSessionTestCase):
    def test_rowcount_return(self):
        table = Table(
            'test', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            engines.Memory()
        )
        table.drop(if_exists=True)
        table.create()

        rv = self.session.execute(table.insert(), [{'x': x} for x in range(5)])
        self.assertEqual(rv.rowcount, 5)
        self.assertEqual(
            self.session.query(func.count()).select_from(table).scalar(), 5
        )

        rv = self.session.execute(
            'INSERT INTO test SELECT * FROM system.numbers LIMIT 5'
        )
        self.assertEqual(rv.rowcount, -1)

    def test_types_check(self):
        table = Table(
            'test', self.metadata(),
            Column('x', types.UInt32, primary_key=True),
            engines.Memory()
        )
        table.drop(if_exists=True)
        table.create()

        with self.assertRaises(DatabaseException) as ex:
            self.session.execute(
                table.insert().execution_options(types_check=True),
                [{'x': -1}]
            )
        self.assertIn('-1 for column "x"', str(ex.exception.orig))

        with self.assertRaises(DatabaseException) as ex:
            self.session.execute(table.insert(), [{'x': -1}])
        self.assertIn(
            'Repeat query with types_check=True for detailed info',
            str(ex.exception.orig)
        )
