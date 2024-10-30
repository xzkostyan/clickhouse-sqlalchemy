import sqlalchemy.exc
from asynch.errors import TypeMismatchError
from sqlalchemy import Column, func, text

from clickhouse_sqlalchemy import Table, engines, types
from tests.testcase import AsynchSessionTestCase


class NativeInsertTestCase(AsynchSessionTestCase):
    async def test_rowcount_return1(self):
        metadata = self.metadata()
        table = Table(
            'test', metadata,
            Column('x', types.UInt32, primary_key=True),
            engines.Memory()
        )
        await self.run_sync(metadata.drop_all)
        await self.run_sync(metadata.create_all)

        rv = await self.session.execute(
            table.insert(),
            [{'x': x} for x in range(5)]
        )

        self.assertEqual(rv.rowcount, 5)
        self.assertEqual(
            await self.session.run_sync(
                lambda sc: sc.query(func.count()).select_from(table).scalar()
            ),
            5
        )

        rv = await self.session.execute(
            text('INSERT INTO test SELECT * FROM system.numbers LIMIT 5')
        )
        self.assertEqual(rv.rowcount, -1)

    async def test_types_check(self):
        metadata = self.metadata()
        table = Table(
            'test', metadata,
            Column('x', types.UInt32, primary_key=True),
            engines.Memory()
        )
        await self.run_sync(metadata.drop_all)
        await self.run_sync(metadata.create_all)

        with self.assertRaises(sqlalchemy.exc.DBAPIError) as ex:
            await self.session.execute(
                table.insert(),
                [{'x': -1}],
                execution_options=dict(types_check=True),
            )
        self.assertTrue(isinstance(ex.exception.orig, TypeMismatchError))
        self.assertIn('-1 for column "x"', str(ex.exception))

        with self.assertRaises(sqlalchemy.exc.DBAPIError) as ex:
            await self.session.execute(table.insert(), {'x': -1})
        self.assertTrue(isinstance(ex.exception.orig, TypeMismatchError))
        self.assertIn(
            'Repeat query with types_check=True for detailed info',
            str(ex.exception)
        )
