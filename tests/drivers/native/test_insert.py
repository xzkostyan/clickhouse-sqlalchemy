from sqlalchemy import Column, func

from clickhouse_sqlalchemy import engines, types, Table
from tests.session import native_session
from tests.testcase import BaseTestCase


class NativeInsertTestCase(BaseTestCase):
    session = native_session

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
