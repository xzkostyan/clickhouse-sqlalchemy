from sqlalchemy import Column, literal_column, select

from clickhouse_sqlalchemy import types, Table, engines
from tests.testcase import NativeSessionTestCase
from tests.util import require_server_version


class InsertTestCase(NativeSessionTestCase):
    @require_server_version(19, 3, 3)
    def test_insert(self):
        table = Table(
            't', self.metadata(),
            Column('x', types.String, primary_key=True),
            engines.Log()
        )

        with self.create_table(table):
            query = table.insert().values(x=literal_column("'test'"))
            self.session.execute(query)

            rv = self.session.execute(select(table.c.x)).scalar()
            self.assertEqual(rv, 'test')
