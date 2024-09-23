from sqlalchemy import Column, literal_column, select

from clickhouse_sqlalchemy import types, Table, engines, sql
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

    @require_server_version(21, 1, 3)
    def test_insert_map(self):
        table = Table(
            't', self.metadata(),
            Column('x', types.Map(types.String, types.Int32),
                   primary_key=True),
            engines.Memory()
        )

        with self.create_table(table):
            dict_map = dict(key1=1, Key2=2)
            x = [
                {'x': dict_map}
            ]
            self.session.execute(table.insert(), x)

            rv = self.session.execute(select(table.c.x)).scalar()
            self.assertEqual(rv, dict_map)

    @require_server_version(19, 3, 3)
    def test_insert_iterator(self):
        table = Table(
            't', self.metadata(),
            Column('x', types.String, primary_key=True),
            engines.Log()
        )

        def generator():
            yield ["foo"]
            yield ["bar"]

        with self.create_table(table):
            query = sql.insert(table).values_iterator([table.c.x], generator())
            self.session.execute(query)

            result = list(self.session.execute(select(table.c.x)))
            print(result)
            self.assertListEqual(result, [('foo',), ('bar',)])

    @require_server_version(19, 3, 3)
    def test_insert_iterator_list(self):
        table = Table(
            't', self.metadata(),
            Column('x', types.String, primary_key=True),
            engines.Log()
        )

        with self.create_table(table):
            query = sql.insert(table).values_iterator(
                [table.c.x],
                [["foo"], ["bar"]]
            )
            self.session.execute(query)

            result = list(self.session.execute(select(table.c.x)))
            print(result)
            self.assertListEqual(result, [('foo',), ('bar',)])
