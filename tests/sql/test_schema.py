
from sqlalchemy import Column, text, Table, inspect

from clickhouse_sqlalchemy import types, Table as CHTable, engines
from tests.testcase import NativeSessionTestCase, BaseTestCase


class NativeSchemaTestCase(NativeSessionTestCase):
    def test_reflect(self):
        """
        checking, that after call metadata.reflect()
        we have a clickhouse-specific table, which has overridden join methods
        """
        session = self.session
        metadata = self.metadata()

        # ### Maybe: ###
        # # session = native_session  # database=test
        # session = self.__class__.session
        # same as in `self.metadata()`  # database=default
        # unbound_metadata = MetaData(bind=session.bind)

        table = CHTable(
            'test_reflect',
            metadata,
            Column('x', types.Int32),
            engines.Log()
        )

        table.drop(session.bind, if_exists=True)
        table.create(session.bind)

        # Sub-test: ensure the `metadata.reflect` makes a CHTable
        metadata.clear()  # reflect from clean state
        self.assertFalse(metadata.tables)
        metadata.reflect(only=[table.name])
        table2 = metadata.tables.get(table.name)
        self.assertIsNotNone(table2)
        self.assertListEqual([c.name for c in table2.columns], ['x'])
        self.assertTrue(isinstance(table2, CHTable))

        # Sub-test: ensure `CHTable(..., autoload=True)` works too
        metadata.clear()
        table3 = CHTable('test_reflect', metadata, autoload=True)
        self.assertListEqual([c.name for c in table3.columns], ['x'])

        # Sub-test: check that they all reflected the same.
        for table_x in [table, table2, table3]:
            query = table_x.select().join(
                text('another_table'),
                table.c.x == 'xxx',
                type='INNER',
                strictness='ALL',
                distribution='GLOBAL'
            )
            self.assertEqual(
                self.compile(query),
                "SELECT x FROM test_reflect "
                "GLOBAL ALL INNER JOIN another_table "
                "ON x = %(x_1)s"
            )

    def test_reflect_generic_table(self):
        """
        checking, that generic table columns are reflected properly
        """
        metadata = self.metadata()

        table = Table(
            'test_reflect',
            metadata,
            Column('x', types.Int32),
            engines.Log()
        )

        self.session.execute('DROP TABLE IF EXISTS test_reflect')
        table.create(self.session.bind)

        # Sub-test: ensure the `metadata.reflect` makes a CHTable
        metadata.clear()  # reflect from clean state
        self.assertFalse(metadata.tables)

        table = Table('test_reflect', metadata, autoload=True)
        self.assertListEqual([c.name for c in table.columns], ['x'])


class HTTPSchemaTestCase(BaseTestCase):
    def test_get_schema_names(self):
        insp = inspect(self.session.bind)
        schema_names = insp.get_schema_names()
        self.assertGreater(len(schema_names), 0)

    def test_get_table_names(self):
        table = Table(
            'test_reflect',
            self.metadata(),
            Column('x', types.Int32),
            engines.Log()
        )

        self.session.execute('DROP TABLE IF EXISTS test_reflect')
        table.create(self.session.bind)

        insp = inspect(self.session.bind)
        self.assertListEqual(insp.get_table_names(), ['test_reflect'])
