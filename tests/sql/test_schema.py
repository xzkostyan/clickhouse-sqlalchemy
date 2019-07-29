
from sqlalchemy import Column, text

from clickhouse_sqlalchemy import types, Table as CHTable, engines
from tests.testcase import NativeSessionTestCase


class SchemaTestCase(NativeSessionTestCase):
    def test_reflect(self):
        """
        checking, that after call metadata.reflect()
        we have a clickhouse-specific table, which has overridden join methods
        """
        session = self.session
        metadata = self.metadata()

        # ### Maybe: ###
        # # session = native_session  # database=test
        # session = self.__class__.session  # same as in `self.metadata()`  # database=default
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
        assert table2 is not None
        assert list(table2.columns)
        self.assertTrue(isinstance(table2, CHTable))

        # Sub-test: ensure `CHTable(..., autoload=True)` works too
        metadata.clear()
        table3 = CHTable('test_reflect', metadata, autoload=True)
        assert list(table3.columns)

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
