
from sqlalchemy import Column, text

from clickhouse_sqlalchemy import types, Table as CHTable, engines
from tests.testcase import NativeSessionTestCase


class SchemaTestCase(NativeSessionTestCase):
    def test_reflect(self):
        """
        checking, that after call metadata.reflect()
        we have clickohouse-specific table, which have overridden join methods
        """
        metadata = self.metadata()
        table = CHTable(
            'test_reflect',
            metadata,
            Column('x', types.Int32),
            engines.Log()
        )
        table.drop(self.session.bind, if_exists=True)
        table.create(self.session.bind)

        metadata.clear()
        metadata.reflect(only=[table.name])
        assert table.name in metadata.tables
        self.assertTrue(isinstance(table, CHTable))

        query = table.select().join(
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
