from sqlalchemy.orm import aliased

from clickhouse_sqlalchemy import (
    types,
    Table as CHTable,
    engines,
)
from sqlalchemy import (
    MetaData,
    Column,
    text,
)
from tests.session import native_session
from tests.testcase import BaseTestCase


class SchemaTestCase(BaseTestCase):
    def test_reflect(self):
        """
        checking, that after call metadata.reflect()
        we have clickohouse-specific table, which have overridden join methods
        """
        unbound_metadata = MetaData(bind=native_session.bind)
        table = CHTable(
            'test_reflect',
            unbound_metadata,
            Column('x', types.Int32),
            engines.Log()
        )
        table.drop(native_session.bind, if_exists=True)
        table.create(native_session.bind)

        std_metadata = self.metadata()
        self.assertFalse(std_metadata.tables)
        std_metadata.reflect(only=[table.name])
        table = std_metadata.tables.get(table.name)
        assert table is not None
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
