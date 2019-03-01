from sqlalchemy import Column

from clickhouse_sqlalchemy import engines, types, Table
from tests.session import native_session
from tests.testcase import BaseTestCase
from tests.config import database as test_database


class ClickHouseDialectTestCase(BaseTestCase):
    @property
    def dialect(self):
        return native_session.bind.dialect

    def setUp(self):
        self.table = Table(
            'test_exists_table',
            self.metadata(),
            Column('x', types.Int32, primary_key=True),
            engines.Memory()
        )
        self.table.drop(native_session.bind, if_exists=True)

    def tearDown(self):
        self.table.drop(native_session.bind, if_exists=True)

    def test_has_table(self):
        self.assertFalse(
            self.dialect.has_table(native_session,
                                   self.table.name)
        )

        self.table.create(native_session.bind)
        self.assertTrue(
            self.dialect.has_table(native_session,
                                   self.table.name)
        )

    def test_get_table_names(self):
        self.table.create(native_session.bind)
        db_tables = self.dialect.get_table_names(native_session)
        assert self.table.name in db_tables

    def test_get_schema_names(self):
        schemas = self.dialect.get_schema_names(native_session)
        assert test_database in schemas
