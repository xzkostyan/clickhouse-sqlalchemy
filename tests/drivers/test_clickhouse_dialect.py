from sqlalchemy import Column, create_engine

from clickhouse_sqlalchemy import make_session, engines, types, Table
from tests.testcase import BaseTestCase
from tests.config import (
    system_http_uri, system_native_uri,
    database as test_database,
)
from tests.util import with_native_and_http_sessions


@with_native_and_http_sessions
class ClickHouseDialectTestCase(BaseTestCase):

    @property
    def dialect(self):
        return self.session.bind.dialect

    def setUp(self):
        super(ClickHouseDialectTestCase, self).setUp()
        self.table = Table(
            'test_exists_table',
            self.metadata(),
            Column('x', types.Int32, primary_key=True),
            engines.Memory()
        )
        self.table.drop(self.session.bind, if_exists=True)

    def tearDown(self):
        self.table.drop(self.session.bind, if_exists=True)
        super(ClickHouseDialectTestCase, self).tearDown()

    def test_has_table(self):
        self.assertFalse(
            self.dialect.has_table(self.session,
                                   self.table.name)
        )

        self.table.create(self.session.bind)
        self.assertTrue(
            self.dialect.has_table(self.session,
                                   self.table.name)
        )

    def test_get_table_names(self):
        self.table.create(self.session.bind)
        db_tables = self.dialect.get_table_names(self.session)
        self.assertIn(self.table.name, db_tables)

    def test_get_schema_names(self):
        schemas = self.dialect.get_schema_names(self.session)
        self.assertIn(test_database, schemas)


class CachedServerVersionTestCase(BaseTestCase):

    def _test_server_version_any(self, uri):
        engine_session = make_session(create_engine(
            system_http_uri,
            connect_args=dict(server_version='123.45.67.89')))
        # Connection and version get initialized on first query:
        engine_session.scalar('select 1')
        ver = engine_session.get_bind().dialect.server_version_info
        self.assertEqual(ver, (123, 45, 67, 89))

    def test_server_version_http(self):
        return self._test_server_version_any(system_http_uri)

    def test_server_version_native(self):
        return self._test_server_version_any(system_native_uri)
