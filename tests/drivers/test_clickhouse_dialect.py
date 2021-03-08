from sqlalchemy import Column, create_engine, inspect

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

    @property
    def connection(self):
        return self.session.connection()

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

    def test_has_table_with_schema(self):
        self.assertFalse(
            self.dialect.has_table(self.session, 'bad', schema='system')
        )
        self.assertTrue(
            self.dialect.has_table(self.session, 'columns', schema='system')
        )

    def test_get_table_names(self):
        self.table.create(self.session.bind)
        db_tables = self.dialect.get_table_names(self.connection)
        self.assertIn(self.table.name, db_tables)

    def test_get_table_names_with_schema(self):
        self.table.create(self.session.bind)
        db_tables = self.dialect.get_table_names(self.connection, 'system')
        self.assertIn('columns', db_tables)

    def test_get_view_names(self):
        self.table.create(self.session.bind)
        db_views = self.dialect.get_view_names(self.connection)
        self.assertNotIn(self.table.name, db_views)

    def test_get_view_names_with_schema(self):
        self.table.create(self.session.bind)
        db_views = self.dialect.get_view_names(self.connection, test_database)
        self.assertNotIn(self.table.name, db_views)

    def test_reflecttable(self):
        self.table.create(self.session.bind)
        meta = self.metadata()
        insp = inspect(self.session.bind)
        reflected_table = Table(self.table.name, meta)
        insp.reflecttable(reflected_table, None)

        self.assertEqual(self.table.name, reflected_table.name)

    def test_reflecttable_with_schema(self):
        # Imitates calling sequence for clients like Superset that look
        # across schemas.
        meta = self.metadata()
        insp = inspect(self.session.bind)
        reflected_table = Table('columns', meta, schema='system')
        insp.reflecttable(reflected_table, None)

        self.assertEqual(reflected_table.name, 'columns')
        self.assertEqual(reflected_table.schema, 'system')

    def test_get_schema_names(self):
        schemas = self.dialect.get_schema_names(self.session)
        self.assertIn(test_database, schemas)

    def test_columns_compilation(self):
        # should not raise UnsupportedCompilationError
        col = Column('x', types.Nullable(types.Int32))
        self.assertEqual(str(col.type), 'Nullable(Int32)')


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
