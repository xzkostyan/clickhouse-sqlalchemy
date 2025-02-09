from sqlalchemy import Column, create_engine, inspect, text
from sqlalchemy.ext.asyncio import create_async_engine

from clickhouse_sqlalchemy import make_session, engines, types, Table
from clickhouse_sqlalchemy.drivers.base import ClickHouseDialect
from tests.testcase import BaseTestCase, BaseAsynchTestCase
from tests.config import (
    system_http_uri, system_native_uri, system_asynch_uri,
    database as test_database,
)
from tests.session import asynch_session
from tests.util import with_native_and_http_sessions, require_server_version, \
    run_async


@with_native_and_http_sessions
class ClickHouseDialectTestCase(BaseTestCase):

    @property
    def dialect(self) -> ClickHouseDialect:
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
        self.table_with_primary = Table(
            'test_primary_table',
            self.metadata(),
            Column('x', types.Int32, primary_key=True),
            engines.MergeTree(
                primary_key=['x']
            )
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

    def test_reflect_table(self):
        self.table.create(self.session.bind)
        meta = self.metadata()
        insp = inspect(self.session.bind)
        reflected_table = Table(self.table.name, meta)
        insp.reflect_table(reflected_table, None)

        self.assertEqual(self.table.name, reflected_table.name)

    def test_reflect_table_with_schema(self):
        # Imitates calling sequence for clients like Superset that look
        # across schemas.
        meta = self.metadata()
        insp = inspect(self.session.bind)
        reflected_table = Table('columns', meta, schema='system')
        insp.reflect_table(reflected_table, None)

        self.assertEqual(reflected_table.name, 'columns')
        self.assertEqual(reflected_table.schema, 'system')
        if self.server_version >= (18, 16, 0):
            self.assertIsNone(reflected_table.engine)

    def test_get_schema_names(self):
        schemas = self.dialect.get_schema_names(self.session)
        self.assertIn(test_database, schemas)

    def test_columns_compilation(self):
        # should not raise UnsupportedCompilationError
        col = Column('x', types.Nullable(types.Int32))
        self.assertEqual(str(col.type), 'Nullable(Int32)')

    @require_server_version(18, 16, 0)
    def test_primary_keys(self):
        self.table_with_primary.create(self.session.bind)
        con = self.dialect.get_pk_constraint(self.session,
                                             self.table_with_primary.name)
        self.assertIsNotNone(con,
                             "Table should contain primary key constrain")
        self.assertTrue("constrained_columns" in con)
        self.assertEqual(("x",), con["constrained_columns"])

    @require_server_version(19, 16, 2)
    def test_empty_set_expr(self):
        numbers = Table(
            'numbers', self.metadata(),
            Column('number', types.UInt64, primary_key=True),
            schema='system'
        )

        rv = self.session.query(numbers.c.number) \
            .filter(numbers.c.number.in_([])) \
            .all()

        self.assertEqual(len(rv), 0)

    def test_enum_type_with_illegal_characters(self):
        empty_string = ''
        mro = 'mro'
        try:
            self.dialect._get_column_type(
                'col_name',
                f"Enum8('f' = -1, '{empty_string}' = 0, 'ok' = 1, '{mro}' = 2)"
            )
            self.dialect._get_column_type(
                'col_name',
                f"Enum8('f' = -1, '{mro}' = 0, 'ok' = 1, '{empty_string}' = 2)"
            )
        except ValueError as e:
            self.fail(f"Enum options parsing failed: {e}")


class ClickHouseAsynchDialectTestCase(BaseAsynchTestCase):

    session = asynch_session

    def setUp(self):
        super(ClickHouseAsynchDialectTestCase, self).setUp()
        self.test_metadata = self.metadata()
        self.table = Table(
            'test_exists_table',
            self.test_metadata,
            Column('x', types.Int32, primary_key=True),
            engines.Memory()
        )
        run_async(self.connection.run_sync)(self.test_metadata.drop_all)

    def tearDown(self):
        run_async(self.connection.run_sync)(self.test_metadata.drop_all)
        super(ClickHouseAsynchDialectTestCase, self).tearDown()

    async def run_inspector_method(self, method, *args, **kwargs):
        def _run(conn):
            insp = inspect(conn)
            return getattr(insp, method)(*args, **kwargs)

        return await self.run_sync(_run)

    async def test_has_table(self):
        self.assertFalse(
            await self.run_inspector_method('has_table', self.table.name)
        )

        await self.run_sync(self.test_metadata.create_all)

        self.assertTrue(
            await self.run_inspector_method('has_table', self.table.name)
        )

    async def test_has_table_with_schema(self):
        self.assertFalse(
            await self.run_inspector_method(
                'has_table',
                'bad',
                schema='system'
            )
        )
        self.assertTrue(
            await self.run_inspector_method(
                'has_table',
                'columns',
                schema='system'
            )
        )

    async def test_get_table_names(self):
        await self.run_sync(self.test_metadata.create_all)

        db_tables = await self.run_inspector_method('get_table_names')

        self.assertIn(self.table.name, db_tables)

    async def test_get_table_names_with_schema(self):
        await self.run_sync(self.test_metadata.create_all)

        db_tables = await self.run_inspector_method(
            'get_table_names',
            schema='system'
        )

        self.assertIn('columns', db_tables)

    async def test_get_view_names(self):
        await self.run_sync(self.test_metadata.create_all)

        db_views = await self.run_inspector_method('get_view_names')

        self.assertNotIn(self.table.name, db_views)

    async def test_get_view_names_with_schema(self):
        await self.run_sync(self.test_metadata.create_all)

        db_views = await self.run_inspector_method(
            'get_view_names',
            test_database
        )

        self.assertNotIn(self.table.name, db_views)

    async def test_reflect_table(self):
        await self.run_sync(self.test_metadata.create_all)
        meta = self.metadata()

        reflected_table = Table(self.table.name, meta)
        await self.run_inspector_method('reflect_table', reflected_table, None)

        self.assertEqual(self.table.name, reflected_table.name)

    async def test_reflect_table_with_schema(self):
        # Imitates calling sequence for clients like Superset that look
        # across schemas.
        meta = self.metadata()
        reflected_table = Table('columns', meta, schema='system')
        await self.run_inspector_method('reflect_table', reflected_table, None)

        self.assertEqual(reflected_table.name, 'columns')
        self.assertEqual(reflected_table.schema, 'system')
        if self.server_version >= (18, 16, 0):
            self.assertIsNone(reflected_table.engine)

    async def test_get_schema_names(self):
        schemas = await self.run_inspector_method('get_schema_names')
        self.assertIn(test_database, schemas)

    async def test_columns_compilation(self):
        # should not raise UnsupportedCompilationError
        col = Column('x', types.Nullable(types.Int32))
        self.assertEqual(str(col.type), 'Nullable(Int32)')

    @require_server_version(19, 16, 2, is_async=True)
    async def test_empty_set_expr(self):
        numbers = Table(
            'numbers', self.metadata(),
            Column('number', types.UInt64, primary_key=True),
            schema='system'
        )

        def _run(session):
            return (
                session
                .query(numbers.c.number)
                .filter(numbers.c.number.in_([]))
                .all()
            )

        rv = await self.session.run_sync(_run)

        self.assertEqual(len(rv), 0)


class CachedServerVersionTestCase(BaseTestCase):

    def _test_server_version_any(self, uri):
        engine_session = make_session(create_engine(
            system_http_uri,
            connect_args=dict(server_version='123.45.67.89')
        ))
        # Connection and version get initialized on first query:
        engine_session.scalar(text('select 1'))
        ver = engine_session.get_bind().dialect.server_version_info
        self.assertEqual(ver, (123, 45, 67, 89))

    def test_server_version_http(self):
        return self._test_server_version_any(system_http_uri)

    def test_server_version_native(self):
        return self._test_server_version_any(system_native_uri)


class CachedServerVersionAsyncTestCase(BaseAsynchTestCase):
    async def test_server_version_asynch(self):
        engine_session = make_session(create_async_engine(
            system_asynch_uri,
            connect_args=dict(server_version='123.45.67.89')
        ), is_async=True)
        await engine_session.scalar(text('select 1'))
        ver = engine_session.get_bind().dialect.server_version_info
        self.assertEqual(ver, (123, 45, 67, 89))

        await engine_session.close()
