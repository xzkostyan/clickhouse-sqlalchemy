import re
from contextlib import contextmanager, asynccontextmanager
from unittest import TestCase

from sqlalchemy import MetaData
from sqlalchemy.orm import Query

from tests.config import database, host, port, http_port, user, password
from tests.session import http_session, native_session, \
    system_native_session, http_engine, asynch_session, system_asynch_session
from tests.util import skip_by_server_version, run_async


class BaseAbstractTestCase(object):
    """ Supporting code for tests """
    required_server_version = None
    server_version = None

    host = host
    port = None
    database = database
    user = user
    password = password

    strip_spaces = re.compile(r'[\n\t]')
    session = native_session

    @classmethod
    def metadata(cls, session=None):
        if session is None:
            session = cls.session
        return MetaData(bind=session.bind)

    def _compile(self, clause, bind=None, literal_binds=False,
                 render_postcompile=False):
        if bind is None:
            bind = self.session.bind
        if isinstance(clause, Query):
            clause = clause._statement_20()

        kw = {}
        compile_kwargs = {}
        if literal_binds:
            compile_kwargs['literal_binds'] = True
        if render_postcompile:
            compile_kwargs['render_postcompile'] = True

        if compile_kwargs:
            kw['compile_kwargs'] = compile_kwargs

        return clause.compile(dialect=bind.dialect, **kw)

    def compile(self, clause, **kwargs):
        return self.strip_spaces.sub(
            '', str(self._compile(clause, **kwargs))
        )

    @contextmanager
    def create_table(self, table):
        table.drop(bind=self.session.bind, if_exists=True)
        table.create(bind=self.session.bind)
        try:
            yield
        finally:
            table.drop(bind=self.session.bind)


class BaseTestCase(BaseAbstractTestCase, TestCase):
    """ Actually tests """

    @classmethod
    def setUpClass(cls):
        # System database is always present.
        system_native_session.execute(
            'DROP DATABASE IF EXISTS {}'.format(cls.database)
        )
        system_native_session.execute(
            'CREATE DATABASE {}'.format(cls.database)
        )

        version = system_native_session.execute('SELECT version()').fetchall()
        cls.server_version = tuple(int(x) for x in version[0][0].split('.'))

        super(BaseTestCase, cls).setUpClass()

    def setUp(self):
        super(BaseTestCase, self).setUp()

        required = self.required_server_version

        if required and required > self.server_version:
            skip_by_server_version(self, self.required_server_version)


class BaseAsynchTestCase(BaseTestCase):
    session = asynch_session

    @classmethod
    @run_async
    async def setUpClass(cls):
        # System database is always present.
        await system_asynch_session.execute(
            'DROP DATABASE IF EXISTS {}'.format(cls.database)
        )
        await system_asynch_session.execute(
            'CREATE DATABASE {}'.format(cls.database)
        )

        version = (
            await system_asynch_session.execute('SELECT version()')
        ).fetchall()
        cls.server_version = tuple(int(x) for x in version[0][0].split('.'))

        super(BaseTestCase, cls).setUpClass()

    @asynccontextmanager
    async def create_table(self, table):
        await self.run_sync(table.metadata.drop_all)
        await self.run_sync(table.metadata.create_all)

        try:
            yield
        finally:
            await self.run_sync(table.metadata.drop_all)

    async def get_connection(self):
        return await self.session.connection()

    async def run_sync(self, f):
        conn = await self.get_connection()
        return await conn.run_sync(f)

    async def session_scalar(self, statement):
        def wrapper(session):
            return session.query(statement).scalar()

        return await self.session.run_sync(wrapper)


class HttpSessionTestCase(BaseTestCase):
    """ Explicitly HTTP-based session Test Case """

    port = http_port
    session = http_session


class HttpEngineTestCase(BaseTestCase):
    """ Explicitly HTTP-based session Test Case """

    port = http_port
    engine = http_engine


class NativeSessionTestCase(BaseTestCase):
    """ Explicitly Native-protocol-based session Test Case """

    port = port
    session = native_session


class AsynchSessionTestCase(BaseAsynchTestCase):
    """ Explicitly Native-protocol-based async session Test Case """

    port = port
    session = asynch_session


class CompilationTestCase(BaseTestCase):
    """ Test Case that should be used only for SQL generation """

    session = native_session

    @classmethod
    def setUpClass(cls):
        super(CompilationTestCase, cls).setUpClass()

        cls._session_connection = cls.session.connection
        cls._session_execute = cls.session.execute

        cls.session.execute = None
        cls.session.connection = None

    @classmethod
    def tearDownClass(cls):
        cls.session.execute = cls._session_execute
        cls.session.connection = cls._session_connection

        super(CompilationTestCase, cls).tearDownClass()
