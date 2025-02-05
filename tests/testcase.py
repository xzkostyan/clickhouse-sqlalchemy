import re
from contextlib import contextmanager
from unittest import TestCase

from sqlalchemy import MetaData, text
from sqlalchemy.orm import Query

from tests.config import database, host, port, http_port, user, password
from tests.session import http_session, native_session, \
    system_native_session, http_engine, asynch_session, system_asynch_session
from tests.util import skip_by_server_version, run_async


class BaseAbstractTestCase:
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
    def metadata(cls):
        return MetaData()

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
            text(f'DROP DATABASE IF EXISTS {cls.database}')
        )
        system_native_session.execute(
            text(f'CREATE DATABASE {cls.database}')
        )

        version = system_native_session.execute(
            text('SELECT version()')
        ).fetchall()
        cls.server_version = tuple(int(x) for x in version[0][0].split('.'))

        super().setUpClass()

    def setUp(self):
        super().setUp()

        required = self.required_server_version

        if required and required > self.server_version:
            skip_by_server_version(self, self.required_server_version)


class BaseAsynchTestCase(BaseTestCase):
    session = asynch_session

    @classmethod
    def setUpClass(cls):
        # System database is always present.
        run_async(system_asynch_session.execute)(
            text(f'DROP DATABASE IF EXISTS {cls.database}')
        )
        run_async(system_asynch_session.execute)(
            text(f'CREATE DATABASE {cls.database}')
        )

        version = (
            run_async(system_asynch_session.execute)(text('SELECT version()'))
        ).fetchall()
        cls.server_version = tuple(int(x) for x in version[0][0].split('.'))

    def setUp(self):
        self.connection = run_async(self.session.connection)()
        super().setUp()

    def _callTestMethod(self, method):
        return run_async(method)()

    async def run_sync(self, f):
        return await self.connection.run_sync(f)


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
        super().setUpClass()

        cls._session_connection = cls.session.connection
        cls._session_execute = cls.session.execute

        cls.session.execute = None
        cls.session.connection = None

    @classmethod
    def tearDownClass(cls):
        cls.session.execute = cls._session_execute
        cls.session.connection = cls._session_connection

        super().tearDownClass()
