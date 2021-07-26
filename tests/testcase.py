import re
from contextlib import contextmanager
from unittest import TestCase

from sqlalchemy import MetaData
from sqlalchemy.orm import Query

from tests.config import database, host, port, http_port, user, password
from tests.session import http_session, native_session, \
    system_native_session, http_engine
from tests.util import skip_by_server_version


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

    def _compile(self, clause, bind=None, literal_binds=False):
        if bind is None:
            bind = self.session.bind
        if isinstance(clause, Query):
            context = clause._compile_context()
            context.statement.use_labels = True
            clause = context.statement

        kw = {}
        compile_kwargs = {}
        if literal_binds:
            compile_kwargs['literal_binds'] = True

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
