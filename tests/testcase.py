import re
from contextlib import contextmanager
from unittest import TestCase

from sqlalchemy import MetaData
from sqlalchemy.orm import Query

from tests.config import database, host, port, http_port, user, password
from tests.session import session, native_session, system_native_session


class BaseTestCase(TestCase):
    host = host
    port = http_port
    database = database
    user = user
    password = password

    strip_spaces = re.compile(r'[\n\t]')
    session = session

    @classmethod
    def metadata(cls):
        return MetaData(bind=cls.session.bind)

    def _compile(self, clause, bind=session.bind, literal_binds=False):
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

        return clause.compile(dialect=session.bind.dialect, **kw)

    def compile(self, clause, **kwargs):
        return self.strip_spaces.sub(
            '', str(self._compile(clause, **kwargs))
        )


class NativeSessionTestCase(BaseTestCase):
    port = port

    session = native_session

    @classmethod
    def setUpClass(cls):
        # System database is always present.
        system_native_session.execute(
            'DROP DATABASE IF EXISTS {}'.format(cls.database)
        )
        system_native_session.execute(
            'CREATE DATABASE {}'.format(cls.database)
        )

        super(BaseTestCase, cls).setUpClass()


class TypesTestCase(NativeSessionTestCase):
    @contextmanager
    def create_table(self, table):
        table.drop(bind=self.session.bind, if_exists=True)
        table.create(bind=self.session.bind)
        try:
            yield
        except Exception:
            raise
        finally:
            table.drop(bind=self.session.bind)
