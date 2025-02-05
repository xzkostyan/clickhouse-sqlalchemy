import re

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine

from clickhouse_sqlalchemy import make_session
from tests.config import http_uri, native_uri, system_native_uri, asynch_uri, \
    system_asynch_uri

http_engine = create_engine(http_uri)
http_session = make_session(http_engine)
http_stream_session = make_session(create_engine(http_uri + '?stream=1'))
native_engine = create_engine(native_uri)
native_session = make_session(native_engine)
asynch_engine = create_async_engine(asynch_uri)
asynch_session = make_session(asynch_engine, is_async=True)

system_native_session = make_session(create_engine(system_native_uri))
system_asynch_session = make_session(
    create_async_engine(system_asynch_uri),
    is_async=True
)


class MockedEngine:

    prev_do_execute = None
    prev_do_executemany = None
    prev_get_server_version_info = None
    prev_get_default_schema_name = None

    def __init__(self, session=None):
        self._buffer = []

        if session is None:
            session = make_session(create_engine(http_uri))

        self.session = session
        self.dialect_cls = session.bind.dialect.__class__

    @property
    def history(self):
        return [re.sub(r'[\n\t]', '', str(ssql)) for ssql in self._buffer]

    def __enter__(self):
        self.prev_do_execute = self.dialect_cls.do_execute
        self.prev_do_executemany = self.dialect_cls.do_executemany
        self.prev_get_server_version_info = \
            self.dialect_cls._get_server_version_info
        self.prev_get_default_schema_name = \
            self.dialect_cls._get_default_schema_name

        def do_executemany(
                instance, cursor, statement, parameters, context=None):
            self._buffer.append(statement)

        def do_execute(instance, cursor, statement, parameters, context=None):
            self._buffer.append(statement)

        def get_server_version_info(*args, **kwargs):
            return (19, 16, 2, 2)

        def get_default_schema_name(*args, **kwargs):
            return 'test'

        self.dialect_cls.do_execute = do_execute
        self.dialect_cls.do_executemany = do_executemany
        self.dialect_cls._get_server_version_info = get_server_version_info
        self.dialect_cls._get_default_schema_name = get_default_schema_name

        return self

    def __exit__(self, *exc_info):
        self.dialect_cls.do_execute = self.prev_do_execute
        self.dialect_cls.do_executemany = self.prev_do_executemany
        self.dialect_cls._get_server_version_info = \
            self.prev_get_server_version_info
        self.dialect_cls._get_default_schema_name = \
            self.prev_get_default_schema_name


mocked_engine = MockedEngine
