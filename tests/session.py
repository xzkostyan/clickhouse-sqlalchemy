import re

from sqlalchemy import create_engine

from clickhouse_sqlalchemy import make_session
from tests.config import uri, native_uri, system_native_uri

session = make_session(create_engine(uri))

native_session = make_session(create_engine(native_uri))

system_native_session = make_session(create_engine(system_native_uri))


class MockedEngine(object):
    def __init__(self, engine_session=None):
        self.buffer = buffer = []

        engine_session = engine_session or session

        self.dialect_cls = engine_session.bind.dialect
        self.prev_do_execute = self.dialect_cls.do_execute
        self.prev_do_executemany = self.dialect_cls.do_executemany

        def do_executemany(cursor, statement, parameters, context=None):
            buffer.append(statement)

        def do_execute(cursor, statement, parameters, context=None):
            buffer.append(statement)

        self.dialect_cls.do_execute = do_execute
        self.dialect_cls.do_executemany = do_executemany

    def assert_sql(self, stmts):
        recv = [re.sub(r'[\n\t]', '', str(s)) for s in self.buffer]
        assert recv == stmts, recv

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.dialect_cls.do_execute = self.prev_do_execute
        self.dialect_cls.do_executemany = self.prev_do_executemany


mocked_engine = MockedEngine
