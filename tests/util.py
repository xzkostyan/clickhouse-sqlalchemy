import asyncio
from contextlib import contextmanager
from functools import wraps

from parameterized import parameterized_class
from sqlalchemy.util.concurrency import greenlet_spawn

from tests.session import http_session, native_session


def skip_by_server_version(testcase, version_required):
    testcase.skipTest(
        'Mininum revision required: {}'.format(
            '.'.join(str(x) for x in version_required)
        )
    )


async def _get_version(conn):
    cursor = await greenlet_spawn(lambda: conn.cursor())
    await greenlet_spawn(lambda: cursor.execute('SELECT version()'))
    version = cursor.fetchall()[0][0].split('.')
    return tuple(int(x) for x in version[:3])


def require_server_version(*version_required, is_async=False):
    def check(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            self = args[0]
            if not is_async:
                conn = self.session.bind.raw_connection()
            else:
                async def _get_conn(session):
                    return await session.bind.raw_connection()

                conn = run_async(lambda: _get_conn(self.session))()

            dialect = self.session.bind.dialect.name
            if dialect in ['clickhouse+native', 'clickhouse+asynch']:
                i = conn.transport.connection.server_info
                current = (i.version_major, i.version_minor, i.version_patch)

            else:
                if not is_async:
                    cursor = conn.cursor()
                    cursor.execute('SELECT version()')
                    version = cursor.fetchall()[0][0].split('.')
                    current = tuple(int(x) for x in version[:3])

                else:
                    current = run_async(_get_version)(conn)

            conn.close()
            if version_required <= current:
                return f(*args, **kwargs)
            else:
                self.skipTest(
                    'Mininum revision required: {}'.format(
                        '.'.join(str(x) for x in version_required)
                    )
                )

        return wrapper

    return check


@contextmanager
def mock_object_attr(dialect, attr, new_value):
    old_value = getattr(dialect, attr)
    setattr(dialect, attr, new_value)

    try:
        yield
    finally:
        setattr(dialect, attr, old_value)


def class_name_func(cls, num, params_dict):
    suffix = 'HTTP' if params_dict['session'] is http_session else 'Native'
    return cls.__name__ + suffix


def run_async(f):
    """
    Decorator to create asyncio context for asyncio methods or functions.
    """
    @wraps(f)
    def g(*args, **kwargs):
        coro = f(*args, **kwargs)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            try:
                loop = asyncio.get_event_loop_policy().get_event_loop()
            except DeprecationWarning:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

        return loop.run_until_complete(coro)
    return g


with_native_and_http_sessions = parameterized_class([
    {'session': http_session},
    {'session': native_session}
], class_name_func=class_name_func)
