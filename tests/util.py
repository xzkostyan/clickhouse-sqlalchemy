from contextlib import contextmanager
from functools import wraps

from parameterized import parameterized_class

from tests.session import http_session, native_session


def skip_by_server_version(testcase, version_required):
    testcase.skipTest(
        'Mininum revision required: {}'.format(
            '.'.join(str(x) for x in version_required)
        )
    )


def require_server_version(*version_required):
    def check(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            self = args[0]
            conn = self.session.bind.raw_connection()

            dialect = self.session.bind.dialect.name
            if dialect == 'clickhouse+native':
                i = conn.transport.connection.server_info
                current = (i.version_major, i.version_minor, i.version_patch)
            else:
                cursor = conn.cursor()
                cursor.execute(
                    'SELECT version() FORMAT TabSeparatedWithNamesAndTypes'
                )
                version = cursor.fetchall()[0][0].split('.')
                current = tuple(int(x) for x in version[:3])

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


with_native_and_http_sessions = parameterized_class([
    {'session': http_session},
    {'session': native_session}
], class_name_func=class_name_func)
