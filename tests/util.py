from functools import wraps


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
