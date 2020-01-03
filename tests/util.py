from functools import wraps


def require_server_version(*version_required):
    def check(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            self = args[0]
            conn = self.session.bind.raw_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT 1')

            # TODO: So dirty. Maybe use direct Client instance and ping?
            i = cursor._client.connection.server_info

            current = (i.version_major, i.version_minor, i.version_patch)
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
