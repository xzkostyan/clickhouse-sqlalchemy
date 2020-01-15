from functools import wraps


def require_server_version(*version_required):
    def check(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            self = args[0]
            conn = self.session.bind.raw_connection()

            # This only works for native transport, not for http transport.
            transport_conn = getattr(conn.transport, 'connection', None)
            current = None
            if transport_conn is not None:
                info = transport_conn.server_info
                current = (
                    info.version_major,
                    info.version_minor,
                    info.version_patch,
                )

            conn.close()
            if current is not None and version_required <= current:
                return f(*args, **kwargs)
            else:
                self.skipTest(
                    'Mininum revision required: {}'.format(
                        '.'.join(str(x) for x in version_required)
                    )
                )

        return wrapper

    return check
