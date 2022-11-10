.. _connection:

Connection configuration
========================

ClickHouse SQLAlchemy uses the following syntax for the connection string:

    .. code-block::

     clickhouse<+driver>://<user>:<password>@<host>:<port>/<database>[?key=value..]

Where:

- **driver** is driver to use. Possible choices: ``http``, ``native``, ``asynch``.
  ``http`` is default. When you omit driver http is used.
- **database** is database connect to. Default is ``default``.
- **user** is database user. Defaults to ``'default'``.
- **password** of the user. Defaults to ``''`` (no password).
- **port** can be customized if ClickHouse server is listening on non-standard
  port.

Additional parameters are passed to driver.

Common options
--------------

- **engine_reflection** controls table engine reflection during table reflection.
  Engine reflection can be very slow if you have thousand of tables. You can
  disable reflection by setting this parameter to ``false``. Possible choices:
  ``true``/``false``. Default is ``true``.
- **server_version** can be used for eliminating initialization
  ``select version()`` query. Generally you shouldn't set this parameter and
  server version will be detected automatically.


Driver options
--------------

There are several options can be specified in query string.

HTTP
~~~~

- **port** is port ClickHouse server is bound to. Default is ``8123``.
- **timeout** in seconds. There is no timeout by default.
- **protocol** to use. Possible choices: ``http``, ``https``. ``http`` is default.
- **verify** controls certificate verification in ``https`` protocol.
  Possible choices: ``true``/``false``. Default is ``true``.

Simple DSN example:

    .. code-block:: RST

         clickhouse+http://host/db

DSN example for ClickHouse https port:

    .. code-block:: RST

         clickhouse+http://user:password@host:8443/db?protocol=https

When you are using `nginx` as proxy server for ClickHouse server connection
string might look like:

    .. code-block:: RST

         clickhouse+http://user:password@host:8124/test?protocol=https

Where ``8124`` is proxy port.

If you need control over the underlying HTTP connection, pass a `requests.Session
<https://requests.readthedocs.io/en/master/user/advanced/#session-objects>`_ instance
to ``create_engine()``, like so:

    .. code-block:: python

        from sqlalchemy import create_engine
        from requests import Session

        engine = create_engine(
            'clickhouse+http://localhost/test',
            connect_args={'http_session': Session()}
        )


Native
~~~~~~

Please note that native connection **is not encrypted**. All data including
user/password is transferred in plain text. You should use this connection over
SSH or VPN (for example) while communicating over untrusted network.

Simple DSN example:

    .. code-block:: RST

        clickhouse+native://host/db

All connection string parameters are proxied to ``clickhouse-driver``.
See it's `parameters <https://clickhouse-driver.readthedocs.io/en/latest/api.html#clickhouse_driver.connection.Connection>`_.

Example DSN with LZ4 compression secured with Let's Encrypt certificate on server side:

    .. code-block:: python

        import certify

        dsn = (
            'clickhouse+native://user:pass@host/db?compression=lz4&'
            'secure=True&ca_certs={}'.format(certify.where())
        )

Example with multiple hosts

    .. code-block:: RST

        clickhouse+native://wronghost/default?alt_hosts=localhost:9000


Asynch
~~~~~~

Same as Native.

Simple DSN example:

    .. code-block:: RST

        clickhouse+asynch://host/db

All connection string parameters are proxied to ``asynch``.
See it's `parameters <https://github.com/long2ice/asynch/blob/dev/asynch/connection.py>`_.