from clickhouse_driver import defines
from sqlalchemy.util import asbool

from . import connector
from ..base import ClickHouseDialect, ClickHouseExecutionContextBase

# Export connector version
VERSION = (0, 0, 2, None)


class ClickHouseExecutionContext(ClickHouseExecutionContextBase):
    def pre_exec(self):
        # Always do executemany on INSERT with VALUES clause.
        if self.isinsert and self.compiled.statement.select is None:
            self.executemany = True


class ClickHouseDialect_native(ClickHouseDialect):
    driver = 'native'
    execution_ctx_cls = ClickHouseExecutionContext

    @classmethod
    def dbapi(cls):
        return connector

    def create_connect_args(self, url):
        kwargs = {}
        query = url.query

        secure = query.get('secure')
        if secure is not None:
            query['secure'] = asbool(secure)

        verify = query.get('verify')
        if verify is not None:
            query['verify'] = asbool(verify)

        kwargs.update(query)

        if url.port:
            port = url.port
        else:
            if query.get('secure'):
                port = defines.DEFAULT_SECURE_PORT
            else:
                port = defines.DEFAULT_PORT

        db_name = url.database or 'default'
        username = url.username or 'default'
        password = url.password or ''

        return (url.host, port, db_name, username, password), kwargs

    def _execute(self, connection, sql):
        return connection.execute(sql)

    def _get_server_version_info(self, connection):
        version = connection.scalar('select version()')
        return tuple(int(x) for x in version.split('.'))


dialect = ClickHouseDialect_native
