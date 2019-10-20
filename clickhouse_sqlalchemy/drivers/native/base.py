
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
        url.drivername = 'clickhouse'

        return (str(url), ), {}

    def _execute(self, connection, sql):
        return connection.execute(sql)

    def _get_server_version_info(self, connection):
        version = connection.scalar('select version()')
        return tuple(int(x) for x in version.split('.'))


dialect = ClickHouseDialect_native
