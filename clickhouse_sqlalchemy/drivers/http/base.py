
from ..base import ClickHouseDialect, ClickHouseExecutionContextBase
from . import connector


# Export connector version
VERSION = (0, 0, 2, None)

FORMAT_SUFFIX = 'FORMAT TabSeparatedWithNamesAndTypes'


class ClickHouseExecutionContext(ClickHouseExecutionContextBase):
    def pre_exec(self):
        # TODO: refactor
        if not self.isinsert and not self.isddl:
            self.statement += ' ' + FORMAT_SUFFIX


class ClickHouseDialect_http(ClickHouseDialect):
    driver = 'http'
    execution_ctx_cls = ClickHouseExecutionContext

    @classmethod
    def dbapi(cls):
        return connector

    def create_connect_args(self, url):
        kwargs = {}
        protocol = url.query.pop('protocol', 'http')
        port = url.port or 8123
        db_name = url.database or 'default'
        endpoint = url.query.pop('endpoint', '')

        kwargs.update(url.query)

        db_url = '%s://%s:%d/%s' % (protocol, url.host, port, endpoint)

        return (db_url, db_name, url.username, url.password), kwargs

    def _execute(self, connection, sql):
        sql += ' ' + FORMAT_SUFFIX
        return connection.execute(sql)

    def _get_server_version_info(self, connection):
        version = connection.scalar(
            'select version() {}'.format(FORMAT_SUFFIX)
        )
        return tuple(int(x) for x in version.split('.'))


dialect = ClickHouseDialect_http
