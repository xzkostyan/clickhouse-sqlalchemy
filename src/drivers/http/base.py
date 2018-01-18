
from ..base import ClickHouseDialect, ClickHouseExecutionContextBase
from . import connector


# Export connector version
VERSION = (0, 0, 2, None)


def add_format(sql):
    dont_add = ['alter', 'drop', 'create']
    if not any([sql[:len(x)].lower() == x for x in dont_add]):
        return sql + ' FORMAT TabSeparatedWithNamesAndTypes'
    return sql


class ClickHouseExecutionContext(ClickHouseExecutionContextBase):
    def pre_exec(self):
        # TODO: refactor
        if not self.isinsert and not self.isddl:
            self.statement = add_format(self.statement)


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

        kwargs.update(url.query)

        db_url = '%s://%s:%d/' % (protocol, url.host, port)

        return (db_url, db_name, url.username, url.password), kwargs

    def _execute(self, connection, sql):
        return connection.execute(add_format(sql))


dialect = ClickHouseDialect_http
