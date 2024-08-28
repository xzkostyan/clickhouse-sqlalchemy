from sqlalchemy.sql.elements import TextClause
from sqlalchemy.pool import AsyncAdaptedQueuePool

from .connector import AsyncAdapt_asynch_dbapi
from ..native.base import ClickHouseDialect_native, ClickHouseExecutionContext

# Export connector version
VERSION = (0, 0, 1, None)


class ClickHouseAsynchExecutionContext(ClickHouseExecutionContext):
    def create_server_side_cursor(self):
        return self.create_default_cursor()


class ClickHouseDialect_asynch(ClickHouseDialect_native):
    driver = 'asynch'
    execution_ctx_cls = ClickHouseAsynchExecutionContext

    is_async = True
    supports_statement_cache = True
    supports_server_side_cursors = True

    @classmethod
    def import_dbapi(cls):
        return AsyncAdapt_asynch_dbapi()

    @classmethod
    def get_pool_class(cls, url):
        return AsyncAdaptedQueuePool

    def _execute(self, connection, sql, scalar=False, **kwargs):
        if isinstance(sql, str):
            # Makes sure the query will go through the
            # `ClickHouseExecutionContext` logic.
            sql = TextClause(sql)
        f = connection.scalar if scalar else connection.execute
        return f(sql, kwargs)

    def do_execute(self, cursor, statement, parameters, context=None):
        cursor.execute(statement, parameters, context)

    def do_executemany(self, cursor, statement, parameters, context=None):
        cursor.executemany(statement, parameters, context)


dialect = ClickHouseDialect_asynch
