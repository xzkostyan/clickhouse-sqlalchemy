import re

import asynch

from sqlalchemy.sql.elements import TextClause
from sqlalchemy.pool import AsyncAdaptedQueuePool

from .connector import AsyncAdapt_asynch_dbapi
from ..native.base import ClickHouseDialect_native, ClickHouseExecutionContext

# Export connector version
VERSION = (0, 0, 1, None)


try:
    from importlib.metadata import version
    _asynch_version = version('asynch')
except Exception:
    _asynch_version = getattr(asynch, '__version__', None) or '0'

_asynch_031 = tuple(
    int(x) if x.isdigit() else 0 for x in str(_asynch_version).split('.')[:3]
) >= (0, 3, 1)
_pyformat_param_re = re.compile(r'%\(([^)]+)\)s')


def _format_asynch_statement(statement):
    return _pyformat_param_re.sub(r'{\1}', statement).replace('%%', '%')


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
        return AsyncAdapt_asynch_dbapi(asynch)

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
        if _asynch_031:
            statement = _format_asynch_statement(statement)
        cursor.execute(statement, parameters, context)

    def do_executemany(self, cursor, statement, parameters, context=None):
        if _asynch_031:
            statement = _format_asynch_statement(statement)
        cursor.executemany(statement, parameters, context)


dialect = ClickHouseDialect_asynch
