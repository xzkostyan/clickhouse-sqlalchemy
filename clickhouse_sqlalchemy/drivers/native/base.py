from urllib.parse import quote

from sqlalchemy.sql.elements import TextClause
from sqlalchemy.util import asbool

from . import connector
from ..base import (
    ClickHouseDialect, ClickHouseExecutionContextBase, ClickHouseSQLCompiler,
)
from sqlalchemy.engine.interfaces import ExecuteStyle
from sqlalchemy import __version__ as sqlalchemy_version

# Export connector version
VERSION = (0, 0, 2, None)

sqlalchemy_version = tuple(
    (int(x) if x.isdigit() else x) for x in sqlalchemy_version.split('.')
)


class ClickHouseExecutionContext(ClickHouseExecutionContextBase):
    def pre_exec(self):
        # Always do executemany on INSERT with VALUES clause.
        if (self.isinsert and self.compiled.statement.select is None and
                self.parameters != [{}]):
            self.execute_style = ExecuteStyle.EXECUTEMANY


class ClickHouseNativeSQLCompiler(ClickHouseSQLCompiler):

    def visit_insert(self, insert_stmt, asfrom=False, **kw):
        rv = super(ClickHouseNativeSQLCompiler, self).visit_insert(
            insert_stmt, asfrom=asfrom, **kw)

        if kw.get('literal_binds') or insert_stmt._values:
            return rv

        pos = rv.lower().rfind('values (')
        # Remove (%s)-templates from VALUES clause if exists.
        # ClickHouse server since version 19.3.3 parse query after VALUES and
        # allows inplace parameters.
        # Example: INSERT INTO test (x) VALUES (1), (2).
        if pos != -1:
            rv = rv[:pos + 6]
        return rv


class ClickHouseDialect_native(ClickHouseDialect):
    driver = 'native'
    execution_ctx_cls = ClickHouseExecutionContext
    statement_compiler = ClickHouseNativeSQLCompiler

    supports_statement_cache = True

    @classmethod
    def import_dbapi(cls):
        return connector

    def create_connect_args(self, url):
        use_quote = sqlalchemy_version < (2, 0, 24)

        url = url.set(drivername='clickhouse')
        if url.username:
            username = quote(url.username) if use_quote else url.username
            url = url.set(username=username)

        if url.password:
            password = quote(url.password) if use_quote else url.password
            url = url.set(password=password)

        self.engine_reflection = asbool(
            url.query.get('engine_reflection', 'true')
        )

        return (url.render_as_string(hide_password=False), ), {}

    def _execute(self, connection, sql, scalar=False, **kwargs):
        if isinstance(sql, str):
            # Makes sure the query will go through the
            # `ClickHouseExecutionContext` logic.
            sql = TextClause(sql)
        f = connection.scalar if scalar else connection.execute
        return f(sql, kwargs)


dialect = ClickHouseDialect_native
