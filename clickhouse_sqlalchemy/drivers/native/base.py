
from clickhouse_driver import dbapi

from ..base import ClickHouseDialect, ClickHouseExecutionContextBase


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
        return dbapi

    def create_connect_args(self, url):
        url.drivername = 'clickhouse'

        return (str(url), ), {}

    def _execute(self, connection, sql):
        return connection.execute(sql)

    def _get_server_version_info(self, connection):
        version = connection.scalar('select version()')
        return tuple(int(x) for x in version.split('.'))

    def _make_external_tables(self, dialect, execution_options):
        external_tables = execution_options.get('external_tables')
        if external_tables is None:
            return

        tables = []
        type_compiler = dialect.type_compiler

        for table in external_tables:
            structure = []
            for c in table.columns:
                type_ = type_compiler.process(c.type, type_expression=c)
                structure.append((c.name, type_))

            tables.append({
                'name': table.name,
                'structure': structure,
                'data': table.dialect_options['clickhouse']['data']
            })

        return tables

    def prepare_cursor(self, cursor, context):
        if context:
            execution_options = context.execution_options

            external_tables = self._make_external_tables(
                context.dialect, execution_options
            )
        else:
            execution_options = {}
            external_tables = None

        stream_results = execution_options.get('stream_results', False)
        if stream_results:
            cursor.set_stream_results(
                stream_results, execution_options['max_row_buffer']
            )

        settings = execution_options.get('settings')
        if settings:
            cursor.set_settings(settings)

        types_check = execution_options.get('types_check', False)
        cursor.set_types_check(types_check)

        for t in (external_tables or []):
            cursor.set_external_table(t['name'], t['structure'], t['data'])

    def do_executemany(self, cursor, statement, parameters, context=None):
        self.prepare_cursor(cursor, context)
        cursor.executemany(statement, parameters)

    def do_execute(self, cursor, statement, parameters, context=None):
        self.prepare_cursor(cursor, context)
        cursor.execute(statement, parameters)


dialect = ClickHouseDialect_native
