import re

import six
from sqlalchemy import types as sqltypes
from sqlalchemy import exc, util as sa_util
from sqlalchemy.engine import default, reflection
from sqlalchemy.sql import compiler, expression
from sqlalchemy.types import DATE, DATETIME, INTEGER, VARCHAR, FLOAT

from .. import types


# Column spec
colspecs = {}


# Type converters
ischema_names = {
    'Int64': INTEGER,
    'Int32': INTEGER,
    'Int16': INTEGER,
    'Int8': INTEGER,
    'UInt64': INTEGER,
    'UInt32': INTEGER,
    'UInt16': INTEGER,
    'UInt8': INTEGER,
    'Date': DATE,
    'DateTime': DATETIME,
    'Float64': FLOAT,
    'Float32': FLOAT,
    'String': VARCHAR,
    'FixedString': VARCHAR,
    'Enum8': types.Enum8,
    'Enum16': types.Enum16,
    'Array': types.Array
}


class ClickHouseIdentifierPreparer(compiler.IdentifierPreparer):
    def quote_identifier(self, value):
        # Never quote identifiers.
        return self._escape_identifier(value)

    def quote(self, ident, force=None):
        return ident


class ClickHouseCompiler(compiler.SQLCompiler):
    def visit_count_func(self, fn, **kw):
        # count accepts zero arguments.
        return 'count()'

    def visit_case(self, clause, **kwargs):
        text = 'CASE '
        if clause.value is not None:
            text += clause.value._compiler_dispatch(self, **kwargs) + " "
        for cond, result in clause.whens:
            text += 'WHEN ' + cond._compiler_dispatch(
                self, **kwargs
            ) + ' THEN ' + result._compiler_dispatch(
                self, **kwargs) + " "
        if clause.else_ is None:
            raise exc.CompileError('ELSE clause is required in CASE')

        text += 'ELSE ' + clause.else_._compiler_dispatch(
            self, **kwargs
        ) + ' END'
        return text

    def visit_if__func(self, func, **kw):
        return "(%s) ? (%s) : (%s)" % (
            self.process(func.clauses.clauses[0], **kw),
            self.process(func.clauses.clauses[1], **kw),
            self.process(func.clauses.clauses[2], **kw)
        )

    def visit_column(self, column, include_table=True, **kwargs):
        # Columns prefixed with table name are not supported
        return super(ClickHouseCompiler, self).visit_column(
            column, include_table=False, **kwargs
        )

    def limit_clause(self, select, **kw):
        text = ''
        if select._limit_clause is not None:
            text += ' \n LIMIT '
            if select._offset_clause is not None:
                text += self.process(select._offset_clause, **kw) + ', '
            text += self.process(select._limit_clause, **kw)
        else:
            if select._offset_clause is not None:
                raise exc.CompileError('OFFSET without LIMIT is not supported')

        return text


class ClickHouseDDLCompiler(compiler.DDLCompiler):
    def visit_create_column(self, create, **kw):
        column = create.element
        nullable = column.nullable

        # All columns including synthetic PKs must be 'nullable'
        column.nullable = True

        rv = super(ClickHouseDDLCompiler, self).visit_create_column(
            create, **kw
        )
        column.nullable = nullable

        return rv

    def visit_primary_key_constraint(self, constraint):
        # Do not render PKs.
        return ''

    def visit_engine(self, engine):
        compiler = self.sql_compiler

        def compile_param(expr):
            if not isinstance(expr, expression.ColumnClause):
                if not hasattr(expr, 'self_group'):
                    # assuming base type (int, string, etc.)
                    return six.text_type(expr)
                else:
                    expr = expr.self_group()
            return compiler.process(
                expr, include_table=False, literal_binds=True
            )

        engine_params = engine.get_params()
        text = engine.name()
        if not engine_params:
            return text

        text += '('

        compiled_params = []
        for param in engine_params:
            if isinstance(param, tuple):
                compiled = (
                    '(' +
                    ', '.join(compile_param(p) for p in param) +
                    ')'
                )
            else:
                compiled = compile_param(param)

            compiled_params.append(compiled)

        text += ', '.join(compiled_params)

        return text + ')'

    def post_create_table(self, table):
        engine = getattr(table, 'engine', None)

        if not engine:
            raise exc.CompileError("No engine for table '%s'" % table.name)

        return ' ENGINE = ' + self.process(engine)

    def visit_drop_table(self, drop):
        text = '\nDROP TABLE '

        if drop.if_exists:
            text += 'IF EXISTS '

        return text + self.preparer.format_table(drop.element)


class ClickHouseTypeCompiler(compiler.GenericTypeCompiler):
    def visit_string(self, type_, **kw):
        if type_.length is None:
            return 'String'
        else:
            return 'FixedString(%s)' % type_.length

    def visit_array(self, type_, **kw):
        return "Array(%s)" % self.process(type_.item_type, **kw)

    def visit_int8(self, type_, **kw):
        return 'Int8'

    def visit_uint8(self, type_, **kw):
        return 'UInt8'

    def visit_int16(self, type_, **kw):
        return 'Int16'

    def visit_uint16(self, type_, **kw):
        return 'UInt16'

    def visit_int32(self, type_, **kw):
        return 'Int32'

    def visit_uint32(self, type_, **kw):
        return 'UInt32'

    def visit_int64(self, type_, **kw):
        return 'Int64'

    def visit_uint64(self, type_, **kw):
        return 'UInt64'

    def visit_date(self, type_, **kw):
        return 'Date'

    def visit_float32(self, type_, **kw):
        return 'Float32'

    def visit_float64(self, type_, **kw):
        return 'Float64'

    def _render_enum(self, db_type, type_, **kw):
        choices = (
            "'%s' = %d" %
            (x.name.replace("'", "\\'"), x.value) for x in type_.enum_type
        )
        return "%s(%s)" % (db_type, ', '.join(choices))

    def visit_enum8(self, type_, **kw):
        return self._render_enum('Enum8', type_, **kw)

    def visit_enum16(self, type_, **kw):
        return self._render_enum('Enum16', type_, **kw)


class ClickHouseExecutionContextBase(default.DefaultExecutionContext):
    @sa_util.memoized_property
    def should_autocommit(self):
        return False  # No DML supported, never autocommit


class ClickHouseDialectBase(default.DefaultDialect):
    name = 'clickhouse'
    supports_cast = True
    supports_unicode_statements = True
    supports_unicode_binds = True
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False
    supports_native_decimal = True
    supports_native_boolean = False
    supports_alter = True
    supports_sequences = False
    supports_native_enum = True  # Do not render check constraints on enums.
    supports_multivalues_insert = True

    max_identifier_length = 127
    default_paramstyle = 'pyformat'
    colspecs = colspecs
    ischema_names = ischema_names
    convert_unicode = True
    returns_unicode_strings = True
    description_encoding = None
    postfetch_lastrowid = False

    preparer = ClickHouseIdentifierPreparer
    type_compiler = ClickHouseTypeCompiler
    statement_compiler = ClickHouseCompiler
    ddl_compiler = ClickHouseDDLCompiler

    def _execute(self, connection, sql):
        raise NotImplementedError

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        return self.get_table_names(connection, schema, **kw)

    def has_table(self, connection, table_name, schema=None):
        query = 'EXISTS TABLE {}'.format(table_name)
        for r in self._execute(connection, query):
            if r.result == 1:
                return True
        return False

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        query = 'DESCRIBE TABLE {}'.format(table_name)
        rows = self._execute(connection, query)

        columns = []
        for name, type_, default_type, default_expression in rows:
            # Get only type without extra modifiers.
            type_ = re.search(r'^\w+', type_).group(0)
            try:
                type_ = ischema_names[type_]
            except KeyError:
                type_ = sqltypes.NullType
            columns.append({
                'name': name,
                'type': type_,
                'nullable': True,
                'default': None,
            })
        return columns

    @reflection.cache
    def get_schema_names(self, connection, **kw):
        # No support for schemas.
        return []

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        # No support for foreign keys.
        return []

    @reflection.cache
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        # No support for primary keys.
        return []

    @reflection.cache
    def get_indexes(self, connection, table_name, schema=None, **kw):
        # No support for indexes.
        return []

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        return [row.name for row in self._execute(connection, 'SHOW TABLES')]

    def do_rollback(self, dbapi_connection):
        # No support for transactions.
        pass

    def _check_unicode_returns(self, connection, additional_tests=None):
        return True

    def _check_unicode_description(self, connection):
        return True
