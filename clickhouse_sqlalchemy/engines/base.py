from sqlalchemy.sql import ClauseElement, roles
from sqlalchemy.sql.base import SchemaEventTarget
from sqlalchemy.sql.coercions import expect_col_expression_collection
from sqlalchemy.sql.schema import ColumnCollectionMixin, SchemaItem
from sqlalchemy.sql.visitors import Visitable
from sqlalchemy.util import zip_longest


class Engine(SchemaEventTarget, Visitable):
    __visit_name__ = 'engine'

    def get_parameters(self):
        return []

    def extend_parameters(self, *params):
        rv = []
        for param in params:
            if isinstance(param, (tuple, list)):
                rv.extend(param)
            elif param is not None:
                rv.append(param)
        return rv

    @property
    def name(self):
        return self.__class__.__name__

    def _set_parent(self, table, **kwargs):
        self.table = table
        self.table.engine = self

    @classmethod
    def reflect(cls, table, engine_full, **kwargs):
        raise NotImplementedError


class TableCol(ColumnCollectionMixin, SchemaItem):
    def __init__(self, column, **kwargs):
        super(TableCol, self).__init__(*[column], **kwargs)

    def get_column(self):
        return list(self.columns)[0]


class KeysExpressionOrColumn(ColumnCollectionMixin, SchemaItem):
    def __init__(self, *expressions, **kwargs):
        columns = []
        self.expressions = []
        items = expect_col_expression_collection(
            roles.DDLConstraintColumnRole, expressions
        )
        for expr, column, strname, add_element in items:
            if add_element is not None:
                columns.append(add_element)
            self.expressions.append(expr)

        super(KeysExpressionOrColumn, self).__init__(*columns, **kwargs)

    def get_expressions_or_columns(self):
        expr_columns = zip_longest(self.expressions, self.columns)
        return [
            (expr if isinstance(expr, ClauseElement) else colexpr)
            for expr, colexpr in expr_columns
        ]
