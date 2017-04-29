from sqlalchemy import util
from sqlalchemy.sql import ClauseElement
from sqlalchemy.sql.base import SchemaEventTarget
from sqlalchemy.sql.schema import ColumnCollectionMixin, SchemaItem
from sqlalchemy.sql.visitors import Visitable


class Engine(SchemaEventTarget, Visitable):
    __visit_name__ = 'engine'

    def get_params(self):
        raise NotImplementedError()

    def name(self):
        return self.__class__.__name__

    def _set_parent(self, table):
        self.table = table
        self.table.engine = self


class TableCol(ColumnCollectionMixin, SchemaItem):
    def __init__(self, column, **kwargs):
        super(TableCol, self).__init__(*[column], **kwargs)

    def get_column(self):
        return list(self.columns)[0]


class KeysExpressionOrColumn(ColumnCollectionMixin, SchemaItem):
    def __init__(self, *expressions, **kwargs):
        columns = []
        self.expressions = []
        for expr, column, strname, add_element in self.\
                _extract_col_expression_collection(expressions):
            if add_element is not None:
                columns.append(add_element)
            self.expressions.append(expr)

        super(KeysExpressionOrColumn, self).__init__(*columns, **kwargs)

    def _set_parent(self, table):
        super(KeysExpressionOrColumn, self)._set_parent(table)

    def get_expressions_or_columns(self):
        expr_columns = util.zip_longest(self.expressions, self.columns)
        return [
            (expr if isinstance(expr, ClauseElement) else colexpr)
            for expr, colexpr in expr_columns
        ]


class MergeTree(Engine):
    def __init__(self, date_col, key_expressions, sampling=None,
                 index_granularity=None):
        self.date_col = TableCol(date_col)
        self.key_cols = KeysExpressionOrColumn(*key_expressions)

        self.sampling = None
        self.index_granularity = 8192

        if sampling is not None:
            self.sampling = KeysExpressionOrColumn(sampling)
        if index_granularity is not None:
            self.index_granularity = index_granularity

        super(MergeTree, self).__init__()

    def _set_parent(self, table):
        super(MergeTree, self)._set_parent(table)

        self.date_col._set_parent(table)
        self.key_cols._set_parent(table)

        if self.sampling:
            self.sampling._set_parent(table)

    def get_params(self):
        params = [
            self.date_col.get_column()
        ]

        if self.sampling:
            params.append(self.sampling.get_expressions_or_columns()[0])

        params.append(tuple(self.key_cols.get_expressions_or_columns()))
        params.append(self.index_granularity)
        return params


class CollapsingMergeTree(MergeTree):
    def __init__(self, date_col, key_expressions, sign_col, sampling=None,
                 index_granularity=None):
        super(CollapsingMergeTree, self).__init__(
            date_col, key_expressions, sampling=sampling,
            index_granularity=index_granularity
        )
        self.sign_col = TableCol(sign_col)

    def get_params(self):
        params = super(CollapsingMergeTree, self).get_params()
        params.append(self.sign_col.get_column())
        return params

    def _set_parent(self, table):
        super(CollapsingMergeTree, self)._set_parent(table)

        self.sign_col._set_parent(table)


class SummingMergeTree(MergeTree):
    def __init__(self, date_col, key_expressions, summing_cols=None,
                 sampling=None, index_granularity=None):
        super(SummingMergeTree, self).__init__(
            date_col, key_expressions, sampling=sampling,
            index_granularity=index_granularity
        )

        self.summing_cols = None
        if summing_cols is not None:
            self.summing_cols = KeysExpressionOrColumn(*summing_cols)

    def _set_parent(self, table):
        super(SummingMergeTree, self)._set_parent(table)

        if self.summing_cols:
            self.summing_cols._set_parent(table)

    def get_params(self):
        params = super(SummingMergeTree, self).get_params()
        if self.summing_cols:
            params.append(
                tuple(self.summing_cols.get_expressions_or_columns())
            )
        return params


class Buffer(Engine):
    def __init__(self, database, table, num_layers=16,
                 min_time=10, max_time=100, min_rows=10000, max_rows=1000000,
                 min_bytes=10000000, max_bytes=100000000):
        self.database = database
        self.table = table
        self.num_layers = num_layers
        self.min_time = min_time
        self.max_time = max_time
        self.min_rows = min_rows
        self.max_rows = max_rows
        self.min_bytes = min_bytes
        self.max_bytes = max_bytes
        super(Buffer, self).__init__()

    def get_params(self):
        return [
            self.database,
            self.table.name,
            self.num_layers,
            self.min_time,
            self.max_time,
            self.min_rows,
            self.max_rows,
            self.min_bytes,
            self.max_bytes
        ]


class Memory(Engine):
    def get_params(self):
        return []
