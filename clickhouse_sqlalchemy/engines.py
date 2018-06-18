from sqlalchemy import util, literal
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


class AggregatingMergeTree(MergeTree):
    pass


class GraphiteMergeTree(MergeTree):
    def __init__(self, date_col, key_expressions, config_name, sampling=None,
                 index_granularity=None):
        super(GraphiteMergeTree, self).__init__(
            date_col, key_expressions, sampling=sampling,
            index_granularity=index_granularity
        )
        self.config_name = config_name

    def get_params(self):
        params = super(GraphiteMergeTree, self).get_params()
        params.append(literal(self.config_name))
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


class ReplacingMergeTree(MergeTree):
    def __init__(self, date_col, key_expressions, version_col=None,
                 sampling=None, index_granularity=None):
        super(ReplacingMergeTree, self).__init__(
            date_col, key_expressions, sampling=sampling,
            index_granularity=index_granularity
        )

        self.version_col = None
        if version_col:
            self.version_col = TableCol(version_col)

    def _set_parent(self, table):
        super(ReplacingMergeTree, self)._set_parent(table)

        if self.version_col:
            self.version_col._set_parent(table)

    def get_params(self):
        params = super(ReplacingMergeTree, self).get_params()
        if self.version_col:
            params.append(self.version_col.get_column())
        return params


class Distributed(Engine):
    def __init__(self, logs, default, hits, sharding_key=None):
        self.logs = logs
        self.default = default
        self.hits = hits
        self.sharding_key = sharding_key
        super(Distributed, self).__init__()

    def get_params(self):
        params = [
            self.logs,
            self.default,
            self.hits
        ]

        if self.sharding_key is not None:
            params.append(self.sharding_key)

        return params


class ReplicatedEngineMixin(object):
    def __init__(self, table_path, replica_name):
        self.table_path = literal(table_path)
        self.replica_name = literal(replica_name)

    def get_params(self):
        return [
            self.table_path,
            self.replica_name
        ]


class ReplicatedMergeTree(ReplicatedEngineMixin, MergeTree):
    def __init__(self, table_path, replica_name,
                 date_col, key_expressions, sampling=None,
                 index_granularity=None):
        ReplicatedEngineMixin.__init__(self, table_path, replica_name)
        MergeTree.__init__(
            self, date_col, key_expressions, sampling=sampling,
            index_granularity=index_granularity)

    def get_params(self):
        return ReplicatedEngineMixin.get_params(self) + \
            MergeTree.get_params(self)


class ReplicatedCollapsingMergeTree(ReplicatedEngineMixin,
                                    CollapsingMergeTree):
    def __init__(self, table_path, replica_name,
                 date_col, key_expressions, sign_col, sampling=None,
                 index_granularity=None):
        ReplicatedEngineMixin.__init__(self, table_path, replica_name)
        CollapsingMergeTree.__init__(
            self, date_col, key_expressions, sign_col, sampling=sampling,
            index_granularity=index_granularity)

    def get_params(self):
        return ReplicatedEngineMixin.get_params(self) + \
            CollapsingMergeTree.get_params(self)


class ReplicatedAggregatingMergeTree(ReplicatedEngineMixin,
                                     AggregatingMergeTree):
    def __init__(self, table_path, replica_name,
                 date_col, key_expressions, sampling=None,
                 index_granularity=None):
        ReplicatedEngineMixin.__init__(self, table_path, replica_name)
        AggregatingMergeTree.__init__(
            self, date_col, key_expressions, sampling=sampling,
            index_granularity=index_granularity)

    def get_params(self):
        return ReplicatedEngineMixin.get_params(self) + \
            AggregatingMergeTree.get_params(self)


class ReplicatedSummingMergeTree(ReplicatedEngineMixin, SummingMergeTree):
    def __init__(self, table_path, replica_name,
                 date_col, key_expressions, summing_cols=None,
                 sampling=None, index_granularity=None):
        ReplicatedEngineMixin.__init__(self, table_path, replica_name)
        SummingMergeTree.__init__(
            self, date_col, key_expressions, summing_cols=summing_cols,
            sampling=sampling, index_granularity=index_granularity)

    def get_params(self):
        return ReplicatedEngineMixin.get_params(self) + \
            SummingMergeTree.get_params(self)


class Buffer(Engine):
    def __init__(self, database, table, num_layers=16,
                 min_time=10, max_time=100, min_rows=10000, max_rows=1000000,
                 min_bytes=10000000, max_bytes=100000000):
        self.database = database
        self.table_name = table
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
            self.table_name,
            self.num_layers,
            self.min_time,
            self.max_time,
            self.min_rows,
            self.max_rows,
            self.min_bytes,
            self.max_bytes
        ]


class _NoParamsEngine(Engine):
    def get_params(self):
        return []


class TinyLog(_NoParamsEngine):
    pass


class Log(_NoParamsEngine):
    pass


class Memory(_NoParamsEngine):
    pass


class Null(_NoParamsEngine):
    pass
