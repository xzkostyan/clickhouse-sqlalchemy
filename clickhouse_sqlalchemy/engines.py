from sqlalchemy import util
from sqlalchemy.sql import ClauseElement
from sqlalchemy.sql.base import SchemaEventTarget
from sqlalchemy.sql.schema import ColumnCollectionMixin, SchemaItem
from sqlalchemy.sql.visitors import Visitable
from sqlalchemy.util import to_list


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

    __visit_name__ = 'merge_tree'

    def __init__(
            self,
            partition_by=None,
            order_by=None,
            primary_key=None,
            sample_by=None,
            **settings
    ):
        self.partition_by = None
        if partition_by is not None:
            self.partition_by = KeysExpressionOrColumn(*to_list(partition_by))

        self.order_by = None
        if order_by is not None:
            self.order_by = KeysExpressionOrColumn(*to_list(order_by))

        self.primary_key = None
        if primary_key is not None:
            self.primary_key = KeysExpressionOrColumn(*to_list(primary_key))

        self.sample_by = None
        if sample_by is not None:
            self.sample_by = KeysExpressionOrColumn(sample_by)
        self.settings = settings
        super(MergeTree, self).__init__()

    def _set_parent(self, table):
        super(MergeTree, self)._set_parent(table)
        if self.partition_by is not None:
            self.partition_by._set_parent(table)
        if self.order_by is not None:
            self.order_by._set_parent(table)
        if self.primary_key is not None:
            self.primary_key._set_parent(table)
        if self.sample_by is not None:
            self.sample_by._set_parent(table)

    def get_parameters(self):
        return []


class AggregatingMergeTree(MergeTree):
    pass


class GraphiteMergeTree(MergeTree):

    def __init__(self, config_name, *args, **kwargs):
        super(GraphiteMergeTree, self).__init__(*args, **kwargs)
        self.config_name = config_name

    def get_parameters(self):
        return "'{}'".format(self.config_name)


class CollapsingMergeTree(MergeTree):
    def __init__(self, sign_col, *args, **kwargs):
        super(CollapsingMergeTree, self).__init__(*args, **kwargs)
        self.sign_col = TableCol(sign_col)

    def get_parameters(self):
        return self.sign_col.get_column()

    def _set_parent(self, table):
        super(CollapsingMergeTree, self)._set_parent(table)

        self.sign_col._set_parent(table)


class VersionedCollapsingMergeTree(MergeTree):
    def __init__(self, sign_col, version_col, *args, **kwargs):
        super(VersionedCollapsingMergeTree, self).__init__(*args, **kwargs)

        self.sign_col = TableCol(sign_col)
        self.version_col = TableCol(version_col)

    def get_parameters(self):
        return [self.sign_col.get_column(), self.version_col.get_column()]

    def _set_parent(self, table):
        super(VersionedCollapsingMergeTree, self)._set_parent(table)

        self.sign_col._set_parent(table)
        self.version_col._set_parent(table)


class SummingMergeTree(MergeTree):
    def __init__(self, *args, **kwargs):
        summing_cols = kwargs.pop('columns', None)
        super(SummingMergeTree, self).__init__(*args, **kwargs)

        self.summing_cols = None
        if summing_cols is not None:
            self.summing_cols = KeysExpressionOrColumn(*summing_cols)

    def _set_parent(self, table):
        super(SummingMergeTree, self)._set_parent(table)

        if self.summing_cols is not None:
            self.summing_cols._set_parent(table)

    def get_parameters(self):
        if self.summing_cols is not None:
            return self.summing_cols.get_expressions_or_columns()


class ReplacingMergeTree(MergeTree):
    def __init__(self, *args, **kwargs):
        version_col = kwargs.pop('version', None)
        super(ReplacingMergeTree, self).__init__(*args, **kwargs)

        self.version_col = None
        if version_col is not None:
            self.version_col = TableCol(version_col)

    def _set_parent(self, table):
        super(ReplacingMergeTree, self)._set_parent(table)

        if self.version_col is not None:
            self.version_col._set_parent(table)

    def get_parameters(self):
        if self.version_col is not None:
            return self.version_col.get_column()


class Distributed(Engine):
    def __init__(self, logs, default, hits, sharding_key=None):
        self.logs = logs
        self.default = default
        self.hits = hits
        self.sharding_key = sharding_key
        super(Distributed, self).__init__()

    def get_parameters(self):
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
        self.table_path = table_path
        self.replica_name = replica_name

    def get_parameters(self):
        return [
            "'{}'".format(self.table_path),
            "'{}'".format(self.replica_name)
        ]


class ReplicatedMergeTree(ReplicatedEngineMixin, MergeTree):
    def __init__(self, table_path, replica_name,
                 *args, **kwargs):
        ReplicatedEngineMixin.__init__(self, table_path, replica_name)
        MergeTree.__init__(self, *args, **kwargs)

    def get_parameters(self):
        return self.extend_parameters(
            ReplicatedEngineMixin.get_parameters(self),
            MergeTree.get_parameters(self)
        )


class ReplicatedCollapsingMergeTree(ReplicatedEngineMixin,
                                    CollapsingMergeTree):
    def __init__(self, table_path, replica_name,
                 *args, **kwargs):
        ReplicatedEngineMixin.__init__(self, table_path, replica_name)
        CollapsingMergeTree.__init__(self, *args, **kwargs)

    def get_parameters(self):
        return self.extend_parameters(
            ReplicatedEngineMixin.get_parameters(self),
            CollapsingMergeTree.get_parameters(self)
        )


class ReplicatedVersionedCollapsingMergeTree(ReplicatedEngineMixin,
                                             VersionedCollapsingMergeTree):
    def __init__(self, table_path, replica_name,
                 *args, **kwargs):
        ReplicatedEngineMixin.__init__(self, table_path, replica_name)
        VersionedCollapsingMergeTree.__init__(self, *args, **kwargs)

    def get_parameters(self):
        return self.extend_parameters(
            ReplicatedEngineMixin.get_parameters(self),
            VersionedCollapsingMergeTree.get_parameters(self)
        )


class ReplicatedReplacingMergeTree(ReplicatedEngineMixin,
                                   ReplacingMergeTree):
    def __init__(self, table_path, replica_name,
                 *args, **kwargs):
        ReplicatedEngineMixin.__init__(self, table_path, replica_name)
        ReplacingMergeTree.__init__(self, *args, **kwargs)

    def get_parameters(self):
        return self.extend_parameters(
            ReplicatedEngineMixin.get_parameters(self),
            ReplacingMergeTree.get_parameters(self)
        )


class ReplicatedAggregatingMergeTree(ReplicatedEngineMixin,
                                     AggregatingMergeTree):
    def __init__(self, table_path, replica_name,
                 *args, **kwargs):
        ReplicatedEngineMixin.__init__(self, table_path, replica_name)
        AggregatingMergeTree.__init__(self, *args, **kwargs)

    def get_parameters(self):
        return self.extend_parameters(
            ReplicatedEngineMixin.get_parameters(self),
            AggregatingMergeTree.get_parameters(self)
        )


class ReplicatedSummingMergeTree(ReplicatedEngineMixin, SummingMergeTree):
    def __init__(self,
                 table_path,
                 replica_name,
                 *args,
                 **kwargs):
        ReplicatedEngineMixin.__init__(self, table_path, replica_name)
        SummingMergeTree.__init__(self, *args, **kwargs)

    def get_parameters(self):
        return self.extend_parameters(
            ReplicatedEngineMixin.get_parameters(self),
            SummingMergeTree.get_parameters(self)
        )


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

    def get_parameters(self):
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
    def get_parameters(self):
        return []


class TinyLog(_NoParamsEngine):
    pass


class Log(_NoParamsEngine):
    pass


class Memory(_NoParamsEngine):
    pass


class Null(_NoParamsEngine):
    pass


class File(Engine):
    supported_data_formats = {
        'tabseparated': 'TabSeparated',
        'tabseparatedwithnames': 'TabSeparatedWithNames',
        'tabseparatedwithnamesandtypes': 'TabSeparatedWithNamesAndTypes',
        'template': 'Template',
        'csv': 'CSV',
        'csvwithnames': 'CSVWithNames',
        'customseparated': 'CustomSeparated',
        'values': 'Values',
        'jsoneachrow': 'JSONEachRow',
        'tskv': 'TSKV',
        'protobuf': 'Protobuf',
        'parquet': 'Parquet',
        'rowbinary': 'RowBinary',
        'rowbinarywithnamesandtypes': 'RowBinaryWithNamesAndTypes',
        'native': 'Native'
    }

    def __init__(self, data_format):
        fmt = self.supported_data_formats.get(data_format.lower())
        if not fmt:
            raise ValueError(
                'Format {0} not supproted for File engine'.format(
                    data_format
                )
            )
        self.data_format = fmt
        super(File, self).__init__()

    def get_parameters(self):
        return (self.data_format, )
