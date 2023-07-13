from contextlib import contextmanager
from unittest.mock import Mock

from sqlalchemy import Column, text, create_engine, inspect

from clickhouse_sqlalchemy import types, engines, Table, make_session
from tests.testcase import BaseTestCase
from tests.util import with_native_and_http_sessions


@with_native_and_http_sessions
class EngineReflectionTestCase(BaseTestCase):
    required_server_version = (18, 16, 0)

    @contextmanager
    def _test_table(self, engine, *columns):
        metadata = self.metadata()
        columns = list(columns) + [Column('x', types.UInt32)] + [engine]
        table = Table('test_reflect', metadata, *columns)

        with self.create_table(table):
            metadata.clear()  # reflect from clean state
            self.assertFalse(metadata.tables)
            table = Table(
                'test_reflect',
                metadata,
                autoload_with=self.session.bind
            )
            yield table, table.engine

    def assertColumns(self, first, second, msg=None):
        self.assertListEqual(list(first), second, msg=msg)

    def test_file(self):
        engine = engines.File('Values')

        with self._test_table(engine) as (table, engine):
            self.assertIsInstance(engine, engines.File)
            self.assertEqual(engine.data_format, 'Values')

    def test_log(self):
        engine = engines.Log()

        with self._test_table(engine) as (table, engine):
            self.assertIsInstance(engine, engines.Log)

    def test_merge_tree(self):
        engine = engines.MergeTree(
            partition_by='x', order_by='x', primary_key='x', sample_by='x'
        )

        with self._test_table(engine) as (table, engine):
            self.assertIsInstance(engine, engines.MergeTree)
            self.assertColumns(engine.partition_by.columns, [table.c.x])
            self.assertColumns(engine.order_by.columns, [table.c.x])
            self.assertColumns(engine.primary_key.columns, [table.c.x])
            self.assertColumns(engine.sample_by.columns, [table.c.x])

    def test_merge_tree_param_expressions(self):
        engine = engines.MergeTree(
            partition_by=text('toYYYYMM(toDate(x))'),
            order_by='x', primary_key='x'
        )

        with self._test_table(engine) as (table, engine):
            self.assertIsInstance(engine, engines.MergeTree)
            self.assertEqual(
                str(engine.partition_by.expressions[0]), 'toYYYYMM(toDate(x))'
            )
            self.assertColumns(engine.order_by.columns, [table.c.x])
            self.assertColumns(engine.primary_key.columns, [table.c.x])

    def test_aggregating_merge_tree(self):
        engine = engines.AggregatingMergeTree(
            partition_by='x', order_by='x', primary_key='x'
        )

        with self._test_table(engine) as (table, engine):
            self.assertIsInstance(engine, engines.AggregatingMergeTree)
            self.assertColumns(engine.partition_by.columns, [table.c.x])
            self.assertColumns(engine.order_by.columns, [table.c.x])
            self.assertColumns(engine.primary_key.columns, [table.c.x])

    def test_collapsing_merge_tree(self):
        sign = Column('sign', types.Int8)
        engine = engines.CollapsingMergeTree(
            sign, partition_by='x', order_by='x', primary_key='x'
        )

        with self._test_table(engine, sign) as (table, engine):
            self.assertIsInstance(engine, engines.CollapsingMergeTree)
            self.assertColumns(engine.sign_col.columns, [table.c.sign])
            self.assertColumns(engine.partition_by.columns, [table.c.x])
            self.assertColumns(engine.order_by.columns, [table.c.x])
            self.assertColumns(engine.primary_key.columns, [table.c.x])

    def test_versioned_collapsing_merge_tree(self):
        sign = Column('sign', types.Int8)
        version = Column('version', types.Int8)
        engine = engines.VersionedCollapsingMergeTree(
            sign, version, partition_by='x', order_by='x', primary_key='x'
        )

        with self._test_table(engine, sign, version) as (table, engine):
            self.assertIsInstance(engine, engines.VersionedCollapsingMergeTree)
            self.assertColumns(engine.sign_col.columns, [table.c.sign])
            self.assertColumns(engine.version_col.columns, [table.c.version])
            self.assertColumns(engine.partition_by.columns, [table.c.x])
            self.assertColumns(
                engine.order_by.columns, [table.c.x, table.c.version]
            )
            self.assertColumns(engine.primary_key.columns, [table.c.x])

    def test_summing_merge_tree(self):
        y = Column('y', types.Int8)

        engine = engines.SummingMergeTree(
            columns=y, partition_by='x', order_by='x', primary_key='x'
        )

        with self._test_table(engine, y) as (table, engine):
            self.assertIsInstance(engine, engines.SummingMergeTree)
            self.assertColumns(engine.summing_cols.columns, [table.c.y])
            self.assertColumns(engine.partition_by.columns, [table.c.x])
            self.assertColumns(engine.order_by.columns, [table.c.x])
            self.assertColumns(engine.primary_key.columns, [table.c.x])

    def test_summing_merge_tree_multiple_columns(self):
        y = Column('y', types.Int8)
        z = Column('z', types.Int8)

        engine = engines.SummingMergeTree(
            columns=(y, z), partition_by='x', order_by='x', primary_key='x'
        )

        with self._test_table(engine, y, z) as (table, engine):
            self.assertIsInstance(engine, engines.SummingMergeTree)
            self.assertColumns(
                engine.summing_cols.columns, [table.c.y, table.c.z]
            )
            self.assertColumns(engine.partition_by.columns, [table.c.x])
            self.assertColumns(engine.order_by.columns, [table.c.x])
            self.assertColumns(engine.primary_key.columns, [table.c.x])

    def test_summing_merge_tree_no_columns(self):
        engine = engines.SummingMergeTree(
            partition_by='x', order_by='x', primary_key='x'
        )

        with self._test_table(engine) as (table, engine):
            self.assertIsInstance(engine, engines.SummingMergeTree)
            self.assertIsNone(engine.summing_cols)
            self.assertColumns(engine.partition_by.columns, [table.c.x])
            self.assertColumns(engine.order_by.columns, [table.c.x])
            self.assertColumns(engine.primary_key.columns, [table.c.x])

    def test_replacing_merge_tree(self):
        version = Column('version', types.Int8)

        engine = engines.ReplacingMergeTree(
            version=version, partition_by='x', order_by='x', primary_key='x'
        )

        with self._test_table(engine, version) as (table, engine):
            self.assertIsInstance(engine, engines.ReplacingMergeTree)
            self.assertColumns(engine.version_col.columns, [table.c.version])
            self.assertColumns(engine.partition_by.columns, [table.c.x])
            self.assertColumns(engine.order_by.columns, [table.c.x])
            self.assertColumns(engine.primary_key.columns, [table.c.x])

    def test_replacing_merge_tree_no_version(self):
        engine = engines.ReplacingMergeTree(
            partition_by='x', order_by='x', primary_key='x'
        )

        with self._test_table(engine) as (table, engine):
            self.assertIsInstance(engine, engines.ReplacingMergeTree)
            self.assertIsNone(engine.version_col)
            self.assertColumns(engine.partition_by.columns, [table.c.x])
            self.assertColumns(engine.order_by.columns, [table.c.x])
            self.assertColumns(engine.primary_key.columns, [table.c.x])

    def test_create_reflected(self):
        metadata = self.metadata()

        table = Table(
            'test_reflect', metadata,
            Column('x', types.Int32),
            engines.MergeTree(partition_by='x', order_by='x')
        )

        with self.create_table(table):
            metadata.clear()  # reflect from clean state
            self.assertFalse(metadata.tables)
            table = Table(
                'test_reflect',
                metadata,
                autoload_with=self.session.connection()
            )

            exists_query = text('EXISTS TABLE test_reflect')
            table.drop(bind=self.session.bind)
            exists = self.session.execute(exists_query).fetchall()
            self.assertEqual(exists, [(0, )])

            table.create(bind=self.session.bind)
            exists = self.session.execute(exists_query).fetchall()
            self.assertEqual(exists, [(1, )])

    def test_disable_engine_reflection(self):
        engine = self.session.connection().engine
        url = str(engine.url)
        prefix = 'clickhouse+{}://'.format(engine.driver)
        if not url.startswith(prefix):
            url = prefix + url.split('://')[1]

        session = make_session(create_engine(url + '?engine_reflection=no'))

        metadata = self.metadata()
        columns = [Column('x', types.Int32)] + [engines.Log()]
        table = Table('test_reflect', metadata, *columns)

        with self.create_table(table):
            metadata.clear()  # reflect from clean state
            self.assertFalse(metadata.tables)
            table = Table(
                'test_reflect',
                metadata,
                autoload_with=session.connection()
            )
            self.assertIsNone(getattr(table, 'engine', None))

    def test_exists_describe_escaping(self):
        metadata = self.metadata()
        table = Table('.test', self.metadata(), Column('x', types.Int32),
                      engines.Log())
        inspect(self.session.connection()).has_table(table.name)

        with self.create_table(table):
            metadata.clear()  # reflect from clean state
            self.assertFalse(metadata.tables)
            Table('.test', metadata, autoload_with=self.session.connection())


class EngineClassReflectionTestCase(BaseTestCase):
    def test_graphite_merge_tree(self):
        engine = engines.GraphiteMergeTree
        engine_full = (
            "GraphiteMergeTree('config_section') "
            "PARTITION BY x ORDER BY x PRIMARY KEY x"
        )

        table = Mock(columns=['x'])
        engine.__init__ = Mock(return_value=None)
        engine.reflect(
            table, engine_full,
            partition_key='x', sorting_key='x', primary_key='x'
        )

        engine.__init__.assert_called_with(
            'config_section',
            partition_by=['x'], order_by=['x'], primary_key=['x']
        )

    def test_distributed(self):
        engine = engines.Distributed
        engine_full = 'Distributed(cluster, test, merge_distributed1, rand())'

        table = Mock(columns=['x'])
        engine.__init__ = Mock(return_value=None)
        engine.reflect(table, engine_full)

        engine.__init__.assert_called_with(
            'cluster', 'test', 'merge_distributed1', 'rand()'
        )

    def test_distributed_no_sharding_key(self):
        engine = engines.Distributed
        engine_full = 'Distributed(cluster, test, merge_distributed1)'

        table = Mock(columns=['x'])
        engine.__init__ = Mock(return_value=None)
        engine.reflect(table, engine_full)

        engine.__init__.assert_called_with(
            'cluster', 'test', 'merge_distributed1'
        )

    def test_replicated_merge_tree(self):
        engine = engines.ReplicatedMergeTree
        engine_full = "ReplicatedMergeTree('/table/path', 'name')"

        table = Mock(columns=['x'])
        engine.__init__ = Mock(return_value=None)
        engine.reflect(
            table, engine_full,
            partition_key='x', sorting_key='x', primary_key='x'
        )

        engine.__init__.assert_called_with(
            '/table/path', 'name',
            partition_by=['x'], order_by=['x'], primary_key=['x']
        )

    def test_replicated_collapsing_merge_tree(self):
        engine = engines.ReplicatedCollapsingMergeTree
        engine_full = (
            "ReplicatedCollapsingMergeTree('/table/path', 'name', sign)"
        )

        table = Mock(columns=['x'])
        engine.__init__ = Mock(return_value=None)
        engine.reflect(
            table, engine_full,
            partition_key='x', sorting_key='x', primary_key='x'
        )

        engine.__init__.assert_called_with(
            '/table/path', 'name', 'sign',
            partition_by=['x'], order_by=['x'], primary_key=['x']
        )

    def test_replicated_versioned_collapsing_merge_tree(self):
        engine = engines.ReplicatedVersionedCollapsingMergeTree
        engine_full = (
            "ReplicatedVersionedCollapsingMergeTree('/table/path', 'name', "
            "sign, version)"
        )

        table = Mock(columns=['x'])
        engine.__init__ = Mock(return_value=None)
        engine.reflect(
            table, engine_full,
            partition_key='x', sorting_key='x', primary_key='x'
        )

        engine.__init__.assert_called_with(
            '/table/path', 'name', 'sign', 'version',
            partition_by=['x'], order_by=['x'], primary_key=['x']
        )

    def test_replicated_replacing_merge_tree(self):
        engine = engines.ReplicatedReplacingMergeTree
        engine_full = (
            "ReplicatedReplacingMergeTree('/table/path', 'name', version)"
        )

        table = Mock(columns=['x'])
        engine.__init__ = Mock(return_value=None)
        engine.reflect(
            table, engine_full,
            partition_key='x', sorting_key='x', primary_key='x'
        )

        engine.__init__.assert_called_with(
            '/table/path', 'name', version='version',
            partition_by=['x'], order_by=['x'], primary_key=['x']
        )

    def test_replicated_replacing_merge_tree_no_version(self):
        engine = engines.ReplicatedReplacingMergeTree
        engine_full = "ReplicatedReplacingMergeTree('/table/path', 'name')"

        table = Mock(columns=['x'])
        engine.__init__ = Mock(return_value=None)
        engine.reflect(
            table, engine_full,
            partition_key='x', sorting_key='x', primary_key='x'
        )

        engine.__init__.assert_called_with(
            '/table/path', 'name', version=None,
            partition_by=['x'], order_by=['x'], primary_key=['x']
        )

    def test_replicated_aggregating_merge_tree(self):
        engine = engines.ReplicatedAggregatingMergeTree
        engine_full = "ReplicatedAggregatingMergeTree('/table/path', 'name')"

        table = Mock(columns=['x'])
        engine.__init__ = Mock(return_value=None)
        engine.reflect(
            table, engine_full,
            partition_key='x', sorting_key='x', primary_key='x'
        )

        engine.__init__.assert_called_with(
            '/table/path', 'name',
            partition_by=['x'], order_by=['x'], primary_key=['x']
        )

    def test_replicated_summing_merge_tree(self):
        engine = engines.ReplicatedSummingMergeTree
        engine_full = "ReplicatedSummingMergeTree('/table/path', 'name', y)"

        table = Mock(columns=['x'])
        engine.__init__ = Mock(return_value=None)
        engine.reflect(
            table, engine_full,
            partition_key='x', sorting_key='x', primary_key='x'
        )

        engine.__init__.assert_called_with(
            '/table/path', 'name', columns=('y', ),
            partition_by=['x'], order_by=['x'], primary_key=['x']
        )

    def test_replicated_summing_merge_tree_multiple_columns(self):
        engine = engines.ReplicatedSummingMergeTree
        engine_full = (
            "ReplicatedSummingMergeTree('/table/path', 'name', (y, z))"
        )

        table = Mock(columns=['x'])
        engine.__init__ = Mock(return_value=None)
        engine.reflect(
            table, engine_full,
            partition_key='x', sorting_key='x', primary_key='x'
        )

        engine.__init__.assert_called_with(
            '/table/path', 'name', columns=('y', 'z'),
            partition_by=['x'], order_by=['x'], primary_key=['x']
        )

    def test_replicated_summing_merge_tree_no_columns(self):
        engine = engines.ReplicatedSummingMergeTree
        engine_full = "ReplicatedSummingMergeTree('/table/path', 'name')"

        table = Mock(columns=['x'])
        engine.__init__ = Mock(return_value=None)
        engine.reflect(
            table, engine_full,
            partition_key='x', sorting_key='x', primary_key='x'
        )

        engine.__init__.assert_called_with(
            '/table/path', 'name', columns=None,
            partition_by=['x'], order_by=['x'], primary_key=['x']
        )

    def test_buffer(self):
        engine = engines.Buffer
        engine_full = (
            "Buffer("
            "default, test, 16, 10, 100, 10000, 1000000, 10000000, 100000000"
            ")"
        )

        table = Mock(columns=['x'])
        engine.__init__ = Mock(return_value=None)
        engine.reflect(table, engine_full)

        engine.__init__.assert_called_with(
            'default', 'test', 16, 10, 100, 10000, 1000000, 10000000, 100000000
        )

    def test_ttl_replicated_merge_tree(self):
        engine = engines.ReplicatedMergeTree
        engine_full = (
            "ReplicatedMergeTree('/table/path', 'name') "
            "PARTITION BY x ORDER BY x PRIMARY KEY x TTL x"
        )

        table = Mock(columns=['x'])
        engine.__init__ = Mock(return_value=None)
        engine.reflect(
            table, engine_full,
            partition_key='x',
            sorting_key='x',
            primary_key='x',
            ttl='x',
        )

        engine.__init__.assert_called_with(
            '/table/path', 'name',
            partition_by=['x'],
            order_by=['x'],
            primary_key=['x'],
            ttl=['x'],
        )
