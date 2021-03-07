from sqlalchemy import Column, func, exc, text
from sqlalchemy.sql.ddl import CreateTable

from clickhouse_sqlalchemy import types, engines, get_declarative_base, Table
from clickhouse_sqlalchemy.sql.ddl import (
    ttl_delete,
    ttl_to_disk,
    ttl_to_volume,
)
from tests.testcase import CompilationTestCase


class EngineTestCaseBase(CompilationTestCase):
    @property
    def base(self):
        return get_declarative_base()


class GenericEngineTestCase(EngineTestCaseBase):
    def test_text_engine_columns_declarative(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)
            y = Column(types.String)

            __table_args__ = (
                engines.MergeTree(
                    partition_by='date',
                    order_by=('date', 'x')
                ),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            'CREATE TABLE test_table (date Date, x Int32, y String) '
            'ENGINE = MergeTree() '
            'PARTITION BY date '
            'ORDER BY (date, x)'
        )

    def test_text_engine_columns(self):
        table = Table(
            't1', self.metadata(),
            Column('date', types.Date, primary_key=True),
            Column('x', types.Int32),
            Column('y', types.String),
            engines.MergeTree(
                partition_by='date',
                order_by=('date', 'x')
            ),
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 (date Date, x Int32, y String) '
            'ENGINE = MergeTree() '
            'PARTITION BY date '
            'ORDER BY (date, x)'
        )

    def test_func_engine_columns(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)
            y = Column(types.String)

            __table_args__ = (
                engines.MergeTree(
                    partition_by='date',
                    order_by=('date', func.intHash32(x)),
                    sample_by=func.intHash32(x)
                ),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            'CREATE TABLE test_table (date Date, x Int32, y String) '
            'ENGINE = MergeTree() PARTITION BY date '
            'ORDER BY (date, intHash32(x)) '
            'SAMPLE BY intHash32(x)'
        )

    def test_create_table_without_engine(self):
        no_engine_table = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            Column('y', types.String)
        )

        with self.assertRaises(exc.CompileError) as ex:
            self.compile(CreateTable(no_engine_table))

        self.assertEqual(str(ex.exception), "No engine for table 't1'")

    def test_multiple_columns_partition_by(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)
            y = Column(types.String)

            __table_args__ = (
                engines.MergeTree(
                    partition_by=(date, x),
                    order_by='date'
                ),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            'CREATE TABLE test_table (date Date, x Int32, y String) '
            'ENGINE = MergeTree() PARTITION BY (date, x) '
            'ORDER BY date'
        )


class AggregatingMergeTree(EngineTestCaseBase):
    def test_basic(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)
            y = Column(types.Int32)

            __table_args__ = (
                engines.AggregatingMergeTree(
                    partition_by=date,
                    order_by=(date, x)
                ),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            'CREATE TABLE test_table (date Date, x Int32, y Int32) '
            'ENGINE = AggregatingMergeTree() '
            'PARTITION BY date '
            'ORDER BY (date, x)'
        )

    def test_replicated(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)
            y = Column(types.Int32)

            __table_args__ = (
                engines.ReplicatedAggregatingMergeTree(
                    '/table/path', 'name',
                    partition_by=date,
                    order_by=(date, x)
                ),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            "CREATE TABLE test_table (date Date, x Int32, y Int32) "
            "ENGINE = ReplicatedAggregatingMergeTree('/table/path', 'name') "
            "PARTITION BY date "
            "ORDER BY (date, x)"
        )


class CollapsingMergeTreeTestCase(EngineTestCaseBase):
    def test_basic(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)
            y = Column(types.String)
            sign = Column(types.Int8)

            __table_args__ = (
                engines.CollapsingMergeTree(
                    sign,
                    partition_by=date,
                    order_by=(date, x),
                    primary_key=(x, y),
                    sample_by=func.random(),
                    key='value'
                ),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            'CREATE TABLE test_table '
            '(date Date, x Int32, y String, sign Int8) '
            'ENGINE = CollapsingMergeTree(sign) '
            'PARTITION BY date '
            'ORDER BY (date, x) '
            'PRIMARY KEY (x, y) '
            'SAMPLE BY random() '
            'SETTINGS key=value'
        )

    def test_replicated(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)
            y = Column(types.String)
            sign = Column(types.Int8)

            __table_args__ = (
                engines.ReplicatedCollapsingMergeTree(
                    '/table/path', 'name',
                    sign,
                    partition_by=date,
                    order_by=(date, x),
                    primary_key=(x, y)
                ),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            "CREATE TABLE test_table "
            "(date Date, x Int32, y String, sign Int8) "
            "ENGINE = ReplicatedCollapsingMergeTree"
            "('/table/path', 'name', sign) "
            "PARTITION BY date "
            "ORDER BY (date, x) "
            "PRIMARY KEY (x, y)"
        )


class VersionedCollapsingMergeTreeTestCase(EngineTestCaseBase):
    def test_basic(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)
            y = Column(types.String)
            sign = Column(types.Int8)
            version = Column(types.Int8)

            __table_args__ = (
                engines.VersionedCollapsingMergeTree(
                    sign, version,
                    partition_by=date,
                    order_by=(date, x),
                    primary_key=(x, y),
                    sample_by=func.random(),
                    key='value'
                ),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            'CREATE TABLE test_table '
            '(date Date, x Int32, y String, sign Int8, version Int8) '
            'ENGINE = VersionedCollapsingMergeTree(sign, version) '
            'PARTITION BY date '
            'ORDER BY (date, x) '
            'PRIMARY KEY (x, y) '
            'SAMPLE BY random() '
            'SETTINGS key=value'
        )

    def test_replicated(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)
            y = Column(types.String)
            sign = Column(types.Int8)
            version = Column(types.Int8)

            __table_args__ = (
                engines.ReplicatedVersionedCollapsingMergeTree(
                    '/table/path', 'name',
                    sign, version,
                    partition_by=date,
                    order_by=(date, x),
                    primary_key=(x, y)
                ),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            "CREATE TABLE test_table "
            "(date Date, x Int32, y String, sign Int8, version Int8) "
            "ENGINE = ReplicatedVersionedCollapsingMergeTree"
            "('/table/path', 'name', sign, version) "
            "PARTITION BY date "
            "ORDER BY (date, x) "
            "PRIMARY KEY (x, y)"
        )


class SummingMergeTreeTestCase(EngineTestCaseBase):
    def test_basic(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)
            y = Column(types.Int32)

            __table_args__ = (
                engines.SummingMergeTree(
                    columns=(y, ),
                    partition_by=date,
                    order_by=(date, x)
                ),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            'CREATE TABLE test_table (date Date, x Int32, y Int32) '
            'ENGINE = SummingMergeTree(y) '
            'PARTITION BY date '
            'ORDER BY (date, x)'
        )

    def test_multiple_columns(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)
            y = Column(types.Int32)
            z = Column(types.Int32)

            __table_args__ = (
                engines.SummingMergeTree(
                    columns=(y, z),
                    partition_by=date,
                    order_by=(date, x)
                ),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            'CREATE TABLE test_table (date Date, x Int32, y Int32, z Int32) '
            'ENGINE = SummingMergeTree((y, z)) '
            'PARTITION BY date '
            'ORDER BY (date, x)'
        )

    def test_replicated(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)
            y = Column(types.Int32)

            __table_args__ = (
                engines.ReplicatedSummingMergeTree(
                    '/table/path', 'name',
                    columns=(y, ),
                    partition_by=date,
                    order_by=(date, x),
                ),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            "CREATE TABLE test_table (date Date, x Int32, y Int32) "
            "ENGINE = ReplicatedSummingMergeTree('/table/path', 'name', y) "
            "PARTITION BY date "
            "ORDER BY (date, x)"
        )


class ReplacingMergeTreeTestCase(EngineTestCaseBase):
    def test_basic(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)
            y = Column(types.String)
            version = Column(types.Int32)

            __table_args__ = (
                engines.ReplacingMergeTree(
                    version='version',
                    partition_by='date',
                    order_by=('date', 'x')
                ),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            "CREATE TABLE test_table "
            "(date Date, x Int32, y String, version Int32) "
            "ENGINE = ReplacingMergeTree(version) "
            "PARTITION BY date "
            "ORDER BY (date, x)"
        )

    def test_no_version(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)
            y = Column(types.String)
            version = Column(types.Int32)

            __table_args__ = (
                engines.ReplacingMergeTree(
                    partition_by='date',
                    order_by=('date', 'x'),
                ),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            "CREATE TABLE test_table "
            "(date Date, x Int32, y String, version Int32) "
            "ENGINE = ReplacingMergeTree() "
            "PARTITION BY date "
            "ORDER BY (date, x)"
        )

    def test_replicated(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)
            y = Column(types.String)
            version = Column(types.Int32)

            __table_args__ = (
                engines.ReplicatedReplacingMergeTree(
                    '/table/path', 'name',
                    version='version',
                    partition_by='date',
                    order_by=('date', 'x')
                ),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            "CREATE TABLE test_table "
            "(date Date, x Int32, y String, version Int32) "
            "ENGINE = ReplicatedReplacingMergeTree("
            "'/table/path', 'name', version) "
            "PARTITION BY date "
            "ORDER BY (date, x)"
        )


class MergeTreeTestCase(EngineTestCaseBase):
    def test_partition_by_func(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)
            y = Column(types.String)
            version = Column(types.Int32)

            __table_args__ = (
                engines.ReplacingMergeTree(
                    version=version,
                    partition_by=func.toYYYYMM(date),
                    order_by=(date, x),
                ),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            'CREATE TABLE test_table '
            '(date Date, x Int32, y String, version Int32) '
            'ENGINE = ReplacingMergeTree(version) '
            'PARTITION BY toYYYYMM(date) '
            'ORDER BY (date, x)'
        )

    def test_partition_by_text(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)
            y = Column(types.String)
            version = Column(types.Int32)

            __table_args__ = (
                engines.ReplacingMergeTree(
                    version=version,
                    partition_by=text('toYYYYMM(date)'),
                    order_by=(date, x),
                ),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            'CREATE TABLE test_table '
            '(date Date, x Int32, y String, version Int32) '
            'ENGINE = ReplacingMergeTree(version) '
            'PARTITION BY toYYYYMM(date) '
            'ORDER BY (date, x)'
        )

    def test_all_settings(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)
            y = Column(types.String)

            __table_args__ = (
                engines.MergeTree(
                    partition_by=date,
                    order_by=(date, x),
                    primary_key=(x, y),
                    sample_by=func.hashFunc(x),
                    setting1=2,
                    setting2=5
                ),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            'CREATE TABLE test_table '
            '(date Date, x Int32, y String) '
            'ENGINE = MergeTree() '
            'PARTITION BY date '
            'ORDER BY (date, x) '
            'PRIMARY KEY (x, y) '
            'SAMPLE BY hashFunc(x) '
            'SETTINGS setting1=2, setting2=5'
        )

    def test_replicated(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)
            y = Column(types.String)

            __table_args__ = (
                engines.ReplicatedMergeTree(
                    '/table/path', 'name',
                    partition_by='date',
                    order_by=('date', 'x')
                ),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            "CREATE TABLE test_table (date Date, x Int32, y String) "
            "ENGINE = ReplicatedMergeTree('/table/path', 'name') "
            "PARTITION BY date "
            "ORDER BY (date, x)"
        )


class FileTestCase(EngineTestCaseBase):
    def test_file(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)
            y = Column(types.String)

            __table_args__ = (engines.File('JSONEachRow'), )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            'CREATE TABLE test_table (date Date, x Int32, y String) '
            'ENGINE = File(JSONEachRow)'
        )

    def test_file_raises(self):
        with self.assertRaises(Exception):
            class TestTable(self.base):
                date = Column(types.Date, primary_key=True)
                x = Column(types.Int32)
                y = Column(types.String)

                __table_args__ = (engines.File('unsupported_format'), )


class MiscEnginesTestCase(EngineTestCaseBase):
    def test_buffer(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)
            y = Column(types.String)

            __table_args__ = (
                engines.Buffer('db', 'table'),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            'CREATE TABLE test_table (date Date, x Int32, y String) '
            'ENGINE = Buffer('
            'db, table, 16, 10, 100, 10000, 1000000, 10000000, 100000000'
            ')'
        )

    def test_distributed(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)

            __table_args__ = (
                engines.Distributed(
                    'cluster', 'test', 'merge_distributed1', 'rand()'
                ),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            'CREATE TABLE test_table (date Date, x Int32) '
            'ENGINE = Distributed(cluster, test, merge_distributed1, rand())'
        )

    def test_graphite_merge_tree(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)

            __table_args__ = (
                engines.GraphiteMergeTree(
                    'config_section',
                    partition_by=date,
                    order_by=(date, x)
                ),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            "CREATE TABLE test_table (date Date, x Int32) "
            "ENGINE = GraphiteMergeTree('config_section') "
            "PARTITION BY date "
            "ORDER BY (date, x)"
        )

    def test_memory(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)

            __table_args__ = (
                engines.Memory(),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            'CREATE TABLE test_table (date Date, x Int32) '
            'ENGINE = Memory'
        )


class TTLTestCase(EngineTestCaseBase):
    def test_ttl_expr(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)

            __table_args__ = (
                engines.MergeTree(
                    ttl=date + func.toIntervalDay(1),
                ),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            'CREATE TABLE test_table (date Date, x Int32) '
            'ENGINE = MergeTree() '
            'TTL date + toIntervalDay(1)'
        )

    def test_ttl_delete(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)

            __table_args__ = (
                engines.MergeTree(
                    ttl=ttl_delete(date + func.toIntervalDay(1)),
                ),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            'CREATE TABLE test_table (date Date, x Int32) '
            'ENGINE = MergeTree() '
            'TTL date + toIntervalDay(1) DELETE'
        )

    def test_ttl_list(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)

            __table_args__ = (
                engines.MergeTree(
                    ttl=[
                        ttl_delete(date + func.toIntervalDay(1)),
                        ttl_to_disk(date + func.toIntervalDay(1), 'hdd'),
                        ttl_to_volume(date + func.toIntervalDay(1), 'slow'),
                    ],
                ),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            'CREATE TABLE test_table (date Date, x Int32) '
            'ENGINE = MergeTree() '
            'TTL date + toIntervalDay(1) DELETE, '
            '    date + toIntervalDay(1) TO DISK \'hdd\', '
            '    date + toIntervalDay(1) TO VOLUME \'slow\''
        )
