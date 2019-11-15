from sqlalchemy import Column, func, exc
from sqlalchemy.sql.ddl import CreateTable

from clickhouse_sqlalchemy import types, engines, get_declarative_base, Table
from tests.testcase import BaseTestCase


class EnginesDeclarativeTestCase(BaseTestCase):
    @property
    def base(self):
        return get_declarative_base()

    def test_text_engine_columns_declarative(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)
            y = Column(types.String)

            __table_args__ = (
                engines.MergeTree('date', ('date', 'x')),
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
            engines.MergeTree('date', ('date', 'x')),
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
                engines.MergeTree('date', ('date', func.intHash32(x)),
                                  sample=func.intHash32(x)),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            'CREATE TABLE test_table (date Date, x Int32, y String) '
            'ENGINE = MergeTree() PARTITION BY date '
            'ORDER BY (date, intHash32(x)) '
            'SAMPLE BY intHash32(x)'
        )

    def test_merge_tree_all_settings(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)
            y = Column(types.String)

            __table_args__ = (
                engines.MergeTree(
                    partition_by=date,
                    order_by=(date, x),
                    primary_key=(x, y),
                    sample=func.hashFunc(x),
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

    def test_create_table_without_engine(self):
        no_engine_table = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            Column('y', types.String)
        )

        with self.assertRaises(exc.CompileError) as ex:
            self.compile(CreateTable(no_engine_table))

        self.assertEqual(str(ex.exception), "No engine for table 't1'")

    def test_collapsing_merge_tree(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)
            y = Column(types.String)
            sign = Column(types.Int8)

            __table_args__ = (
                engines.CollapsingMergeTree(
                    sign,
                    date,
                    (date, x),
                    (x, y),
                    func.random(),
                    key='value'),
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

    def test_summing_merge_tree(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)
            y = Column(types.Int32)

            __table_args__ = (
                engines.SummingMergeTree((y, ), date, (date, x)),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            'CREATE TABLE test_table (date Date, x Int32, y Int32) '
            'ENGINE = SummingMergeTree(y) '
            'PARTITION BY date '
            'ORDER BY (date, x)'
        )

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
            "CREATE TABLE test_table (date Date, x Int32, y String) "
            "ENGINE = Buffer("
            "db, table, 16, 10, 100, 10000, 1000000, 10000000, 100000000"
            ")"
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
            "CREATE TABLE test_table (date Date, x Int32) "
            "ENGINE = Distributed(cluster, test, merge_distributed1, rand())"
        )

    def test_graphite_merge_tree(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)

            __table_args__ = (
                engines.GraphiteMergeTree('config_section', date, (date, x)),
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
            "CREATE TABLE test_table (date Date, x Int32) "
            "ENGINE = Memory"
        )

    def test_replicated_merge_tree(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)
            y = Column(types.String)

            __table_args__ = (
                engines.ReplicatedMergeTree(
                    '/table/path', 'name', 'date', ('date', 'x')
                ),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            "CREATE TABLE test_table (date Date, x Int32, y String) "
            "ENGINE = ReplicatedMergeTree('/table/path', 'name') "
            "PARTITION BY date "
            "ORDER BY (date, x)"
        )

    def test_replacing_merge_tree(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)
            y = Column(types.String)
            version = Column(types.Int32)

            __table_args__ = (
                engines.ReplacingMergeTree(
                    'version',
                    'date',
                    ('date', 'x'),
                ),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            "CREATE TABLE test_table ("
            "date Date, x Int32, y String, version Int32"
            ") "
            "ENGINE = ReplacingMergeTree(version) "
            "PARTITION BY date "
            "ORDER BY (date, x)"
        )

    def test_replicated_replacing_merge_tree(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)
            y = Column(types.String)
            version = Column(types.Int32)

            __table_args__ = (
                engines.ReplicatedReplacingMergeTree(
                    '/table/path', 'name',
                    'version', 'date', ('date', 'x'),
                ),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            "CREATE TABLE test_table ("
            "date Date, x Int32, y String, version Int32"
            ") "
            "ENGINE = ReplicatedReplacingMergeTree("
            "'/table/path', 'name', version"
            ") "
            "PARTITION BY date "
            "ORDER BY (date, x)"
        )

    def test_partition_by_func(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)
            y = Column(types.String)
            version = Column(types.Int32)

            __table_args__ = (
                engines.ReplacingMergeTree(
                    version,
                    partition_by=func.toYYYYMM(date),
                    order_by=(date, x),
                ),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            "CREATE TABLE test_table ("
            "date Date, x Int32, y String, version Int32"
            ") "
            "ENGINE = ReplacingMergeTree(version) "
            "PARTITION BY toYYYYMM(date) "
            "ORDER BY (date, x)"
        )
