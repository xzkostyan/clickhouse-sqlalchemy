from sqlalchemy import Column, func, exc
from sqlalchemy.sql.ddl import CreateTable

from src import types, engines, get_declarative_base, Table
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
            'ENGINE = MergeTree(date, (date, x), 8192)'
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
            'ENGINE = MergeTree(date, (date, x), 8192)'
        )

    def test_func_engine_columns(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)
            y = Column(types.String)

            __table_args__ = (
                engines.MergeTree('date', ('date', func.intHash32(x)),
                                  sampling=func.intHash32(x)),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            'CREATE TABLE test_table (date Date, x Int32, y String) '
            'ENGINE = MergeTree('
            'date, intHash32(x), (date, intHash32(x)), 8192'
            ')'
        )

    def test_merge_tree_index_granularity(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)
            y = Column(types.String)

            __table_args__ = (
                engines.MergeTree(date, (date, x), index_granularity=4096),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            'CREATE TABLE test_table (date Date, x Int32, y String) '
            'ENGINE = MergeTree(date, (date, x), 4096)'
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
                engines.CollapsingMergeTree(date, (date, x), sign),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            'CREATE TABLE test_table '
            '(date Date, x Int32, y String, sign Int8) '
            'ENGINE = CollapsingMergeTree(date, (date, x), 8192, sign)'
        )

    def test_summing_merge_tree(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)
            y = Column(types.Int32)

            __table_args__ = (
                engines.SummingMergeTree(date, (date, x), (y, )),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            'CREATE TABLE test_table (date Date, x Int32, y Int32) '
            'ENGINE = SummingMergeTree(date, (date, x), 8192, (y))'
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
                engines.GraphiteMergeTree(date, (date, x), 'config_section'),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            "CREATE TABLE test_table (date Date, x Int32) "
            "ENGINE = GraphiteMergeTree("
            "date, (date, x), 8192, 'config_section'"
            ")"
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
            "ENGINE = ReplicatedMergeTree("
            "'/table/path', 'name', date, (date, x), 8192"
            ")"
        )

    def test_replacing_merge_tree(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)
            y = Column(types.String)
            version = Column(types.Int32)

            __table_args__ = (
                engines.ReplacingMergeTree(
                    'date', ('date', 'x'), 'version'
                ),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            "CREATE TABLE test_table ("
            "date Date, x Int32, y String, version Int32"
            ") "
            "ENGINE = ReplacingMergeTree(date, (date, x), 8192, version)"
        )
