from sqlalchemy import Column, event, func, text, select, inspect, ForeignKey
from sqlalchemy.sql.ddl import CreateTable, CreateColumn

from clickhouse_sqlalchemy import (
    types, engines, Table, get_declarative_base, MaterializedView
)
from clickhouse_sqlalchemy.sql.ddl import DropTable
from tests.testcase import BaseTestCase
from tests.session import mocked_engine
from tests.util import require_server_version


class DDLTestCase(BaseTestCase):
    def test_create_table(self):
        table = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            Column('y', types.String),
            Column('z', types.String(10)),
            # Must be quoted:
            Column('index', types.String),
            engines.Memory()
        )

        # No NOT NULL. And any PKS.
        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 ('
            'x Int32, y String, z FixedString(10), "index" String) '
            'ENGINE = Memory'
        )

    def test_create_table_nested_types(self):
        table = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            Column('y', types.Array(types.String)),
            engines.Memory()
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 '
            '(x Int32, y Array(String)) '
            'ENGINE = Memory'
        )

        table = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            Column('y', types.Array(types.Array(types.String))),
            engines.Memory()
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 '
            '(x Int32, y Array(Array(String))) '
            'ENGINE = Memory'
        )

        table = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            Column('y', types.Array(types.Array(types.String))),
            engines.Memory()
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 '
            '(x Int32, y Array(Array(String))) '
            'ENGINE = Memory'
        )

    def test_create_table_nullable(self):
        table = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            Column('y', types.Nullable(types.String)),
            Column('z', types.Nullable(types.String(10))),
            engines.Memory()
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 '
            '(x Int32, y Nullable(String), z Nullable(FixedString(10))) '
            'ENGINE = Memory'
        )

    def test_create_table_nested(self):
        table = Table(
            't1',
            self.metadata(),
            Column('x', types.Int32, primary_key=True),
            Column('parent', types.Nested(
                Column('child1', types.Int32),
                Column('child2', types.String),
            )),
            engines.Memory()
        )
        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 ('
            'x Int32, '
            'parent Nested('
            'child1 Int32, '
            "child2 String"
            ')'
            ') ENGINE = Memory'
        )

    def test_create_table_nested_nullable(self):
        table = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            Column('y', types.Array(types.Nullable(types.String))),
            engines.Memory()
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 '
            '(x Int32, y Array(Nullable(String))) '
            'ENGINE = Memory'
        )

    def test_create_table_nullable_nested_nullable(self):
        table = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            Column('y', types.Nullable(
                types.Array(types.Nullable(types.String)))
            ),
            engines.Memory()
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 '
            '(x Int32, y Nullable(Array(Nullable(String)))) '
            'ENGINE = Memory'
        )

    def test_create_table_with_codec(self):
        table = Table(
            't1', self.metadata(),
            Column(
                'list',
                types.DateTime,
                clickhouse_codec=['DoubleDelta', 'ZSTD'],
            ),
            Column(
                'tuple',
                types.UInt8,
                clickhouse_codec=('T64', 'ZSTD(5)'),
            ),
            Column('explicit_none', types.UInt32, clickhouse_codec=None),
            Column('str', types.Int8, clickhouse_codec='ZSTD'),
            engines.Memory()
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 ('
            'list DateTime CODEC(DoubleDelta, ZSTD), '
            'tuple UInt8 CODEC(T64, ZSTD(5)), '
            'explicit_none UInt32, '
            'str Int8 CODEC(ZSTD)) '
            'ENGINE = Memory'
        )

    def test_create_table_column_default(self):
        table = Table(
            't1', self.metadata(),
            Column('x', types.Int8),
            Column('dt', types.DateTime, server_default=func.now()),
            engines.Memory()
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 ('
            'x Int8, '
            'dt DateTime DEFAULT now()) '
            'ENGINE = Memory'
        )

    def test_create_table_column_default_another_column(self):
        class TestTable(get_declarative_base()):
            x = Column(types.Int8, primary_key=True)
            y = Column(types.Int8, server_default=x)

            __table_args__ = (
                engines.Memory(),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            'CREATE TABLE test_table ('
            'x Int8, '
            'y Int8 DEFAULT x) '
            'ENGINE = Memory'
        )

    def test_create_table_column_materialized(self):
        table = Table(
            't1', self.metadata(),
            Column('x', types.Int8),
            Column('dt', types.DateTime, clickhouse_materialized=func.now()),
            engines.Memory()
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 ('
            'x Int8, '
            'dt DateTime MATERIALIZED now()) '
            'ENGINE = Memory'
        )

    def test_create_table_column_materialized_another_column(self):
        class TestTable(get_declarative_base()):
            x = Column(types.Int8, primary_key=True)
            y = Column(types.Int8, clickhouse_materialized=x)

            __table_args__ = (
                engines.Memory(),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            'CREATE TABLE test_table ('
            'x Int8, '
            'y Int8 MATERIALIZED x) '
            'ENGINE = Memory'
        )

    def test_create_table_column_alias(self):
        table = Table(
            't1', self.metadata(),
            Column('x', types.Int8),
            Column('dt', types.DateTime, clickhouse_alias=func.now()),
            engines.Memory()
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 ('
            'x Int8, '
            'dt DateTime ALIAS now()) '
            'ENGINE = Memory'
        )

    def test_create_table_column_all_defaults(self):
        table = Table(
            't1', self.metadata(),
            Column('x', types.Int8),
            Column(
                'dt', types.DateTime, server_default=func.now(),
                clickhouse_materialized=func.now(), clickhouse_alias=func.now()
            ),
            engines.Memory()
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 ('
            'x Int8, '
            'dt DateTime DEFAULT now()) '
            'ENGINE = Memory'
        )

    def test_add_column(self):
        col = Column(
            'x2', types.Int8, nullable=True, clickhouse_after=text('x1')
        )

        self.assertEqual(
            self.compile(CreateColumn(col)),
            'x2 Int8 AFTER x1'
        )

    def test_create_table_tuple(self):
        table = Table(
            't1', self.metadata(),
            Column('x', types.Tuple(types.Int8, types.Float32)),
            engines.Memory()
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 ('
            'x Tuple(Int8, Float32)) '
            'ENGINE = Memory'
        )

    @require_server_version(21, 1, 3)
    def test_create_table_map(self):
        table = Table(
            't1', self.metadata(),
            Column('x', types.Map(types.String, types.String)),
            engines.Memory()
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 ('
            'x Map(String, String)) '
            'ENGINE = Memory'
        )

    def test_create_aggregate_function(self):
        table = Table(
            't1', self.metadata(),
            Column('total', types.AggregateFunction(func.sum(), types.UInt32)),
            engines.Memory()
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 ('
            'total AggregateFunction(sum(), UInt32)) '
            'ENGINE = Memory'
        )

    @require_server_version(22, 8, 21)
    def test_create_simple_aggregate_function(self):
        table = Table(
            't1', self.metadata(),
            Column(
                'total', types.SimpleAggregateFunction(
                    func.sum(), types.UInt32
                )
            ),
            engines.Memory()
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 ('
            'total SimpleAggregateFunction(sum(), UInt32)) '
            'ENGINE = Memory'
        )

    def test_table_create_on_cluster(self):
        create_sql = (
            'CREATE TABLE t1 ON CLUSTER test_cluster '
            '(x Int32) ENGINE = Memory'
        )

        with mocked_engine() as engine:
            table = Table(
                't1', self.metadata(),
                Column('x', types.Int32, primary_key=True),
                engines.Memory(),
                clickhouse_cluster='test_cluster'
            )

            table.create(bind=engine.session.bind)
            self.assertEqual(engine.history, [create_sql])

        self.assertEqual(
            self.compile(CreateTable(table)),
            create_sql
        )

    def test_drop_table_clause(self):
        table = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True)
        )

        self.assertEqual(
            self.compile(DropTable(table)),
            'DROP TABLE t1'
        )
        self.assertEqual(
            self.compile(DropTable(table, if_exists=True)),
            'DROP TABLE IF EXISTS t1'
        )

    def test_table_drop(self):
        with mocked_engine() as engine:
            table = Table(
                't1', self.metadata(),
                Column('x', types.Int32, primary_key=True)
            )
            table.drop(bind=engine.session.bind, if_exists=True)
            self.assertEqual(engine.history, ['DROP TABLE IF EXISTS t1'])

    def test_drop_table_event(self):
        events_triggered = []

        @event.listens_for(Table, "before_drop")
        def record_before_event(target, conn, **kwargs):
            events_triggered.append(("before_drop", target.name))

        @event.listens_for(Table, "after_drop")
        def record_after_event(target, conn, **kwargs):
            events_triggered.append(("after_drop", target.name))

        with mocked_engine() as engine:
            table = Table(
                't1', self.metadata(),
                Column('x', types.Int32, primary_key=True)
            )
            table.drop(bind=engine.session.bind, if_exists=True)

        assert events_triggered == [
            ("before_drop", "t1"),
            ("after_drop", "t1"),
        ]

    def test_table_drop_on_cluster(self):
        drop_sql = 'DROP TABLE IF EXISTS t1 ON CLUSTER test_cluster'

        with mocked_engine() as engine:
            table = Table(
                't1', self.metadata(),
                Column('x', types.Int32, primary_key=True),
                clickhouse_cluster='test_cluster'
            )
            table.drop(bind=engine.session.bind, if_exists=True)
            self.assertEqual(engine.history, [drop_sql])

        self.assertEqual(
            self.compile(DropTable(table, if_exists=True)),
            drop_sql
        )

    def test_create_all_drop_all(self):
        metadata = self.metadata()

        Table(
            't1', metadata,
            Column('x', types.Int32, primary_key=True),
            engines.Memory(),
        )

        metadata.create_all(bind=self.session.bind)
        metadata.drop_all(bind=self.session.bind)

    def test_create_drop_mat_view(self):
        Base = get_declarative_base(self.metadata())

        class Statistics(Base):
            date = Column(types.Date, primary_key=True)
            sign = Column(types.Int8, nullable=False)
            grouping = Column(types.Int32, nullable=False)
            metric1 = Column(types.Int32, nullable=False)

            __table_args__ = (
                engines.CollapsingMergeTree(
                    sign,
                    partition_by=func.toYYYYMM(date),
                    order_by=(date, grouping)
                ),
            )

        # Define storage for Materialized View
        class GroupedStatistics(Base):
            date = Column(types.Date, primary_key=True)
            metric1 = Column(types.Int32, nullable=False)

            __table_args__ = (
                engines.SummingMergeTree(
                    partition_by=func.toYYYYMM(date),
                    order_by=(date,)
                ),
            )

        # Define SELECT for Materialized View
        MatView = MaterializedView(GroupedStatistics, select(
            Statistics.date.label('date'),
            func.sum(Statistics.metric1 * Statistics.sign).label('metric1'),
        ).where(
            Statistics.grouping > 42
        ).group_by(
            Statistics.date
        ))

        Statistics.__table__.create(bind=self.session.bind)
        MatView.create(bind=self.session.bind)

        inspector = inspect(self.session.connection())

        self.assertTrue(inspector.has_table(MatView.name))
        MatView.drop(bind=self.session.bind)
        self.assertFalse(inspector.has_table(MatView.name))

    def test_create_table_with_comment(self):
        table = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            engines.Memory(),
            comment='table_comment'
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            "CREATE TABLE t1 (x Int32) ENGINE = Memory COMMENT 'table_comment'"
        )

    def test_create_table_with_column_comment(self):
        table = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True, comment='col_comment'),
            engines.Memory()
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            "CREATE TABLE t1 (x Int32 COMMENT 'col_comment') ENGINE = Memory"
        )

    def test_do_not_render_fk(self):
        table = Table(
            't1', self.metadata(),
            Column('x', types.Int32, ForeignKey('t2.x'), primary_key=True),
            engines.Memory()
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            "CREATE TABLE t1 (x Int32) ENGINE = Memory"
        )
