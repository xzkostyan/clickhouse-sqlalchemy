import enum
from sqlalchemy import Column, func, inspect, types as sa_types

from clickhouse_sqlalchemy import types, engines, Table

from tests.testcase import BaseTestCase
from tests.util import require_server_version, with_native_and_http_sessions


@with_native_and_http_sessions
class ReflectionTestCase(BaseTestCase):
    def test_server_default(self):
        metadata = self.metadata()

        args = (
            [Column('x1', sa_types.String),
             Column('x2', sa_types.String, server_default='')] +
            [engines.Memory()]
        )

        table = Table('t', metadata, *args)
        with self.create_table(table):
            self.assertEqual([
                c['default'] for c in inspect(self.session.bind
                                              ).get_columns('t')
            ], [None, "''"])

    def _type_round_trip(self, *types):
        metadata = self.metadata()

        args = (
            [Column('t%d' % i, type_) for i, type_ in enumerate(types)] +
            [engines.Memory()]
        )

        table = Table('t', metadata, *args)
        with self.create_table(table):
            return inspect(self.session.bind).get_columns('t')

    def test_array(self):
        coltype = self._type_round_trip(types.Array(types.Int32))[0]['type']

        self.assertIsInstance(coltype, types.Array)
        self.assertEqual(coltype.item_type, types.Int32)

    def test_array_of_array(self):
        coltype = self._type_round_trip(
            types.Array(types.Array(types.Int32))
        )[0]['type']

        self.assertIsInstance(coltype, types.Array)
        self.assertIsInstance(coltype.item_type, types.Array)
        self.assertEqual(coltype.item_type.item_type, types.Int32)

    def test_sting_length(self):
        coltype = self._type_round_trip(types.String(10))[0]['type']

        self.assertIsInstance(coltype, types.String)
        self.assertEqual(coltype.length, 10)

    def test_nullable(self):
        col = self._type_round_trip(types.Nullable(types.Int32))[0]

        self.assertIsInstance(col['type'], types.Nullable)
        self.assertTrue(col['nullable'])
        self.assertIsInstance(col['type'].nested_type, types.Int32)

    def test_not_null(self):
        metadata = self.metadata()
        table = Table(
            't', metadata,
            Column('x', types.Int32, nullable=False),
            engines.Memory()
        )
        with self.create_table(table):
            col = inspect(self.session.bind).get_columns('t')[0]

        self.assertIsInstance(col['type'], types.Int32)
        self.assertFalse(col['nullable'])

    @require_server_version(19, 3, 3)
    def test_low_cardinality(self):
        coltype = self._type_round_trip(
            types.LowCardinality(types.String)
        )[0]['type']

        self.assertIsInstance(coltype, types.LowCardinality)
        self.assertIsInstance(coltype.nested_type, types.String)

    def test_tuple(self):
        coltype = self._type_round_trip(
            types.Tuple(types.String, types.Int32)
        )[0]['type']

        self.assertIsInstance(coltype, types.Tuple)
        self.assertEqual(coltype.nested_types[0], types.String)
        self.assertEqual(coltype.nested_types[1], types.Int32)

    @require_server_version(21, 1, 3)
    def test_map(self):
        coltype = self._type_round_trip(
            types.Map(types.String, types.String)
        )[0]['type']

        self.assertIsInstance(coltype, types.Map)
        self.assertEqual(coltype.key_type, types.String)
        self.assertEqual(coltype.value_type, types.String)

    def test_enum8(self):
        enum_options = {'three': 3, "quoted' ": 9, 'comma, ': 14}
        coltype = self._type_round_trip(
            types.Enum8(enum.Enum('any8_enum', enum_options))
        )[0]['type']

        self.assertIsInstance(coltype, types.Enum8)
        self.assertEqual(
            {o.name: o.value for o in coltype.enum_class}, enum_options
        )

    def test_enum16(self):
        enum_options = {'first': 1024, 'second': 2048}
        coltype = self._type_round_trip(
            types.Enum16(enum.Enum('any16_enum', enum_options))
        )[0]['type']

        self.assertIsInstance(coltype, types.Enum16)
        self.assertEqual(
            {o.name: o.value for o in coltype.enum_class}, enum_options
        )

    def test_decimal(self):
        coltype = self._type_round_trip(types.Decimal(38, 38))[0]['type']

        self.assertIsInstance(coltype, types.Decimal)
        self.assertEqual(coltype.precision, 38)
        self.assertEqual(coltype.scale, 38)

    @require_server_version(20, 1, 2)
    def test_datetime64(self):
        coltype = self._type_round_trip(
            types.DateTime64(2, 'Europe/Moscow')
        )[0]['type']

        self.assertIsInstance(coltype, types.DateTime64)
        self.assertEqual(coltype.precision, 2)
        self.assertEqual(coltype.timezone, "'Europe/Moscow'")

        coltype = self._type_round_trip(
            types.DateTime64()
        )[0]['type']

        self.assertIsInstance(coltype, types.DateTime64)
        self.assertEqual(coltype.precision, 3)
        self.assertIsNone(coltype.timezone)

    def test_datetime(self):
        coltype = self._type_round_trip(
            types.DateTime('Europe/Moscow')
        )[0]['type']

        self.assertIsInstance(coltype, types.DateTime)
        self.assertEqual(coltype.timezone, "'Europe/Moscow'")

        coltype = self._type_round_trip(
            types.DateTime()
        )[0]['type']

        self.assertIsInstance(coltype, types.DateTime)
        self.assertIsNone(coltype.timezone)

    def test_aggregate_function(self):
        coltype = self._type_round_trip(
            types.AggregateFunction(func.sum(), types.UInt16)
        )[0]['type']

        self.assertIsInstance(coltype, types.AggregateFunction)
        self.assertEqual(coltype.agg_func, 'sum')
        self.assertEqual(len(coltype.nested_types), 1)
        self.assertIsInstance(coltype.nested_types[0], types.UInt16)

        coltype = self._type_round_trip(
            types.AggregateFunction('quantiles(0.5, 0.9)', types.UInt32)
        )[0]['type']
        self.assertIsInstance(coltype, types.AggregateFunction)
        self.assertEqual(coltype.agg_func, 'quantiles(0.5, 0.9)')
        self.assertEqual(len(coltype.nested_types), 1)
        self.assertIsInstance(coltype.nested_types[0], types.UInt32)

        coltype = self._type_round_trip(
            types.AggregateFunction(
                func.argMin(), types.Float32, types.Float32
            )
        )[0]['type']
        self.assertIsInstance(coltype, types.AggregateFunction)
        self.assertEqual(coltype.agg_func, 'argMin')
        self.assertEqual(len(coltype.nested_types), 2)
        self.assertIsInstance(coltype.nested_types[0], types.Float32)
        self.assertIsInstance(coltype.nested_types[1], types.Float32)

        coltype = self._type_round_trip(
            types.AggregateFunction(
                'sum', types.Decimal(18, 2)
            )
        )[0]['type']
        self.assertIsInstance(coltype, types.AggregateFunction)
        self.assertEqual(coltype.agg_func, 'sum')
        self.assertEqual(len(coltype.nested_types), 1)
        self.assertIsInstance(coltype.nested_types[0], types.Decimal)
        self.assertEqual(coltype.nested_types[0].precision, 18)
        self.assertEqual(coltype.nested_types[0].scale, 2)

    @require_server_version(22, 8, 21)
    def test_simple_aggregate_function(self):
        coltype = self._type_round_trip(
            types.SimpleAggregateFunction(func.sum(), types.UInt64)
        )[0]['type']

        self.assertIsInstance(coltype, types.SimpleAggregateFunction)
        self.assertEqual(coltype.agg_func, 'sum')
        self.assertEqual(len(coltype.nested_types), 1)
        self.assertIsInstance(coltype.nested_types[0], types.UInt64)

        coltype = self._type_round_trip(
            types.SimpleAggregateFunction(
                'sum', types.Float64
            )
        )[0]['type']
        self.assertIsInstance(coltype, types.SimpleAggregateFunction)
        self.assertEqual(coltype.agg_func, 'sum')
        self.assertEqual(len(coltype.nested_types), 1)
        self.assertIsInstance(coltype.nested_types[0], types.Float64)
