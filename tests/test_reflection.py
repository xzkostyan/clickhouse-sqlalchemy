import enum

from sqlalchemy import Column, inspect

from clickhouse_sqlalchemy import types, engines, Table
from tests.testcase import TypesTestCase


class ReflectionTestCase(TypesTestCase):
    def _type_round_trip(self, *types):
        metadata = self.metadata()

        args = (
            [Column('t%d' % i, type_) for i, type_ in enumerate(types)] +
            [engines.Memory()]
        )

        table = Table('t', metadata, *args)
        with self.create_table(table):
            return [
                c['type'] for c in
                inspect(metadata.bind).get_columns('t')
            ]

    def test_array(self):
        coltype = self._type_round_trip(types.Array(types.Int32))[0]

        self.assertIsInstance(coltype, types.Array)
        self.assertEqual(coltype.item_type, types.Int32)

    def test_array_of_array(self):
        coltype = self._type_round_trip(
            types.Array(types.Array(types.Int32))
        )[0]

        self.assertIsInstance(coltype, types.Array)
        self.assertIsInstance(coltype.item_type, types.Array)
        self.assertEqual(coltype.item_type.item_type, types.Int32)

    def test_sting_length(self):
        coltype = self._type_round_trip(types.String(10))[0]

        self.assertIsInstance(coltype, types.String)
        self.assertEqual(coltype.length, 10)

    def test_nullable(self):
        coltype = self._type_round_trip(types.Nullable(types.Int32))[0]

        self.assertIsInstance(coltype, types.Nullable)
        self.assertEqual(coltype.nested_type, types.Int32)

    def test_enum8(self):
        enum_options = {'three': 3, "quoted' ": 9, 'comma, ': 14}
        coltype = self._type_round_trip(
            types.Enum8(enum.Enum('any8_enum', enum_options))
        )[0]

        self.assertIsInstance(coltype, types.Enum8)
        self.assertEqual(
            {o.name: o.value for o in coltype.enum_class}, enum_options
        )

    def test_enum16(self):
        enum_options = {'first': 1024, 'second': 2048}
        coltype = self._type_round_trip(
            types.Enum16(enum.Enum('any16_enum', enum_options))
        )[0]

        self.assertIsInstance(coltype, types.Enum16)
        self.assertEqual(
            {o.name: o.value for o in coltype.enum_class}, enum_options
        )
