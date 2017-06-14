import enum
from sqlalchemy import sql

from tests.testcase import BaseTestCase
from src import types


class VisitTestCase(BaseTestCase):
    def test_true_false(self):
        self.assertEqual(self.compile(sql.false()), '0')
        self.assertEqual(self.compile(sql.true()), '1')

    def test_array(self):
        self.assertEqual(
            self.compile(types.Array(types.Int32())),
            'Array(Int32)'
        )
        self.assertEqual(
            self.compile(types.Array(types.Array(types.Int32()))),
            'Array(Array(Int32))'
        )

    def test_enum(self):
        class MyEnum(enum.Enum):
            foo = 100
            bar = 500

        self.assertEqual(
            self.compile(types.Enum16(MyEnum)),
            "Enum16('foo' = 100, 'bar' = 500)"
        )

        MyEnum = enum.Enum('MyEnum', {" ' t = ": -1, "test": 2})

        self.assertEqual(
            self.compile(types.Enum16(MyEnum)),
            "Enum16(' \\' t = ' = -1, 'test' = 2)"
        )
