import enum
from sqlalchemy import sql

from src import types
from src.util import compat
from tests.testcase import BaseTestCase


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
            __order__ = 'foo bar'
            foo = 100
            bar = 500

        self.assertEqual(
            self.compile(types.Enum16(MyEnum)),
            "Enum16('foo' = 100, 'bar' = 500)"
        )

        if compat.PY3:
            data = [" ' t = ", "test"]
        else:
            from collections import OrderedDict
            data = OrderedDict([(" ' t = ", 1), ("test", 2)])

        MyEnum = enum.Enum('MyEnum', data)

        self.assertEqual(
            self.compile(types.Enum8(MyEnum)),
            "Enum8(' \\' t = ' = 1, 'test' = 2)"
        )
