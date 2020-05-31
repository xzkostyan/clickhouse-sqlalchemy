from unittest import TestCase

from clickhouse_sqlalchemy.engines.util import parse_columns


class ParseColumnsTestCase(TestCase):
    def test_empty(self):
        self.assertEqual(parse_columns(''), [])

    def test_simple(self):
        self.assertEqual(parse_columns('x, y, z'), ['x', 'y', 'z'])
        self.assertEqual(parse_columns('xx, yy, zz'), ['xx', 'yy', 'zz'])

    def test_one(self):
        self.assertEqual(parse_columns('x'), ['x'])

    def test_quoted(self):
        self.assertEqual(parse_columns('` , `, `  ,  `'), [' , ', '  ,  '])

    def test_escaped(self):
        self.assertEqual(
            parse_columns('` \\`, `, `  \\`,  `'),
            [' `, ', '  `,  ']
        )
        self.assertEqual(parse_columns('` \\`\\` `'), [' `` '])

    def test_brackets(self):
        self.assertEqual(
            parse_columns('test(a, b), test(c, d)'),
            ['test(a, b)', 'test(c, d)']
        )

        self.assertEqual(
            parse_columns('x, (y, z)'), ['x', '(y, z)']
        )
