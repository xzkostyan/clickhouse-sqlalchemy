from unittest import TestCase

from clickhouse_sqlalchemy.drivers.util import get_inner_spec, parse_arguments


class GetInnerSpecTestCase(TestCase):
    def test_get_inner_spec(self):
        self.assertEqual(
            get_inner_spec("DateTime('Europe/Paris')"), "'Europe/Paris'"
        )
        self.assertEqual(get_inner_spec('Decimal(18, 2)'), "18, 2")
        self.assertEqual(get_inner_spec('DateTime64(3)'), "3")


class ParseArgumentsTestCase(TestCase):
    def test_parse_arguments(self):
        self.assertEqual(
            parse_arguments('uniq, UInt64'), ('uniq', 'UInt64')
        )
        self.assertEqual(
            parse_arguments('anyIf, String, UInt8'),
            ('anyIf', 'String', 'UInt8')
        )
        self.assertEqual(
            parse_arguments('quantiles(0.5, 0.9), UInt64'),
            ('quantiles(0.5, 0.9)', 'UInt64')
        )
        self.assertEqual(
            parse_arguments('sum, Int64, Int64'), ('sum', 'Int64', 'Int64')
        )
        self.assertEqual(
            parse_arguments('sum, Nullable(Int64), Int64'),
            ('sum', 'Nullable(Int64)', 'Int64')
        )
        self.assertEqual(
            parse_arguments('Float32, Decimal(18, 2)'),
            ('Float32', 'Decimal(18, 2)')
        )
        self.assertEqual(
            parse_arguments('sum, Float32, Decimal(18, 2)'),
            ('sum', 'Float32', 'Decimal(18, 2)')
        )
        self.assertEqual(
            parse_arguments('quantiles(0.5, 0.9), UInt64'),
            ('quantiles(0.5, 0.9)', 'UInt64')
        )
        self.assertEqual(
            parse_arguments("sumIf(total, status = 'accepted'), Float32"),
            ("sumIf(total, status = 'accepted')", "Float32")
        )
