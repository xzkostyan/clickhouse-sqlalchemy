from unittest import TestCase

from clickhouse_sqlalchemy.drivers.util import get_inner_spec


class GetInnerSpecTestCase(TestCase):
    def test_get_inner_spec(self):
        self.assertEqual(
            get_inner_spec("DateTime('Europe/Paris')"), "'Europe/Paris'"
        )
        self.assertEqual(get_inner_spec('Decimal(18, 2)'), "18, 2")
        self.assertEqual(get_inner_spec('DateTime64(3)'), "3")
