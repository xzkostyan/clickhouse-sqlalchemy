from decimal import Decimal
from datetime import date

from sqlalchemy import literal

from src.drivers.escaper import Escaper
from tests.session import session
from tests.testcase import BaseTestCase


class EscapingTestCase(BaseTestCase):
    def compile(self, clause, **kwargs):
        return str(self._compile(clause, **kwargs))

    def test_select_escaping(self):
        self.assertEqual(
            self.compile(session.query(literal('\t')), literal_binds=True),
            "SELECT '\t' AS param_1"
        )

    def test_escaper(self):
        e = Escaper()
        self.assertEqual(e.escape([None]), ['NULL'])
        self.assertEqual(e.escape([[123]]), [[123]])
        self.assertEqual(e.escape({'x': 'str'}), {'x': "'str'"})
        self.assertEqual(e.escape([Decimal('10')]), [10.0])
        self.assertEqual(e.escape([10.0]), [10.0])
        self.assertEqual(e.escape([date(2017, 1, 2)]), ["'2017-01-02'"])

        with self.assertRaises(Exception) as ex:
            e.escape([object()])

        self.assertIn('Unsupported object', str(ex.exception))

        with self.assertRaises(Exception) as ex:
            e.escape('str')

        self.assertIn('Unsupported param format', str(ex.exception))
