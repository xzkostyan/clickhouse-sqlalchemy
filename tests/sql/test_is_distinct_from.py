# coding: utf-8

from parameterized import parameterized
from sqlalchemy import select, literal

from tests.testcase import BaseTestCase


class IsDistinctFromTestCase(BaseTestCase):
    def _select_bool(self, expr):
        query = select([expr])
        (result,), = self.session.execute(query).fetchall()
        return bool(result)

    @parameterized.expand([
        (1, 2),
        (1, None),
        (None, "NULL"),
        (None, u"ᴺᵁᴸᴸ"),
        ((1, None), (2, None)),
        ((1, (1, None)), (1, (2, None)))
    ])
    def test_is_distinct_from(self, a, b):
        self.assertTrue(self._select_bool(literal(a).is_distinct_from(b)))
        self.assertFalse(self._select_bool(literal(a).isnot_distinct_from(b)))

    @parameterized.expand([
        (None, ),
        ((1, None), ),
        ((None, None), ),
        ((1, (1, None)), )
    ])
    def test_is_self_distinct_from(self, v):
        self.assertTrue(self._select_bool(literal(v).isnot_distinct_from(v)))
        self.assertFalse(self._select_bool(literal(v).is_distinct_from(v)))
