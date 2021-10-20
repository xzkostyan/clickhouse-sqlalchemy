from sqlalchemy import select, literal

from tests.testcase import BaseTestCase


class IsDistinctFromTestCase(BaseTestCase):

    def _select_bool(self, expr) -> bool:
        query = select([expr])
        (result,), = self.session.execute(query).fetchall()
        return bool(result)

    def test_is_distinct_from(self):
        distinct_value_pairs = [
            (1, 2),
            (1, None),
            (None, "NULL"),
            (None, "ᴺᵁᴸᴸ"),
            ((1, None), (2, None)),
            ((1, (1, None)), (1, (2, None)))
        ]
        for a, b in distinct_value_pairs:
            with self.subTest():
                self.assertTrue(self._select_bool(literal(a).is_distinct_from(b)))
                self.assertFalse(self._select_bool(literal(a).isnot_distinct_from(b)))

        values = [None, (1, None), (None, None), (1, (1, None))]
        for v in values:
            with self.subTest():
                self.assertTrue(self._select_bool(literal(v).isnot_distinct_from(v)))
                self.assertFalse(self._select_bool(literal(v).is_distinct_from(v)))
