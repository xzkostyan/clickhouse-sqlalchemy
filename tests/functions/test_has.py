from sqlalchemy import func

from tests.testcase import BaseTestCase


class HasTestCase(BaseTestCase):
    def test_has_any(self):
        self.assertEqual(
            self.compile(func.has([1, 2], 1), literal_binds=True),
            'has([1, 2], 1)'
        )

        self.assertEqual(
            self.compile(func.hasAny([1], []), literal_binds=True),
            'hasAny([1], [])'
        )

        self.assertEqual(
            self.compile(func.hasAll(['a', 'b'], ['a']), literal_binds=True),
            "hasAll(['a', 'b'], ['a'])"
        )
