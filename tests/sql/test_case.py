from sqlalchemy import literal, case

from tests.testcase import BaseTestCase


class CaseTestCase(BaseTestCase):
    def test_else_required(self):
        expression = case((literal(1), 0))

        self.assertEqual(
            self.compile(expression, literal_binds=True),
            'CASE WHEN 1 THEN 0 END'
        )

    def test_case(self):
        expression = case((literal(1), 0), else_=1)
        self.assertEqual(
            self.compile(expression, literal_binds=True),
            'CASE WHEN 1 THEN 0 ELSE 1 END'
        )

        expression = case((literal(1), 0), (literal(2), 1), else_=1)
        self.assertEqual(
            self.compile(expression, literal_binds=True),
            'CASE WHEN 1 THEN 0 WHEN 2 THEN 1 ELSE 1 END'
        )
