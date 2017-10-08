from sqlalchemy import literal, exc, case

from tests.testcase import BaseTestCase


class CaseTestCase(BaseTestCase):
    def test_else_required(self):
        with self.assertRaises(exc.CompileError) as ex:
            self.compile(case([(literal(1), 0)]))

        self.assertEqual(
            str(ex.exception),
            'ELSE clause is required in CASE'
        )

    def test_case(self):
        expression = case([(literal(1), 0)], else_=1)
        self.assertEqual(
            self.compile(expression, literal_binds=True),
            'CASE WHEN 1 THEN 0 ELSE 1 END'
        )
