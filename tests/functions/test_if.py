from sqlalchemy import literal, func, text

from tests.testcase import BaseTestCase


class IfTestCase(BaseTestCase):
    def test_if(self):
        expression = func.if_(
            literal(1) > literal(2),
            text('a'),
            text('b'),
        )

        self.assertEqual(
            self.compile(expression, literal_binds=True),
            'if(1 > 2, a, b)'
        )
