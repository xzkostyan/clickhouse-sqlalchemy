from six import text_type
from sqlalchemy import literal

from tests.session import session
from tests.testcase import BaseTestCase


class EscapingTestCase(BaseTestCase):
    def compile(self, clause, **kwargs):
        return text_type(self._compile(clause, **kwargs))

    def test_select_escaping(self):
        self.assertEqual(
            self.compile(session.query(literal('\t')), literal_binds=True),
            "SELECT '\t' AS param_1"
        )
    # TODO: test escaping \t, etc.
