from sqlalchemy import sql

from tests.testcase import BaseTestCase


class VisitTestCase(BaseTestCase):
    def test_true_false(self):
        self.assertEqual(self.compile(sql.false()), '0')
        self.assertEqual(self.compile(sql.true()), '1')
