from tests.testcase import HttpSessionTestCase


class CursorTestCase(HttpSessionTestCase):
    def test_check_iter_cursor(self):
        rv = self.session.execute('SELECT number FROM system.numbers LIMIT 5')
        self.assertListEqual(list(rv), [(x,) for x in range(5)])
