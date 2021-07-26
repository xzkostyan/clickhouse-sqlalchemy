from tests.testcase import HttpSessionTestCase, HttpEngineTestCase


class CursorTestCase(HttpSessionTestCase, HttpEngineTestCase):
    def test_check_iter_cursor_by_session(self):
        rv = self.session.execute('SELECT number FROM system.numbers LIMIT 5')
        self.assertListEqual(list(rv), [(x,) for x in range(5)])

    def test_check_iter_cursor_by_engine(self):
        rv = self.engine.execute('SELECT number FROM system.numbers LIMIT 5')
        self.assertListEqual(list(rv), [(x,) for x in range(5)])
