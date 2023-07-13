from sqlalchemy import text

from tests.testcase import HttpSessionTestCase, HttpEngineTestCase


class CursorTestCase(HttpSessionTestCase, HttpEngineTestCase):
    def test_check_iter_cursor_by_session(self):
        rv = self.session.execute(
            text('SELECT number FROM system.numbers LIMIT 5')
        )
        self.assertListEqual(list(rv), [(x,) for x in range(5)])

    def test_check_iter_cursor_by_engine(self):
        with self.engine.connect() as conn:
            rv = conn.execute(
                text('SELECT number FROM system.numbers LIMIT 5')
            )
            self.assertListEqual(list(rv), [(x,) for x in range(5)])
