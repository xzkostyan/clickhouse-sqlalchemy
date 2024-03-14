from sqlalchemy import text

from tests.testcase import HttpSessionTestCase, HttpEngineTestCase
from tests.util import require_server_version


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

    @require_server_version(23, 2, 1)
    def test_with_settings_in_execution_options(self):
        rv = self.session.execute(
            text("SELECT number FROM system.numbers LIMIT 5"),
            execution_options={"settings": {"final": 1}}
        )
        self.assertEqual(
            dict(rv.context.execution_options), {"settings": {"final": 1}}
        )
        self.assertListEqual(list(rv), [(x,) for x in range(5)])
