from sqlalchemy import text

from tests.testcase import NativeSessionTestCase
from tests.util import require_server_version


class CursorTestCase(NativeSessionTestCase):
    def test_execute_without_context(self):
        raw = self.session.bind.raw_connection()
        cur = raw.cursor()

        cur.execute("SELECT * FROM system.numbers LIMIT 1")
        rv = cur.fetchall()
        self.assertEqual(len(rv), 1)

    def test_execute_with_context(self):
        rv = self.session.execute(text("SELECT * FROM system.numbers LIMIT 1"))

        self.assertEqual(len(rv.fetchall()), 1)

    def test_check_iter_cursor(self):
        rv = self.session.execute(
            text('SELECT number FROM system.numbers LIMIT 5')
        )
        self.assertListEqual(list(rv), [(x,) for x in range(5)])

    def test_execute_with_stream(self):
        rv = self.session.execute(
            text("SELECT * FROM system.numbers LIMIT 1")
        ).yield_per(10)

        self.assertEqual(len(rv.fetchall()), 1)

    def test_with_stream_results(self):
        rv = self.session.execute(text("SELECT * FROM system.numbers LIMIT 1"),
                                  execution_options={"stream_results": True})

        self.assertEqual(len(rv.fetchall()), 1)

    @require_server_version(23, 2, 1)
    def test_with_settings_in_execution_options(self):
        rv = self.session.execute(
            text("SELECT * FROM system.numbers LIMIT 1"),
            execution_options={"settings": {"final": 1}}
        )
        self.assertEqual(
            dict(rv.context.execution_options), {"settings": {"final": 1}}
        )
        self.assertEqual(len(rv.fetchall()), 1)
