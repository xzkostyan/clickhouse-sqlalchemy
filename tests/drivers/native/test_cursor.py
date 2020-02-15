from tests.testcase import NativeSessionTestCase


class CursorTestCase(NativeSessionTestCase):
    def test_execute_without_context(self):
        raw = self.session.bind.raw_connection()
        cur = raw.cursor()

        cur.execute("SELECT * FROM system.numbers LIMIT 1")
        rv = cur.fetchall()
        self.assertEqual(len(rv), 1)

    def test_execute_with_context(self):
        rv = self.session.execute("SELECT * FROM system.numbers LIMIT 1")

        self.assertEqual(len(rv.fetchall()), 1)

    def test_check_iter_cursor(self):
        rv = self.session.execute('SELECT number FROM system.numbers LIMIT 5')
        self.assertListEqual(list(rv), [(x,) for x in range(5)])
