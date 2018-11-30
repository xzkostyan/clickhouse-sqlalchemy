from tests.testcase import NativeSessionTestCase


class CursorTestCase(NativeSessionTestCase):
    def test_execute_without_context(self):
        raw = self.session.bind.raw_connection()
        cur = raw.cursor()

        cur.execute("SELECT * FROM system.numbers LIMIT 1")
        rv = cur.fetchall()
        assert len(rv) == 1

    def test_execute_with_context(self):
        rv = self.session.execute("SELECT * FROM system.numbers LIMIT 1")

        assert len(rv.fetchall()) == 1
