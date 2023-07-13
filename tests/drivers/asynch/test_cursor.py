from sqlalchemy import text
from sqlalchemy.util.concurrency import greenlet_spawn

from tests.testcase import AsynchSessionTestCase
from tests.util import run_async


class CursorTestCase(AsynchSessionTestCase):
    @run_async
    async def test_execute_without_context(self):
        raw = await self.session.bind.raw_connection()
        cur = await greenlet_spawn(lambda: raw.cursor())

        await greenlet_spawn(
            lambda: cur.execute('SELECT * FROM system.numbers LIMIT 1')
        )
        rv = cur.fetchall()

        self.assertEqual(len(rv), 1)

        raw.close()

    @run_async
    async def test_execute_with_context(self):
        rv = await self.session.execute(
            text('SELECT * FROM system.numbers LIMIT 1')
        )

        self.assertEqual(len(rv.fetchall()), 1)

    @run_async
    async def test_check_iter_cursor(self):
        rv = await self.session.execute(
            text('SELECT number FROM system.numbers LIMIT 5')
        )

        self.assertListEqual(list(rv), [(x,) for x in range(5)])
