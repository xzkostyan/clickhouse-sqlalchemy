from sqlalchemy import text
from sqlalchemy.util.concurrency import greenlet_spawn

from tests.testcase import AsynchSessionTestCase


class CursorTestCase(AsynchSessionTestCase):
    async def test_execute_without_context(self):
        raw = await self.session.bind.raw_connection()
        cur = await greenlet_spawn(lambda: raw.cursor())

        await greenlet_spawn(
            lambda: cur.execute('SELECT * FROM system.numbers LIMIT 1')
        )
        rv = cur.fetchall()

        self.assertEqual(len(rv), 1)

        raw.close()

    async def test_execute_with_context(self):
        rv = await self.session.execute(
            text('SELECT * FROM system.numbers LIMIT 1')
        )

        self.assertEqual(len(rv.fetchall()), 1)

    async def test_check_iter_cursor(self):
        rv = await self.session.execute(
            text('SELECT number FROM system.numbers LIMIT 5')
        )

        self.assertListEqual(list(rv), [(x,) for x in range(5)])

    async def test_execute_with_stream(self):
        async with self.connection.stream(
            text("SELECT * FROM system.numbers LIMIT 10"),
            execution_options={'max_block_size': 1}
        ) as result:
            idx = 0
            async for r in result:
                self.assertEqual(r[0], idx)
                idx += 1

        self.assertEqual(idx, 10)
