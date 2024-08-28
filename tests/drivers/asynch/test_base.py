import asynch.errors
import sqlalchemy
import sqlalchemy.event
from sqlalchemy.engine.url import URL

from clickhouse_sqlalchemy.drivers.asynch.base import ClickHouseDialect_asynch
from tests.testcase import AsynchSessionTestCase, BaseTestCase


class TestConnectArgs(BaseTestCase):
    def setUp(self):
        self.dialect = ClickHouseDialect_asynch()

    def test_simple_url(self):
        url = URL.create(
            drivername='clickhouse+asynch',
            host='localhost',
            database='default',
        )
        connect_args = self.dialect.create_connect_args(url)
        self.assertEqual(
            str(connect_args[0][0]), 'clickhouse://localhost/default'
        )

    def test_secure_false(self):
        url = URL.create(
            drivername='clickhouse+asynch',
            username='default',
            password='default',
            host='localhost',
            port=9001,
            database='default',
            query={'secure': 'False'}
        )
        connect_args = self.dialect.create_connect_args(url)
        self.assertEqual(
            str(connect_args[0][0]),
            'clickhouse://default:default@localhost:9001/default?secure=False'
        )

    def test_no_auth(self):
        url = URL.create(
            drivername='clickhouse+asynch',
            host='localhost',
            port=9001,
            database='default',
        )
        connect_args = self.dialect.create_connect_args(url)
        self.assertEqual(
            str(connect_args[0][0]), 'clickhouse://localhost:9001/default'
        )


class DBApiTestCase(AsynchSessionTestCase):
    async def test_error_handler(self):
        class MockedException(Exception):
            pass

        def handle_error(e: sqlalchemy.engine.ExceptionContext):
            if isinstance(
                e.original_exception,
                asynch.errors.ServerException,
            ):
                raise MockedException()

        engine = self.session.get_bind()
        sqlalchemy.event.listen(engine, 'handle_error', handle_error)

        with self.assertRaises(MockedException):
            await self.session.execute(sqlalchemy.text('SELECT'))
