from sqlalchemy.engine.url import URL

from clickhouse_sqlalchemy.drivers.asynch.base import ClickHouseDialect_asynch
from tests.testcase import BaseTestCase


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
