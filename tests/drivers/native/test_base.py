from sqlalchemy.engine.url import URL

from clickhouse_sqlalchemy.drivers.native.base import ClickHouseDialect_native
from tests.testcase import BaseTestCase


class TestCreateEngineUrlQueryParameterSecure(BaseTestCase):
    def setUp(self):
        self.dialect = ClickHouseDialect_native()
        self.url = URL(
            drivername='clickhouse+native',
            username='default',
            password='default',
            host='localhost',
            port='9000',
            database='default',
        )

    def test_secure_false(self):
        self.url.query = {'secure': 'false'}
        connect_args = self.dialect.create_connect_args(self.url)
        self.assertEqual(connect_args[1], {'secure': False})

    def test_secure_true(self):
        self.url.query = {'secure': 'true'}
        connect_args = self.dialect.create_connect_args(self.url)
        self.assertEqual(connect_args[1], {'secure': True})

    def test_secure_not_specified(self):
        self.url.query = {}
        connect_args = self.dialect.create_connect_args(self.url)
        self.assertEqual(connect_args[1], {})
