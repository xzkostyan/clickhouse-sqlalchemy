from sqlalchemy.engine.url import URL

from clickhouse_sqlalchemy.drivers.native.base import ClickHouseDialect_native
from tests.testcase import BaseTestCase


class TestConnectArgs(BaseTestCase):
    def setUp(self):
        self.dialect = ClickHouseDialect_native()
        self.url = URL(
            drivername='clickhouse+native',
            username='default',
            password='default',
            host='localhost',
            port='9001',
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

    def test_no_auth(self):
        self.url.username = None
        self.url.password = None
        connect_args = self.dialect.create_connect_args(self.url)
        self.assertEqual(
            connect_args[0],
            ('localhost', 9001, 'default', 'default', '')
        )

    def test_default_ports(self):
        self.url.port = None
        connect_args = self.dialect.create_connect_args(self.url)
        self.assertEqual(
            connect_args[0],
            ('localhost', 9000, 'default', 'default', 'default')
        )

        self.url.query = {'secure': 'true'}
        connect_args = self.dialect.create_connect_args(self.url)
        self.assertEqual(
            connect_args[0],
            ('localhost', 9440, 'default', 'default', 'default')
        )
