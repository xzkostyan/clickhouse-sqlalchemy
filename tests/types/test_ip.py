from __future__ import unicode_literals

from ipaddress import IPv4Address, IPv4Network, IPv6Address, IPv6Network
from sqlalchemy import Column, and_
from sqlalchemy.sql.ddl import CreateTable

from clickhouse_sqlalchemy import types, engines, Table
from tests.testcase import BaseTestCase
from tests.util import with_native_and_http_sessions


@with_native_and_http_sessions
class IPv4TestCase(BaseTestCase):
    required_server_version = (19, 3, 3)

    table = Table(
        'test', BaseTestCase.metadata(),
        Column('x', types.IPv4),
        engines.Memory()
    )

    def test_create_table(self):
        self.assertEqual(
            self.compile(CreateTable(self.table)),
            'CREATE TABLE test (x IPv4) ENGINE = Memory'
        )

    def test_select_insert(self):
        a = IPv4Address('10.0.0.1')

        with self.create_table(self.table):
            self.session.execute(self.table.insert(), [{'x': a}])
            self.assertEqual(self.session.query(self.table.c.x).scalar(), a)

    def test_select_insert_string(self):
        a = '10.0.0.1'

        with self.create_table(self.table):
            self.session.execute(self.table.insert(), [{'x': a}])
            self.assertEqual(self.session.query(self.table.c.x).scalar(),
                             IPv4Address('10.0.0.1'))

    def test_select_where_address(self):
        a = IPv4Address('10.0.0.1')

        with self.create_table(self.table):
            self.session.execute(self.table.insert(), [{'x': a}])

            self.assertEqual(self.session.query(self.table.c.x).filter(
                self.table.c.x == IPv4Address('10.0.0.1')).scalar(), a)
            self.assertEqual(self.session.query(self.table.c.x).filter(
                and_(IPv4Address('10.0.0.0') < self.table.c.x,
                     self.table.c.x < IPv4Address('10.0.0.2'))).scalar(), a)

    def test_select_where_string(self):
        a = IPv4Address('10.0.0.1')

        with self.create_table(self.table):
            self.session.execute(self.table.insert(), [{'x': a}])

            self.assertEqual(self.session.query(self.table.c.x).filter(
                and_('10.0.0.0' < self.table.c.x,
                     self.table.c.x < '10.0.0.2')).scalar(), a)

    def test_select_where_literal(self):
        a = IPv4Address('10.0.0.1')

        with self.create_table(self.table):
            self.session.execute(self.table.insert(), [{'x': a}])
            qs = self.session.query(self.table.c.x).filter(
                self.table.c.x == '10.0.0.1'
            )
            statement = self.compile(qs, literal_binds=True)
            self.assertEqual(statement,
                             "SELECT test.x AS test_x FROM test "
                             "WHERE test.x = toIPv4('10.0.0.1')")

    def test_select_in_network(self):
        ips = [
            IPv4Address('10.0.0.1'),
            IPv4Address('10.0.0.2'),
            IPv4Address('10.0.0.3'),
            IPv4Address('192.168.0.1')
        ]

        with self.create_table(self.table):
            self.session.execute(self.table.insert(),
                                 [{'x': ip} for ip in ips])

            self.assertEqual(
                self.session.query(self.table.c.x).filter(
                    self.table.c.x.in_(IPv4Network('10.0.0.0/8'))).all(), [
                    (IPv4Address('10.0.0.1'),),
                    (IPv4Address('10.0.0.2'),),
                    (IPv4Address('10.0.0.3'),)
                ])

    def test_select_in_list_address(self):
        ips = [
            IPv4Address('10.0.0.1'),
            IPv4Address('10.0.0.2'),
            IPv4Address('10.0.0.3'),
            IPv4Address('192.168.0.1')
        ]

        with self.create_table(self.table):
            self.session.execute(self.table.insert(),
                                 [{'x': ip} for ip in ips])

            self.assertEqual(
                self.session.query(self.table.c.x).filter(
                    self.table.c.x.in_([
                        IPv4Address('10.0.0.1'),
                        IPv4Address('10.0.0.2'),
                        '10.0.0.3'
                    ])).all(), [
                    (IPv4Address('10.0.0.1'),),
                    (IPv4Address('10.0.0.2'),),
                    (IPv4Address('10.0.0.3'),)
                ])

    def test_select_in_list_network(self):
        ips = [
            IPv4Address('10.0.0.1'),
            IPv4Address('10.0.0.2'),
            IPv4Address('10.1.0.3'),
            IPv4Address('192.168.0.1')
        ]

        with self.create_table(self.table):
            self.session.execute(self.table.insert(),
                                 [{'x': ip} for ip in ips])

            self.assertEqual(
                self.session.query(self.table.c.x).filter(
                    self.table.c.x.in_(['10.0.0.0/24',
                                        '10.1.0.0/24'])).all(), [
                    (IPv4Address('10.0.0.1'),),
                    (IPv4Address('10.0.0.2'),),
                    (IPv4Address('10.1.0.3'),)
                ])

    def test_select_in_list_network_and_address(self):
        ips = [
            IPv4Address('10.0.0.1'),
            IPv4Address('10.0.0.2'),
            IPv4Address('10.1.0.3'),
            IPv4Address('192.168.0.1')
        ]

        with self.create_table(self.table):
            self.session.execute(self.table.insert(),
                                 [{'x': ip} for ip in ips])

            self.assertEqual(
                self.session.query(self.table.c.x).filter(
                    self.table.c.x.in_(['10.0.0.0/24', '10.1.0.3'])).all(), [
                    (IPv4Address('10.0.0.1'),),
                    (IPv4Address('10.0.0.2'),),
                    (IPv4Address('10.1.0.3'),)
                ])

    def test_select_in_list_empty(self):
        ips = [
            IPv4Address('10.0.0.1'),
            IPv4Address('10.0.0.2'),
            IPv4Address('10.1.0.3'),
            IPv4Address('192.168.0.1')
        ]

        with self.create_table(self.table):
            self.session.execute(self.table.insert(),
                                 [{'x': ip} for ip in ips])

            self.assertEqual(
                self.session.query(self.table.c.x).filter(
                    self.table.c.x.in_([])).all(), [])

    def test_select_in_string(self):
        ips = [
            IPv4Address('10.0.0.1'),
            IPv4Address('10.0.0.2'),
            IPv4Address('10.0.0.3'),
            IPv4Address('192.168.0.1')
        ]

        with self.create_table(self.table):
            self.session.execute(self.table.insert(),
                                 [{'x': ip} for ip in ips])

            self.assertEqual(
                self.session.query(self.table.c.x).filter(
                    self.table.c.x.in_('10.0.0.0/8')).all(), [
                    (IPv4Address('10.0.0.1'),),
                    (IPv4Address('10.0.0.2'),),
                    (IPv4Address('10.0.0.3'),)
                ])

    def test_select_not_in_network(self):
        ips = [
            IPv4Address('10.0.0.1'),
            IPv4Address('10.0.0.2'),
            IPv4Address('10.0.0.3'),
            IPv4Address('192.168.0.1')
        ]

        with self.create_table(self.table):
            self.session.execute(self.table.insert(),
                                 [{'x': ip} for ip in ips])

            self.assertEqual(
                self.session.query(self.table.c.x).filter(
                    self.table.c.x.notin_(IPv4Network('10.0.0.0/8'))).all(), [
                    (IPv4Address('192.168.0.1'),)
                ])
            self.assertEqual(
                self.session.query(self.table.c.x).filter(
                    ~self.table.c.x.in_(IPv4Network('10.0.0.0/8'))).all(), [
                    (IPv4Address('192.168.0.1'),)
                ])

    def test_select_not_in_string(self):
        ips = [
            IPv4Address('10.0.0.1'),
            IPv4Address('10.0.0.2'),
            IPv4Address('10.0.0.3'),
            IPv4Address('192.168.0.1')
        ]

        with self.create_table(self.table):
            self.session.execute(self.table.insert(),
                                 [{'x': ip} for ip in ips])

            self.assertEqual(
                self.session.query(self.table.c.x).filter(
                    self.table.c.x.notin_('10.0.0.0/8')).all(), [
                    (IPv4Address('192.168.0.1'),)
                ])
            self.assertEqual(
                self.session.query(self.table.c.x).filter(
                    ~self.table.c.x.in_('10.0.0.0/8')).all(), [
                    (IPv4Address('192.168.0.1'),)
                ])

    def test_select_not_in_list_address(self):
        ips = [
            IPv4Address('10.0.0.1'),
            IPv4Address('10.0.0.2'),
            IPv4Address('10.1.0.3'),
            IPv4Address('192.168.0.1')
        ]

        with self.create_table(self.table):
            self.session.execute(self.table.insert(),
                                 [{'x': ip} for ip in ips])

            self.assertEqual(
                self.session.query(self.table.c.x).filter(
                    self.table.c.x.notin_(['10.0.0.2', '10.1.0.3'])).all(), [
                    (IPv4Address('10.0.0.1'),),
                    (IPv4Address('192.168.0.1'),)
                ])
            self.assertEqual(
                self.session.query(self.table.c.x).filter(
                    ~self.table.c.x.in_(['10.0.0.2', '10.1.0.3'])).all(), [
                    (IPv4Address('10.0.0.1'),),
                    (IPv4Address('192.168.0.1'),)
                ])

    def test_select_not_in_list_network(self):
        ips = [
            IPv4Address('10.0.0.1'),
            IPv4Address('10.0.0.2'),
            IPv4Address('10.1.0.3'),
            IPv4Address('192.168.0.1')
        ]

        with self.create_table(self.table):
            self.session.execute(self.table.insert(),
                                 [{'x': ip} for ip in ips])

            self.assertEqual(
                self.session.query(self.table.c.x).filter(
                    self.table.c.x.notin_(['10.0.0.0/24',
                                           '10.1.0.0/24'])).all(), [
                    (IPv4Address('192.168.0.1'),)
                ])
            self.assertEqual(
                self.session.query(self.table.c.x).filter(
                    ~self.table.c.x.in_(['10.0.0.0/24',
                                         '10.1.0.0/24'])).all(), [
                    (IPv4Address('192.168.0.1'),)
                ])

    def test_select_not_in_list_network_address(self):
        ips = [
            IPv4Address('10.0.0.1'),
            IPv4Address('10.0.0.2'),
            IPv4Address('10.1.0.3'),
            IPv4Address('192.168.0.1')
        ]

        with self.create_table(self.table):
            self.session.execute(self.table.insert(),
                                 [{'x': ip} for ip in ips])

            self.assertEqual(
                self.session.query(self.table.c.x).filter(
                    self.table.c.x.notin_(['10.0.0.0/24', '10.1.0.3'])).all(),
                [
                    (IPv4Address('192.168.0.1'),)
                ])
            self.assertEqual(
                self.session.query(self.table.c.x).filter(
                    ~self.table.c.x.in_(['10.0.0.0/24', '10.1.0.3'])).all(),
                [
                    (IPv4Address('192.168.0.1'),)
                ])

    def test_select_not_in_list_empty(self):
        ips = [
            IPv4Address('10.0.0.1'),
            IPv4Address('10.0.0.2'),
            IPv4Address('10.1.0.3'),
            IPv4Address('192.168.0.1')
        ]

        with self.create_table(self.table):
            self.session.execute(self.table.insert(),
                                 [{'x': ip} for ip in ips])

            self.assertEqual(
                self.session.query(self.table.c.x).filter(
                    self.table.c.x.notin_([])).all(), [
                    (IPv4Address('10.0.0.1'),),
                    (IPv4Address('10.0.0.2'),),
                    (IPv4Address('10.1.0.3'),),
                    (IPv4Address('192.168.0.1'),)
                ])
            self.assertEqual(
                self.session.query(self.table.c.x).filter(
                    ~self.table.c.x.in_([])).all(), [
                    (IPv4Address('10.0.0.1'),),
                    (IPv4Address('10.0.0.2'),),
                    (IPv4Address('10.1.0.3'),),
                    (IPv4Address('192.168.0.1'),)
                ])


class IPv6TestCase(BaseTestCase):
    required_server_version = (19, 3, 3)

    table = Table(
        'test', BaseTestCase.metadata(),
        Column('x', types.IPv6),
        engines.Memory()
    )

    def test_select_insert(self):
        a = IPv6Address('79f4:e698:45de:a59b:2765:28e3:8d3a:35ae')

        with self.create_table(self.table):
            self.session.execute(self.table.insert(), [{'x': a}])
            self.assertEqual(self.session.query(self.table.c.x).scalar(), a)

    def test_select_insert_string(self):
        a = '79f4:e698:45de:a59b:2765:28e3:8d3a:35ae'

        with self.create_table(self.table):
            self.session.execute(self.table.insert(), [{'x': a}])
            self.assertEqual(self.session.query(self.table.c.x).scalar(),
                             IPv6Address(a))

    def test_create_table(self):
        self.assertEqual(
            self.compile(CreateTable(self.table)),
            'CREATE TABLE test (x IPv6) ENGINE = Memory'
        )

    def test_select_where_address(self):
        a = IPv6Address('42e::2')

        with self.create_table(self.table):
            self.session.execute(self.table.insert(), [{'x': a}])

            self.assertEqual(self.session.query(self.table.c.x).filter(
                self.table.c.x == IPv6Address('42e::2')).scalar(), a)

            self.assertEqual(self.session.query(self.table.c.x).filter(
                and_(IPv6Address('42e::1') < self.table.c.x,
                     self.table.c.x < IPv6Address('42e::3'))).scalar(), a)

    def test_select_where_string(self):
        a = IPv6Address('42e::2')

        with self.create_table(self.table):
            self.session.execute(self.table.insert(), [{'x': a}])

            self.assertEqual(self.session.query(self.table.c.x).filter(
                self.table.c.x == '42e::2').scalar(), a)

            self.assertEqual(self.session.query(self.table.c.x).filter(
                and_('42e::1' < self.table.c.x,
                     self.table.c.x < '42e::3')).scalar(), a)

    def test_select_where_literal(self):
        a = IPv6Address('42e::2')

        with self.create_table(self.table):
            self.session.execute(self.table.insert(), [{'x': a}])
            qs = self.session.query(self.table.c.x).filter(
                self.table.c.x == '42e::2')
            statement = self.compile(qs, literal_binds=True)
            self.assertEqual(statement,
                             "SELECT test.x AS test_x FROM test "
                             "WHERE test.x = toIPv6('42e::2')")

    def test_select_in_network(self):
        ips = [
            IPv6Address('42e::1'),
            IPv6Address('42e::2'),
            IPv6Address('42e::3'),
            IPv6Address('f42e::ffff')
        ]

        with self.create_table(self.table):
            self.session.execute(self.table.insert(),
                                 [{'x': ip} for ip in ips])

            self.assertEqual(
                self.session.query(self.table.c.x).filter(
                    self.table.c.x.in_(IPv6Network('42e::/64'))).all(), [
                    (IPv6Address('42e::1'),),
                    (IPv6Address('42e::2'),),
                    (IPv6Address('42e::3'),)
                ])

    def test_select_in_list_address(self):
        ips = [
            IPv6Address('42e::1'),
            IPv6Address('42e::2'),
            IPv6Address('7::3'),
            IPv6Address('f42e::ffff')
        ]

        with self.create_table(self.table):
            self.session.execute(self.table.insert(),
                                 [{'x': ip} for ip in ips])

            self.assertEqual(
                self.session.query(self.table.c.x).filter(
                    self.table.c.x.in_(['42e::1', '42e::2',
                                        'f42e::ffff'])).all(), [
                    (IPv6Address('42e::1'),),
                    (IPv6Address('42e::2'),),
                    (IPv6Address('f42e::ffff'),)
                ])

    def test_select_in_list_network(self):
        ips = [
            IPv6Address('42e::1'),
            IPv6Address('42e::2'),
            IPv6Address('a42e::3'),
            IPv6Address('f42e::ffff')
        ]

        with self.create_table(self.table):
            self.session.execute(self.table.insert(),
                                 [{'x': ip} for ip in ips])

            self.assertEqual(
                self.session.query(self.table.c.x).filter(
                    self.table.c.x.in_([IPv6Network('42e::/64'),
                                        IPv6Network('a42e::/48')])).all(), [
                    (IPv6Address('42e::1'),),
                    (IPv6Address('42e::2'),),
                    (IPv6Address('a42e::3'),)
                ])

    def test_select_in_list_network_address(self):
        ips = [
            IPv6Address('42e::1'),
            IPv6Address('42e::2'),
            IPv6Address('a42e::3'),
            IPv6Address('f42e::ffff')
        ]

        with self.create_table(self.table):
            self.session.execute(self.table.insert(),
                                 [{'x': ip} for ip in ips])

            self.assertEqual(
                self.session.query(self.table.c.x).filter(
                    self.table.c.x.in_(['42e::/64', 'a42e::3'])).all(), [
                    (IPv6Address('42e::1'),),
                    (IPv6Address('42e::2'),),
                    (IPv6Address('a42e::3'),)
                ])

    def test_select_in_list_empty(self):
        ips = [
            IPv6Address('42e::1'),
            IPv6Address('42e::2'),
            IPv6Address('a42e::3'),
            IPv6Address('f42e::ffff')
        ]

        with self.create_table(self.table):
            self.session.execute(self.table.insert(),
                                 [{'x': ip} for ip in ips])

            self.assertEqual(
                self.session.query(self.table.c.x).filter(
                    self.table.c.x.in_([])).all(), [])

    def test_select_in_string(self):
        ips = [
            IPv6Address('42e::1'),
            IPv6Address('42e::2'),
            IPv6Address('42e::3'),
            IPv6Address('f42e::ffff')
        ]

        with self.create_table(self.table):
            self.session.execute(self.table.insert(),
                                 [{'x': ip} for ip in ips])

            self.assertEqual(
                self.session.query(self.table.c.x).filter(
                    self.table.c.x.in_('42e::/64')).all(), [
                    (IPv6Address('42e::1'),),
                    (IPv6Address('42e::2'),),
                    (IPv6Address('42e::3'),)
                ])

    def test_select_not_in_network(self):
        ips = [
            IPv6Address('42e::1'),
            IPv6Address('42e::2'),
            IPv6Address('42e::3'),
            IPv6Address('f42e::ffff')
        ]

        with self.create_table(self.table):
            self.session.execute(self.table.insert(),
                                 [{'x': ip} for ip in ips])

            self.assertEqual(
                self.session.query(self.table.c.x).filter(
                    self.table.c.x.notin_(IPv6Network('42e::/64'))).all(), [
                    (IPv6Address('f42e::ffff'),)
                ])
            self.assertEqual(
                self.session.query(self.table.c.x).filter(
                    ~self.table.c.x.in_(IPv6Network('42e::/64'))).all(), [
                    (IPv6Address('f42e::ffff'),)
                ])

    def test_select_not_in_string(self):
        ips = [
            IPv6Address('42e::1'),
            IPv6Address('42e::2'),
            IPv6Address('42e::3'),
            IPv6Address('f42e::ffff')
        ]

        with self.create_table(self.table):
            self.session.execute(self.table.insert(),
                                 [{'x': ip} for ip in ips])

            self.assertEqual(
                self.session.query(self.table.c.x).filter(
                    self.table.c.x.notin_('42e::/64')).all(), [
                    (IPv6Address('f42e::ffff'),)
                ])
            self.assertEqual(
                self.session.query(self.table.c.x).filter(
                    ~self.table.c.x.in_('42e::/64')).all(), [
                    (IPv6Address('f42e::ffff'),)
                ])

    def test_select_not_in_list_address(self):
        ips = [
            IPv6Address('42e::1'),
            IPv6Address('42e::2'),
            IPv6Address('42e::3'),
            IPv6Address('f42e::ffff')
        ]

        with self.create_table(self.table):
            self.session.execute(self.table.insert(),
                                 [{'x': ip} for ip in ips])

            self.assertEqual(
                self.session.query(self.table.c.x).filter(
                    self.table.c.x.notin_(['42e::1', '42e::3'])).all(), [
                    (IPv6Address('42e::2'),),
                    (IPv6Address('f42e::ffff'),),
                ])
            self.assertEqual(
                self.session.query(self.table.c.x).filter(
                    ~self.table.c.x.in_(['42e::1', '42e::3'])).all(), [
                    (IPv6Address('42e::2'),),
                    (IPv6Address('f42e::ffff'),),
                ])

    def test_select_not_in_list_network(self):
        ips = [
            IPv6Address('1234::1'),
            IPv6Address('42e::2'),
            IPv6Address('beef::3'),
            IPv6Address('f42e::ffff')
        ]

        with self.create_table(self.table):
            self.session.execute(self.table.insert(),
                                 [{'x': ip} for ip in ips])

            self.assertEqual(
                self.session.query(self.table.c.x).filter(
                    self.table.c.x.notin_(['42e::/64', 'beef::/64'])).all(), [
                    (IPv6Address('1234::1'),),
                    (IPv6Address('f42e::ffff'),)
                ])
            self.assertEqual(
                self.session.query(self.table.c.x).filter(
                    ~self.table.c.x.in_(['42e::/64', 'beef::/64'])).all(), [
                    (IPv6Address('1234::1'),),
                    (IPv6Address('f42e::ffff'),)
                ])

    def test_select_not_in_list_network_address(self):
        ips = [
            IPv6Address('1234::1'),
            IPv6Address('42e::2'),
            IPv6Address('beef::3'),
            IPv6Address('f42e::ffff')
        ]

        with self.create_table(self.table):
            self.session.execute(self.table.insert(),
                                 [{'x': ip} for ip in ips])

            self.assertEqual(
                self.session.query(self.table.c.x).filter(
                    self.table.c.x.notin_(['42e::/64', 'beef::3'])).all(), [
                    (IPv6Address('1234::1'),),
                    (IPv6Address('f42e::ffff'),)
                ])
            self.assertEqual(
                self.session.query(self.table.c.x).filter(
                    ~self.table.c.x.in_(['42e::/64', 'beef::3'])).all(), [
                    (IPv6Address('1234::1'),),
                    (IPv6Address('f42e::ffff'),)
                ])

    def test_select_not_in_list_empty(self):
        ips = [
            IPv6Address('1234::1'),
            IPv6Address('42e::2'),
            IPv6Address('beef::3'),
            IPv6Address('f42e::ffff')
        ]

        with self.create_table(self.table):
            self.session.execute(self.table.insert(),
                                 [{'x': ip} for ip in ips])

            self.assertEqual(
                self.session.query(self.table.c.x).filter(
                    self.table.c.x.notin_([])).all(), [
                    (IPv6Address('1234::1'),),
                    (IPv6Address('42e::2'),),
                    (IPv6Address('beef::3'),),
                    (IPv6Address('f42e::ffff'),)
                ])
            self.assertEqual(
                self.session.query(self.table.c.x).filter(
                    ~self.table.c.x.in_([])).all(), [
                    (IPv6Address('1234::1'),),
                    (IPv6Address('42e::2'),),
                    (IPv6Address('beef::3'),),
                    (IPv6Address('f42e::ffff'),)
                ])
