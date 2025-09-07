from datetime import date, datetime
from decimal import Decimal
from unittest.mock import patch

from responses import mock, matchers
from sqlalchemy import Column, func, create_engine, literal

from clickhouse_sqlalchemy import types, Table, make_session
from clickhouse_sqlalchemy.drivers.http.base import ClickHouseDialect_http
from tests.testcase import HttpSessionTestCase


class TransportCase(HttpSessionTestCase):
    @property
    def url(self):
        return 'http://{host}:{port}'.format(host=self.host, port=self.port)

    @mock.activate
    def test_parse_func_count(self):
        mock.add(
            mock.POST, self.url, status=200,
            body='count_1\nUInt64\n42\n'
        )

        table = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True)
        )

        rv = self.session.query(func.count()).select_from(table).scalar()
        self.assertEqual(rv, 42)

    @mock.activate
    def test_parse_int_types(self):
        types_ = [
            'Int8', 'UInt8', 'Int16', 'UInt16', 'Int32', 'UInt32', 'Int64',
            'UInt64'
        ]
        columns = [chr(i + ord('a')) for i in range(len(types_))]

        mock.add(
            mock.POST, self.url, status=200,
            body=(
                '\t'.join(columns) + '\n' +
                '\t'.join(types_) + '\n' +
                '\t'.join(['42'] * len(types_)) + '\n'
            )
        )

        table = Table(
            't1', self.metadata(),
            *[Column(col, types.Int) for col in columns]
        )

        rv = self.session.query(*table.c).first()
        self.assertEqual(rv, tuple([42] * len(columns)))

    @mock.activate
    def test_parse_float_types(self):
        types_ = ['Float32', 'Float64']
        columns = ['a', 'b']

        mock.add(
            mock.POST, self.url, status=200,
            body=(
                '\t'.join(columns) + '\n' +
                '\t'.join(types_) + '\n' +
                '\t'.join(['42'] * len(types_)) + '\n'
            )
        )

        table = Table(
            't1', self.metadata(),
            *[Column(col, types.Float) for col in columns]
        )

        rv = self.session.query(*table.c).first()
        self.assertEqual(rv, tuple([42.0] * len(columns)))

    # do not call that method
    @patch.object(ClickHouseDialect_http, '_get_server_version_info')
    @mock.activate
    def test_parse_date_types(self, patched_server_info):
        mock.add(
            mock.POST, self.url, status=200,
            body=(
                'a\n' +
                'Date\n' +
                '2012-10-25\n'
            )
        )

        table = Table(
            't1', self.metadata(),
            Column('a', types.Date)
        )

        rv = self.session.query(*table.c).first()
        self.assertEqual(rv, (date(2012, 10, 25), ))

    @patch.object(ClickHouseDialect_http, '_get_server_version_info')
    @mock.activate
    def test_parse_date_time_type(self, patched_server_info):
        mock.add(
            mock.POST, self.url, status=200,
            body=(
                'a\n' +
                'DateTime64(3)\n' +
                '2012-10-25 00:00:00.0\n'
            )
        )

        table = Table(
            't1', self.metadata(),
            Column('a', types.DateTime)
        )

        rv = self.session.query(*table.c).first()
        self.assertEqual(rv, (datetime(2012, 10, 25), ))

    @mock.activate
    def test_parse_nullable_type(self):
        mock.add(
            mock.POST, self.url, status=200,
            body=(
                'a\n' +
                'String\n' +
                '\\N\n' +
                '\\\\N\n' +
                '\n'
            )
        )

        table = Table(
            't1', self.metadata(),
            Column('a', types.String)
        )

        rv = self.session.query(*table.c).all()
        self.assertEqual(rv, [(None, ), ('\\N', ), ('', )])

    @mock.activate
    def test_parse_decimal(self):
        mock.add(
            mock.POST, self.url, status=200,
            body=(
                'a\n' +
                'Decimal(8,8)\n' +
                '1.1\n'
            )
        )

        table = Table(
            't1', self.metadata(),
            Column('a', types.Decimal)
        )

        rv = self.session.query(*table.c).all()
        self.assertEqual(rv, [(Decimal('1.1'), )])

    @mock.activate
    def test_parse_decimal_bits(self):
        mock.add(
            mock.POST, self.url, status=200,
            body=(
                'a\n' +
                'Decimal32(8)\n' +
                '1.1\n'
            )
        )

        table = Table(
            't1', self.metadata(),
            Column('a', types.Decimal)
        )

        rv = self.session.query(*table.c).all()
        self.assertEqual(rv, [(Decimal('1.1'), )])

    @mock.activate
    def test_parse_nullable_with_subtype(self):
        mock.add(
            mock.POST, self.url, status=200,
            body=(
                'a\n' +
                'Nullable(Float64)\n' +
                '\\N\n' +
                '1.1\n'
            )
        )

        table = Table(
            't1', self.metadata(),
            Column('a', types.Float)
        )

        rv = self.session.query(*table.c).all()
        self.assertEqual(rv, [(None, ), (1.1, )])

    @mock.activate
    def test_parse_nullable_nothing(self):
        mock.add(
            mock.POST, self.url, status=200,
            body=(
                'a\n' +
                'Nullable(Nothing)\n' +
                '\\N\n'
            )
        )

        table = Table(
            't1', self.metadata(),
            Column('a', types.Float)
        )

        rv = self.session.query(*table.c).all()
        self.assertEqual(rv, [(None, )])

    @mock.activate
    def test_auth_header(self):
        url = 'clickhouse+http://testuser:password@{host}:{port}'.format(
            host=self.host, port=self.port
        )
        http_engine = create_engine(url)
        http_session = make_session(http_engine)
        mock.add(
            mock.POST, self.url, status=200,
            body='version()\nString\n23.3.13.6\n',
            match=[
                matchers.header_matcher(
                    {"Authorization": "Basic dGVzdHVzZXI6cGFzc3dvcmQ="}
                ),
            ],
        )

        rv = http_session.query(literal(1)).scalar()
        self.assertEqual(rv, '23.3.13.6')

    @mock.activate
    def test_auth_header_without_auth(self):
        url = 'clickhouse+http://{host}:{port}'.format(
            host=self.host, port=self.port
        )
        http_engine = create_engine(url)
        http_session = make_session(http_engine)

        def match_no_auth_header(request):
            auth_headers = [
                name for name, _ in request.headers.items()
                if name == "Authorization"
            ]
            return (len(auth_headers) == 0, 'Authorization header is sent')
        mock.add(
            mock.POST, self.url, status=200,
            body='version()\nString\n23.3.13.6\n',
            match=[match_no_auth_header],
        )

        rv = http_session.query(literal(1)).scalar()
        self.assertEqual(rv, '23.3.13.6')
