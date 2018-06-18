from datetime import date

from responses import mock
from sqlalchemy import Column, func

from clickhouse_sqlalchemy import types, Table
from tests.session import session
from tests.testcase import BaseTestCase


class TransportCase(BaseTestCase):
    @mock.activate
    def test_parse_func_count(self):
        mock.add(
            mock.POST, 'http://localhost:8123', status=200,
            body='count_1\nUInt64\n42\n'
        )

        table = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True)
        )

        rv = session.query(func.count()).select_from(table).scalar()
        self.assertEqual(rv, 42)

    @mock.activate
    def test_parse_int_types(self):
        types_ = [
            'Int8', 'UInt8', 'Int16', 'UInt16', 'Int32', 'UInt32', 'Int64',
            'UInt64'
        ]
        columns = [chr(i + ord('a')) for i in range(len(types_))]

        mock.add(
            mock.POST, 'http://localhost:8123', status=200,
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

        rv = session.query(*table.c).first()
        self.assertEqual(rv, tuple([42] * len(columns)))

    @mock.activate
    def test_parse_float_types(self):
        types_ = ['Float32', 'Float64']
        columns = ['a', 'b']

        mock.add(
            mock.POST, 'http://localhost:8123', status=200,
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

        rv = session.query(*table.c).first()
        self.assertEqual(rv, tuple([42.0] * len(columns)))

    @mock.activate
    def test_parse_date_types(self):
        mock.add(
            mock.POST, 'http://localhost:8123', status=200,
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

        rv = session.query(*table.c).first()
        self.assertEqual(rv, (date(2012, 10, 25), ))

    @mock.activate
    def test_parse_nullable_type(self):
        mock.add(
            mock.POST, 'http://localhost:8123', status=200,
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

        rv = session.query(*table.c).all()
        self.assertEqual(rv, [(None, ), ('\\N', ), ('', )])
