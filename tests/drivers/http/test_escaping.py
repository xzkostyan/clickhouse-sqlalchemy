from decimal import Decimal
from datetime import date
import uuid

from sqlalchemy import Column, literal

from clickhouse_sqlalchemy import types, engines, Table
from clickhouse_sqlalchemy.drivers.http.escaper import Escaper
from tests.testcase import HttpSessionTestCase


class EscapingTestCase(HttpSessionTestCase):
    def escaped_compile(self, clause, **kwargs):
        return str(self._compile(clause, **kwargs))

    def test_select_escaping(self):
        query = self.session.query(literal('\t'))
        self.assertEqual(
            self.escaped_compile(query, literal_binds=True),
            "SELECT '\t' AS anon_1"
        )

    def test_escaper(self):
        e = Escaper()
        self.assertEqual(e.escape([None]), '[NULL]')
        self.assertEqual(e.escape([[None]]), '[[NULL]]')
        self.assertEqual(e.escape([[123]]), '[[123]]')
        self.assertEqual(e.escape({'x': 'str'}), {'x': "'str'"})
        self.assertEqual(e.escape([Decimal('10')]), '[10.0]')
        self.assertEqual(e.escape([10.0]), '[10.0]')
        self.assertEqual(e.escape([date(2017, 1, 2)]), "['2017-01-02']")
        self.assertEqual(e.escape(dict(x=10, y=20)), {'x': 10, 'y': 20})
        self.assertEqual(
            e.escape([uuid.UUID("ef3e3d4b-c782-4993-83fc-894ff0aba8ff")]),
            '[ef3e3d4b-c782-4993-83fc-894ff0aba8ff]'
        )
        with self.assertRaises(Exception) as ex:
            e.escape([object()])

        self.assertIn('Unsupported object', str(ex.exception))

        with self.assertRaises(Exception) as ex:
            e.escape('str')

        self.assertIn('Unsupported param format', str(ex.exception))

    def test_escape_binary_mod(self):
        query = self.session.query(literal(1) % literal(2))
        self.assertEqual(
            self.compile(query, literal_binds=True),
            'SELECT 1 %% 2 AS anon_1'
        )

        table = Table(
            't', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            engines.Memory()
        )

        query = self.session.query(table.c.x % table.c.x)
        self.assertEqual(
            self.compile(query, literal_binds=True),
            'SELECT t.x %% t.x AS anon_1 FROM t'
        )
