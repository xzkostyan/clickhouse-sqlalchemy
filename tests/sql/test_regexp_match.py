from sqlalchemy import Column, not_
from clickhouse_sqlalchemy import types, Table

from tests.testcase import BaseTestCase


class RegexpMatch(BaseTestCase):
    table = Table(
        't1',
        BaseTestCase.metadata(),
        Column('x', types.Int32, primary_key=True),
        Column('y', types.String)
    )

    def test_regex_match(self):
        query = (
            self.session.query(self.table.c.x)
            .where(self.table.c.y.regexp_match('^s.*'))
        )

        self.assertEqual(
            self.compile(query, literal_binds=True),
            "SELECT t1.x AS t1_x FROM t1 WHERE match(t1.y, '^s.*')"
        )

    def test_not_regex_match(self):
        query = (
            self.session.query(self.table.c.x)
            .where(not_(self.table.c.y.regexp_match('^s.*')))
        )

        self.assertEqual(
            self.compile(query, literal_binds=True),
            "SELECT t1.x AS t1_x FROM t1 WHERE NOT match(t1.y, '^s.*')"
        )
