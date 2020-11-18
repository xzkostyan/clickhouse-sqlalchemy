from sqlalchemy import Column, and_
from sqlalchemy.exc import CompileError
from sqlalchemy.sql import expression

from clickhouse_sqlalchemy import types, select, Table, engines
from tests.testcase import BaseTestCase


class SelectTestCase(BaseTestCase):
    def _make_table(self, table_name=None, *columns):
        metadata = self.metadata()
        columns = (
            (columns or (Column('x', types.Int32, primary_key=True), )) +
            (engines.Memory(), )
        )

        return Table(
            table_name or 't1',
            metadata,
            *columns
        )

    def test_group_by_with_totals(self):
        table = self._make_table()

        query = select([table.c.x]).group_by(table.c.x)
        self.assertEqual(
            self.compile(query),
            'SELECT t1.x FROM t1 GROUP BY t1.x'
        )

        query = select([table.c.x]).group_by(table.c.x).with_totals()
        self.assertEqual(
            self.compile(query),
            'SELECT t1.x FROM t1 GROUP BY t1.x WITH TOTALS'
        )

    def test_sample(self):
        table = self._make_table()

        query = select([table.c.x]).sample(0.1).group_by(table.c.x)
        self.assertEqual(
            self.compile(query),
            'SELECT t1.x FROM t1 SAMPLE %(param_1)s GROUP BY t1.x'
        )

        self.assertEqual(
            self.compile(query, literal_binds=True),
            'SELECT t1.x FROM t1 SAMPLE 0.1 GROUP BY t1.x'
        )

    def test_final(self):
        table = self._make_table()

        query = select([table.c.x]).final().group_by(table.c.x)
        self.assertEqual(
            self.compile(query),
            'SELECT t1.x FROM t1 FINAL GROUP BY t1.x'
        )

    def test_limit_by(self):
        table = self._make_table()

        query = select([table.c.x]).order_by(table.c.x)\
            .limit_by([table.c.x], limit=1)
        self.assertEqual(
            self.compile(query),
            'SELECT t1.x FROM t1 ORDER BY t1.x LIMIT %(param_1)s BY t1.x'
        )
        self.assertEqual(
            self.compile(query, literal_binds=True),
            'SELECT t1.x FROM t1 ORDER BY t1.x LIMIT 1 BY t1.x'
        )

    def test_limit_by_with_offset(self):
        table = self._make_table()

        query = select([table.c.x]).order_by(table.c.x)\
            .limit_by([table.c.x], offset=1, limit=2)
        self.assertEqual(
            self.compile(query),
            'SELECT t1.x FROM t1 ORDER BY t1.x '
            'LIMIT %(param_1)s, %(param_2)s BY t1.x'
        )
        self.assertEqual(
            self.compile(query, literal_binds=True),
            'SELECT t1.x FROM t1 ORDER BY t1.x LIMIT 1, 2 BY t1.x'
        )

    def test_nested_type(self):
        table = self._make_table(
            't1',
            Column('x', types.Int32, primary_key=True),
            Column('parent', types.Nested(
                Column('child1', types.Int32),
                Column('child2', types.String),
            ))
        )

        query = select(
            [table.c.parent.child1]
        ).where(
            and_(
                table.c.parent.child1 == [1, 2],
                table.c.parent.child2 == ['foo', 'bar']
            )
        )
        self.assertEqual(
            self.compile(query, literal_binds=True),
            'SELECT t1.parent.child1 FROM t1 '
            'WHERE t1.parent.child1 = [1, 2] '
            'AND t1.parent.child2 = [\'foo\', \'bar\']'
        )

    def test_nested_array_join(self):
        table = self._make_table(
            't1',
            Column('x', types.Int32, primary_key=True),
            Column('parent', types.Nested(
                Column('child1', types.Int32),
                Column('child2', types.String),
            ))
        )
        query = select(
            [
                table.c.parent.child1,
                table.c.parent.child2,
            ]
        ).array_join(
            table.c.parent
        )
        self.assertEqual(
            self.compile(query),
            'SELECT t1.parent.child1, t1.parent.child2 '
            'FROM t1 '
            'ARRAY JOIN t1.parent'
        )

        query = select(
            [
                table.c.parent.child1,
                table.c.parent.child2,
            ]
        ).array_join(
            table.c.parent.child1,
        )
        self.assertEqual(
            self.compile(query),
            'SELECT t1.parent.child1, t1.parent.child2 '
            'FROM t1 '
            'ARRAY JOIN t1.parent.child1'
        )

        query = select(
            [
                table.c.parent.child1,
                table.c.parent.child2,
            ]
        ).array_join(
            table.c.parent.child1,
            table.c.parent.child2,
        )
        self.assertEqual(
            self.compile(query),
            'SELECT t1.parent.child1, t1.parent.child2 '
            'FROM t1 '
            'ARRAY JOIN t1.parent.child1, t1.parent.child2'
        )

        parent_labeled = table.c.parent.label('p')

        query = select(
            [
                table.c.parent.child1,
                table.c.parent.child2,
            ]
        ).array_join(
            parent_labeled
        )
        self.assertEqual(
            self.compile(query),
            'SELECT t1.parent.child1, t1.parent.child2 '
            'FROM t1 '
            'ARRAY JOIN t1.parent AS p'
        )

        query = select(
            [
                parent_labeled.child1,
                parent_labeled.child2,
                table.c.parent.child1,
            ]
        ).array_join(
            parent_labeled
        )
        self.assertEqual(
            self.compile(query),
            'SELECT p.child1, p.child2, t1.parent.child1 '
            'FROM t1 '
            'ARRAY JOIN t1.parent AS p'
        )

    def test_join(self):
        table_1 = self._make_table(
            'table_1', Column('x', types.UInt32, primary_key=True)
        )
        table_2 = self._make_table(
            'table_2', Column('y', types.UInt32, primary_key=True)
        )

        def make_statement(type=None,
                           strictness=None,
                           distribution=None,
                           full=False,
                           isouter=False):
            join = table_1.join(
                table_2,
                table_2.c.y == table_1.c.x,
                isouter=isouter,
                full=full,
                type=type,
                strictness=strictness,
                distribution=distribution
            )
            return select([table_1.c.x]).select_from(join)

        self.assertEqual(
            self.compile(make_statement(
                type='INNER'
            )),
            'SELECT table_1.x FROM table_1 '
            'INNER JOIN table_2 ON table_2.y = table_1.x'
        )

        self.assertEqual(
            self.compile(make_statement(
                type='INNER',
                strictness='all'
            )),
            'SELECT table_1.x FROM table_1 '
            'ALL INNER JOIN table_2 ON table_2.y = table_1.x'
        )

        self.assertEqual(
            self.compile(make_statement(
                type='INNER',
                strictness='any'
            )),
            'SELECT table_1.x FROM table_1 '
            'ANY INNER JOIN table_2 ON table_2.y = table_1.x'
        )

        self.assertEqual(
            self.compile(make_statement(
                type='INNER',
                distribution='global'
            )),
            'SELECT table_1.x FROM table_1 '
            'GLOBAL INNER JOIN table_2 ON table_2.y = table_1.x'
        )

        self.assertEqual(
            self.compile(make_statement(
                type='INNER',
                distribution='global',
                strictness='any'
            )),
            'SELECT table_1.x FROM table_1 '
            'GLOBAL ANY INNER JOIN table_2 ON table_2.y = table_1.x'
        )

        self.assertEqual(
            self.compile(make_statement(
                type='INNER',
                distribution='global',
                strictness='all'
            )),
            'SELECT table_1.x FROM table_1 '
            'GLOBAL ALL INNER JOIN table_2 ON table_2.y = table_1.x'
        )

        self.assertEqual(
            self.compile(make_statement(
                type='LEFT OUTER',
                distribution='global',
                strictness='all')),
            'SELECT table_1.x FROM table_1 '
            'GLOBAL ALL LEFT OUTER JOIN table_2 ON table_2.y = table_1.x'
        )

        self.assertEqual(
            self.compile(make_statement(
                type='RIGHT OUTER',
                distribution='global',
                strictness='all')),
            'SELECT table_1.x FROM table_1 '
            'GLOBAL ALL RIGHT OUTER JOIN table_2 ON table_2.y = table_1.x'
        )

        self.assertEqual(
            self.compile(make_statement(
                type='CROSS',
                distribution='global',
                strictness='all')),
            'SELECT table_1.x FROM table_1 '
            'GLOBAL ALL CROSS JOIN table_2 ON table_2.y = table_1.x'
        )

        self.assertEqual(
            self.compile(make_statement(type='FULL OUTER')),
            'SELECT table_1.x FROM table_1 '
            'FULL OUTER JOIN table_2 ON table_2.y = table_1.x'
        )

        self.assertEqual(
            self.compile(make_statement(isouter=False,
                                        full=False)),
            'SELECT table_1.x FROM table_1 '
            'INNER JOIN table_2 ON table_2.y = table_1.x'
        )

        self.assertEqual(
            self.compile(make_statement(isouter=True,
                                        full=False)),
            'SELECT table_1.x FROM table_1 '
            'LEFT OUTER JOIN table_2 ON table_2.y = table_1.x'
        )

        self.assertEqual(
            self.compile(make_statement(isouter=True,
                                        full=True)),
            'SELECT table_1.x FROM table_1 '
            'FULL LEFT OUTER JOIN table_2 ON table_2.y = table_1.x'
        )

        self.assertEqual(
            self.compile(make_statement(isouter=False,
                                        full=True)),
            'SELECT table_1.x FROM table_1 '
            'FULL INNER JOIN table_2 ON table_2.y = table_1.x'
        )

        self.assertEqual(
            self.compile(make_statement(
                isouter=False, full=False,
                type='INNER'
            )),
            'SELECT table_1.x FROM table_1 '
            'INNER JOIN table_2 ON table_2.y = table_1.x'
        )

        self.assertEqual(
            self.compile(make_statement(
                isouter=False, full=False,
                type='LEFT OUTER'
            )),
            'SELECT table_1.x FROM table_1 '
            'LEFT OUTER JOIN table_2 ON table_2.y = table_1.x'
        )

        self.assertEqual(
            self.compile(make_statement(
                isouter=False, full=False,
                type='LEFT', strictness='ALL'
            )),
            'SELECT table_1.x FROM table_1 '
            'ALL LEFT JOIN table_2 ON table_2.y = table_1.x'
        )

        self.assertEqual(
            self.compile(make_statement(
                isouter=False, full=False,
                type='LEFT', strictness='ANY'
            )),
            'SELECT table_1.x FROM table_1 '
            'ANY LEFT JOIN table_2 ON table_2.y = table_1.x'
        )

        self.assertEqual(
            self.compile(make_statement(
                isouter=False, full=False,
                type='LEFT', strictness='ANY', distribution='GLOBAL'
            )),
            'SELECT table_1.x FROM table_1 '
            'GLOBAL ANY LEFT JOIN table_2 ON table_2.y = table_1.x'
        )

        self.assertRaises(
            CompileError,
            lambda: self.compile(make_statement(
                isouter=True, full=False,
                type='INNER', strictness='ANY', distribution='GLOBAL'
            )),
        )

        self.assertEqual(
            self.compile(make_statement(
                isouter=True, full=True,
                type='LEFT OUTER', strictness='ANY', distribution='GLOBAL'
            )),
            'SELECT table_1.x FROM table_1 GLOBAL '
            'ANY FULL LEFT OUTER JOIN table_2 ON table_2.y = table_1.x'
        )

        self.assertEqual(
            self.compile(make_statement(
                isouter=True, full=False,
                type='CROSS',
            )),
            'SELECT table_1.x FROM table_1 '
            'CROSS JOIN table_2 ON table_2.y = table_1.x'
        )

    def test_join_same_column_name(self):
        table_1 = self._make_table(
            'table_1', Column('x', types.UInt32, primary_key=True)
        )
        table_2 = self._make_table(
            'table_2', Column('x', types.UInt32, primary_key=True)
        )

        join = table_1.join(table_2, table_1.c.x == table_2.c.x, type='INNER')

        self.assertEqual(
            self.compile(select([table_1.c.x]).select_from(join)),
            'SELECT table_1.x FROM table_1 '
            'INNER JOIN table_2 ON table_1.x = table_2.x'
        )

    def test_sql_expression_join(self):
        table_1 = self._make_table(
            'table_1', Column('x', types.UInt32, primary_key=True)
        )
        table_2 = self._make_table(
            'table_2', Column('x', types.UInt32, primary_key=True)
        )

        join = expression.join(
            table_1, table_2, onclause=table_1.c.x == table_2.c.x,
            isouter=False
        )

        self.assertEqual(
            self.compile(join),
            'table_1 INNER JOIN table_2 ON table_1.x = table_2.x'
        )
