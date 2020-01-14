from sqlalchemy import (
    Column,
    exc,
    func,
    literal,
    select,
)
from sqlalchemy import text
from sqlalchemy import tuple_

from clickhouse_sqlalchemy import types, Table
from clickhouse_sqlalchemy.ext.clauses import Lambda
from tests.testcase import BaseAbstractTestCase, HttpSessionTestCase, NativeSessionTestCase


class SelectTestCase(BaseAbstractTestCase):

    def create_table(self, *columns):
        metadata = self.metadata()

        return Table(
            't1', metadata,
            Column('x', types.Int32, primary_key=True),
            *columns
        )

    def test_select(self):
        table = self.create_table()

        query = self.session.query(table.c.x)\
            .filter(table.c.x.in_([1, 2]))\
            .having(func.count() > 0)\
            .order_by(table.c.x.desc())
        self.assertEqual(
            self.compile(query),
            'SELECT x AS t1_x '
            'FROM t1 '
            'WHERE x IN (%(x_1)s, %(x_2)s) '
            'HAVING count(*) > %(count_1)s '
            'ORDER BY x DESC'
        )

    def test_very_simple_select(self):
        """
        A non-CH specific select statement should work too.
        """
        query = select([literal(1).label('col1')])
        self.assertEqual(
            self.compile(query),
            'SELECT %(param_1)s AS col1'
        )

    def test_group_by_query(self):
        table = self.create_table()

        query = self.session.query(table.c.x).group_by(table.c.x)
        self.assertEqual(
            self.compile(query),
            'SELECT x AS t1_x FROM t1 GROUP BY x'
        )

        query = self.session.query(table.c.x).group_by(table.c.x).with_totals()
        self.assertEqual(
            self.compile(query),
            'SELECT x AS t1_x FROM t1 GROUP BY x WITH TOTALS'
        )

        with self.assertRaises(exc.InvalidRequestError) as ex:
            self.session.query(table.c.x).with_totals()

        self.assertIn('with_totals', str(ex.exception))

    def test_array_join(self):
        table = self.create_table(
            Column('nested.array_column', types.Array(types.Int8)),
            Column('nested.another_array_column', types.Array(types.Int8))
        )
        first_label = table.c['nested.array_column'].label('from_array')
        second_not_label = table.c['nested.another_array_column']
        query = self.session.query(first_label, second_not_label)\
            .array_join(first_label, second_not_label)
        self.assertEqual(
            self.compile(query),
            'SELECT '
            '"nested.array_column" AS from_array, '
            '"nested.another_array_column" '
            'AS "t1_nested.another_array_column" '
            'FROM t1 '
            'ARRAY JOIN "nested.array_column" AS from_array, '
            '"nested.another_array_column"'
        )

    def test_sample(self):
        table = self.create_table()

        query = self.session.query(table.c.x).sample(0.1).group_by(table.c.x)
        self.assertEqual(
            self.compile(query),
            'SELECT x AS t1_x FROM t1 SAMPLE %(param_1)s GROUP BY x'
        )
        self.assertEqual(
            self.compile(query, literal_binds=True),
            'SELECT x AS t1_x FROM t1 SAMPLE 0.1 GROUP BY x'
        )

    def test_lambda_functions(self):
        query = self.session.query(
            func.arrayFilter(
                Lambda(lambda x: x.like('%World%')),
                literal(['Hello', 'abc World'], types.Array(types.String))
            ).label('test')
        )
        self.assertEqual(
            self.compile(query, literal_binds=True),
            "SELECT arrayFilter("
            "x -> x LIKE '%%World%%', ['Hello', 'abc World']"
            ") AS test"
        )


class SelectHttpTestCase(SelectTestCase, HttpSessionTestCase):
    """ ... """


class SelectNativeTestCase(SelectTestCase, NativeSessionTestCase):
    """ ... """


class JoinTestCase(BaseAbstractTestCase):
    def create_tables(self, num):
        metadata = self.metadata()

        return [Table(
            't{}'.format(i), metadata,
            Column('x', types.Int32, primary_key=True),
            Column('y', types.Int32, primary_key=True),
        ) for i in range(num)]

    def test_joins(self):
        t1, t2 = self.create_tables(2)

        query = self.session.query(t1.c.x, t2.c.x) \
            .join(
            t2,
            t1.c.x == t1.c.y,
            strictness='any')

        self.assertEqual(
            self.compile(query),
            "SELECT x AS t0_x, x AS t1_x FROM t0 "
            "ANY INNER JOIN t1 ON x = y"
        )

        query = self.session.query(t1.c.x, t2.c.x) \
            .join(
            t2,
            t1.c.x == t1.c.y,
            type='inner',
            strictness='any')

        self.assertEqual(
            self.compile(query),
            "SELECT x AS t0_x, x AS t1_x FROM t0 "
            "ANY INNER JOIN t1 ON x = y"
        )

        query = self.session.query(t1.c.x, t2.c.x) \
            .join(
            t2,
            tuple_(t1.c.x, t1.c.y),
            type='inner',
            strictness='all'
        )

        self.assertEqual(
            self.compile(query),
            "SELECT x AS t0_x, x AS t1_x FROM t0 "
            "ALL INNER JOIN t1 USING x, y"
        )

        query = self.session.query(t1.c.x, t2.c.x) \
            .join(t2,
                  tuple_(t1.c.x, t1.c.y),
                  type='inner',
                  strictness='all',
                  distribution='global')

        self.assertEqual(
            self.compile(query),
            "SELECT x AS t0_x, x AS t1_x FROM t0 "
            "GLOBAL ALL INNER JOIN t1 USING x, y"
        )

        query = self.session.query(t1.c.x, t2.c.x) \
            .outerjoin(
            t2,
            tuple_(t1.c.x, t1.c.y),
            type='left outer',
            strictness='all',
            distribution='global'
        )

        self.assertEqual(
            self.compile(query),
            "SELECT x AS t0_x, x AS t1_x FROM t0 "
            "GLOBAL ALL LEFT OUTER JOIN t1 USING x, y"
        )

        query = self.session.query(t1.c.x, t2.c.x) \
            .outerjoin(
            t2,
            tuple_(t1.c.x, t1.c.y),
            type='LEFT OUTER',
            strictness='ALL',
            distribution='GLOBAL'
        )

        self.assertEqual(
            self.compile(query),
            "SELECT x AS t0_x, x AS t1_x FROM t0 "
            "GLOBAL ALL LEFT OUTER JOIN t1 USING x, y"
        )

        query = self.session.query(t1.c.x, t2.c.x) \
            .outerjoin(t2,
                       tuple_(t1.c.x, t1.c.y),
                       strictness='ALL',
                       type='FULL OUTER')

        self.assertEqual(
            self.compile(query),
            "SELECT x AS t0_x, x AS t1_x FROM t0 "
            "ALL FULL OUTER JOIN t1 USING x, y"
        )


class JoinHttpTestCase(JoinTestCase, HttpSessionTestCase):
    """ Join over a HTTP session """


class JoinNativeTestCase(JoinTestCase, NativeSessionTestCase):
    """ Join over a native protocol session """


class YieldTest(NativeSessionTestCase):
    def test_yield_per_and_execution_options(self):
        numbers = Table(
            'numbers', self.metadata(),
            Column('number', types.UInt64, primary_key=True),
        )

        query = self.session.query(numbers.c.number).limit(100).yield_per(15)
        query = query.execution_options(foo='bar')
        self.assertIsNotNone(query._yield_per)
        self.assertEqual(
            query._execution_options,
            {'stream_results': True, 'foo': 'bar', 'max_row_buffer': 15}
        )

    def test_basic(self):
        numbers = Table(
            'numbers', self.metadata(),
            Column('number', types.UInt64, primary_key=True),
        )

        q = iter(
            self.session.query(numbers.c.number)
            .yield_per(1)
            .from_statement(text('SELECT * FROM system.numbers LIMIT 3'))
        )

        ret = []
        ret.append(next(q))
        ret.append(next(q))
        ret.append(next(q))
        try:
            next(q)
            assert False
        except StopIteration:
            pass

        self.assertEqual(ret, [(0, ), (1, ), (2, )])
