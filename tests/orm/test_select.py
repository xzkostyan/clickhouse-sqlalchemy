from datetime import date, datetime as dt
from sqlalchemy import Column, exc, func, literal, select, text, tuple_

from clickhouse_sqlalchemy import types, Table, engines
from clickhouse_sqlalchemy.ext.clauses import Lambda
from tests.testcase import NativeSessionTestCase, CompilationTestCase


class SelectTestCase(CompilationTestCase):

    def _make_table(self, *columns):
        columns = columns + (engines.Memory(), )
        return Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            *columns
        )

    def test_select(self):
        table = self._make_table()

        query = self.session.query(table.c.x)\
            .filter(table.c.x.in_([1, 2]))\
            .having(func.count() > 0)\
            .order_by(table.c.x.desc())
        self.assertEqual(
            self.compile(query),
            'SELECT t1.x AS t1_x '
            'FROM t1 '
            'WHERE t1.x IN (%(x_1)s, %(x_2)s) '
            'HAVING count(*) > %(count_1)s '
            'ORDER BY t1.x DESC'
        )

    def test_very_simple_select(self):
        # A non-CH specific select statement should work too.

        query = select([literal(1).label('col1')])
        self.assertEqual(
            self.compile(query),
            'SELECT %(param_1)s AS col1'
        )

    def test_group_by_query(self):
        table = self._make_table()

        query = self.session.query(table.c.x).group_by(table.c.x)
        self.assertEqual(
            self.compile(query),
            'SELECT t1.x AS t1_x FROM t1 GROUP BY t1.x'
        )

        query = self.session.query(table.c.x).group_by(table.c.x).with_totals()
        self.assertEqual(
            self.compile(query),
            'SELECT t1.x AS t1_x FROM t1 GROUP BY t1.x WITH TOTALS'
        )

        with self.assertRaises(exc.InvalidRequestError) as ex:
            self.session.query(table.c.x).with_totals()

        self.assertIn('with_totals', str(ex.exception))

    def test_array_join(self):
        table = self._make_table(
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
            't1."nested.array_column" AS from_array, '
            't1."nested.another_array_column" '
            'AS "t1_nested.another_array_column" '
            'FROM t1 '
            'ARRAY JOIN t1."nested.array_column" AS from_array, '
            't1."nested.another_array_column"'
        )

    def test_sample(self):
        table = self._make_table()

        query = self.session.query(table.c.x).sample(0.1).group_by(table.c.x)
        self.assertEqual(
            self.compile(query),
            'SELECT t1.x AS t1_x FROM t1 SAMPLE %(param_1)s GROUP BY t1.x'
        )
        self.assertEqual(
            self.compile(query, literal_binds=True),
            'SELECT t1.x AS t1_x FROM t1 SAMPLE 0.1 GROUP BY t1.x'
        )

    def test_final(self):
        table = self._make_table()

        query = self.session.query(table.c.x).final().group_by(table.c.x)
        self.assertEqual(
            self.compile(query),
            'SELECT t1.x AS t1_x FROM t1 FINAL GROUP BY t1.x'
        )

    def test_limit_by(self):
        table = self._make_table()

        query = self.session.query(table.c.x).order_by(table.c.x)\
            .limit_by([table.c.x], limit=1)
        self.assertEqual(
            self.compile(query),
            'SELECT t1.x AS t1_x FROM t1 ORDER BY t1.x '
            'LIMIT %(param_1)s BY t1.x'
        )
        self.assertEqual(
            self.compile(query, literal_binds=True),
            'SELECT t1.x AS t1_x FROM t1 ORDER BY t1.x LIMIT 1 BY t1.x'
        )

    def test_limit_by_with_offset(self):
        table = self._make_table()

        query = self.session.query(table.c.x).order_by(table.c.x)\
            .limit_by([table.c.x], offset=1, limit=2)
        self.assertEqual(
            self.compile(query),
            'SELECT t1.x AS t1_x FROM t1 ORDER BY t1.x '
            'LIMIT %(param_1)s, %(param_2)s BY t1.x'
        )
        self.assertEqual(
            self.compile(query, literal_binds=True),
            'SELECT t1.x AS t1_x FROM t1 ORDER BY t1.x LIMIT 1, 2 BY t1.x'
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

    def test_with_fill(self):
        columns = [Column('y', types.Int32), Column('z', types.Int32)]
        table = self._make_table(*columns)

        query = self.session.query(table.c.x) \
            .order_by(WithFill(table.c.x))

        self.assertEqual(
            self.compile(query),
            'SELECT t1.x AS t1_x FROM t1 ORDER BY t1.x '
            'WITH FILL'
        )

    def test_with_fill_from(self):
        columns = [Column('y', types.Int32), Column('z', types.Int32)]
        table = self._make_table(*columns)

        query = self.session.query(table.c.x) \
            .order_by(WithFill(table.c.x, from_=literal(2)))

        self.assertEqual(
            self.compile(query),
            'SELECT t1.x AS t1_x FROM t1 ORDER BY t1.x '
            'WITH FILL FROM %(param_1)s'
        )

        self.assertEqual(
            self.compile(query, literal_binds=True),
            'SELECT t1.x AS t1_x FROM t1 ORDER BY t1.x '
            'WITH FILL FROM 2'
        )

    def test_with_fill_from_to(self):
        columns = [Column('y', types.Int32), Column('z', types.Int32)]
        table = self._make_table(*columns)

        query = self.session.query(table.c.x) \
            .order_by(WithFill(table.c.x, from_=literal(2), to=literal(5)))

        self.assertEqual(
            self.compile(query),
            'SELECT t1.x AS t1_x FROM t1 ORDER BY t1.x '
            'WITH FILL FROM %(param_1)s TO %(param_2)s'
        )

        self.assertEqual(
            self.compile(query, literal_binds=True),
            'SELECT t1.x AS t1_x FROM t1 ORDER BY t1.x '
            'WITH FILL FROM 2 TO 5'
        )

    def test_with_fill_from_to_step(self):
        columns = [Column('y', types.Int32), Column('z', types.Int32)]
        table = self._make_table(*columns)

        query = self.session.query(table.c.x) \
            .order_by(
            WithFill(
                table.c.x, from_=literal(1), to=literal(5), step=literal(1)
            )
        )

        self.assertEqual(
            self.compile(query),
            'SELECT t1.x AS t1_x FROM t1 ORDER BY t1.x '
            'WITH FILL FROM %(param_1)s TO %(param_2)s STEP %(param_3)s'
        )

        self.assertEqual(
            self.compile(query, literal_binds=True),
            'SELECT t1.x AS t1_x FROM t1 ORDER BY t1.x '
            'WITH FILL FROM 1 TO 5 STEP 1'
        )

    def test_with_fill_from_to_step_multiply(self):
        columns = [Column('y', types.Int32), Column('z', types.Int32)]
        table = self._make_table(*columns)

        query = self.session.query(table.c.x) \
            .order_by(
            WithFill(
                table.c.x, from_=literal(1), to=literal(5), step=literal(1)
            ),
            WithFill(
                table.c.z, from_=literal(2), to=literal(6), step=literal(1)
            )
        )

        self.assertEqual(
            self.compile(query),
            'SELECT t1.x AS t1_x FROM t1 ORDER BY t1.x '
            'WITH FILL '
            'FROM %(param_1)s TO %(param_2)s STEP %(param_3)s, t1.z '
            'WITH FILL '
            'FROM %(param_4)s TO %(param_5)s STEP %(param_6)s'
        )
        self.assertEqual(
            self.compile(query, literal_binds=True),
            'SELECT t1.x AS t1_x FROM t1 ORDER BY t1.x '
            'WITH FILL '
            'FROM 1 TO 5 STEP 1, '
            't1.z WITH FILL FROM 2 TO 6 STEP 1'
        )

    def test_with_fill_from_date(self):
        columns = [Column('y', types.Int32), Column('date', types.DateTime)]
        table = self._make_table(*columns)

        date_from = date(year=2025, month=1, day=1)
        query = self.session.query(table.c.x) \
            .order_by(
            WithFill(
                table.c.date,
                from_=func.toDateTime(
                    literal(date_from, type_=types.DateTime)
                )
            )
        )

        self.assertEqual(
            self.compile(query),
            "SELECT t1.x AS t1_x FROM t1 ORDER BY t1.date "
            "WITH FILL "
            "FROM toDateTime(%(param_1)s)"
        )

        self.assertEqual(
            self.compile(query, literal_binds=True),
            "SELECT t1.x AS t1_x FROM t1 ORDER BY t1.date "
            "WITH FILL "
            "FROM toDateTime('2025-01-01')"
        )
        dt_from = dt(year=2025, month=1, day=1, hour=1, minute=1, second=1)
        query = self.session.query(table.c.x) \
            .order_by(
            WithFill(
                table.c.date, from_=func.toDateTime(
                    literal(dt_from, type_=types.Date))
            )
        )

        self.assertEqual(
            self.compile(query),
            "SELECT t1.x AS t1_x FROM t1 ORDER BY t1.date "
            "WITH FILL "
            "FROM toDateTime(%(param_1)s)"
        )

        self.assertEqual(
            self.compile(query, literal_binds=True),
            "SELECT t1.x AS t1_x FROM t1 ORDER BY t1.date "
            "WITH FILL "
            "FROM toDateTime('2025-01-01 01:01:01')"
        )

    def test_with_fill_from_to_step_date(self):
        columns = [Column('y', types.Int32), Column('date', types.Date)]
        table = self._make_table(*columns)

        date_from = date(year=2025, month=1, day=1)
        date_to = date(year=2025, month=2, day=1)
        query = self.session.query(table.c.x) \
            .order_by(
            WithFill(
                table.c.date,
                from_=func.toDate(literal(date_from, type_=types.Date)),
                to=func.toDate(literal(date_to, type_=types.Date)),
                step=literal(1)
            )
        )

        self.assertEqual(
            self.compile(query),
            "SELECT t1.x AS t1_x FROM t1 ORDER BY t1.date "
            "WITH FILL "
            "FROM toDate(%(param_1)s) "
            "TO toDate(%(param_2)s) "
            "STEP %(param_3)s"
        )

        self.assertEqual(
            self.compile(query, literal_binds=True),
            "SELECT t1.x AS t1_x FROM t1 ORDER BY t1.date "
            "WITH FILL "
            "FROM toDate('2025-01-01') "
            "TO toDate('2025-02-01') "
            "STEP 1"
        )

    def test_with_fill_from_to_step_with_datetime(self):
        columns = [
            Column('y', types.Int32),
            Column('dt', types.DateTime)
        ]
        table = self._make_table(*columns)

        date_from = date(year=2025, month=1, day=1)
        date_to = date(year=2025, month=2, day=1)
        query = self.session.query(table.c.x) \
            .order_by(
            WithFill(
                table.c.dt,
                from_=func.toDateTime(literal(date_from, type_=types.Date)),
                to=func.toDateTime(literal(date_to, type_=types.Date)),
                step=literal(1)
            )
        )

        self.assertEqual(
            self.compile(query),
            "SELECT t1.x AS t1_x FROM t1 ORDER BY t1.dt "
            "WITH FILL "
            "FROM toDateTime(%(param_1)s) "
            "TO toDateTime(%(param_2)s) "
            "STEP %(param_3)s"
        )

        dt_from = dt(year=2025, month=1, day=1, hour=1, minute=1, second=1)
        dt_to = dt(year=2025, month=2, day=1)
        query = self.session.query(table.c.x) \
            .order_by(
            WithFill(
                table.c.dt,
                from_=func.toDateTime(
                    literal(dt_from, type_=types.Date)
                ),
                to=func.toDateTime(literal(dt_to, type_=types.Date)),
                step=literal(1)
            )
        )

        self.assertEqual(
            self.compile(query),
            "SELECT t1.x AS t1_x FROM t1 ORDER BY t1.dt "
            "WITH FILL "
            "FROM toDateTime(%(param_1)s) "
            "TO toDateTime(%(param_2)s) "
            "STEP %(param_3)s"
        )
        self.assertEqual(
            self.compile(query, literal_binds=True),
            "SELECT t1.x AS t1_x FROM t1 ORDER BY t1.dt "
            "WITH FILL "
            "FROM toDateTime('2025-01-01 01:01:01') "
            "TO toDateTime('2025-02-01 00:00:00') "
            "STEP 1"
        )

    def test_with_fill_from_to_step_with_datetime64(self):
        columns = [
            Column('y', types.Int32),
            Column('dt', types.DateTime64)
        ]
        table = self._make_table(*columns)

        date_from = date(year=2025, month=1, day=1)
        date_to = date(year=2025, month=2, day=1)
        query = self.session.query(table.c.x) \
            .order_by(
            WithFill(
                table.c.dt,
                from_=func.toDateTime64(
                    literal(
                        date_from,
                        type_=types.DateTime64
                    ), table.c.dt.type.precision
                ),
                to=func.toDateTime64(
                    literal(
                        date_to,
                        type_=types.DateTime64
                    ), table.c.dt.type.precision),
                step=literal(1)
            )
        )

        self.assertEqual(
            self.compile(query),
            "SELECT t1.x AS t1_x FROM t1 ORDER BY t1.dt "
            "WITH FILL FROM "
            "toDateTime64(%(param_1)s, %(toDateTime64_1)s)"
            " TO "
            "toDateTime64(%(param_2)s, %(toDateTime64_2)s)"
            " STEP %(param_3)s"
        )

        self.assertEqual(
            self.compile(query, literal_binds=True),
            "SELECT t1.x AS t1_x FROM t1 ORDER BY t1.dt "
            "WITH FILL "
            "FROM toDateTime64('2025-01-01', 3) "
            "TO toDateTime64('2025-02-01', 3) "
            "STEP 1"
        )


class JoinTestCase(CompilationTestCase):
    def test_joins(self):
        t1, t2 = [Table(
            't{}'.format(i), self.metadata(),
            Column('x', types.Int32, primary_key=True),
            Column('y', types.Int32, primary_key=True),
            engines.Memory()
        ) for i in range(1, 3)]

        query = self.session.query(t1.c.x, t2.c.x) \
            .join(
            t2,
            t1.c.x == t2.c.y,
            strictness='any')

        self.assertEqual(
            self.compile(query),
            "SELECT t1.x AS t1_x, t2.x AS t2_x FROM t1 "
            "ANY INNER JOIN t2 ON t1.x = t2.y"
        )

        query = self.session.query(t1.c.x, t2.c.x) \
            .join(
            t2,
            t1.c.x == t2.c.y,
            type='inner',
            strictness='any')

        self.assertEqual(
            self.compile(query),
            "SELECT t1.x AS t1_x, t2.x AS t2_x FROM t1 "
            "ANY INNER JOIN t2 ON t1.x = t2.y"
        )

        query = self.session.query(t1.c.x, t2.c.x) \
            .join(
            t2,
            tuple_(t1.c.x, t2.c.y),
            type='inner',
            strictness='all'
        )

        self.assertEqual(
            self.compile(query),
            "SELECT t1.x AS t1_x, t2.x AS t2_x FROM t1 "
            "ALL INNER JOIN t2 USING x, y"
        )

        query = self.session.query(t1.c.x, t2.c.x) \
            .join(t2,
                  tuple_(t1.c.x, t2.c.y),
                  type='inner',
                  strictness='all',
                  distribution='global')

        self.assertEqual(
            self.compile(query),
            "SELECT t1.x AS t1_x, t2.x AS t2_x FROM t1 "
            "GLOBAL ALL INNER JOIN t2 USING x, y"
        )

        query = self.session.query(t1.c.x, t2.c.x) \
            .outerjoin(
            t2,
            tuple_(t1.c.x, t2.c.y),
            type='left outer',
            strictness='all',
            distribution='global'
        )

        self.assertEqual(
            self.compile(query),
            "SELECT t1.x AS t1_x, t2.x AS t2_x FROM t1 "
            "GLOBAL ALL LEFT OUTER JOIN t2 USING x, y"
        )

        query = self.session.query(t1.c.x, t2.c.x) \
            .outerjoin(
            t2,
            tuple_(t1.c.x, t2.c.y),
            type='LEFT OUTER',
            strictness='ALL',
            distribution='GLOBAL'
        )

        self.assertEqual(
            self.compile(query),
            "SELECT t1.x AS t1_x, t2.x AS t2_x FROM t1 "
            "GLOBAL ALL LEFT OUTER JOIN t2 USING x, y"
        )

        query = self.session.query(t1.c.x, t2.c.x) \
            .outerjoin(t2,
                       tuple_(t1.c.x, t2.c.y),
                       strictness='ALL',
                       type='FULL OUTER')

        self.assertEqual(
            self.compile(query),
            "SELECT t1.x AS t1_x, t2.x AS t2_x FROM t1 "
            "ALL FULL OUTER JOIN t2 USING x, y"
        )


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
            self.assertTrue(False)
        except StopIteration:
            pass

        self.assertEqual(ret, [(0, ), (1, ), (2, )])
