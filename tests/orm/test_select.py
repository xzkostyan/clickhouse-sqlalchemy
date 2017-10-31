from sqlalchemy import Column, exc, func, literal
from sqlalchemy import tuple_

from src import types, Table
from src.ext.clauses import Lambda
from tests.session import session
from tests.testcase import BaseTestCase


class SelectTestCase(BaseTestCase):
    def create_table(self):
        metadata = self.metadata()

        return Table(
            't1', metadata,
            Column('x', types.Int32, primary_key=True)
        )

    def test_select(self):
        table = self.create_table()

        query = session.query(table.c.x).filter(table.c.x.in_([1, 2]))
        self.assertEqual(
            self.compile(query),
            'SELECT x AS t1_x FROM t1 WHERE x IN (%(x_1)s, %(x_2)s)'
        )

    def test_group_by_query(self):
        table = self.create_table()

        query = session.query(table.c.x).group_by(table.c.x)
        self.assertEqual(
            self.compile(query),
            'SELECT x AS t1_x FROM t1 GROUP BY x'
        )

        query = session.query(table.c.x).group_by(table.c.x).with_totals()
        self.assertEqual(
            self.compile(query),
            'SELECT x AS t1_x FROM t1 GROUP BY x WITH TOTALS'
        )

        with self.assertRaises(exc.InvalidRequestError) as ex:
            session.query(table.c.x).with_totals()

        self.assertIn('with_totals', str(ex.exception))

    def test_sample(self):
        table = self.create_table()

        query = session.query(table.c.x).sample(0.1).group_by(table.c.x)
        self.assertEqual(
            self.compile(query),
            'SELECT x AS t1_x FROM t1 SAMPLE %(param_1)s GROUP BY x'
        )

        self.assertEqual(
            self.compile(query, literal_binds=True),
            'SELECT x AS t1_x FROM t1 SAMPLE 0.1 GROUP BY x'
        )

    def test_lambda_functions(self):
        query = session.query(
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


class JoinTestCase(BaseTestCase):
    def create_tables(self, num):
        metadata = self.metadata()

        return [Table(
            't{}'.format(i), metadata,
            Column('x', types.Int32, primary_key=True),
            Column('y', types.Int32, primary_key=True),
        ) for i in range(num)]

    def test_unsupported_expressoin(self):
        t1, t2 = self.create_tables(2)

        query = session.query(t1.c.x).join(t2, literal(True), any=True)
        with self.assertRaises(exc.CompileError) as ex:
            self.compile(query)

        self.assertIn('Only tuple elements are supported', str(ex.exception))

    def test_joins(self):
        t1, t2 = self.create_tables(2)

        query = session.query(t1.c.x, t2.c.x) \
            .join(t2, tuple_(t1.c.x, t1.c.y), any=True)

        self.assertEqual(
            self.compile(query),
            "SELECT x AS t0_x, x AS t1_x FROM t0 "
            "ANY INNER JOIN t1 USING x, y"
        )

        query = session.query(t1.c.x, t2.c.x) \
            .join(t2, tuple_(t1.c.x, t1.c.y), all=True)

        self.assertEqual(
            self.compile(query),
            "SELECT x AS t0_x, x AS t1_x FROM t0 "
            "ALL INNER JOIN t1 USING x, y"
        )

        query = session.query(t1.c.x, t2.c.x) \
            .join(t2, tuple_(t1.c.x, t1.c.y), all=True, global_=True)

        self.assertEqual(
            self.compile(query),
            "SELECT x AS t0_x, x AS t1_x FROM t0 "
            "GLOBAL ALL INNER JOIN t1 USING x, y"
        )

        query = session.query(t1.c.x, t2.c.x) \
            .outerjoin(t2, tuple_(t1.c.x, t1.c.y), all=True, global_=True)

        self.assertEqual(
            self.compile(query),
            "SELECT x AS t0_x, x AS t1_x FROM t0 "
            "GLOBAL ALL LEFT OUTER JOIN t1 USING x, y"
        )

        query = session.query(t1.c.x, t2.c.x) \
            .outerjoin(t2, tuple_(t1.c.x, t1.c.y), all=True, global_=True)

        self.assertEqual(
            self.compile(query),
            "SELECT x AS t0_x, x AS t1_x FROM t0 "
            "GLOBAL ALL LEFT OUTER JOIN t1 USING x, y"
        )
