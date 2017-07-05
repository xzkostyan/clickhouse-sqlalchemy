from six import text_type
from sqlalchemy import Column, func, literal, exc, case

from src import types
from src.schema import Table
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

    def test_select_count(self):
        table = self.create_table()

        self.assertEqual(
            self.compile(session.query(func.count(table.c.x))),
            'SELECT count(x) AS count_1 FROM t1'
        )

        self.assertEqual(
            self.compile(session.query(func.count()).select_from(table)),
            'SELECT count(*) AS count_1 FROM t1'
        )

    def test_limit(self):
        table = self.create_table()

        query = session.query(table.c.x).limit(10)
        self.assertEqual(
            self.compile(query, literal_binds=True),
            'SELECT x AS t1_x FROM t1  LIMIT 10'
        )

        query = session.query(table.c.x).limit(10).offset(5)
        self.assertEqual(
            self.compile(query, literal_binds=True),
            'SELECT x AS t1_x FROM t1  LIMIT 5, 10'
        )

        query = session.query(table.c.x).offset(5).limit(10)
        self.assertEqual(
            self.compile(query, literal_binds=True),
            'SELECT x AS t1_x FROM t1  LIMIT 5, 10'
        )

        with self.assertRaises(exc.CompileError) as ex:
            query = session.query(table.c.x).offset(5)
            self.compile(query, literal_binds=True)

        self.assertEqual(
            text_type(ex.exception),
            'OFFSET without LIMIT is not supported'
        )

    def test_case(self):
        with self.assertRaises(exc.CompileError) as ex:
            self.compile(case([(literal(1), 0)]))

        self.assertEqual(
            text_type(ex.exception),
            'ELSE clause is required in CASE'
        )

        expression = case([(literal(1), 0)], else_=1)
        self.assertEqual(
            self.compile(expression, literal_binds=True),
            'CASE WHEN 1 THEN 0 ELSE 1 END'
        )


class SelectEscapingTestCase(BaseTestCase):
    def compile(self, clause, **kwargs):
        return text_type(self._compile(clause, **kwargs))

    def test_select_escaping(self):
        self.assertEqual(
            self.compile(session.query(literal('\t')), literal_binds=True),
            "SELECT '\t' AS param_1"
        )


class FormatSectionTestCase(BaseTestCase):
    execution_ctx_cls = session.bind.dialect.execution_ctx_cls

    def _compile(self, clause, bind=session.bind, **kwargs):
        statement = super(FormatSectionTestCase, self)._compile(
            clause, bind=bind, **kwargs
        )

        context = self.execution_ctx_cls._init_compiled(
            bind.dialect, bind, bind, statement, []
        )
        context.pre_exec()

        return context.statement

    def test_select(self):
        metadata = self.metadata()

        bind = session.bind
        bind.cursor = lambda: None

        table = Table(
            't1', metadata,
            Column('x', types.Int32, primary_key=True)
        )

        statement = self.compile(session.query(table.c.x), bind=bind)
        self.assertEqual(
            statement,
            'SELECT x AS t1_x FROM t1 FORMAT TabSeparatedWithNamesAndTypes'
        )

    def test_insert_from_select(self):
        metadata = self.metadata()

        bind = session.bind
        bind.cursor = lambda: None

        t1 = Table(
            't1', metadata,
            Column('x', types.Int32, primary_key=True)
        )

        t2 = Table(
            't2', metadata,
            Column('x', types.Int32, primary_key=True)
        )

        query = t2.insert() \
            .from_select(['x'], session.query(t1.c.x).subquery())
        statement = self.compile(query, bind=bind)
        self.assertEqual(statement, 'INSERT INTO t2 (x) SELECT x FROM t1')

    # TODO: test escaping \t, etc.
