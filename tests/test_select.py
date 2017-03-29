from sqlalchemy import Column, func, literal, exc

from src.base import ClickHouseExecutionContext
from src.schema import Table
from src import types
from session import session
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

        self.assertEqual(
            self.compile(session.query(table.c.x).filter(table.c.x.in_([1, 2]))),
            'SELECT x AS t1_x FROM t1 WHERE x IN (%(x_1)s, %(x_2)s)'
        )

    def test_select_count(self):
        table = self.create_table()

        self.assertEqual(
            self.compile(session.query(func.count(table.c.x))),
            'SELECT count() AS count_1 FROM t1'
        )

        self.assertEqual(
            self.compile(session.query(func.count()).select_from(table)),
            'SELECT count() AS count_1 FROM t1'
        )

    def test_limit(self):
        table = self.create_table()

        self.assertEqual(
            self.compile(session.query(table.c.x).limit(10), literal_binds=True),
            'SELECT x AS t1_x FROM t1  LIMIT 10'
        )

        self.assertEqual(
            self.compile(session.query(table.c.x).limit(10).offset(5), literal_binds=True),
            'SELECT x AS t1_x FROM t1  LIMIT 5, 10'
        )

        self.assertEqual(
            self.compile(session.query(table.c.x).offset(5).limit(10), literal_binds=True),
            'SELECT x AS t1_x FROM t1  LIMIT 5, 10'
        )

        with self.assertRaises(exc.CompileError) as ex:
            self.compile(session.query(table.c.x).offset(5), literal_binds=True)

        self.assertEqual(unicode(ex.exception), "Offset without limit is not supported")


class SelectEscapingTestCase(BaseTestCase):
    def compile(self, clause, **kwargs):
        return unicode(self._compile(clause, **kwargs))

    def test_select_escaping(self):
        self.assertEqual(
            self.compile(session.query(literal('\t')), literal_binds=True),
            "SELECT '\t' AS param_1"
        )


class FormatSectionTestCase(BaseTestCase):
    def _compile(self, clause, bind=session.bind, **kwargs):
        statement = super(FormatSectionTestCase, self)._compile(clause, bind=bind, **kwargs)

        context = ClickHouseExecutionContext._init_compiled(bind.dialect, bind, bind, statement, [])
        context.pre_exec()

        return context.statement

    def test_select(self):
        metadata = self.metadata()

        bind = session.bind
        bind.cursor = lambda : None

        table = Table(
            't1', metadata,
            Column('x', types.Int32, primary_key=True)
        )

        statement = self.compile(session.query(table.c.x), bind=bind)
        self.assertEqual(statement, 'SELECT x AS t1_x FROM t1 FORMAT TabSeparatedWithNamesAndTypes')

    def test_insert_from_select(self):
        metadata = self.metadata()

        bind = session.bind
        bind.cursor = lambda : None

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
