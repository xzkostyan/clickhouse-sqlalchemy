from sqlalchemy import Column, func, literal, exc
from src.base import ClickHouseExecutionContext
from src.schema import Table
from src.compat import unicode
from src import types

from .testcase import BaseTestCase
from .session import session


class SelectTestCase(BaseTestCase):

    def create_table(self):
        return Table('t1', self.metadata(),
                     Column('x', types.Int32, primary_key=True))

    def test_select(self):
        table = self.create_table()

        query = session.query(table.c.x).filter(table.c.x.in_([1, 2]))
        query = self.compile(query)
        expected = 'SELECT x AS t1_x FROM t1 WHERE x IN (%(x_1)s, %(x_2)s)'
        self.assertEqual(query, expected)

    def test_select_count(self):
        table = self.create_table()

        query = self.compile(session.query(func.count(table.c.x)))
        self.assertEqual(query, 'SELECT count() AS count_1 FROM t1')

        query = self.compile(session.query(func.count()).select_from(table))
        self.assertEqual(query, 'SELECT count() AS count_1 FROM t1')

    def test_limit(self):
        table = self.create_table()

        query = self.compile(session.query(table.c.x).limit(10),
                             literal_binds=True)
        self.assertEqual(query, 'SELECT x AS t1_x FROM t1  LIMIT 10')

        query = self.compile(session.query(table.c.x).limit(10).offset(5),
                             literal_binds=True)
        self.assertEqual(query, 'SELECT x AS t1_x FROM t1  LIMIT 5, 10')

        query = self.compile(session.query(table.c.x).offset(5).limit(10),
                             literal_binds=True)
        self.assertEqual(query, 'SELECT x AS t1_x FROM t1  LIMIT 5, 10')

        with self.assertRaises(exc.CompileError) as ex:
            self.compile(session.query(table.c.x).offset(5),
                         literal_binds=True)

        expected = 'Offset without limit is not supported'
        self.assertEqual(unicode(ex.exception), expected)


class SelectEscapingTestCase(BaseTestCase):

    def compile(self, clause, **kwargs):
        return unicode(self._compile(clause, **kwargs))

    def test_select_escaping(self):
        query = self.compile(session.query(literal('\t')), literal_binds=True)
        self.assertEqual(query, "SELECT '\t' AS param_1")


class FormatSectionTestCase(BaseTestCase):

    def _compile(self, clause, bind=session.bind, **kwargs):
        statement = super(FormatSectionTestCase, self)._compile(
            clause, bind=bind, **kwargs)

        context = ClickHouseExecutionContext._init_compiled(
            bind.dialect, bind, bind, statement, [])
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
        expected = 'SELECT x AS t1_x FROM t1 FORMAT TabSeparatedWithNamesAndTypes'
        self.assertEqual(statement, expected)

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
