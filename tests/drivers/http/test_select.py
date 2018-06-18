from sqlalchemy import Column

from clickhouse_sqlalchemy import types, Table
from tests.session import session
from tests.testcase import BaseTestCase


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

    def test_select_format_clause(self):
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

    def test_insert_from_select_no_format_clause(self):
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
