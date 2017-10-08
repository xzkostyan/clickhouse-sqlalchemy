import re
from unittest import TestCase

from sqlalchemy import MetaData
from sqlalchemy.orm import Query

from tests.session import session


class BaseTestCase(TestCase):
    strip_spaces = re.compile(r'[\n\t]')
    session = session

    def metadata(self):
        return MetaData(bind=self.session.bind)

    def _compile(self, clause, bind=session.bind, literal_binds=False):
        if isinstance(clause, Query):
            context = clause._compile_context()
            context.statement.use_labels = True
            clause = context.statement

        kw = {}
        compile_kwargs = {}
        if literal_binds:
            compile_kwargs['literal_binds'] = True

        if compile_kwargs:
            kw['compile_kwargs'] = compile_kwargs

        return clause.compile(dialect=session.bind.dialect, **kw)

    def compile(self, clause, **kwargs):
        return self.strip_spaces.sub(
            '', str(self._compile(clause, **kwargs))
        )
