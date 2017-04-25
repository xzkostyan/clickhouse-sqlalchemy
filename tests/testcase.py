import re

from src.compat import unicode

from unittest import TestCase
from sqlalchemy import MetaData
from sqlalchemy.orm import Query

from .session import session


class BaseTestCase(TestCase):
    strip_spaces = re.compile(r'[\n\t]')

    def metadata(self):
        return MetaData(bind=session.bind)

    def _compile(self, clause, bind=session.bind, literal_binds=False):
        if isinstance(clause, Query):
            context = clause._compile_context()
            context.statement.use_labels = True
            clause = context.statement

        compile_kwargs = {}
        if literal_binds:
            compile_kwargs['literal_binds'] = True

        return clause.compile(dialect=session.bind.dialect,
                              compile_kwargs=compile_kwargs)

    def compile(self, clause, **kwargs):
        query = unicode(self._compile(clause, **kwargs))
        return self.strip_spaces.sub('', query)
