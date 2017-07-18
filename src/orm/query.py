from sqlalchemy import exc
from sqlalchemy.orm.query import Query as BaseQuery

from ..ext.clauses import sample_clause


class Query(BaseQuery):
    _with_totals = False
    _sample = None

    def _compile_context(self, labels=True):
        context = super(Query, self)._compile_context(labels=labels)
        statement = context.statement

        statement._with_totals = self._with_totals
        statement._sample_clause = sample_clause(self._sample)

        return context

    def with_totals(self):
        if not self._group_by:
            raise exc.InvalidRequestError(
                "Query.with_totals() can be used only with specified "
                "GROUP BY, call group_by()"
            )

        self._with_totals = True

        return self

    def sample(self, sample):
        self._sample = sample

        return self
