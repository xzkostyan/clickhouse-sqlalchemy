from sqlalchemy import exc
from sqlalchemy.orm.query import Query as BaseQuery


class Query(BaseQuery):
    _with_totals = False

    def _compile_context(self, labels=True):
        context = super(Query, self)._compile_context(labels=labels)
        context.statement._with_totals = self._with_totals
        return context

    def with_totals(self):
        if not self._group_by:
            raise exc.InvalidRequestError(
                "Query.with_totals() can be used only with specified "
                "GROUP BY, call group_by()"
            )

        self._with_totals = True

        return self
