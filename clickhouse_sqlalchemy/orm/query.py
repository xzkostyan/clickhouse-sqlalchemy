from sqlalchemy import exc
from sqlalchemy.sql.base import _generative
from sqlalchemy.orm.query import Query as BaseQuery

from ..ext.clauses import (
    ArrayJoin,
    LeftArrayJoin,
    LimitByClause,
    sample_clause,
)


class Query(BaseQuery):
    _with_cube = False
    _with_rollup = False
    _with_totals = False
    _final = None
    _sample = None
    _limit_by = None
    _array_join = None

    def _compile_context(self, *args, **kwargs):
        context = super(Query, self)._compile_context(*args, **kwargs)
        query = context.query

        query._with_cube = self._with_cube
        query._with_rollup = self._with_rollup
        query._with_totals = self._with_totals
        query._final_clause = self._final
        query._sample_clause = sample_clause(self._sample)
        query._limit_by_clause = self._limit_by
        query._array_join = self._array_join

        return context

    @_generative
    def with_cube(self):
        if not self._group_by_clauses:
            raise exc.InvalidRequestError(
                "Query.with_cube() can be used only with specified "
                "GROUP BY, call group_by()"
            )
        if self._with_rollup:
            raise exc.InvalidRequestError(
                "Query.with_cube() and Query.with_rollup() are mutually "
                "exclusive"
            )

        self._with_cube = True
        return self

    @_generative
    def with_rollup(self):
        if not self._group_by_clauses:
            raise exc.InvalidRequestError(
                "Query.with_rollup() can be used only with specified "
                "GROUP BY, call group_by()"
            )
        if self._with_cube:
            raise exc.InvalidRequestError(
                "Query.with_cube() and Query.with_rollup() are mutually "
                "exclusive"
            )

        self._with_rollup = True
        return self

    @_generative
    def with_totals(self):
        if not self._group_by_clauses:
            raise exc.InvalidRequestError(
                "Query.with_totals() can be used only with specified "
                "GROUP BY, call group_by()"
            )

        self._with_totals = True
        return self

    def _add_array_join(self, columns, left):
        join_type = ArrayJoin if not left else LeftArrayJoin
        self._array_join = join_type(*columns)

    @_generative
    def array_join(self, *columns, **kwargs):
        left = kwargs.get("left", False)
        self._add_array_join(columns, left=left)
        return self

    @_generative
    def left_array_join(self, *columns):
        self._add_array_join(columns, left=True)
        return self

    @_generative
    def final(self):
        self._final = True
        return self

    @_generative
    def sample(self, sample):
        self._sample = sample
        return self

    @_generative
    def limit_by(self, by_clauses, limit, offset=None):
        self._limit_by = LimitByClause(by_clauses, limit, offset)
        return self

    def join(self, target, onclause=None, **kwargs):
        spec = {
            'type': kwargs.pop('type', None),
            'strictness': kwargs.pop('strictness', None),
            'distribution': kwargs.pop('distribution', None)
        }
        rv = super().join(target, onclause, **kwargs)
        x = rv._setup_joins[-1]
        x_spec = dict(spec)
        # use 'full' key to pass extra flags
        x_spec['full'] = x[-1]['full']
        x[-1]['full'] = x_spec

        return rv

    def outerjoin(self, *props, **kwargs):
        kwargs['type'] = kwargs.get('type') or 'LEFT OUTER'
        return self.join(*props, **kwargs)
