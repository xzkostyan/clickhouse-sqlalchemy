from sqlalchemy.sql.selectable import (
    Select as StandardSelect,
    Join as StandardJoin,
)

from clickhouse_sqlalchemy.ext.clauses import ArrayJoin
from ..ext.clauses import sample_clause


__all__ = ('Select', 'select')


class Join(StandardJoin):

    def __init__(self, left, right,
                 onclause=None, isouter=False, full=False,
                 type=None, strictness=None, distribution=None):
        if type is not None:
            type = type.upper()
        super(Join, self).__init__(left, right, onclause,
                                   isouter=isouter, full=full)
        self.strictness = None
        if strictness:
            self.strictness = strictness
        self.distribution = distribution
        self.type = type


class Select(StandardSelect):
    _with_totals = False
    _final_clause = None
    _sample_clause = None
    _array_join = None

    def with_totals(self):
        self._with_totals = True
        return self

    def final(self):
        self._final_clause = True
        return self

    def sample(self, sample):
        self._sample_clause = sample_clause(sample)
        return self

    def array_join(self, *columns):
        self._array_join = ArrayJoin(*columns)
        return self

    def join(self, right, onclause=None, isouter=False, full=False, type=None,
             strictness=None, distribution=None):
        return Join(self, right,
                    onclause=onclause, type=type,
                    isouter=isouter, full=full,
                    strictness=strictness, distribution=distribution)


select = Select
join = Join
