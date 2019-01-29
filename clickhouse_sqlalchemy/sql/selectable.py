from sqlalchemy.sql.selectable import Select as StandardSelect

from ..ext.clauses import sample_clause


__all__ = ('Select', 'select')


class Select(StandardSelect):
    _with_totals = False
    _sample_clause = None
    _array_join = None

    def with_totals(self):
        self._with_totals = True
        return self

    def sample(self, sample):
        self._sample_clause = sample_clause(sample)
        return self

    def array_join(self, *columns):
        self._array_join = columns
        return self


select = Select
