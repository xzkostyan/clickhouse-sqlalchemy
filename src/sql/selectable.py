from sqlalchemy.sql.selectable import Select as StandardSelect


__all__ = ('Select', 'select')


class Select(StandardSelect):
    _with_totals = False

    def with_totals(self):
        self._with_totals = True
        return self


select = Select
