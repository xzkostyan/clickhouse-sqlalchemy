from sqlalchemy.sql.dml import Insert as BaseInsert

__all__ = ('Insert', 'insert')


class Insert(BaseInsert):
    _values_iterator: None

    def values_iterator(self, columns, iterator):
        self._values_iterator = iterator
        self._multi_values = ([{column: None for column in columns}],)
        return self


insert = Insert
