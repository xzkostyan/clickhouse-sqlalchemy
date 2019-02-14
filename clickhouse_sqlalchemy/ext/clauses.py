from sqlalchemy import util, exc
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import type_api
from sqlalchemy.sql.elements import (
    BindParameter,
    ColumnElement,
    ClauseList,
    ColumnClause,
    Label,
)
from sqlalchemy.sql.visitors import Visitable


class SampleParam(BindParameter):
    pass


def sample_clause(element):
    """Convert the given value to an "sample" clause.

    This handles incoming element to an expression; if
    an expression is already given, it is passed through.

    """
    if element is None:
        return None
    elif hasattr(element, '__clause_element__'):
        return element.__clause_element__()
    elif isinstance(element, Visitable):
        return element
    else:
        return SampleParam(None, element, unique=True)


class Lambda(ColumnElement):
    """Represent a lambda function, ``Lambda(lambda x: 2 * x)``."""

    __visit_name__ = 'lambda'

    def __init__(self, func):
        if not util.callable(func):
            raise exc.ArgumentError('func must be callable')

        self.type = type_api.NULLTYPE
        self.func = func


class NestedColumn(ColumnClause):
    def __init__(self, parent, sub_column):
        self.parent = parent
        self.sub_column = sub_column
        if isinstance(self.parent, Label):
            table = self.parent.element.table
        else:
            table = self.parent.table
        super(NestedColumn, self).__init__(
            sub_column.name,
            sub_column.type,
            _selectable=table
        )


@compiles(NestedColumn)
def _comp(element, compiler, **kw):
    from_labeled_label = False
    if isinstance(element.parent, Label):
        from_labeled_label = True
    return "%s.%s" % (
        compiler.process(element.parent,
                         from_labeled_label=from_labeled_label,
                         within_label_clause=False,
                         within_columns_clause=True),
        compiler.visit_column(element, include_table=False),
    )


class ArrayJoin(ClauseList):
    __visit_name__ = 'array_join'
