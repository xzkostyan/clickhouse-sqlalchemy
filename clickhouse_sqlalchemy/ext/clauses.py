from sqlalchemy import util, exc
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import type_api
from sqlalchemy.sql.elements import (
    BindParameter,
    ColumnElement,
    ClauseList,
    ColumnClause,
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
    """serves the role of the "nested" column in a nested type expression."""

    def __init__(self, parent, sub_column):
        self.parent = parent
        self.sub_column = sub_column
        super(NestedColumn, self).__init__(
            sub_column.name, sub_column.type, _selectable=self.parent.table
        )


@compiles(NestedColumn)
def _comp(element, compiler, **kw):
    return "%s.%s" % (
        compiler.process(element.parent),
        compiler.visit_column(element, include_table=False),
    )


class ArrayJoin(ClauseList):
    __visit_name__ = 'ARRAY_JOIN'
