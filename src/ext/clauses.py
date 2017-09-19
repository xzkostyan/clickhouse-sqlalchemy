from sqlalchemy import util, exc
from sqlalchemy.sql import type_api
from sqlalchemy.sql.elements import BindParameter, ColumnElement
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
