from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeVar

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import coercions, roles
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.sql.functions import GenericFunction

from clickhouse_sqlalchemy import types

if TYPE_CHECKING:
    from sqlalchemy.sql._typing import _ColumnExpressionArgument

_T = TypeVar('_T', bound=Any)


class quantile(GenericFunction[_T]):
    inherit_cache = True

    def __init__(
        self, level: float, expr: _ColumnExpressionArgument[Any],
        condition: _ColumnExpressionArgument[Any] = None, **kwargs: Any
    ):
        arg: ColumnElement[Any] = coercions.expect(
            roles.ExpressionElementRole, expr, apply_propagate_attrs=self
        )

        args = [arg]
        if condition is not None:
            condition = coercions.expect(
                roles.ExpressionElementRole, condition,
                apply_propagate_attrs=self
            )
            args.append(condition)

        self.level = level

        if isinstance(arg.type, (types.Decimal, types.Float, types.Int)):
            return_type = types.Float64
        elif isinstance(arg.type, types.DateTime):
            return_type = types.DateTime
        elif isinstance(arg.type, types.Date):
            return_type = types.Date
        else:
            return_type = types.Float64

        kwargs['type_'] = return_type
        kwargs['_parsed_args'] = args
        super().__init__(arg, **kwargs)


class quantileIf(quantile[_T]):
    inherit_cache = True


@compiles(quantile, 'clickhouse')
@compiles(quantileIf, 'clickhouse')
def compile_quantile(element, compiler, **kwargs):
    args_str = compiler.function_argspec(element, **kwargs)
    return f'{element.name}({element.level}){args_str}'
