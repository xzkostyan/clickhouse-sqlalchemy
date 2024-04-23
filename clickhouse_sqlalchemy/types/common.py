from typing import Type, Union

from sqlalchemy import types
from sqlalchemy.sql.functions import Function
from sqlalchemy.sql.type_api import to_instance


class ClickHouseTypeEngine(types.TypeEngine):
    def compile(self, dialect=None):
        from clickhouse_sqlalchemy.drivers.base import clickhouse_dialect

        return super(ClickHouseTypeEngine, self).compile(
            dialect=clickhouse_dialect
        )


class String(types.String, ClickHouseTypeEngine):
    pass


class Int(types.Integer, ClickHouseTypeEngine):
    pass


class Float(types.Float, ClickHouseTypeEngine):
    pass


class Boolean(types.Boolean, ClickHouseTypeEngine):
    pass


class Array(ClickHouseTypeEngine):
    __visit_name__ = 'array'

    hashable = False

    def __init__(self, item_type):
        self.item_type = item_type
        self.item_type_impl = to_instance(item_type)
        super(Array, self).__init__()

    def __repr__(self):
        nested_type_str = \
            f'{self.item_type_impl.__module__}.{self.item_type_impl!r}'
        return f'Array({nested_type_str})'

    @property
    def python_type(self):
        return list

    def literal_processor(self, dialect):
        item_processor = self.item_type_impl.literal_processor(dialect)

        def process(value):
            processed_value = []
            for x in value:
                if item_processor:
                    x = item_processor(x)
                processed_value.append(x)
            return '[' + ', '.join(processed_value) + ']'
        return process


class Nullable(ClickHouseTypeEngine):
    __visit_name__ = 'nullable'

    def __init__(self, nested_type):
        self.nested_type = to_instance(nested_type)
        super(Nullable, self).__init__()


class UUID(String):
    __visit_name__ = 'uuid'


class LowCardinality(ClickHouseTypeEngine):
    __visit_name__ = 'lowcardinality'

    def __init__(self, nested_type):
        self.nested_type = to_instance(nested_type)
        super(LowCardinality, self).__init__()

    def __repr__(self):
        nested_type_str = f'{self.nested_type.__module__}.{self.nested_type!r}'
        return f'LowCardinality({nested_type_str})'


class Int8(Int):
    __visit_name__ = 'int8'


class UInt8(Int):
    __visit_name__ = 'uint8'


class Int16(Int):
    __visit_name__ = 'int16'


class UInt16(Int):
    __visit_name__ = 'uint16'


class Int32(Int):
    __visit_name__ = 'int32'


class UInt32(Int):
    __visit_name__ = 'uint32'


class Int64(Int):
    __visit_name__ = 'int64'


class UInt64(Int):
    __visit_name__ = 'uint64'


class Int128(Int):
    __visit_name__ = 'int128'


class UInt128(Int):
    __visit_name__ = 'uint128'


class Int256(Int):
    __visit_name__ = 'int256'


class UInt256(Int):
    __visit_name__ = 'uint256'


class Float32(Float):
    __visit_name__ = 'float32'


class Float64(Float):
    __visit_name__ = 'float64'


class Date(types.Date, ClickHouseTypeEngine):
    __visit_name__ = 'date'


class Date32(types.Date, ClickHouseTypeEngine):
    __visit_name__ = 'date32'


class DateTime(types.DateTime, ClickHouseTypeEngine):
    __visit_name__ = 'datetime'

    def __init__(self, timezone=None):
        super(DateTime, self).__init__()
        self.timezone = timezone


class DateTime64(DateTime, ClickHouseTypeEngine):
    __visit_name__ = 'datetime64'

    def __init__(self, precision=3, timezone=None):
        self.precision = precision
        super(DateTime64, self).__init__(timezone=timezone)


class Enum(types.Enum, ClickHouseTypeEngine):
    __visit_name__ = 'enum'
    native_enum = True

    def __init__(self, *enums, **kw):
        if not enums:
            enums = kw.get('_enums', ())  # passed as keyword

        super(Enum, self).__init__(*enums, **kw, convert_unicode=False)


class Enum8(Enum):
    __visit_name__ = 'enum8'


class Enum16(Enum):
    __visit_name__ = 'enum16'


class Decimal(types.Numeric, ClickHouseTypeEngine):
    __visit_name__ = 'numeric'


class Tuple(ClickHouseTypeEngine):
    __visit_name__ = 'tuple'

    def __init__(self, *nested_types):
        self.nested_types = nested_types
        super(Tuple, self).__init__()


class Map(ClickHouseTypeEngine):
    __visit_name__ = 'map'

    def __init__(self, key_type, value_type):
        self.key_type = key_type
        self.value_type = value_type
        super(Map, self).__init__()


class AggregateFunction(ClickHouseTypeEngine):
    __visit_name__ = 'aggregatefunction'

    def __init__(
        self,
        agg_func: Union[Function, str],
        *nested_types: Union[Type[ClickHouseTypeEngine], ClickHouseTypeEngine],
    ):
        self.agg_func = agg_func
        self.nested_types = [to_instance(val) for val in nested_types]
        super(AggregateFunction, self).__init__()

    def __repr__(self) -> str:
        type_strs = [f'{val.__module__}.{val!r}' for val in self.nested_types]

        if isinstance(self.agg_func, str):
            agg_str = self.agg_func
        else:
            agg_str = f'sa.func.{self.agg_func}'

        return f"AggregateFunction({agg_str}, {', '.join(type_strs)})"


class SimpleAggregateFunction(ClickHouseTypeEngine):
    __visit_name__ = 'simpleaggregatefunction'

    def __init__(
        self,
        agg_func: Union[Function, str],
        *nested_types: Union[Type[ClickHouseTypeEngine], ClickHouseTypeEngine],
    ):
        self.agg_func = agg_func
        self.nested_types = [to_instance(val) for val in nested_types]
        super(SimpleAggregateFunction, self).__init__()

    def __repr__(self) -> str:
        type_strs = [f'{val.__module__}.{val!r}' for val in self.nested_types]

        if isinstance(self.agg_func, str):
            agg_str = self.agg_func
        else:
            agg_str = f'sa.func.{self.agg_func}'

        return f"SimpleAggregateFunction({agg_str}, {', '.join(type_strs)})"
