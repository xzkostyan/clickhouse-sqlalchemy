from ipaddress import IPv4Network, IPv6Network

from sqlalchemy.sql.type_api import to_instance, UserDefinedType
from sqlalchemy import types, func, and_, or_
from .ext.clauses import NestedColumn
from .util.compat import text_type


class String(types.String):
    pass


class Int(types.Integer):
    pass


class Float(types.Float):
    pass


class Array(types.TypeEngine):
    __visit_name__ = 'array'

    def __init__(self, item_type):
        self.item_type = item_type
        self.item_type_impl = to_instance(item_type)
        super(Array, self).__init__()

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


class Nullable(types.TypeEngine):
    __visit_name__ = 'nullable'

    def __init__(self, nested_type):
        self.nested_type = nested_type
        super(Nullable, self).__init__()


class UUID(String):
    __visit_name__ = 'uuid'


class LowCardinality(types.TypeEngine):
    __visit_name__ = 'lowcardinality'

    def __init__(self, nested_type):
        self.nested_type = nested_type
        super(LowCardinality, self).__init__()


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


class Float32(Float):
    __visit_name__ = 'float32'


class Float64(Float):
    __visit_name__ = 'float64'


class Date(types.Date):
    __visit_name__ = 'date'


class DateTime(types.Date):
    __visit_name__ = 'datetime'


class Enum(types.Enum):
    __visit_name__ = 'enum'

    def __init__(self, *enums, **kw):
        if not enums:
            enums = kw.get('_enums', ())  # passed as keyword

        super(Enum, self).__init__(*enums, **kw)


class Enum8(Enum):
    __visit_name__ = 'enum8'


class Enum16(Enum):
    __visit_name__ = 'enum16'


class Decimal(types.Numeric):
    __visit_name__ = 'numeric'


class Nested(types.TypeEngine):
    __visit_name__ = 'nested'

    def __init__(self, *columns):
        if not columns:
            raise ValueError('columns must be specified for nested type')
        self.columns = columns
        self._columns_dict = {col.name: col for col in columns}
        super(Nested, self).__init__()

    class Comparator(UserDefinedType.Comparator):
        def __getattr__(self, key):
            str_key = key.rstrip("_")
            try:
                sub = self.type._columns_dict[str_key]
            except KeyError:
                raise AttributeError(key)
            else:
                original_type = sub.type
                try:
                    sub.type = Array(sub.type)
                    expr = NestedColumn(self.expr, sub)
                    return expr
                finally:
                    sub.type = original_type

    comparator_factory = Comparator


class IPv4(types.UserDefinedType):
    __visit_name__ = "ipv4"

    def bind_processor(self, dialect):
        def process(value):
            return text_type(value)

        return process

    def literal_processor(self, dialect):
        bp = self.bind_processor(dialect)

        def process(value):
            return "'%s'" % bp(value)

        return process

    def bind_expression(self, bindvalue):
        return func.toIPv4(bindvalue)

    class comparator_factory(types.UserDefinedType.Comparator):
        def in_(self, other):
            if not isinstance(other, IPv4Network):
                other = IPv4Network(other)

            return and_(
                self >= other[0],
                self <= other[-1]
            )

        def notin_(self, other):
            if not isinstance(other, IPv4Network):
                other = IPv4Network(other)

            return or_(
                self < other[0],
                self > other[-1]
            )


class IPv6(types.UserDefinedType):
    __visit_name__ = "ipv6"

    def bind_processor(self, dialect):
        def process(value):
            return text_type(value)

        return process

    def literal_processor(self, dialect):
        bp = self.bind_processor(dialect)

        def process(value):
            return "'%s'" % bp(value)

        return process

    def bind_expression(self, bindvalue):
        return func.toIPv6(bindvalue)

    class comparator_factory(types.UserDefinedType.Comparator):
        def in_(self, other):
            if not isinstance(other, IPv6Network):
                other = IPv6Network(other)

            return and_(
                self >= other[0],
                self <= other[-1]
            )

        def notin_(self, other):
            if not isinstance(other, IPv6Network):
                other = IPv6Network(other)

            return or_(
                self < other[0],
                self > other[-1]
            )
