from datetime import date, datetime
from decimal import Decimal
import enum
import uuid


class Escaper:

    number_types = (int, float, )

    escape_chars = {
        "\b": "\\b",
        "\f": "\\f",
        "\r": "\\r",
        "\n": "\\n",
        "\t": "\\t",
        "\0": "\\0",
        "\\": "\\\\",
        "'": "\\'"
    }

    def escape_string(self, value):
        value = ''.join(self.escape_chars.get(c, c) for c in value)
        return "'" + value + "'"

    def escape(self, parameters):
        if isinstance(parameters, dict):
            return {k: self.escape_item(v) for k, v in parameters.items()}
        elif isinstance(parameters, (list, tuple)):
            return "[" + ",".join(
                [str(self.escape_item(x)) for x in parameters]) + "]"
        else:
            raise Exception(f"Unsupported param format: {parameters}")

    def escape_number(self, item):
        return item

    def escape_date(self, item):
        # XXX: shouldn't this be `toDate(...)`?
        return self.escape_string(item.strftime('%Y-%m-%d'))

    def escape_datetime(self, item):
        # XXX: shouldn't this be `toDateTime(...)`?
        return self.escape_string(item.strftime('%Y-%m-%d %H:%M:%S'))

    def escape_datetime64(self, item):
        # XXX: shouldn't this be `toDateTime64(...)`?
        return self.escape_string(item.strftime('%Y-%m-%d %H:%M:%S.%f'))

    def escape_decimal(self, item):
        return float(item)

    def escape_uuid(self, item):
        return str(item)

    def escape_item(self, item):
        if item is None:
            return 'NULL'
        elif isinstance(item, self.number_types):
            return self.escape_number(item)
        elif isinstance(item, datetime):
            return self.escape_datetime(item)
        elif isinstance(item, date):
            return self.escape_date(item)
        elif isinstance(item, Decimal):
            return self.escape_decimal(item)
        elif isinstance(item, str):
            return self.escape_string(item)
        elif isinstance(item, (list, tuple)):
            return "[" + ", ".join(
                [str(self.escape_item(x)) for x in item]
            ) + "]"
        elif isinstance(item, dict):
            return "{" + ", ".join(
                ["{}: {}".format(
                    self.escape_item(k),
                    self.escape_item(v)
                ) for k, v in item.items()]
            ) + "}"
        elif isinstance(item, enum.Enum):
            return self.escape_string(item.name)
        elif isinstance(item, uuid.UUID):
            return self.escape_uuid(item)
        else:
            raise Exception(f"Unsupported object {item}")
