from datetime import date
from decimal import Decimal

import six


class Escaper(object):

    number_types = six.integer_types + (float, )
    string_types = six.string_types

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
            return [self.escape_item(x) for x in parameters]
        else:
            raise Exception("Unsupported param format: {}".format(parameters))

    def escape_number(self, item):
        return item

    def escape_date(self, item):
        return self.escape_string(item.strftime('%Y-%m-%d'))

    def escape_decimal(self, item):
        return float(item)

    def escape_item(self, item):
        if item is None:
            return 'NULL'
        elif isinstance(item, self.number_types):
            return self.escape_number(item)
        elif isinstance(item, date):
            return self.escape_date(item)
        elif isinstance(item, Decimal):
            return self.escape_decimal(item)
        elif isinstance(item, self.string_types):
            return self.escape_string(item)
        elif isinstance(item, (list, tuple)):
            return [self.escape_item(x) for x in item]
        else:
            raise Exception("Unsupported object {}".format(item))
