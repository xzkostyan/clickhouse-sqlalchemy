from datetime import datetime

import requests

from ...exceptions import DatabaseException
from .exceptions import HTTPException
from .utils import parse_tsv


converters = {
    'Int8': int,
    'UInt8': int,
    'Int16': int,
    'UInt16': int,
    'Int32': int,
    'UInt32': int,
    'Int64': int,
    'UInt64': int,
    'Float32': float,
    'Float64': float,
    'Date': lambda x: datetime.strptime(x, '%Y-%m-%d').date(),
    'DateTime': lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
}


def strip_first_and_last(value):
    if ((value.startswith("'") and value.endswith("'")) or
            (value.startswith('[') and value.endswith(']'))):
        return value[1:-1]
    return value


class RequestsTransport(object):
    def __init__(self, db_url, db_name, username, password, timeout=None,
                 **kwargs):
        self.db_url = db_url
        self.db_name = db_name
        if username and password:
            self.auth = (username, password)
        else:
            self.auth = None
        self.timeout = float(timeout) if timeout is not None else None
        super(RequestsTransport, self).__init__()

    def convert_type(self, value, db_type):
        if db_type.lower().startswith('array'):
            inner_type = db_type[6:-1]
            return [self.convert_type(strip_first_and_last(item.strip()), inner_type)  # noqa
                    for item in strip_first_and_last(value).split(',')]
        converter = converters.get(db_type)
        if converter:
            return converter(value)
        return value

    def execute(self, query, params=None):
        """
        Query is returning rows and these rows should be parsed or
        there is nothing to return.
        """
        r = self._send(query, params=params, stream=True)
        lines = r.iter_lines()
        names = parse_tsv(next(lines))
        types = parse_tsv(next(lines))

        yield names
        yield types

        for line in lines:
            yield [
                self.convert_type(column, db_type)
                for column, db_type in zip(parse_tsv(line), types)
            ]

    def raw(self, query, params=None, stream=False):
        """
        Performs raw query to database. Returns its output
        :param query: Query to execute
        :param params: Additional params should be passed during query.
        :param stream: If flag is true, Http response from ClickHouse will be
            streamed.
        :return: Query execution result
        """
        return self._send(query, params=params, stream=stream).text

    def _send(self, data, params=None, stream=False):
        data = data.encode('utf-8')
        params = params or {}
        params['database'] = self.db_name

        # TODO: retries, prepared requests
        r = requests.post(
            self.db_url, auth=self.auth, params=params, data=data,
            stream=stream, timeout=self.timeout
        )
        if r.status_code != 200:
            orig = HTTPException(r.text)
            raise DatabaseException(orig)
        return r
