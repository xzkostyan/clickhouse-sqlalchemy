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
    'Date': lambda x: datetime.strptime(x, '%Y-%m-%d').date()
}


class RequestsTransport(object):
    def __init__(self, db_url, db_name, username, password, timeout=None,
                 **kwargs):
        self.db_url = db_url
        self.db_name = db_name
        self.auth = (username, password)
        self.timeout = float(timeout) if timeout is not None else None
        super(RequestsTransport, self).__init__()

    def execute(self, query, params=None):
        """
        Query is returning rows and these rows should be parsed or
        there is nothing to return.
        """
        r = self._send(query, params=params, stream=True)
        lines = r.iter_lines()
        names = parse_tsv(next(lines))
        types = parse_tsv(next(lines))
        convs = [converters.get(type_) for type_ in types]

        yield names
        yield types

        for line in lines:
            yield [
                (converter(x) if converter else x)
                for x, converter in zip(parse_tsv(line), convs)
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
