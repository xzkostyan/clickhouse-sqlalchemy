from datetime import datetime

import requests

from ...exceptions import DatabaseException
from .exceptions import HTTPException
from .utils import parse_tsv


DEFAULT_DDL_TIMEOUT = None
DATE_NULL = '0000-00-00'
DATETIME_NULL = '0000-00-00 00:00:00'


def date_converter(x):
    if x != DATE_NULL:
        return datetime.strptime(x, '%Y-%m-%d').date()
    return None


def datetime_converter(x):
    if x != DATETIME_NULL:
        return datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
    return None


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
    'Date': date_converter,
    'DateTime': datetime_converter,
}


def _get_type(type_str):
    return converters.get(type_str)


class RequestsTransport(object):
    def __init__(self, db_url, db_name, username, password, timeout=None,
                 **kwargs):
        self.db_url = db_url
        self.db_name = db_name
        self.auth = (username, password)
        self.timeout = float(timeout) if timeout is not None else None
        self.headers = {
            k.replace('header__', ''): v for k, v in kwargs.items()
            if k.startswith('header__')
        }
        ddl_timeout = kwargs.pop('ddl_timeout', DEFAULT_DDL_TIMEOUT)
        if ddl_timeout is not None:
            ddl_timeout = int(ddl_timeout)
        self.ddl_timeout = ddl_timeout
        super(RequestsTransport, self).__init__()

    def execute(self, query, params=None):
        """
        Query is returning rows and these rows should be parsed or
        there is nothing to return.
        """
        r = self._send(query, params=params, stream=True)
        lines = r.iter_lines()
        try:
            names = parse_tsv(next(lines))
            types = parse_tsv(next(lines))
        except StopIteration:
            return

        convs = [_get_type(type_) for type_ in types]

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
        if self.ddl_timeout:
            params['distributed_ddl_task_timeout'] = self.ddl_timeout

        # TODO: retries, prepared requests
        r = requests.post(
            self.db_url, auth=self.auth, params=params, data=data,
            stream=stream, timeout=self.timeout, headers=self.headers,
        )
        if r.status_code != 200:
            orig = HTTPException(r.text)
            orig.code = r.status_code
            raise DatabaseException(orig)
        return r
