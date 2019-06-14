from datetime import datetime
from functools import partial
from decimal import Decimal

import pytz

import requests
from requests.exceptions import RequestException

from ...exceptions import DatabaseException, DatabaseNotAvailableException
from .exceptions import HTTPException
from .utils import parse_tsv


class RequestsTransport(object):
    def __init__(self, db_url, db_name, username, password, timeout=None,
                tz=None, converters=None, **kwargs):
        """

        :param db_url:
        :param db_name:
        :param username:
        :param password:
        :param timeout:
        :param tz: timezone in the server config
        :param converters: functions dict to replace standart
                           functions parsing response from ClickHouse
        :param kwargs:
        """
        self.db_url = db_url
        self.db_name = db_name
        self.auth = (username, password)
        self.timeout = float(timeout) if timeout is not None else None
        if tz:
            # except using tz in this class, Cursor use tz from this class
            self.tz = pytz.timezone(tz)

        self.converters = {
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
            'Date': lambda x: datetime.strptime(
                x.replace("0000-00-00", "1970-01-01"), '%Y-%m-%d').date(),
            'DateTime': lambda dt, tz=tz: datetime.strptime(
                    dt.replace("0000-00-00", "1970-01-01"), '%Y-%m-%d %H:%M:%S'
                ).replace(tzinfo=pytz.timezone(tz) if tz else None),
        }
        if converters:
            self.converters.update(converters)

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
        convs = []
        for type_ in types:
            if type_.startswith("Nullable("):
                type_ = type_[len("Nullable("):-1]
            if self.converters.get(type_):  # simple case
                convs.append(self.converters[type_])
            elif type_.startswith("DateTime("):  # datetime with timezone
                convs.append(partial(self.converters['DateTime'], tz=type_[10:-2]))
            elif type_.startswith("Decimal"):
                convs.append(Decimal)
            else:
                convs.append(None)

        yield names
        yield types

        for line in lines:
            if line in ['', b'']:  # TODO: separator for total row
                continue    # total is latest row, maybe somehow standalone?
            yield [
                (converter(x) if converter and x is not None else x)
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
        try:
            r = requests.post(
                self.db_url, auth=self.auth, params=params, data=data,
                stream=stream, timeout=self.timeout
            )
        except RequestException as e:
            raise DatabaseNotAvailableException(e)

        if r.status_code != 200:
            orig = HTTPException(r.text)
            raise DatabaseException(orig)
        return r
