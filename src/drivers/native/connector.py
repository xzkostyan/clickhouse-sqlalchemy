from clickhouse_driver.client import Client
from clickhouse_driver.errors import Error as DriverError

from ...exceptions import DatabaseException
from ..escaper import Escaper

# PEP 249 module globals
apilevel = '2.0'
# Threads may share the module and connections.
threadsafety = 2
# Python extended format codes, e.g. ...WHERE name=%(name)s
paramstyle = 'pyformat'


class Error(Exception):
    """
    Exception that is the base class of all other error exceptions.
    You can use this to catch all errors with one single except statement.
    """
    pass


def connect(*args, **kwargs):
    """
    Make new connection.
    """
    return Connection(*args, **kwargs)


class Connection(object):
    transport_cls = Client

    def __init__(self, *args, **kwargs):
        self.transport = self.transport_cls(*args, **kwargs)
        super(Connection, self).__init__()

    def close(self):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass

    def cursor(self):
        return Cursor(self)


class Cursor(object):
    """
    These objects represent a database cursor, which is used to manage
    the context of a fetch operation.

    Cursors are not isolated, i.e., any changes done to the database
    by a cursor are immediately visible by other cursors or connections.
    """
    class States(object):
        (
            NONE,
            RUNNING,
            FINISHED
        ) = range(3)

    _states = States()

    _params_escaper = Escaper()

    def __init__(self, connection):
        self._connection = connection
        self._reset_state()
        self._arraysize = 1
        super(Cursor, self).__init__()

    @property
    def rowcount(self):
        # TODO: initial len(self._rows)?
        return -1

    @property
    def description(self):
        columns = self._columns or []
        types = self._types or []

        return [
            (name, type_code, None, None, None, None, True)
            for name, type_code in zip(columns, types)
        ]

    def close(self):
        self._connection.transport.disconnect()

    def make_external_tables(self, dialect, execution_options):
        external_tables = execution_options.get('external_tables')
        if external_tables is None:
            return

        tables = []
        type_compiler = dialect.type_compiler

        for table in external_tables:
            structure = []
            for c in table.columns:
                type_ = type_compiler.process(c.type, type_expression=c)
                structure.append((c.name, type_))

            tables.append({
                'name': table.name,
                'structure': structure,
                'data': table.dialect_options['clickhouse']['data']
            })

        return tables

    def execute(self, operation, parameters=None, context=None):
        if parameters is not None:
            operation = operation % self._params_escaper.escape(parameters)

        self._reset_state()
        self._begin_query()

        settings = context.execution_options.get('settings')

        transport = self._connection.transport
        try:
            external_tables = self.make_external_tables(
                context.dialect, context.execution_options
            )
            response = transport.execute(
                operation, with_column_types=True,
                external_tables=external_tables, settings=settings
            )

        except DriverError as orig:
            raise DatabaseException(orig)

        self._process_response(response)
        self._end_query()

    def executemany(self, operation, seq_of_parameters, context=None):
        self._reset_state()
        self._begin_query()

        settings = context.execution_options.get('settings')

        transport = self._connection.transport
        try:
            external_tables = self.make_external_tables(
                context.dialect, context.execution_options
            )
            response = transport.execute(
                operation, params=seq_of_parameters,
                external_tables=external_tables, settings=settings
            )

        except DriverError as orig:
            raise DatabaseException(orig)

        self._process_response(response)
        self._end_query()

    def fetchone(self):
        if self._state == self._states.NONE:
            raise RuntimeError("No query yet")

        if not self._rows:
            return None

        return self._rows.pop(0)

    def fetchmany(self, size=None):
        if size is None:
            size = 1
        rows = []
        for _ in range(size):
            row = self.fetchone()
            if row is None:
                break
            rows.append(row)

        return rows

    def fetchall(self):
        rows = []
        while True:
            row = self.fetchone()
            if row is None:
                break
            rows.append(row)

        return rows

    @property
    def arraysize(self):
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value):
        self._arraysize = value

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass

    # Iteration support.
    def __next__(self):
        one = self.fetchone()
        if one is None:
            raise StopIteration
        return one

    next = __next__

    def __iter__(self):
        return self

    def _process_response(self, response):
        if not response:
            self._columns = self._types = self._rows = []
            return

        rows, columns_with_types = response

        if columns_with_types:
            self._columns, self._types = zip(*columns_with_types)
        else:
            self._columns = self._types = []

        self._rows = rows

    def _reset_state(self):
        """
        Resets query state and get ready for another query.
        """
        self._state = self._states.NONE

        self._columns = None
        self._types = None
        self._rows = None

    def _begin_query(self):
        self._state = self._states.RUNNING

    def _end_query(self):
        self._state = self._states.FINISHED
