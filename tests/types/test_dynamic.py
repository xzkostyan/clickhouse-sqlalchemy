import Dynamic
from sqlalchemy import Column, text, inspect, func
from sqlalchemy.sql.ddl import CreateTable

from clickhouse_sqlalchemy import types, engines, Table
from tests.testcase import BaseTestCase, CompilationTestCase
from tests.util import class_name_func
from parameterized import parameterized_class
from tests.session import native_session


class DynamicCompilationTestCase(CompilationTestCase):
    def test_create_table(self):
        table = Table(
            'test', CompilationTestCase.metadata(),
            Column('x', types.Dynamic),
            engines.Memory()
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE test (x Dynamic) ENGINE = Memory'
        )


@parameterized_class(
    [{'session': native_session}],
    class_name_func=class_name_func
)
class DynamicTestCase(BaseTestCase):
    required_server_version = (24, 5, 1)

    table = Table(
        'test', BaseTestCase.metadata(),
        Column('x', types.Dynamic),
        engines.Memory()
    )

    def test_select_insert(self):
        data = {'k1': 1, 'k2': '2', 'k3': True}

        self.table.drop(bind=self.session.bind, if_exists=True)
        try:
            # http session is unsupport
            self.session.execute(
                text('SET allow_experimental_object_type = 1;')
            )
            self.session.execute(text(self.compile(CreateTable(self.table))))
            self.session.execute(self.table.insert(), [{'x': data}])
            coltype = inspect(self.session.bind).get_columns('test')[0]['type']
            self.assertIsInstance(coltype, types.Dynamic)
            # https://clickhouse.com/docs/en/sql-reference/functions/Dynamic-functions#toDynamicstring
            # The Dynamic type returns a tuple of values by default,
            # which needs to be converted to Dynamic using the
            # toDynamicString function.
            res = self.session.query(
                func.toDynamicString(self.table.c.x)
            ).scalar()
            self.assertEqual(Dynamic.loads(res), data)
        finally:
            self.table.drop(bind=self.session.bind, if_exists=True)
