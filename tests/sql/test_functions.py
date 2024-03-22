from sqlalchemy import Column, func

from clickhouse_sqlalchemy import types, Table

from tests.testcase import CompilationTestCase


class FunctionTestCase(CompilationTestCase):
    table = Table(
        't1', CompilationTestCase.metadata(),
        Column('x', types.Int32, primary_key=True),
        Column('time', types.DateTime)
    )

    def test_quantile(self):
        func0 = func.quantile(0.5, self.table.c.x)
        self.assertIsInstance(func0.type, types.Float64)
        func1 = func.quantile(0.5, self.table.c.time)
        self.assertIsInstance(func1.type, types.DateTime)
        self.assertEqual(
            self.compile(self.session.query(func0)),
            'SELECT quantile(0.5)(t1.x) AS quantile_1 FROM t1'
        )

        func2 = func.quantileIf(0.5, self.table.c.x, self.table.c.x > 10)

        self.assertEqual(
            self.compile(
                self.session.query(func2)
            ),
            'SELECT quantileIf(0.5)(t1.x, t1.x > %(x_1)s) AS ' +
            '"quantileIf_1" FROM t1'
        )
