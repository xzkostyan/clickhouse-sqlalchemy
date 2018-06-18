from sqlalchemy import exc

from clickhouse_sqlalchemy.ext.clauses import Lambda
from tests.testcase import BaseTestCase


class LabmdaTestCase(BaseTestCase):
    def test_lambda_is_callable(self):
        with self.assertRaises(exc.ArgumentError) as ex:
            self.compile(Lambda(1))

        self.assertEqual(str(ex.exception), 'func must be callable')

    def test_lambda_no_arg_kwargs(self):
        with self.assertRaises(exc.CompileError) as ex:
            self.compile(Lambda(lambda x, *args: x * 2))

        self.assertEqual(
            str(ex.exception),
            'Lambdas with *args are not supported'
        )

        with self.assertRaises(exc.CompileError) as ex:
            self.compile(Lambda(lambda x, **kwargs: x * 2))

        self.assertEqual(
            str(ex.exception),
            'Lambdas with **kwargs are not supported'
        )

    def test_lambda_ok(self):
        self.assertEqual(
            self.compile(Lambda(lambda x: x * 2), literal_binds=True),
            'x -> x * 2'
        )

        def mul(x):
            return x * 2

        self.assertEqual(
            self.compile(Lambda(mul), literal_binds=True),
            'x -> x * 2'
        )
