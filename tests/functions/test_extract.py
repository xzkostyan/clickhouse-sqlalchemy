"""
Test EXTRACT
"""
from sqlalchemy import Column, extract

from clickhouse_sqlalchemy import types
from tests.testcase import BaseTestCase


def get_date_column(name):
    """
    Return types.Date column
    :param name: Column name
    :return: sqlalchemy.Column
    """
    return Column(name, types.Date)


class ExtractTestCase(BaseTestCase):
    def test_extract_year(self):
        self.assertEqual(
            self.compile(extract('year', get_date_column('x'))),
            'toYear(x)'
        )

    def test_extract_month(self):
        self.assertEqual(
            self.compile(extract('month', get_date_column('x'))),
            'toMonth(x)'
        )

    def test_extract_day(self):
        self.assertEqual(
            self.compile(extract('day', get_date_column('x'))),
            'toDayOfMonth(x)'
        )

    def test_extract_unknown(self):
        self.assertEqual(
            self.compile(extract('test', get_date_column('x'))),
            'x'
        )
