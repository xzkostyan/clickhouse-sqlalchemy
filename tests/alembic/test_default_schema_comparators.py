from unittest import TestCase
from clickhouse_sqlalchemy.alembic.comparators import comparators, compare_mat_view


class AlembicComparatorsTestCase(TestCase):
    def test_default_schema_comparators(self):
        default_schema_comparators = comparators._registry[('schema', 'default')]
        clickhouse_schema_comparators = comparators._registry[('schema', 'clickhouse')]

        for comparator in default_schema_comparators:
            self.assertIn(comparator, clickhouse_schema_comparators)

        self.assertNotIn(compare_mat_view, default_schema_comparators)
