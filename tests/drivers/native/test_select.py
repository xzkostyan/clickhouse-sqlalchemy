from sqlalchemy import Column

from clickhouse_sqlalchemy import engines, types, Table
from tests.session import native_session, mocked_engine
from tests.testcase import BaseTestCase


class SanityTestCase(BaseTestCase):
    session = native_session

    def test_sanity(self):
        table = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            engines.Memory()
        )

        with mocked_engine(self.session) as engine:
            table.drop(if_exists=True)
            engine.assert_sql(['DROP TABLE IF EXISTS t1'])
