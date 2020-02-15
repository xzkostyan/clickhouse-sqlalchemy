from sqlalchemy import Column

from clickhouse_sqlalchemy import engines, types, Table
from tests.session import native_session, mocked_engine
from tests.testcase import BaseTestCase


class SanityTestCase(BaseTestCase):
    session = native_session

    def test_sanity(self):
        mock = mocked_engine(self.session)
        with mock as session:
            table = Table(
                't1', self.metadata(session=session),
                Column('x', types.Int32, primary_key=True),
                engines.Memory()
            )
            table.drop(if_exists=True)
            self.assertEqual(mock.history, ['DROP TABLE IF EXISTS t1'])
