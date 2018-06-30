from contextlib import contextmanager
from datetime import datetime

from sqlalchemy import Column
from sqlalchemy.sql.ddl import CreateTable

from clickhouse_sqlalchemy import types, engines, Table
from tests.session import native_session, system_native_session
from tests.testcase import BaseTestCase
from tests.config import database as test_database


class TypesTestCase(BaseTestCase):
    session = native_session

    @classmethod
    def setUpClass(cls):
        # System database is always present.
        system_native_session.execute(
            'DROP DATABASE IF EXISTS {}'.format(test_database)
        )
        system_native_session.execute(
            'CREATE DATABASE {}'.format(test_database)
        )

        super(BaseTestCase, cls).setUpClass()

    @contextmanager
    def create_table(self, table):
        table.drop(bind=self.session.bind, if_exists=True)
        table.create(bind=self.session.bind)
        try:
            yield
        except Exception:
            raise
        finally:
            table.drop(bind=self.session.bind)


class DateTimeTypeTestCase(TypesTestCase):
    table = Table(
        'test', TypesTestCase.metadata(),
        Column('x', types.DateTime, primary_key=True),
        engines.Memory()
    )

    def test_select_insert(self):
        dt = datetime(2018, 1, 1, 15, 20)

        with self.create_table(self.table):
            self.session.execute(self.table.insert(), [{'x': dt}])
            self.assertEqual(self.session.query(self.table.c.x).scalar(), dt)

    def test_create_table(self):
        self.assertEqual(
            self.compile(CreateTable(self.table)),
            'CREATE TABLE test (x DateTime) ENGINE = Memory'
        )
