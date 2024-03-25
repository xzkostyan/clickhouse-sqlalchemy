from sqlalchemy import Column
from sqlalchemy.sql.ddl import CreateTable

from clickhouse_sqlalchemy import types, engines, Table
from tests.testcase import BaseTestCase
from tests.util import with_native_and_http_sessions


@with_native_and_http_sessions
class GeoPointTestCase(BaseTestCase):
    table = Table(
        'test', BaseTestCase.metadata(),
        Column('p', types.Point),
        engines.Memory()
    )

    def test_create_table(self):
        self.assertEqual(
            self.compile(CreateTable(self.table)),
            'CREATE TABLE test (p Point) ENGINE = Memory'
        )

    def test_select_insert(self):
        a = (10.1, 12.3)

        with self.create_table(self.table):
            self.session.execute(self.table.insert(), [{'p': a}])
            qres = self.session.query(self.table.c.p)
            res = qres.scalar()
            self.assertEqual(res, a)

    def test_select_where_point(self):
        a = (10.1, 12.3)

        with self.create_table(self.table):
            self.session.execute(self.table.insert(), [{'p': a}])

            self.assertEqual(self.session.query(self.table.c.p).filter(
                self.table.c.p == (10.1, 12.3)).scalar(), a)


@with_native_and_http_sessions
class GeoRingTestCase(BaseTestCase):
    table = Table(
        'test', BaseTestCase.metadata(),
        Column('r', types.Ring),
        engines.Memory()
    )

    def test_create_table(self):
        self.assertEqual(
            self.compile(CreateTable(self.table)),
            'CREATE TABLE test (r Ring) ENGINE = Memory'
        )

    def test_select_insert(self):
        a = [(0, 0), (10, 0), (10, 10), (0, 10)]

        with self.create_table(self.table):
            self.session.execute(self.table.insert(), [{'r': a}])
            qres = self.session.query(self.table.c.r)
            res = qres.scalar()
            self.assertEqual(res, a)


@with_native_and_http_sessions
class GeoPolygonTestCase(BaseTestCase):
    table = Table(
        'test', BaseTestCase.metadata(),
        Column('pg', types.Polygon),
        engines.Memory()
    )

    def test_create_table(self):
        self.assertEqual(
            self.compile(CreateTable(self.table)),
            'CREATE TABLE test (pg Polygon) ENGINE = Memory'
        )

    def test_select_insert(self):
        a = [[(20, 20), (50, 20), (50, 50), (20, 50)],
             [(30, 30), (50, 50), (50, 30)]]

        with self.create_table(self.table):
            self.session.execute(self.table.insert(), [{'pg': a}])
            qres = self.session.query(self.table.c.pg)
            res = qres.scalar()
            self.assertEqual(res, a)


@with_native_and_http_sessions
class GeoMultiPolygonTestCase(BaseTestCase):
    table = Table(
        'test', BaseTestCase.metadata(),
        Column('mpg', types.MultiPolygon),
        engines.Memory()
    )

    def test_create_table(self):
        self.assertEqual(
            self.compile(CreateTable(self.table)),
            'CREATE TABLE test (mpg MultiPolygon) ENGINE = Memory'
        )

    def test_select_insert(self):
        a = [[[(0, 0), (10, 0), (10, 10), (0, 10)]],
             [[(20, 20), (50, 20), (50, 50), (20, 50)],
              [(30, 30), (50, 50), (50, 30)]]]

        with self.create_table(self.table):
            self.session.execute(self.table.insert(), [{'mpg': a}])
            qres = self.session.query(self.table.c.mpg)
            res = qres.scalar()
            self.assertEqual(res, a)
