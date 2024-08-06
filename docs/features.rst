.. _features:

Features
========

This section describes features that current dialect supports.

Tables and models definition
----------------------------

Both declarative and constructor-style tables supported:

    .. code-block:: python

        from sqlalchemy import create_engine, Column, MetaData, literal

        from clickhouse_sqlalchemy import (
            Table, make_session, get_declarative_base, types, engines
        )

        uri = 'clickhouse://default:@localhost/test'

        engine = create_engine(uri)
        session = make_session(engine)
        metadata = MetaData(bind=engine)

        Base = get_declarative_base(metadata=metadata)

        class Rate(Base):
            day = Column(types.Date, primary_key=True)
            value = Column(types.Int32, comment='Rate value')
            other_value = Column(types.DateTime)

            __table_args__ = (
                engines.Memory(),
                {'comment': 'Store rates'}
            )

        another_table = Table('another_rate', metadata,
            Column('day', types.Date, primary_key=True),
            Column('value', types.Int32, server_default=literal(1)),
            engines.Memory()
        )

Tables created in declarative way have lowercase with words separated by
underscores naming convention. But you can easy set you own via SQLAlchemy
``__tablename__`` attribute.


Functions
+++++++++

Many of the ClickHouse functions can be called using the SQLAlchemy ``func``
proxy. A few of aggregate functions require special handling though. There
following functions are supported:

* ``func.quantile(0.5, column1)`` becomes ``quantile(0.5)(column1)``
* ``func.quantileIf(0.5, column1, column2 > 10)`` becomes ``quantileIf(0.5)(column1, column2 > 10)``


Dialect-specific options
++++++++++++++++++++++++

You can specify particular codec for column:

    .. code-block:: python

        class Rate(Base):
            day = Column(types.Date, primary_key=True)
            value = Column(types.Int32)
            other_value = Column(
                types.DateTime,
                clickhouse_codec=('DoubleDelta', 'ZSTD')
            )

            __table_args__ = (
                engines.Memory(),
            )


    .. code-block:: sql

        CREATE TABLE rate (
            day Date,
            value Int32,
            other_value DateTime CODEC(DoubleDelta, ZSTD)
        ) ENGINE = Memory


``server_default`` will render as ``DEFAULT``

    .. code-block:: python

        class Rate(Base):
            day = Column(types.Date, primary_key=True)
            value = Column(types.Int32)
            other_value = Column(
                types.DateTime, server_default=func.now()
            )

            __table_args__ = (
                engines.Memory(),
            )

    .. code-block:: sql

        CREATE TABLE rate (
            day Date,
            value Int32,
            other_value DateTime DEFAULT now()
        ) ENGINE = Memory

``MATERIALIZED`` and ``ALIAS`` also supported

    .. code-block:: python

        class Rate(Base):
            day = Column(types.Date, primary_key=True)
            value = Column(types.Int32)
            other_value = Column(
                types.DateTime, clickhouse_materialized=func.now()
            )

            __table_args__ = (
                engines.Memory(),
            )

    .. code-block:: sql

        CREATE TABLE rate (
            day Date,
            value Int32,
            other_value DateTime MATERIALIZED now()
        ) ENGINE = Memory


    .. code-block:: python

        class Rate(Base):
            day = Column(types.Date, primary_key=True)
            value = Column(types.Int32)
            other_value = Column(
                types.DateTime, clickhouse_alias=func.now()
            )

            __table_args__ = (
                engines.Memory(),
            )

    .. code-block:: sql

        CREATE TABLE rate (
            day Date,
            value Int32,
            other_value DateTime ALIAS now()
        ) ENGINE = Memory

You can also specify another column as default, materialized and alias

    .. code-block:: python

        class Rate(Base):
            day = Column(types.Date, primary_key=True)
            value = Column(types.Int32)
            other_value = Column(types.Int32, server_default=value)

            __table_args__ = (
                engines.Memory(),
            )

    .. code-block:: sql

        CREATE TABLE rate (
            day Date,
            value Int32,
            other_value Int32 DEFAULT value
        ) ENGINE = Memory


Table Engines
+++++++++++++

Every table in ClickHouse requires engine. Engine can be specified in
declarative ``__table_args__``:

    .. code-block:: python

        from sqlalchemy import create_engine, MetaData, Column
        from clickhouse_sqlalchemy import (
            get_declarative_base, types, engines
        )

        engine = create_engine('clickhouse://localhost')
        metadata = MetaData(bind=engine)
        Base = get_declarative_base(metadata=metadata)

        class Statistics(Base):
            date = Column(types.Date, primary_key=True)
            sign = Column(types.Int8)
            grouping = Column(types.Int32)
            metric1 = Column(types.Int32)

            __table_args__ = (
                engines.CollapsingMergeTree(
                    sign,
                    partition_by=func.toYYYYMM(date),
                    order_by=(date, grouping)
                ),
            )

Or in table:

    .. code-block:: python

        from sqlalchemy import create_engine, MetaData, Column, text
        from clickhouse_sqlalchemy import (
            get_declarative_base, types, engines
        )

        engine = create_engine('clickhouse+native://localhost/default')
        metadata = MetaData(bind=engine)

        statistics = Table(
            'statistics', metadata,
            Column('date', types.Date, primary_key=True),
            Column('sign', types.Int8),
            Column('grouping', types.Int32),
            Column('metric1', types.Int32),

            engines.CollapsingMergeTree(
                'sign',
                partition_by=text('toYYYYMM(date)'),
                order_by=('date', 'grouping')
            )
        )

Engine parameters can be column variables or column names.

.. note::

    SQLAlchemy functions can be applied to variables, but not to names.

    This will work ``partition_by=func.toYYYYMM(date)`` and this will not:
    ``partition_by=func.toYYYYMM('date')``. You should use
    ``partition_by=text('toYYYYMM(date)')`` in the second case.

Currently supported engines:

* \*MergeTree
* Replicated*MergeTree
* Distributed
* Buffer
* View/MaterializedView
* Log/TinyLog
* Memory
* Null
* File

Each engine has it's own parameters. Please refer to ClickHouse documentation
about engines.

Engine settings can be passed as additional keyword arguments

    .. code-block:: python

        engines.MergeTree(
            partition_by=date,
            key='value'
        )

Will render to

    .. code-block:: sql

        MergeTree()
        PARTITION BY date
        SETTINGS key=value

More complex examples

    .. code-block:: python

        engines.MergeTree(order_by=func.tuple_())

        engines.MergeTree(
            primary_key=('device_id', 'timestamp'),
            order_by=('device_id', 'timestamp'),
            partition_by=func.toYYYYMM(timestamp)
        )

        engines.MergeTree(
            partition_by=text('toYYYYMM(date)'),
            order_by=('date', func.intHash32(x)),
            sample_by=func.intHash32(x)
        )

        engines.MergeTree(
            partition_by=date,
            order_by=(date, x),
            primary_key=(x, y),
            sample_by=func.random(),
            key='value'
        )

        engines.CollapsingMergeTree(
            sign,
            partition_by=date,
            order_by=(date, x)
        )

        engines.ReplicatedCollapsingMergeTree(
            '/table/path', 'name',
            sign,
            partition_by=date,
            order_by=(date, x)
        )

        engines.VersionedCollapsingMergeTree(
            sign, version,
            partition_by=date,
            order_by=(date, x),
        )

        engines.SummingMergeTree(
            columns=(y, ),
            partition_by=date,
            order_by=(date, x)
        )

        engines.ReplacingMergeTree(
            version='version',
            partition_by='date',
            order_by=('date', 'x')
        )


Tables can be reflected with engines

    .. code-block:: python

        from sqlalchemy import create_engine, MetaData
        from clickhouse_sqlalchemy import Table

        engine = create_engine('clickhouse+native://localhost/default')
        metadata = MetaData(bind=engine)

        statistics = Table('statistics', metadata, autoload=True)

.. note::

    Reflection is possible for tables created with modern syntax. Table with
    following engine can't be reflected.

    .. code-block::

        MergeTree(EventDate, (CounterID, EventDate), 8192)

.. note::

    Engine reflection can take long time if your database have many  tables.
    You can control engine reflection with **engine_reflection** connection
    parameter.

ON CLUSTER
~~~~~~~~~~

``ON CLUSTER`` clause will be automatically added to DDL queries (
``CREATE TABLE``, ``DROP TABLE``, etc.) if cluster is specified in
``__table_args__``


    .. code-block:: python

        class TestTable(...):
            ...

            __table_args__ = (
                engines.ReplicatedMergeTree(...),
                {'clickhouse_cluster': 'my_cluster'}
            )


TTL
~~~

``TTL`` clause can be rendered during table creation

    .. code-block:: python

        class TestTable(...):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)

            __table_args__ = (
                engines.MergeTree(ttl=date + func.toIntervalDay(1)),
            )


    .. code-block:: sql

        CREATE TABLE test_table (date Date, x Int32)
        ENGINE = MergeTree()
        TTL date + toIntervalDay(1)

Deletion

    .. code-block:: python

        from clickhouse_sqlalchemy.sql.ddl import ttl_delete

        class TestTable(...):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)

            __table_args__ = (
                engines.MergeTree(
                    ttl=ttl_delete(date + func.toIntervalDay(1))
                ),
            )

    .. code-block:: sql

        CREATE TABLE test_table (date Date, x Int32)
        ENGINE = MergeTree()
        TTL date + toIntervalDay(1) DELETE

Multiple clauses at once

    .. code-block:: python

        from clickhouse_sqlalchemy.sql.ddl import (
            ttl_delete,
            ttl_to_disk,
            ttl_to_volume
        )

        ttl = [
            ttl_delete(date + func.toIntervalDay(1)),
            ttl_to_disk(date + func.toIntervalDay(1), 'hdd'),
            ttl_to_volume(date + func.toIntervalDay(1), 'slow'),
        ]

        class TestTable(...):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)

            __table_args__ = (
                engines.MergeTree(ttl=ttl),
            )

    .. code-block:: sql

        CREATE TABLE test_table (date Date, x Int32)
        ENGINE = MergeTree()
        TTL date + toIntervalDay(1) DELETE,
            date + toIntervalDay(1) TO DISK 'hdd',
            date + toIntervalDay(1) TO VOLUME 'slow'

Custom engines
++++++++++++++

If some engine is not supported yet, you can add new one into your code in the
following way:

    .. code-block:: python

        from sqlalchemy import create_engine, MetaData, Column
        from clickhouse_sqlalchemy import (
            Table, get_declarative_base, types
        )
        from clickhouse_sqlalchemy.engines.base import Engine

        engine = create_engine('clickhouse://localhost/default')
        metadata = MetaData(bind=engine)
        Base = get_declarative_base(metadata=metadata)

        class Kafka(Engine):
            def __init__(self, broker_list, topic_list):
                self.broker_list = broker_list
                self.topic_list = topic_list
                super(Kafka, self).__init__()

            @property
            def name(self):
                return (
                    super(Kafka, self).name + '()' +
                    '\nSETTINGS kafka_broker_list={},'
                    '\nkafka_topic_list={}'.format(
                        self.broker_list, self.topic_list
                    )
                )

        table = Table(
            'test', metadata,
            Column('x', types.Int32),
            Kafka(
                broker_list='host:port',
                topic_list = 'topic1,topic2,...'
            )
        )

Materialized Views
------------------

Materialized Views can be defined in the same way as models. Definition consists
from two steps:

* storage definition (table that will store data);
* ``SELECT`` query definition.

    .. code-block:: python

        from clickhouse_sqlalchemy import MaterializedView, select

        class Statistics(Base):
            date = Column(types.Date, primary_key=True)
            sign = Column(types.Int8, nullable=False)
            grouping = Column(types.Int32, nullable=False)
            metric1 = Column(types.Int32, nullable=False)

            __table_args__ = (
                engines.CollapsingMergeTree(
                    sign,
                    partition_by=func.toYYYYMM(date),
                    order_by=(date, grouping)
                ),
            )


        # Define storage for Materialized View
        class GroupedStatistics(Base):
            date = Column(types.Date, primary_key=True)
            metric1 = Column(types.Int32, nullable=False)

            __table_args__ = (
                engines.SummingMergeTree(
                    partition_by=func.toYYYYMM(date),
                    order_by=(date, )
                ),
            )


        Stat = Statistics

        # Define SELECT for Materialized View
        MatView = MaterializedView(GroupedStatistics, select([
            Stat.date.label('date'),
            func.sum(Stat.metric1 * Stat.sign).label('metric1')
        ]).where(
            Stat.grouping > 42
        ).group_by(
            Stat.date
        ))

        Stat.__table__.create()
        MatView.create()

Defining materialized views in code is useful for further migrations.
Autogeneration can reduce possible human errors in case of columns and
materialized views.

.. note::

    Currently it's not possible to detect **database** engine during startup. It's
    required to specify whether or not materialized view will use ``TO [db.]name``
    syntax.

There are two database engines now: Ordinary and Atomic.

If your database has ``Ordinary`` engine inner table will be created
automatically for materialized view. You can control name generation only by
defining class for inner table with appropriate name.
``class GroupedStatistics`` in example above.

If your database has ``Atomic`` engine inner tables are not used for
materialized view you must add ``use_to`` for materialized view object:
``MaterializedView(..., use_to=True)``. You can optionally specify materialized
view name with ``name=...``. By default view name is table name with
``mv_suffix='_mv'``.

Examples:

* ``MaterializedView(TestTable, use_to=True)`` is declaration of materialized
  view ``test_table_mv``.
* ``MaterializedView(TestTable, use_to=True, name='my_mv')`` is declaration of
  materialized  view ``my_mv``.
* ``MaterializedView(TestTable, use_to=True, mv_suffix='_mat_view')`` is
  declaration of materialized  view ``test_table_mat_view``.

You can specify cluster for materialized view in inner table definition.

    .. code-block:: python

        class GroupedStatistics(...):
            ...

            __table_args__ = (
                engines.ReplicatedSummingMergeTree(...),
                {'clickhouse_cluster': 'my_cluster'}
            )

Materialized views can also store the aggregated data in a table using the
``AggregatingMergeTree`` engine. The aggregate columns are defined using
``AggregateFunction`` or ``SimpleAggregateFunction``.

    .. code-block:: python


        # Define storage for Materialized View
        class GroupedStatistics(Base):
            date = Column(types.Date, primary_key=True)
            metric1 = Column(SimpleAggregateFunction(sa.func.sum(), types.Int32), nullable=False)

            __table_args__ = (
                engines.AggregatingMergeTree(
                    partition_by=func.toYYYYMM(date),
                    order_by=(date, )
                ),
            )


Basic DDL support
-----------------

You can emit simple DDL. Example ``CREATE`` / ``DROP`` table:

    .. code-block:: python

        table = Rate.__table__
        table.create()
        another_table.create()

        another_table.drop()
        table.drop()

Query method chaining
---------------------

Common ``order_by``, ``filter``, ``limit``, ``offset``, etc. are supported
alongside with ClickHouse specific ``final`` and others.

    .. code-block:: python

        session.query(func.count(Rate.day)) \
            .filter(Rate.day > today - timedelta(20)) \
            .scalar()

        session.query(Rate.value) \
            .order_by(Rate.day.desc()) \
            .first()

        session.query(Rate.value) \
            .order_by(Rate.day) \
            .limit(10) \
            .all()

        session.query(func.sum(Rate.value)) \
            .scalar()

INSERT
------

Simple batch INSERT:

    .. code-block:: python

        from datetime import date, timedelta
        from sqlalchemy import func

        today = date.today()
        rates = [
            {'day': today - timedelta(i), 'value': 200 - i}
            for i in range(100)
        ]

        # Emits single INSERT statement.
        session.execute(table.insert(), rates)

INSERT FROM SELECT statement:

    .. code-block:: python

        from sqlalchemy import cast

        # Labels must be present.
        select_query = session.query(
            Rate.day.label('day'),
            cast(Rate.value * 1.5, types.Int32).label('value')
        ).subquery()

        # Emits single INSERT FROM SELECT statement
        session.execute(
            another_table.insert()
            .from_select(['day', 'value'], select_query)
        )

UPDATE and DELETE
-----------------

SQLAlchemy's update statement are mapped into ClickHouse's ``ALTER UPDATE``

    .. code-block:: python

        tbl = Table(...)
        session.execute(t1.update().where(t1.c.x == 25).values(x=5))

or

    .. code-block:: python

        tbl = Table(...)
        session.execute(update(t1).where(t1.c.x == 25).values(x=5))

becomes

    .. code-block:: sql

        ALTER TABLE ... UPDATE x=5 WHERE x = 25

Delete statement is also supported and mapped into ``ALTER DELETE``

    .. code-block:: python

        tbl = Table(...)
        session.execute(t1.delete().where(t1.c.x == 25))

or

    .. code-block:: python

        tbl = Table(...)
        session.execute(delete(t1).where(t1.c.x == 25))

becomes

    .. code-block:: sql

        ALTER TABLE ... DELETE WHERE x = 25


Many other SQLAlchemy features are supported out of the box. UNION ALL example:

    .. code-block:: python

        from sqlalchemy import union_all

        select_rate = session.query(
            Rate.day.label('date'),
            Rate.value.label('x')
        )
        select_another_rate = session.query(
            another_table.c.day.label('date'),
            another_table.c.value.label('x')
        )

        union_all(select_rate, select_another_rate) \
            .execute() \
            .fetchone()


SELECT extensions
-----------------

Dialect supports some ClickHouse extensions for ``SELECT`` query.

SAMPLE
++++++

    .. code-block:: python

        session.query(table.c.x).sample(0.1)

or

    .. code-block:: python

        select([table.c.x]).sample(0.1)

becomes

    .. code-block:: sql

        SELECT ... FROM ... SAMPLE 0.1

LIMIT BY
++++++++

    .. code-block:: python

        session.query(table.c.x).order_by(table.c.x) \
            .limit_by([table.c.x], offset=1, limit=2)

or

    .. code-block:: python

        select([table.c.x]).order_by(table.c.x) \
            .limit_by([table.c.x], offset=1, limit=2)

becomes

    .. code-block:: sql

        SELECT ... FROM ... ORDER BY ... LIMIT 1, 2 BY ...

Lambda
++++++

    .. code-block:: python

        from clickhouse_sqlalchemy.ext.clauses import Lambda

        session.query(
            func.arrayFilter(
                Lambda(lambda x: x.like('%World%')),
                literal(
                    ['Hello', 'abc World'],
                    types.Array(types.String)
                )
            ).label('test')
        )

becomes

    .. code-block:: sql

        SELECT arrayFilter(
            x -> x LIKE '%%World%%',
            ['Hello', 'abc World']
        ) AS test

JOIN
++++

ClickHouse's join is bit more powerful than usual SQL join. In this dialect
join is parametrized with following arguments:

* type: ``INNER|LEFT|RIGHT|FULL|CROSS``
* strictness: ``OUTER|SEMI|ANTI|ANY|ASOF``
* distribution: ``GLOBAL``

Here are some examples

    .. code-block:: python

        session.query(t1.c.x, t2.c.x).join(
            t2,
            t1.c.x == t2.c.y,
            type='inner',
            strictness='all',
            distribution='global'
        )

or

    .. code-block:: python

        select([t1.c.x, t2.c.x]).join(
            t2,
            t1.c.x == t2.c.y,
            type='inner',
            strictness='all',
            distribution='global'
        )

becomes

    .. code-block:: sql

        SELECT ... FROM ... GLOBAL ALL INNER JOIN ... ON ...


You can also control join parameters with native SQLAlchemy options as well:
``isouter`` and ``full``.


    .. code-block:: python

        session.query(t1.c.x, t2.c.x).join(
            t2,
            t1.c.x == t2.c.y,
            isouter=True,
            full=True
        )

becomes

    .. code-block:: sql

        SELECT ... FROM ... FULL OUTER JOIN ... ON ...

ARRAY JOIN
++++++++++

    .. code-block:: python

        session.query(...).array_join(...)

or

    .. code-block:: python

        select([...]).array_join(...)

becomes

    .. code-block:: sql

        SELECT ... FROM ... ARRAY JOIN ...

WITH CUBE/ROLLUP/TOTALS
+++++++++++++++++++++++

    .. code-block:: python

        session.query(table.c.x).group_by(table.c.x).with_cube()
        session.query(table.c.x).group_by(table.c.x).with_rollup()
        session.query(table.c.x).group_by(table.c.x).with_totals()

or

    .. code-block:: python

        select([table.c.x]).group_by(table.c.x).with_cube()
        select([table.c.x]).group_by(table.c.x).with_rollup()
        select([table.c.x]).group_by(table.c.x).with_totals()

becomes (respectively)

    .. code-block:: sql

        SELECT ... FROM ... GROUP BY ... WITH CUBE
        SELECT ... FROM ... GROUP BY ... WITH ROLLUP
        SELECT ... FROM ... GROUP BY ... WITH TOTALS

FINAL
+++++

.. note:: Currently ``FINAL`` clause is supported only for table specified in ``FROM`` clause. To apply ``FINAL`` modifier to all tables in a query, settings with "final=1" can be passed using execution options.

    .. code-block:: python

        session.query(table.c.x).final().group_by(table.c.x)

or

    .. code-block:: python

        select([table.c.x]).final().group_by(table.c.x)

becomes

    .. code-block:: sql

        SELECT ... FROM ... FINAL GROUP BY ...

Miscellaneous
-------------

Batching
++++++++

You may want to fetch very large result sets in chunks.

    .. code-block:: python

        session.query(...).yield_per(N)

.. attention:: This supported only in native driver.

In this case clickhouse-driver's ``execute_iter`` is used and setting
``max_block_size`` is set into ``N``.

There is side effect. If next query will be emitted before end of iteration over
query with yield there will be an error. Example

    .. code-block:: python

        def gen(session):
            yield from session.query(...).yield_per(N)

        rv = gen(session)

        # There will be an error
        session.query(...).all()

To avoid this side effect you should create another session

    .. code-block:: python

        class another_session():
            def __init__(self, engine):
                self.engine = engine
                self.session = None

            def __enter__(self):
                self.session = make_session(self.engine)
                return self.session

            def __exit__(self, *exc_info):
                self.session.close()

        def gen(session):
            with another_session(session.bind) as new_session:
                yield from new_session.query(...).yield_per(N)

        rv = gen(session)

        # There will be no error
        session.query(...).all()


Execution options
+++++++++++++++++

.. attention:: This supported only in native and asynch drivers.

You can override default ClickHouse server settings and pass desired settings
with  ``execution_options``. Set lower priority to query and limit max number
threads to execute the request

    .. code-block:: python

        settings = {'max_threads': 2, 'priority': 10}

        session.query(...).execution_options(settings=settings)


You can pass external tables to ClickHouse server with ``execution_options``

    .. code-block:: python

        table = Table(
            'ext_table1', metadata,
            Column('id', types.UInt64),
            Column('name', types.String),
            clickhouse_data=[(x, 'name' + str(x)) for x in range(10)],
            extend_existing=True
        )

        session.query(func.sum(table.c.id)) \
            .execution_options(external_tables=[table])
            .scalar()
