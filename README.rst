|Build Status|

.. |Build Status| image:: http://drone.lensa.com:8000/api/badges/kszucs/clickhouse-sqlalchemy/status.svg
   :target: http://drone.lensa.com:8000/kszucs/clickhouse-sqlalchemy


ClickHouse SQLAlchemy
=====================

ClickHouse dialect for SQLAlchemy with basic types support that uses HTTP interface to
`ClickHouse database <https://clickhouse.yandex/>`_.

Installation
============

The package can be installed using ``pip``:

    .. code-block:: bash

       pip install clickhouse-sqlalchemy


Connection Parameters
=====================

ClickHouse SQLAlchemy uses the following syntax for the connection string:

    .. code-block:: python

     'clickhouse://<user>:<password>@<host>:<port>/<database>[?key=value..]'

Where:

- *port* is port ClickHouse server is bound to. Default is ``8123``.
- *database* is database connect to. Default is ``default``.

There are several options can be specified in query string:

- *timeout* in seconds. There is no timeout by default.
- *protocol* to use. Possible choices: ``http``, ``https``. ``http`` is default.


Connection string to database `test` in default ClickHouse installation:

    .. code-block:: python

         'clickhouse://default:@localhost/test'


When you are using `nginx` as proxy server for ClickHouse server connection string might look like:

    .. code-block:: python

         'clickhouse://user:password@example.com:8124/test?protocol=https'

Where ``8124`` is proxy port.


Features
========

Native SQLAlchemy declarative support
-------------------------------------

Both declarative and constructor-style tables support:

    .. code-block:: python

        from sqlalchemy import create_engine, Column, Table, MetaData, literal

        from clickhouse_sqlalchemy import types, engines
        from clickhouse_sqlalchemy.session import make_session
        from clickhouse_sqlalchemy.declarative import get_declarative_base

        uri = 'clickhouse://default:@localhost/test'

        engine = create_engine(uri)
        session = make_session(engine)
        metadata = MetaData(bind=engine)

        Base = get_declarative_base(metadata=metadata)

        class Rate(Base):
            day = Column(types.Date, primary_key=True)
            value = Column(types.Int32)

            __table_args__ = (
                engines.Memory(),
            )

        another_table = Table('another_rate', metadata,
            Column('day', types.Date, primary_key=True),
            Column('value', types.Int32, server_default=literal(1)),
            engines.Memory()
        )

Tables created in declarative way have lowercase with words separated by underscores naming convention.
But you can easy set you own via SQLAlchemy ``__tablename__`` attribute.

Basic DDL support
-----------------

You can emit simple DDL. Example ``CREATE/DROP`` table:

    .. code-block:: python

        table = Rate.__table__
        table.create()
        another_table.create()


        another_table.drop()
        table.drop()


Basic INSERT clause support
---------------------------

Simple batch INSERT:

    .. code-block:: python

        from datetime import date, timedelta
        from sqlalchemy import func

        today = date.today()
        rates = [{'day': today - timedelta(i), 'value': 200 - i} for i in range(100)]

        # Emits single INSERT statement.
        session.execute(table.insert(), rates)


Native SQLAlchemy query method chaining
---------------------------------------

``order_by``, ``filter``, ``limit``, ``offset``, etc. are supported:

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


Advanced INSERT clause support
------------------------------
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


Many but not all of SQLAlchemy features are supported out of the box.

UNION ALL example:

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

        union_all(select_rate, select_another_rate).execute().fetchone()


Running tests
=============

    .. code-block:: bash

        mkvirtualenv testenv && python setup.py test

``pip`` will automatically install all required modules for testing.


License
=======

ClickHouse SQLAlchemy is distributed under the `MIT license
<http://www.opensource.org/licenses/mit-license.php>`_.
