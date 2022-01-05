ClickHouse SQLAlchemy
=====================

ClickHouse dialect for SQLAlchemy to `ClickHouse database <https://clickhouse.yandex/>`_.


.. image:: https://img.shields.io/pypi/v/clickhouse-sqlalchemy.svg
    :target: https://pypi.org/project/clickhouse-sqlalchemy

.. image:: https://coveralls.io/repos/github/xzkostyan/clickhouse-sqlalchemy/badge.svg?branch=master
    :target: https://coveralls.io/github/xzkostyan/clickhouse-sqlalchemy?branch=master

.. image:: https://img.shields.io/pypi/l/clickhouse-sqlalchemy.svg
    :target: https://pypi.org/project/clickhouse-sqlalchemy

.. image:: https://img.shields.io/pypi/pyversions/clickhouse-sqlalchemy.svg
    :target: https://pypi.org/project/clickhouse-sqlalchemy

.. image:: https://img.shields.io/pypi/dm/clickhouse-sqlalchemy.svg
    :target: https://pypi.org/project/clickhouse-sqlalchemy

.. image:: https://github.com/xzkostyan/clickhouse-sqlalchemy/actions/workflows/actions.yml/badge.svg
   :target: https://github.com/xzkostyan/clickhouse-sqlalchemy/actions/workflows/actions.yml

Installation
============

The package can be installed using ``pip``:

    .. code-block:: bash

       pip install clickhouse-sqlalchemy

Interfaces support
------------------

- **native** [recommended] (TCP) via `clickhouse-driver <https://github.com/mymarilyn/clickhouse-driver>`_
- **http** via requests


Connection Parameters
=====================

ClickHouse SQLAlchemy uses the following syntax for the connection string:

    .. code-block:: python

     'clickhouse+<driver>://<user>:<password>@<host>:<port>/<database>[?key=value..]'

Where:

- *driver* is driver to use. Possible choices: ``http``, ``native``. ``http`` is default.
- *database* is database connect to. Default is ``default``.


Drivers options
===============

There are several options can be specified in query string.

HTTP
----

- *port* is port ClickHouse server is bound to. Default is ``8123``.
- *timeout* in seconds. There is no timeout by default.
- *protocol* to use. Possible choices: ``http``, ``https``. ``http`` is default.

Connection string to database `test` in default ClickHouse installation:

    .. code-block:: python

         'clickhouse://default:@localhost/test'


When you are using `nginx` as proxy server for ClickHouse server connection string might look like:

    .. code-block:: python

         'clickhouse://user:password@example.com:8124/test?protocol=https'

Where ``8124`` is proxy port.

If you need control over the underlying HTTP connection, pass a `requests.Session
<https://requests.readthedocs.io/en/master/user/advanced/#session-objects>`_ instance
to ``create_engine()``, like so:

    .. code-block:: python

        from sqlalchemy import create_engine
        from requests import Session

        uri = 'clickhouse://default:@localhost/test'

        engine = create_engine(uri, connect_args={'http_session': Session()})


Native
------

Please note that native connection **is not encrypted**. All data including
user/password is transferred in plain text. You should use this connection over
SSH or VPN (for example) while communicating over untrusted network.

Connection string to database `test` in default ClickHouse installation:

    .. code-block:: python

         'clickhouse+native://default:@localhost/test'

All connection string parameters are proxied to `clickhouse-driver`.
See it's `parameters <https://clickhouse-driver.readthedocs.io/en/latest/api.html#clickhouse_driver.connection.Connection>`_.


Features
========

SQLAlchemy declarative support
------------------------------

Both declarative and constructor-style tables support:

    .. code-block:: python

        from sqlalchemy import create_engine, Column, MetaData, literal

        from clickhouse_sqlalchemy import Table, make_session, get_declarative_base, types, engines

        uri = 'clickhouse://default:@localhost/test'

        engine = create_engine(uri)
        session = make_session(engine)
        metadata = MetaData(bind=engine)

        Base = get_declarative_base(metadata=metadata)

        class Rate(Base):
            day = Column(types.Date, primary_key=True)
            value = Column(types.Int32)
            other_value = Column(
                types.DateTime,
                clickhouse_codec=('DoubleDelta', 'ZSTD'),
            )

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


Common SQLAlchemy query method chaining
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


External data for query processing
----------------------------------

Currently can be used with native interface.

    .. code-block:: python

        ext = Table(
            'ext', metadata, Column('x', types.Int32),
            clickhouse_data=[(101, ), (103, ), (105, )], extend_existing=True
        )

        rv = session.query(Rate) \
            .filter(Rate.value.in_(session.query(ext.c.x))) \
            .execution_options(external_tables=[ext]) \
            .all()

        print(rv)

Supported ClickHouse-specific SQL
---------------------------------

- ``SELECT`` query:
    - ``WITH TOTALS``
    - ``SAMPLE``
    - lambda functions: ``x -> expr``
    - ``JOIN``

See `tests <https://github.com/xzkostyan/clickhouse-sqlalchemy/tree/master/tests>`_ for examples.


Overriding default query settings
---------------------------------

Set lower priority to query and limit max number threads to execute the request.

    .. code-block:: python

        rv = session.query(func.sum(Rate.value)) \
            .execution_options(settings={'max_threads': 2, 'priority': 10}) \
            .scalar()

        print(rv)


Running tests
=============

    .. code-block:: bash

        mkvirtualenv testenv && python setup.py test

``pip`` will automatically install all required modules for testing.


License
=======

ClickHouse SQLAlchemy is distributed under the `MIT license
<http://www.opensource.org/licenses/mit-license.php>`_.

How to Contribute
=================

#. Check for open issues or open a fresh issue to start a discussion around a feature idea or a bug.
#. Fork `the repository <https://github.com/xzkostyan/clickhouse-sqlalchemy>`_ on GitHub to start making your changes to the **master** branch (or branch off of it).
#. Write a test which shows that the bug was fixed or that the feature works as expected.
#. Send a pull request and bug the maintainer until it gets merged and published.
