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


Documentation
=============

Documentation is available at https://clickhouse-sqlalchemy.readthedocs.io.


Usage
=====

Supported interfaces:

- **native** [recommended] (TCP) via `clickhouse-driver <https://github.com/mymarilyn/clickhouse-driver>`
- **async native** (TCP) via `asynch <https://github.com/long2ice/asynch>`
- **http** via requests

Define table

    .. code-block:: python

        from sqlalchemy import create_engine, Column, MetaData
        from sqlalchemy.orm import declarative_base, sessionmaker
        
        from clickhouse_sqlalchemy import types, engines
        
        uri = 'clickhouse+native://localhost/default'
        
        # Create an engine.
        engine = create_engine(uri)
        
        # Create a session.
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Define the base class using the declarative base.
        Base = declarative_base()
        
        class Rate(Base):
            __tablename__ = 'rate'
            day = Column(types.Date, primary_key=True)
            value = Column(types.Int32)
        
            __table_args__ = (
                engines.Memory(),
            )
        
        # Create the table.
        Rate.__table__.create(bind=engine)



Insert some data

    .. code-block:: python

        from datetime import date, timedelta

        from sqlalchemy import func

        today = date.today()
        rates = [
            {'day': today - timedelta(i), 'value': 200 - i}
            for i in range(100)
        ]


And query inserted data

    .. code-block:: python

        session.execute(Rate.__table__.insert(), rates)

        session.query(func.count(Rate.day)) \
            .filter(Rate.day > today - timedelta(20)) \
            .scalar()


License
=======

ClickHouse SQLAlchemy is distributed under the `MIT license
<http://www.opensource.org/licenses/mit-license.php>`_.
