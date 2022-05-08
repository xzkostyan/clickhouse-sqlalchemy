.. _quickstart:

Quickstart
==========

This page gives a good introduction to clickhouse-sqlalchemy.
It assumes you already have clickhouse-sqlalchemy installed.
If you do not, head over to the :ref:`installation` section.

It should be pointed that session must be created with
``clickhouse_sqlalchemy.make_session``. Otherwise ``session.query`` and
``session.execute`` will not have ClickHouse SQL extensions. The same is
applied to ``Table`` and ``get_declarative_base``.


Let's define some table, insert data into it and query inserted data.

    .. code-block:: python

        from sqlalchemy import create_engine, Column, MetaData

        from clickhouse_sqlalchemy import (
            Table, make_session, get_declarative_base, types, engines
        )

        uri = 'clickhouse+native://localhost/default'

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

        # Emits CREATE TABLE statement
        Rate.__table__.create()


Now it's time to insert some data

    .. code-block:: python

        from datetime import date, timedelta

        from sqlalchemy import func

        today = date.today()
        rates = [
            {'day': today - timedelta(i), 'value': 200 - i}
            for i in range(100)
        ]


Let's query inserted data

    .. code-block:: python

        session.execute(Rate.__table__.insert(), rates)

        session.query(func.count(Rate.day)) \
            .filter(Rate.day > today - timedelta(20)) \
            .scalar()

Now you are ready to :ref:`configure your connection<connection>` and see more
ClickHouse :ref:`features<features>` support.