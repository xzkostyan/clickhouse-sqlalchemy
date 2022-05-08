Migrations
==========

Since version 0.1.10 clickhouse-sqlalchemy has alembic support. This support
allows autogenerate migrations from source code with some limitations.
This is the main advantage comparing to other migration tools when you need to
write plain migrations by yourself.

.. note::

    It is necessary to notice that ClickHouse doesn't have transactions.
    Therefore migration will not rolled back after some command with an error.
    And schema will remain in partially migrated state.

Autogenerate **will detect**:

* Table and materialized view additions, removals.
* Column additions, removals.

Example project with migrations https://github.com/xzkostyan/clickhouse-sqlalchemy-alembic-example.

Requirements
------------

Minimal versions:

* ClickHouse server 21.11.11.1
* clickhouse-sqlalchemy 0.1.10
* alembic 1.5.x

You can always write you migrations with pure alembic's ``op.execute`` if
autogenerate is not possible for your schema objects or your are using
``clickhouse-sqlalchemy<0.1.10``.

Limitations
-----------

Common limitations:

* Engines are not added into ``op.create_table``.
* ``Nullable(T)`` columns generation via ``Column(..., nullable=True)`` is not
  supported.

Currently ``ATTACH MATERIALIZED VIEW`` with modified ``SELECT`` statement
doesn't work for ``Atomic`` engine.

Migration adjusting
-------------------

You can and should adjust migrations after autogeneration.

Following parameters can be specified for:

* ``op.detach_mat_view``: ``if_exists``, ``on_cluster``, ``permanently``.
* ``op.attach_mat_view``: ``if_not_exists``, ``on_cluster``.
* ``op.create_mat_view``: ``if_not_exists``, ``on_cluster``, ``populate``.

See ClickHouse's Materialized View documentation.

For ``op.add_column`` you can add:

* ``AFTER name_after``: ``op.add_column(..., sa.Column(..., clickhouse_after=sa.text('my_column')))``.
