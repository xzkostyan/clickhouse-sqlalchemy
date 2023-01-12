User's Guide
------------

This part of the documentation focuses on step-by-step instructions for development with clickhouse-sqlalchemy.

It assumes that you have experience with SQLAlchemy. Consider `its <https://www.sqlalchemy.org>`_ docs at the first, if that is not so.
Experience with ClickHouse is also highly recommended.

ClickHouse server provides a lot of interfaces. This dialect supports:

* HTTP interface (port 8123 by default);
* Native (TCP) interface (port 9000 by default).

Each interface has it's own support by corresponding "driver":

- **http** via ``requests``
- **native** via ``clickhouse-driver`` or via ``asynch`` for async support

Native driver is recommended due to rich ``clickhouse-driver`` support. HTTP
driver has poor development support compare to native driver.
However default driver is still ``http``.

.. toctree::
   :maxdepth: 2

   installation
   quickstart
   connection
   features
   types
   migrations

Additional Notes
----------------

Legal information, changelog and contributing are here for the interested.

.. toctree::
   :maxdepth: 2

   development
   changelog
   license
   contributing
