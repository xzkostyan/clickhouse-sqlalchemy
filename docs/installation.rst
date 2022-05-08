.. _installation:

Installation
============

Python Version
--------------

Clickhouse-sqlalchemy supports Python 2.7 and newer.

Dependencies
------------

These distributions will be installed automatically when installing
clickhouse-sqlalchemy:

* `clickhouse-driver`_ ClickHouse Python Driver with native (TCP) interface support.
* `requests`_ a simple and elegant HTTP library.
* `ipaddress`_ backport ipaddress module.

.. _clickhouse-driver: https://pypi.org/project/clickhouse-driver/
.. _requests: https://pypi.org/project/requests/
.. _ipaddress: https://pypi.org/project/ipaddress/

If you are planning to use ``clickhouse-driver`` with compression you should
also install compression extras as well. See clickhouse-driver `documentation <https://clickhouse-driver.readthedocs.io>`_.

.. _installation-pypi:

Installation from PyPI
----------------------

The package can be installed using ``pip``:

    .. code-block:: bash

       pip install clickhouse-sqlalchemy


Installation from github
------------------------

Development version can be installed directly from github:

    .. code-block:: bash

       pip install git+https://github.com/xzkostyan/clickhouse-sqlalchemy@master#egg=clickhouse-sqlalchemy
