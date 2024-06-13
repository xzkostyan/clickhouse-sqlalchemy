.. _development:

Development
===========

Test configuration
------------------

In ``setup.cfg`` you can find ClickHouse server ports, credentials, logging
level and another options than can be tuned during local testing.

Running tests locally
---------------------

Install desired Python version with system package manager/pyenv/another manager.

Install test requirements and build package:

    .. code-block:: bash

        python testsrequire.py && python setup.py develop

ClickHouse on host machine
^^^^^^^^^^^^^^^^^^^^^^^^^^

Install desired versions of ``clickhouse-server`` and ``clickhouse-client`` on
your machine.

Run tests:

    .. code-block:: bash

        pytest -v

ClickHouse in docker
^^^^^^^^^^^^^^^^^^^^

Create container desired version of ``clickhouse-server``:

    .. code-block:: bash

        docker run --rm -p 127.0.0.1:9000:9000 -p 127.0.0.1:8123:8123 --name test-clickhouse-server clickhouse/clickhouse-server:$VERSION

Or run the docker-compose defined in tests folder:

    .. code-block:: bash

        cd tests && docker compose up -d

        # Optionally test connecting to server.
        docker run -it --rm --network clickhouse-net clickhouse/clickhouse-client --host test-clickhouse-server

And run tests:

    .. code-block:: bash

        pytest -v

``pip`` will automatically install all required modules for testing.

GitHub Actions in forked repository
-----------------------------------

Workflows in forked repositories can be used for running tests.

Workflows don't run in forked repositories by default.
You must enable GitHub Actions in the **Actions** tab of the forked repository.
