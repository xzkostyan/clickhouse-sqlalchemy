
DefaultDialect = None

try:
    from .native import base as native_driver

    DefaultDialect = native_driver.dialect
except ImportError:
    pass

if DefaultDialect is None:
    try:
        from .http import base as http_driver

        DefaultDialect = http_driver.dialect
    except ImportError:
        pass

if DefaultDialect is None:
    try:
        from .asynch import base as asynch_driver

        DefaultDialect = asynch_driver.dialect
    except ImportError:
        pass

if DefaultDialect is None:
    raise RuntimeError('Unable to detect default dialect. Please install one.')

dialect = DefaultDialect
