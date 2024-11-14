from . import base

try:
    from .native import base as native_driver

    base.dialect = native_driver.dialect
except ImportError:
    pass

if not hasattr(base, 'dialect'):
    try:
        from .http import base as http_driver

        base.dialect = http_driver.dialect
    except ImportError:
        pass

if not hasattr(base, 'dialect'):
    try:
        from .asynch import base as asynch_driver
        base.dialect = asynch_driver.dialect
    except ImportError:
        pass

if not hasattr(base, 'dialect'):
    raise RuntimeError('Unable to detect default dialect. Please install one.')
